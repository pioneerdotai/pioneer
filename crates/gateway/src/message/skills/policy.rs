use super::*;

impl MessageProcessor {
    pub(crate) async fn skills_policy_list(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsPolicyListParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_POLICY_LIST,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let rows = match self
            .crud_store
            .list_workspace_skill_policies(workspace_id.as_str())
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to load workspace skill policies",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let payload = SkillsPolicyListResponse {
            policies: rows
                .iter()
                .map(|row| SkillWorkspacePolicy {
                    workspace_id: row.workspace_id.clone(),
                    skill_slug: row.skill_slug.clone(),
                    source_kind: row.source_kind.clone(),
                    enabled: row.enabled,
                    allow_implicit_invocation: row.allow_implicit_invocation,
                })
                .collect(),
        };
        let response = match JsonRpcResponse::from_result(request_id, &payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        None,
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to encode skills/policy/list response",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send skills/policy/list response"
            );
        }
    }

    pub(crate) async fn skills_policy_set(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsPolicySetParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_POLICY_SET,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        if params.skill_slug.trim().is_empty() || params.source_kind.trim().is_empty() {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "skill_slug and source_kind are required",
                    json!({
                        "skill_slug": params.skill_slug,
                        "source_kind": params.source_kind,
                    }),
                ),
            )
            .await;
            return;
        }
        if !is_qualified_slug(params.skill_slug.as_str()) {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "skill_slug must use owner/slug",
                    json!({
                        "skill_slug": params.skill_slug,
                        "source_kind": params.source_kind,
                    }),
                ),
            )
            .await;
            return;
        }

        let now = now_timestamp_secs();
        if params.enabled.is_none() && params.allow_implicit_invocation.is_none() {
            if let Err(error) = self
                .crud_store
                .delete_workspace_skill_policy(
                    workspace_id.as_str(),
                    params.skill_slug.as_str(),
                    params.source_kind.as_str(),
                )
                .await
            {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to delete workspace policy override",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }

            let policy = SkillWorkspacePolicy {
                workspace_id: workspace_id.clone(),
                skill_slug: params.skill_slug.clone(),
                source_kind: params.source_kind.clone(),
                enabled: None,
                allow_implicit_invocation: None,
            };

            let response = match JsonRpcResponse::from_result(
                request_id,
                &SkillsPolicySetResponse {
                    policy: policy.clone(),
                },
            ) {
                Ok(response) => response,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            None,
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to encode skills/policy/set response",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };

            if let Err(error) = self.send_json(connection_id, &response).await {
                warn!(
                    connection_id,
                    error = %format!("{error:#}"),
                    "failed to send skills/policy/set response"
                );
                return;
            }

            self.notify_skills_changed(
                workspace_id.as_str(),
                "policy_updated",
                vec![SkillChangedItem {
                    slug: policy.skill_slug,
                    source_kind: policy.source_kind,
                    change_type: "policy".to_owned(),
                    fingerprint_before: None,
                    fingerprint_after: None,
                }],
                now,
            )
            .await;
            return;
        }

        let record = WorkspaceSkillPolicyRecord {
            workspace_id: workspace_id.clone(),
            skill_slug: params.skill_slug.clone(),
            source_kind: params.source_kind.clone(),
            enabled: params.enabled,
            allow_implicit_invocation: params.allow_implicit_invocation,
        };
        if let Err(error) = self
            .crud_store
            .upsert_workspace_skill_policy(&record, now)
            .await
        {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to persist workspace skill policy",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }

        let payload = SkillsPolicySetResponse {
            policy: SkillWorkspacePolicy {
                workspace_id: record.workspace_id.clone(),
                skill_slug: record.skill_slug.clone(),
                source_kind: record.source_kind.clone(),
                enabled: record.enabled,
                allow_implicit_invocation: record.allow_implicit_invocation,
            },
        };
        let response = match JsonRpcResponse::from_result(request_id, &payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        None,
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to encode skills/policy/set response",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send skills/policy/set response"
            );
            return;
        }

        self.notify_skills_changed(
            workspace_id.as_str(),
            "policy_updated",
            vec![SkillChangedItem {
                slug: record.skill_slug,
                source_kind: record.source_kind,
                change_type: "policy".to_owned(),
                fingerprint_before: None,
                fingerprint_after: None,
            }],
            now,
        )
        .await;
    }
}
