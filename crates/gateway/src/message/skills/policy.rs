use super::*;

impl MessageProcessor {
    pub(crate) async fn skills_policy_list(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsPolicyListParams,
    ) {
        let connection_id = request_context.connection_id();
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
        let context = match self.skills_runtime_context(workspace_id.as_str()) {
            Ok(context) => context,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to resolve skills runtime context",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        let catalog = match self
            .load_skills_catalog(workspace_id.as_str(), &context)
            .await
        {
            Ok(catalog) => catalog,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to load skills catalog",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let catalog_by_id = catalog
            .skills
            .iter()
            .map(|skill| (skill.identity.skill_id.clone(), skill))
            .collect::<HashMap<_, _>>();
        let policies = rows
            .iter()
            .filter_map(|row| {
                let Some(skill) = catalog_by_id.get(&row.skill_id) else {
                    warn!(
                        workspace_id = %row.workspace_id,
                        skill_id = %row.skill_id,
                        "omitting orphaned skill policy from list response"
                    );
                    return None;
                };
                Some(SkillWorkspacePolicy {
                    workspace_id: row.workspace_id.clone(),
                    skill_id: row.skill_id.clone(),
                    owner: skill.identity.owner.clone(),
                    slug: skill.identity.slug.clone(),
                    source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                    enabled: row.enabled,
                    allow_implicit_invocation: row.allow_implicit_invocation,
                })
            })
            .collect();
        let payload = SkillsPolicyListResponse { policies };
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsPolicySetParams,
    ) {
        let connection_id = request_context.connection_id();
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

        let context = match self.skills_runtime_context(workspace_id.as_str()) {
            Ok(context) => context,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to resolve skills runtime context",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        let catalog = match self
            .load_skills_catalog(workspace_id.as_str(), &context)
            .await
        {
            Ok(catalog) => catalog,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to load skills catalog",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        let Some(target) = catalog
            .skills
            .iter()
            .find(|skill| skill.identity.skill_id == params.skill_id)
        else {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_NOT_FOUND,
                    "skill was not found",
                    json!({"skill_id": params.skill_id}),
                ),
            )
            .await;
            return;
        };
        let now = now_timestamp_secs();
        if params.enabled.is_none() && params.allow_implicit_invocation.is_none() {
            if let Err(error) = self
                .crud_store
                .delete_workspace_skill_policy(workspace_id.as_str(), &params.skill_id)
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
                skill_id: params.skill_id.clone(),
                owner: target.identity.owner.clone(),
                slug: target.identity.slug.clone(),
                source_kind: target.identity.source_kind.as_db_value().to_owned(),
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
                    skill_id: policy.skill_id,
                    owner: policy.owner,
                    slug: policy.slug,
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

        if matches!(params.allow_implicit_invocation, Some(false)) {
            if !skill_implicit_invocation_editable(target) {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INVALID_REQUEST,
                        "allow_implicit_invocation cannot be disabled for this system skill",
                        json!({
                            "skill_id": params.skill_id,
                            "allow_implicit_invocation": false,
                        }),
                    ),
                )
                .await;
                return;
            }
        }

        let record = WorkspaceSkillPolicyRecord {
            id: pioneer_protocol::generate_id(21),
            workspace_id: workspace_id.clone(),
            skill_id: params.skill_id.clone(),
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
                skill_id: record.skill_id.clone(),
                owner: target.identity.owner.clone(),
                slug: target.identity.slug.clone(),
                source_kind: target.identity.source_kind.as_db_value().to_owned(),
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
                skill_id: record.skill_id,
                owner: target.identity.owner.clone(),
                slug: target.identity.slug.clone(),
                source_kind: target.identity.source_kind.as_db_value().to_owned(),
                change_type: "policy".to_owned(),
                fingerprint_before: None,
                fingerprint_after: None,
            }],
            now,
        )
        .await;
    }
}
