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

        let skill_slug = params.skill_slug.trim().to_owned();
        let source_kind = params.source_kind.trim().to_ascii_lowercase();

        if skill_slug.is_empty() || source_kind.is_empty() {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "skill_slug and source_kind are required",
                    json!({
                        "skill_slug": skill_slug,
                        "source_kind": source_kind,
                    }),
                ),
            )
            .await;
            return;
        }
        if !matches!(source_kind.as_str(), "system" | "user" | "registry") {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_SOURCE_NOT_SUPPORTED,
                    "source_kind must be system, user, or registry",
                    json!({
                        "skill_slug": skill_slug,
                        "source_kind": source_kind,
                    }),
                ),
            )
            .await;
            return;
        }
        if !is_qualified_slug(skill_slug.as_str()) {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "skill_slug must use owner/slug",
                    json!({
                        "skill_slug": skill_slug,
                        "source_kind": source_kind,
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
                    skill_slug.as_str(),
                    source_kind.as_str(),
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
                skill_slug: skill_slug.clone(),
                source_kind: source_kind.clone(),
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

        if matches!(params.allow_implicit_invocation, Some(false)) {
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
            let catalog = match load_catalog(&context.catalog_params) {
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

            let implicit_invocation_locked = catalog.skills.iter().any(|skill| {
                skill.identity.source_kind.as_db_value() == source_kind
                    && qualified_skill_slug(
                        skill.identity.owner.as_str(),
                        skill.identity.slug.as_str(),
                    ) == skill_slug
                    && !skill_implicit_invocation_editable(skill)
            });

            if implicit_invocation_locked {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INVALID_REQUEST,
                        "allow_implicit_invocation cannot be disabled for this system skill",
                        json!({
                            "skill_slug": skill_slug,
                            "source_kind": source_kind,
                            "allow_implicit_invocation": false,
                        }),
                    ),
                )
                .await;
                return;
            }
        }

        let record = WorkspaceSkillPolicyRecord {
            workspace_id: workspace_id.clone(),
            skill_slug,
            source_kind,
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
