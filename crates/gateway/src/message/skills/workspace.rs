use super::*;

impl MessageProcessor {
    pub(super) async fn validate_skills_workspace(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        workspace_id: String,
        method: &str,
    ) -> Result<String, JsonRpcErrorResponse> {
        if workspace_id.trim().is_empty() {
            return Err(skills_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                SKILLS_ERROR_INVALID_REQUEST,
                format!("`workspace_id` is required for `{method}`"),
                json!({}),
            ));
        }

        let workspace_id = self
            .workspace_manager
            .validate_workspace_id(workspace_id.as_str())
            .await
            .map_err(|error| match error {
                WorkspaceError::Internal(message) => skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    format!("failed to validate workspace: {message}"),
                    json!({}),
                ),
                WorkspaceError::WorkspaceNotFound(_) | WorkspaceError::WorkspaceInactive(_) => {
                    skills_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        SKILLS_ERROR_NOT_FOUND,
                        format!("workspace `{}` is unavailable", workspace_id),
                        json!({"workspace_id": workspace_id}),
                    )
                }
                WorkspaceError::InvalidWorkspaceId
                | WorkspaceError::InvalidWorkspaceName
                | WorkspaceError::NoWorkspaceUpdateFields => skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    format!("invalid workspace_id for `{method}`"),
                    json!({"workspace_id": workspace_id}),
                ),
            })?;

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        Ok(workspace_id)
    }

    pub(super) fn skills_runtime_context(
        &self,
        workspace_id: &str,
    ) -> Result<SkillsRuntimeContext> {
        let skills = &self.tool_loop_config.skills;
        let catalog_params = SkillCatalogLoadParams {
            system_roots: skills
                .system_roots
                .iter()
                .map(|raw| resolve_root_path(raw.as_str(), workspace_id))
                .collect(),
            user_roots: skills
                .user_roots
                .iter()
                .map(|raw| resolve_root_path(raw.as_str(), workspace_id))
                .collect(),
            registry_roots: skills
                .registry_roots
                .iter()
                .map(|raw| resolve_root_path(raw.as_str(), workspace_id))
                .collect(),
            max_skills_per_source: skills.max_skills_per_source,
            max_file_bytes: skills.max_skill_file_bytes,
        };
        let Some(registry_root) = catalog_params.registry_roots.first().cloned() else {
            bail!("skills registry root is not configured");
        };
        let Some(user_root) = catalog_params.user_roots.first().cloned() else {
            bail!("skills user root is not configured");
        };
        let skill_runtime_root = user_root
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| user_root.clone());

        Ok(SkillsRuntimeContext {
            user_lock_path: user_root.join("skills-lock.toml"),
            registry_lock_path: registry_root.join("skills-lock.toml"),
            user_root,
            registry_root,
            upload_root: skill_runtime_root.join("uploads"),
            materialized_root: skill_runtime_root.join(".materialized"),
            upload_ttl_secs: skills.security.upload_ttl_secs.max(60),
            upload_recommended_chunk_size_bytes: skills
                .security
                .upload_recommended_chunk_size_bytes
                .max(1),
            upload_max_chunk_size_bytes: skills.security.upload_max_chunk_size_bytes.max(1),
            max_upload_compressed_bytes: skills
                .security
                .max_install_archive_compressed_bytes
                .max(1),
            max_upload_uncompressed_bytes: skills
                .security
                .max_install_archive_uncompressed_bytes
                .max(1),
            max_upload_archive_entries: skills.security.max_install_archive_entries.max(1),
            catalog_params,
            validation_policy: SkillValidationPolicy {
                strict_agentskills: skills.validation.strict_agentskills,
                accept_openclaw_profile: skills.validation.accept_openclaw_profile,
                preflight_on_resolve: skills.dependencies.preflight_on_resolve,
                allow_untrusted_install: skills.security.allow_untrusted_install,
                security_scan_on_resolve: true,
                max_security_scan_file_bytes: skills.security.max_install_file_bytes,
            },
            security_policy: SkillSecurityPolicy {
                allow_untrusted_install: skills.security.allow_untrusted_install,
                min_trust_for_shell_tools: skills.security.min_trust_for_shell_tools.clone(),
                min_trust_for_http_tools: skills.security.min_trust_for_http_tools.clone(),
                min_trust_for_function_proxy_tools: skills
                    .security
                    .min_trust_for_function_proxy_tools
                    .clone(),
                max_install_archive_bytes: skills.security.max_install_archive_bytes,
                max_install_file_bytes: skills.security.max_install_file_bytes,
            },
            global_policy_defaults: SkillPolicy {
                enabled: Some(skills.enabled),
                allow_implicit_invocation: Some(skills.allow_implicit_invocation),
            },
        })
    }

    pub(super) fn build_policy_set(
        &self,
        skills: &[pioneer_skills::SkillDefinition],
        workspace_policies: &[WorkspaceSkillPolicyRecord],
        context: &SkillsRuntimeContext,
    ) -> SkillPolicySet {
        let global_by_key = skills
            .iter()
            .map(|skill| {
                (
                    SkillPolicyKey::new(
                        qualified_skill_slug(
                            skill.identity.owner.as_str(),
                            skill.identity.slug.as_str(),
                        ),
                        skill.identity.source_kind.as_db_value().to_owned(),
                    ),
                    context.global_policy_defaults.clone(),
                )
            })
            .collect::<HashMap<_, _>>();
        let workspace_by_key = workspace_policies
            .iter()
            .map(|policy| {
                (
                    SkillPolicyKey::new(policy.skill_slug.clone(), policy.source_kind.clone()),
                    SkillPolicy {
                        enabled: policy.enabled,
                        allow_implicit_invocation: policy.allow_implicit_invocation,
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        SkillPolicySet {
            global_by_key,
            workspace_by_key,
        }
    }

    pub(super) fn compute_skill_health_summary(
        &self,
        skill: &pioneer_skills::SkillDefinition,
        context: &SkillsRuntimeContext,
    ) -> SkillHealthSummary {
        let validation_issues = blocking_validation_issues(skill, &context.validation_policy)
            .iter()
            .map(to_protocol_validation_issue)
            .collect::<Vec<_>>();

        let dependency_failures = if context.validation_policy.preflight_on_resolve {
            pioneer_skills::evaluate_skill_dependencies(skill, &DependencyCheckInput::baseline())
                .failing_diagnostics()
                .iter()
                .map(to_protocol_dependency)
                .collect()
        } else {
            Vec::new()
        };

        let security_blocks = if context.validation_policy.security_scan_on_resolve {
            let source_root = Path::new(skill.identity.source_root.as_str());
            let skill_dir = Path::new(skill.identity.skill_dir.as_str());
            if source_root.exists() && skill_dir.exists() {
                pioneer_skills::scan_skill_directory(
                    source_root,
                    skill_dir,
                    context
                        .validation_policy
                        .max_security_scan_file_bytes
                        .max(1),
                )
                .findings
                .iter()
                .filter(|finding| finding.severity == "block")
                .map(to_protocol_security_finding)
                .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let status = if dependency_failures.is_empty() && security_blocks.is_empty() {
            "ok"
        } else {
            "blocked"
        };
        let status = if validation_issues.is_empty() {
            status
        } else {
            "blocked"
        };

        SkillHealthSummary {
            status: status.to_owned(),
            dependency_failures,
            security_blocks,
            validation_issues,
        }
    }

    pub(super) fn build_trust_gate(
        &self,
        skill: &pioneer_skills::SkillDefinition,
    ) -> Vec<SkillTrustGateStatus> {
        let security = &self.tool_loop_config.skills.security;
        let trust_level = skill.runtime.trust_level.clone();
        let rows = vec![
            (
                "shell",
                security.min_trust_for_shell_tools.clone(),
                pioneer_skills::SkillRuntimeToolKind::Shell,
            ),
            (
                "http",
                security.min_trust_for_http_tools.clone(),
                pioneer_skills::SkillRuntimeToolKind::Http,
            ),
            (
                "function_proxy",
                security.min_trust_for_function_proxy_tools.clone(),
                pioneer_skills::SkillRuntimeToolKind::FunctionProxy,
            ),
        ];

        rows.into_iter()
            .map(|(tool_kind, minimum_trust, runtime_kind)| {
                let allowed = if !security.allow_untrusted_install
                    && matches!(trust_level, pioneer_skills::SkillTrustLevel::Untrusted)
                {
                    false
                } else {
                    let min_required = pioneer_skills::minimum_trust_for_tool_kind(
                        &runtime_kind,
                        &SkillSecurityPolicy {
                            allow_untrusted_install: security.allow_untrusted_install,
                            min_trust_for_shell_tools: security.min_trust_for_shell_tools.clone(),
                            min_trust_for_http_tools: security.min_trust_for_http_tools.clone(),
                            min_trust_for_function_proxy_tools: security
                                .min_trust_for_function_proxy_tools
                                .clone(),
                            max_install_archive_bytes: security.max_install_archive_bytes,
                            max_install_file_bytes: security.max_install_file_bytes,
                        },
                    );
                    pioneer_skills::trust_satisfies_minimum(trust_level.clone(), min_required)
                };

                SkillTrustGateStatus {
                    tool_kind: tool_kind.to_owned(),
                    minimum_trust: trust_level_as_str(&minimum_trust).to_owned(),
                    allowed,
                }
            })
            .collect()
    }

    pub(super) async fn notify_skills_changed(
        &self,
        workspace_id: &str,
        reason: &str,
        changes: Vec<SkillChangedItem>,
        created_at: i64,
    ) {
        let notification = SkillsChangedNotification {
            workspace_id: workspace_id.to_owned(),
            snapshot_version: self.next_skills_snapshot_version(),
            reason: reason.to_owned(),
            changes,
            created_at,
        };

        self.send_notification_to_workspace_connections(
            workspace_id,
            events::SKILLS_CHANGED,
            &notification,
        )
        .await;
    }
}
