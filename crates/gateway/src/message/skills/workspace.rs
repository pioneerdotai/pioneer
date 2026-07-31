use super::*;

pub(crate) fn skills_runtime_context_from_config(
    tool_loop_config: &ToolLoopConfig,
    workspace_id: &str,
) -> Result<SkillsRuntimeContext> {
    let skills = &tool_loop_config.skills;
    let system_roots = skills
        .system_roots
        .iter()
        .map(|raw| resolve_root_path(raw.as_str(), workspace_id))
        .collect::<Vec<_>>();
    let bundled = system_roots
        .first()
        .map(|root| crate::system_skills::bundled_system_skill_catalog_entries(root.as_path()))
        .transpose()?
        .unwrap_or_default();
    let user_roots = skills
        .user_roots
        .iter()
        .map(|raw| resolve_root_path(raw.as_str(), workspace_id))
        .collect::<Vec<_>>();
    let registry_roots = skills
        .registry_roots
        .iter()
        .map(|raw| resolve_root_path(raw.as_str(), workspace_id))
        .collect::<Vec<_>>();
    let catalog_params = SkillCatalogLoadParams {
        installations: Vec::new(),
        bundled,
        max_file_bytes: skills.max_skill_file_bytes,
    };
    let Some(registry_root) = registry_roots.first().cloned() else {
        bail!("skills registry root is not configured");
    };
    let Some(user_root) = user_roots.first().cloned() else {
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
        max_upload_compressed_bytes: skills.security.max_install_archive_compressed_bytes.max(1),
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

pub(crate) async fn load_skills_catalog_from_store(
    crud_store: &CrudStore,
    workspace_id: &str,
    context: &SkillsRuntimeContext,
) -> Result<pioneer_skills::SkillCatalogSnapshot> {
    let installations = crud_store
        .list_skill_installations()
        .await?
        .into_iter()
        .filter(|row| row.scope_key == workspace_id || row.source_kind == "system")
        .map(|row| {
            let source_kind = match row.source_kind.as_str() {
                "system" => SkillSourceKind::System,
                "user" => SkillSourceKind::User,
                "registry" => SkillSourceKind::Registry,
                other => bail!("unknown installed skill source kind `{other}`"),
            };
            let trust_level = match row.trust_level.as_str() {
                "internal" => pioneer_skills::SkillTrustLevel::Internal,
                "verified" => pioneer_skills::SkillTrustLevel::Verified,
                "community" => pioneer_skills::SkillTrustLevel::Community,
                "untrusted" => pioneer_skills::SkillTrustLevel::Untrusted,
                other => bail!("unknown installed skill trust level `{other}`"),
            };
            Ok(pioneer_skills::SkillCatalogInstallation {
                skill_id: row.skill_id,
                owner: row.owner,
                slug: row.slug,
                version: row.version,
                source_kind,
                source_ref: row.source_ref,
                install_path: PathBuf::from(row.install_path),
                trust_level,
                fingerprint: row.fingerprint,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut params = context.catalog_params.clone();
    params.installations = installations;
    load_catalog(&params)
}

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

    pub(crate) fn skills_runtime_context(
        &self,
        workspace_id: &str,
    ) -> Result<SkillsRuntimeContext> {
        skills_runtime_context_from_config(&self.tool_loop_config, workspace_id)
    }

    pub(crate) async fn load_skills_catalog(
        &self,
        workspace_id: &str,
        context: &SkillsRuntimeContext,
    ) -> Result<pioneer_skills::SkillCatalogSnapshot> {
        load_skills_catalog_from_store(self.crud_store.as_ref(), workspace_id, context).await
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
                    SkillPolicyKey::new(skill.identity.skill_id.clone()),
                    context.global_policy_defaults.clone(),
                )
            })
            .collect::<HashMap<_, _>>();
        let workspace_by_key = workspace_policies
            .iter()
            .map(|policy| {
                (
                    SkillPolicyKey::new(policy.skill_id.clone()),
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
        self.notify_skill_projection_changed(workspace_id, reason, changes, Vec::new(), created_at)
            .await;
    }

    pub(super) async fn notify_skill_projection_changed(
        &self,
        workspace_id: &str,
        reason: &str,
        changes: Vec<SkillChangedItem>,
        pack_changes: Vec<SkillPackChangedItem>,
        created_at: i64,
    ) {
        let notification = skill_projection_changed_notification(
            workspace_id,
            self.next_skills_snapshot_version(),
            reason,
            changes,
            pack_changes,
            created_at,
        );

        self.send_gateway_management_notification(events::SKILLS_CHANGED, &notification)
            .await;
    }
}

fn skill_projection_changed_notification(
    workspace_id: &str,
    snapshot_version: u64,
    reason: &str,
    changes: Vec<SkillChangedItem>,
    pack_changes: Vec<SkillPackChangedItem>,
    created_at: i64,
) -> SkillsChangedNotification {
    SkillsChangedNotification {
        workspace_id: workspace_id.to_owned(),
        snapshot_version,
        reason: reason.to_owned(),
        changes,
        pack_changes: ordered_skill_pack_changes(pack_changes),
        created_at,
    }
}

fn ordered_skill_pack_changes(
    mut pack_changes: Vec<SkillPackChangedItem>,
) -> Vec<SkillPackChangedItem> {
    pack_changes.sort_by(|left, right| left.pack_id.cmp(&right.pack_id));
    pack_changes
}

#[cfg(test)]
mod tests {
    use super::{ordered_skill_pack_changes, skill_projection_changed_notification};
    use pioneer_protocol::{SkillChangedItem, SkillId, SkillPackChangedItem, SkillPackId};

    fn pack_id(character: char) -> SkillPackId {
        SkillPackId::new(character.to_string().repeat(21)).expect("valid pack id")
    }

    fn skill_id(character: char) -> SkillId {
        SkillId::new(character.to_string().repeat(21)).expect("valid skill id")
    }

    #[test]
    fn skill_pack_changes_are_ordered_by_pack_id_without_losing_names() {
        let changes = ordered_skill_pack_changes(vec![
            SkillPackChangedItem {
                pack_id: pack_id('Z'),
                change_type: "uninstalled".to_owned(),
                name_before: Some("Removed".to_owned()),
                name_after: None,
            },
            SkillPackChangedItem {
                pack_id: pack_id('A'),
                change_type: "installed".to_owned(),
                name_before: None,
                name_after: Some("Installed".to_owned()),
            },
            SkillPackChangedItem {
                pack_id: pack_id('M'),
                change_type: "updated".to_owned(),
                name_before: Some("Before".to_owned()),
                name_after: Some("After".to_owned()),
            },
        ]);

        assert_eq!(
            changes
                .iter()
                .map(|change| change.pack_id.clone())
                .collect::<Vec<_>>(),
            vec![pack_id('A'), pack_id('M'), pack_id('Z')]
        );
        assert_eq!(changes[0].name_before, None);
        assert_eq!(changes[0].name_after.as_deref(), Some("Installed"));
        assert_eq!(changes[1].name_before.as_deref(), Some("Before"));
        assert_eq!(changes[1].name_after.as_deref(), Some("After"));
        assert_eq!(changes[2].name_before.as_deref(), Some("Removed"));
        assert_eq!(changes[2].name_after, None);
    }

    #[test]
    fn combined_projection_notification_preserves_preordered_children_and_orders_parents() {
        let child_changes = vec![
            SkillChangedItem {
                skill_id: skill_id('A'),
                owner: None,
                slug: "alpha".to_owned(),
                source_kind: "user".to_owned(),
                change_type: "install".to_owned(),
                fingerprint_before: None,
                fingerprint_after: Some("alpha-after".to_owned()),
            },
            SkillChangedItem {
                skill_id: skill_id('Z'),
                owner: None,
                slug: "zeta".to_owned(),
                source_kind: "user".to_owned(),
                change_type: "install".to_owned(),
                fingerprint_before: None,
                fingerprint_after: Some("zeta-after".to_owned()),
            },
        ];
        let notification = skill_projection_changed_notification(
            "workspace-one",
            7,
            "pack_installed",
            child_changes.clone(),
            vec![
                SkillPackChangedItem {
                    pack_id: pack_id('Z'),
                    change_type: "installed".to_owned(),
                    name_before: None,
                    name_after: Some("Zeta".to_owned()),
                },
                SkillPackChangedItem {
                    pack_id: pack_id('A'),
                    change_type: "installed".to_owned(),
                    name_before: None,
                    name_after: Some("Alpha".to_owned()),
                },
            ],
            9,
        );

        assert_eq!(notification.changes, child_changes);
        assert_eq!(notification.pack_changes[0].pack_id, pack_id('A'));
        assert_eq!(notification.pack_changes[1].pack_id, pack_id('Z'));
    }
}
