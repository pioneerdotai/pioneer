use super::*;

fn ordered_cli_runtime_explicit_skills(
    attachments: &[crate::cli_runtime::skills::CliRuntimeSkillAttachment],
    explicit_refs: &[SkillExplicitRef],
    catalog: &pioneer_skills::SkillCatalogSnapshot,
    resolution: &pioneer_skills::SkillResolutionResult,
) -> anyhow::Result<Vec<pioneer_skills::ResolvedSkill>> {
    let mut ordered = Vec::with_capacity(attachments.len());
    for (attachment, explicit_ref) in attachments.iter().zip(explicit_refs) {
        let expected_capability_id = explicit_ref.capability_id();
        if attachment.capability_id != expected_capability_id {
            anyhow::bail!(
                "cli_runtime.skill_resolve_failed: capability `{}` must use exact ID `{expected_capability_id}`",
                attachment.capability_id,
            );
        }
        let Some(definition) = catalog
            .skills
            .iter()
            .find(|definition| explicit_ref_matches_skill(explicit_ref, definition))
        else {
            anyhow::bail!(
                "cli_runtime.skill_resolve_failed: capability `{}` references missing skill `{}`",
                attachment.capability_id,
                attachment.skill_id,
            );
        };
        if let Some(resolved) = resolution.active.iter().find(|resolved| {
            matches!(
                resolved.reason,
                pioneer_skills::SkillResolvedReason::ExplicitCapability
            ) && resolved.skill_id == definition.identity.skill_id
        }) {
            ordered.push(resolved.clone());
            continue;
        }
        let excluded_reason = resolution
            .excluded
            .iter()
            .find(|excluded| excluded.skill_id == definition.identity.skill_id)
            .map(|excluded| excluded.reason.as_db_value())
            .unwrap_or("not_matched");
        anyhow::bail!(
            "cli_runtime.skill_resolve_failed: capability `{}` skill `{}` was excluded at resolve stage: {excluded_reason}",
            attachment.capability_id,
            attachment.skill_id,
        );
    }
    Ok(ordered)
}

#[cfg(test)]
mod cli_runtime_resolver_tests {
    use super::*;
    use pioneer_skills::{
        SkillCatalogSnapshot, SkillDependencies, SkillPolicy, SkillPolicyKey, SkillPolicySet,
        SkillSourceKind, SkillTrustLevel,
        compile::{CompileSkillInput, compile_skill_definition},
        contract::default_skill_conformance,
    };

    fn test_skill_id(slug: &str, source_kind: SkillSourceKind) -> pioneer_protocol::SkillId {
        let suffix = match source_kind {
            SkillSourceKind::System => 'S',
            SkillSourceKind::User => 'U',
            SkillSourceKind::Registry => 'R',
        };
        let mut value = slug
            .chars()
            .filter(char::is_ascii_alphanumeric)
            .collect::<String>();
        value.truncate(20);
        while value.len() < 20 {
            value.push(suffix);
        }
        value.push(suffix);
        pioneer_protocol::SkillId::new(value).expect("valid CLI resolver test SkillId")
    }

    fn definition(slug: &str, source_kind: SkillSourceKind) -> pioneer_skills::SkillDefinition {
        compile_skill_definition(CompileSkillInput {
            skill_id: test_skill_id(slug, source_kind),
            owner: Some("workspace".to_owned()),
            slug: slug.to_owned(),
            name: slug.to_owned(),
            display_name: slug.to_owned(),
            description: "description".to_owned(),
            body: "body".to_owned(),
            source_kind,
            source_root: "/tmp".to_owned(),
            skill_dir: format!("/tmp/{slug}"),
            skill_file: format!("/tmp/{slug}/SKILL.md"),
            version_hint: None,
            fingerprint: format!("fingerprint-{slug}"),
            user_invocable: true,
            disable_model_invocation: false,
            paths: Vec::new(),
            allowed_tools: Vec::new(),
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Community,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: serde_json::json!({}),
            conformance: default_skill_conformance(),
        })
    }

    fn attachment(
        slug: &str,
        source_kind: SkillSourceKind,
    ) -> crate::cli_runtime::skills::CliRuntimeSkillAttachment {
        let skill_id = test_skill_id(slug, source_kind);
        crate::cli_runtime::skills::CliRuntimeSkillAttachment {
            capability_id: format!("skill:{skill_id}"),
            label: Some(slug.to_owned()),
            skill_id,
        }
    }

    fn resolve_for(
        catalog: &SkillCatalogSnapshot,
        attachments: &[crate::cli_runtime::skills::CliRuntimeSkillAttachment],
        policy_set: &SkillPolicySet,
        mcp_availability: &pioneer_agent::AgentMcpAvailability,
    ) -> anyhow::Result<Vec<pioneer_skills::ResolvedSkill>> {
        let refs = attachments
            .iter()
            .map(|attachment| SkillExplicitRef {
                skill_id: attachment.skill_id.clone(),
                label: attachment.label.clone(),
            })
            .collect::<Vec<_>>();
        let dependency_input = DependencyCheckInput {
            available_mcp: mcp_availability.available_mcp.clone(),
            blocked_mcp: mcp_availability.blocked_mcp.clone(),
            ..DependencyCheckInput::baseline()
        };
        let resolution = resolve_skills(SkillResolutionInput {
            explicit_refs: &refs,
            touched_paths: &[],
            catalog,
            policy_set,
            validation_policy: SkillValidationPolicy::default(),
            dependency_input: &dependency_input,
        });
        ordered_cli_runtime_explicit_skills(attachments, &refs, catalog, &resolution)
    }

    #[test]
    fn cli_runtime_skill_resolver_restores_user_and_registry_attachment_order() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![
                definition("alpha", SkillSourceKind::User),
                definition("zeta", SkillSourceKind::Registry),
            ],
        };
        let attachments = [
            attachment("zeta", SkillSourceKind::Registry),
            attachment("alpha", SkillSourceKind::User),
        ];
        let resolved = resolve_for(
            &catalog,
            &attachments,
            &SkillPolicySet::default(),
            &pioneer_agent::AgentMcpAvailability::default(),
        )
        .unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].definition.identity.slug, "zeta");
        assert_eq!(resolved[1].definition.identity.slug, "alpha");
    }

    #[test]
    fn cli_runtime_skill_resolver_selects_duplicate_labels_by_exact_id() {
        let first = definition("shared", SkillSourceKind::User);
        let second = definition("shared", SkillSourceKind::Registry);
        let first_id = first.identity.skill_id.clone();
        let second_id = second.identity.skill_id.clone();
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![first, second],
        };

        let resolved = resolve_for(
            &catalog,
            &[attachment("shared", SkillSourceKind::Registry)],
            &SkillPolicySet::default(),
            &pioneer_agent::AgentMcpAvailability::default(),
        )
        .expect("duplicate labels must resolve by the selected exact ID");

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].skill_id, second_id);
        assert_ne!(resolved[0].skill_id, first_id);
    }

    #[test]
    fn cli_runtime_skill_resolver_rejects_capability_id_mismatch() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![definition("alpha", SkillSourceKind::User)],
        };
        let mut mismatched = attachment("alpha", SkillSourceKind::User);
        mismatched.capability_id = format!(
            "skill:{}",
            test_skill_id("different", SkillSourceKind::Registry)
        );

        let error = resolve_for(
            &catalog,
            &[mismatched],
            &SkillPolicySet::default(),
            &pioneer_agent::AgentMcpAvailability::default(),
        )
        .expect_err("capability ID must agree with its exact SkillId")
        .to_string();

        assert!(error.contains("must use exact ID"));
    }

    #[test]
    fn cli_runtime_skill_resolver_accepts_explicit_user_controlled_system_browser() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![definition("browser", SkillSourceKind::System)],
        };
        let attachments = [attachment("browser", SkillSourceKind::System)];

        let resolved = resolve_for(
            &catalog,
            &attachments,
            &SkillPolicySet::default(),
            &pioneer_agent::AgentMcpAvailability::default(),
        )
        .expect("a user-controlled system skill must resolve as an explicit capability");

        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].definition.identity.slug, "browser");
        assert!(matches!(
            resolved[0].definition.identity.source_kind,
            SkillSourceKind::System
        ));
        assert!(matches!(
            resolved[0].reason,
            pioneer_skills::SkillResolvedReason::ExplicitCapability
        ));
    }

    #[test]
    fn cli_runtime_skill_resolver_rejects_mismatch_disabled_and_missing() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![definition("alpha", SkillSourceKind::User)],
        };
        for attachments in [
            vec![attachment("alpha", SkillSourceKind::Registry)],
            vec![attachment("missing", SkillSourceKind::User)],
        ] {
            assert!(
                resolve_for(
                    &catalog,
                    &attachments,
                    &SkillPolicySet::default(),
                    &pioneer_agent::AgentMcpAvailability::default(),
                )
                .unwrap_err()
                .to_string()
                .contains("cli_runtime.skill_resolve_failed")
            );
        }

        let mut policy_set = SkillPolicySet::default();
        let alpha_id = test_skill_id("alpha", SkillSourceKind::User);
        policy_set.workspace_by_key.insert(
            SkillPolicyKey::new(alpha_id),
            SkillPolicy {
                enabled: Some(false),
                allow_implicit_invocation: None,
            },
        );
        let error = resolve_for(
            &catalog,
            &[attachment("alpha", SkillSourceKind::User)],
            &policy_set,
            &pioneer_agent::AgentMcpAvailability::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("disabled_by_policy"));
    }

    #[test]
    fn cli_runtime_skill_dependency_uses_exact_projected_mcp_snapshot() {
        let mut dependent = definition("mcp-dependent", SkillSourceKind::User);
        dependent.dependencies = pioneer_skills::compile::SkillDependencySet {
            mcp: vec!["resend/send".to_owned()],
            ..pioneer_skills::compile::SkillDependencySet::default()
        };
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![dependent],
        };
        let attachments = [attachment("mcp-dependent", SkillSourceKind::User)];

        let projected = pioneer_agent::AgentMcpAvailability {
            available_mcp: vec!["resend/send".to_owned()],
            blocked_mcp: Vec::new(),
        };
        let resolved = resolve_for(
            &catalog,
            &attachments,
            &SkillPolicySet::default(),
            &projected,
        )
        .expect("an exact projected tool must satisfy the skill dependency");
        assert_eq!(resolved.len(), 1);

        let wider_workspace_but_unprojected = pioneer_agent::AgentMcpAvailability {
            available_mcp: vec!["resend/domains".to_owned()],
            blocked_mcp: Vec::new(),
        };
        let error = resolve_for(
            &catalog,
            &attachments,
            &SkillPolicySet::default(),
            &wider_workspace_but_unprojected,
        )
        .expect_err("a workspace tool outside the projection must not satisfy the dependency")
        .to_string();
        assert!(
            error.contains("dependency_missing"),
            "unexpected error: {error}"
        );
    }
}

impl MessageProcessor {
    pub(crate) async fn resolve_cli_runtime_skill_attachments(
        &self,
        workspace_id: &str,
        attachments: &[crate::cli_runtime::skills::CliRuntimeSkillAttachment],
        mcp_availability: &pioneer_agent::AgentMcpAvailability,
    ) -> anyhow::Result<Vec<pioneer_skills::ResolvedSkill>> {
        if attachments.is_empty() {
            return Ok(Vec::new());
        }
        let context = self.skills_runtime_context(workspace_id)?;
        let catalog = self.load_skills_catalog(workspace_id, &context).await?;
        let workspace_policies = self
            .crud_store
            .list_workspace_skill_policies(workspace_id)
            .await?;
        let policy_set = self.build_policy_set(
            catalog.skills.as_slice(),
            workspace_policies.as_slice(),
            &context,
        );
        let explicit_refs = attachments
            .iter()
            .map(|attachment| SkillExplicitRef {
                skill_id: attachment.skill_id.clone(),
                label: attachment.label.clone(),
            })
            .collect::<Vec<_>>();
        let dependency_input = DependencyCheckInput {
            available_mcp: mcp_availability.available_mcp.clone(),
            blocked_mcp: mcp_availability.blocked_mcp.clone(),
            ..DependencyCheckInput::baseline()
        };
        let resolution = resolve_skills(SkillResolutionInput {
            explicit_refs: explicit_refs.as_slice(),
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &policy_set,
            validation_policy: context.validation_policy,
            dependency_input: &dependency_input,
        });

        ordered_cli_runtime_explicit_skills(attachments, &explicit_refs, &catalog, &resolution)
    }

    pub(crate) async fn skills_list(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillListParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_LIST,
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

        let installations = match self.crud_store.list_skill_installations().await {
            Ok(rows) => rows,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to load skill installations",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        let installation_by_id = installations
            .into_iter()
            .filter(|row| row.scope_key.as_str() == workspace_id.as_str())
            .map(|row| (row.skill_id.clone(), row))
            .collect::<HashMap<_, _>>();

        let workspace_policies = match self
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

        let policy_set = self.build_policy_set(
            catalog.skills.as_slice(),
            workspace_policies.as_slice(),
            &context,
        );

        let explicit_inputs = catalog
            .skills
            .iter()
            .map(|skill| SkillExplicitRef {
                skill_id: skill.identity.skill_id.clone(),
                label: Some(skill.identity.display_name.clone()),
            })
            .collect::<Vec<_>>();

        let resolution = resolve_skills(SkillResolutionInput {
            explicit_refs: explicit_inputs.as_slice(),
            touched_paths: &[],
            catalog: &catalog,
            policy_set: &policy_set,
            validation_policy: context.validation_policy,
            dependency_input: &DependencyCheckInput::baseline(),
        });

        let active_by_id = resolution
            .active
            .iter()
            .map(|resolved| (resolved.skill_id.clone(), resolved))
            .collect::<HashMap<_, _>>();

        let excluded_by_id = resolution
            .excluded
            .iter()
            .map(|excluded| (excluded.skill_id.clone(), excluded))
            .collect::<HashMap<_, _>>();

        let mut response_items = catalog
            .skills
            .iter()
            .map(|skill| {
                let installation = installation_by_id.get(&skill.identity.skill_id);
                let is_system = matches!(&skill.identity.source_kind, SkillSourceKind::System);
                let effective_policy = effective_policy_for_skill(skill, &policy_set);
                let health = if params.include_health {
                    self.compute_skill_health_summary(skill, &context)
                } else {
                    SkillHealthSummary {
                        status: "ok".to_owned(),
                        dependency_failures: Vec::new(),
                        security_blocks: Vec::new(),
                        validation_issues: Vec::new(),
                    }
                };

                let status = if !skill.is_available() {
                    "unavailable".to_owned()
                } else if !effective_policy.enabled {
                    "disabled".to_owned()
                } else if active_by_id.contains_key(&skill.identity.skill_id) {
                    "active".to_owned()
                } else {
                    "blocked".to_owned()
                };
                let status_reason = excluded_by_id
                    .get(&skill.identity.skill_id)
                    .map(|excluded| excluded.reason.as_db_value().to_owned());

                SkillListItem {
                    skill_id: skill.identity.skill_id.clone(),
                    owner: skill.identity.owner.clone(),
                    slug: skill.identity.slug.clone(),
                    source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                    display_name: skill.identity.display_name.clone(),
                    description: skill.instructions.description.clone(),
                    version: skill.identity.version_hint.clone(),
                    fingerprint: skill.identity.fingerprint.clone(),
                    trust_level: trust_level_as_str(&skill.runtime.trust_level).to_owned(),
                    install: SkillInstallState {
                        managed: is_system
                            || matches!(skill.identity.source_kind, SkillSourceKind::Registry),
                        installed: is_system || installation.is_some(),
                        lifecycle_editable: !is_system,
                        install_path: if is_system {
                            Some(skill.identity.skill_dir.clone())
                        } else {
                            installation.map(|item| item.install_path.clone())
                        },
                        updated_at: if is_system {
                            None
                        } else {
                            installation.map(|item| item.updated_at_unix)
                        },
                    },
                    policy: SkillPolicyState {
                        enabled: effective_policy.enabled,
                        allow_implicit_invocation: effective_policy.allow_implicit_invocation,
                        allow_implicit_invocation_editable: skill_implicit_invocation_editable(
                            skill,
                        ),
                    },
                    health,
                    status,
                    status_reason,
                }
            })
            .collect::<Vec<_>>();

        response_items.sort_by(|left, right| {
            left.source_kind
                .cmp(&right.source_kind)
                .then_with(|| left.owner.cmp(&right.owner))
                .then_with(|| left.slug.cmp(&right.slug))
                .then_with(|| left.skill_id.cmp(&right.skill_id))
        });

        let response_payload = SkillListResponse {
            snapshot_version: self.current_skills_snapshot_version(),
            generated_at: now_timestamp_secs(),
            skills: response_items,
        };

        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        None,
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to encode skills/list response",
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
                "failed to send skills/list response"
            );
        }
    }

    pub(crate) async fn skills_health(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsHealthParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_HEALTH,
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

        let filter = params
            .skills
            .iter()
            .map(|target| target.skill_id.clone())
            .collect::<std::collections::HashSet<_>>();

        let include_all = filter.is_empty();

        let mut health_items = Vec::new();

        for skill in &catalog.skills {
            if !include_all && !filter.contains(&skill.identity.skill_id) {
                continue;
            }

            let dependency_result = if context.validation_policy.preflight_on_resolve {
                pioneer_skills::evaluate_skill_dependencies(
                    skill,
                    &DependencyCheckInput::baseline(),
                )
            } else {
                pioneer_skills::DependencyCheckResult::default()
            };
            let security_findings = if context.validation_policy.security_scan_on_resolve {
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
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };

            let recent_audit_rows = self
                .crud_store
                .list_skill_audit_event_records(&skill.identity.skill_id, params.audit_limit.max(1))
                .await
                .unwrap_or_default();

            health_items.push(SkillHealthItem {
                skill_id: skill.identity.skill_id.clone(),
                owner: skill.identity.owner.clone(),
                slug: skill.identity.slug.clone(),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
                trust_level: trust_level_as_str(&skill.runtime.trust_level).to_owned(),
                dependency_diagnostics: dependency_result
                    .diagnostics
                    .iter()
                    .map(to_protocol_dependency)
                    .collect(),
                security_findings: security_findings
                    .iter()
                    .map(to_protocol_security_finding)
                    .collect(),
                validation_issues: blocking_validation_issues(skill, &context.validation_policy)
                    .iter()
                    .map(to_protocol_validation_issue)
                    .collect(),
                trust_gate: self.build_trust_gate(skill),
                recent_audit: recent_audit_rows
                    .iter()
                    .map(|row| SkillAuditTimelineItem {
                        action: row.action.clone(),
                        decision: row.decision.clone(),
                        reason_code: row.reason_code.clone(),
                        created_at: row.created_at_unix,
                        details_json: row.details_json.clone(),
                    })
                    .collect(),
            });
        }

        health_items.sort_by(|left, right| {
            left.source_kind
                .cmp(&right.source_kind)
                .then_with(|| left.owner.cmp(&right.owner))
                .then_with(|| left.slug.cmp(&right.slug))
                .then_with(|| left.skill_id.cmp(&right.skill_id))
        });

        let payload = SkillsHealthResponse {
            snapshot_version: self.current_skills_snapshot_version(),
            generated_at: now_timestamp_secs(),
            skills: health_items,
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
                        "failed to encode skills/health response",
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
                "failed to send skills/health response"
            );
        }
    }
}
