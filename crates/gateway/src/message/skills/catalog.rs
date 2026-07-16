use super::*;

fn ordered_cli_runtime_explicit_skills(
    attachments: &[crate::cli_runtime::skills::CliRuntimeSkillAttachment],
    explicit_refs: &[SkillExplicitRef],
    catalog: &pioneer_skills::SkillCatalogSnapshot,
    resolution: &pioneer_skills::SkillResolutionResult,
) -> anyhow::Result<Vec<pioneer_skills::ResolvedSkill>> {
    let mut ordered = Vec::with_capacity(attachments.len());
    for (attachment, explicit_ref) in attachments.iter().zip(explicit_refs) {
        let matching_catalog = catalog
            .skills
            .iter()
            .filter(|definition| explicit_ref_matches_skill(explicit_ref, definition))
            .collect::<Vec<_>>();
        if matching_catalog.len() != 1 {
            anyhow::bail!(
                "cli_runtime.skill_resolve_failed: capability `{}` skill `{}` ({}) matched {} authoritative catalog entries",
                attachment.capability_id,
                attachment.slug,
                attachment.claimed_source_kind,
                matching_catalog.len()
            );
        }
        let definition = matching_catalog[0];
        let qualified_slug = qualified_skill_slug(
            definition.identity.owner.as_str(),
            definition.identity.slug.as_str(),
        );
        if let Some(resolved) = resolution.active.iter().find(|resolved| {
            matches!(
                resolved.reason,
                pioneer_skills::SkillResolvedReason::ExplicitCapability
            ) && resolved.slug == qualified_slug
                && resolved.definition.identity.source_kind == definition.identity.source_kind
        }) {
            ordered.push(resolved.clone());
            continue;
        }
        let excluded_reason = resolution
            .excluded
            .iter()
            .find(|excluded| {
                excluded.slug == qualified_slug
                    && excluded.source_kind == definition.identity.source_kind.as_db_value()
            })
            .map(|excluded| excluded.reason.as_db_value())
            .unwrap_or("not_matched");
        anyhow::bail!(
            "cli_runtime.skill_resolve_failed: capability `{}` skill `{}` ({}) was excluded at resolve stage: {excluded_reason}",
            attachment.capability_id,
            attachment.slug,
            attachment.claimed_source_kind
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

    fn definition(slug: &str, source_kind: SkillSourceKind) -> pioneer_skills::SkillDefinition {
        compile_skill_definition(CompileSkillInput {
            owner: "workspace".to_owned(),
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
        id: &str,
        slug: &str,
        source_kind: &str,
    ) -> crate::cli_runtime::skills::CliRuntimeSkillAttachment {
        crate::cli_runtime::skills::CliRuntimeSkillAttachment {
            capability_id: id.to_owned(),
            label: Some(slug.to_owned()),
            slug: slug.to_owned(),
            claimed_source_kind: source_kind.to_owned(),
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
                capability_id: attachment.capability_id.clone(),
                label: attachment.label.clone(),
                slug: attachment.slug.clone(),
                source_kind: attachment.claimed_source_kind.clone(),
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
            attachment("z", "zeta", "registry"),
            attachment("a", "alpha", "user"),
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
    fn cli_runtime_skill_resolver_rejects_mismatch_disabled_and_missing() {
        let catalog = SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: 1,
            skills: vec![definition("alpha", SkillSourceKind::User)],
        };
        for attachments in [
            vec![attachment("mismatch", "alpha", "registry")],
            vec![attachment("missing", "missing", "user")],
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
        policy_set.workspace_by_key.insert(
            SkillPolicyKey::new("workspace/alpha", "user"),
            SkillPolicy {
                enabled: Some(false),
                allow_implicit_invocation: None,
            },
        );
        let error = resolve_for(
            &catalog,
            &[attachment("disabled", "alpha", "user")],
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
        let attachments = [attachment("dependent", "mcp-dependent", "user")];

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
        let catalog = load_catalog(&context.catalog_params)?;
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
                capability_id: attachment.capability_id.clone(),
                label: attachment.label.clone(),
                slug: attachment.slug.clone(),
                source_kind: attachment.claimed_source_kind.clone(),
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

        let installation_by_key = installations
            .into_iter()
            .filter(|row| row.scope_key.as_str() == workspace_id.as_str())
            .map(|row| (skill_key(row.slug.as_str(), row.source_kind.as_str()), row))
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
                capability_id: format!(
                    "skills/list:{}:{}",
                    skill.identity.source_kind.as_db_value(),
                    qualified_skill_slug(
                        skill.identity.owner.as_str(),
                        skill.identity.slug.as_str()
                    )
                ),
                label: Some(skill.identity.display_name.clone()),
                slug: qualified_skill_slug(
                    skill.identity.owner.as_str(),
                    skill.identity.slug.as_str(),
                ),
                source_kind: skill.identity.source_kind.as_db_value().to_owned(),
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

        let active_by_key = resolution
            .active
            .iter()
            .map(|resolved| {
                (
                    skill_key(
                        resolved.slug.as_str(),
                        resolved.definition.identity.source_kind.as_db_value(),
                    ),
                    resolved,
                )
            })
            .collect::<HashMap<_, _>>();

        let excluded_by_key = resolution
            .excluded
            .iter()
            .map(|excluded| {
                (
                    skill_key(excluded.slug.as_str(), excluded.source_kind.as_str()),
                    excluded,
                )
            })
            .collect::<HashMap<_, _>>();

        let mut response_items = catalog
            .skills
            .iter()
            .map(|skill| {
                let qualified_slug = qualified_skill_slug(
                    skill.identity.owner.as_str(),
                    skill.identity.slug.as_str(),
                );
                let key = skill_key(
                    qualified_slug.as_str(),
                    skill.identity.source_kind.as_db_value(),
                );
                let installation = installation_by_key.get(&key);
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

                let status = if !effective_policy.enabled {
                    "disabled".to_owned()
                } else if active_by_key.contains_key(&key) {
                    "active".to_owned()
                } else {
                    "blocked".to_owned()
                };
                let status_reason = excluded_by_key
                    .get(&key)
                    .map(|excluded| excluded.reason.as_db_value().to_owned());

                SkillListItem {
                    slug: qualified_slug,
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
                .then_with(|| left.slug.cmp(&right.slug))
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

        if let Some(invalid) = params
            .skills
            .iter()
            .find(|target| !is_qualified_slug(target.slug.as_str()))
        {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "skills/health targets must use owner/slug",
                    json!({
                        "slug": invalid.slug,
                        "source_kind": invalid.source_kind,
                    }),
                ),
            )
            .await;
            return;
        }

        let filter = params
            .skills
            .iter()
            .map(|target| skill_key(target.slug.as_str(), target.source_kind.as_str()))
            .collect::<std::collections::HashSet<_>>();

        let include_all = filter.is_empty();

        let mut health_items = Vec::new();

        for skill in &catalog.skills {
            let qualified_slug =
                qualified_skill_slug(skill.identity.owner.as_str(), skill.identity.slug.as_str());
            let key = skill_key(
                qualified_slug.as_str(),
                skill.identity.source_kind.as_db_value(),
            );
            if !include_all && !filter.contains(&key) {
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
                .list_skill_audit_event_records_for_source(
                    qualified_slug.as_str(),
                    skill.identity.source_kind.as_db_value(),
                    params.audit_limit.max(1),
                )
                .await
                .unwrap_or_default();

            health_items.push(SkillHealthItem {
                slug: qualified_slug,
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
                .then_with(|| left.slug.cmp(&right.slug))
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
