use super::*;

impl MessageProcessor {
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
                let effective_policy = merge_policy(
                    qualified_slug.as_str(),
                    skill.identity.source_kind.as_db_value(),
                    &policy_set,
                );
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
                        managed: matches!(skill.identity.source_kind, SkillSourceKind::Registry),
                        installed: installation.is_some(),
                        install_path: installation.map(|item| item.install_path.clone()),
                        updated_at: installation.map(|item| item.updated_at_unix),
                    },
                    policy: SkillPolicyState {
                        enabled: effective_policy.enabled,
                        allow_implicit_invocation: effective_policy.allow_implicit_invocation,
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
