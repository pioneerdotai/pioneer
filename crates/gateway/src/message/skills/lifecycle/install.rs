use super::*;

fn rollback_committed_install(
    install_result: &pioneer_skills::InstallSkillResult,
    location: &SkillInstallLocation,
) {
    if let Err(error) =
        pioneer_skills::rollback_prepared_skill_commit(install_result, location.lock_path.as_path())
    {
        warn!(
            skill_id = %install_result.definition.identity.skill_id,
            error = %format!("{error:#}"),
            "failed to roll back published skill after database error"
        );
    }
}

fn generate_install_skill_id(upload_id: &str) -> Result<SkillId> {
    loop {
        let candidate = SkillId::new(pioneer_protocol::generate_id(
            pioneer_protocol::SKILL_ID_LEN,
        ))
        .map_err(|error| anyhow::anyhow!("generated an invalid skill identity: {error}"))?;
        if candidate.as_str() != upload_id {
            return Ok(candidate);
        }
    }
}

impl MessageProcessor {
    async fn allocate_install_skill_id(
        &self,
        upload_id: &str,
        context: &SkillsRuntimeContext,
    ) -> Result<SkillId> {
        loop {
            let candidate = generate_install_skill_id(upload_id)?;
            if context
                .catalog_params
                .bundled
                .iter()
                .any(|entry| entry.skill_id == candidate)
            {
                continue;
            }
            if self
                .crud_store
                .find_skill_installation(&candidate)
                .await?
                .is_none()
            {
                return Ok(candidate);
            }
        }
    }

    pub(crate) async fn skills_install(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsInstallParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_INSTALL,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };
        let target_source_kind =
            match parse_installable_source_kind(params.target_source_kind.as_str()) {
                Some(kind) => kind,
                None => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            SKILLS_ERROR_SOURCE_NOT_SUPPORTED,
                            "install target_source_kind must be `user` or `registry`",
                            json!({"target_source_kind": params.target_source_kind}),
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
                        Some(request_id),
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
        let upload_id = match parse_lifecycle_upload_id(params.source) {
            Ok(upload_id) => upload_id,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        SKILLS_ERROR_INVALID_REQUEST,
                        "invalid lifecycle source",
                        json!({"error": error}),
                    ),
                )
                .await;
                return;
            }
        };
        let materialized = match self
            .materialize_uploaded_skill_source(
                connection_id,
                workspace_id.as_str(),
                upload_id.as_str(),
                &context,
                &request_id,
            )
            .await
        {
            Ok(materialized) => materialized,
            Err(error) => {
                self.send_error(connection_id, error).await;
                return;
            }
        };

        let skill_id = match self
            .allocate_install_skill_id(materialized.upload.upload_id.as_str(), &context)
            .await
        {
            Ok(skill_id) => skill_id,
            Err(error) => {
                let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to allocate skill identity",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        let source_ref = format!("upload:{}", materialized.upload.upload_id);
        let prepared = match pioneer_skills::prepare_materialized_skill(
            pioneer_skills::PrepareMaterializedSkillRequest {
                skill_id: skill_id.clone(),
                source_kind: target_source_kind,
                source_ref: source_ref.clone(),
                materialized_source_path: materialized.source_dir.clone(),
                policy: installer_policy(&context),
            },
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let mapped = map_lifecycle_error(&error, methods::SKILLS_INSTALL);
                let (message, details) =
                    lifecycle_error_payload(&error, &mapped, None, &context.validation_policy);
                let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        mapped.jsonrpc_code,
                        mapped.code,
                        message,
                        details,
                    ),
                )
                .await;
                return;
            }
        };
        let Some(location) = install_location_for_source_kind(&context, &target_source_kind) else {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "validated install source has no managed location",
                    json!({"target_source_kind": target_source_kind.as_db_value()}),
                ),
            )
            .await;
            return;
        };
        let now = now_timestamp_secs();
        let _guard = self.acquire_skills_write_lock().await;
        if let Err(error) = self
            .ensure_skills_lock_v2_locked(
                location.lock_path.as_path(),
                target_source_kind,
                workspace_id.as_str(),
            )
            .await
        {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to convert skills lock",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }
        let identity_collision = context
            .catalog_params
            .bundled
            .iter()
            .any(|entry| entry.skill_id == skill_id)
            || match self.crud_store.find_skill_installation(&skill_id).await {
                Ok(existing) => existing.is_some(),
                Err(error) => {
                    let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                    self.send_error(
                        connection_id,
                        skills_error(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to verify allocated skill identity",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await;
                    return;
                }
            };
        if identity_collision {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "allocated skill identity became unavailable; retry installation",
                    json!({"skill_id": skill_id}),
                ),
            )
            .await;
            return;
        }
        let install_result = match pioneer_skills::commit_prepared_skill(
            pioneer_skills::CommitPreparedSkillRequest {
                operation: pioneer_skills::InstallOperation::Install,
                prepared,
                install_root: location.install_root.clone(),
                lock_path: location.lock_path.clone(),
                previous: None,
                expected_previous_fingerprint: None,
                now_unix: now,
                policy: installer_policy(&context),
            },
        ) {
            Ok(result) => result,
            Err(error) => {
                let mapped = map_lifecycle_error(&error, methods::SKILLS_INSTALL);
                let (message, details) =
                    lifecycle_error_payload(&error, &mapped, None, &context.validation_policy);
                let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        mapped.jsonrpc_code,
                        mapped.code,
                        message,
                        details,
                    ),
                )
                .await;
                return;
            }
        };
        let install_path = install_result.install_path.display().to_string();
        let installation_record = SkillInstallationRecord {
            skill_id: skill_id.clone(),
            owner: install_result.definition.identity.owner.clone(),
            slug: install_result.definition.identity.slug.clone(),
            version: install_result.definition.identity.version_hint.clone(),
            source_kind: target_source_kind.as_db_value().to_owned(),
            scope_key: workspace_id.clone(),
            source_ref,
            install_path: install_path.clone(),
            trust_level: trust_level_as_str(&install_result.definition.runtime.trust_level)
                .to_owned(),
            fingerprint: install_result.definition.identity.fingerprint.clone(),
            updated_at_unix: now,
        };
        let policy_record = WorkspaceSkillPolicyRecord {
            id: pioneer_protocol::generate_id(21),
            workspace_id: workspace_id.clone(),
            skill_id: skill_id.clone(),
            enabled: Some(true),
            allow_implicit_invocation: Some(false),
        };
        if let Err(error) = self
            .crud_store
            .insert_skill_installation_with_policy(&installation_record, &policy_record, now)
            .await
        {
            rollback_committed_install(&install_result, &location);
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to persist skill installation and default policy",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }

        pioneer_skills::finalize_prepared_skill_commit(&install_result);

        let audit_records = skill_audit_records(install_result.audit_events.as_slice());
        if let Err(error) = self
            .crud_store
            .insert_skill_audit_event_records(audit_records.as_slice())
            .await
        {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to persist skill audit events",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }
        if let Err(error) = self
            .mark_upload_consumed(materialized.upload.upload_id.as_str(), now)
            .await
        {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to mark skill upload consumed",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }
        self.cleanup_upload_artifacts(&materialized.upload, materialized.cleanup_root.as_path());

        let payload = SkillsInstallResponse {
            status: "installed".to_owned(),
            skill: SkillLifecycleResultSkill {
                skill_id: skill_id.clone(),
                owner: installation_record.owner.clone(),
                slug: installation_record.slug.clone(),
                source_kind: installation_record.source_kind.clone(),
                version: installation_record.version.clone(),
                fingerprint: installation_record.fingerprint.clone(),
                trust_level: installation_record.trust_level.clone(),
                install_path,
            },
            audit: SkillLifecycleAuditSummary {
                events_written: audit_records.len(),
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
                        "failed to encode skills/install response",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(connection_id, error = %format!("{error:#}"), "failed to send skills/install response");
            return;
        }
        self.notify_skills_changed(
            workspace_id.as_str(),
            "installed",
            vec![SkillChangedItem {
                skill_id,
                owner: installation_record.owner,
                slug: installation_record.slug,
                source_kind: installation_record.source_kind,
                change_type: "install".to_owned(),
                fingerprint_before: None,
                fingerprint_after: Some(installation_record.fingerprint),
            }],
            now,
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::generate_install_skill_id;

    #[test]
    fn installation_id_is_separate_from_upload_id() {
        let upload_id = "AAAAAAAAAAAAAAAAAAAAA";
        let skill_id = generate_install_skill_id(upload_id).expect("allocate skill identity");
        assert_ne!(skill_id.as_str(), upload_id);
        assert_eq!(skill_id.as_str().len(), pioneer_protocol::SKILL_ID_LEN);
    }
}
