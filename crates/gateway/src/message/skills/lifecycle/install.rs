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
    pub(super) async fn allocate_install_skill_id(
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsInstallParams,
    ) {
        let connection_id = request_context.connection_id();
        let result = self
            .install_skill_source_deferred(
                request_context,
                request_id.clone(),
                params.workspace_id,
                params.target_source_kind,
                SkillInstallSource::UploadedSkill(params.source),
            )
            .await;
        match result {
            Ok((payload, publication)) => {
                match JsonRpcResponse::from_result(request_id, &payload) {
                    Ok(response) => {
                        if let Err(error) = self.send_json(connection_id, &response).await {
                            warn!(connection_id, error = %error, "failed to send skills/install response");
                        }
                    }
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
                        .await
                    }
                }
                self.publish_skill_change(publication).await;
            }
            Err(error) => self.send_error(connection_id, error).await,
        }
    }

    pub(crate) async fn install_skill_source(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        workspace: String,
        target_kind: String,
        source: SkillInstallSource,
    ) -> std::result::Result<SkillsInstallResponse, JsonRpcErrorResponse> {
        let (response, publication) = self
            .install_skill_source_deferred(
                request_context,
                request_id,
                workspace,
                target_kind,
                source,
            )
            .await?;
        self.publish_skill_change(publication).await;
        Ok(response)
    }

    async fn install_skill_source_deferred(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        workspace: String,
        target_kind: String,
        source: SkillInstallSource,
    ) -> std::result::Result<(SkillsInstallResponse, SkillChangePublication), JsonRpcErrorResponse>
    {
        let connection_id = request_context.connection_id();
        let authenticated_owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                workspace,
                methods::SKILLS_INSTALL,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                return Err(error);
            }
        };
        let target_source_kind = match parse_installable_source_kind(target_kind.as_str()) {
            Some(kind) => kind,
            None => {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_SOURCE_NOT_SUPPORTED,
                    "install target_source_kind must be `user` or `registry`",
                    json!({"target_source_kind": target_kind}),
                ));
            }
        };
        let context = match self.skills_runtime_context(workspace_id.as_str()) {
            Ok(context) => context,
            Err(error) => {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to resolve skills runtime context",
                    json!({"error": format!("{error:#}")}),
                ));
            }
        };
        let materialized = self
            .materialize_install_source(
                request_context,
                &workspace_id,
                source,
                &context,
                target_source_kind,
                &request_id,
            )
            .await?;

        let skill_id = match materialized.ownership() {
            Some(owner) => SkillId::new(owner.child_id.clone()).map_err(|_| {
                skills_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "invalid reserved skill identity",
                    json!({}),
                )
            })?,
            None => self
                .allocate_install_skill_id(
                    materialized.upload_id().expect("uploaded source"),
                    &context,
                )
                .await
                .map_err(|error| {
                    materialized.cleanup_failure();
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to allocate skill identity",
                        json!({"error": format!("{error:#}")}),
                    )
                })?,
        };
        let source_ref = materialized.source_ref();
        let prepared = match pioneer_skills::prepare_materialized_skill(
            pioneer_skills::PrepareMaterializedSkillRequest {
                skill_id: skill_id.clone(),
                source_kind: target_source_kind,
                source_ref: source_ref.clone(),
                materialized_source_path: materialized.source_dir().to_path_buf(),
                policy: installer_policy(&context),
            },
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let mapped = map_lifecycle_error(&error, methods::SKILLS_INSTALL);
                let (message, details) =
                    lifecycle_error_payload(&error, &mapped, None, &context.validation_policy);
                materialized.cleanup_failure();
                return Err(skills_error(
                    Some(request_id),
                    mapped.jsonrpc_code,
                    mapped.code,
                    message,
                    details,
                ));
            }
        };
        let Some(location) = install_location_for_source_kind(&context, &target_source_kind) else {
            materialized.cleanup_failure();
            return Err(skills_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_INTERNAL,
                "validated install source has no managed location",
                json!({"target_source_kind": target_source_kind.as_db_value()}),
            ));
        };
        let now = now_timestamp_secs();
        if materialized.ownership().is_some()
            && !prepared.definition.conformance.agentskills_strict.compliant
        {
            return Err(skills_error(
                Some(request_id),
                INVALID_PARAMS_CODE,
                SKILLS_ERROR_INVALID_REQUEST,
                "package skill does not conform to Agent Skills",
                json!({}),
            ));
        }
        let upload_guard = match materialized.upload_id() {
            Some(id) => Some(self.acquire_skill_upload_lock(id).await),
            None => None,
        };
        let write_guard = self.acquire_skills_write_lock().await;
        if let Some(id) = materialized.upload_id() {
            if let Err(error) = self
                .revalidate_finalized_upload_locked(
                    &authenticated_owner,
                    &workspace_id,
                    id,
                    &request_id,
                )
                .await
            {
                materialized.cleanup_failure();
                drop(write_guard);
                drop(upload_guard);
                return Err(error);
            }
        }
        if let Err(error) = self
            .ensure_skills_lock_v2_locked(
                location.lock_path.as_path(),
                target_source_kind,
                workspace_id.as_str(),
            )
            .await
        {
            materialized.cleanup_failure();
            return Err(skills_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_INTERNAL,
                "failed to convert skills lock",
                json!({"error": format!("{error:#}")}),
            ));
        }
        let identity_collision = context
            .catalog_params
            .bundled
            .iter()
            .any(|entry| entry.skill_id == skill_id)
            || match self.crud_store.find_skill_installation(&skill_id).await {
                Ok(existing) => existing.is_some(),
                Err(error) => {
                    materialized.cleanup_failure();
                    return Err(skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to verify allocated skill identity",
                        json!({"error": format!("{error:#}")}),
                    ));
                }
            };
        if identity_collision {
            materialized.cleanup_failure();
            return Err(skills_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_INTERNAL,
                "allocated skill identity became unavailable; retry installation",
                json!({"skill_id": skill_id}),
            ));
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
                materialized.cleanup_failure();
                return Err(skills_error(
                    Some(request_id),
                    mapped.jsonrpc_code,
                    mapped.code,
                    message,
                    details,
                ));
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
            pack_id: None,
            pack_member_key: None,
        };
        let policy_record = WorkspaceSkillPolicyRecord {
            id: pioneer_protocol::generate_id(21),
            workspace_id: workspace_id.clone(),
            skill_id: skill_id.clone(),
            enabled: Some(true),
            allow_implicit_invocation: Some(false),
        };
        let audit_records = skill_audit_records(install_result.audit_events.as_slice());
        let persisted = self
            .crud_store
            .install_skill_lifecycle_with_ownership(
                &installation_record,
                &policy_record,
                audit_records.as_slice(),
                materialized.upload_id(),
                materialized.ownership(),
                now,
            )
            .await;
        if !matches!(persisted, Ok(true)) {
            rollback_committed_install(&install_result, &location);
            materialized.cleanup_failure();
            let error = match persisted {
                Ok(false) => anyhow::anyhow!(
                    "upload `{}` changed state before skill publication",
                    materialized.source_ref()
                ),
                Err(error) => error,
                Ok(true) => unreachable!(),
            };
            return Err(skills_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_INTERNAL,
                "failed to persist skill installation and consume upload",
                json!({"error": format!("{error:#}")}),
            ));
        }

        pioneer_skills::finalize_prepared_skill_commit(&install_result);
        if let Some(upload) = materialized.uploaded() {
            self.cleanup_upload_artifacts(&upload.upload, upload.cleanup_root.as_path());
        }
        drop(write_guard);
        drop(upload_guard);

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
        Ok((
            payload,
            SkillChangePublication {
                workspace_id,
                reason: "installed",
                changes: vec![SkillChangedItem {
                    skill_id,
                    owner: installation_record.owner,
                    slug: installation_record.slug,
                    source_kind: installation_record.source_kind,
                    change_type: "install".to_owned(),
                    fingerprint_before: None,
                    fingerprint_after: Some(installation_record.fingerprint),
                }],
                pack_changes: Vec::new(),
                created_at: now,
            },
        ))
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
