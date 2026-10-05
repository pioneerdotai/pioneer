use super::*;

impl MessageProcessor {
    pub(crate) async fn skills_update(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsUpdateParams,
    ) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
        let native = match self
            .begin_native_plugin_change(
                request_context,
                &request_id,
                &params.workspace_id,
                "skill",
                params.skill_id.as_str(),
                "update",
                &["skill_source"],
                deadline,
            )
            .await
        {
            Ok(native) => native,
            Err(error) => {
                self.send_error(
                    request_context.connection_id(),
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INVALID_REQUEST,
                        super::super::super::plugins::native_plugin_error_code(&error),
                        json!({}),
                    ),
                )
                .await;
                return;
            }
        };
        let source = match &native {
            Some(change) => {
                SkillInstallSource::OwnedUpload(params.source.clone(), change.write.clone())
            }
            None => SkillInstallSource::UploadedSkill(params.source.clone()),
        };
        let owned = native.is_some();
        let work = self.update_skill_source(
            request_context,
            request_id.clone(),
            SkillUpdateInput {
                workspace_id: params.workspace_id,
                skill_id: params.skill_id,
                expected_previous_fingerprint: params.expected_previous_fingerprint,
            },
            source,
        );
        let result = if owned {
            tokio::time::timeout_at(deadline, work)
                .await
                .unwrap_or_else(|_| {
                    Err(skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "plugin change deadline exceeded",
                        json!({}),
                    ))
                })
        } else {
            work.await
        };
        let result = match (result, native) {
            (Ok(payload), Some(change)) => self
                .finish_native_plugin_change(change)
                .await
                .map(|_| payload)
                .map_err(|error| {
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        super::super::super::plugins::native_plugin_error_code(&error),
                        json!({}),
                    )
                }),
            (Err(_), Some(change)) => {
                let _ = self
                    .interrupt_native_plugin_change(&change, "plugins.component_update_failed")
                    .await;
                Err(skills_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "plugin component update requires repair",
                    json!({}),
                ))
            }
            (result, None) => result,
        };
        match result {
            Ok(payload) => match JsonRpcResponse::from_result(request_id, &payload) {
                Ok(response) => {
                    if let Err(error) = self
                        .send_json(request_context.connection_id(), &response)
                        .await
                    {
                        warn!(error = %error, "failed to send skills/update response");
                    }
                }
                Err(error) => {
                    self.send_error(
                        request_context.connection_id(),
                        skills_error(
                            None,
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to encode skills/update response",
                            json!({"error": format!("{error:#}")}),
                        ),
                    )
                    .await
                }
            },
            Err(error) => {
                self.send_error(request_context.connection_id(), error)
                    .await
            }
        }
    }

    pub(crate) async fn update_skill_source(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillUpdateInput,
        source: SkillInstallSource,
    ) -> std::result::Result<SkillsUpdateResponse, JsonRpcErrorResponse> {
        let connection_id = request_context.connection_id();
        let authenticated_owner = AuthenticatedTransferOwner::from_request_context(request_context);
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_UPDATE,
            )
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                return Err(error);
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
        let existing = match self
            .crud_store
            .find_skill_installation(&params.skill_id)
            .await
        {
            Ok(Some(existing))
                if existing.scope_key == workspace_id
                    && matches!(existing.source_kind.as_str(), "user" | "registry") =>
            {
                existing
            }
            Ok(_) => {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_NOT_FOUND,
                    "skill installation was not found",
                    json!({"skill_id": params.skill_id}),
                ));
            }
            Err(error) => {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to read existing installation",
                    json!({"error": format!("{error:#}")}),
                ));
            }
        };
        let (source_kind, location) = match install_location_for_stored_source_kind(
            &context,
            existing.source_kind.as_str(),
        ) {
            Ok(value) => value,
            Err(error) => {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "stored skill installation has an invalid lifecycle source",
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
                source_kind,
                &request_id,
            )
            .await?;
        let source_ref = materialized.source_ref();
        let tree_changed = if let Some(owner) = materialized.ownership() {
            if owner.child_id != params.skill_id.as_str() {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "package update identity mismatch",
                    json!({}),
                ));
            }
            let link = self
                .crud_store
                .find_skill_plugin_owner(&params.skill_id)
                .await
                .map_err(|_| {
                    skills_error(
                        Some(request_id.clone()),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to read skill ownership",
                        json!({}),
                    )
                })?;
            let Some(link) = link.filter(|link| {
                link.plugin_id == owner.plugin_id && link.member_key == owner.member_key
            }) else {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "package update ownership mismatch",
                    json!({}),
                ));
            };
            link.status != "installed"
                || link.package_fingerprint.as_deref() != Some(owner.package_fingerprint.as_str())
        } else {
            materialized.native_write().is_some()
        };
        let prepared = match pioneer_skills::prepare_materialized_skill(
            pioneer_skills::PrepareMaterializedSkillRequest {
                skill_id: params.skill_id.clone(),
                source_kind,
                source_ref: source_ref.clone(),
                materialized_source_path: materialized.source_dir().to_path_buf(),
                policy: installer_policy(&context),
            },
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let mapped = map_lifecycle_error(&error, methods::SKILLS_UPDATE);
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
                source_kind,
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
        let row_unchanged = self
            .crud_store
            .find_skill_installation(&params.skill_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|current| current == existing);
        if !row_unchanged {
            materialized.cleanup_failure();
            return Err(skills_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_UPDATE_CONFLICT_FINGERPRINT,
                "skill installation changed while update was prepared",
                json!({"skill_id": params.skill_id}),
            ));
        }
        let previous_managed_install_path =
            pioneer_owned_install_path(&location, existing.install_path.as_str());
        if let Some(expected) = params.expected_previous_fingerprint.as_deref()
            && expected != existing.fingerprint
        {
            let error = anyhow::anyhow!(
                "update blocked: expected previous fingerprint `{expected}`, found `{}`",
                existing.fingerprint
            );
            let mapped = map_lifecycle_error(&error, methods::SKILLS_UPDATE);
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
        let now = now_timestamp_secs();
        if !tree_changed
            && prepared.definition.identity.fingerprint == existing.fingerprint
            && stored_skill_revision_is_available(
                &existing,
                source_kind,
                previous_managed_install_path.as_deref(),
                context.security_policy.max_install_file_bytes,
            )
        {
            if let Some(id) = materialized.upload_id() {
                if let Err(error) = self.mark_upload_consumed(id, now).await {
                    materialized.cleanup_failure();
                    return Err(skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to mark skill upload consumed",
                        json!({"error": format!("{error:#}")}),
                    ));
                }
            }
            if let Some(upload) = materialized.uploaded() {
                self.cleanup_upload_artifacts(&upload.upload, &upload.cleanup_root);
            }
            let payload = SkillsUpdateResponse {
                status: "already_up_to_date".to_owned(),
                skill: SkillLifecycleResultSkill {
                    skill_id: existing.skill_id,
                    owner: existing.owner,
                    slug: existing.slug,
                    source_kind: existing.source_kind,
                    version: existing.version,
                    fingerprint: existing.fingerprint,
                    trust_level: existing.trust_level,
                    install_path: existing.install_path,
                },
                audit: SkillLifecycleAuditSummary { events_written: 0 },
            };
            return Ok(payload);
        }

        let update_result = match pioneer_skills::commit_prepared_skill(
            pioneer_skills::CommitPreparedSkillRequest {
                operation: pioneer_skills::InstallOperation::Update,
                prepared,
                install_root: location.install_root.clone(),
                lock_path: location.lock_path.clone(),
                previous: Some(pioneer_skills::PreviousSkillInstallation {
                    managed_install_path: previous_managed_install_path,
                    fingerprint: existing.fingerprint.clone(),
                }),
                expected_previous_fingerprint: params.expected_previous_fingerprint,
                now_unix: now,
                policy: installer_policy(&context),
            },
        ) {
            Ok(result) => result,
            Err(error) => {
                let mapped = map_lifecycle_error(&error, methods::SKILLS_UPDATE);
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
        let install_path = update_result.install_path.display().to_string();
        // The native row is authoritative for trust. A same-key package update
        // replaces bundled files without relaxing or resetting this restriction.
        let updated_trust = if materialized.ownership().is_some() {
            existing.trust_level.clone()
        } else {
            trust_level_as_str(&update_result.definition.runtime.trust_level).to_owned()
        };
        let patch = SkillInstallationPatch {
            owner: Some(update_result.definition.identity.owner.clone()),
            slug: Some(update_result.definition.identity.slug.clone()),
            version: Some(update_result.definition.identity.version_hint.clone()),
            source_ref: Some(source_ref),
            install_path: Some(install_path.clone()),
            trust_level: Some(updated_trust.clone()),
            fingerprint: Some(update_result.definition.identity.fingerprint.clone()),
            ..SkillInstallationPatch::default()
        };
        let audit_records = skill_audit_records(update_result.audit_events.as_slice());
        let persisted = self
            .crud_store
            .update_skill_lifecycle_with_plugin_change(
                &params.skill_id,
                &patch,
                audit_records.as_slice(),
                materialized.upload_id(),
                materialized.ownership(),
                materialized.native_write(),
                now,
            )
            .await;
        if !matches!(persisted, Ok(true)) {
            if let Err(error) = pioneer_skills::rollback_prepared_skill_commit(
                &update_result,
                location.lock_path.as_path(),
            ) {
                warn!(
                    skill_id = %params.skill_id,
                    error = %format!("{error:#}"),
                    "failed to roll back updated skill after lifecycle transaction error"
                );
            }
            materialized.cleanup_failure();
            let error = match persisted {
                Ok(false) => anyhow::anyhow!(
                    "upload `{}` changed state before skill update publication",
                    materialized.source_ref()
                ),
                Err(error) => error,
                Ok(true) => unreachable!(),
            };
            return Err(skills_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_INTERNAL,
                "failed to persist updated skill and consume upload",
                json!({"error": format!("{error:#}")}),
            ));
        }
        pioneer_skills::finalize_prepared_skill_commit(&update_result);
        if let Some(upload) = materialized.uploaded() {
            self.cleanup_upload_artifacts(&upload.upload, &upload.cleanup_root);
        }
        drop(write_guard);
        drop(upload_guard);
        let updated_owner = update_result.definition.identity.owner;
        let updated_slug = update_result.definition.identity.slug;
        let updated_fingerprint = update_result.definition.identity.fingerprint;
        let payload = SkillsUpdateResponse {
            status: "updated".to_owned(),
            skill: SkillLifecycleResultSkill {
                skill_id: params.skill_id.clone(),
                owner: updated_owner.clone(),
                slug: updated_slug.clone(),
                source_kind: existing.source_kind.clone(),
                version: update_result.definition.identity.version_hint,
                fingerprint: updated_fingerprint.clone(),
                trust_level: updated_trust,
                install_path,
            },
            audit: SkillLifecycleAuditSummary {
                events_written: audit_records.len(),
            },
        };
        self.notify_skills_changed(
            workspace_id.as_str(),
            "updated",
            vec![SkillChangedItem {
                skill_id: params.skill_id,
                owner: updated_owner,
                slug: updated_slug,
                source_kind: existing.source_kind,
                change_type: "update".to_owned(),
                fingerprint_before: Some(existing.fingerprint),
                fingerprint_after: Some(updated_fingerprint),
            }],
            now,
        )
        .await;
        Ok(payload)
    }
}

fn stored_skill_revision_is_available(
    existing: &SkillInstallationRecord,
    source_kind: SkillSourceKind,
    managed_install_path: Option<&Path>,
    max_skill_file_bytes: usize,
) -> bool {
    if existing
        .source_ref
        .strip_prefix("import-path:")
        .is_some_and(|source_path| existing.install_path == source_path)
    {
        return false;
    }
    let Some(install_path) = managed_install_path else {
        return false;
    };
    if !install_path.is_dir() || !install_path.join("SKILL.md").is_file() {
        return false;
    }
    let source_root = install_path
        .parent()
        .and_then(Path::parent)
        .unwrap_or(install_path);
    pioneer_skills::parse_skill_from_file(
        existing.skill_id.clone(),
        install_path.join("SKILL.md").as_path(),
        source_kind,
        source_root,
        max_skill_file_bytes.max(1),
    )
    .is_ok_and(|definition| definition.identity.fingerprint == existing.fingerprint)
}
