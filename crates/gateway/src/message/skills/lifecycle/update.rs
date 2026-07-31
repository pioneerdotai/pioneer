use super::*;

impl MessageProcessor {
    pub(crate) async fn skills_update(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsUpdateParams,
    ) {
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
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_NOT_FOUND,
                        "skill installation was not found",
                        json!({"skill_id": params.skill_id}),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to read existing installation",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        let (source_kind, location) = match install_location_for_stored_source_kind(
            &context,
            existing.source_kind.as_str(),
        ) {
            Ok(value) => value,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "stored skill installation has an invalid lifecycle source",
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
        let source_ref = format!("upload:{}", materialized.upload.upload_id);
        let prepared = match pioneer_skills::prepare_materialized_skill(
            pioneer_skills::PrepareMaterializedSkillRequest {
                skill_id: params.skill_id.clone(),
                source_kind,
                source_ref: source_ref.clone(),
                materialized_source_path: materialized.source_dir.clone(),
                policy: installer_policy(&context),
            },
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                let mapped = map_lifecycle_error(&error, methods::SKILLS_UPDATE);
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
        let upload_guard = self
            .acquire_skill_upload_lock(materialized.upload.upload_id.as_str())
            .await;
        let write_guard = self.acquire_skills_write_lock().await;
        if let Err(error) = self
            .revalidate_finalized_upload_locked(
                &authenticated_owner,
                workspace_id.as_str(),
                materialized.upload.upload_id.as_str(),
                &request_id,
            )
            .await
        {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            drop(write_guard);
            drop(upload_guard);
            self.send_error(connection_id, error).await;
            return;
        }
        if let Err(error) = self
            .ensure_skills_lock_v2_locked(
                location.lock_path.as_path(),
                source_kind,
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
        let row_unchanged = self
            .crud_store
            .find_skill_installation(&params.skill_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|current| current == existing);
        if !row_unchanged {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_UPDATE_CONFLICT_FINGERPRINT,
                    "skill installation changed while update was prepared",
                    json!({"skill_id": params.skill_id}),
                ),
            )
            .await;
            return;
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
        let now = now_timestamp_secs();
        if prepared.definition.identity.fingerprint == existing.fingerprint
            && stored_skill_revision_is_available(
                &existing,
                source_kind,
                previous_managed_install_path.as_deref(),
                context.security_policy.max_install_file_bytes,
            )
        {
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
            self.cleanup_upload_artifacts(
                &materialized.upload,
                materialized.cleanup_root.as_path(),
            );
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
            let response = match JsonRpcResponse::from_result(request_id, &payload) {
                Ok(response) => response,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        skills_error(
                            None,
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            "failed to encode skills/update response",
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
                    "failed to send skills/update response"
                );
            }
            return;
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
        let install_path = update_result.install_path.display().to_string();
        let patch = SkillInstallationPatch {
            owner: Some(update_result.definition.identity.owner.clone()),
            slug: Some(update_result.definition.identity.slug.clone()),
            version: Some(update_result.definition.identity.version_hint.clone()),
            source_ref: Some(source_ref),
            install_path: Some(install_path.clone()),
            trust_level: Some(
                trust_level_as_str(&update_result.definition.runtime.trust_level).to_owned(),
            ),
            fingerprint: Some(update_result.definition.identity.fingerprint.clone()),
            ..SkillInstallationPatch::default()
        };
        let audit_records = skill_audit_records(update_result.audit_events.as_slice());
        let persisted = self
            .crud_store
            .update_skill_lifecycle(
                &params.skill_id,
                &patch,
                audit_records.as_slice(),
                materialized.upload.upload_id.as_str(),
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
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            let error = match persisted {
                Ok(false) => anyhow::anyhow!(
                    "upload `{}` changed state before skill update publication",
                    materialized.upload.upload_id
                ),
                Err(error) => error,
                Ok(true) => unreachable!(),
            };
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to persist updated skill and consume upload",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }
        pioneer_skills::finalize_prepared_skill_commit(&update_result);
        self.cleanup_upload_artifacts(&materialized.upload, materialized.cleanup_root.as_path());
        let updated_owner = update_result.definition.identity.owner;
        let updated_slug = update_result.definition.identity.slug;
        let updated_fingerprint = update_result.definition.identity.fingerprint;
        let updated_trust =
            trust_level_as_str(&update_result.definition.runtime.trust_level).to_owned();
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
        let response = match JsonRpcResponse::from_result(request_id, &payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        None,
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to encode skills/update response",
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(connection_id, error = %format!("{error:#}"), "failed to send skills/update response");
            return;
        }
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
