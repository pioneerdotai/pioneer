use super::*;

impl MessageProcessor {
    pub(crate) async fn skills_update(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsUpdateParams,
    ) {
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

        let source_kind = match parse_installable_source_kind(params.source_kind.as_str()) {
            Some(kind) => kind,
            None => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        SKILLS_ERROR_SOURCE_NOT_SUPPORTED,
                        "update supports only `user` or `registry` source_kind",
                        json!({"source_kind": params.source_kind}),
                    ),
                )
                .await;
                return;
            }
        };
        let source_kind_db = source_kind.as_db_value().to_owned();
        let scope_key = skill_installation_scope_key(&source_kind, workspace_id.as_str());

        if params.slug.trim().is_empty() {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "slug is required",
                    json!({}),
                ),
            )
            .await;
            return;
        }

        if !is_qualified_slug(params.slug.as_str()) {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id.clone()),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "slug must use owner/slug",
                    json!({"slug": params.slug, "source_kind": params.source_kind}),
                ),
            )
            .await;
            return;
        }

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

        let source_ref = format!("upload:{}", materialized.upload.upload_id);
        let prepared = match pioneer_skills::prepare_materialized_skill(
            pioneer_skills::PrepareMaterializedSkillRequest {
                source_kind: source_kind.clone(),
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

        let preview_slug = qualified_skill_slug(
            prepared.definition.identity.owner.as_str(),
            prepared.definition.identity.slug.as_str(),
        );

        if preview_slug != params.slug {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    "source skill slug does not match requested slug",
                    json!({
                        "requested_slug": params.slug,
                        "source_slug": preview_slug,
                    }),
                ),
            )
            .await;
            return;
        }

        let now = now_timestamp_secs();
        let install_location = match install_location_for_source_kind(&context, &source_kind) {
            Some(location) => location,
            None => {
                let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        SKILLS_ERROR_SOURCE_NOT_SUPPORTED,
                        "update supports only `user` or `registry` source_kind",
                        json!({"source_kind": params.source_kind}),
                    ),
                )
                .await;
                return;
            }
        };

        let _guard = self.acquire_skills_write_lock().await;

        let existing = match self
            .crud_store
            .find_skill_installation(
                params.slug.as_str(),
                source_kind_db.as_str(),
                scope_key.as_str(),
            )
            .await
        {
            Ok(row) => row,
            Err(error) => {
                let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
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

        let Some(existing) = existing else {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_NOT_FOUND,
                    "skill installation was not found",
                    json!({"slug": params.slug, "source_kind": source_kind_db.clone()}),
                ),
            )
            .await;
            return;
        };

        if prepared.definition.identity.fingerprint == existing.fingerprint {
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
                    slug: existing.slug,
                    source_kind: existing.source_kind,
                    version: prepared.definition.identity.version_hint.clone(),
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
                install_root: install_location.install_root.clone(),
                lock_path: install_location.lock_path.clone(),
                expected_previous_fingerprint: params.expected_previous_fingerprint.clone(),
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

        let installation_record = SkillInstallationRecord {
            slug: qualified_skill_slug(
                update_result.definition.identity.owner.as_str(),
                update_result.definition.identity.slug.as_str(),
            ),
            version: update_result.definition.identity.version_hint.clone(),
            source_kind: source_kind_db.clone(),
            scope_key: scope_key.clone(),
            source_ref,
            install_path: install_path.clone(),
            trust_level: trust_level_as_str(&update_result.definition.runtime.trust_level)
                .to_owned(),
            fingerprint: update_result.definition.identity.fingerprint.clone(),
            updated_at_unix: now,
        };

        if let Err(error) = self
            .crud_store
            .upsert_skill_installation(&installation_record, now)
            .await
        {
            let _ = std::fs::remove_dir_all(materialized.cleanup_root.as_path());
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to persist skill installation",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }

        let audit_records = skill_audit_records(update_result.audit_events.as_slice());

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

        let payload = SkillsUpdateResponse {
            status: "updated".to_owned(),
            skill: SkillLifecycleResultSkill {
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
            return;
        }

        self.notify_skills_changed(
            workspace_id.as_str(),
            "updated",
            vec![SkillChangedItem {
                slug: installation_record.slug,
                source_kind: installation_record.source_kind,
                change_type: "update".to_owned(),
                fingerprint_before: Some(existing.fingerprint),
                fingerprint_after: Some(installation_record.fingerprint),
            }],
            now,
        )
        .await;
    }
}
