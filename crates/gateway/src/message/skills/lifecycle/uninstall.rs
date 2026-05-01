use super::*;

impl MessageProcessor {
    pub(crate) async fn skills_uninstall(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: SkillsUninstallParams,
    ) {
        let workspace_id = match self
            .validate_skills_workspace(
                connection_id,
                request_id.clone(),
                params.workspace_id,
                methods::SKILLS_UNINSTALL,
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
                        "uninstall supports only `user` or `registry` source_kind",
                        json!({"source_kind": params.source_kind}),
                    ),
                )
                .await;
                return;
            }
        };
        let source_kind_db = source_kind.as_db_value().to_owned();

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

        let _guard = self.acquire_skills_write_lock().await;

        let existing = match self
            .crud_store
            .find_skill_installation(params.slug.as_str(), source_kind_db.as_str())
            .await
        {
            Ok(row) => row,
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

        let Some(existing) = existing else {
            let payload = SkillsUninstallResponse {
                status: "not_installed".to_owned(),
                slug: params.slug,
                source_kind: source_kind_db.clone(),
                removed_install_path: None,
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
                            "failed to encode skills/uninstall response",
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
                    "failed to send skills/uninstall response"
                );
            }
            return;
        };

        let now = now_timestamp_secs();

        let install_location = match install_location_for_source_kind(&context, &source_kind) {
            Some(location) => location,
            None => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        SKILLS_ERROR_SOURCE_NOT_SUPPORTED,
                        "uninstall supports only `user` or `registry` source_kind",
                        json!({"source_kind": params.source_kind}),
                    ),
                )
                .await;
                return;
            }
        };

        let uninstall_result =
            pioneer_skills::uninstall_skill(pioneer_skills::UninstallSkillRequest {
                slug: params.slug.clone(),
                source_kind: source_kind.clone(),
                install_root: install_location.install_root.clone(),
                lock_path: install_location.lock_path.clone(),
                now_unix: now,
                policy: installer_policy(&context),
            });

        let uninstall_result = match uninstall_result {
            Ok(result) => result,
            Err(error) => {
                let mapped = map_lifecycle_error(&error, methods::SKILLS_UNINSTALL);
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        mapped.jsonrpc_code,
                        mapped.code,
                        mapped.message,
                        json!({"error": format!("{error:#}")}),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self
            .crud_store
            .delete_skill_installation(params.slug.as_str(), source_kind_db.as_str())
            .await
        {
            self.send_error(
                connection_id,
                skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to remove skill installation row",
                    json!({"error": format!("{error:#}")}),
                ),
            )
            .await;
            return;
        }

        let audit_records = skill_audit_records(uninstall_result.audit_events.as_slice());

        if let Err(error) = self
            .crud_store
            .insert_skill_audit_event_records(audit_records.as_slice())
            .await
        {
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

        let removed_install_path = uninstall_result
            .removed_path
            .as_ref()
            .map(|path| path.display().to_string())
            .or(Some(existing.install_path.clone()));

        let payload = SkillsUninstallResponse {
            status: "uninstalled".to_owned(),
            slug: params.slug.clone(),
            source_kind: source_kind_db.clone(),
            removed_install_path: removed_install_path.clone(),
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
                        "failed to encode skills/uninstall response",
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
                "failed to send skills/uninstall response"
            );
            return;
        }

        self.notify_skills_changed(
            workspace_id.as_str(),
            "uninstalled",
            vec![SkillChangedItem {
                slug: params.slug,
                source_kind: source_kind_db,
                change_type: "uninstall".to_owned(),
                fingerprint_before: Some(existing.fingerprint),
                fingerprint_after: None,
            }],
            now,
        )
        .await;
    }
}
