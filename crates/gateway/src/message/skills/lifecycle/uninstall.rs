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
        if let Err(error) = self
            .ensure_skills_lock_v2_locked(
                location.lock_path.as_path(),
                source_kind,
                workspace_id.as_str(),
            )
            .await
        {
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
        let now = now_timestamp_secs();
        let remove_install_path =
            pioneer_owned_install_path(&location, existing.install_path.as_str()).is_some();
        let uninstall_result =
            match pioneer_skills::uninstall_skill(pioneer_skills::UninstallSkillRequest {
                skill_id: params.skill_id.clone(),
                owner: existing.owner.clone(),
                slug: existing.slug.clone(),
                source_kind: existing.source_kind.clone(),
                install_path: PathBuf::from(existing.install_path.as_str()),
                remove_install_path,
                install_root: location.install_root.clone(),
                lock_path: location.lock_path.clone(),
                now_unix: now,
                policy: installer_policy(&context),
            }) {
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

        match self
            .crud_store
            .delete_skill_installation_with_workspace_policy(
                workspace_id.as_str(),
                &params.skill_id,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                self.send_error(
                    connection_id,
                    skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to remove skill installation row",
                        json!({"skill_id": params.skill_id}),
                    ),
                )
                .await;
                return;
            }
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
            skill_id: params.skill_id.clone(),
            owner: existing.owner.clone(),
            slug: existing.slug.clone(),
            source_kind: existing.source_kind.clone(),
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
            warn!(connection_id, error = %format!("{error:#}"), "failed to send skills/uninstall response");
            return;
        }
        self.notify_skills_changed(
            workspace_id.as_str(),
            "uninstalled",
            vec![SkillChangedItem {
                skill_id: params.skill_id,
                owner: existing.owner,
                slug: existing.slug,
                source_kind: existing.source_kind,
                change_type: "uninstall".to_owned(),
                fingerprint_before: Some(existing.fingerprint),
                fingerprint_after: None,
            }],
            now,
        )
        .await;
    }
}
