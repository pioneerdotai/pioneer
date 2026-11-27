use super::*;

impl MessageProcessor {
    pub(crate) async fn skills_uninstall(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsUninstallParams,
    ) {
        match self
            .uninstall_skill_deferred(request_context, request_id.clone(), params)
            .await
        {
            Ok((payload, publication)) => {
                match JsonRpcResponse::from_result(request_id, &payload) {
                    Ok(response) => {
                        if let Err(error) = self
                            .send_json(request_context.connection_id(), &response)
                            .await
                        {
                            warn!(error = %error, "failed to send skills_uninstall response");
                        }
                    }
                    Err(error) => {
                        self.send_error(
                            request_context.connection_id(),
                            skills_error(
                                None,
                                INVALID_REQUEST_CODE,
                                SKILLS_ERROR_INTERNAL,
                                "failed to encode uninstall response",
                                json!({"error": format!("{error:#}")}),
                            ),
                        )
                        .await
                    }
                }
                self.publish_skill_change(publication).await;
            }
            Err(error) => {
                self.send_error(request_context.connection_id(), error)
                    .await
            }
        }
    }

    async fn uninstall_skill_deferred(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsUninstallParams,
    ) -> std::result::Result<(SkillsUninstallResponse, SkillChangePublication), JsonRpcErrorResponse>
    {
        let deadline = self.agent_manager.control_operation_deadline();
        let native = self
            .begin_native_plugin_change(
                request_context,
                &request_id,
                &params.workspace_id,
                "skill",
                params.skill_id.as_str(),
                "remove",
                &[],
                deadline,
            )
            .await
            .map_err(|error| {
                skills_error(
                    Some(request_id.clone()),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INVALID_REQUEST,
                    super::super::super::packages::native_plugin_error_code(&error),
                    json!({}),
                )
            })?;
        let work = self.uninstall_skill_deferred_with_plugin_change(
            request_context,
            request_id.clone(),
            params,
            native.as_ref().map(|change| &change.write),
            false,
        );
        let result = work.await;
        match (result, native) {
            (Ok(payload), Some(change)) => {
                self.finish_native_plugin_change(change)
                    .await
                    .map_err(|error| {
                        skills_error(
                            None,
                            INVALID_REQUEST_CODE,
                            SKILLS_ERROR_INTERNAL,
                            super::super::super::packages::native_plugin_error_code(&error),
                            json!({}),
                        )
                    })?;
                Ok(payload)
            }
            (Err(error), Some(change)) => {
                let _ = self
                    .interrupt_native_plugin_change(&change, "plugins.component_remove_failed")
                    .await;
                Err(skills_error(
                    error.id,
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "plugin component removal requires repair",
                    json!({}),
                ))
            }
            (result, None) => result,
        }
    }

    pub(crate) async fn uninstall_skill_with_plugin_change(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsUninstallParams,
        native: Option<&pioneer_crud::PluginNativeWrite>,
        removing_package: bool,
    ) -> std::result::Result<SkillsUninstallResponse, JsonRpcErrorResponse> {
        let (response, publication) = self
            .uninstall_skill_deferred_with_plugin_change(
                request_context,
                request_id,
                params,
                native,
                removing_package,
            )
            .await?;
        self.publish_skill_change(publication).await;
        Ok(response)
    }

    async fn uninstall_skill_deferred_with_plugin_change(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: SkillsUninstallParams,
        native: Option<&pioneer_crud::PluginNativeWrite>,
        removing_package: bool,
    ) -> std::result::Result<(SkillsUninstallResponse, SkillChangePublication), JsonRpcErrorResponse>
    {
        let connection_id = request_context.connection_id();
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
        let parent = if let Some(pack_id) = existing.pack_id.as_ref() {
            match self
                .crud_store
                .find_skill_pack_installation(workspace_id.as_str(), pack_id)
                .await
            {
                Ok(Some(parent)) => Some(parent),
                Ok(None) => {
                    return Err(skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "skill pack for the installed child was not found",
                        json!({"pack_id": pack_id, "skill_id": existing.skill_id}),
                    ));
                }
                Err(error) => {
                    return Err(skills_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        SKILLS_ERROR_INTERNAL,
                        "failed to read skill pack for installed child",
                        json!({"error": format!("{error:#}")}),
                    ));
                }
            }
        } else {
            None
        };
        let app_write = native
            .filter(|write| write.kind == pioneer_protocol::PackageKind::App)
            .map(|_| {
                if removing_package {
                    AppSkillLockWrite::RemovePackage
                } else {
                    AppSkillLockWrite::Budgeted
                }
            });
        let lock_preparation = async {
            if let Some(write) =
                native.filter(|_| matches!(app_write, Some(AppSkillLockWrite::RemovePackage)))
            {
                // Both the parent mutation lock and the Skill write lock are
                // held. Validate the current owner before any early deletion.
                let owner = self
                    .crud_store
                    .find_skill_plugin_owner(&existing.skill_id)
                    .await?;
                anyhow::ensure!(
                    existing.pack_id.is_none()
                        && write.child_id == existing.skill_id.as_str()
                        && owner.is_some_and(|link| {
                            link.package_id == write.plugin_id
                                && link.member_key == write.member_key
                        }),
                    "plugins.component_owner_changed"
                );
                return self
                    .skill_lock_conversion_candidates(source_kind, workspace_id.as_str())
                    .await
                    .map(Some);
            }
            self.ensure_skills_lock_for_write(
                location.lock_path.as_path(),
                source_kind,
                workspace_id.as_str(),
                app_write,
            )
            .await?;
            Ok(None)
        }
        .await;
        let reclaim_candidates = match lock_preparation {
            Ok(candidates) => candidates,
            Err(error) => {
                if let Some(response) = super::source::app_skill_capacity_error(&request_id, &error)
                {
                    return Err(response);
                }
                return Err(skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to prepare skills lock",
                    json!({"error": format!("{error:#}")}),
                ));
            }
        };
        let now = now_timestamp_secs();
        let remove_install_path =
            pioneer_owned_install_path(&location, existing.install_path.as_str()).is_some();
        let uninstall_request = pioneer_skills::UninstallSkillRequest {
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
        };
        let removed = if let Some(candidates) = reclaim_candidates {
            pioneer_skills::uninstall_skill_reclaiming(
                uninstall_request,
                &candidates,
                |path, bytes| {
                    AppSkillLockWrite::RemovePackage.reserve(&self.app_storage, path, bytes)
                },
            )
        } else if let Some(write) = app_write {
            pioneer_skills::uninstall_skill_admitted(uninstall_request, |path, bytes| {
                write.reserve(&self.app_storage, path, bytes)
            })
        } else {
            pioneer_skills::uninstall_skill(uninstall_request)
        };
        let uninstall_result = match removed {
            Ok(result) => result,
            Err(error) => {
                if let Some(response) = super::source::app_skill_capacity_error(&request_id, &error)
                {
                    return Err(response);
                }
                let mapped = map_lifecycle_error(&error, methods::SKILLS_UNINSTALL);
                return Err(skills_error(
                    Some(request_id),
                    mapped.jsonrpc_code,
                    mapped.code,
                    mapped.message,
                    json!({"error": format!("{error:#}")}),
                ));
            }
        };

        let audit_records = skill_audit_records(uninstall_result.audit_events.as_slice());
        let packed_child = parent.is_some();
        let removed = if packed_child || native.is_some() {
            self.crud_store
                .uninstall_skill_installation_lifecycle_with_plugin_change(
                    &existing,
                    audit_records.as_slice(),
                    native,
                    now,
                )
                .await
        } else {
            self.crud_store
                .delete_skill_installation_with_workspace_policy(
                    workspace_id.as_str(),
                    &params.skill_id,
                )
                .await
        };
        match removed {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                return Err(skills_error(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    SKILLS_ERROR_INTERNAL,
                    "failed to remove skill installation row",
                    json!({"skill_id": params.skill_id}),
                ));
            }
        }
        if !packed_child
            && native.is_none()
            && let Err(error) = self
                .crud_store
                .insert_skill_audit_event_records(audit_records.as_slice())
                .await
        {
            return Err(skills_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                SKILLS_ERROR_INTERNAL,
                "failed to persist skill audit events",
                json!({"error": format!("{error:#}")}),
            ));
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

        let child_change = SkillChangedItem {
            skill_id: params.skill_id,
            owner: existing.owner,
            slug: existing.slug,
            source_kind: existing.source_kind,
            change_type: "uninstall".to_owned(),
            fingerprint_before: Some(existing.fingerprint),
            fingerprint_after: None,
        };
        let pack_changes = parent
            .map(|parent| SkillPackChangedItem {
                pack_id: parent.pack_id,
                change_type: "updated".to_owned(),
                name_before: Some(parent.name.clone()),
                name_after: Some(parent.name),
            })
            .into_iter()
            .collect();
        Ok((
            payload,
            SkillChangePublication {
                workspace_id,
                reason: "uninstalled",
                changes: vec![child_change],
                pack_changes,
                created_at: now,
            },
        ))
    }
}
