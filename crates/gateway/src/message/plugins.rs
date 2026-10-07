//! Bounded package delivery and sequential calls to the native installers.
use super::*;
use anyhow::Context;
mod lifecycle;
use pioneer_crud::PluginOwnershipWrite;
use pioneer_plugins::{ComponentPlan, Entry, LoadedPluginPlan, Snapshot};
use pioneer_protocol::constants::methods;
use pioneer_protocol::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct PendingInstall {
    children: Vec<ReservedChild>,
    published_fingerprint: String,
    target_fingerprint: String,
    denied: Vec<PluginComponentKey>,
}
#[derive(Serialize, Deserialize, Clone)]
struct ReservedChild {
    kind: String,
    key: String,
    id: String,
}

fn plugin_error(id: &RequestId, message: &str) -> JsonRpcErrorResponse {
    JsonRpcErrorResponse::new(Some(id.clone()), INVALID_REQUEST_CODE, message)
}
fn public_diagnostics(plan: &LoadedPluginPlan) -> Vec<PluginDiagnostic> {
    let mut diagnostics: Vec<_> = plan
        .diagnostics
        .iter()
        .take(256)
        .map(|d| PluginDiagnostic {
            code: d.code.clone(),
            path: d.pointer.clone(),
            message: d.message.clone(),
        })
        .collect();
    if plan.diagnostics.len() > 256 {
        diagnostics.push(PluginDiagnostic {
            code: "additional_package_diagnostics".into(),
            path: String::new(),
            message: "Additional package paths need attention".into(),
        });
    }
    diagnostics
}
fn component_preview(component: &ComponentPlan) -> PluginComponentItem {
    let (kind, key) = match component {
        ComponentPlan::Skill { member_key, .. } => ("skill", member_key),
        ComponentPlan::Mcp { member_key, .. } => ("mcp", member_key),
    };
    PluginComponentItem {
        kind: kind.into(),
        member_key: key.clone(),
        status: "discovered".into(),
        diagnostic: None,
        skill_id: None,
        mcp_installation_id: None,
        runtime_status: None,
    }
}

// Snapshot has already normalized contained links. Only immutable regular data
// is published; denied assets cause a component failure, never a security bypass.
fn publish_package(snapshot: &Snapshot, root: &Path) -> anyhow::Result<()> {
    std::fs::create_dir(root)?;
    let result = (|| {
        for (key, entry) in snapshot.entries() {
            let path = root.join(key);
            match entry {
                Entry::Directory { .. } => std::fs::create_dir_all(&path)?,
                Entry::File { bytes, .. } => std::fs::write(&path, bytes)?,
                Entry::Denied => continue,
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for (key, entry) in snapshot.entries().iter().rev() {
                let mode = match entry {
                    Entry::Directory { mode } | Entry::File { mode, .. } => *mode,
                    Entry::Denied => continue,
                };
                std::fs::set_permissions(
                    root.join(key),
                    std::fs::Permissions::from_mode(mode & 0o777),
                )?;
            }
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(snapshot.root_mode()))?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(root);
    }
    result
}

impl MessageProcessor {
    async fn plugin_admit(
        &self,
        context: &RequestContext,
        id: &RequestId,
        workspace: &str,
        management: bool,
    ) -> Result<(), JsonRpcErrorResponse> {
        // The same authorization registry and resource resolver used by native
        // RPCs check both domains. No plugin-specific role/permission engine.
        for method in if management {
            [methods::SKILLS_INSTALL, methods::MCP_INSTALL]
        } else {
            [methods::SKILLS_LIST, methods::MCP_LIST]
        } {
            let request = JsonRpcRequest {
                jsonrpc: "2.0".into(),
                id: id.clone(),
                method: method.into(),
                params: Some(json!({"workspace_id": workspace})),
            };
            self.authorize_normal_request(context, &request).await?;
        }
        Ok(())
    }

    pub(super) async fn plugins_request(&self, context: &RequestContext, request: JsonRpcRequest) {
        let id = request.id.clone();
        let result = self.plugins_request_result(context, request).await;
        match result {
            Ok(value) => {
                let response = JsonRpcResponse::from_result(id.clone(), &value);
                match response {
                    Ok(response) => {
                        let _ = self.send_json(context.connection_id(), &response).await;
                    }
                    Err(_) => {
                        self.send_error(
                            context.connection_id(),
                            plugin_error(&id, "plugins.response_failed"),
                        )
                        .await
                    }
                }
            }
            Err(error) => self.send_error(context.connection_id(), error).await,
        }
    }
    async fn plugins_request_result(
        &self,
        context: &RequestContext,
        request: JsonRpcRequest,
    ) -> Result<serde_json::Value, JsonRpcErrorResponse> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
        let id = &request.id;
        let params = request.params.unwrap_or_else(|| json!({}));
        let invalid = || plugin_error(id, "plugins.invalid_request");
        let workspace = params
            .get("workspace_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(invalid)?;
        let management = matches!(
            request.method.as_str(),
            methods::PLUGINS_PREVIEW
                | methods::PLUGINS_INSTALL
                | methods::PLUGINS_SET_ENABLED
                | methods::PLUGINS_UPDATE
                | methods::PLUGINS_REMOVE
                | methods::PLUGINS_RETRY
                | methods::PLUGINS_CONTINUE
        );
        self.plugin_admit(context, id, workspace, management)
            .await?;
        let workspace = self
            .workspace_manager
            .validate_workspace_id(workspace)
            .await
            .map_err(|_| invalid())?;
        match request.method.as_str() {
            methods::PLUGINS_LIST => {
                let _: PluginsListParams =
                    serde_json::from_value(params.clone()).map_err(|_| invalid())?;
                let parents = self
                    .crud_store
                    .list_plugin_installations(&workspace)
                    .await
                    .map_err(|_| plugin_error(id, "plugins.inventory_failed"))?;
                let mut plugins = Vec::with_capacity(parents.len());
                for parent in parents {
                    plugins.push(
                        self.plugin_item(context, &parent)
                            .await
                            .map_err(|_| plugin_error(id, "plugins.inventory_failed"))?,
                    );
                }
                Ok(json!(PluginsListResponse { plugins }))
            }
            methods::PLUGINS_DETAILS => {
                let params: PluginsDetailsParams =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                let parent = self
                    .crud_store
                    .find_plugin_installation(&params.plugin_id)
                    .await
                    .map_err(|_| invalid())?
                    .filter(|p| p.workspace_id == workspace)
                    .ok_or_else(invalid)?;
                Ok(json!(
                    self.plugin_item(context, &parent)
                        .await
                        .map_err(|_| invalid())?
                ))
            }
            methods::PLUGINS_SET_ENABLED => {
                let params: PluginsSetEnabledParams =
                    serde_json::from_value(params).map_err(|_| invalid())?;
                self.set_plugin_enabled(context, id, params, deadline)
                    .await
                    .map(|item| json!(item))
            }
            methods::PLUGINS_UPDATE
            | methods::PLUGINS_REMOVE
            | methods::PLUGINS_RETRY
            | methods::PLUGINS_CONTINUE => {
                let params = match request.method.as_str() {
                    methods::PLUGINS_UPDATE => {
                        let p: PluginsUpdateParams =
                            serde_json::from_value(params).map_err(|_| invalid())?;
                        PluginsMutateParams {
                            workspace_id: p.workspace_id,
                            plugin_id: p.plugin_id,
                            expected_revision: p.expected_revision,
                            intent: PluginManagementIntent::Update {
                                upload_id: p.upload_id,
                                expected_fingerprint: p.expected_fingerprint,
                                confirm_changes: p.confirm_changes,
                            },
                        }
                    }
                    methods::PLUGINS_REMOVE => {
                        let p: PluginsRemoveParams =
                            serde_json::from_value(params).map_err(|_| invalid())?;
                        PluginsMutateParams {
                            workspace_id: p.workspace_id,
                            plugin_id: p.plugin_id,
                            expected_revision: p.expected_revision,
                            intent: PluginManagementIntent::Remove {
                                purge_data: p.purge_data,
                            },
                        }
                    }
                    methods::PLUGINS_RETRY => {
                        let p: PluginsRetryParams =
                            serde_json::from_value(params).map_err(|_| invalid())?;
                        PluginsMutateParams {
                            workspace_id: p.workspace_id,
                            plugin_id: p.plugin_id,
                            expected_revision: p.expected_revision,
                            intent: PluginManagementIntent::Retry {
                                components: p.components.unwrap_or_default(),
                                restore_removed: p.restore_removed,
                            },
                        }
                    }
                    _ => {
                        let p: PluginsContinueParams =
                            serde_json::from_value(params).map_err(|_| invalid())?;
                        PluginsMutateParams {
                            workspace_id: p.workspace_id,
                            plugin_id: p.plugin_id,
                            expected_revision: p.expected_revision,
                            intent: PluginManagementIntent::Continue,
                        }
                    }
                };
                self.mutate_plugin(context, id, params, deadline)
                    .await
                    .map(|result| json!(result))
            }
            methods::PLUGINS_PREVIEW | methods::PLUGINS_INSTALL => {
                let (upload_id, expected) = if request.method == methods::PLUGINS_INSTALL {
                    let params: PluginsInstallParams =
                        serde_json::from_value(params).map_err(|_| invalid())?;
                    (params.upload_id, Some(params.expected_fingerprint))
                } else {
                    let params: PluginsSourceParams =
                        serde_json::from_value(params).map_err(|_| invalid())?;
                    if let Some(target) = params.target {
                        return self
                            .preview_plugin_update(
                                context,
                                id,
                                PluginsUpdatePreviewParams {
                                    workspace_id: params.workspace_id,
                                    upload_id: params.upload_id,
                                    plugin_id: target.plugin_id,
                                    expected_revision: target.expected_revision,
                                },
                            )
                            .await
                            .map(|preview| json!(preview));
                    }
                    (params.upload_id, None)
                };
                if expected.is_some()
                    && let Some(parent) = self
                        .crud_store
                        .find_plugin_by_upload(&upload_id)
                        .await
                        .map_err(|_| invalid())?
                {
                    if parent.workspace_id != workspace
                        || expected.as_deref() != Some(&parent.package_fingerprint)
                    {
                        return Err(invalid());
                    }
                    return Ok(json!(
                        self.plugin_item(context, &parent)
                            .await
                            .map_err(|_| invalid())?
                    ));
                }
                let runtime = self
                    .skills_runtime_context(&workspace)
                    .map_err(|_| invalid())?;
                let source = self
                    .materialize_uploaded_archive_source(
                        context.connection_id(),
                        &workspace,
                        &upload_id,
                        &runtime,
                        id,
                    )
                    .await
                    .map_err(|_| {
                        plugin_error(id, "plugins.upload_unavailable_or_invalid_archive")
                    })?;
                let loaded = (|| {
                    if source.upload.purpose != "plugin" {
                        anyhow::bail!("purpose mismatch");
                    }
                    let snapshot =
                        Snapshot::capture(&source.source_dir, Default::default(), || false)?;
                    let plan = pioneer_plugins::load(&snapshot)
                        .map_err(|_| anyhow::anyhow!("package rejected"))?;
                    Ok::<_, anyhow::Error>((snapshot, plan))
                })();
                let (snapshot, plan) = match loaded {
                    Ok(v) => v,
                    Err(_) => {
                        let _ = std::fs::remove_dir_all(&source.cleanup_root);
                        return Err(plugin_error(id, "plugins.package_rejected"));
                    }
                };
                if let Some(expected) = expected {
                    if expected != plan.tree_digest {
                        let _ = std::fs::remove_dir_all(&source.cleanup_root);
                        return Err(plugin_error(id, "plugins.fingerprint_changed"));
                    }
                    let _upload_guard = self.acquire_skill_upload_lock(&upload_id).await;
                    let outcome = if let Some(parent) = self
                        .crud_store
                        .find_plugin_by_upload(&upload_id)
                        .await
                        .map_err(|_| invalid())?
                    {
                        if parent.workspace_id != workspace
                            || parent.package_fingerprint != expected
                        {
                            Err(invalid())
                        } else {
                            self.plugin_item(context, &parent)
                                .await
                                .map_err(|_| invalid())
                        }
                    } else {
                        self.install_plugin(
                            context,
                            id,
                            &workspace,
                            &source.upload,
                            &runtime.plugin_root(),
                            snapshot,
                            plan,
                        )
                        .await
                    };
                    if self
                        .crud_store
                        .find_plugin_by_upload(&upload_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some()
                    {
                        self.cleanup_upload_artifacts(&source.upload, &source.cleanup_root);
                    } else {
                        let _ = std::fs::remove_dir_all(&source.cleanup_root);
                    }
                    outcome.map(|item| json!(item))
                } else {
                    let _ = std::fs::remove_dir_all(&source.cleanup_root);
                    Ok(json!(PluginsPreviewResponse {
                        name: plan.manifest.name.clone(),
                        version: plan.manifest.version.clone(),
                        fingerprint: plan.tree_digest.clone(),
                        components: plan.components.iter().map(component_preview).collect(),
                        diagnostics: public_diagnostics(&plan)
                    }))
                }
            }
            _ => Err(invalid()),
        }
    }

    async fn install_plugin(
        &self,
        context: &RequestContext,
        request_id: &RequestId,
        workspace: &str,
        upload: &pioneer_crud::SkillUploadSessionRecord,
        runtime_root: &Path,
        snapshot: Snapshot,
        plan: LoadedPluginPlan,
    ) -> Result<PluginItem, JsonRpcErrorResponse> {
        let error = || plugin_error(request_id, "plugins.install_failed");
        let plugin_id = pioneer_protocol::generate_id(21);
        let root = runtime_root.join(&plugin_id);
        let package = root.join("package");
        let data = root.join("data");
        std::fs::create_dir_all(&root).map_err(|_| error())?;
        if publish_package(&snapshot, &package).is_err() || std::fs::create_dir(&data).is_err() {
            let _ = std::fs::remove_dir_all(&root);
            return Err(error());
        }
        let pending = PendingInstall {
            children: plan
                .components
                .iter()
                .map(|c| {
                    let item = component_preview(c);
                    ReservedChild {
                        kind: item.kind,
                        key: item.member_key,
                        id: pioneer_protocol::generate_id(21),
                    }
                })
                .collect(),
            published_fingerprint: Snapshot::capture(&package, Default::default(), || false)
                .map_err(|_| error())?
                .tree_digest(),
            target_fingerprint: plan.tree_digest.clone(),
            denied: lifecycle::denied_keys(&snapshot, &plan),
        };
        let pending_json = serde_json::to_string(&pending).map_err(|_| error())?;
        if pending_json.len() > 65536 {
            let _ = std::fs::remove_dir_all(&root);
            return Err(error());
        }
        let now = chrono::Utc::now().fixed_offset();
        let _parent_guard = self
            .plugin_mutation_lock(&plugin_id)
            .await
            .try_lock_owned()
            .map_err(|_| error())?;
        let parent = pioneer_entity::plugin_installation::Model {
            id: plugin_id.clone(),
            workspace_id: workspace.into(),
            name: plan.manifest.name.clone(),
            version: plan.manifest.version.clone(),
            source_upload_id: upload.upload_id.clone(),
            package_path: package.to_string_lossy().into(),
            data_path: data.to_string_lossy().into(),
            package_fingerprint: plan.tree_digest.clone(),
            enabled: true,
            state: "installing".into(),
            revision: 1,
            pending_json: Some(pending_json),
            last_error: None,
            created_at: now,
            updated_at: now,
        };
        // Authentication ownership (not just connection ID) is rechecked through
        // the existing upload contract immediately before the atomic DB reserve.
        let owner = AuthenticatedTransferOwner::from_request_context(context);
        if let Err(error) = self
            .revalidate_plugin_upload(&owner, workspace, &upload.upload_id, request_id)
            .await
        {
            let _ = std::fs::remove_dir_all(&root);
            return Err(error);
        }
        if self
            .crud_store
            .reserve_plugin_installation(&parent, context.connection_id(), now.timestamp())
            .await
            .is_err()
        {
            let _ = std::fs::remove_dir_all(&root);
            return Err(error());
        }
        let parent_guard = PluginMutationGuard {
            parent: parent.clone(),
            _lock: _parent_guard,
        };
        self.skill_upload_owners
            .lock()
            .await
            .remove(&upload.upload_id);

        for (component, reserved) in plan.components.iter().zip(&pending.children) {
            let mut ownership = PluginOwnershipWrite {
                plugin_id: plugin_id.clone(),
                expected_revision: 1,
                member_key: reserved.key.clone(),
                member_path: None,
                package_fingerprint: plan.tree_digest.clone(),
                child_id: reserved.id.clone(),
            };
            let result = match component {
                ComponentPlan::Skill {
                    member_path,
                    tree_digest,
                    ..
                } => {
                    ownership.member_path = Some(member_path.clone());
                    ownership.package_fingerprint = tree_digest.clone();
                    let denied = snapshot.entries().iter().any(|(key, entry)| {
                        matches!(entry, Entry::Denied)
                            && (key == member_path || key.starts_with(&format!("{member_path}/")))
                    });
                    if denied {
                        Err(error())
                    } else {
                        self.install_skill_source(
                            context,
                            request_id.clone(),
                            workspace.into(),
                            "user".into(),
                            skills::SkillInstallSource::PackageMember(ownership.clone()),
                        )
                        .await
                        .map(|_| ())
                    }
                }
                ComponentPlan::Mcp { server, .. } => {
                    match mcp::portable::portable_install_plan(
                        server, &package, &data, workspace, &ownership,
                    ) {
                        Ok(native) => self
                            .install_mcp_plan(
                                context,
                                request_id.clone(),
                                workspace,
                                native,
                                Some(&ownership),
                                None,
                                true,
                            )
                            .await
                            .and_then(|response| {
                                if response.servers.iter().all(|s| {
                                    matches!(
                                        s.status,
                                        McpInstallResultStatus::Installed
                                            | McpInstallResultStatus::Updated
                                    )
                                }) && response.servers.len() == 1
                                {
                                    Ok(())
                                } else {
                                    Err(error())
                                }
                            }),
                        Err(_) => Err(error()),
                    }
                }
            };
            if result.is_err() {
                // An operation can fail after its native commit (for example a
                // reload error). Retain that real link and mark the component failed.
                self.crud_store
                    .record_plugin_component_failure(
                        &ownership,
                        &reserved.kind,
                        if let ComponentPlan::Skill { member_path, .. } = component {
                            if snapshot.entries().iter().any(|(path, entry)| {
                                matches!(entry, Entry::Denied)
                                    && (path == member_path
                                        || path.starts_with(&format!("{member_path}/")))
                            }) {
                                "component_path_denied"
                            } else {
                                "component_install_failed"
                            }
                        } else {
                            "component_install_failed"
                        },
                    )
                    .await
                    .map_err(|_| error())?;
            }
        }
        let diagnostics = serde_json::to_string(&public_diagnostics(&plan)).map_err(|_| error())?;
        lifecycle::save_package_integrity(
            &package,
            &Snapshot::capture(&package, Default::default(), || false)
                .map_err(|_| error())?
                .tree_digest(),
        )
        .map_err(|_| error())?;
        self.crud_store
            .prepare_plugin_reload(&plugin_id, 1)
            .await
            .map_err(|_| error())?;
        if self
            .mcp_service
            .reload_after_plugin_change(&parent_guard)
            .await
            .is_err()
        {
            self.crud_store
                .finish_plugin_mutation(
                    &plugin_id,
                    1,
                    "interrupted",
                    Some("plugins.reload_unconfirmed".into()),
                )
                .await
                .map_err(|_| error())?;
        } else {
            self.crud_store
                .finish_plugin_mutation(&plugin_id, 1, "installed", Some(diagnostics))
                .await
                .map_err(|_| error())?;
        }
        let parent = self
            .crud_store
            .find_plugin_installation(&plugin_id)
            .await
            .map_err(|_| error())?
            .ok_or_else(error)?;
        let item = self
            .plugin_item(context, &parent)
            .await
            .map_err(|_| error())?;
        self.send_gateway_management_notification(
            methods::PLUGINS_CHANGED,
            &PluginsChangedNotification {
                workspace_id: workspace.into(),
                plugin_id,
                revision: 1,
            },
        )
        .await;
        Ok(item)
    }

    async fn plugin_item(
        &self,
        context: &RequestContext,
        parent: &pioneer_entity::plugin_installation::Model,
    ) -> anyhow::Result<PluginItem> {
        let links = self.crud_store.list_plugin_components(&parent.id).await?;
        let member = crate::authorization::AuthorizationService::new().role_disclosure_policy(
            context.principal().kind,
            context.principal().role_key.as_ref(),
        ) != Some(crate::authorization::RoleDisclosurePolicy::Administrative);
        let mut skills = std::collections::HashMap::new();
        if links.iter().any(|l| l.kind == "skill") {
            let runtime = self.skills_runtime_context(&parent.workspace_id)?;
            let catalog = self
                .load_skills_catalog(&parent.workspace_id, &runtime)
                .await?;
            let policies = self
                .crud_store
                .list_workspace_skill_policies(&parent.workspace_id)
                .await?;
            let policies = self.build_policy_set(&catalog.skills, &policies, &runtime);
            let installations = self.crud_store.list_skill_installations().await?;
            let refs = catalog
                .skills
                .iter()
                .map(|s| pioneer_skills::SkillExplicitRef {
                    skill_id: s.identity.skill_id.clone(),
                    label: None,
                })
                .collect::<Vec<_>>();
            let resolution = pioneer_skills::resolve_skills(pioneer_skills::SkillResolutionInput {
                explicit_refs: &refs,
                touched_paths: &[],
                catalog: &catalog,
                policy_set: &policies,
                validation_policy: runtime.validation_policy,
                dependency_input: &pioneer_skills::DependencyCheckInput::baseline(),
            });
            for skill in &catalog.skills {
                let policy = pioneer_skills::effective_policy_for_skill(skill, &policies);
                let installed = installations.iter().any(|i| {
                    i.scope_key == parent.workspace_id && i.skill_id == skill.identity.skill_id
                });
                let visible = super::skills::skill_is_disclosed(
                    context.principal(),
                    skill,
                    &policy,
                    installed,
                );
                let status = if !skill.is_available() {
                    "unavailable"
                } else if !policy.enabled {
                    "disabled"
                } else if resolution
                    .active
                    .iter()
                    .any(|a| a.skill_id == skill.identity.skill_id)
                {
                    "active"
                } else {
                    "blocked"
                };
                skills.insert(
                    skill.identity.skill_id.to_string(),
                    (visible, status.to_owned()),
                );
            }
        }
        let mcp = self
            .crud_store
            .list_mcp_server_installations("workspace", &parent.workspace_id)
            .await?;
        let runtime = self
            .mcp_service
            .runtime_snapshot("workspace", &parent.workspace_id)
            .await;
        let mut components = Vec::with_capacity(links.len());
        let mut hidden = false;
        for link in links {
            let visible = match link.kind.as_str() {
                "skill" => link
                    .skill_id
                    .as_ref()
                    .and_then(|id| skills.get(id))
                    .map(|s| s.0)
                    .unwrap_or(!member),
                "mcp" => link
                    .mcp_installation_id
                    .as_ref()
                    .and_then(|id| mcp.iter().find(|r| r.id.as_ref() == Some(id)))
                    .map(|row| {
                        super::mcp::mcp_installation_is_disclosed(
                            context.principal(),
                            row.id.as_deref().unwrap(),
                            row.enabled,
                        )
                    })
                    .unwrap_or(!member),
                _ => !member,
            };
            if !visible {
                hidden = true;
                continue;
            }
            let runtime_status = if let Some(id) = &link.mcp_installation_id {
                if mcp.iter().any(|r| r.id.as_ref() == Some(id) && !r.enabled) {
                    Some("disabled".into())
                } else {
                    runtime
                        .get(id)
                        .map(|state| format!("{:?}", state.state).to_ascii_lowercase())
                }
            } else {
                link.skill_id
                    .as_ref()
                    .and_then(|id| skills.get(id))
                    .map(|s| s.1.clone())
            };
            components.push(PluginComponentItem {
                kind: link.kind,
                member_key: link.member_key,
                status: link.status,
                diagnostic: if member { None } else { link.diagnostic },
                skill_id: link.skill_id.map(SkillId::new).transpose()?,
                mcp_installation_id: link.mcp_installation_id,
                runtime_status,
            });
        }
        let mut diagnostics: Vec<PluginDiagnostic> = parent
            .last_error
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_else(|| {
                if parent.state == "interrupted" {
                    vec![PluginDiagnostic {
                        code: match parent.last_error.as_deref() {
                            Some("plugins.reapply_native_change") => "reapply_native_change",
                            Some(
                                "plugins.fresh_package_required"
                                | "plugins.package_unavailable"
                                | "plugins.package_changed"
                                | "plugins.package_rejected"
                                | "plugins.package_repair_required",
                            ) => "fresh_package_required",
                            _ => "operation_interrupted",
                        }
                        .into(),
                        path: String::new(),
                        message: "Operation was interrupted; continue or remove the plugin".into(),
                    }]
                } else {
                    Vec::new()
                }
            });
        // Package pointers can name a hidden child even when its ID is removed.
        // Members receive only a neutral parent notice; management retains the
        // complete authored inventory and package diagnostics.
        if member {
            let unavailable = hidden || !diagnostics.is_empty();
            diagnostics.clear();
            if unavailable {
                diagnostics.push(PluginDiagnostic {
                    code: "components_unavailable".into(),
                    path: String::new(),
                    message: "Some components are unavailable".into(),
                });
            }
        }
        let partial = diagnostics.iter().any(|d| {
            !matches!(
                d.code.as_str(),
                "unknown_manifest_field" | "invalid_extensions_container"
            )
        }) || components.iter().any(|c| c.status != "installed");
        Ok(PluginItem {
            id: parent.id.clone(),
            name: parent.name.clone(),
            version: parent.version.clone(),
            enabled: parent.enabled,
            state: if parent.pending_json.is_some() && parent.state == "installed" {
                "updating".into()
            } else {
                parent.state.clone()
            },
            revision: parent.revision,
            status: if parent.state == "installed" && parent.pending_json.is_some() {
                "updating".into()
            } else if parent.state != "installed" {
                parent.state.clone()
            } else if partial {
                "partial".into()
            } else {
                "installed".into()
            },
            components,
            diagnostics,
        })
    }
}

/// Request-owned parent admission. Internal native calls borrow this typed
/// context instead of reacquiring the parent before their native lifecycle lock.
pub(crate) struct PluginMutationGuard {
    pub(super) parent: pioneer_entity::plugin_installation::Model,
    _lock: tokio::sync::OwnedMutexGuard<()>,
}

impl PluginMutationGuard {
    pub(crate) fn parent(&self) -> &pioneer_entity::plugin_installation::Model {
        &self.parent
    }
}

impl MessageProcessor {
    async fn plugin_mutation_lock(&self, id: &str) -> Arc<tokio::sync::Mutex<()>> {
        plugin_mutex(&self.plugin_mutation_locks, id).await
    }

    pub(super) async fn acquire_plugin_mutation(
        &self,
        workspace: &str,
        id: &str,
        revision: i64,
    ) -> anyhow::Result<PluginMutationGuard> {
        // Scope is checked before busy disclosure, then revalidated after
        // acquiring the same parent mutex as native launch/recovery.
        anyhow::ensure!(
            self.crud_store
                .find_plugin_installation(id)
                .await?
                .is_some_and(|parent| parent.workspace_id == workspace),
            "plugins.not_found"
        );
        let lock = self
            .plugin_mutation_lock(id)
            .await
            .try_lock_owned()
            .map_err(|_| anyhow::anyhow!("plugins.busy"))?;
        let parent = self
            .crud_store
            .find_plugin_installation(id)
            .await?
            .filter(|parent| parent.workspace_id == workspace)
            .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
        anyhow::ensure!(parent.revision == revision, "plugins.stale");
        Ok(PluginMutationGuard {
            parent,
            _lock: lock,
        })
    }

    /// Final Gateway start seam shares the mutation mutex. The immutable
    /// trusted Turn snapshot is revalidated while these guards remain owned
    /// through native enqueue/publication ACK. Sorted acquisition prevents
    /// cycles for a request selecting several parents; no launch lease is added.
    pub(super) async fn acquire_plugin_launch_guards(
        &self,
        workspace: &str,
        turn: &str,
    ) -> anyhow::Result<Vec<tokio::sync::OwnedMutexGuard<()>>> {
        acquire_plugin_launch_admission(
            &self.crud_store,
            &self.plugin_mutation_locks,
            workspace,
            turn,
        )
        .await
    }

    pub(super) async fn acquire_task_plugin_launch_guards(
        &self,
        task: &pioneer_protocol::Task,
        turn: &str,
    ) -> anyhow::Result<Vec<tokio::sync::OwnedMutexGuard<()>>> {
        if self.crud_store.get_plugin_selection(turn).await?.is_some() {
            return self
                .acquire_plugin_launch_guards(&task.workspace_id, turn)
                .await;
        }
        let Some(launch) = self
            .crud_store
            .get_task_actor_contract(&task.id)
            .await?
            .and_then(|contract| contract.launch)
        else {
            return Ok(Vec::new());
        };
        let normalized = self
            .normalize_task_launch_capabilities(&task.workspace_id, &launch.execution)
            .await?;
        let Some(snapshot) = normalized.plugin_selection else {
            return Ok(Vec::new());
        };
        let guards = acquire_plugin_selection_admission(
            self.crud_store.as_ref(),
            &self.plugin_mutation_locks,
            &task.workspace_id,
            &snapshot,
        )
        .await?;
        self.crud_store
            .prepare_plugin_selection(turn, &snapshot)
            .await?;
        Ok(guards)
    }

    pub(super) async fn acquire_cli_instance_plugin_guards(
        &self,
        instance: &crate::cli_runtime::session_instance::CliSessionInstanceId,
    ) -> anyhow::Result<Vec<tokio::sync::OwnedMutexGuard<()>>> {
        let manager = self
            .cli_runtime_manager
            .as_ref()
            .context("CLI manager unavailable")?;
        let snapshot = manager.plugin_selection_for_instance(instance)?;
        let guards = match snapshot {
            Some(snapshot) => {
                anyhow::ensure!(snapshot.phase == "ready", "plugins.selection_not_ready");
                acquire_plugin_selection_admission(
                    self.crud_store.as_ref(),
                    &self.plugin_mutation_locks,
                    &instance.key().workspace_id,
                    &snapshot,
                )
                .await?
            }
            None => Vec::new(),
        };
        anyhow::ensure!(
            manager.is_current_instance(instance).await,
            "CLI instance changed before continuation"
        );
        Ok(guards)
    }
}

impl MessageProcessor {
    pub(super) async fn stop_plugin_execution(
        &self,
        guard: &PluginMutationGuard,
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<()> {
        let parent = &guard.parent;
        let cli_owners = self
            .cli_runtime_manager
            .as_ref()
            .map(|manager| manager.plugin_stop_inventory(&parent.workspace_id, &parent.id))
            .unwrap_or_default();
        // Actual native inventory supplies candidate IDs. DB status is never
        // used to infer quiescence, and stale historical rows are not scanned.
        let ids = self.agent_manager.native_stop_thread_ids().await?;
        let threads = self
            .crud_store
            .plugin_workspace_native_threads(&parent.workspace_id, &ids)
            .await?;
        let candidates = self
            .agent_manager
            .native_stop_turn_candidates(&threads)
            .await?;
        let mut candidates: std::collections::BTreeSet<_> = candidates.into_iter().collect();
        candidates.extend(
            self.crud_store
                .plugin_graph_stop_candidates(&parent.workspace_id, &parent.id)
                .await?,
        );
        for (thread, turn) in candidates {
            let Some(selection) = self.crud_store.get_plugin_selection(&turn).await? else {
                continue;
            };
            if !selection
                .parents
                .iter()
                .any(|selected| selected.id == parent.id)
            {
                continue;
            }
            let (_, row) = self
                .crud_store
                .get_turn(&thread, &turn)
                .await?
                .ok_or_else(|| anyhow::anyhow!("plugins.execution_owner_unknown"))?;
            self.cancel_root_agent_work_graph_for_turn_and_wait(
                &thread,
                &row,
                "plugin configuration changed",
                deadline,
            )
            .await?;
        }
        if let Some(manager) = &self.cli_runtime_manager {
            for owner in &cli_owners {
                manager.stop_and_wait(owner, deadline).await?;
            }
            anyhow::ensure!(
                manager
                    .plugin_stop_inventory(&parent.workspace_id, &parent.id)
                    .is_empty(),
                "plugins.execution_publication_changed"
            );
        }
        // Graph/fallback stop drains model tool work. The same installation
        // lifecycle then acknowledges MCP invocation/session/process shutdown.
        for child in self.crud_store.list_plugin_components(&parent.id).await? {
            let Some(id) = child.mcp_installation_id else {
                continue;
            };
            let name = mcp::portable::internal_mcp_name(&parent.id, &child.member_key);
            let _native = self
                .mcp_service
                .installation_lifecycle_guard("workspace", &parent.workspace_id, &name)
                .await;
            let row = self
                .crud_store
                .find_mcp_server_installation("workspace", &parent.workspace_id, &name)
                .await?
                .filter(|row| row.id.as_deref() == Some(id.as_str()))
                .ok_or_else(|| anyhow::anyhow!("plugins.component_owner_changed"))?;
            let owner = self
                .crud_store
                .find_mcp_plugin_owner(&id)
                .await?
                .filter(|owner| {
                    owner.plugin_id == parent.id && owner.member_key == child.member_key
                })
                .ok_or_else(|| anyhow::anyhow!("plugins.component_owner_changed"))?;
            let _ = owner;
            tokio::time::timeout_at(deadline, self.mcp_service.stop_admitted_installation(&row))
                .await
                .map_err(|_| anyhow::anyhow!("plugins.stop_timeout"))??;
        }
        Ok(())
    }

    async fn publish_plugin_changed(&self, parent: &pioneer_entity::plugin_installation::Model) {
        self.send_gateway_management_notification(
            methods::PLUGINS_CHANGED,
            &PluginsChangedNotification {
                workspace_id: parent.workspace_id.clone(),
                plugin_id: parent.id.clone(),
                revision: parent.revision,
            },
        )
        .await;
    }

    pub(super) async fn set_plugin_enabled(
        &self,
        context: &RequestContext,
        request: &RequestId,
        params: PluginsSetEnabledParams,
        deadline: tokio::time::Instant,
    ) -> Result<PluginItem, JsonRpcErrorResponse> {
        let mut guard = self
            .acquire_plugin_mutation(
                &params.workspace_id,
                &params.plugin_id,
                params.expected_revision,
            )
            .await
            .map_err(|error| {
                plugin_error(
                    request,
                    match error.to_string().as_str() {
                        "plugins.busy" => "plugins.busy",
                        "plugins.stale" => "plugins.stale",
                        _ => "plugins.not_found",
                    },
                )
            })?;
        if guard.parent.state != "installed" || guard.parent.pending_json.is_some() {
            return Err(plugin_error(request, "plugins.interrupted"));
        }
        if guard.parent.enabled == params.enabled {
            return self
                .plugin_item(context, &guard.parent)
                .await
                .map_err(|_| plugin_error(request, "plugins.inventory_failed"));
        }
        let pending = serde_json::to_string(
            &json!({"kind": "set_enabled", "enabled": params.enabled, "children": []}),
        )
        .map_err(|_| plugin_error(request, "plugins.invalid_request"))?;
        guard.parent = self
            .crud_store
            .begin_plugin_mutation(
                &params.workspace_id,
                &params.plugin_id,
                params.expected_revision,
                "updating",
                params.enabled,
                &pending,
            )
            .await
            .map_err(|_| plugin_error(request, "plugins.stale"))?;
        self.publish_plugin_changed(&guard.parent).await;
        // One budget spans stop and reload, matching the ordinary RPC budget.
        // Cancellation leaves this durable closed plan available for Continue.
        let work = tokio::time::timeout_at(deadline, async {
            self.stop_plugin_execution(&guard, deadline).await?;
            self.crud_store
                .prepare_plugin_reload(&guard.parent.id, guard.parent.revision)
                .await?;
            self.mcp_service.reload_after_plugin_change(&guard).await?;
            self.crud_store
                .finish_plugin_mutation(&guard.parent.id, guard.parent.revision, "installed", None)
                .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(work, Ok(Ok(()))) {
            // A cancelled acknowledgement may follow a committed final writer.
            // Re-read it instead of reopening a completed action with stale data.
            if let Some(current) = self
                .crud_store
                .find_plugin_installation(&guard.parent.id)
                .await
                .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?
            {
                if current.pending_json.is_some() {
                    self.crud_store
                        .finish_plugin_mutation(
                            &current.id,
                            current.revision,
                            "interrupted",
                            Some("plugins.stop_or_reload_unconfirmed".into()),
                        )
                        .await
                        .map_err(|_| plugin_error(request, "plugins.interrupted"))?;
                }
            }
        }
        let parent = self
            .crud_store
            .find_plugin_installation(&guard.parent.id)
            .await
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?
            .ok_or_else(|| plugin_error(request, "plugins.not_found"))?;
        self.publish_plugin_changed(&parent).await;
        self.plugin_item(context, &parent)
            .await
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))
    }
}

/// A native controller owns the same parent admission before its native lock.
/// Internal package operations already own PluginMutationGuard and never call
/// this acquisition path recursively.
pub(super) struct NativePluginMutation {
    pub(super) guard: PluginMutationGuard,
    pub(super) write: pioneer_crud::PluginNativeWrite,
    pub(super) deadline: tokio::time::Instant,
}
impl MessageProcessor {
    pub(super) async fn begin_native_plugin_change(
        &self,
        context: &RequestContext,
        request: &RequestId,
        workspace: &str,
        kind: &str,
        child: &str,
        action: &str,
        fields: &[&str],
        deadline: tokio::time::Instant,
    ) -> anyhow::Result<Option<NativePluginMutation>> {
        let link = match kind {
            "mcp" => self.crud_store.find_mcp_plugin_owner(child).await?,
            "skill" => {
                self.crud_store
                    .find_skill_plugin_owner(&SkillId::new(child)?)
                    .await?
            }
            _ => anyhow::bail!("plugins.invalid_request"),
        };
        let Some(link) = link else {
            return Ok(None);
        };
        self.plugin_admit(context, request, workspace, true)
            .await
            .map_err(|_| anyhow::anyhow!("plugins.management_denied"))?;
        let parent = self
            .crud_store
            .find_plugin_installation(&link.plugin_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
        let mut guard = self
            .acquire_plugin_mutation(workspace, &parent.id, parent.revision)
            .await?;
        let interrupted = guard.parent.pending_json.clone();
        if let Some(pending) = interrupted.as_deref() {
            let value: serde_json::Value = serde_json::from_str(pending)?;
            anyhow::ensure!(
                value.get("kind").and_then(serde_json::Value::as_str) == Some("native")
                    && value.get("action").and_then(serde_json::Value::as_str) == Some(action)
                    && value
                        .get("native_committed")
                        .and_then(serde_json::Value::as_bool)
                        == Some(false)
                    && value
                        .pointer("/children/0/id")
                        .and_then(serde_json::Value::as_str)
                        == Some(child),
                "plugins.interrupted"
            );
            // Clean the previous uncertain fresh refs before replacing its plan.
            // A successful native writer would have atomically changed the marker.
            self.stop_plugin_execution(&guard, deadline).await?;
            self.cleanup_pending_refs(&guard).await?;
        } else {
            anyhow::ensure!(guard.parent.state == "installed", "plugins.interrupted");
        }
        // The first ownership read precedes parent admission. Reload under the
        // mutex so a completed competing edit cannot lose its override fields.
        let current = match kind {
            "mcp" => self.crud_store.find_mcp_plugin_owner(child).await?,
            _ => {
                self.crud_store
                    .find_skill_plugin_owner(&SkillId::new(child)?)
                    .await?
            }
        };
        let link = current
            .filter(|current| {
                current.plugin_id == guard.parent.id && current.member_key == link.member_key
            })
            .ok_or_else(|| anyhow::anyhow!("plugins.stale"))?;
        let mut overrides: std::collections::BTreeSet<String> =
            serde_json::from_str(&link.override_fields_json)?;
        overrides.extend(fields.iter().map(|field| field.to_string()));
        let mut pending = json!({"kind":"native", "action":action,
            "children":[{"kind":kind,"key":link.member_key,"id":child}],
            "native_committed":false});
        let before = serde_json::to_string(&pending)?;
        guard.parent = if let Some(previous) = interrupted.as_deref() {
            self.crud_store
                .resume_plugin_mutation(
                    workspace,
                    &parent.id,
                    parent.revision,
                    previous,
                    &before,
                    "updating",
                )
                .await?
        } else {
            self.crud_store
                .begin_plugin_mutation(
                    workspace,
                    &parent.id,
                    parent.revision,
                    "updating",
                    parent.enabled,
                    &before,
                )
                .await?
        };
        self.publish_plugin_changed(&guard.parent).await;
        if !matches!(
            tokio::time::timeout_at(deadline, self.stop_plugin_execution(&guard, deadline)).await,
            Ok(Ok(()))
        ) {
            self.crud_store
                .interrupt_plugin_mutation(
                    &parent.id,
                    guard.parent.revision,
                    &before,
                    "plugins.stop_unconfirmed",
                )
                .await?;
            self.publish_plugin_changed(&guard.parent).await;
            anyhow::bail!("plugins.stop_unconfirmed");
        }
        pending["native_committed"] = json!(true);
        let write = pioneer_crud::PluginNativeWrite {
            plugin_id: guard.parent.id.clone(),
            expected_revision: guard.parent.revision,
            member_key: link.member_key,
            child_id: child.into(),
            override_fields_json: serde_json::to_string(&overrides)?,
            pending_after: serde_json::to_string(&pending)?,
        };
        Ok(Some(NativePluginMutation {
            guard,
            write,
            deadline,
        }))
    }
    pub(super) async fn finish_native_plugin_change(
        &self,
        change: NativePluginMutation,
    ) -> anyhow::Result<()> {
        let parent = &change.guard.parent;
        let result = tokio::time::timeout_at(change.deadline, async {
            self.cleanup_pending_refs(&change.guard).await?;
            self.crud_store
                .prepare_plugin_reload(&parent.id, parent.revision)
                .await?;
            self.mcp_service
                .reload_after_plugin_change(&change.guard)
                .await?;
            self.crud_store
                .finish_plugin_mutation(&parent.id, parent.revision, "installed", None)
                .await
        })
        .await;
        if !matches!(result, Ok(Ok(()))) {
            // Read the current DB outcome, including its atomic native commit
            // marker. Do not replace it with pre-commit pending after timeout.
            let current = self
                .crud_store
                .find_plugin_installation(&parent.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
            if current.revision == parent.revision
                && current.state == "installed"
                && current.pending_json.is_none()
            {
                // Deadline can expire just after the final DB commit. That
                // commit only follows confirmed reload under this same guard.
                self.publish_plugin_changed(&current).await;
                return Ok(());
            }
            let pending = current
                .pending_json
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("plugins.outcome_unconfirmed"))?;
            let _ = pending;
            self.crud_store
                .finish_plugin_mutation(
                    &parent.id,
                    parent.revision,
                    "interrupted",
                    Some("plugins.reload_unconfirmed".into()),
                )
                .await?;
            self.publish_plugin_changed(&current).await;
            anyhow::bail!("plugins.interrupted");
        }
        self.publish_plugin_changed(parent).await;
        Ok(())
    }
}

pub(super) fn native_plugin_error_code(error: &anyhow::Error) -> &'static str {
    match error.to_string().as_str() {
        "plugins.busy" => "plugins.busy",
        "plugins.stale" => "plugins.stale",
        "plugins.not_found" => "plugins.not_found",
        "plugins.interrupted" => "plugins.interrupted",
        "plugins.stop_unconfirmed" => "plugins.stop_unconfirmed",
        _ => "plugins.component_change_failed",
    }
}
impl MessageProcessor {
    pub(super) async fn interrupt_native_plugin_change(
        &self,
        change: &NativePluginMutation,
        code: &str,
    ) -> anyhow::Result<()> {
        let parent = self
            .crud_store
            .find_plugin_installation(&change.guard.parent.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
        if let Some(pending) = &parent.pending_json {
            let _ = pending;
            self.crud_store
                .finish_plugin_mutation(
                    &parent.id,
                    change.write.expected_revision,
                    "interrupted",
                    Some(code.into()),
                )
                .await?;
            self.publish_plugin_changed(&parent).await;
        }
        Ok(())
    }
}

impl MessageProcessor {
    pub(super) async fn acquire_plugin_child_admission(
        &self,
        workspace: &str,
        kind: &str,
        child: &str,
    ) -> anyhow::Result<Option<PluginMutationGuard>> {
        let link = match kind {
            "mcp" => self.crud_store.find_mcp_plugin_owner(child).await?,
            "skill" => {
                self.crud_store
                    .find_skill_plugin_owner(&SkillId::new(child)?)
                    .await?
            }
            _ => anyhow::bail!("plugins.invalid_request"),
        };
        let Some(link) = link else {
            return Ok(None);
        };
        let parent = self
            .crud_store
            .find_plugin_installation(&link.plugin_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
        let guard = self
            .acquire_plugin_mutation(workspace, &parent.id, parent.revision)
            .await?;
        anyhow::ensure!(
            guard.parent.state == "installed" && guard.parent.pending_json.is_none(),
            "plugins.interrupted"
        );
        let current = match kind {
            "mcp" => self.crud_store.find_mcp_plugin_owner(child).await?,
            _ => {
                self.crud_store
                    .find_skill_plugin_owner(&SkillId::new(child)?)
                    .await?
            }
        };
        anyhow::ensure!(
            current.is_some_and(|current| current.plugin_id == guard.parent.id
                && current.member_key == link.member_key),
            "plugins.stale"
        );
        Ok(Some(guard))
    }
}

pub(crate) type PluginMutationLocks =
    Arc<Mutex<HashMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>>;

async fn plugin_mutex(gates: &PluginMutationLocks, id: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = gates.lock().await;
    locks.retain(|_, gate| gate.strong_count() != 0);
    if let Some(lock) = locks.get(id).and_then(std::sync::Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(id.into(), Arc::downgrade(&lock));
    lock
}

/// Shared Gateway admission seam for ordinary API starts and native recovery.
/// Callers retain the returned mutex guards through actual publication ACK.
/// No database permit is retained; recovery passes its Maintenance store.
pub(crate) async fn acquire_plugin_launch_admission(
    store: &pioneer_crud::CrudStore,
    gates: &PluginMutationLocks,
    workspace: &str,
    turn: &str,
) -> anyhow::Result<Vec<tokio::sync::OwnedMutexGuard<()>>> {
    let Some(snapshot) = store.get_plugin_selection(turn).await? else {
        return Ok(Vec::new());
    };
    let guards = acquire_plugin_selection_admission(store, gates, workspace, &snapshot).await?;
    anyhow::ensure!(
        store.get_plugin_selection(turn).await?.as_ref() == Some(&snapshot),
        "plugins.selection_changed"
    );
    Ok(guards)
}

/// The same parent locks also cover CLI preparation before its Turn exists.
/// The snapshot is built by the Gateway normalizer, never by public input.
pub(super) async fn acquire_plugin_selection_admission(
    store: &pioneer_crud::CrudStore,
    gates: &PluginMutationLocks,
    workspace: &str,
    snapshot: &pioneer_protocol::PluginSelectionSnapshot,
) -> anyhow::Result<Vec<tokio::sync::OwnedMutexGuard<()>>> {
    let mut parents = snapshot.parents.clone();
    parents.sort_by(|a, b| a.id.cmp(&b.id));
    let mut guards = Vec::with_capacity(parents.len());
    for selected in parents {
        let guard = plugin_mutex(gates, &selected.id)
            .await
            .try_lock_owned()
            .map_err(|_| anyhow::anyhow!("plugins.busy"))?;
        let parent = store
            .find_plugin_installation(&selected.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
        anyhow::ensure!(
            parent.workspace_id == workspace
                && parent.enabled
                && parent.state == "installed"
                && parent.pending_json.is_none()
                && parent.revision == selected.revision,
            "plugins.stale_or_disabled"
        );
        guards.push(guard);
    }
    Ok(guards)
}

/// Validate the existing committed snapshot and late native admission, without
/// resolving a historical parent into a silently newer revision.
pub(super) async fn validate_cli_plugin_selection(
    store: &pioneer_crud::CrudStore,
    workspace: &str,
    turn: &str,
) -> anyhow::Result<Option<pioneer_protocol::PluginSelectionSnapshot>> {
    let Some(snapshot) = store.get_plugin_selection(turn).await? else {
        // Missing metadata is not standalone authority for frozen owned leaves
        // (including a crash before prepared publication).
        for binding in store.list_turn_skill_bindings(turn).await? {
            anyhow::ensure!(
                store
                    .find_skill_plugin_owner(&binding.skill_id)
                    .await?
                    .is_none(),
                "plugins.selection_missing_for_owned_skill"
            );
        }
        let installations = store
            .list_turn_mcp_bindings(turn)
            .await?
            .into_iter()
            .map(|binding| binding.server_installation_id)
            .collect::<std::collections::BTreeSet<_>>();
        for id in installations {
            anyhow::ensure!(
                store.find_mcp_plugin_owner(&id).await?.is_none(),
                "plugins.selection_missing_for_owned_mcp"
            );
        }
        return Ok(None);
    };
    anyhow::ensure!(snapshot.phase == "ready", "plugins.selection_not_ready");
    for selected in &snapshot.parents {
        let parent = store
            .find_plugin_installation(&selected.id)
            .await?
            .context("plugin parent missing")?;
        anyhow::ensure!(
            parent.workspace_id == workspace
                && parent.revision == selected.revision
                && parent.enabled
                && parent.state == "installed"
                && parent.pending_json.is_none(),
            "plugins.stale_or_disabled"
        );
    }
    for child in &snapshot.children {
        anyhow::ensure!(
            store
                .plugin_turn_child_available(turn, &child.kind, &child.id, workspace)
                .await?,
            "plugins.child_unavailable"
        );
    }
    // The frozen native projection may not introduce an owned component that
    // is absent from this ready selection, including after interrupted writes.
    for binding in store.list_turn_skill_bindings(turn).await? {
        if store
            .find_skill_plugin_owner(&binding.skill_id)
            .await?
            .is_some()
        {
            anyhow::ensure!(
                snapshot
                    .children
                    .iter()
                    .any(|child| child.kind == "skill" && child.id == binding.skill_id.as_str()),
                "plugins.owned_skill_outside_selection"
            );
        }
    }
    for binding in store.list_turn_mcp_bindings(turn).await? {
        if store
            .find_mcp_plugin_owner(&binding.server_installation_id)
            .await?
            .is_some()
        {
            anyhow::ensure!(
                snapshot
                    .children
                    .iter()
                    .any(|child| child.kind == "mcp" && child.id == binding.server_installation_id),
                "plugins.owned_mcp_outside_selection"
            );
        }
    }
    Ok(Some(snapshot))
}
