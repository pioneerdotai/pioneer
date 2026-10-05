//! Stage B: bounded delivery and sequential calls to the native installers.
use super::*;
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
}
#[derive(Serialize, Deserialize)]
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
        let id = &request.id;
        let params = request.params.unwrap_or_else(|| json!({}));
        let invalid = || plugin_error(id, "plugins.invalid_request");
        let workspace = params
            .get("workspace_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(invalid)?;
        let management = matches!(
            request.method.as_str(),
            methods::PLUGINS_PREVIEW | methods::PLUGINS_INSTALL
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
            methods::PLUGINS_PREVIEW | methods::PLUGINS_INSTALL => {
                let (upload_id, expected) = if request.method == methods::PLUGINS_INSTALL {
                    let params: PluginsInstallParams =
                        serde_json::from_value(params).map_err(|_| invalid())?;
                    (params.upload_id, Some(params.expected_fingerprint))
                } else {
                    let params: PluginsSourceParams =
                        serde_json::from_value(params).map_err(|_| invalid())?;
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
        };
        let pending_json = serde_json::to_string(&pending).map_err(|_| error())?;
        if pending_json.len() > 65536 {
            let _ = std::fs::remove_dir_all(&root);
            return Err(error());
        }
        let now = chrono::Utc::now().fixed_offset();
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
                        "component_install_failed",
                    )
                    .await
                    .map_err(|_| error())?;
            }
        }
        let diagnostics = serde_json::to_string(&public_diagnostics(&plan)).map_err(|_| error())?;
        self.crud_store
            .settle_plugin_installation(&plugin_id, 1, "installed", Some(diagnostics))
            .await
            .map_err(|_| error())?;
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
            .unwrap_or_default();
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
            state: parent.state.clone(),
            revision: parent.revision,
            status: if parent.state != "installed" {
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
