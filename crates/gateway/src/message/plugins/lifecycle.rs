use super::*;

#[derive(Serialize, Deserialize, Default)]
struct PackageAction {
    #[serde(default)]
    kind: String,
    children: Vec<ReservedChild>,
    #[serde(default)]
    target_fingerprint: String,
    #[serde(default)]
    published_fingerprint: String,
    #[serde(default)]
    denied: Vec<PluginComponentKey>,
    #[serde(default)]
    restore_removed: bool,
    #[serde(default)]
    purge_data: bool,
    #[serde(default)]
    cleanup_refs: Vec<String>,
    #[serde(default)]
    source_upload_id: Option<String>,
    #[serde(default)]
    staging_name: String,
    #[serde(default)]
    prior_fingerprint: String,
}
impl PackageAction {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.children.len() <= 256
                && self.denied.len() <= 256
                && matches!(
                    self.kind.as_str(),
                    "" | "install" | "update" | "retry" | "remove"
                )
                && matches!(
                    self.staging_name.as_str(),
                    "" | "package-next" | "package-repair"
                ),
            "plugins.pending_invalid"
        );
        let mut keys = std::collections::BTreeSet::new();
        for child in &self.children {
            anyhow::ensure!(
                matches!(child.kind.as_str(), "skill" | "mcp")
                    && !child.key.is_empty()
                    && keys.insert((&child.kind, &child.key))
                    && (self.kind == "remove" || !child.id.is_empty()),
                "plugins.pending_invalid"
            );
        }
        Ok(())
    }
}
fn package_payload_missing(package: &Path) -> bool {
    [package.to_path_buf(),package.with_file_name("package-backup")].iter()
        .all(|path|matches!(path.symlink_metadata(),Err(error) if error.kind()==std::io::ErrorKind::NotFound))
}
fn key(component: &ComponentPlan) -> PluginComponentKey {
    let component = component_preview(component);
    PluginComponentKey {
        kind: component.kind,
        member_key: component.member_key,
    }
}
fn matches_key(component: &PluginComponentKey, kind: &str, member: &str) -> bool {
    component.kind == kind && component.member_key == member
}
fn package_snapshot(root: &Path) -> anyhow::Result<(Snapshot, LoadedPluginPlan)> {
    let snapshot = Snapshot::capture(root, Default::default(), || false)
        .map_err(|_| anyhow::anyhow!("plugins.package_unavailable"))?;
    let plan = pioneer_plugins::load(&snapshot)
        .map_err(|_| anyhow::anyhow!("plugins.package_rejected"))?;
    Ok((snapshot, plan))
}
pub(super) fn denied_keys(snapshot: &Snapshot, plan: &LoadedPluginPlan) -> Vec<PluginComponentKey> {
    plan.components
        .iter()
        .filter_map(|component| match component {
            ComponentPlan::Skill { member_path, .. }
                if snapshot.entries().iter().any(|(path, entry)| {
                    matches!(entry, Entry::Denied)
                        && (path == member_path || path.starts_with(&format!("{member_path}/")))
                }) =>
            {
                Some(key(component))
            }
            _ => None,
        })
        .collect()
}
fn integrity_path(package: &Path) -> anyhow::Result<std::path::PathBuf> {
    Ok(package
        .parent()
        .ok_or_else(|| anyhow::anyhow!("plugins.package_unavailable"))?
        .join("package.integrity"))
}
pub(super) fn save_package_integrity(package: &Path, digest: &str) -> anyhow::Result<()> {
    let receipt = integrity_path(package)?;
    let next = receipt.with_extension("integrity-next");
    use std::io::Write;
    if next.symlink_metadata().is_ok() {
        std::fs::remove_file(&next)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&next)?;
    file.write_all(digest.as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(next, receipt)?;
    Ok(())
}
fn read_package_integrity(package: &Path) -> anyhow::Result<Option<String>> {
    let path = integrity_path(package)?;
    let metadata = match path.symlink_metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() == 64,
        "plugins.package_changed"
    );
    use std::io::Read;
    let mut bytes = Vec::with_capacity(65);
    std::fs::File::open(path)?
        .take(65)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() == 64 && bytes.iter().all(u8::is_ascii_hexdigit),
        "plugins.package_changed"
    );
    Ok(Some(String::from_utf8(bytes)?))
}
fn verified_package(
    parent: &pioneer_entity::plugin_installation::Model,
) -> anyhow::Result<(Snapshot, LoadedPluginPlan)> {
    let package = Path::new(&parent.package_path);
    let (snapshot, plan) = package_snapshot(package)?;
    let expected =
        read_package_integrity(package)?.unwrap_or_else(|| parent.package_fingerprint.clone());
    anyhow::ensure!(
        snapshot.tree_digest() == expected,
        "plugins.package_changed"
    );
    Ok((snapshot, plan))
}

impl MessageProcessor {
    pub(super) async fn preview_plugin_update(
        &self,
        context: &RequestContext,
        request: &RequestId,
        params: PluginsUpdatePreviewParams,
    ) -> Result<PluginsUpdatePreviewResponse, JsonRpcErrorResponse> {
        let guard = self
            .acquire_plugin_mutation(
                &params.workspace_id,
                &params.plugin_id,
                params.expected_revision,
            )
            .await
            .map_err(|error| plugin_error(request, native_plugin_error_code(&error)))?;
        if guard.parent.state == "removing" {
            return Err(plugin_error(request, "plugins.interrupted"));
        }
        let runtime = self
            .skills_runtime_context(&params.workspace_id)
            .map_err(|_| plugin_error(request, "plugins.workspace_unavailable"))?;
        let source = self
            .materialize_uploaded_archive_source(
                context.connection_id(),
                &params.workspace_id,
                &params.upload_id,
                &runtime,
                request,
            )
            .await?;
        if source.upload.purpose != "plugin" {
            let _ = std::fs::remove_dir_all(&source.cleanup_root);
            return Err(plugin_error(request, "plugins.upload_unavailable"));
        }
        let result = package_snapshot(&source.source_dir)
            .map_err(|_| plugin_error(request, "plugins.package_rejected"));
        let _ = std::fs::remove_dir_all(&source.cleanup_root);
        let (_, plan) = result?;
        let (mut removed, mut authorization_changes) = self
            .plugin_update_changes(&guard, &plan)
            .await
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
        let disclosed = self
            .plugin_item(context, &guard.parent)
            .await
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
        removed.retain(|key| {
            disclosed
                .components
                .iter()
                .any(|component| matches_key(key, &component.kind, &component.member_key))
        });
        authorization_changes.retain(|key| {
            disclosed
                .components
                .iter()
                .any(|component| matches_key(key, &component.kind, &component.member_key))
        });
        let existing = self
            .crud_store
            .list_plugin_components(&guard.parent.id)
            .await
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
        let target = plan.components.iter().map(key).collect::<Vec<_>>();
        let (added, updated) = target.into_iter().partition(|key| {
            !existing
                .iter()
                .any(|link| matches_key(key, &link.kind, &link.member_key))
        });
        let mut preview = PluginsUpdatePreviewResponse {
            identity_resets: existing
                .iter()
                .filter(|link| {
                    link.diagnostic.as_deref() == Some("component_removed_for_update")
                        && plan.components.iter().any(|component| {
                            matches_key(&key(component), &link.kind, &link.member_key)
                        })
                })
                .map(|link| PluginComponentKey {
                    kind: link.kind.clone(),
                    member_key: link.member_key.clone(),
                })
                .collect(),
            added,
            updated,
            package: PluginsPreviewResponse {
                name: plan.manifest.name.clone(),
                version: plan.manifest.version.clone(),
                fingerprint: plan.tree_digest.clone(),
                components: plan.components.iter().map(component_preview).collect(),
                diagnostics: public_diagnostics(&plan),
            },
            removed,
            authorization_changes,
        };
        let administrative =
            crate::authorization::AuthorizationService::new().role_disclosure_policy(
                context.principal().kind,
                context.principal().role_key.as_ref(),
            ) == Some(crate::authorization::RoleDisclosurePolicy::Administrative);
        let existing = existing
            .iter()
            .map(|link| PluginComponentKey {
                kind: link.kind.clone(),
                member_key: link.member_key.clone(),
            })
            .collect::<Vec<_>>();
        let visible = disclosed
            .components
            .iter()
            .map(|component| PluginComponentKey {
                kind: component.kind.clone(),
                member_key: component.member_key.clone(),
            })
            .collect::<Vec<_>>();
        disclose_update_preview(&mut preview, &existing, &visible, administrative);
        Ok(preview)
    }
    async fn plugin_update_changes(
        &self,
        guard: &PluginMutationGuard,
        plan: &LoadedPluginPlan,
    ) -> anyhow::Result<(Vec<PluginComponentKey>, Vec<PluginComponentKey>)> {
        let links = self
            .crud_store
            .list_plugin_components(&guard.parent.id)
            .await?;
        let target = plan.components.iter().map(key).collect::<Vec<_>>();
        let removed = links
            .iter()
            .filter(|link| {
                !target
                    .iter()
                    .any(|key| matches_key(key, &link.kind, &link.member_key))
            })
            .map(|link| PluginComponentKey {
                kind: link.kind.clone(),
                member_key: link.member_key.clone(),
            })
            .collect();
        if guard.parent.pending_json.is_some()
            && package_payload_missing(Path::new(&guard.parent.package_path))
        {
            let authorization_changes = links
                .iter()
                .filter(|link| {
                    link.kind == "mcp"
                        && link.mcp_installation_id.is_some()
                        && target
                            .iter()
                            .any(|key| matches_key(key, &link.kind, &link.member_key))
                })
                .map(|link| PluginComponentKey {
                    kind: link.kind.clone(),
                    member_key: link.member_key.clone(),
                })
                .collect();
            return Ok((removed, authorization_changes));
        }
        let (_, previous) = comparison_package(&guard.parent)?;
        let mut authorization_changes = Vec::new();
        for component in &plan.components {
            if let ComponentPlan::Mcp { member_key, server } = component {
                if previous
                    .components
                    .iter()
                    .find_map(|previous| match previous {
                        ComponentPlan::Mcp {
                            member_key: old,
                            server,
                        } if old == member_key => Some(server),
                        _ => None,
                    })
                    .is_some_and(|old| {
                        serde_json::to_value(old).ok() != serde_json::to_value(server).ok()
                    })
                {
                    authorization_changes.push(key(component));
                }
            }
        }
        Ok((removed, authorization_changes))
    }
    pub(super) async fn mutate_plugin(
        &self,
        context: &RequestContext,
        request: &RequestId,
        params: PluginsMutateParams,
        deadline: tokio::time::Instant,
    ) -> Result<PluginsMutationResponse, JsonRpcErrorResponse> {
        let current = self
            .crud_store
            .find_plugin_installation(&params.plugin_id)
            .await
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
        if current
            .as_ref()
            .is_none_or(|parent| parent.workspace_id != params.workspace_id)
            && matches!(&params.intent, PluginManagementIntent::Remove { .. })
        {
            return Ok(PluginsMutationResponse {
                plugin: None,
                removed: true,
            });
        }
        if let (
            Some(parent),
            PluginManagementIntent::Update {
                upload_id,
                expected_fingerprint,
                ..
            },
        ) = (&current, &params.intent)
        {
            if parent.workspace_id == params.workspace_id
                && params.expected_revision > 0
                && params.expected_revision <= parent.revision
            {
                let pending: Option<serde_json::Value> = parent
                    .pending_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|_| plugin_error(request, "plugins.pending_invalid"))?;
                let same_pending = pending.as_ref().is_some_and(|pending| {
                    pending.get("kind").and_then(serde_json::Value::as_str) == Some("update")
                        && pending
                            .get("source_upload_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(upload_id.as_str())
                        && pending
                            .get("target_fingerprint")
                            .and_then(serde_json::Value::as_str)
                            == Some(expected_fingerprint.as_str())
                });
                if same_pending
                    || (parent.source_upload_id == *upload_id
                        && parent.package_fingerprint == *expected_fingerprint
                        && parent.pending_json.is_none())
                {
                    return Ok(PluginsMutationResponse {
                        plugin: Some(
                            self.plugin_item(context, parent)
                                .await
                                .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?,
                        ),
                        removed: false,
                    });
                }
            }
        }
        let mut guard = self
            .acquire_plugin_mutation(
                &params.workspace_id,
                &params.plugin_id,
                params.expected_revision,
            )
            .await
            .map_err(|error| plugin_error(request, native_plugin_error_code(&error)))?;
        let before = guard.parent.pending_json.clone();
        let action = match params.intent {
            PluginManagementIntent::Update {
                upload_id,
                expected_fingerprint,
                confirm_changes,
            } => {
                let previous_action = before
                    .as_deref()
                    .map(serde_json::from_str::<PackageAction>)
                    .transpose()
                    .map_err(|_| plugin_error(request, "plugins.reapply_native_change"))?;
                if previous_action.as_ref().is_some_and(|action| {
                    !matches!(action.kind.as_str(), "" | "install" | "update" | "retry")
                }) || guard.parent.state == "removing"
                {
                    return Err(plugin_error(request, "plugins.use_continue_or_remove"));
                }
                let prior = if before.is_some()
                    && package_payload_missing(Path::new(&guard.parent.package_path))
                {
                    None
                } else {
                    Some(
                        comparison_package(&guard.parent)
                            .map_err(|_| plugin_error(request, "plugins.package_changed"))?
                            .0,
                    )
                };
                let runtime = self
                    .skills_runtime_context(&params.workspace_id)
                    .map_err(|_| plugin_error(request, "plugins.workspace_unavailable"))?;
                let source = self
                    .materialize_uploaded_archive_source(
                        context.connection_id(),
                        &params.workspace_id,
                        &upload_id,
                        &runtime,
                        request,
                    )
                    .await?;
                if source.upload.purpose != "plugin" {
                    let _ = std::fs::remove_dir_all(&source.cleanup_root);
                    return Err(plugin_error(request, "plugins.upload_unavailable"));
                }
                let loaded = package_snapshot(&source.source_dir);
                let (snapshot, plan) = match loaded {
                    Ok(loaded) => loaded,
                    Err(_) => {
                        let _ = std::fs::remove_dir_all(&source.cleanup_root);
                        return Err(plugin_error(request, "plugins.package_rejected"));
                    }
                };
                if plan.tree_digest != expected_fingerprint {
                    let _ = std::fs::remove_dir_all(&source.cleanup_root);
                    return Err(plugin_error(request, "plugins.fingerprint_changed"));
                }
                let (removed, auth) = self
                    .plugin_update_changes(&guard, &plan)
                    .await
                    .map_err(|_| plugin_error(request, "plugins.package_changed"))?;
                let links = self
                    .crud_store
                    .list_plugin_components(&guard.parent.id)
                    .await
                    .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
                let identity_resets = links.iter().any(|link| {
                    link.diagnostic.as_deref() == Some("component_removed_for_update")
                        && plan.components.iter().any(|component| {
                            matches_key(&key(component), &link.kind, &link.member_key)
                        })
                });
                if !confirm_changes && (!removed.is_empty() || !auth.is_empty() || identity_resets)
                {
                    let _ = std::fs::remove_dir_all(&source.cleanup_root);
                    return Err(plugin_error(request, "plugins.confirm_changes"));
                }
                let children = plan
                    .components
                    .iter()
                    .map(|component| {
                        let component = key(component);
                        let id = links
                            .iter()
                            .find(|link| {
                                link.kind == component.kind
                                    && link.member_key == component.member_key
                            })
                            .and_then(|link| {
                                if component.kind == "skill" {
                                    link.skill_id.clone()
                                } else {
                                    link.mcp_installation_id.clone()
                                }
                            })
                            .unwrap_or_else(|| pioneer_protocol::generate_id(21));
                        ReservedChild {
                            kind: component.kind,
                            key: component.member_key,
                            id,
                        }
                    })
                    .collect();
                let package = Path::new(&guard.parent.package_path);
                let staging_name = if previous_action
                    .as_ref()
                    .is_some_and(|action| action.staging_name == "package-repair")
                {
                    "package-next"
                } else if previous_action.is_some() {
                    "package-repair"
                } else {
                    "package-next"
                };
                let next = package.with_file_name(staging_name);
                if next.exists() {
                    std::fs::remove_dir_all(&next)
                        .map_err(|_| plugin_error(request, "plugins.staging_cleanup_failed"))?;
                }
                publish_package(&snapshot, &next)
                    .map_err(|_| plugin_error(request, "plugins.staging_failed"))?;
                let published = Snapshot::capture(&next, Default::default(), || false)
                    .map_err(|_| plugin_error(request, "plugins.staging_failed"))?
                    .tree_digest();
                let action = PackageAction {
                    kind: "update".into(),
                    source_upload_id: Some(upload_id.clone()),
                    staging_name: staging_name.into(),
                    prior_fingerprint: prior
                        .as_ref()
                        .map(Snapshot::tree_digest)
                        .unwrap_or_default(),
                    cleanup_refs: previous_action
                        .as_ref()
                        .map(|action| action.cleanup_refs.clone())
                        .unwrap_or_default(),
                    children,
                    target_fingerprint: plan.tree_digest.clone(),
                    published_fingerprint: published,
                    denied: denied_keys(&snapshot, &plan),
                    ..Default::default()
                };
                let pending = serde_json::to_string(&action)
                    .map_err(|_| plugin_error(request, "plugins.plan_too_large"))?;
                let _upload_lock = self.acquire_skill_upload_lock(&upload_id).await;
                let begin = self
                    .crud_store
                    .begin_plugin_package_update(
                        &params.workspace_id,
                        &params.plugin_id,
                        params.expected_revision,
                        &pending,
                        &upload_id,
                        context.connection_id(),
                        now_timestamp_secs(),
                        before.as_deref(),
                    )
                    .await;
                match begin {
                    Ok(parent) => {
                        guard.parent = parent;
                        self.cleanup_upload_artifacts(&source.upload, &source.cleanup_root);
                    }
                    Err(_) => {
                        // The writer may have committed before cancellation. Retain
                        // staged files unless a reread proves that no plan owns them.
                        if self
                            .crud_store
                            .find_plugin_installation(&params.plugin_id)
                            .await
                            .ok()
                            .flatten()
                            .is_some_and(|parent| parent.pending_json.is_none())
                        {
                            let _ = std::fs::remove_dir_all(&next);
                        }
                        let _ = std::fs::remove_dir_all(&source.cleanup_root);
                        return Err(plugin_error(request, "plugins.stale_or_upload_unavailable"));
                    }
                }
                action
            }
            PluginManagementIntent::Retry {
                mut components,
                restore_removed,
            } => {
                if before.is_some() {
                    return Err(plugin_error(request, "plugins.use_continue"));
                }
                if components.len() > 256 {
                    return Err(plugin_error(request, "plugins.invalid_request"));
                }
                let links = self
                    .crud_store
                    .list_plugin_components(&guard.parent.id)
                    .await
                    .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
                if components.is_empty() {
                    if restore_removed {
                        return Err(plugin_error(request, "plugins.explicit_restore_required"));
                    }
                    components = links
                        .iter()
                        .filter(|link| link.status == "failed")
                        .map(|link| PluginComponentKey {
                            kind: link.kind.clone(),
                            member_key: link.member_key.clone(),
                        })
                        .collect();
                    if components.is_empty() {
                        return Err(plugin_error(request, "plugins.no_failed_components"));
                    }
                }
                let mut unique = std::collections::BTreeSet::new();
                let mut children = Vec::new();
                for component in components {
                    if !unique.insert((component.kind.clone(), component.member_key.clone())) {
                        return Err(plugin_error(request, "plugins.invalid_request"));
                    }
                    let link = links
                        .iter()
                        .find(|link| matches_key(&component, &link.kind, &link.member_key))
                        .filter(|link| {
                            link.status == "failed"
                                || (restore_removed && link.status == "removed_by_user")
                        })
                        .ok_or_else(|| plugin_error(request, "plugins.component_not_retryable"))?;
                    if link.diagnostic.as_deref() == Some("component_path_denied") {
                        return Err(plugin_error(request, "plugins.fresh_package_required"));
                    }
                    children.push(ReservedChild {
                        kind: component.kind,
                        key: component.member_key,
                        id: link
                            .skill_id
                            .clone()
                            .or(link.mcp_installation_id.clone())
                            .unwrap_or_else(|| pioneer_protocol::generate_id(21)),
                    });
                }
                let (_, plan) = verified_package(&guard.parent)
                    .map_err(|_| plugin_error(request, "plugins.package_changed"))?;
                let action = PackageAction {
                    kind: "retry".into(),
                    children,
                    target_fingerprint: guard.parent.package_fingerprint.clone(),
                    published_fingerprint: plan.tree_digest.clone(),
                    restore_removed,
                    ..Default::default()
                };
                let pending = serde_json::to_string(&action)
                    .map_err(|_| plugin_error(request, "plugins.plan_too_large"))?;
                guard.parent = self
                    .crud_store
                    .begin_plugin_mutation(
                        &params.workspace_id,
                        &params.plugin_id,
                        params.expected_revision,
                        "updating",
                        guard.parent.enabled,
                        &pending,
                    )
                    .await
                    .map_err(|_| plugin_error(request, "plugins.stale"))?;
                action
            }
            PluginManagementIntent::Continue => {
                let pending = before
                    .as_deref()
                    .ok_or_else(|| plugin_error(request, "plugins.no_pending_action"))?;
                let value: serde_json::Value = serde_json::from_str(pending)
                    .map_err(|_| plugin_error(request, "plugins.pending_invalid"))?;
                if matches!(
                    value.get("kind").and_then(serde_json::Value::as_str),
                    Some("native" | "set_enabled")
                ) {
                    return self
                        .continue_native_plugin_action(context, request, guard, value, deadline)
                        .await;
                }
                let mut action: PackageAction = serde_json::from_str(pending)
                    .map_err(|_| plugin_error(request, "plugins.pending_invalid"))?;
                if action.kind.is_empty() {
                    action.kind = "install".into();
                    action.target_fingerprint = guard.parent.package_fingerprint.clone();
                }
                action
                    .validate()
                    .map_err(|_| plugin_error(request, "plugins.pending_invalid"))?;
                guard.parent = self
                    .crud_store
                    .resume_plugin_mutation(
                        &params.workspace_id,
                        &params.plugin_id,
                        params.expected_revision,
                        pending,
                        &serde_json::to_string(&action).unwrap(),
                        if action.kind == "remove" {
                            "removing"
                        } else {
                            "updating"
                        },
                    )
                    .await
                    .map_err(|_| plugin_error(request, "plugins.stale"))?;
                action
            }
            PluginManagementIntent::Remove { purge_data } => {
                let links = self
                    .crud_store
                    .list_plugin_components(&guard.parent.id)
                    .await
                    .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
                let children = links
                    .iter()
                    .map(|link| ReservedChild {
                        kind: link.kind.clone(),
                        key: link.member_key.clone(),
                        id: link
                            .skill_id
                            .clone()
                            .or(link.mcp_installation_id.clone())
                            .unwrap_or_default(),
                    })
                    .collect();
                let action = PackageAction {
                    kind: "remove".into(),
                    children,
                    purge_data,
                    ..Default::default()
                };
                let mut value = serde_json::to_value(&action).unwrap();
                if let Some(before) = before.as_deref() {
                    let old: serde_json::Value = serde_json::from_str(before)
                        .map_err(|_| plugin_error(request, "plugins.pending_invalid"))?;
                    // Retain uncertain credential cleanup when converting an
                    // interrupted edit to Remove. Never keep previous plans.
                    if let Some(refs) = old.get("cleanup_refs") {
                        value["cleanup_refs"] = refs.clone();
                    }
                    let pending = serde_json::to_string(&value).unwrap();
                    guard.parent = self
                        .crud_store
                        .resume_plugin_mutation(
                            &params.workspace_id,
                            &params.plugin_id,
                            params.expected_revision,
                            before,
                            &pending,
                            "removing",
                        )
                        .await
                        .map_err(|_| plugin_error(request, "plugins.stale"))?;
                } else {
                    guard.parent = self
                        .crud_store
                        .begin_plugin_mutation(
                            &params.workspace_id,
                            &params.plugin_id,
                            params.expected_revision,
                            "removing",
                            guard.parent.enabled,
                            &serde_json::to_string(&value).unwrap(),
                        )
                        .await
                        .map_err(|_| plugin_error(request, "plugins.stale"))?;
                }
                action
            }
        };
        action
            .validate()
            .map_err(|_| plugin_error(request, "plugins.pending_invalid"))?;
        self.publish_plugin_changed(&guard.parent).await;
        let result = tokio::time::timeout_at(deadline, async {
            self.stop_plugin_execution(&guard, deadline).await?;
            self.cleanup_pending_refs(&guard).await?;
            if action.kind == "remove" {
                let links = self
                    .crud_store
                    .list_plugin_components(&guard.parent.id)
                    .await?;
                for link in links {
                    self.remove_plugin_component(context, request, &guard, &link)
                        .await?;
                }
                // Only Gateway-owned package/staging paths are removed. Data is
                // retained by default and only this parent's host data is purged.
                let package = Path::new(&guard.parent.package_path);
                for path in [
                    package.to_path_buf(),
                    package.with_file_name("package-next"),
                    package.with_file_name("package-repair"),
                    package.with_file_name("package-backup"),
                ] {
                    if path.exists() {
                        std::fs::remove_dir_all(path)?;
                    }
                }
                let receipt = integrity_path(package)?;
                for path in [receipt.clone(), receipt.with_extension("integrity-next")] {
                    if path.symlink_metadata().is_ok() {
                        std::fs::remove_file(path)?;
                    }
                }
                if action.purge_data && Path::new(&guard.parent.data_path).exists() {
                    std::fs::remove_dir_all(&guard.parent.data_path)?;
                }
                self.crud_store
                    .delete_plugin_parent(&guard.parent.id, guard.parent.revision)
                    .await?;
                self.mcp_service
                    .reload_workspace(&guard.parent.workspace_id)
                    .await?;
                return Ok::<_, anyhow::Error>(true);
            }
            if action.kind == "update" {
                prepare_replacement_after_stop(Path::new(&guard.parent.package_path), &action)?;
            }
            let (snapshot, plan) = package_snapshot(Path::new(&guard.parent.package_path))?;
            if !action.published_fingerprint.is_empty() {
                anyhow::ensure!(
                    snapshot.tree_digest() == action.published_fingerprint,
                    "plugins.package_changed"
                );
            } else {
                anyhow::ensure!(
                    snapshot.tree_digest() == guard.parent.package_fingerprint,
                    "plugins.fresh_package_required"
                );
            }
            if action.kind == "update" {
                let target = plan.components.iter().map(key).collect::<Vec<_>>();
                for link in self
                    .crud_store
                    .list_plugin_components(&guard.parent.id)
                    .await?
                {
                    if !target
                        .iter()
                        .any(|key| matches_key(key, &link.kind, &link.member_key))
                    {
                        self.remove_plugin_component(context, request, &guard, &link)
                            .await?;
                        self.crud_store
                            .forget_plugin_component(
                                &guard.parent.id,
                                guard.parent.revision,
                                &link.kind,
                                &link.member_key,
                            )
                            .await?;
                    }
                }
                self.crud_store
                    .publish_plugin_package(
                        &guard.parent.id,
                        guard.parent.revision,
                        &plan.manifest.name,
                        plan.manifest.version.as_deref(),
                        &action.target_fingerprint,
                        action.source_upload_id.as_deref(),
                    )
                    .await?;
            }
            self.apply_plugin_components(context, request, &guard, &action, &plan)
                .await?;
            for name in ["package-next", "package-repair"] {
                let path = Path::new(&guard.parent.package_path).with_file_name(name);
                if path.exists() {
                    std::fs::remove_dir_all(path)?;
                }
            }
            save_package_integrity(
                Path::new(&guard.parent.package_path),
                &snapshot.tree_digest(),
            )?;
            let backup = Path::new(&guard.parent.package_path).with_file_name("package-backup");
            if backup.exists() {
                std::fs::remove_dir_all(backup)?;
            }
            self.crud_store
                .prepare_plugin_reload(&guard.parent.id, guard.parent.revision)
                .await?;
            self.mcp_service.reload_after_plugin_change(&guard).await?;
            self.crud_store
                .finish_plugin_mutation(
                    &guard.parent.id,
                    guard.parent.revision,
                    "installed",
                    Some(serde_json::to_string(&public_diagnostics(&plan))?),
                )
                .await?;
            Ok(false)
        })
        .await;
        if !matches!(result, Ok(Ok(_))) {
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
                            Some(
                                match &result {
                                    Ok(Err(error)) => match error.to_string().as_str() {
                                        "plugins.package_unavailable" => {
                                            "plugins.package_unavailable"
                                        }
                                        "plugins.package_changed" => "plugins.package_changed",
                                        "plugins.package_rejected" => "plugins.package_rejected",
                                        "plugins.fresh_package_required" => {
                                            "plugins.fresh_package_required"
                                        }
                                        "plugins.package_repair_required" => {
                                            "plugins.package_repair_required"
                                        }
                                        _ => "plugins.repair_required",
                                    },
                                    _ => "plugins.stop_or_reload_unconfirmed",
                                }
                                .into(),
                            ),
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
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?;
        self.publish_plugin_changed(parent.as_ref().unwrap_or(&guard.parent))
            .await;
        match parent {
            Some(parent) => Ok(PluginsMutationResponse {
                plugin: Some(
                    self.plugin_item(context, &parent)
                        .await
                        .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?,
                ),
                removed: false,
            }),
            None => Ok(PluginsMutationResponse {
                plugin: None,
                removed: true,
            }),
        }
    }
    async fn native_write_for_link(
        &self,
        guard: &PluginMutationGuard,
        link: &pioneer_entity::plugin_component::Model,
    ) -> anyhow::Result<pioneer_crud::PluginNativeWrite> {
        let parent = self
            .crud_store
            .find_plugin_installation(&guard.parent.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
        anyhow::ensure!(parent.revision == guard.parent.revision, "plugins.stale");
        Ok(pioneer_crud::PluginNativeWrite {
            plugin_id: parent.id,
            expected_revision: parent.revision,
            member_key: link.member_key.clone(),
            child_id: link
                .skill_id
                .clone()
                .or(link.mcp_installation_id.clone())
                .ok_or_else(|| anyhow::anyhow!("plugins.child_missing"))?,
            override_fields_json: link.override_fields_json.clone(),
            pending_after: parent
                .pending_json
                .ok_or_else(|| anyhow::anyhow!("plugins.pending_missing"))?,
        })
    }
    async fn remove_plugin_component(
        &self,
        context: &RequestContext,
        request: &RequestId,
        guard: &PluginMutationGuard,
        link: &pioneer_entity::plugin_component::Model,
    ) -> anyhow::Result<()> {
        if link.skill_id.is_none() && link.mcp_installation_id.is_none() {
            return Ok(());
        }
        let write = self.native_write_for_link(guard, link).await?;
        if let Some(id) = &link.skill_id {
            self.uninstall_skill_with_plugin_change(
                context,
                request.clone(),
                SkillsUninstallParams {
                    workspace_id: guard.parent.workspace_id.clone(),
                    skill_id: SkillId::new(id.clone())?,
                },
                Some(&write),
            )
            .await
            .map_err(|_| anyhow::anyhow!("plugins.component_remove_failed"))?;
        } else {
            self.uninstall_mcp_with_plugin_change(
                context,
                request.clone(),
                McpUninstallParams {
                    workspace_id: guard.parent.workspace_id.clone(),
                    name: mcp::portable::internal_mcp_name(&guard.parent.id, &link.member_key),
                    scope_kind: pioneer_protocol::McpScopeKind::Workspace,
                },
                Some(&write),
            )
            .await
            .map_err(|_| anyhow::anyhow!("plugins.component_remove_failed"))?;
        }
        Ok(())
    }
    async fn apply_plugin_components(
        &self,
        context: &RequestContext,
        request: &RequestId,
        guard: &PluginMutationGuard,
        action: &PackageAction,
        plan: &LoadedPluginPlan,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(action.children.len() <= 256, "plugins.pending_invalid");
        for reserved in &action.children {
            let component = plan
                .components
                .iter()
                .find(|component| matches_key(&key(component), &reserved.kind, &reserved.key))
                .ok_or_else(|| anyhow::anyhow!("plugins.fresh_package_required"))?;
            let links = self
                .crud_store
                .list_plugin_components(&guard.parent.id)
                .await?;
            let current = links
                .iter()
                .find(|link| link.kind == reserved.kind && link.member_key == reserved.key);
            if current.is_some_and(|link| link.status == "removed_by_user") {
                if !action.restore_removed {
                    continue;
                }
                self.crud_store
                    .forget_plugin_component(
                        &guard.parent.id,
                        guard.parent.revision,
                        &reserved.kind,
                        &reserved.key,
                    )
                    .await?;
            }
            if current.is_some_and(|link| {
                link.diagnostic.as_deref() == Some("component_removed_for_update")
            }) {
                self.crud_store
                    .forget_plugin_component(
                        &guard.parent.id,
                        guard.parent.revision,
                        &reserved.kind,
                        &reserved.key,
                    )
                    .await?;
            }
            let mut ownership = PluginOwnershipWrite {
                plugin_id: guard.parent.id.clone(),
                expected_revision: guard.parent.revision,
                member_key: reserved.key.clone(),
                member_path: None,
                package_fingerprint: action.target_fingerprint.clone(),
                child_id: reserved.id.clone(),
            };
            if let ComponentPlan::Skill {
                member_path,
                tree_digest,
                ..
            } = component
            {
                ownership.member_path = Some(member_path.clone());
                ownership.package_fingerprint = tree_digest.clone();
            }
            if action
                .denied
                .iter()
                .any(|key| matches_key(key, &reserved.kind, &reserved.key))
            {
                self.crud_store
                    .record_plugin_component_failure(
                        &ownership,
                        &reserved.kind,
                        "component_path_denied",
                    )
                    .await?;
                continue;
            }
            let masks: std::collections::BTreeSet<String> = current
                .filter(|link| link.skill_id.is_some() || link.mcp_installation_id.is_some())
                .map(|link| serde_json::from_str(&link.override_fields_json))
                .transpose()?
                .unwrap_or_default();
            if current.is_some_and(|link| {
                link.status == "installed"
                    && link.package_fingerprint.as_deref() == Some(&ownership.package_fingerprint)
            }) {
                continue; // Includes a previous native commit after interruption.
            }
            // A compatible native source override remains authoritative. Update
            // still validates/discovers the target package; it never overwrites
            // the user's installed source or trust settings with bundled files.
            if reserved.kind == "skill"
                && masks.contains("skill_source")
                && current.is_some_and(|link| link.skill_id.is_some())
            {
                continue;
            }
            let result = match component {
                ComponentPlan::Skill { .. } => {
                    if current.is_some_and(|link| link.skill_id.is_some()) {
                        self.update_skill_source(
                            context,
                            request.clone(),
                            skills::SkillUpdateInput {
                                workspace_id: guard.parent.workspace_id.clone(),
                                skill_id: SkillId::new(reserved.id.clone())?,
                                expected_previous_fingerprint: None,
                            },
                            skills::SkillInstallSource::PackageMember(ownership.clone()),
                        )
                        .await
                        .map(|_| ())
                        .map_err(|_| ())
                    } else {
                        self.install_skill_source(
                            context,
                            request.clone(),
                            guard.parent.workspace_id.clone(),
                            "user".into(),
                            skills::SkillInstallSource::PackageMember(ownership.clone()),
                        )
                        .await
                        .map(|_| ())
                        .map_err(|_| ())
                    }
                }
                ComponentPlan::Mcp { server, .. } => {
                    let plan = mcp::portable::portable_install_plan(
                        server,
                        Path::new(&guard.parent.package_path),
                        Path::new(&guard.parent.data_path),
                        &guard.parent.workspace_id,
                        &ownership,
                    )?;
                    self.install_mcp_plan(
                        context,
                        request.clone(),
                        &guard.parent.workspace_id,
                        plan,
                        Some(&ownership),
                        None,
                        true,
                    )
                    .await
                    .and_then(|response| {
                        if response.servers.len() == 1
                            && response.servers.iter().all(|server| {
                                matches!(
                                    server.status,
                                    McpInstallResultStatus::Installed
                                        | McpInstallResultStatus::Updated
                                )
                            })
                        {
                            Ok(())
                        } else {
                            Err(plugin_error(request, "plugins.component_install_failed"))
                        }
                    })
                    .map_err(|_| ())
                }
            };
            if result.is_err() {
                self.crud_store
                    .record_plugin_component_failure(
                        &ownership,
                        &reserved.kind,
                        "component_install_failed",
                    )
                    .await?;
            }
        }
        Ok(())
    }
    pub(super) async fn cleanup_pending_refs(
        &self,
        guard: &PluginMutationGuard,
    ) -> anyhow::Result<()> {
        let parent = self
            .crud_store
            .find_plugin_installation(&guard.parent.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("plugins.not_found"))?;
        let value: serde_json::Value = serde_json::from_str(
            parent
                .pending_json
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("plugins.pending_missing"))?,
        )?;
        let refs = value
            .get("cleanup_refs")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow::anyhow!("plugins.pending_invalid"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        if refs.is_empty() {
            return Ok(());
        }
        let mut active = std::collections::BTreeSet::new();
        for (kind, _, payload) in self.crud_store.plugin_credential_retention().await? {
            if kind == "native" {
                let refs: Vec<pioneer_mcp::McpSecretRef> = serde_json::from_str(&payload)?;
                active.extend(refs.into_iter().map(|reference| reference.ref_id));
            }
        }
        anyhow::ensure!(
            !refs.iter().any(|reference| active.contains(reference)),
            "plugins.cleanup_owner_changed"
        );
        let cleanup = self
            .gateway_secrets
            .delete_mcp_secrets(refs.iter().map(String::as_str));
        anyhow::ensure!(
            cleanup.failed.is_empty(),
            "plugins.credential_cleanup_failed"
        );
        Ok(())
    }
    async fn continue_native_plugin_action(
        &self,
        context: &RequestContext,
        request: &RequestId,
        mut guard: PluginMutationGuard,
        value: serde_json::Value,
        deadline: tokio::time::Instant,
    ) -> Result<PluginsMutationResponse, JsonRpcErrorResponse> {
        let before = guard.parent.pending_json.clone().unwrap();
        guard.parent = self
            .crud_store
            .resume_plugin_mutation(
                &guard.parent.workspace_id,
                &guard.parent.id,
                guard.parent.revision,
                &before,
                &before,
                "updating",
            )
            .await
            .map_err(|_| plugin_error(request, "plugins.stale"))?;
        let result = tokio::time::timeout_at(deadline, async {
            self.stop_plugin_execution(&guard, deadline).await?;
            self.cleanup_pending_refs(&guard).await?;
            let kind = value.get("kind").and_then(serde_json::Value::as_str);
            if kind == Some("native")
                && value
                    .get("native_committed")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
            {
                let action = value.get("action").and_then(serde_json::Value::as_str);
                if action == Some("remove") {
                    let children = value
                        .get("children")
                        .and_then(serde_json::Value::as_array)
                        .ok_or_else(|| anyhow::anyhow!("plugins.pending_invalid"))?;
                    anyhow::ensure!(children.len() == 1, "plugins.pending_invalid");
                    let links = self
                        .crud_store
                        .list_plugin_components(&guard.parent.id)
                        .await?;
                    let key = children[0]
                        .get("key")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let kind = children[0]
                        .get("kind")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let link = links
                        .iter()
                        .find(|link| link.kind == kind && link.member_key == key)
                        .ok_or_else(|| anyhow::anyhow!("plugins.child_missing"))?;
                    self.remove_plugin_component(context, request, &guard, link)
                        .await?;
                } else if action == Some("oauth_disconnect") {
                    let id = value
                        .pointer("/children/0/id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| anyhow::anyhow!("plugins.pending_invalid"))?;
                    let link = self
                        .crud_store
                        .find_mcp_plugin_owner(id)
                        .await?
                        .filter(|link| link.plugin_id == guard.parent.id)
                        .ok_or_else(|| anyhow::anyhow!("plugins.component_owner_changed"))?;
                    let _native = self
                        .mcp_service
                        .installation_lifecycle_guard(
                            "workspace",
                            &guard.parent.workspace_id,
                            &mcp::portable::internal_mcp_name(&guard.parent.id, &link.member_key),
                        )
                        .await;
                    self.mcp_service
                        .oauth()
                        .disconnect(id)
                        .await
                        .map_err(|_| anyhow::anyhow!("plugins.oauth_cleanup_failed"))?;
                    let mut write = self.native_write_for_link(&guard, &link).await?;
                    let mut outcome: serde_json::Value =
                        serde_json::from_str(&write.pending_after)?;
                    outcome["native_committed"] = json!(true);
                    write.pending_after = serde_json::to_string(&outcome)?;
                    self.crud_store
                        .complete_plugin_native_effect(&write, &guard.parent.workspace_id, "mcp")
                        .await?;
                } else {
                    anyhow::bail!("plugins.reapply_native_change");
                }
            }
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
        if !matches!(result, Ok(Ok(()))) {
            let current = self
                .crud_store
                .find_plugin_installation(&guard.parent.id)
                .await
                .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?
                .ok_or_else(|| plugin_error(request, "plugins.not_found"))?;
            if current.pending_json.is_some() {
                let code = if matches!(&result,Ok(Err(error)) if error.to_string()=="plugins.reapply_native_change")
                {
                    "plugins.reapply_native_change"
                } else {
                    "plugins.stop_or_reload_unconfirmed"
                };
                self.crud_store
                    .finish_plugin_mutation(
                        &current.id,
                        current.revision,
                        "interrupted",
                        Some(code.into()),
                    )
                    .await
                    .map_err(|_| plugin_error(request, "plugins.interrupted"))?;
            }
        }
        let parent = self
            .crud_store
            .find_plugin_installation(&guard.parent.id)
            .await
            .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?
            .ok_or_else(|| plugin_error(request, "plugins.not_found"))?;
        self.publish_plugin_changed(&parent).await;
        Ok(PluginsMutationResponse {
            plugin: Some(
                self.plugin_item(context, &parent)
                    .await
                    .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?,
            ),
            removed: false,
        })
    }
}

/// Caller has a confirmed execution stop and owns the parent mutex. The three
/// fixed host paths plus target digest identify crash boundaries; no file in the
/// used package is replaced before that ACK, including assets-only updates.
fn swap_package_after_stop(package: &Path, target: &str) -> anyhow::Result<()> {
    let next = package.with_file_name("package-next");
    let backup = package.with_file_name("package-backup");
    match (package.exists(), next.exists(), backup.exists()) {
        (true, true, false) => {
            std::fs::rename(package, &backup)?;
            std::fs::rename(&next, package)?;
        }
        (false, true, true) => {
            std::fs::rename(&next, package)?;
        }
        (true, false, true) | (true, false, false) => {}
        _ => anyhow::bail!("plugins.package_repair_required"),
    }
    let snapshot = Snapshot::capture(package, Default::default(), || false)?;
    anyhow::ensure!(snapshot.tree_digest() == target, "plugins.package_changed");
    Ok(())
}

fn disclose_update_preview(
    preview: &mut PluginsUpdatePreviewResponse,
    existing: &[PluginComponentKey],
    visible: &[PluginComponentKey],
    administrative: bool,
) {
    if administrative {
        return;
    }
    let hidden = existing
        .iter()
        .filter(|key| !visible.contains(key))
        .collect::<Vec<_>>();
    let allowed = |key: &PluginComponentKey| !hidden.contains(&key);
    preview.added.retain(allowed);
    preview.updated.retain(allowed);
    preview.identity_resets.retain(allowed);
    preview.removed.retain(allowed);
    preview.authorization_changes.retain(allowed);
    preview.package.components.retain(|component| {
        !hidden
            .iter()
            .any(|key| matches_key(key, &component.kind, &component.member_key))
    });
    if !hidden.is_empty() && !preview.package.diagnostics.is_empty() {
        preview.package.diagnostics = vec![PluginDiagnostic {
            code: "components_unavailable".into(),
            path: String::new(),
            message: "Some components are unavailable".into(),
        }];
    }
}

#[cfg(test)]
mod tests {
    // Regression sources only: NOT_RUN / NOT_COMPILED.
    use super::*;
    #[test]
    fn fresh_payload_repairs_a_lost_stage_and_survives_each_swap_boundary() {
        for crash_after_first_rename in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let package = root.path().join("package");
            let repair = root.path().join("package-repair");
            std::fs::create_dir(&package).unwrap();
            std::fs::write(package.join("asset"), "old").unwrap();
            let prior = Snapshot::capture(&package, Default::default(), || false)
                .unwrap()
                .tree_digest();
            std::fs::create_dir(&repair).unwrap();
            std::fs::write(repair.join("asset"), "fresh").unwrap();
            let target = Snapshot::capture(&repair, Default::default(), || false)
                .unwrap()
                .tree_digest();
            let action = PackageAction {
                kind: "update".into(),
                staging_name: "package-repair".into(),
                prior_fingerprint: prior,
                published_fingerprint: target,
                ..Default::default()
            };
            if crash_after_first_rename {
                std::fs::rename(&package, root.path().join("package-backup")).unwrap();
            }
            prepare_replacement_after_stop(&package, &action).unwrap();
            prepare_replacement_after_stop(&package, &action).unwrap();
            assert_eq!(
                std::fs::read_to_string(package.join("asset")).unwrap(),
                "fresh"
            );
            assert_eq!(
                std::fs::read_to_string(root.path().join("package-backup/asset")).unwrap(),
                "old"
            );
        }
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("package");
        let repair = root.path().join("package-repair");
        std::fs::create_dir(&repair).unwrap();
        std::fs::write(repair.join("asset"), "replacement").unwrap();
        let target = Snapshot::capture(&repair, Default::default(), || false)
            .unwrap()
            .tree_digest();
        let action = PackageAction {
            kind: "update".into(),
            staging_name: "package-repair".into(),
            published_fingerprint: target,
            ..Default::default()
        };
        assert!(package_payload_missing(&package));
        prepare_replacement_after_stop(&package, &action).unwrap();
        assert!(!package_payload_missing(&package));
        prepare_replacement_after_stop(&package, &action).unwrap();
    }
    #[test]
    fn integrity_metadata_rejects_oversized_values_and_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("package");
        assert_eq!(read_package_integrity(&package).unwrap(), None);
        let receipt = integrity_path(&package).unwrap();
        std::fs::write(&receipt, "a".repeat(65)).unwrap();
        assert!(read_package_integrity(&package).is_err());
        std::fs::write(&receipt, "z".repeat(64)).unwrap();
        assert!(read_package_integrity(&package).is_err());
        std::fs::remove_file(&receipt).unwrap();
        #[cfg(unix)]
        {
            let external = root.path().join("external");
            std::fs::write(&external, "a".repeat(64)).unwrap();
            std::os::unix::fs::symlink(external, receipt).unwrap();
            assert!(read_package_integrity(&package).is_err());
        }
    }
    #[test]
    fn assets_only_swap_resumes_a_crash_after_first_rename_and_checks_target() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("package");
        let next = root.path().join("package-next");
        for path in [&package, &next] {
            std::fs::create_dir(path).unwrap();
            std::fs::write(path.join("SKILL.md"), "unchanged markdown").unwrap();
        }
        std::fs::write(package.join("asset"), "before").unwrap();
        std::fs::write(next.join("asset"), "after").unwrap();
        let target = Snapshot::capture(&next, Default::default(), || false)
            .unwrap()
            .tree_digest();
        std::fs::rename(&package, root.path().join("package-backup")).unwrap();
        swap_package_after_stop(&package, &target).unwrap();
        assert_eq!(
            std::fs::read_to_string(package.join("SKILL.md")).unwrap(),
            "unchanged markdown"
        );
        assert_eq!(
            std::fs::read_to_string(package.join("asset")).unwrap(),
            "after"
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("package-backup/asset")).unwrap(),
            "before"
        );
        swap_package_after_stop(&package, &target).unwrap(); // Already swapped, no second rename.
        assert!(swap_package_after_stop(&package, "wrong-target").is_err());
    }
    #[test]
    fn private_integrity_hash_does_not_become_a_package_asset() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("package");
        std::fs::create_dir(&package).unwrap();
        std::fs::write(
            package.join("plugin.json"),
            serde_json::to_vec(
                &json!({"$schema":pioneer_plugins::PLUGIN_SCHEMA,"name":"fixture.tools"}),
            )
            .unwrap(),
        )
        .unwrap();
        let digest = Snapshot::capture(&package, Default::default(), || false)
            .unwrap()
            .tree_digest();
        save_package_integrity(&package, &digest).unwrap();
        assert_eq!(
            Snapshot::capture(&package, Default::default(), || false)
                .unwrap()
                .tree_digest(),
            digest
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("package.integrity")).unwrap(),
            digest
        );
    }
    #[test]
    fn update_preview_does_not_disclose_hidden_old_members_or_diagnostic_pointers() {
        let hidden = PluginComponentKey {
            kind: "skill".into(),
            member_key: "private-member".into(),
        };
        let visible = PluginComponentKey {
            kind: "mcp".into(),
            member_key: "public-member".into(),
        };
        let mut preview = PluginsUpdatePreviewResponse {
            package: PluginsPreviewResponse {
                name: "fixture.tools".into(),
                version: None,
                fingerprint: "tree".into(),
                components: vec![PluginComponentItem {
                    kind: hidden.kind.clone(),
                    member_key: hidden.member_key.clone(),
                    status: "discovered".into(),
                    diagnostic: None,
                    skill_id: None,
                    mcp_installation_id: None,
                    runtime_status: None,
                }],
                diagnostics: vec![PluginDiagnostic {
                    code: "path_denied".into(),
                    path: "skills/private-member/asset".into(),
                    message: "private-member".into(),
                }],
            },
            added: vec![],
            identity_resets: vec![],
            updated: vec![hidden.clone()],
            removed: vec![hidden.clone()],
            authorization_changes: vec![],
        };
        let mut admin = preview.clone();
        disclose_update_preview(
            &mut preview,
            &[hidden.clone(), visible.clone()],
            &[visible.clone()],
            false,
        );
        assert!(
            !serde_json::to_string(&preview)
                .unwrap()
                .contains("private-member")
        );
        disclose_update_preview(&mut admin, &[hidden, visible.clone()], &[visible], true);
        assert!(
            serde_json::to_string(&admin)
                .unwrap()
                .contains("private-member")
        );
    }
}

fn comparison_package(
    parent: &pioneer_entity::plugin_installation::Model,
) -> anyhow::Result<(Snapshot, LoadedPluginPlan)> {
    if parent.pending_json.is_none() {
        return verified_package(parent);
    }
    let value: serde_json::Value = serde_json::from_str(parent.pending_json.as_deref().unwrap())?;
    let package = Path::new(&parent.package_path);
    let candidate = if package.exists() {
        package.to_path_buf()
    } else {
        package.with_file_name("package-backup")
    };
    let (snapshot, plan) = package_snapshot(&candidate)?;
    let digest = snapshot.tree_digest();
    let receipt =
        read_package_integrity(package)?.unwrap_or_else(|| parent.package_fingerprint.clone());
    anyhow::ensure!(
        digest == receipt
            || value
                .get("published_fingerprint")
                .and_then(serde_json::Value::as_str)
                == Some(digest.as_str()),
        "plugins.package_changed"
    );
    Ok((snapshot, plan))
}
fn prepare_replacement_after_stop(package: &Path, action: &PackageAction) -> anyhow::Result<()> {
    let stage = match action.staging_name.as_str() {
        "" | "package-next" => "package-next",
        "package-repair" => "package-repair",
        _ => anyhow::bail!("plugins.pending_invalid"),
    };
    if stage == "package-next" && action.prior_fingerprint.is_empty() {
        return swap_package_after_stop(package, &action.published_fingerprint);
    }
    let next = package.with_file_name(stage);
    let backup = package.with_file_name("package-backup");
    if next.exists() && action.prior_fingerprint.is_empty() && package_payload_missing(package) {
        anyhow::ensure!(
            Snapshot::capture(&next, Default::default(), || false)?.tree_digest()
                == action.published_fingerprint,
            "plugins.package_changed"
        );
        std::fs::rename(&next, package)?;
        return Ok(());
    }
    if next.exists() {
        if !package.exists() && backup.exists() {
            anyhow::ensure!(
                Snapshot::capture(&backup, Default::default(), || false)?.tree_digest()
                    == action.prior_fingerprint,
                "plugins.package_changed"
            );
            std::fs::rename(&backup, package)?;
        }
        anyhow::ensure!(
            Snapshot::capture(package, Default::default(), || false)?.tree_digest()
                == action.prior_fingerprint,
            "plugins.package_changed"
        );
        // An explicit new payload supersedes the interrupted plan. Its obsolete
        // backup can be removed only after the same real execution stop proof.
        if backup.exists() {
            std::fs::remove_dir_all(&backup)?;
        }
        std::fs::rename(package, &backup)?;
        std::fs::rename(next, package)?;
    }
    anyhow::ensure!(
        Snapshot::capture(package, Default::default(), || false)?.tree_digest()
            == action.published_fingerprint,
        "plugins.package_changed"
    );
    Ok(())
}
