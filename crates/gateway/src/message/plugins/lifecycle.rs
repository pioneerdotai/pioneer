use super::*;

#[derive(Clone, Serialize, Deserialize, Default)]
struct PackageAction {
    #[serde(default)]
    kind: String,
    children: Vec<ReservedChild>,
    #[serde(default)]
    target_fingerprint: String,
    #[serde(default)]
    restore_removed: bool,
    #[serde(default)]
    purge_data: bool,
    #[serde(default)]
    cleanup_refs: Vec<String>,
    #[serde(default)]
    source_upload_id: Option<String>,
}
impl PackageAction {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(
                    self.kind.as_str(),
                    "" | "install" | "update" | "retry" | "remove" | "retained"
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
fn package_snapshot(root: &Path, limits: pioneer_plugins::Limits) -> anyhow::Result<(Snapshot, LoadedPluginPlan)> {
    package_snapshot_cancellable(root, &tokio_util::sync::CancellationToken::new(), limits)
}
fn package_snapshot_cancellable(
    root: &Path,
    cancellation: &tokio_util::sync::CancellationToken,
    limits: pioneer_plugins::Limits,
) -> anyhow::Result<(Snapshot, LoadedPluginPlan)> {
    let snapshot = Snapshot::capture(root, limits, || cancellation.is_cancelled())
        .map_err(|_| anyhow::anyhow!("plugins.package_unavailable"))?;
    let plan = pioneer_plugins::load(&snapshot)
        .map_err(|_| anyhow::anyhow!("plugins.package_rejected"))?;
    Ok((snapshot, plan))
}
pub(super) fn verified_package(
    parent: &pioneer_entity::plugin_installation::Model,
    cancellation: &tokio_util::sync::CancellationToken,
    limits: pioneer_plugins::Limits,
) -> anyhow::Result<(Snapshot, LoadedPluginPlan)> {
    let package = Path::new(&parent.package_path);
    let (snapshot, plan) = package_snapshot_cancellable(package, cancellation, limits)?;
    anyhow::ensure!(
        snapshot.tree_digest() == parent.package_fingerprint,
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
        if guard.parent.pending_json.is_some() || guard.parent.state == "removing" {
            return Err(plugin_error(request, "plugins.use_continue_or_remove"));
        }
        let owner = AuthenticatedTransferOwner::from_request_context(context);
        {
            let _upload_lock = self.acquire_skill_upload_lock(&params.upload_id).await;
            self.revalidate_plugin_upload(&owner, &params.workspace_id, &params.upload_id, request).await?;
        }
        let runtime = self.package_delivery_context();
        let source = self
            .materialize_uploaded_archive_source(
                &owner,
                &params.workspace_id,
                &params.upload_id,
                &runtime,
                request,
                        "plugin",
            )
            .await?;
        let (_source, candidate, plan) = load_uploaded_plugin(source)
            .await
            .map_err(|_| plugin_error(request, "plugins.package_rejected"))?;
        let (mut removed, mut authorization_changes, current_content) = self
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
        let added = target
            .into_iter()
            .filter(|key| {
                !existing
                    .iter()
                    .any(|link| matches_key(key, &link.kind, &link.member_key))
            })
            .collect();
        let candidate_plan = plan.clone();
        let (updated, supporting_files_changed) = guard
            .fs_prepare(move |_parent, _cancellation| {
                let (current, previous) = current_content;
                let (updated, supporting) =
                    package_content_changes(&current, &previous, &candidate, &candidate_plan);
                Ok((updated, supporting))
            })
            .await
            .map_err(|_| plugin_error(request, "plugins.package_changed"))?;
        let mut preview = PluginsUpdatePreviewResponse {
            supporting_files_changed,
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
                description: public_description(&plan),
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
        let _upload_lock = self.acquire_skill_upload_lock(&params.upload_id).await;
        self.revalidate_plugin_upload(&owner, &params.workspace_id, &params.upload_id, request).await?;
        Ok(preview)
    }
    async fn plugin_update_changes(
        &self,
        guard: &PluginMutationGuard,
        plan: &LoadedPluginPlan,
    ) -> anyhow::Result<(
        Vec<PluginComponentKey>,
        Vec<PluginComponentKey>,
        (Snapshot, LoadedPluginPlan),
    )> {
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
        let limits = guard.limits;
        let (snapshot, previous) = guard.fs_prepare(move |parent, cancellation| verified_package(parent, cancellation, limits)).await?;
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
        Ok((removed, authorization_changes, (snapshot, previous)))
    }
    pub(super) async fn mutate_plugin(
        &self,
        context: &RequestContext,
        request: &RequestId,
        params: PluginsMutateParams,
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
                if before.is_some() || guard.parent.state == "removing" {
                    return Err(plugin_error(request, "plugins.use_continue_or_remove"));
                }
                let owner = AuthenticatedTransferOwner::from_request_context(context);
                {
                    let _upload_lock = self.acquire_skill_upload_lock(&upload_id).await;
                    self.revalidate_plugin_upload(&owner, &params.workspace_id, &upload_id, request).await?;
                }
                let runtime = self.package_delivery_context();
                let source = self
                    .materialize_uploaded_archive_source(
                        &owner,
                        &params.workspace_id,
                        &upload_id,
                        &runtime,
                        request,
                        "plugin",
                    )
                    .await?;
                let (source, snapshot, plan) = load_uploaded_plugin(source)
                    .await
                    .map_err(|_| plugin_error(request, "plugins.package_rejected"))?;
                if plan.tree_digest != expected_fingerprint {
                    return Err(plugin_error(request, "plugins.fingerprint_changed"));
                }
                let (removed, auth, _) = self
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
                let next = package.with_file_name("package-next");
                let staged = next.clone();
                let limits = snapshot.limits();
                let expected = plan.tree_digest.clone();
                guard
                    .fs(move |_| {
                        if staged.exists() {
                            std::fs::remove_dir_all(&staged)?;
                        }
                        publish_package(&snapshot, &staged)?;
                        anyhow::ensure!(Snapshot::capture(&staged, limits, || false)?.tree_digest() == expected,
                            "plugins.package_changed");
                        Ok(())
                    })
                    .await
                    .map_err(|_| plugin_error(request, "plugins.staging_failed"))?;
                let action = PackageAction {
                    kind: "update".into(),
                    source_upload_id: Some(upload_id.clone()),
                    children,
                    target_fingerprint: plan.tree_digest.clone(),
                    ..Default::default()
                };
                let pending = serde_json::to_string(&action)
                    .map_err(|_| plugin_error(request, "plugins.plan_too_large"))?;
                let _upload_lock = self.acquire_skill_upload_lock(&upload_id).await;
                self.revalidate_plugin_upload(&owner, &params.workspace_id, &upload_id, request).await?;
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
                        let payload = source.upload.clone();
                        let _ = guard
                            .fs(move |_| {
                                super::super::package_delivery::upload::cleanup_consumed_payload(
                                    &payload,
                                )
                            })
                            .await;
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
                            let abandoned = next.clone();
                            let _ = guard
                                .fs(move |_| {
                                    let _ = std::fs::remove_dir_all(abandoned);
                                    Ok(())
                                })
                                .await;
                        }
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
                let limits = guard.limits;
                guard
                    .fs_prepare(move |parent, cancellation| verified_package(parent, cancellation, limits))
                    .await
                    .map_err(|_| plugin_error(request, "plugins.package_changed"))?;
                let action = PackageAction {
                    kind: "retry".into(),
                    children,
                    target_fingerprint: guard.parent.package_fingerprint.clone(),
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
                        .continue_native_plugin_action(context, request, guard, value)
                        .await;
                }
                let mut action: PackageAction = serde_json::from_str(pending)
                    .map_err(|_| plugin_error(request, "plugins.pending_invalid"))?;
                if action.kind == "retained" {
                    return Err(plugin_error(request, "plugins.fresh_package_required"));
                }
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
        let deadline = self.agent_manager.control_operation_deadline();
        let result = async {
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
                let purge_data = action.purge_data;
                guard
                    .fs(move |parent| {
                        let package = Path::new(&parent.package_path);
                        for path in [
                            package.to_path_buf(),
                            package.with_file_name("package-next"),
                            package.with_file_name("package-backup"),
                        ] {
                            if path.exists() {
                                std::fs::remove_dir_all(path)?;
                            }
                        }
                        if purge_data && Path::new(&parent.data_path).exists() {
                            std::fs::remove_dir_all(&parent.data_path)?;
                        }
                        Ok(())
                    })
                    .await?;
                if action.purge_data {
                    self.crud_store
                        .delete_plugin_parent(&guard.parent.id, guard.parent.revision)
                        .await?;
                } else {
                    // Keep an addressable, execution-fenced owner for data. A
                    // fresh package update on this exact ID restores it; Remove
                    // with purge explicitly deletes it. Never match by name.
                    let retained = PackageAction {
                        kind: "retained".into(),
                        ..Default::default()
                    };
                    self.crud_store
                        .replace_plugin_pending(
                            &guard.parent.id,
                            guard.parent.revision,
                            &serde_json::to_string(&retained)?,
                        )
                        .await?;
                    self.crud_store
                        .finish_plugin_mutation(
                            &guard.parent.id,
                            guard.parent.revision,
                            "interrupted",
                            Some("plugins.data_retained".into()),
                        )
                        .await?;
                }
                self.mcp_service
                    .reload_workspace(&guard.parent.workspace_id)
                    .await?;
                return Ok::<_, anyhow::Error>(true);
            }
            let fs_action = action.clone();
            let limits = guard.limits;
            let plan = guard
                .fs(move |parent| {
                    let package = Path::new(&parent.package_path);
                    if fs_action.kind == "update" {
                        swap_package_after_stop(package, &fs_action.target_fingerprint, limits)?;
                    }
                    let (snapshot, plan) = package_snapshot(package, limits)?;
                    let digest = snapshot.tree_digest();
                    let expected = &fs_action.target_fingerprint;
                    anyhow::ensure!(&digest == expected, "plugins.package_changed");
                    Ok(plan)
                })
                .await?;
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
                    .publish_plugin_package_with_metadata(
                        &guard.parent.id,
                        guard.parent.revision,
                        &plan.manifest.name,
                        plan.manifest.version.as_deref(),
                        &action.target_fingerprint,
                        action.source_upload_id.as_deref(),
                        Some(&plan.manifest.metadata),
                    )
                    .await?;
            }
            self.apply_plugin_components(context, request, &guard, &action, &plan)
                .await?;
            guard
                .fs(move |parent| {
                    let next = Path::new(&parent.package_path).with_file_name("package-next");
                    if next.exists() {
                        std::fs::remove_dir_all(next)?;
                    }
                    let backup = Path::new(&parent.package_path).with_file_name("package-backup");
                    if backup.exists() {
                        std::fs::remove_dir_all(backup)?;
                    }
                    Ok(())
                })
                .await?;
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
        }
        .await;
        if result.is_err() {
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
                                    Err(error) => match error.to_string().as_str() {
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
                    name: link.member_key.clone(),
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
                            skills::SkillInstallSource::PackageMember(
                                ownership.clone(),
                                guard._lock.clone(),
                            ),
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
                            skills::SkillInstallSource::PackageMember(
                                ownership.clone(),
                                guard._lock.clone(),
                            ),
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
        let deadline = self.agent_manager.control_operation_deadline();
        let result = async {
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
                            &link.member_key,
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
        }
        .await;
        if !matches!(result, Ok(())) {
            let current = self
                .crud_store
                .find_plugin_installation(&guard.parent.id)
                .await
                .map_err(|_| plugin_error(request, "plugins.inventory_failed"))?
                .ok_or_else(|| plugin_error(request, "plugins.not_found"))?;
            if current.pending_json.is_some() {
                let code = if matches!(&result,Err(error) if error.to_string()=="plugins.reapply_native_change")
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
fn swap_package_after_stop(package: &Path, target: &str, limits: pioneer_plugins::Limits) -> anyhow::Result<()> {
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
    let snapshot = Snapshot::capture(package, limits, || false)?;
    anyhow::ensure!(snapshot.tree_digest() == target, "plugins.package_changed");
    Ok(())
}

/// Compare immutable loader facts. Skill member digests include bytes and modes;
/// MCP equality compares parsed config semantics, not JSON formatting. No config
/// values, host paths or secret material cross the public summary boundary.
fn package_content_changes(
    current: &Snapshot,
    previous: &LoadedPluginPlan,
    candidate: &Snapshot,
    next: &LoadedPluginPlan,
) -> (Vec<PluginComponentKey>, bool) {
    let updated = next
        .components
        .iter()
        .filter(|component| {
            previous
                .components
                .iter()
                .find(|old| key(old) == key(component))
                .is_some_and(|old| match (old, *component) {
                    (
                        ComponentPlan::Skill { tree_digest: a, .. },
                        ComponentPlan::Skill { tree_digest: b, .. },
                    ) => a != b,
                    (
                        ComponentPlan::Mcp { server: a, .. },
                        ComponentPlan::Mcp { server: b, .. },
                    ) => a != b,
                    _ => true,
                })
        })
        .map(key)
        .collect();
    // Shared files and metadata have package meaning even with no member diff.
    // Exclude member directories and MCP bytes, whose semantic changes are
    // already represented above. Retain root and MCP document modes separately.
    fn supporting<'a>(
        snapshot: &'a Snapshot,
        plan: &LoadedPluginPlan,
    ) -> std::collections::BTreeMap<&'a str, &'a pioneer_plugins::Entry> {
        snapshot
            .entries()
            .iter()
            .filter(|(path, _)| {
                !matches!(path.as_str(), "mcp.json" | ".mcp.json")
                    && !plan.components.iter().any(|component| match component {
                        ComponentPlan::Skill { member_path, .. } => {
                            *path == member_path || path.starts_with(&format!("{member_path}/"))
                        }
                        _ => false,
                    })
            })
            .map(|(path, entry)| (path.as_str(), entry))
            .collect()
    }
    let mcp_document_modes = |snapshot: &Snapshot| {
        ["mcp.json", ".mcp.json"].map(|path| match snapshot.entries().get(path) {
            Some(
                pioneer_plugins::Entry::Directory { mode }
                | pioneer_plugins::Entry::File { mode, .. },
            ) => Some(*mode),
            _ => None,
        })
    };
    (
        updated,
        current.root_mode() != candidate.root_mode()
            || mcp_document_modes(current) != mcp_document_modes(candidate)
            || supporting(current, previous) != supporting(candidate, next),
    )
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
    fn snapshot_limits() -> pioneer_plugins::Limits {
        pioneer_plugins::Limits { expanded_bytes: 50 * 1024 * 1024, entry_bytes: 1024 * 1024,
            entries: 2048, depth: 2048, components: 2048 }
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
        let target = Snapshot::capture(&next, snapshot_limits(), || false)
            .unwrap()
            .tree_digest();
        std::fs::rename(&package, root.path().join("package-backup")).unwrap();
        swap_package_after_stop(&package, &target, snapshot_limits()).unwrap();
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
        swap_package_after_stop(&package, &target, snapshot_limits()).unwrap(); // Already swapped, no second rename.
        assert!(swap_package_after_stop(&package, "wrong-target", snapshot_limits()).is_err());
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
            supporting_files_changed: false,
            package: PluginsPreviewResponse {
                name: "fixture.tools".into(),
                version: None,
                description: None,
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
    #[test]
    fn preview_compares_member_content_separately_from_shared_assets_and_metadata() {
        use pioneer_plugins::Entry;
        let package = |asset: &str, skill: &str, config: &str, metadata: serde_json::Value| {
            let mut entries = std::collections::BTreeMap::new();
            for path in ["skills", "skills/one", "assets"] {
                entries.insert(path.into(), Entry::Directory { mode: 0o755 });
            }
            for (path, bytes) in [
                ("plugin.json", serde_json::to_vec(&metadata).unwrap()),
                (
                    "skills/one/SKILL.md",
                    format!("---\nname: one\ndescription: Read data.\n---\n{skill}").into_bytes(),
                ),
                ("assets/guide.txt", asset.as_bytes().to_vec()),
                ("mcp.json", config.as_bytes().to_vec()),
            ] {
                entries.insert(path.into(), Entry::File { bytes, mode: 0o644 });
            }
            let snapshot = Snapshot::from_entries(entries, snapshot_limits()).unwrap();
            let plan = pioneer_plugins::load(&snapshot).unwrap();
            assert_eq!(plan.components.len(), 2);
            (snapshot, plan)
        };
        let manifest = json!({"$schema":pioneer_plugins::PLUGIN_SCHEMA,"name":"preview.tools","description":" Read shared data. "});
        let config = format!(
            r#"{{"$schema":"{}","mcpServers":{{"bridge":{{"type":"stdio","command":"node","args":["one.js"]}}}}}}"#,
            pioneer_plugins::MCP_SCHEMA
        );
        let (current, previous) = package("old", "Read assets.", &config, manifest.clone());
        let (candidate, next) = package("new", "Read assets.", &config, manifest.clone());
        let (changed, supporting) = package_content_changes(&current, &previous, &candidate, &next);
        assert!(
            changed.is_empty(),
            "assets alone must not report all children as changed"
        );
        assert!(supporting);
        assert_eq!(
            public_description(&next).as_deref(),
            Some("Read shared data.")
        );
        let (candidate, next) = package("old", "Changed instructions.", &config, manifest.clone());
        let (changed, supporting) = package_content_changes(&current, &previous, &candidate, &next);
        assert_eq!(
            changed,
            vec![PluginComponentKey {
                kind: "skill".into(),
                member_key: "one".into()
            }]
        );
        assert!(!supporting);
        let reformatted = serde_json::to_string_pretty(
            &serde_json::from_str::<serde_json::Value>(&config).unwrap(),
        )
        .unwrap();
        let (candidate, next) = package("old", "Read assets.", &reformatted, manifest.clone());
        assert_eq!(
            package_content_changes(&current, &previous, &candidate, &next),
            (vec![], false)
        );
        let (candidate, next) = package(
            "old",
            "Read assets.",
            &config.replace("one.js", "two.js"),
            manifest.clone(),
        );
        assert_eq!(
            package_content_changes(&current, &previous, &candidate, &next),
            (
                vec![PluginComponentKey {
                    kind: "mcp".into(),
                    member_key: "bridge".into()
                }],
                false
            )
        );
        let (_, next) = package(
            "old",
            "Read assets.",
            &config,
            json!({"$schema":pioneer_plugins::PLUGIN_SCHEMA,"name":"preview.tools"}),
        );
        assert!(public_description(&next).is_none());
        let (_, next) = package(
            "old",
            "Read assets.",
            &config,
            json!({"$schema":pioneer_plugins::PLUGIN_SCHEMA,"name":"preview.tools","description":"x".repeat(4096)}),
        );
        assert_eq!(public_description(&next).unwrap().chars().count(), 2048);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            std::fs::create_dir_all(root.join("skills/one")).unwrap();
            std::fs::create_dir(root.join("assets")).unwrap();
            for (path, bytes) in [
                ("plugin.json", serde_json::to_vec(&manifest).unwrap()),
                (
                    "skills/one/SKILL.md",
                    b"---\nname: one\ndescription: Read data.\n---\nRead assets.".to_vec(),
                ),
                ("assets/guide.txt", b"old".to_vec()),
                ("mcp.json", config.as_bytes().to_vec()),
                (".mcp.json", config.as_bytes().to_vec()),
            ] {
                std::fs::write(root.join(path), bytes).unwrap();
            }
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755)).unwrap();
            for path in ["mcp.json", ".mcp.json"] {
                std::fs::set_permissions(root.join(path), std::fs::Permissions::from_mode(0o644))
                    .unwrap();
            }
            let capture = || {
                let snapshot = Snapshot::capture(root, snapshot_limits(), || false).unwrap();
                let plan = pioneer_plugins::load(&snapshot).unwrap();
                assert_eq!(plan.components.len(), 2);
                (snapshot, plan)
            };
            let (current, previous) = capture();
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o750)).unwrap();
            let (candidate, next) = capture();
            assert_ne!(current.tree_digest(), candidate.tree_digest());
            assert_eq!(
                package_content_changes(&current, &previous, &candidate, &next),
                (vec![], true),
                "retained root permissions are a supporting package change"
            );
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755)).unwrap();
            for path in ["mcp.json", ".mcp.json"] {
                std::fs::set_permissions(root.join(path), std::fs::Permissions::from_mode(0o640))
                    .unwrap();
                let (candidate, next) = capture();
                assert_ne!(current.tree_digest(), candidate.tree_digest());
                assert_eq!(
                    package_content_changes(&current, &previous, &candidate, &next),
                    (vec![], true),
                    "retained {path} permissions must not disappear behind config semantics"
                );
                std::fs::set_permissions(root.join(path), std::fs::Permissions::from_mode(0o644))
                    .unwrap();
                let (restored, plan) = capture();
                assert_eq!(
                    package_content_changes(&current, &previous, &restored, &plan),
                    (vec![], false),
                    "restoring permissions must clear the supporting change"
                );
            }
        }
    }
}
