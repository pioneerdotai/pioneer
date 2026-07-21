use super::storage_relocation::{import_configured_skill_roots, scan_managed_skill_roots};
use super::*;

impl MessageProcessor {
    pub(crate) async fn start_skills_watcher(self: &Arc<Self>) {
        let mut guard = self.skills_watcher_worker.lock().await;
        if guard.is_some() || !self.tool_loop_config.skills.enabled {
            return;
        }

        let this = self.clone();

        let handle: JoinHandle<()> = tokio::spawn(async move {
            let mut timer = interval(Duration::from_millis(SKILLS_WATCH_DEBOUNCE_MS));
            let mut initialized = false;
            let mut configured_digests = HashMap::new();
            let mut managed_digests = HashMap::new();
            loop {
                timer.tick().await;
                let workspaces = match this.workspace_manager.list_workspaces().await {
                    Ok(workspaces) => workspaces
                        .into_iter()
                        .filter(|workspace| workspace.is_active)
                        .collect::<Vec<_>>(),
                    Err(error) => {
                        warn!(
                            error = %error,
                            "failed to list workspaces for skills watcher notification"
                        );
                        continue;
                    }
                };
                let mut next_configured_digests = HashMap::new();
                let mut next_managed_digests = HashMap::new();
                let mut changes = Vec::with_capacity(workspaces.len());
                for (index, workspace) in workspaces.into_iter().enumerate() {
                    let workspace_id = workspace.id;
                    let configured =
                        match this.configured_root_import_config(workspace_id.as_str(), true) {
                            Ok(mut config) => {
                                if index > 0 {
                                    config
                                        .roots
                                        .retain(|root| root.source_kind != SkillSourceKind::System);
                                }
                                config.roots.retain(|root| {
                                    let key = (
                                        root.source_kind.as_db_value().to_owned(),
                                        root.scope_key.clone(),
                                        root.source_root.clone(),
                                    );
                                    let digest =
                                        hash_skill_roots(std::slice::from_ref(&root.source_root));
                                    next_configured_digests.insert(key.clone(), digest);
                                    initialized && configured_digests.get(&key) != Some(&digest)
                                });
                                (!config.roots.is_empty()).then_some(config)
                            }
                            Err(error) => {
                                warn!(
                                    workspace_id,
                                    error = %format!("{error:#}"),
                                    "failed to configure changed skill imports"
                                );
                                None
                            }
                        };
                    let managed = match this.managed_root_scan_config(workspace_id.as_str()) {
                        Ok(mut config) => {
                            if index > 0 {
                                config
                                    .roots
                                    .retain(|root| root.source_kind != SkillSourceKind::System);
                            }
                            config.roots.retain(|root| {
                                let key = (
                                    root.source_kind.as_db_value().to_owned(),
                                    root.scope_key.clone(),
                                    root.managed_root.clone(),
                                );
                                let digest =
                                    hash_skill_roots(std::slice::from_ref(&root.managed_root));
                                next_managed_digests.insert(key.clone(), digest);
                                initialized && managed_digests.get(&key) != Some(&digest)
                            });
                            (!config.roots.is_empty()).then_some(config)
                        }
                        Err(error) => {
                            warn!(
                                workspace_id,
                                error = %format!("{error:#}"),
                                "failed to configure changed managed skills scan"
                            );
                            None
                        }
                    };
                    changes.push((workspace_id, configured, managed));
                }
                configured_digests = next_configured_digests;
                managed_digests = next_managed_digests;
                if !initialized {
                    initialized = true;
                    continue;
                }

                let now = now_timestamp_secs();
                for (workspace_id, configured, managed) in changes {
                    let changed = configured.is_some() || managed.is_some();
                    if let Some(config) = configured
                        && let Err(error) = import_configured_skill_roots(
                            this.crud_store.as_ref(),
                            &this.skills_write_lock,
                            &config,
                        )
                        .await
                    {
                        warn!(
                            workspace_id,
                            error = %format!("{error:#}"),
                            "failed to import changed configured skills"
                        );
                    }
                    if let Some(config) = managed
                        && let Err(error) = scan_managed_skill_roots(
                            this.crud_store.as_ref(),
                            &this.skills_write_lock,
                            &config,
                        )
                        .await
                    {
                        warn!(
                            workspace_id,
                            error = %format!("{error:#}"),
                            "failed to scan changed managed skills"
                        );
                    }
                    if changed {
                        this.notify_skills_changed(
                            workspace_id.as_str(),
                            "catalog_changed",
                            Vec::new(),
                            now,
                        )
                        .await;
                    }
                }
            }
        });

        *guard = Some(handle);
    }
}
