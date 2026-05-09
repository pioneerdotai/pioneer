use super::*;

impl MessageProcessor {
    pub(crate) async fn start_skills_watcher(self: &Arc<Self>) {
        let mut guard = self.skills_watcher_worker.lock().await;
        if guard.is_some() || !self.tool_loop_config.skills.enabled {
            return;
        }

        let roots = watch_roots(self.tool_loop_config.skills.system_roots.as_slice())
            .into_iter()
            .chain(watch_roots(
                self.tool_loop_config.skills.user_roots.as_slice(),
            ))
            .chain(watch_roots(
                self.tool_loop_config.skills.registry_roots.as_slice(),
            ))
            .collect::<Vec<_>>();

        if roots.is_empty() {
            return;
        }

        let this = self.clone();

        let handle: JoinHandle<()> = tokio::spawn(async move {
            let mut timer = interval(Duration::from_millis(SKILLS_WATCH_DEBOUNCE_MS));
            let mut last_digest = hash_skill_roots(roots.as_slice());
            loop {
                timer.tick().await;
                let next_digest = hash_skill_roots(roots.as_slice());
                if next_digest == last_digest {
                    continue;
                }
                last_digest = next_digest;

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
                        Vec::new()
                    }
                };
                let now = now_timestamp_secs();
                for workspace in workspaces {
                    this.notify_skills_changed(
                        workspace.id.as_str(),
                        "catalog_changed",
                        Vec::new(),
                        now,
                    )
                    .await;
                }
            }
        });

        *guard = Some(handle);
    }
}
