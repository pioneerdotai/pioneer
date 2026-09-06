use super::*;
use pioneer_client::threads::tree as thread_tree;

impl PioneerDesktop {
    pub(crate) fn upsert_thread_for_workspace(&mut self, thread_id: &str, workspace_id: &str) {
        self.upsert_thread_coordinator(thread_id, workspace_id);
    }

    pub(crate) fn thread_workspace_matches(&self, thread_id: &str, workspace_id: &str) -> bool {
        self.thread_workspace_id(thread_id).as_deref() == Some(workspace_id)
    }

    fn apply_thread_tree_refresh_success_reduction(
        &mut self,
        reduction: thread_tree::ThreadTreeRefreshSuccessReduction,
        connection_id: u64,
        cx: &mut Context<Self>,
    ) {
        let workspace_thread_ids = self
            .thread_coordinator_snapshots()
            .iter()
            .filter(|(_, coordinator)| coordinator.workspace_id == reduction.workspace_id)
            .map(|(thread_id, _)| thread_id.clone())
            .collect::<std::collections::HashSet<_>>();
        self.thread_unread
            .retain(|thread_id, _| !workspace_thread_ids.contains(thread_id));
        self.thread_unread.extend(
            pioneer_client::threads::read::project_thread_unread(&reduction.unread)
                .into_iter()
                .filter(|summary| summary.unread_count > 0)
                .map(|summary| (summary.thread_id, summary.unread_count)),
        );
        for thread in reduction.threads {
            let thread_id = thread.id.clone();
            let workspace_id = thread.workspace_id.clone();
            self.upsert_thread_snapshot(thread);
            self.upsert_thread_for_workspace(thread_id.as_str(), workspace_id.as_str());
        }

        self.set_thread_tree_snapshot(
            reduction.folders,
            reduction.placements,
            reduction.agents_docs,
        );
        self.rebuild_sidebar_tree_state(cx);

        if let Some(thread_id) = reduction.set_active_thread_id {
            self.set_active_thread_id(Some(thread_id));
        }
        if let Some(workspace_id) = reduction.set_preferred_workspace_id {
            self.set_preferred_workspace_id(Some(workspace_id));
        }
        if let Some(action) = reduction.ensure_thread_subscription {
            let targets_active_thread =
                self.current_active_thread_id() == Some(action.thread_id.as_str());
            if !targets_active_thread || self.active_thread_resubscribe_pending {
                self.ensure_thread_subscription(
                    action.thread_id,
                    action.workspace_id,
                    connection_id,
                    cx,
                );
            }
        }
        if let Some(thread_id) = reduction.ensure_thread_timeline_loaded {
            self.ensure_thread_semantic_timeline_loaded(thread_id.as_str(), cx);
        }
        if reduction.request_thread_start_if_needed {
            self.request_thread_start_if_needed();
        }
        if reduction.drive_thread_start_queue {
            let _ = self.drive_thread_start_queue(cx);
        }
        if reduction.sync_composer_model_selection {
            self.startup
                .begin(pioneer_observability::DesktopStartupStage::ComposerModelSelectionResolve);
            self.sync_composer_model_selection_for_active_thread();
            self.startup
                .succeed(pioneer_observability::DesktopStartupStage::ComposerModelSelectionResolve);
        }
    }

    fn apply_thread_tree_refresh_failure_reduction(
        &mut self,
        reduction: thread_tree::ThreadTreeRefreshFailureReduction,
        cx: &mut Context<Self>,
    ) {
        if reduction.request_thread_start_if_needed {
            self.request_thread_start_if_needed();
        }
        if reduction.drive_thread_start_queue {
            let _ = self.drive_thread_start_queue(cx);
        }
    }

    pub(crate) fn open_thread_from_sidebar(
        &mut self,
        thread_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.task_thread_navigation_stack.clear();
        self.set_main_content_view(MainContentView::Threads, cx);
        self.activate_thread_with_draft_restore(thread_id.clone(), window, cx);

        if let Some(workspace_id) = self.thread_workspace_id(thread_id.as_str()) {
            self.set_preferred_workspace_id(Some(workspace_id.clone()));
            if let Some(connection_id) = self.gateway.ws_connection_id {
                self.ensure_thread_subscription(thread_id.clone(), workspace_id, connection_id, cx);
            }
            if let Some(connection_id) = self.gateway.ws_connection_id
                && let Some(workspace_id) = self.thread_workspace_id(thread_id.as_str())
            {
                self.refresh_cli_runtime_thread_binding(
                    thread_id.clone(),
                    workspace_id.to_owned(),
                    connection_id,
                    cx,
                );
            }
        }

        self.ensure_thread_semantic_timeline_loaded(thread_id.as_str(), cx);
        self.rebuild_sidebar_tree_state(cx);
    }

    pub(crate) fn open_task_child_thread(
        &mut self,
        child_thread_id: String,
        title: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(parent_thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        if parent_thread_id == child_thread_id {
            return;
        }
        let Some(workspace_id) = self
            .thread_workspace_id(parent_thread_id.as_str())
            .or_else(|| self.active_workspace_id().map(str::to_owned))
        else {
            return;
        };

        self.remember_active_thread_draft(cx);
        self.task_thread_navigation_stack
            .retain(|entry| entry.child_thread_id != child_thread_id);
        self.task_thread_navigation_stack
            .push(TaskThreadNavigationEntry {
                parent_thread_id,
                child_thread_id: child_thread_id.clone(),
                workspace_id: workspace_id.clone(),
                title,
            });
        self.set_main_content_view(MainContentView::Threads, cx);
        self.set_active_thread_id(Some(child_thread_id.clone()));
        self.clear_composer(window, cx);
        self.set_preferred_workspace_id(Some(workspace_id.clone()));

        if let Some(connection_id) = self.gateway.ws_connection_id {
            self.ensure_thread_subscription(
                child_thread_id.clone(),
                workspace_id.clone(),
                connection_id,
                cx,
            );
            self.refresh_cli_runtime_thread_binding(
                child_thread_id.clone(),
                workspace_id,
                connection_id,
                cx,
            );
        }

        self.ensure_thread_semantic_timeline_loaded(child_thread_id.as_str(), cx);
        cx.notify();
    }

    pub(crate) fn close_task_child_thread(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(entry) = self.task_thread_navigation_stack.pop() else {
            return;
        };
        self.set_active_thread_id(Some(entry.parent_thread_id.clone()));
        self.restore_thread_draft(entry.parent_thread_id.as_str(), window, cx);
        self.set_preferred_workspace_id(Some(entry.workspace_id.clone()));

        if let Some(connection_id) = self.gateway.ws_connection_id {
            self.ensure_thread_subscription(
                entry.parent_thread_id.clone(),
                entry.workspace_id.clone(),
                connection_id,
                cx,
            );
            self.refresh_cli_runtime_thread_binding(
                entry.parent_thread_id.clone(),
                entry.workspace_id,
                connection_id,
                cx,
            );
        }

        self.ensure_thread_semantic_timeline_loaded(entry.parent_thread_id.as_str(), cx);
        cx.notify();
    }

    pub(crate) fn refresh_thread_list(&mut self, cx: &mut Context<Self>) {
        if self.thread_list_loading {
            return;
        }

        if self.workspaces_loading() {
            return;
        }

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };

        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id);
        let workspace_id = resolve_thread_tree_workspace_id(
            self.active_workspace_id(),
            self.preferred_workspace_id(),
            runtime_workspace_id,
        );
        let Some(workspace_id) = workspace_id else {
            if self.current_active_thread_id().is_none() {
                self.request_thread_start_if_needed();
                let _ = self.drive_thread_start_queue(cx);
            }
            return;
        };

        self.startup
            .begin(pioneer_observability::DesktopStartupStage::ThreadTreeLoad);
        self.startup
            .begin(pioneer_observability::DesktopStartupStage::ThreadTreeRequest);
        self.thread_list_loading = true;
        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let workspace_id_for_request = workspace_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.thread_tree(pioneer_client::threads::tree::thread_tree_params(
                            workspace_id_for_request,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    let runtime_workspace_id = view
                        .gateway
                        .runtime
                        .as_ref()
                        .and_then(GatewayRuntime::active_workspace_id);
                    let current_workspace_id = resolve_thread_tree_workspace_id(
                        view.active_workspace_id(),
                        view.preferred_workspace_id(),
                        runtime_workspace_id,
                    );
                    if current_workspace_id.as_deref() != Some(workspace_id.as_str()) {
                        return;
                    }

                    view.thread_list_loading = false;

                    match result {
                        Ok(response) => {
                            view.startup.succeed(
                                pioneer_observability::DesktopStartupStage::ThreadTreeRequest,
                            );
                            view.startup.begin(
                                pioneer_observability::DesktopStartupStage::ThreadTreeResponseApply,
                            );
                            let active_thread_id =
                                view.current_active_thread_id().map(str::to_owned);
                            let active_thread_workspace_id = active_thread_id
                                .as_deref()
                                .and_then(|thread_id| view.thread_workspace_id(thread_id));
                            let (existing_draft_thread_id, existing_draft_thread_workspace_id) =
                                if active_thread_id.is_none() {
                                    let draft_thread_id = view.resolve_existing_draft_thread_id();
                                    let draft_workspace_id = draft_thread_id
                                        .as_deref()
                                        .and_then(|thread_id| view.thread_workspace_id(thread_id));
                                    (draft_thread_id, draft_workspace_id)
                                } else {
                                    (None, None)
                                };
                            let reduction = thread_tree::reduce_thread_tree_refresh_success(
                                response,
                                thread_tree::ThreadTreeRefreshContext {
                                    active_thread_id: active_thread_id.as_deref(),
                                    active_thread_workspace_id: active_thread_workspace_id
                                        .as_deref(),
                                    existing_draft_thread_id: existing_draft_thread_id.as_deref(),
                                    existing_draft_thread_workspace_id:
                                        existing_draft_thread_workspace_id.as_deref(),
                                    has_known_threads_for_workspace: false,
                                },
                            );
                            view.startup.begin(
                                pioneer_observability::DesktopStartupStage::ActiveThreadResolve,
                            );
                            view.apply_thread_tree_refresh_success_reduction(
                                reduction,
                                connection_id,
                                cx,
                            );
                            view.startup.succeed(
                                pioneer_observability::DesktopStartupStage::ActiveThreadResolve,
                            );
                            view.startup.succeed(
                                pioneer_observability::DesktopStartupStage::ThreadTreeResponseApply,
                            );
                            view.startup.succeed(
                                pioneer_observability::DesktopStartupStage::ThreadTreeLoad,
                            );
                            if let Some(thread_id) =
                                view.current_active_thread_id().map(str::to_owned)
                                && view.draft_thread_id().as_deref() != Some(thread_id.as_str())
                                && let Some(workspace_id) =
                                    view.thread_workspace_id(thread_id.as_str())
                            {
                                view.refresh_cli_runtime_thread_binding(
                                    thread_id,
                                    workspace_id,
                                    connection_id,
                                    cx,
                                );
                            }
                        }
                        Err(error) => {
                            view.startup.fail(
                                pioneer_observability::DesktopStartupStage::ThreadTreeRequest,
                            );
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to refresh thread tree"
                            );
                            let active_thread_id =
                                view.current_active_thread_id().map(str::to_owned);
                            let active_thread_workspace_id = active_thread_id
                                .as_deref()
                                .and_then(|thread_id| view.thread_workspace_id(thread_id));
                            let reduction = thread_tree::reduce_thread_tree_refresh_failure(
                                thread_tree::ThreadTreeRefreshContext {
                                    active_thread_id: active_thread_id.as_deref(),
                                    active_thread_workspace_id: active_thread_workspace_id
                                        .as_deref(),
                                    existing_draft_thread_id: None,
                                    existing_draft_thread_workspace_id: None,
                                    has_known_threads_for_workspace: view
                                        .has_known_threads_for_workspace(workspace_id.as_str()),
                                },
                            );
                            view.apply_thread_tree_refresh_failure_reduction(reduction, cx);
                            view.startup
                                .fail(pioneer_observability::DesktopStartupStage::ThreadTreeLoad);
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn ensure_thread_subscription(
        &mut self,
        thread_id: String,
        workspace_id: String,
        connection_id: u64,
        _cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || self.gateway.ws_connection_id != Some(connection_id)
        {
            return;
        }

        let tracks_startup_active_thread =
            self.current_active_thread_id() == Some(thread_id.as_str());
        if tracks_startup_active_thread {
            self.startup
                .begin(pioneer_observability::DesktopStartupStage::ActiveThreadSubscribe);
            self.active_thread_resubscribe_pending = true;
        }

        self.thread_bindings
            .track_summary(&workspace_id, &thread_id);
        self.gateway
            .client_runtime
            .client_core()
            .schedule_thread_subscription(&thread_id, &workspace_id);
    }

    fn ensure_thread_semantic_timeline_loaded(&mut self, thread_id: &str, cx: &mut Context<Self>) {
        self.request_semantic_thread_newest_page(thread_id.to_owned(), cx);
    }

    fn refresh_cli_runtime_thread_binding(
        &mut self,
        thread_id: String,
        workspace_id: String,
        connection_id: u64,
        _cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || self.gateway.ws_connection_id != Some(connection_id)
        {
            return;
        }

        self.gateway
            .client_runtime
            .client_core()
            .schedule_thread_cli_binding(&thread_id, &workspace_id);
    }
}

pub(crate) fn resolve_thread_tree_workspace_id(
    active_workspace_id: Option<&str>,
    preferred_workspace_id: Option<&str>,
    runtime_workspace_id: Option<&str>,
) -> Option<String> {
    pioneer_client::workspaces::selectors::resolve_workspace_scope(
        active_workspace_id,
        preferred_workspace_id,
        runtime_workspace_id,
    )
}
