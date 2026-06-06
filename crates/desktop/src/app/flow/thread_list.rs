use super::*;
use pioneer_client::threads::start as thread_start;
pub(super) use pioneer_client::turns::timeline_refresh::{
    TurnTimelineRefreshTransitionEvent, transition_turn_timeline_refresh_state,
};

impl PioneerDesktop {
    pub(crate) fn upsert_thread_for_workspace(&mut self, thread_id: &str, workspace_id: &str) {
        self.upsert_thread_coordinator(thread_id, workspace_id);
    }

    pub(crate) fn thread_workspace_matches(&self, thread_id: &str, workspace_id: &str) -> bool {
        self.thread_workspace_id(thread_id) == Some(workspace_id)
    }

    pub(crate) fn open_thread_from_sidebar(
        &mut self,
        thread_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_main_content_view(MainContentView::Threads, cx);
        self.activate_thread_with_draft_restore(thread_id.clone(), window, cx);

        if let Some(workspace_id) = self
            .thread_workspace_id(thread_id.as_str())
            .map(str::to_owned)
        {
            self.set_preferred_workspace_id(Some(workspace_id.clone()));
            if let Some(connection_id) = self.gateway.ws_connection_id {
                self.ensure_thread_subscription(thread_id.clone(), workspace_id, connection_id, cx);
            }
        }

        self.ensure_thread_history_loaded(thread_id.as_str(), cx);
        self.rebuild_sidebar_tree_state(cx);
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

        self.thread_list_loading = true;
        let ws_sender = self.gateway.ws_command_sender.clone();

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
                            for thread in response.threads {
                                view.upsert_thread_snapshot(thread.clone());
                                view.upsert_thread_for_workspace(
                                    thread.id.as_str(),
                                    thread.workspace_id.as_str(),
                                );
                            }

                            view.set_thread_tree_snapshot(
                                response.folders,
                                response.placements,
                                response.agents_docs,
                            );
                            view.rebuild_sidebar_tree_state(cx);

                            if view.current_active_thread_id().is_none() {
                                if let Some(draft_thread_id) =
                                    view.resolve_existing_draft_thread_id()
                                {
                                    view.set_active_thread_id(Some(draft_thread_id.clone()));
                                    if let Some(workspace_id) = view
                                        .thread_workspace_id(draft_thread_id.as_str())
                                        .map(str::to_owned)
                                    {
                                        view.set_preferred_workspace_id(Some(workspace_id.clone()));
                                        view.ensure_thread_subscription(
                                            draft_thread_id.clone(),
                                            workspace_id,
                                            connection_id,
                                            cx,
                                        );
                                    }
                                    view.ensure_thread_history_loaded(draft_thread_id.as_str(), cx);
                                }
                                view.request_thread_start_if_needed();
                                let _ = view.drive_thread_start_queue(cx);
                            } else if let Some(active_thread_id) = view.active_thread_id.clone() {
                                view.ensure_thread_history_loaded(active_thread_id.as_str(), cx);
                            }
                            view.sync_composer_model_selection_for_active_thread();
                        }
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to refresh thread tree"
                            );
                            if !view.has_known_threads_for_workspace(workspace_id.as_str())
                                && view.current_active_thread_id().is_none()
                            {
                                view.request_thread_start_if_needed();
                                let _ = view.drive_thread_start_queue(cx);
                            }
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
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || self.gateway.ws_connection_id != Some(connection_id)
        {
            return;
        }

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_request = thread_id.clone();
            let workspace_id_for_request = workspace_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.thread_start(thread_start::thread_start_params(
                            thread_id_for_request,
                            workspace_id_for_request,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            let thread = response.thread;
                            view.upsert_thread_snapshot(thread.clone());
                            view.upsert_thread_for_workspace(
                                thread.id.as_str(),
                                thread.workspace_id.as_str(),
                            );
                        }
                        Err(error) => {
                            warn!(
                                thread_id = thread_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to subscribe to thread"
                            );
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn ensure_thread_history_loaded(&mut self, thread_id: &str, cx: &mut Context<Self>) {
        if self.is_thread_history_loaded(thread_id) || self.is_thread_history_loading(thread_id) {
            return;
        }

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };

        self.set_thread_history_loading(thread_id, true);

        let thread_id = thread_id.to_owned();
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_request = thread_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let response = ws_sender.thread_history(
                            pioneer_client::threads::history::thread_history_params(
                                thread_id_for_request,
                                None,
                            ),
                        )?;
                        let timelines = load_task_turn_timelines(&ws_sender, &response);
                        Ok::<_, anyhow::Error>((response, timelines))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.set_thread_history_loading(thread_id.as_str(), false);

                    match result {
                        Ok((response, timelines)) => {
                            if response.thread_id != thread_id {
                                return;
                            }

                            if !response.events.is_empty() {
                                view.clear_draft_thread_if_matches(response.thread_id.as_str());
                            }

                            view.upsert_thread_for_workspace(
                                response.thread_id.as_str(),
                                response.workspace_id.as_str(),
                            );

                            if let Some(coordinator) =
                                view.thread_coordinator_mut(response.thread_id.as_str())
                            {
                                coordinator.conversation.hydrate_history(&response.events);
                                for timeline in &timelines {
                                    coordinator
                                        .conversation
                                        .apply_composed_turn_timeline(timeline);
                                }
                            }

                            view.mark_thread_history_loaded(thread_id.as_str(), true);
                            view.sync_composer_model_selection_for_active_thread();
                        }
                        Err(error) => {
                            warn!(
                                thread_id = thread_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to load thread history"
                            );
                            view.mark_thread_history_loaded(thread_id.as_str(), false);
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(crate) fn refresh_turn_timeline(
        &mut self,
        thread_id: String,
        turn_id: String,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        if self.thread_coordinator(thread_id.as_str()).is_none() {
            return;
        }
        if !self.is_thread_history_loaded(thread_id.as_str()) {
            return;
        }
        let Some(generation) =
            self.request_turn_timeline_refresh(thread_id.as_str(), turn_id.as_str())
        else {
            return;
        };

        self.spawn_turn_timeline_refresh(thread_id, turn_id, connection_id, generation, cx);
    }

    fn spawn_turn_timeline_refresh(
        &self,
        thread_id: String,
        turn_id: String,
        connection_id: u64,
        generation: u64,
        cx: &mut Context<Self>,
    ) {
        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_request = thread_id.clone();
            let turn_id_for_request = turn_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.turn_timeline(
                            pioneer_client::threads::history::composed_task_turn_timeline_param(
                                thread_id_for_request,
                                turn_id_for_request,
                            ),
                        )
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    let has_matching_connection =
                        view.gateway.ws_connection_id == Some(connection_id);
                    if has_matching_connection {
                        match result {
                            Ok(timeline) => {
                                if let Some(coordinator) =
                                    view.thread_coordinator_mut(timeline.thread_id.as_str())
                                {
                                    coordinator
                                        .conversation
                                        .apply_composed_turn_timeline(&timeline);
                                }
                            }
                            Err(error) => {
                                warn!(
                                    thread_id = thread_id.as_str(),
                                    turn_id = turn_id.as_str(),
                                    error = %format!("{error:#}"),
                                    "failed to refresh composed turn timeline"
                                );
                            }
                        }
                    }

                    let queued_generation = view.complete_turn_timeline_refresh(
                        thread_id.as_str(),
                        turn_id.as_str(),
                        generation,
                    );
                    if let Some(next_generation) = queued_generation {
                        let next_connection_id = match view.gateway.ws_connection_id {
                            Some(id)
                                if view.gateway.connection_state
                                    == GatewayConnectionState::Connected =>
                            {
                                id
                            }
                            _ => {
                                view.abort_turn_timeline_refresh(
                                    thread_id.as_str(),
                                    turn_id.as_str(),
                                );
                                cx.notify();
                                return;
                            }
                        };

                        view.spawn_turn_timeline_refresh(
                            thread_id.clone(),
                            turn_id.clone(),
                            next_connection_id,
                            next_generation,
                            cx,
                        );
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn request_turn_timeline_refresh(&mut self, thread_id: &str, turn_id: &str) -> Option<u64> {
        let key = (thread_id.to_owned(), turn_id.to_owned());
        let current = self.turn_timeline_refresh.remove(&key);
        let (next_state, generation) = transition_turn_timeline_refresh_state(
            current,
            TurnTimelineRefreshTransitionEvent::Request,
        );
        if let Some(state) = next_state {
            self.turn_timeline_refresh.insert(key, state);
        }
        generation
    }

    fn complete_turn_timeline_refresh(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        completed_generation: u64,
    ) -> Option<u64> {
        let key = (thread_id.to_owned(), turn_id.to_owned());
        let Some(current) = self.turn_timeline_refresh.remove(&key) else {
            return None;
        };
        let (next_state, next_generation) = transition_turn_timeline_refresh_state(
            Some(current),
            TurnTimelineRefreshTransitionEvent::Complete {
                generation: completed_generation,
            },
        );
        if let Some(state) = next_state {
            self.turn_timeline_refresh.insert(key, state);
        }
        next_generation
    }

    fn abort_turn_timeline_refresh(&mut self, thread_id: &str, turn_id: &str) {
        self.turn_timeline_refresh
            .remove(&(thread_id.to_owned(), turn_id.to_owned()));
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

fn load_task_turn_timelines(
    ws_sender: &crate::gateway::GatewayWsCommandSender,
    response: &ThreadHistoryResponse,
) -> Vec<TurnTimelineResponse> {
    pioneer_client::threads::history::composed_task_turn_timeline_params(response)
        .into_iter()
        .filter_map(|params| {
            ws_sender
                .turn_timeline(params)
                .map_err(|error| {
                    warn!(
                        thread_id = response.thread_id.as_str(),
                        error = %format!("{error:#}"),
                        "failed to load composed turn timeline"
                    );
                    error
                })
                .ok()
        })
        .collect()
}
