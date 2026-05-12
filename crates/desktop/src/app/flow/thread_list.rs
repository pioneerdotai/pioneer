use super::*;

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

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };

        let workspace_id = self
            .preferred_workspace_id()
            .map(str::to_owned)
            .or_else(|| {
                self.gateway
                    .runtime
                    .as_ref()
                    .and_then(GatewayRuntime::active_workspace_id)
                    .map(str::to_owned)
            });
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
                        ws_sender.thread_tree(ThreadTreeParams {
                            workspace_id: workspace_id_for_request,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
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
                        }
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to refresh thread tree"
                            );
                            if !view.has_known_threads()
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
                        ws_sender.thread_start(ThreadStartParams {
                            thread_id: thread_id_for_request,
                            workspace_id: workspace_id_for_request,
                            name: None,
                            model: None,
                            model_provider: None,
                            sandbox: None,
                            mode: None,
                            origin_kind: None,
                            sidebar_visibility: None,
                            agent_nickname: None,
                            agent_role: None,
                        })
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
                        let response = ws_sender.thread_history(ThreadHistoryParams {
                            thread_id: thread_id_for_request,
                            limit: None,
                        })?;
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
                        ws_sender.turn_timeline(TurnTimelineParams {
                            thread_id: thread_id_for_request,
                            turn_id: turn_id_for_request,
                            compose_tasks: true,
                            include_collapsed_task_events: false,
                            max_child_items_per_task: Some(500),
                        })
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
        let state = self.turn_timeline_refresh.entry(key).or_default();
        state.next_generation = state.next_generation.saturating_add(1);
        if state.in_flight {
            state.dirty = true;
            return None;
        }
        state.in_flight = true;
        state.dirty = false;
        state.in_flight_generation = state.next_generation;
        Some(state.in_flight_generation)
    }

    fn complete_turn_timeline_refresh(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        completed_generation: u64,
    ) -> Option<u64> {
        let key = (thread_id.to_owned(), turn_id.to_owned());
        let Some(state) = self.turn_timeline_refresh.get_mut(&key) else {
            return None;
        };
        let should_rerun = state.dirty || state.next_generation > completed_generation;
        state.in_flight = false;
        state.dirty = false;
        if should_rerun {
            state.in_flight = true;
            state.in_flight_generation = state.next_generation;
            return Some(state.in_flight_generation);
        }
        self.turn_timeline_refresh.remove(&key);
        None
    }

    fn abort_turn_timeline_refresh(&mut self, thread_id: &str, turn_id: &str) {
        self.turn_timeline_refresh
            .remove(&(thread_id.to_owned(), turn_id.to_owned()));
    }
}

fn load_task_turn_timelines(
    ws_sender: &crate::gateway::GatewayWsCommandSender,
    response: &ThreadHistoryResponse,
) -> Vec<TurnTimelineResponse> {
    turn_ids_with_task_anchors(response)
        .into_iter()
        .filter_map(|turn_id| {
            ws_sender
                .turn_timeline(TurnTimelineParams {
                    thread_id: response.thread_id.clone(),
                    turn_id,
                    compose_tasks: true,
                    include_collapsed_task_events: false,
                    max_child_items_per_task: Some(500),
                })
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

fn turn_ids_with_task_anchors(response: &ThreadHistoryResponse) -> Vec<String> {
    let mut turn_ids = Vec::new();
    for event in &response.events {
        let has_task_anchor = match &event.payload {
            pioneer_protocol::ThreadHistoryEventPayload::ItemStarted { item, .. }
            | pioneer_protocol::ThreadHistoryEventPayload::ItemCompleted { item, .. }
            | pioneer_protocol::ThreadHistoryEventPayload::ItemUpdated { item, .. } => {
                matches!(item, pioneer_protocol::TurnItem::Task { .. })
            }
            _ => false,
        };
        if has_task_anchor && !turn_ids.iter().any(|turn_id| turn_id == &event.turn_id) {
            turn_ids.push(event.turn_id.clone());
        }
    }
    turn_ids
}

#[cfg(test)]
pub(super) fn transition_turn_timeline_refresh_state(
    state: Option<super::super::root::TurnTimelineRefreshState>,
    event: TurnTimelineRefreshTransitionEvent,
) -> (
    Option<super::super::root::TurnTimelineRefreshState>,
    Option<u64>,
) {
    let mut state = state.unwrap_or_default();
    match event {
        TurnTimelineRefreshTransitionEvent::Request => {
            state.next_generation = state.next_generation.saturating_add(1);
            if state.in_flight {
                state.dirty = true;
                return (Some(state), None);
            }
            state.in_flight = true;
            state.dirty = false;
            state.in_flight_generation = state.next_generation;
            let in_flight_generation = state.in_flight_generation;
            (Some(state), Some(in_flight_generation))
        }
        TurnTimelineRefreshTransitionEvent::Complete { generation } => {
            if !state.in_flight {
                return (Some(state), None);
            }
            let should_rerun = state.dirty || state.next_generation > generation;
            state.in_flight = false;
            state.dirty = false;
            if should_rerun {
                state.in_flight = true;
                state.in_flight_generation = state.next_generation;
                let in_flight_generation = state.in_flight_generation;
                return (Some(state), Some(in_flight_generation));
            }
            (None, None)
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TurnTimelineRefreshTransitionEvent {
    Request,
    Complete { generation: u64 },
}
