use super::*;

impl PioneerDesktop {
    pub(in crate::app::flow) fn schedule_thread_start_retry(
        &mut self,
        connection_id: u64,
        thread_id: &str,
        error: &anyhow::Error,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.ws_connection_id != Some(connection_id)
            || self.gateway.connection_state != GatewayConnectionState::Connected
        {
            self.reset_thread_start_state();
            return;
        }

        let start = self.thread_start_coordinator_mut();

        let delay = thread_start_retry_delay(start.retry_attempt);
        let attempt = start.retry_attempt.saturating_add(1);

        start.pending_thread_id = Some(thread_id.to_owned());
        start.retry_attempt = attempt;
        start.next_attempt_at = Some(std::time::Instant::now() + delay);

        let expected_attempt = attempt;
        let thread_id_for_timer = thread_id.to_owned();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                cx.background_executor().timer(delay).await;

                let _ = this.update(&mut cx, |app, cx| {
                    if app.gateway.ws_connection_id != Some(connection_id)
                        || app.gateway.connection_state != GatewayConnectionState::Connected
                    {
                        return;
                    }

                    let start = app.thread_start_coordinator();

                    if start.retry_attempt != expected_attempt
                        || start.pending_thread_id.as_deref() != Some(thread_id_for_timer.as_str())
                        || start.in_progress
                    {
                        return;
                    }

                    app.enqueue_thread_start_request();
                    let started = app.drive_thread_start_queue(cx);
                    if started {
                        cx.notify();
                    }
                });
            }
        })
        .detach();

        warn!(
            attempt,
            thread_id,
            retry_after_ms = delay.as_millis(),
            error = %format!("{error:#}"),
            "thread/start failed; scheduling retry"
        );
    }

    pub(in crate::app::flow) fn ensure_thread_started(
        &mut self,
        connection_id: u64,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.gateway.ws_connection_id != Some(connection_id) {
            return false;
        }

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return false;
        }

        let start = self.thread_start_coordinator_mut();
        if start.in_progress {
            return false;
        }

        start.in_progress = true;
        start.next_attempt_at = None;

        let requested_thread_id = start
            .pending_thread_id
            .clone()
            .unwrap_or_else(|| generate_id(ID_LEN));
        start.pending_thread_id = Some(requested_thread_id.clone());

        let scope = self.default_thread_start_scope();
        let requested_workspace_id =
            (scope.as_str() != WORKSPACE_START_SCOPE_BOOTSTRAP).then_some(scope);

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let requested_thread_id_for_retry = requested_thread_id.clone();
            let requested_thread_id_for_request = requested_thread_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let workspace_id = match resolve_workspace_id_for_thread_start(
                            &ws_sender,
                            requested_workspace_id,
                        ) {
                            Ok(workspace_id) => workspace_id,
                            Err(error) => {
                                return Err(ThreadStartBootstrapFailure { error });
                            }
                        };

                        let response = match ws_sender.thread_start(ThreadStartParams {
                            thread_id: requested_thread_id_for_request,
                            workspace_id: workspace_id.clone(),
                            name: None,
                            model: None,
                            model_provider: None,
                            sandbox: None,
                            mode: None,
                            origin_kind: None,
                            sidebar_visibility: None,
                            agent_nickname: None,
                            agent_role: None,
                        }) {
                            Ok(response) => response,
                            Err(error) => {
                                return Err(ThreadStartBootstrapFailure { error });
                            }
                        };

                        Ok::<_, ThreadStartBootstrapFailure>(ThreadStartBootstrapOutcome {
                            workspace_id,
                            response,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |app, cx| {
                    if app.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    app.thread_start_coordinator_mut().in_progress = false;

                    match result {
                        Ok(outcome) => {
                            app.persist_active_gateway_workspace_id(outcome.workspace_id.clone());
                            let thread = outcome.response.thread;
                            let thread_id = thread.id.clone();
                            let thread_workspace_id = thread.workspace_id.clone();

                            app.upsert_thread_snapshot(thread);
                            app.upsert_thread_for_workspace(
                                thread_id.as_str(),
                                thread_workspace_id.as_str(),
                            );
                            app.set_draft_thread_id(Some(thread_id.clone()));

                            if app.current_active_thread_id().is_none() {
                                app.set_active_thread_id(Some(thread_id));
                            }
                            app.set_preferred_workspace_id(Some(thread_workspace_id));
                            app.reset_thread_start_state();
                        }
                        Err(failure) => {
                            if is_transient_thread_start_error(&failure.error) {
                                app.schedule_thread_start_retry(
                                    connection_id,
                                    requested_thread_id_for_retry.as_str(),
                                    &failure.error,
                                    cx,
                                );
                            } else {
                                app.reset_thread_start_state();
                                warn!(
                                    error = %format!("{:#}", failure.error),
                                    "failed to start thread after websocket connect"
                                );
                            }
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();

        true
    }
}
