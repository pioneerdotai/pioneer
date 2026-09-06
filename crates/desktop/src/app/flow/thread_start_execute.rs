use super::*;
use pioneer_client::threads::start as thread_start;

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

        let retry_plan = thread_start::apply_thread_start_retry(
            &mut self.thread_start_coordinator_mut(),
            thread_id,
            std::time::Instant::now(),
        );
        let delay = retry_plan.delay;
        let attempt = retry_plan.attempt;

        let expected_attempt = attempt;
        let thread_id_for_timer = thread_id.to_owned();

        pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
            pioneer_observability::AnimationSourceId::ThreadStartRetryClock,
            pioneer_observability::DiagnosticAction::Scheduled,
            pioneer_observability::Visibility::NotApplicable,
        ));
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                cx.background_executor().timer(delay).await;
                pioneer_observability::record_qualification_diagnostic!(record_animation_activity(
                    pioneer_observability::AnimationSourceId::ThreadStartRetryClock,
                    pioneer_observability::DiagnosticAction::Woke,
                    pioneer_observability::Visibility::NotApplicable,
                ));

                #[cfg(not(feature = "qualification-diagnostics"))]
                {
                    let _ = this.update(&mut cx, |app, cx| {
                        if app.gateway.ws_connection_id != Some(connection_id)
                            || app.gateway.connection_state != GatewayConnectionState::Connected
                        {
                            return;
                        }

                        let start = app.thread_start_coordinator();

                        if !thread_start::should_fire_scheduled_thread_start_retry(
                            &start,
                            expected_attempt,
                            thread_id_for_timer.as_str(),
                        ) {
                            return;
                        }

                        app.enqueue_thread_start_request();
                        let started = app.drive_thread_start_queue(cx);
                        if started {
                            cx.notify();
                        }
                    });
                }
                #[cfg(feature = "qualification-diagnostics")]
                {
                    let handoff = this.update(&mut cx, |app, cx| {
                        if app.gateway.ws_connection_id != Some(connection_id)
                            || app.gateway.connection_state != GatewayConnectionState::Connected
                        {
                            return false;
                        }

                        let start = app.thread_start_coordinator();

                        if !thread_start::should_fire_scheduled_thread_start_retry(
                            &start,
                            expected_attempt,
                            thread_id_for_timer.as_str(),
                        ) {
                            return false;
                        }

                        pioneer_observability::record_qualification_diagnostic!(
                            record_animation_activity(
                                pioneer_observability::AnimationSourceId::ThreadStartRetryClock,
                                pioneer_observability::DiagnosticAction::Requested,
                                pioneer_observability::Visibility::NotApplicable,
                            )
                        );
                        app.enqueue_thread_start_request();
                        let started = app.drive_thread_start_queue(cx);
                        if started {
                            cx.notify();
                        }
                        true
                    });
                    pioneer_observability::record_qualification_diagnostic!(
                        record_animation_activity(
                            pioneer_observability::AnimationSourceId::ThreadStartRetryClock,
                            if matches!(handoff, Ok(true)) {
                                pioneer_observability::DiagnosticAction::Completed
                            } else {
                                pioneer_observability::DiagnosticAction::Cancelled
                            },
                            pioneer_observability::Visibility::NotApplicable,
                        )
                    );
                }
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

        let scope = self.default_thread_start_scope();
        let Some(start_plan) = thread_start::begin_thread_start_attempt(
            &mut self.thread_start_coordinator_mut(),
            thread_start::generate_thread_start_id(),
            scope,
        ) else {
            return false;
        };
        let requested_thread_id = start_plan.requested_thread_id;
        let requested_workspace_id = start_plan.requested_workspace_id;
        let requested_visibility = self.pending_thread_create_visibility;
        self.startup
            .begin(pioneer_observability::DesktopStartupStage::ActiveThreadBootstrap);

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let requested_thread_id_for_failure = requested_thread_id.clone();
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
                                return Err(error);
                            }
                        };

                        let response =
                            match ws_sender.thread_start(thread_start::thread_create_params(
                                requested_thread_id_for_request,
                                workspace_id.clone(),
                                requested_visibility,
                            )) {
                                Ok(response) => response,
                                Err(error) => {
                                    return Err(error);
                                }
                            };

                        Ok::<_, anyhow::Error>((workspace_id, response))
                    })
                    .await;

                let _ = this.update(&mut cx, |app, cx| {
                    if app.gateway.ws_connection_id != Some(connection_id) {
                        app.startup
                            .fail(pioneer_observability::DesktopStartupStage::ActiveThreadBootstrap);
                        return;
                    }

                    thread_start::finish_thread_start_attempt(&mut app.thread_start_coordinator_mut());

                    match result {
                        Ok((workspace_id, response)) => {
                            let reduction = thread_start::reduce_thread_start_bootstrap_success(
                                workspace_id,
                                response,
                                app.current_active_thread_id(),
                            );
                            app.apply_thread_start_bootstrap_reduction(reduction);
                            app.active_thread_resubscribe_pending = false;
                            app.startup.succeed(
                                pioneer_observability::DesktopStartupStage::ActiveThreadBootstrap,
                            );
                        }
                        Err(error) => {
                            let error_message = format!("{error:#}");
                            match thread_start::plan_thread_start_bootstrap_failure(
                                requested_thread_id_for_failure.as_str(),
                                error_message.as_str(),
                            ) {
                                thread_start::ThreadStartBootstrapFailurePlan::Retry {
                                    thread_id,
                                } => {
                                    app.schedule_thread_start_retry(
                                        connection_id,
                                        thread_id.as_str(),
                                        &error,
                                        cx,
                                    );
                                }
                                thread_start::ThreadStartBootstrapFailurePlan::Reset => {
                                    app.reset_thread_start_state();
                                    app.startup.fail(
                                        pioneer_observability::DesktopStartupStage::ActiveThreadBootstrap,
                                    );
                                    warn!(
                                        error = %error_message,
                                        "failed to start thread after websocket connect"
                                    );
                                }
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

    fn apply_thread_start_bootstrap_reduction(
        &mut self,
        reduction: thread_start::ThreadStartBootstrapReduction,
    ) {
        let thread = reduction.thread;
        let thread_id = reduction.thread_id;
        let workspace_id = reduction.workspace_id;
        let persist_workspace_id = reduction.persist_active_gateway_workspace_id;
        let draft_thread_id = reduction.set_draft_thread_id;
        let active_thread_id = reduction.set_active_thread_id;
        let preferred_workspace_id = reduction.set_preferred_workspace_id;
        let reset_thread_start = reduction.reset_thread_start;

        self.persist_active_gateway_workspace_id(persist_workspace_id);
        self.upsert_thread_snapshot(thread);
        self.upsert_thread_for_workspace(thread_id.as_str(), workspace_id.as_str());
        self.set_draft_thread_id(Some(draft_thread_id));

        if let Some(thread_id) = active_thread_id {
            self.set_active_thread_id(Some(thread_id));
        }
        self.set_preferred_workspace_id(Some(preferred_workspace_id));
        if reset_thread_start {
            self.reset_thread_start_state();
        }
    }
}
