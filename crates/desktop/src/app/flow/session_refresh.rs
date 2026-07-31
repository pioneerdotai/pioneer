use super::*;
use crate::gateway::DesktopSessionConnectionOutcome;
use pioneer_client::threads::start as thread_start;

const DESKTOP_ACCESS_REFRESH_LEEWAY_SECONDS: u64 = 60;
const DESKTOP_ACCESS_REFRESH_WORKSPACE_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DesktopSessionConnectionMode {
    ReplaceActive,
    RecoverDisconnected,
}

impl PioneerDesktop {
    pub(in crate::app::flow) fn schedule_gateway_session_refresh(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let Some(delay) = self.gateway.runtime.as_ref().and_then(|runtime| {
            runtime.active_session_refresh_delay(
                unix_timestamp_secs(),
                DESKTOP_ACCESS_REFRESH_LEEWAY_SECONDS,
            )
        }) else {
            return;
        };
        let Some(generation) = next_refresh_generation(
            &mut self.gateway.session_refresh_generation,
            self.gateway.session_refresh_in_flight,
        ) else {
            return;
        };
        let delay = delay.max(Duration::from_millis(1));

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                cx.background_executor().timer(delay).await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.refresh_gateway_session_if_current(
                        generation,
                        DesktopSessionConnectionMode::ReplaceActive,
                        cx,
                    );
                });
            }
        })
        .detach();
    }

    pub(in crate::app::flow) fn refresh_gateway_session_now(&mut self, cx: &mut Context<Self>) {
        self.start_gateway_session_refresh(DesktopSessionConnectionMode::ReplaceActive, cx);
    }

    pub(in crate::app::flow) fn recover_gateway_session_now(&mut self, cx: &mut Context<Self>) {
        self.start_gateway_session_refresh(DesktopSessionConnectionMode::RecoverDisconnected, cx);
    }

    pub(crate) fn recover_gateway_session_on_foreground(&mut self, cx: &mut Context<Self>) {
        let Some(runtime) = self.gateway.runtime.as_ref() else {
            return;
        };
        let Some(endpoint_id) = runtime.active_gateway_id() else {
            return;
        };
        if runtime.session_terminal_reason(endpoint_id).is_some() {
            return;
        }
        let disconnected = self.gateway.ws_connection_id.is_none()
            || self.gateway.connection_state == GatewayConnectionState::Disconnected;
        let refresh_due = runtime
            .active_session_refresh_delay(
                unix_timestamp_secs(),
                DESKTOP_ACCESS_REFRESH_LEEWAY_SECONDS,
            )
            .is_some_and(|delay| delay.is_zero());
        if disconnected {
            self.recover_gateway_session_now(cx);
        } else if refresh_due {
            self.refresh_gateway_session_now(cx);
        }
    }

    fn start_gateway_session_refresh(
        &mut self,
        mode: DesktopSessionConnectionMode,
        cx: &mut Context<Self>,
    ) {
        let Some(generation) = next_refresh_generation(
            &mut self.gateway.session_refresh_generation,
            self.gateway.session_refresh_in_flight,
        ) else {
            return;
        };
        self.refresh_gateway_session_if_current(generation, mode, cx);
    }

    fn refresh_gateway_session_if_current(
        &mut self,
        generation: u64,
        mode: DesktopSessionConnectionMode,
        cx: &mut Context<Self>,
    ) {
        if generation != self.gateway.session_refresh_generation
            || self.gateway.session_refresh_in_flight
        {
            return;
        }
        if self.workspace_action_in_progress()
            || self.desktop_voice_context_locked()
            || self.composer_upload_in_progress
        {
            cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let mut cx = cx.clone();
                async move {
                    cx.background_executor()
                        .timer(DESKTOP_ACCESS_REFRESH_WORKSPACE_RETRY_DELAY)
                        .await;
                    let _ = this.update(&mut cx, |view, cx| {
                        view.refresh_gateway_session_if_current(generation, mode, cx);
                    });
                }
            })
            .detach();
            return;
        }
        let Some(endpoint_id) = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_gateway_id)
            .map(str::to_owned)
        else {
            return;
        };
        self.discard_deferred_gateway_ws_events();
        self.gateway.session_refresh_in_flight = true;
        if self.current_active_thread_id().is_some() {
            self.active_thread_resubscribe_pending = true;
        }
        let active_thread_scope = self.current_active_thread_id().and_then(|thread_id| {
            self.thread_workspace_id(thread_id)
                .map(|workspace_id| (thread_id.to_owned(), workspace_id.to_owned()))
        });
        let sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let sender = sender.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        let mut runtime = GatewayRuntime::load()?;
                        let outcome = match mode {
                            DesktopSessionConnectionMode::ReplaceActive => runtime
                                .replace_gateway_session_access(endpoint_id.as_str(), &sender)?,
                            DesktopSessionConnectionMode::RecoverDisconnected => runtime
                                .recover_gateway_session_access(endpoint_id.as_str(), &sender)?,
                        };
                        let restored_thread = match (&outcome, active_thread_scope) {
                            (
                                DesktopSessionConnectionOutcome::Connected { .. },
                                Some((thread_id, workspace_id)),
                            ) => {
                                let response =
                                    sender.thread_start(thread_start::thread_start_params(
                                        thread_id.clone(),
                                        workspace_id,
                                    ));
                                Some((thread_id, response))
                            }
                            _ => None,
                        };
                        Ok::<_, anyhow::Error>((runtime, outcome, mode, restored_thread))
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    if generation != view.gateway.session_refresh_generation {
                        return;
                    }
                    view.gateway.session_refresh_in_flight = false;
                    match result {
                        Ok((
                            runtime,
                            DesktopSessionConnectionOutcome::Connected { connection_id, .. },
                            mode,
                            restored_thread,
                        )) => {
                            view.gateway.runtime = Some(runtime);
                            view.gateway.ws_connection_id = Some(connection_id);
                            if mode == DesktopSessionConnectionMode::RecoverDisconnected {
                                view.gateway.connection_state = GatewayConnectionState::Connecting;
                            }
                            view.gateway.error = None;
                            if mode == DesktopSessionConnectionMode::ReplaceActive {
                                // Access rotation does not change the selected Gateway or its
                                // data. Keep the existing Connected presentation and apply only
                                // notifications received while the replacement was in flight;
                                // replaying lifecycle events would run the full visible bootstrap.
                                view.replay_deferred_gateway_ws_notifications(cx);
                            } else {
                                view.replay_deferred_gateway_ws_events(cx);
                            }
                            if let Some((thread_id, result)) = restored_thread {
                                match result {
                                    Ok(response)
                                        if view.current_active_thread_id()
                                            == Some(thread_id.as_str()) =>
                                    {
                                        let reduction =
                                            thread_start::reduce_thread_start_subscription_success(
                                                response,
                                            );
                                        view.upsert_thread_snapshot(reduction.thread);
                                        view.upsert_thread_for_workspace(
                                            reduction.thread_id.as_str(),
                                            reduction.workspace_id.as_str(),
                                        );
                                        view.active_thread_resubscribe_pending = false;
                                        view.reconcile_semantic_timeline_after_reconnect(cx);
                                    }
                                    Ok(_) => {}
                                    Err(error) => {
                                        warn!(
                                            thread_id = thread_id.as_str(),
                                            error = %format!("{error:#}"),
                                            "failed to restore active thread during session refresh"
                                        );
                                    }
                                }
                            }
                            view.schedule_gateway_session_refresh(cx);
                        }
                        Ok((
                            runtime,
                            DesktopSessionConnectionOutcome::Terminal(terminal),
                            _,
                            _,
                        )) => {
                            view.active_thread_resubscribe_pending = false;
                            view.gateway.runtime = Some(runtime);
                            view.discard_deferred_gateway_ws_events();
                            view.gateway.ws_connection_id = None;
                            view.gateway.connection_state = GatewayConnectionState::Disconnected;
                            view.gateway.error =
                                Some(desktop_session_terminal_message(terminal.reason));
                            let _ = view.gateway.ws_command_sender.disconnect();
                        }
                        Err(error) => {
                            view.active_thread_resubscribe_pending = false;
                            view.discard_deferred_gateway_ws_events();
                            view.gateway.error = Some(format!("{error:#}"));
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }
}

fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn next_refresh_generation(current: &mut u64, in_flight: bool) -> Option<u64> {
    if in_flight {
        return None;
    }
    *current = current.wrapping_add(1);
    Some(*current)
}

#[cfg(test)]
mod tests {
    use super::{DesktopSessionConnectionMode, next_refresh_generation};

    #[test]
    fn duplicate_trigger_does_not_invalidate_an_in_flight_refresh() {
        let mut generation = 7;

        assert_eq!(next_refresh_generation(&mut generation, true), None);
        assert_eq!(generation, 7);
        assert_eq!(next_refresh_generation(&mut generation, false), Some(8));
    }

    #[test]
    fn connection_modes_keep_replacement_and_recovery_semantics_distinct() {
        assert_ne!(
            DesktopSessionConnectionMode::ReplaceActive,
            DesktopSessionConnectionMode::RecoverDisconnected
        );
    }
}
