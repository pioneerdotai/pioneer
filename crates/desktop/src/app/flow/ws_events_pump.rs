use super::helpers::desktop_session_terminal_message;
use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop},
    gateway::GatewayRuntime,
};
use gpui::{AsyncApp, Context, WeakEntity, prelude::*};
use pioneer_client::runtime::ClientRuntimePostEventSink;
use pioneer_client::transport::ws::GatewayWsEvent;
use pioneer_protocol::GatewayNotification;
use std::collections::VecDeque;

const MAX_DEFERRED_GATEWAY_WS_EVENTS: usize = 32;

impl PioneerDesktop {
    pub(crate) fn start_gateway_ws_event_pump(&self, cx: &mut Context<Self>) {
        let client_runtime = self.gateway.client_runtime.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let client_runtime = client_runtime.clone();

            async move {
                loop {
                    let first_event = cx
                        .background_spawn({
                            let client_runtime = client_runtime.clone();
                            async move { client_runtime.recv_ws_event() }
                        })
                        .await;
                    let should_break = first_event.is_none();

                    let updated = this.update(&mut cx, |view, cx| {
                        view.handle_gateway_ws_events(first_event, cx);
                    });

                    if updated.is_err() {
                        break;
                    }

                    if should_break {
                        break;
                    }
                }
            }
        })
        .detach();
    }

    pub(crate) fn handle_gateway_ws_events(
        &mut self,
        first_event: Option<GatewayWsEvent>,
        cx: &mut Context<Self>,
    ) {
        let events = first_event
            .into_iter()
            .chain(self.gateway.client_runtime.drain_ws_events());
        let applicable = partition_gateway_ws_events(
            self.gateway.ws_connection_id,
            self.gateway.connecting || self.gateway.session_refresh_in_flight,
            events,
            &mut self.gateway.deferred_ws_events,
        );
        self.apply_gateway_ws_event_batch(applicable, cx);
    }

    fn apply_gateway_ws_event_batch(
        &mut self,
        events: impl IntoIterator<Item = GatewayWsEvent>,
        cx: &mut Context<Self>,
    ) {
        let mut events_applied = false;
        for event in events {
            self.apply_gateway_ws_event(event, cx);
            events_applied = true;
        }
        let outcome = {
            let client_runtime = self.gateway.client_runtime.clone();
            let mut sink = DesktopPostEventSink { app: self, cx };
            client_runtime.drive_post_event_batch(events_applied, &mut sink)
        };

        if outcome.should_notify() {
            cx.notify();
        }
    }

    pub(in crate::app::flow) fn replay_deferred_gateway_ws_events(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let active_connection_id = self.gateway.ws_connection_id;
        let applicable = self
            .gateway
            .deferred_ws_events
            .drain(..)
            .filter(|event| {
                pioneer_client::transport::ws::should_apply_ws_event(active_connection_id, event)
            })
            .collect::<Vec<_>>();
        self.apply_gateway_ws_event_batch(applicable, cx);
    }

    pub(in crate::app::flow) fn replay_deferred_gateway_ws_notifications(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let active_connection_id = self.gateway.ws_connection_id;
        let applicable = self
            .gateway
            .deferred_ws_events
            .drain(..)
            .filter(|event| should_replay_after_silent_replacement(active_connection_id, event))
            .collect::<Vec<_>>();
        self.apply_gateway_ws_event_batch(applicable, cx);
    }

    pub(in crate::app::flow) fn discard_deferred_gateway_ws_events(&mut self) {
        self.gateway.deferred_ws_events.clear();
    }

    pub(in crate::app::flow) fn next_gateway_connection_epoch(&mut self) -> u64 {
        // A user-initiated Gateway operation supersedes any scheduled or
        // in-flight UI refresh result. The durable refresh transaction may
        // still finish under its per-endpoint mutation lock, but it must not
        // replace the runtime selected by the newer operation.
        self.gateway.session_refresh_generation =
            self.gateway.session_refresh_generation.wrapping_add(1);
        self.gateway.session_refresh_in_flight = false;
        self.discard_deferred_gateway_ws_events();
        self.gateway.connection_epoch =
            pioneer_client::gateway::runtime::next_gateway_operation_epoch(
                self.gateway.connection_epoch,
            );
        self.gateway.connection_epoch
    }

    pub(in crate::app::flow) fn apply_gateway_ws_event(
        &mut self,
        event: GatewayWsEvent,
        cx: &mut Context<Self>,
    ) {
        match &event {
            GatewayWsEvent::Connecting { .. } => {
                self.startup.gateway_session_attempt_started();
            }
            GatewayWsEvent::Reconnecting { .. } => {
                self.startup.gateway_session_retry_scheduled();
            }
            GatewayWsEvent::Connected { .. } => {
                self.startup.gateway_session_transport_connected();
            }
            GatewayWsEvent::Disconnected { .. } | GatewayWsEvent::ConnectFailed { .. } => {
                self.startup.gateway_session_transport_failed();
            }
            GatewayWsEvent::Notification { .. } => {}
        }
        if self.apply_gateway_auth_control_event(&event, cx) {
            return;
        }
        self.apply_gateway_ws_event_after_auth_control(event, cx);
    }

    fn apply_gateway_ws_event_after_auth_control(
        &mut self,
        event: GatewayWsEvent,
        cx: &mut Context<Self>,
    ) {
        let context = pioneer_client::runtime::ClientRuntimeWsEventContext {
            queue_skills_refresh: matches!(
                self.main_content_view,
                MainContentView::Skills | MainContentView::SkillDetails
            ),
            should_resume_in_flight_turn: self.should_resume_in_flight_turn(),
        };
        match self.gateway.client_runtime.reduce_ws_event(event, context) {
            pioneer_client::runtime::ClientRuntimeWsEvent::Connection(reduction) => {
                self.apply_gateway_connection_reduction(reduction, Some(cx));
            }
            pioneer_client::runtime::ClientRuntimeWsEvent::Notification(notification) => {
                self.apply_gateway_notification(notification, cx);
            }
        }
    }

    fn apply_gateway_auth_control_event(
        &mut self,
        event: &GatewayWsEvent,
        cx: &mut Context<Self>,
    ) -> bool {
        let reason = match event {
            GatewayWsEvent::Connected {
                connection_id,
                endpoint_id,
                ..
            } => {
                let connection_id = *connection_id;
                let endpoint_id = endpoint_id.clone();
                let connected_event = event.clone();
                let sender = self.gateway.ws_command_sender.clone();
                cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                    let mut cx = cx.clone();
                    async move {
                        let verification = cx
                            .background_spawn({
                                let endpoint_id = endpoint_id.clone();
                                async move {
                                    let runtime = GatewayRuntime::load()?;
                                    runtime.verify_gateway_session_identity(
                                        endpoint_id.as_str(),
                                        &sender,
                                    )
                                }
                            })
                            .await;
                        let _ = this.update(&mut cx, |view, cx| {
                            if view.gateway.ws_connection_id != Some(connection_id) {
                                return;
                            }
                            match verification {
                                Ok(None)
                                    if view
                                        .gateway
                                        .runtime
                                        .as_ref()
                                        .is_none_or(|runtime| {
                                            runtime
                                                .session_terminal_reason(endpoint_id.as_str())
                                                .is_none()
                                        }) =>
                                {
                                    view.apply_gateway_ws_event_after_auth_control(
                                        connected_event,
                                        cx,
                                    );
                                }
                                Ok(None) => {
                                    view.startup.gateway_session_identity_failed();
                                }
                                Ok(Some(reason)) => {
                                    view.startup.gateway_session_identity_failed();
                                    view.apply_gateway_session_terminal(reason, cx);
                                }
                                Err(error) => {
                                    view.startup.gateway_session_identity_failed();
                                    let rendered = format!("{error:#}");
                                    if let Some(reason) =
                                        pioneer_client::gateway::session_lifecycle::terminal_reason_from_auth_code(
                                            rendered.as_str(),
                                        )
                                    {
                                        view.apply_gateway_session_terminal(reason, cx);
                                    } else if pioneer_client::gateway::session_lifecycle::auth_code_requires_refresh(
                                        rendered.as_str(),
                                    ) {
                                        view.gateway.ws_connection_id = None;
                                        view.gateway.connection_state =
                                            GatewayConnectionState::Disconnected;
                                        view.recover_gateway_session_now(cx);
                                        cx.notify();
                                    } else {
                                        view.gateway.ws_connection_id = None;
                                        view.gateway.connection_state =
                                            GatewayConnectionState::Disconnected;
                                        view.gateway.error = Some(rendered);
                                        let _ = view.gateway.ws_command_sender.disconnect();
                                        cx.notify();
                                    }
                                }
                            }
                        });
                    }
                })
                .detach();
                return true;
            }
            GatewayWsEvent::Notification {
                notification:
                    GatewayNotification::AuthSessionRevoked(pioneer_protocol::AuthSessionRevokedNotification {
                        session_id,
                        reason,
                    }),
                ..
            } if self
                .gateway
                .runtime
                .as_ref()
                .is_some_and(|runtime| runtime.active_session_matches(session_id)) =>
            {
                Some(match reason {
                    pioneer_protocol::AuthSessionTerminationReason::SessionRevoked => {
                        pioneer_client::gateway::session_lifecycle::SessionTerminalReason::SessionRevoked
                    }
                    pioneer_protocol::AuthSessionTerminationReason::SessionExpired => {
                        pioneer_client::gateway::session_lifecycle::SessionTerminalReason::SessionExpired
                    }
                    pioneer_protocol::AuthSessionTerminationReason::SessionCompromised => {
                        pioneer_client::gateway::session_lifecycle::SessionTerminalReason::SessionCompromised
                    }
                    pioneer_protocol::AuthSessionTerminationReason::PrincipalSuspended => {
                        pioneer_client::gateway::session_lifecycle::SessionTerminalReason::PrincipalSuspended
                    }
                    pioneer_protocol::AuthSessionTerminationReason::PrincipalRemoved => {
                        pioneer_client::gateway::session_lifecycle::SessionTerminalReason::PrincipalRemoved
                    }
                })
            }
            GatewayWsEvent::Disconnected { reason, .. }
                if pioneer_client::gateway::session_lifecycle::auth_code_requires_refresh(reason) =>
            {
                self.gateway.ws_connection_id = None;
                self.gateway.connection_state = GatewayConnectionState::Disconnected;
                self.recover_gateway_session_now(cx);
                cx.notify();
                return true;
            }
            GatewayWsEvent::ConnectFailed { error, .. }
                if pioneer_client::gateway::session_lifecycle::auth_code_requires_refresh(error) =>
            {
                self.gateway.ws_connection_id = None;
                self.gateway.connection_state = GatewayConnectionState::Disconnected;
                self.recover_gateway_session_now(cx);
                cx.notify();
                return true;
            }
            GatewayWsEvent::Disconnected { reason, .. } => {
                pioneer_client::gateway::session_lifecycle::terminal_reason_from_auth_code(reason)
            }
            GatewayWsEvent::Notification {
                notification: GatewayNotification::AuthAccessExpiring(notification),
                ..
            } if self
                .gateway
                .runtime
                .as_ref()
                .is_some_and(|runtime| runtime.active_session_matches(&notification.session_id)) =>
            {
                self.refresh_gateway_session_now(cx);
                return true;
            }
            _ => return false,
        };
        let Some(reason) = reason else {
            return false;
        };
        self.apply_gateway_session_terminal(reason, cx);
        true
    }

    fn apply_gateway_session_terminal(
        &mut self,
        reason: pioneer_client::gateway::session_lifecycle::SessionTerminalReason,
        cx: &mut Context<Self>,
    ) {
        let terminal = self
            .gateway
            .runtime
            .as_mut()
            .and_then(|runtime| runtime.mark_active_session_terminal(reason).ok().flatten());
        self.gateway.session_refresh_generation =
            self.gateway.session_refresh_generation.wrapping_add(1);
        self.gateway.session_refresh_in_flight = false;
        self.gateway.auth_session_action_pending = None;
        self.discard_deferred_gateway_ws_events();
        self.clear_authorization_epoch_cache();
        self.gateway.current_auth = None;
        self.gateway.capability_snapshot = None;
        self.clear_task_user_notification_inbox();
        self.administration.clear_for_session_termination();
        self.member_avatar_state.clear();
        self.member_workspaces_saving = false;
        self.gateway.auth_sessions.clear();
        self.gateway.ws_connection_id = None;
        self.gateway.connection_state = GatewayConnectionState::Disconnected;
        self.gateway.error = Some(desktop_session_terminal_message(
            terminal.map_or(reason, |terminal| terminal.reason),
        ));
        let _ = self.gateway.ws_command_sender.disconnect();
        cx.notify();
    }
}

fn partition_gateway_ws_events(
    active_connection_id: Option<u64>,
    defer_unmatched: bool,
    events: impl IntoIterator<Item = GatewayWsEvent>,
    deferred: &mut VecDeque<GatewayWsEvent>,
) -> Vec<GatewayWsEvent> {
    let mut applicable = Vec::new();
    for event in events {
        if pioneer_client::transport::ws::should_apply_ws_event(active_connection_id, &event) {
            applicable.push(event);
        } else if defer_unmatched {
            if deferred.len() == MAX_DEFERRED_GATEWAY_WS_EVENTS {
                deferred.pop_front();
            }
            deferred.push_back(event);
        }
    }
    applicable
}

fn should_replay_after_silent_replacement(
    active_connection_id: Option<u64>,
    event: &GatewayWsEvent,
) -> bool {
    pioneer_client::transport::ws::should_apply_ws_event(active_connection_id, event)
        && matches!(event, GatewayWsEvent::Notification { .. })
}

struct DesktopPostEventSink<'a, 'cx> {
    app: &'a mut PioneerDesktop,
    cx: &'a mut Context<'cx, PioneerDesktop>,
}

impl ClientRuntimePostEventSink for DesktopPostEventSink<'_, '_> {
    fn refresh_thread_list_if_requested(&mut self) -> bool {
        if !self.app.take_thread_list_refresh_request() {
            return false;
        }
        self.app.refresh_thread_list(self.cx);
        true
    }

    fn refresh_skills_if_requested(&mut self) -> bool {
        if !self.app.take_skills_refresh_request() {
            return false;
        }
        self.app.refresh_installed_skills(self.cx);
        true
    }

    fn refresh_mcp_if_requested(&mut self) -> bool {
        if !self.app.take_mcp_refresh_request() {
            return false;
        }
        self.app.refresh_mcp_servers(self.cx);
        true
    }

    fn refresh_mcp_details_if_requested(&mut self) -> bool {
        if !self.app.take_mcp_details_refresh_request() {
            return false;
        }
        self.app.refresh_mcp_server_details(self.cx);
        true
    }

    fn drive_thread_start_queue(&mut self) -> bool {
        self.app.drive_thread_start_queue(self.cx)
    }

    fn drive_turn_resume_queue(&mut self) -> bool {
        self.app.drive_turn_resume_queue(self.cx)
    }

    fn tick_thread_conversations(&mut self) -> bool {
        self.app.tick_thread_conversations()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::gateway::types::GatewayEndpointKind;
    use pioneer_protocol::{GatewayNotification, UnknownGatewayNotification};
    use serde_json::json;

    fn connecting(connection_id: u64) -> GatewayWsEvent {
        GatewayWsEvent::Connecting {
            connection_id,
            endpoint_id: "local".to_owned(),
            endpoint_name: "Local".to_owned(),
            endpoint_kind: GatewayEndpointKind::Local,
        }
    }

    #[test]
    fn replacement_events_are_deferred_until_the_new_connection_is_published() {
        let mut deferred = VecDeque::new();
        let applicable = partition_gateway_ws_events(Some(7), true, [connecting(8)], &mut deferred);

        assert!(applicable.is_empty());
        assert_eq!(deferred.len(), 1);

        let replayed =
            partition_gateway_ws_events(Some(8), false, deferred.drain(..), &mut VecDeque::new());
        assert_eq!(replayed.len(), 1);
        assert_eq!(
            pioneer_client::transport::ws::event_connection_id(&replayed[0]),
            8
        );
    }

    #[test]
    fn deferred_event_buffer_is_bounded_and_drops_the_oldest_event() {
        let mut deferred = VecDeque::new();
        let events = (1..=(MAX_DEFERRED_GATEWAY_WS_EVENTS as u64 + 1)).map(connecting);

        assert!(partition_gateway_ws_events(None, true, events, &mut deferred).is_empty());
        assert_eq!(deferred.len(), MAX_DEFERRED_GATEWAY_WS_EVENTS);
        assert_eq!(
            deferred
                .front()
                .map(pioneer_client::transport::ws::event_connection_id),
            Some(2)
        );
    }

    #[test]
    fn silent_replacement_keeps_notifications_without_replaying_lifecycle() {
        let notification = GatewayWsEvent::Notification {
            connection_id: 8,
            notification: GatewayNotification::Unknown(UnknownGatewayNotification {
                method: "test.notification".to_owned(),
                workspace_id: None,
                thread_id: None,
                turn_id: None,
                item_id: None,
                params: json!({}),
            }),
        };

        assert!(!should_replay_after_silent_replacement(
            Some(8),
            &connecting(8)
        ));
        assert!(should_replay_after_silent_replacement(
            Some(8),
            &notification
        ));
        assert!(!should_replay_after_silent_replacement(
            Some(7),
            &notification
        ));
    }
}
