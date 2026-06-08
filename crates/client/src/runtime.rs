//! Shared client runtime primitives.
//!
//! This module owns shell-neutral orchestration that sits above the websocket
//! transport. Shell crates still own rendering, localization, dialogs, and
//! platform adapters, but websocket event filtering and protocol event
//! reduction belong here so desktop and mobile do not grow separate client
//! loops.

use crate::{
    state::reducers::{
        GatewayConnectionEvent, GatewayConnectionReduction, reduce_gateway_connection_event,
    },
    transport::ws::{
        GatewayWsClient, GatewayWsCommandSender, GatewayWsEvent, should_apply_ws_event,
    },
};
use pioneer_protocol::GatewayNotification;

#[derive(Clone)]
pub struct ClientRuntime {
    ws_client: GatewayWsClient,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientRuntimeWsEventContext {
    pub queue_skills_refresh: bool,
    pub should_resume_in_flight_turn: bool,
}

#[derive(Clone, Debug)]
pub enum ClientRuntimeWsEvent {
    Connection(GatewayConnectionReduction),
    Notification(GatewayNotification),
}

impl ClientRuntime {
    pub fn new() -> Self {
        Self {
            ws_client: GatewayWsClient::new(),
        }
    }

    pub fn ws_command_sender(&self) -> GatewayWsCommandSender {
        self.ws_client.command_sender()
    }

    pub fn recv_ws_event(&self) -> Option<GatewayWsEvent> {
        self.ws_client.recv_event()
    }

    pub fn drain_ws_events(&self) -> Vec<GatewayWsEvent> {
        self.ws_client.drain_events()
    }

    pub fn drain_applicable_ws_events(
        &self,
        active_connection_id: Option<u64>,
        first_event: Option<GatewayWsEvent>,
    ) -> Vec<GatewayWsEvent> {
        first_event
            .into_iter()
            .chain(self.drain_ws_events())
            .filter(|event| should_apply_ws_event(active_connection_id, event))
            .collect()
    }

    pub fn reduce_ws_event(
        &self,
        event: GatewayWsEvent,
        context: ClientRuntimeWsEventContext,
    ) -> ClientRuntimeWsEvent {
        reduce_gateway_ws_event(event, context)
    }
}

impl Default for ClientRuntime {
    fn default() -> Self {
        Self::new()
    }
}

pub fn reduce_gateway_ws_event(
    event: GatewayWsEvent,
    context: ClientRuntimeWsEventContext,
) -> ClientRuntimeWsEvent {
    match event {
        GatewayWsEvent::Connecting {
            endpoint_name,
            endpoint_kind,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Connecting {
                endpoint_name,
                endpoint_kind,
            },
        )),
        GatewayWsEvent::Connected {
            endpoint_name,
            address,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Connected {
                endpoint_name,
                address,
                queue_skills_refresh: context.queue_skills_refresh,
            },
        )),
        GatewayWsEvent::Reconnecting {
            endpoint_name,
            attempt,
            delay_ms,
            reason,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Reconnecting {
                endpoint_name,
                attempt,
                delay_ms,
                reason,
                should_resume_in_flight_turn: context.should_resume_in_flight_turn,
            },
        )),
        GatewayWsEvent::Disconnected {
            endpoint_name,
            endpoint_kind,
            address,
            reason,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::Disconnected {
                endpoint_name,
                endpoint_kind,
                address,
                reason,
                should_resume_in_flight_turn: context.should_resume_in_flight_turn,
            },
        )),
        GatewayWsEvent::ConnectFailed {
            endpoint_name,
            endpoint_kind,
            address,
            error,
            ..
        } => ClientRuntimeWsEvent::Connection(reduce_gateway_connection_event(
            GatewayConnectionEvent::ConnectFailed {
                endpoint_name,
                endpoint_kind,
                address,
                error,
                should_resume_in_flight_turn: context.should_resume_in_flight_turn,
            },
        )),
        GatewayWsEvent::Notification { notification, .. } => {
            ClientRuntimeWsEvent::Notification(notification)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        gateway::{timings::GatewayWsTimings, types::GatewayEndpointKind},
        notifications::effects::ClientEffect,
        state::{
            client_state::{GatewayConnectionState, GatewayStatusLevel},
            reducers::GatewayStatusMessage,
        },
        transport::ws::GatewayWsConnectSpec,
    };
    use pioneer_protocol::{
        UnknownGatewayNotification, Workspace, WorkspaceChangeKind, WorkspaceChangedNotification,
    };
    use serde_json::json;
    use std::time::Duration;

    fn timings() -> GatewayWsTimings {
        GatewayWsTimings {
            connect_timeout: Duration::from_millis(100),
            ping_interval: Duration::from_millis(100),
            pong_timeout: Duration::from_millis(100),
            reconnect_initial: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(100),
            reconnect_jitter_percent: 0,
        }
    }

    fn connect_spec(endpoint_id: &str) -> GatewayWsConnectSpec {
        GatewayWsConnectSpec {
            endpoint_id: endpoint_id.to_owned(),
            endpoint_name: "Remote".to_owned(),
            endpoint_kind: GatewayEndpointKind::Remote,
            address: "127.0.0.1:17878".to_owned(),
            auth_token: None,
            timings: timings(),
        }
    }

    #[test]
    fn runtime_filters_events_by_active_connection() {
        let runtime = ClientRuntime::new();
        let events = runtime.drain_applicable_ws_events(
            Some(2),
            Some(GatewayWsEvent::Connected {
                connection_id: 1,
                endpoint_id: "old".to_owned(),
                endpoint_name: "Old".to_owned(),
                address: "127.0.0.1:1".to_owned(),
            }),
        );
        assert!(events.is_empty());

        let events = runtime.drain_applicable_ws_events(
            Some(2),
            Some(GatewayWsEvent::Connected {
                connection_id: 2,
                endpoint_id: "new".to_owned(),
                endpoint_name: "New".to_owned(),
                address: "127.0.0.1:2".to_owned(),
            }),
        );
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn runtime_reduces_connected_ws_event_with_effects() {
        let event = GatewayWsEvent::Connected {
            connection_id: 7,
            endpoint_id: "remote".to_owned(),
            endpoint_name: "Remote".to_owned(),
            address: "127.0.0.1:17878".to_owned(),
        };

        let reduced = reduce_gateway_ws_event(
            event,
            ClientRuntimeWsEventContext {
                queue_skills_refresh: true,
                should_resume_in_flight_turn: false,
            },
        );

        let ClientRuntimeWsEvent::Connection(reduction) = reduced else {
            panic!("expected connection reduction");
        };

        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Connected
        );
        assert_eq!(reduction.status_level, GatewayStatusLevel::Connected);
        assert!(matches!(
            reduction.status,
            GatewayStatusMessage::ConnectedEndpoint { .. }
        ));
        assert_eq!(
            reduction.effects,
            vec![
                ClientEffect::RefreshWorkspaceList,
                ClientEffect::RefreshGatewaySettings,
                ClientEffect::QueueSkillsRefresh,
                ClientEffect::EnqueueInFlightTurnsForResume,
            ]
        );
    }

    #[test]
    fn runtime_reduces_reconnecting_event_with_resume_context() {
        let event = GatewayWsEvent::Reconnecting {
            connection_id: 7,
            endpoint_id: "remote".to_owned(),
            endpoint_name: "Remote".to_owned(),
            attempt: 2,
            delay_ms: 250,
            reason: "temporary".to_owned(),
        };

        let reduced = reduce_gateway_ws_event(
            event,
            ClientRuntimeWsEventContext {
                queue_skills_refresh: false,
                should_resume_in_flight_turn: true,
            },
        );

        let ClientRuntimeWsEvent::Connection(reduction) = reduced else {
            panic!("expected connection reduction");
        };

        assert_eq!(
            reduction.connection_state,
            GatewayConnectionState::Reconnecting
        );
        assert!(!reduction.clear_active_thread);
        assert_eq!(reduction.gateway_error.as_deref(), Some("temporary"));
    }

    #[test]
    fn runtime_preserves_notifications_without_shell_conversion() {
        let workspace = Workspace {
            id: "workspace-1".to_owned(),
            name: "Workspace".to_owned(),
            is_active: true,
            is_current: true,
            created_at: 1,
            updated_at: 2,
        };
        let notification = GatewayNotification::WorkspaceChanged(WorkspaceChangedNotification {
            kind: WorkspaceChangeKind::Updated,
            workspace: workspace.clone(),
        });
        let event = GatewayWsEvent::Notification {
            connection_id: 7,
            notification,
        };

        let reduced = reduce_gateway_ws_event(event, ClientRuntimeWsEventContext::default());

        let ClientRuntimeWsEvent::Notification(GatewayNotification::WorkspaceChanged(actual)) =
            reduced
        else {
            panic!("expected workspace notification");
        };
        assert_eq!(actual.workspace, workspace);
    }

    #[test]
    fn runtime_preserves_unknown_notifications() {
        let event = GatewayWsEvent::Notification {
            connection_id: 7,
            notification: GatewayNotification::Unknown(UnknownGatewayNotification {
                method: "custom.event".to_owned(),
                workspace_id: None,
                thread_id: None,
                turn_id: None,
                item_id: None,
                params: json!({"ok": true}),
            }),
        };

        let reduced = reduce_gateway_ws_event(event, ClientRuntimeWsEventContext::default());

        let ClientRuntimeWsEvent::Notification(GatewayNotification::Unknown(actual)) = reduced
        else {
            panic!("expected unknown notification");
        };
        assert_eq!(actual.method, "custom.event");
        assert_eq!(actual.params, json!({"ok": true}));
    }

    #[test]
    fn runtime_exposes_shared_ws_sender() {
        let runtime = ClientRuntime::new();
        let _sender = runtime.ws_command_sender();
        let _spec = connect_spec("remote");
    }
}
