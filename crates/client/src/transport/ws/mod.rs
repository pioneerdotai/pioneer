//! WebSocket transport client.

use crate::gateway::{timings::GatewayWsTimings, types::GatewayEndpointKind};
use pioneer_protocol::GatewayNotification;

pub mod backoff;
pub mod client;
pub mod command_sender;
pub mod decode;
pub mod download;
pub mod frames;
pub mod rpc;
pub mod runtime;
pub mod worker;

pub use runtime::{GatewayWsClient, GatewayWsCommandSender};

#[derive(Clone, Debug)]
pub struct GatewayWsConnectSpec {
    pub endpoint_id: String,
    pub endpoint_name: String,
    pub endpoint_kind: GatewayEndpointKind,
    pub address: String,
    pub auth_token: Option<String>,
    pub timings: GatewayWsTimings,
}

#[derive(Clone, Debug)]
pub enum GatewayWsEvent {
    Connecting {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
    },
    Connected {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        address: String,
    },
    Reconnecting {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        attempt: u32,
        delay_ms: u64,
        reason: String,
    },
    Disconnected {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        reason: String,
    },
    ConnectFailed {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        error: String,
    },
    Notification {
        connection_id: u64,
        notification: GatewayNotification,
    },
}

pub fn event_connection_id(event: &GatewayWsEvent) -> u64 {
    match event {
        GatewayWsEvent::Connecting { connection_id, .. }
        | GatewayWsEvent::Connected { connection_id, .. }
        | GatewayWsEvent::Reconnecting { connection_id, .. }
        | GatewayWsEvent::Disconnected { connection_id, .. }
        | GatewayWsEvent::ConnectFailed { connection_id, .. }
        | GatewayWsEvent::Notification { connection_id, .. } => *connection_id,
    }
}

pub fn should_apply_ws_event(active_connection_id: Option<u64>, event: &GatewayWsEvent) -> bool {
    active_connection_id == Some(event_connection_id(event))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{GatewayNotification, UnknownGatewayNotification};
    use serde_json::json;

    fn unknown_notification() -> GatewayNotification {
        GatewayNotification::Unknown(UnknownGatewayNotification {
            method: "unknown".to_owned(),
            workspace_id: None,
            thread_id: None,
            turn_id: None,
            item_id: None,
            params: json!({}),
        })
    }

    #[test]
    fn transport_ws_event_connection_id_covers_all_event_variants() {
        let events = vec![
            GatewayWsEvent::Connecting {
                connection_id: 7,
                endpoint_id: "local".to_owned(),
                endpoint_name: "Local".to_owned(),
                endpoint_kind: GatewayEndpointKind::Local,
            },
            GatewayWsEvent::Connected {
                connection_id: 7,
                endpoint_id: "local".to_owned(),
                endpoint_name: "Local".to_owned(),
                address: "127.0.0.1:17878".to_owned(),
            },
            GatewayWsEvent::Reconnecting {
                connection_id: 7,
                endpoint_id: "local".to_owned(),
                endpoint_name: "Local".to_owned(),
                attempt: 1,
                delay_ms: 100,
                reason: "closed".to_owned(),
            },
            GatewayWsEvent::Disconnected {
                connection_id: 7,
                endpoint_id: "local".to_owned(),
                endpoint_name: "Local".to_owned(),
                endpoint_kind: GatewayEndpointKind::Local,
                address: "127.0.0.1:17878".to_owned(),
                reason: "closed".to_owned(),
            },
            GatewayWsEvent::ConnectFailed {
                connection_id: 7,
                endpoint_id: "local".to_owned(),
                endpoint_name: "Local".to_owned(),
                endpoint_kind: GatewayEndpointKind::Local,
                address: "127.0.0.1:17878".to_owned(),
                error: "refused".to_owned(),
            },
            GatewayWsEvent::Notification {
                connection_id: 7,
                notification: unknown_notification(),
            },
        ];

        for event in events {
            assert_eq!(event_connection_id(&event), 7);
        }
    }

    #[test]
    fn transport_ws_should_apply_event_matches_active_connection() {
        let event = GatewayWsEvent::Connected {
            connection_id: 7,
            endpoint_id: "local".to_owned(),
            endpoint_name: "Local".to_owned(),
            address: "127.0.0.1:17878".to_owned(),
        };

        assert!(should_apply_ws_event(Some(7), &event));
        assert!(!should_apply_ws_event(Some(8), &event));
        assert!(!should_apply_ws_event(None, &event));
    }
}
