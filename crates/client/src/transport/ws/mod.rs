//! WebSocket transport client.

use crate::gateway::{
    endpoint::GatewayBaseUrl, timings::GatewayWsTimings, types::GatewayEndpointKind,
};
use pioneer_protocol::GatewayNotification;
use pioneer_protocol::{AuthSecretString, AuthSessionId, DeviceId, GatewayId};

pub mod auth_exchange;
pub mod backoff;
pub mod client;
pub mod command_sender;
pub mod decode;
pub mod frames;
pub mod rpc;
pub mod runtime;
pub mod worker;

pub use runtime::{GatewayWsClient, GatewayWsCommandSender};

#[derive(Clone)]
pub struct GatewayWsConnectSpec {
    pub endpoint_id: String,
    pub endpoint_name: String,
    pub endpoint_kind: GatewayEndpointKind,
    pub gateway_base_url: GatewayBaseUrl,
    pub auth_token: Option<AuthSecretString>,
    pub session: Option<GatewayWsSessionIdentity>,
    pub timings: GatewayWsTimings,
}

impl std::fmt::Debug for GatewayWsConnectSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayWsConnectSpec")
            .field("endpoint_id", &self.endpoint_id)
            .field("endpoint_name", &self.endpoint_name)
            .field("endpoint_kind", &self.endpoint_kind)
            .field("gateway_base_url", &self.gateway_base_url)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[redacted]"),
            )
            .field("session", &self.session)
            .field("timings", &self.timings)
            .finish()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GatewayWsSessionIdentity {
    pub server_gateway_id: GatewayId,
    pub session_id: AuthSessionId,
    pub device_id: DeviceId,
    pub access_expires_at_unix: u64,
    pub refresh_leeway_seconds: u64,
}

#[derive(Clone, Debug)]
pub struct GatewayWsSessionSpec {
    pub endpoint_id: String,
    pub endpoint_name: String,
    pub endpoint_kind: GatewayEndpointKind,
    pub gateway_base_url: GatewayBaseUrl,
    pub identity: GatewayWsSessionIdentity,
    pub access_token: AuthSecretString,
    pub timings: GatewayWsTimings,
}

impl GatewayWsSessionSpec {
    pub fn into_connect_spec(self) -> GatewayWsConnectSpec {
        GatewayWsConnectSpec {
            endpoint_id: self.endpoint_id,
            endpoint_name: self.endpoint_name,
            endpoint_kind: self.endpoint_kind,
            gateway_base_url: self.gateway_base_url,
            auth_token: Some(self.access_token),
            session: Some(self.identity),
            timings: self.timings,
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
        gateway_base_url: GatewayBaseUrl,
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
        gateway_base_url: GatewayBaseUrl,
        reason: String,
    },
    ConnectFailed {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        gateway_base_url: GatewayBaseUrl,
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
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("127.0.0.1:17878").unwrap(),
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
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("127.0.0.1:17878").unwrap(),
                reason: "closed".to_owned(),
            },
            GatewayWsEvent::ConnectFailed {
                connection_id: 7,
                endpoint_id: "local".to_owned(),
                endpoint_name: "Local".to_owned(),
                endpoint_kind: GatewayEndpointKind::Local,
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("127.0.0.1:17878").unwrap(),
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
            gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("127.0.0.1:17878").unwrap(),
        };

        assert!(should_apply_ws_event(Some(7), &event));
        assert!(!should_apply_ws_event(Some(8), &event));
        assert!(!should_apply_ws_event(None, &event));
    }
}
