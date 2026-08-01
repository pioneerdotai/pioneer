//! WebSocket worker connection-loop helpers.

use super::{GatewayWsConnectSpec, GatewayWsEvent};
use crate::gateway::session_lifecycle::{SessionTerminalReason, terminal_reason_from_auth_code};
use crate::transport::ws::backoff::{duration_to_millis_u64, jitter_delay, next_backoff};
use std::fmt;
use std::time::Duration;

pub const WEBSOCKET_PONG_TIMEOUT_MESSAGE: &str = "websocket pong timeout";
pub const WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE: &str = "websocket command channel closed";
pub const WEBSOCKET_CLOSED_BY_PEER_MESSAGE: &str = "websocket closed by peer";
pub const WEBSOCKET_STREAM_ENDED_MESSAGE: &str = "websocket stream ended";
pub const AUTH_ACCESS_CLOSE_CODE: u16 = 4401;
pub const AUTH_SESSION_TERMINAL_CLOSE_CODE: u16 = 4403;

#[derive(Clone, Debug)]
pub struct GatewayReconnectPlan {
    pub event: GatewayWsEvent,
    pub delay: Duration,
    pub delay_ms: u64,
    pub next_backoff: Duration,
    pub attempt: u32,
}

pub fn connecting_event(connection_id: u64, spec: &GatewayWsConnectSpec) -> GatewayWsEvent {
    GatewayWsEvent::Connecting {
        connection_id,
        endpoint_id: spec.endpoint_id.clone(),
        endpoint_name: spec.endpoint_name.clone(),
        endpoint_kind: spec.endpoint_kind,
    }
}

pub fn connected_event(connection_id: u64, spec: &GatewayWsConnectSpec) -> GatewayWsEvent {
    GatewayWsEvent::Connected {
        connection_id,
        endpoint_id: spec.endpoint_id.clone(),
        endpoint_name: spec.endpoint_name.clone(),
        gateway_base_url: spec.gateway_base_url.clone(),
    }
}

pub fn disconnected_event(
    connection_id: u64,
    spec: &GatewayWsConnectSpec,
    reason: String,
) -> GatewayWsEvent {
    GatewayWsEvent::Disconnected {
        connection_id,
        endpoint_id: spec.endpoint_id.clone(),
        endpoint_name: spec.endpoint_name.clone(),
        endpoint_kind: spec.endpoint_kind,
        gateway_base_url: spec.gateway_base_url.clone(),
        reason,
    }
}

pub fn connect_failed_event(
    connection_id: u64,
    spec: &GatewayWsConnectSpec,
    error: String,
) -> GatewayWsEvent {
    GatewayWsEvent::ConnectFailed {
        connection_id,
        endpoint_id: spec.endpoint_id.clone(),
        endpoint_name: spec.endpoint_name.clone(),
        endpoint_kind: spec.endpoint_kind,
        gateway_base_url: spec.gateway_base_url.clone(),
        error,
    }
}

pub fn next_reconnect_plan(
    connection_id: u64,
    spec: &GatewayWsConnectSpec,
    previous_attempt: u32,
    current_backoff: Duration,
    reason: String,
) -> GatewayReconnectPlan {
    let attempt = previous_attempt.saturating_add(1);
    let delay = jitter_delay(current_backoff, spec.timings.reconnect_jitter_percent);
    let delay_ms = duration_to_millis_u64(delay);
    let event = GatewayWsEvent::Reconnecting {
        connection_id,
        endpoint_id: spec.endpoint_id.clone(),
        endpoint_name: spec.endpoint_name.clone(),
        attempt,
        delay_ms,
        reason,
    };
    let next_backoff = next_backoff(current_backoff, spec.timings.reconnect_max);

    GatewayReconnectPlan {
        event,
        delay,
        delay_ms,
        next_backoff,
        attempt,
    }
}

pub fn should_retry_after_connect_failure(
    has_connected: bool,
    retry_initial_failure: bool,
) -> bool {
    has_connected || retry_initial_failure
}

pub fn terminal_reason_from_disconnect(reason: &str) -> Option<SessionTerminalReason> {
    terminal_reason_from_auth_code(reason.trim())
}

pub fn disconnect_reason_from_close(code: u16, reason: &str) -> String {
    let reason = reason.trim();
    if terminal_reason_from_auth_code(reason).is_some()
        || matches!(reason, "access_expired" | "credential_expired")
    {
        return reason.to_owned();
    }

    match code {
        AUTH_ACCESS_CLOSE_CODE => "access_expired".to_owned(),
        AUTH_SESSION_TERMINAL_CLOSE_CODE => "session_revoked".to_owned(),
        // A remote peer controls arbitrary close text and could echo the
        // access credential it received during the handshake. Only the
        // bounded auth reasons above may enter events/diagnostics.
        _ => WEBSOCKET_CLOSED_BY_PEER_MESSAGE.to_owned(),
    }
}

pub fn websocket_write_failed_message(error: impl fmt::Display) -> String {
    format!("websocket write failed: {error}")
}

pub fn websocket_ping_failed_message(error: impl fmt::Display) -> String {
    format!("websocket ping failed: {error}")
}

pub fn websocket_pong_send_failed_message(error: impl fmt::Display) -> String {
    format!("websocket pong send failed: {error}")
}

pub fn websocket_read_failed_message(error: impl fmt::Display) -> String {
    format!("websocket read failed: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::{timings::GatewayWsTimings, types::GatewayEndpointKind};

    fn spec() -> GatewayWsConnectSpec {
        GatewayWsConnectSpec {
            endpoint_id: "remote".to_owned(),
            endpoint_name: "Remote".to_owned(),
            endpoint_kind: GatewayEndpointKind::Remote,
            gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("127.0.0.1:22000").unwrap(),
            auth_token: None,
            session: None,
            timings: GatewayWsTimings::from_millis(100, 200, 300, 400, 1_000, 0).expect("timings"),
        }
    }

    #[test]
    fn transport_ws_worker_builds_connection_events() {
        let spec = spec();

        assert!(matches!(
            connecting_event(7, &spec),
            GatewayWsEvent::Connecting {
                connection_id: 7,
                endpoint_kind: GatewayEndpointKind::Remote,
                ..
            }
        ));
        assert!(matches!(
            connected_event(7, &spec),
            GatewayWsEvent::Connected {
                connection_id: 7,
                gateway_base_url,
                ..
            } if gateway_base_url.as_str() == "http://127.0.0.1:22000/"
        ));
        assert!(matches!(
            connect_failed_event(7, &spec, "boom".to_owned()),
            GatewayWsEvent::ConnectFailed {
                connection_id: 7,
                error,
                ..
            } if error == "boom"
        ));
    }

    #[test]
    fn transport_ws_worker_reconnect_plan_advances_attempt_and_backoff() {
        let spec = spec();
        let plan = next_reconnect_plan(7, &spec, 2, Duration::from_millis(400), "lost".to_owned());

        assert_eq!(plan.attempt, 3);
        assert_eq!(plan.delay, Duration::from_millis(400));
        assert_eq!(plan.delay_ms, 400);
        assert_eq!(plan.next_backoff, Duration::from_millis(800));
        assert!(matches!(
            plan.event,
            GatewayWsEvent::Reconnecting {
                connection_id: 7,
                attempt: 3,
                delay_ms: 400,
                ..
            }
        ));
    }

    #[test]
    fn transport_ws_worker_retry_policy_matches_initial_failure_semantics() {
        assert!(!should_retry_after_connect_failure(false, false));
        assert!(should_retry_after_connect_failure(false, true));
        assert!(should_retry_after_connect_failure(true, false));
    }

    #[test]
    fn transport_ws_worker_disconnect_reason_messages_match_desktop_contract() {
        assert_eq!(WEBSOCKET_PONG_TIMEOUT_MESSAGE, "websocket pong timeout");
        assert_eq!(
            WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE,
            "websocket command channel closed"
        );
        assert_eq!(WEBSOCKET_CLOSED_BY_PEER_MESSAGE, "websocket closed by peer");
        assert_eq!(WEBSOCKET_STREAM_ENDED_MESSAGE, "websocket stream ended");
        assert_eq!(
            websocket_write_failed_message("closed"),
            "websocket write failed: closed"
        );
        assert_eq!(
            websocket_ping_failed_message("closed"),
            "websocket ping failed: closed"
        );
        assert_eq!(
            websocket_pong_send_failed_message("closed"),
            "websocket pong send failed: closed"
        );
        assert_eq!(
            websocket_read_failed_message("closed"),
            "websocket read failed: closed"
        );
    }

    #[test]
    fn auth_close_codes_fail_closed_when_reason_is_missing_or_unrecognized() {
        assert_eq!(
            disconnect_reason_from_close(AUTH_ACCESS_CLOSE_CODE, ""),
            "access_expired"
        );
        assert_eq!(
            disconnect_reason_from_close(AUTH_SESSION_TERMINAL_CLOSE_CODE, "proxy closed"),
            "session_revoked"
        );
        assert_eq!(
            disconnect_reason_from_close(AUTH_SESSION_TERMINAL_CLOSE_CODE, "session_compromised"),
            "session_compromised"
        );
        assert_eq!(
            disconnect_reason_from_close(1000, ""),
            WEBSOCKET_CLOSED_BY_PEER_MESSAGE
        );
        assert_eq!(
            disconnect_reason_from_close(1000, "malicious access-secret echo"),
            WEBSOCKET_CLOSED_BY_PEER_MESSAGE
        );
    }
}
