//! Client DTO contracts intended for shell boundaries.
//!
//! These types describe the outer client contract a shell may consume. They are
//! kept separate from reducer internals so schema export stays limited to
//! explicit shell-facing DTOs.

use crate::{
    gateway::{
        timings::{GatewayTimingError, GatewayWsTimings},
        types::GatewayEndpoint,
    },
    notifications::effects::ClientEffect,
    state::{
        client_state::GatewayConnectionState, reducers::GatewayConnectionReduction,
        snapshot::ClientSnapshot,
    },
};
use pioneer_protocol::GatewayNotification;

pub mod export;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum ClientEvent {
    SnapshotChanged(ClientSnapshot),
    GatewayConnectionChanged(ClientGatewayConnectionEvent),
    GatewayNotification(GatewayNotification),
    EffectsPlanned(Vec<ClientEffect>),
    Error(ClientErrorEvent),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientGatewayConnectionEvent {
    pub connection_state: GatewayConnectionState,
    pub gateway_error: Option<String>,
}

impl From<GatewayConnectionReduction> for ClientGatewayConnectionEvent {
    fn from(reduction: GatewayConnectionReduction) -> Self {
        Self {
            connection_state: reduction.connection_state,
            gateway_error: reduction.gateway_error,
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientErrorEvent {
    pub message: String,
    pub code: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ClientCommand {
    RefreshWorkspaceList,
    RefreshGatewaySettings,
    RefreshProviders,
    RefreshSkills,
    RefreshMcp,
    RefreshThreadArtifacts { thread_id: String },
    RefreshTurnTimeline { thread_id: String, turn_id: String },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientGatewayWsTimings {
    pub connect_timeout_ms: u64,
    pub ping_interval_ms: u64,
    pub pong_timeout_ms: u64,
    pub reconnect_initial_ms: u64,
    pub reconnect_max_ms: u64,
    pub reconnect_jitter_percent: u8,
}

impl ClientGatewayWsTimings {
    pub fn to_gateway_ws_timings(self) -> Result<GatewayWsTimings, GatewayTimingError> {
        GatewayWsTimings::from_millis(
            self.connect_timeout_ms,
            self.ping_interval_ms,
            self.pong_timeout_ms,
            self.reconnect_initial_ms,
            self.reconnect_max_ms,
            self.reconnect_jitter_percent,
        )
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientGatewayConnectRequest {
    pub endpoint: GatewayEndpoint,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub timings: ClientGatewayWsTimings,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClientGatewayConnectResult {
    pub connection_id: u64,
}
