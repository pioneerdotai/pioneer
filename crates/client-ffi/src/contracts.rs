use pioneer_client::{
    gateway::{
        timings::{GatewayTimingError, GatewayWsTimings},
        types::GatewayEndpoint,
    },
    notifications::effects::ClientEffect,
    runtime::{ClientRuntimeWsEvent, ClientRuntimeWsEventContext, reduce_gateway_ws_event},
    state::{
        client_state::GatewayConnectionState, reducers::GatewayConnectionReduction,
        snapshot::ClientSnapshot,
    },
    transport::ws::GatewayWsEvent,
};
use pioneer_protocol::GatewayNotification;
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ClientEvent {
    SnapshotChanged(ClientSnapshot),
    GatewayConnectionChanged(ClientGatewayConnectionEvent),
    GatewayNotification(GatewayNotification),
    EffectsPlanned(Vec<ClientEffect>),
    Error(ClientErrorEvent),
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientErrorEvent {
    pub message: String,
    pub code: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientGatewayConnectRequest {
    pub endpoint: GatewayEndpoint,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub timings: ClientGatewayWsTimings,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientGatewayConnectResult {
    pub connection_id: u64,
}

pub fn reduce_gateway_ws_events_to_client_events(
    events: impl IntoIterator<Item = GatewayWsEvent>,
    context: ClientRuntimeWsEventContext,
) -> Vec<ClientEvent> {
    events
        .into_iter()
        .map(|event| reduce_gateway_ws_event(event, context))
        .map(|event| match event {
            ClientRuntimeWsEvent::Connection(reduction) => {
                ClientEvent::GatewayConnectionChanged(reduction.into())
            }
            ClientRuntimeWsEvent::Notification(notification) => {
                ClientEvent::GatewayNotification(notification)
            }
        })
        .collect()
}
