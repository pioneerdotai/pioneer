use pioneer_client::settings::voice::{
    VoiceInputSettingsPlan, VoiceInputSettingsPlanRequest, VoiceInputStatusReduction,
};
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
    voice::{VoiceSessionResultReduction, reduce_voice_session_result_notification},
};
use pioneer_protocol::{GatewayNotification, GatewaySettingsUpdate, GatewayVoiceInputSettings};
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ClientEvent {
    SnapshotChanged(ClientSnapshot),
    GatewayConnectionChanged(ClientGatewayConnectionEvent),
    GatewayNotification(GatewayNotification),
    VoiceSessionResultReduced(VoiceSessionResultReduction),
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

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientGatewaySettingsGetRequest {}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientGatewaySettingsUpdateRequest {
    pub update: GatewaySettingsUpdate,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientVoiceInputPlanRequest {
    SettingsAction {
        request: VoiceInputSettingsPlanRequest,
    },
    StatusReduction {
        current: GatewayVoiceInputSettings,
    },
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ClientVoiceInputPlanResult {
    SettingsAction {
        plan: VoiceInputSettingsPlan,
    },
    StatusReduction {
        reduction: VoiceInputStatusReduction,
    },
}

pub fn reduce_gateway_ws_events_to_client_events(
    events: impl IntoIterator<Item = GatewayWsEvent>,
    context: ClientRuntimeWsEventContext,
) -> Vec<ClientEvent> {
    events
        .into_iter()
        .map(|event| reduce_gateway_ws_event(event, context))
        .flat_map(|event| match event {
            ClientRuntimeWsEvent::Connection(reduction) => {
                vec![ClientEvent::GatewayConnectionChanged(reduction.into())]
            }
            ClientRuntimeWsEvent::Notification(notification) => {
                let voice_reduction = match &notification {
                    GatewayNotification::VoiceSessionResult(notification) => {
                        Some(ClientEvent::VoiceSessionResultReduced(
                            reduce_voice_session_result_notification(notification),
                        ))
                    }
                    _ => None,
                };
                std::iter::once(ClientEvent::GatewayNotification(notification))
                    .chain(voice_reduction)
                    .collect()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::ClientGatewayConnectRequest;
    use crate::ClientFfiVoiceAudioChunkParams;

    #[test]
    fn mobile_connect_contract_remains_endpoint_bearer_and_timings_only() {
        let value = serde_json::json!({
            "endpoint": {
                "id": "local-mobile-profile",
                "name": "Local",
                "address": "127.0.0.1:17878",
                "kind": "local",
                "auth_token_ref": null,
                "workspace_id": null,
                "service_name": null
            },
            "auth_token": "legacy-superuser-bearer",
            "timings": {
                "connect_timeout_ms": 5000,
                "ping_interval_ms": 15000,
                "pong_timeout_ms": 10000,
                "reconnect_initial_ms": 500,
                "reconnect_max_ms": 30000,
                "reconnect_jitter_percent": 20
            }
        });

        let request: ClientGatewayConnectRequest =
            serde_json::from_value(value.clone()).expect("existing Pioneer App connect contract");
        assert_eq!(request.endpoint.address, "127.0.0.1:17878");
        assert_eq!(
            request.auth_token.as_deref(),
            Some("legacy-superuser-bearer")
        );

        let encoded =
            serde_json::to_value(&request).expect("connect request should preserve its wire shape");
        let object = encoded
            .as_object()
            .expect("connect request should encode as an object");
        assert_eq!(object.len(), 3);
        assert!(object.contains_key("endpoint"));
        assert!(object.contains_key("auth_token"));
        assert!(object.contains_key("timings"));
        assert!(!object.contains_key("gateway_id"));
        assert!(!object.contains_key("principal_id"));
        assert_eq!(encoded["endpoint"]["id"], "local-mobile-profile");
    }

    #[test]
    fn mobile_nitro_voice_array_buffer_uses_the_shared_binary_frame_contract() {
        let input = serde_json::json!({
            "session_id": "voice_mobile_binary_1",
            "sequence": 7,
            "audio_format": {
                "sample_rate_hz": 16000,
                "channels": 1,
                "encoding": "pcm_s16_le"
            },
            "captured_at_unix_ms": 1_725_000_000_020_u64,
            "duration_ms": 20
        });
        let params: ClientFfiVoiceAudioChunkParams =
            serde_json::from_value(input).expect("Pioneer App voice JSON contract");
        let array_buffer_bytes = [0x00, 0x80, 0xff, 0x7f];
        let frame = pioneer_client::transport::ws::frames::encode_voice_audio_chunk_frame(
            params.session_id,
            params.sequence,
            params.audio_format,
            params.captured_at_unix_ms,
            params.duration_ms,
            array_buffer_bytes.as_slice(),
        )
        .expect("Nitro ArrayBuffer bytes should enter the shared voice frame encoder");

        let decoded = pioneer_protocol::decode_voice_chunk_frame(frame.as_slice())
            .expect("Gateway-compatible voice frame");
        assert_eq!(decoded.header.session_id, "voice_mobile_binary_1");
        assert_eq!(decoded.header.sequence, 7);
        assert_eq!(decoded.audio_payload, array_buffer_bytes);
    }
}
