use pioneer_client::settings::voice::{
    VoiceInputSettingsPlan, VoiceInputSettingsPlanRequest, VoiceInputStatusReduction,
};
use pioneer_client::{
    gateway::timings::{GatewayTimingError, GatewayWsTimings},
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
    use super::*;
    use crate::ClientFfiVoiceAudioChunkParams;
    use pioneer_protocol::{JSONRPC_VERSION, JsonRpcNotification, constants::events};
    use serde_json::json;

    #[test]
    fn access_changed_event_round_trip_is_additive_and_payload_safe() {
        let notification =
            GatewayNotification::AccessChanged(pioneer_protocol::AccessChangedNotification {
                authorization_revision: 11,
                workspace_id: "workspace_a".to_owned(),
                thread_id: Some("thread_a".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: pioneer_protocol::AccessChangeKind::ThreadParticipantRemoved,
            });
        let event = ClientEvent::GatewayNotification(notification);
        let encoded = serde_json::to_value(&event).expect("client event JSON");
        assert_eq!(
            encoded,
            json!({
                "GatewayNotification": {
                    "kind": "access_changed",
                    "params": {
                        "authorization_revision": 11,
                        "workspace_id": "workspace_a",
                        "thread_id": "thread_a",
                        "outcome": "revoked",
                        "change": "thread_participant_removed"
                    }
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<ClientEvent>(encoded).expect("client event round-trip"),
            event
        );
    }

    #[test]
    fn future_access_change_kind_degrades_to_unknown_notification_at_bridge() {
        let notification = GatewayNotification::from_jsonrpc(JsonRpcNotification {
            jsonrpc: JSONRPC_VERSION.to_owned(),
            method: events::ACCESS_CHANGED.to_owned(),
            params: Some(json!({
                "authorization_revision": 12,
                "workspace_id": "workspace_a",
                "change": "future_access_change"
            })),
        })
        .expect("notification fallback");

        let events = reduce_gateway_ws_events_to_client_events(
            [GatewayWsEvent::Notification {
                connection_id: 1,
                notification,
            }],
            ClientRuntimeWsEventContext::default(),
        );

        assert!(matches!(
            events.as_slice(),
            [ClientEvent::GatewayNotification(GatewayNotification::Unknown(
                unknown
            ))] if unknown.method == events::ACCESS_CHANGED
        ));
    }

    #[test]
    fn epic5_administration_events_cross_the_bridge_additively_without_secrets() {
        let events = [
            GatewayNotification::InvitationChanged(
                pioneer_protocol::InvitationChangedNotification {
                    revision: 1,
                    invitation_id: pioneer_protocol::InvitationId::new("IAAAAAAAAAAAAAAAAAAAA")
                        .expect("valid invitation id"),
                },
            ),
            GatewayNotification::MemberChanged(pioneer_protocol::MemberChangedNotification {
                revision: 2,
                principal_id: pioneer_protocol::PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA")
                    .expect("valid principal id"),
            }),
            GatewayNotification::WorkspaceMembersChanged(
                pioneer_protocol::WorkspaceMembersChangedNotification {
                    revision: 3,
                    workspace_id: pioneer_protocol::WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA")
                        .expect("valid workspace id"),
                },
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, notification)| GatewayWsEvent::Notification {
            connection_id: u64::try_from(index + 1).unwrap(),
            notification,
        });

        let reduced = reduce_gateway_ws_events_to_client_events(
            events,
            ClientRuntimeWsEventContext::default(),
        );
        let encoded = serde_json::to_string(&reduced).expect("bridge event JSON");

        assert_eq!(reduced.len(), 3);
        assert!(encoded.contains("invitation_changed"));
        assert!(encoded.contains("member_changed"));
        assert!(encoded.contains("workspace_members_changed"));
        assert!(!encoded.contains("pinv1_"));
        assert!(!encoded.contains("pioneer://invite"));
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
