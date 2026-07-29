use pioneer_client::gateway::{
    runtime::GatewayProfileError,
    setup::{
        ActivateGatewayRegistryPlan, AddRemoteGatewayApplyMode, AddRemoteGatewayInput,
        DeleteRemoteGatewayRegistryInput, DeleteRemoteGatewayRegistryPlan, RemoteGatewayValidation,
        RemoteGatewayValidationError, SetGatewayWorkspaceRegistryPlan,
        UpdateRemoteGatewayRegistryInput, UpdateRemoteGatewayRegistryPlan,
        generated_remote_gateway_endpoint_id, plan_activate_gateway_registry,
        plan_add_remote_gateway, plan_delete_remote_gateway_registry,
        plan_set_gateway_workspace_registry, plan_update_remote_gateway_registry,
        validate_remote_gateway_address,
    },
    types::{GatewayEndpoint, GatewayRegistry},
};
use pioneer_client::{
    providers::list::order_transcription_selector_models,
    rpc::JsonRpcRequestTransport,
    settings::voice::{voice_input_settings_plan, voice_input_status_reduction},
};
use pioneer_protocol::{
    GatewaySettingsGetResponse, GatewaySettingsUpdateResponse, ProviderListModelsParams,
    ProviderListModelsResponse,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::contracts::{
    ClientGatewaySettingsGetRequest, ClientGatewaySettingsUpdateRequest,
    ClientVoiceInputPlanRequest, ClientVoiceInputPlanResult,
};

pub const CLIENT_NOT_INITIALIZED_CODE: &str = "client_not_initialized";
pub const GATEWAY_DISCONNECTED_CODE: &str = "gateway_disconnected";
pub const INVALID_GATEWAY_SETTINGS_REQUEST_CODE: &str = "invalid_gateway_settings_request";
pub const INVALID_TRANSCRIPTION_MODELS_REQUEST_CODE: &str = "invalid_transcription_models_request";
pub const INVALID_VOICE_INPUT_PLAN_REQUEST_CODE: &str = "invalid_voice_input_plan_request";
pub const VOICE_RECONFIGURATION_BUSY_CODE: &str = "voice_reconfiguration_busy";

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemoteGatewayValidationRequest {
    pub address: String,
    pub timeout_ms: u64,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanAddRemoteGatewayRequest {
    pub registry: GatewayRegistry,
    pub name: String,
    pub address: String,
    pub new_endpoint_id: Option<String>,
    pub default_remote_name: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct AddRemoteGatewayPlan {
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: Option<GatewayEndpoint>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize)]
pub struct AddAndActivateRemoteGatewayRegistryPlan {
    pub registry: GatewayRegistry,
    pub endpoint: GatewayEndpoint,
    pub previous_endpoint: Option<GatewayEndpoint>,
    pub previous_active_gateway_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanActivateGatewayRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanSetGatewayWorkspaceRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
    #[serde(default)]
    pub workspace_id: Option<String>,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanUpdateRemoteGatewayRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
    pub name: String,
    pub address: String,
    pub default_remote_name: String,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanDeleteRemoteGatewayRequest {
    pub registry: GatewayRegistry,
    pub gateway_id: String,
    #[serde(default)]
    pub local_gateway_id: Option<String>,
}

pub fn validate_remote_gateway_request(
    request: &RemoteGatewayValidationRequest,
) -> Result<RemoteGatewayValidation, RemoteGatewayValidationError> {
    if request.timeout_ms == 0 {
        return Err(RemoteGatewayValidationError::InvalidTimeout {
            timeout_ms: request.timeout_ms,
        });
    }

    validate_remote_gateway_address(
        request.address.as_str(),
        Duration::from_millis(request.timeout_ms),
    )
}

pub fn plan_add_remote_gateway_request(
    request: PlanAddRemoteGatewayRequest,
) -> Result<AddRemoteGatewayPlan, GatewayProfileError> {
    let new_endpoint_id = request
        .new_endpoint_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(generated_remote_gateway_endpoint_id);

    let change = plan_add_remote_gateway(
        &request.registry,
        AddRemoteGatewayInput {
            name: request.name.as_str(),
            address: request.address.as_str(),
            new_endpoint_id,
            default_remote_name: request.default_remote_name,
        },
    )?;

    Ok(AddRemoteGatewayPlan {
        endpoint: change.endpoint().clone(),
        previous_endpoint: change.previous_endpoint().cloned(),
    })
}

pub fn plan_add_and_activate_remote_gateway_registry_request(
    request: PlanAddRemoteGatewayRequest,
) -> Result<AddAndActivateRemoteGatewayRegistryPlan, GatewayProfileError> {
    let new_endpoint_id = request
        .new_endpoint_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(generated_remote_gateway_endpoint_id);

    let mut registry = request.registry;
    let change = plan_add_remote_gateway(
        &registry,
        AddRemoteGatewayInput {
            name: request.name.as_str(),
            address: request.address.as_str(),
            new_endpoint_id,
            default_remote_name: request.default_remote_name,
        },
    )?;
    let commit =
        change.apply_to_registry(&mut registry, AddRemoteGatewayApplyMode::ActivateEndpoint)?;

    Ok(AddAndActivateRemoteGatewayRegistryPlan {
        registry,
        endpoint: commit.endpoint,
        previous_endpoint: commit.previous_endpoint,
        previous_active_gateway_id: commit.previous_active_gateway_id,
    })
}

pub fn plan_activate_gateway_registry_request(
    request: PlanActivateGatewayRequest,
) -> Result<ActivateGatewayRegistryPlan, GatewayProfileError> {
    plan_activate_gateway_registry(&request.registry, request.gateway_id.as_str())
}

pub fn plan_set_gateway_workspace_registry_request(
    request: PlanSetGatewayWorkspaceRequest,
) -> Result<SetGatewayWorkspaceRegistryPlan, GatewayProfileError> {
    plan_set_gateway_workspace_registry(
        &request.registry,
        request.gateway_id.as_str(),
        request.workspace_id,
    )
}

pub fn plan_update_remote_gateway_registry_request(
    request: PlanUpdateRemoteGatewayRequest,
) -> Result<UpdateRemoteGatewayRegistryPlan, GatewayProfileError> {
    let PlanUpdateRemoteGatewayRequest {
        registry,
        gateway_id,
        name,
        address,
        default_remote_name,
    } = request;

    plan_update_remote_gateway_registry(
        &registry,
        UpdateRemoteGatewayRegistryInput {
            gateway_id: gateway_id.as_str(),
            name: name.as_str(),
            address: address.as_str(),
            default_remote_name,
        },
    )
}

pub fn plan_delete_remote_gateway_registry_request(
    request: PlanDeleteRemoteGatewayRequest,
) -> Result<DeleteRemoteGatewayRegistryPlan, GatewayProfileError> {
    plan_delete_remote_gateway_registry(
        &request.registry,
        DeleteRemoteGatewayRegistryInput {
            gateway_id: request.gateway_id.as_str(),
            local_gateway_id: request.local_gateway_id.as_deref(),
        },
    )
}

pub fn gateway_settings_get_for_bridge<TTransport>(
    transport: &TTransport,
    _request: ClientGatewaySettingsGetRequest,
) -> anyhow::Result<GatewaySettingsGetResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    pioneer_client::transport::ws::command_sender::gateway_settings_get(transport)
}

pub fn gateway_settings_update_for_bridge<TTransport>(
    transport: &TTransport,
    request: ClientGatewaySettingsUpdateRequest,
) -> anyhow::Result<GatewaySettingsUpdateResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    pioneer_client::transport::ws::command_sender::gateway_settings_update(
        transport,
        request.update,
    )
}

pub fn gateway_settings_error_code(message: &str) -> &'static str {
    if message.contains(VOICE_RECONFIGURATION_BUSY_CODE) {
        VOICE_RECONFIGURATION_BUSY_CODE
    } else {
        crate::ClientFfiError::GENERIC_CODE
    }
}

pub fn provider_list_transcription_models_for_bridge<TTransport>(
    transport: &TTransport,
    params: ProviderListModelsParams,
) -> anyhow::Result<ProviderListModelsResponse>
where
    TTransport: JsonRpcRequestTransport + ?Sized,
{
    let mut response =
        pioneer_client::transport::ws::command_sender::provider_list_transcription_models(
            transport, params,
        )?;
    order_transcription_selector_models(response.models.as_mut_slice());
    Ok(response)
}

pub fn voice_input_plan_for_bridge(
    request: ClientVoiceInputPlanRequest,
) -> ClientVoiceInputPlanResult {
    match request {
        ClientVoiceInputPlanRequest::SettingsAction { request } => {
            ClientVoiceInputPlanResult::SettingsAction {
                plan: voice_input_settings_plan(request),
            }
        }
        ClientVoiceInputPlanRequest::StatusReduction { current } => {
            ClientVoiceInputPlanResult::StatusReduction {
                reduction: voice_input_status_reduction(&current),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::rpc::JsonRpcResponseSender;
    use pioneer_protocol::{
        GatewayGeneralSettings, GatewayMemorySettings, GatewaySettingsSnapshot,
        GatewayVoiceInputProvider, GatewayVoiceInputRuntimePhase, GatewayVoiceInputRuntimeSnapshot,
        GatewayVoiceInputSettings, ProviderModelCapabilities, ProviderModelInfo,
        ProviderModelLimits, ProviderTranscriptionModelMetadata,
    };
    use serde_json::{Value as JsonValue, json};
    use std::sync::Mutex;

    struct ImmediateTransport {
        response: Mutex<Option<Result<JsonValue, String>>>,
        request: Mutex<Option<JsonValue>>,
    }

    impl ImmediateTransport {
        fn success(response: JsonValue) -> Self {
            Self {
                response: Mutex::new(Some(Ok(response))),
                request: Mutex::new(None),
            }
        }

        fn error(message: impl Into<String>) -> Self {
            Self {
                response: Mutex::new(Some(Err(message.into()))),
                request: Mutex::new(None),
            }
        }

        fn request(&self) -> JsonValue {
            self.request
                .lock()
                .expect("request lock")
                .clone()
                .expect("request payload")
        }
    }

    impl JsonRpcRequestTransport for ImmediateTransport {
        fn send_json_rpc_request(
            &self,
            _request_id: String,
            payload: String,
            response_tx: JsonRpcResponseSender,
        ) -> Result<(), String> {
            *self.request.lock().expect("request lock") =
                Some(serde_json::from_str(payload.as_str()).expect("JSON-RPC request payload"));
            response_tx
                .send(
                    self.response
                        .lock()
                        .expect("response lock")
                        .take()
                        .expect("single response"),
                )
                .map_err(|error| error.to_string())
        }
    }

    fn settings_snapshot() -> GatewaySettingsSnapshot {
        GatewaySettingsSnapshot {
            general: GatewayGeneralSettings::default(),
            memory: GatewayMemorySettings::default(),
            self_improvement: Default::default(),
            thread_episodic: Default::default(),
            cli_runtimes: Default::default(),
            remote_access: Default::default(),
            voice_input: GatewayVoiceInputSettings {
                enabled: true,
                provider: Some(GatewayVoiceInputProvider::Local),
                model: Some("parakeet-tdt-0.6b-v3".to_owned()),
                runtime: GatewayVoiceInputRuntimeSnapshot {
                    phase: GatewayVoiceInputRuntimePhase::Downloading,
                    effective_enabled: false,
                    model: Some("parakeet-tdt-0.6b-v3".to_owned()),
                    downloaded_bytes: Some(512),
                    total_bytes: Some(1024),
                    error: None,
                },
            },
        }
    }

    fn transcription_model(id: &str, recommended: bool) -> ProviderModelInfo {
        ProviderModelInfo {
            id: id.to_owned(),
            name: Some(id.to_owned()),
            description: None,
            created: None,
            provider: "local".to_owned(),
            owned_by: None,
            limits: ProviderModelLimits::default(),
            capabilities: ProviderModelCapabilities {
                transcription: Some(true),
                ..Default::default()
            },
            transcription: Some(ProviderTranscriptionModelMetadata {
                engine: "test".to_owned(),
                download_size_mb: 1,
                accuracy_score: 1,
                speed_score: 1,
                supports_translation: false,
                supported_languages: vec!["en".to_owned()],
                supports_language_selection: false,
                recommended,
            }),
            pricing: None,
            active: Some(true),
            family: Some("test".to_owned()),
            lifecycle_status: None,
        }
    }

    #[test]
    fn gateway_settings_get_bridge_preserves_voice_runtime_fields() {
        let transport = ImmediateTransport::success(
            serde_json::to_value(GatewaySettingsGetResponse {
                settings: settings_snapshot(),
            })
            .expect("settings response JSON"),
        );

        let response =
            gateway_settings_get_for_bridge(&transport, ClientGatewaySettingsGetRequest::default())
                .expect("settings get");

        assert_eq!(
            response.settings.voice_input.runtime.phase,
            GatewayVoiceInputRuntimePhase::Downloading
        );
        assert_eq!(
            response.settings.voice_input.runtime.downloaded_bytes,
            Some(512)
        );
        assert_eq!(
            transport.request()["method"],
            pioneer_protocol::constants::methods::SETTINGS_GET
        );
    }

    #[test]
    fn gateway_settings_update_bridge_sends_full_typed_update() {
        let transport = ImmediateTransport::success(
            serde_json::to_value(GatewaySettingsUpdateResponse {
                settings: settings_snapshot(),
            })
            .expect("settings response JSON"),
        );
        let request: ClientGatewaySettingsUpdateRequest = serde_json::from_value(json!({
            "update": {
                "voice_input": {
                    "enabled": true,
                    "provider": "local",
                    "model": "parakeet-tdt-0.6b-v3",
                    "retry_install": true
                }
            }
        }))
        .expect("typed settings update request");

        let response =
            gateway_settings_update_for_bridge(&transport, request).expect("settings update");

        assert_eq!(
            response.settings.voice_input.runtime.total_bytes,
            Some(1024)
        );
        let payload = transport.request();
        assert_eq!(
            payload["method"],
            pioneer_protocol::constants::methods::SETTINGS_UPDATE
        );
        assert_eq!(
            payload["params"]["update"]["voice_input"]["retry_install"],
            true
        );
    }

    #[test]
    fn gateway_settings_bridge_preserves_typed_voice_conflict_code() {
        let transport = ImmediateTransport::error(
            "voice_reconfiguration_busy: voice input cannot be reconfigured while a voice session is active",
        );
        let error = gateway_settings_update_for_bridge(
            &transport,
            ClientGatewaySettingsUpdateRequest {
                update: pioneer_protocol::GatewaySettingsUpdate::default(),
            },
        )
        .expect_err("gateway error");

        assert_eq!(
            gateway_settings_error_code(error.to_string().as_str()),
            VOICE_RECONFIGURATION_BUSY_CODE
        );
    }

    #[test]
    fn provider_list_transcription_models_bridge_returns_ordered_safe_catalog() {
        let expected_ids = [
            "small",
            "medium",
            "turbo",
            "large",
            "breeze-asr",
            "parakeet-tdt-0.6b-v2",
            "parakeet-tdt-0.6b-v3",
            "moonshine-base",
            "moonshine-tiny-streaming-en",
            "moonshine-small-streaming-en",
            "moonshine-medium-streaming-en",
            "sense-voice-int8",
            "gigaam-v3-e2e-ctc",
            "canary-180m-flash",
            "canary-1b-v2",
            "cohere-int8",
        ];
        let models = expected_ids
            .iter()
            .map(|id| transcription_model(id, *id == "parakeet-tdt-0.6b-v3"))
            .collect();
        let transport = ImmediateTransport::success(
            serde_json::to_value(ProviderListModelsResponse {
                provider: "local".to_owned(),
                models,
            })
            .expect("catalog response JSON"),
        );

        let response = provider_list_transcription_models_for_bridge(
            &transport,
            ProviderListModelsParams {
                workspace_id: "workspace-1".to_owned(),
                provider: "local".to_owned(),
            },
        )
        .expect("transcription models");

        assert_eq!(response.models.len(), expected_ids.len());
        assert_eq!(response.models[0].id, "parakeet-tdt-0.6b-v3");
        let mut actual_ids = response
            .models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>();
        actual_ids.sort_unstable();
        let mut expected_ids = expected_ids.to_vec();
        expected_ids.sort_unstable();
        assert_eq!(actual_ids, expected_ids);

        let public_json = serde_json::to_string(&response).expect("public catalog JSON");
        for private_field in ["url", "sha256", "checksum", "artifact", "install_dir"] {
            assert!(!public_json.contains(private_field));
        }
        assert_eq!(
            transport.request()["method"],
            pioneer_protocol::constants::methods::PROVIDER_TRANSCRIPTION_MODELS_LIST
        );
    }

    #[test]
    fn voice_input_plan_bridge_round_trips_every_shared_operation() {
        let disabled = settings_snapshot().voice_input;
        let mut selected = disabled.clone();
        selected.runtime.phase = GatewayVoiceInputRuntimePhase::Ready;
        let mut failed = selected.clone();
        failed.runtime.phase = GatewayVoiceInputRuntimePhase::Failed;

        let requests = [
            json!({
                "operation": "settings_action",
                "request": {"current": disabled, "action": {"kind": "enable"}}
            }),
            json!({
                "operation": "settings_action",
                "request": {
                    "current": selected,
                    "action": {"kind": "select", "provider": "local", "model": "small"}
                }
            }),
            json!({
                "operation": "settings_action",
                "request": {"current": settings_snapshot().voice_input, "action": {"kind": "disable"}}
            }),
            json!({
                "operation": "settings_action",
                "request": {"current": failed, "action": {"kind": "retry"}}
            }),
            json!({
                "operation": "status_reduction",
                "current": settings_snapshot().voice_input
            }),
        ];

        for request in requests {
            let request: ClientVoiceInputPlanRequest =
                serde_json::from_value(request).expect("typed plan request");
            let request = serde_json::to_string(&request)
                .and_then(|json| serde_json::from_str(json.as_str()))
                .expect("request JSON round-trip");
            let result = voice_input_plan_for_bridge(request);
            let encoded = serde_json::to_string(&result).expect("plan result JSON");
            let _: ClientVoiceInputPlanResult =
                serde_json::from_str(encoded.as_str()).expect("result JSON round-trip");
        }
    }

    #[test]
    fn voice_input_plan_bridge_rejects_invalid_selection_deterministically() {
        let request: ClientVoiceInputPlanRequest = serde_json::from_value(json!({
            "operation": "settings_action",
            "request": {
                "current": settings_snapshot().voice_input,
                "action": {"kind": "select", "provider": "remote", "model": "model"}
            }
        }))
        .expect("typed invalid selection");

        let ClientVoiceInputPlanResult::SettingsAction { plan } =
            voice_input_plan_for_bridge(request)
        else {
            panic!("settings action result")
        };
        assert!(matches!(
            plan,
            pioneer_client::settings::voice::VoiceInputSettingsPlan::Rejected {
                reason: pioneer_client::settings::voice::VoiceInputSettingsPlanRejection::InvalidProvider
            }
        ));
    }
}
