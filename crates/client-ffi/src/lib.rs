//! C ABI boundary for native Pioneer client shells.
//!
//! This crate intentionally owns ABI/runtime glue and shell-boundary DTOs.
//! Client domain logic remains in `pioneer-client`.

mod active_thread;
mod composer;
mod contracts;
mod diagnostics;
mod gateway;
mod pending_requests;
#[cfg(feature = "schema")]
pub mod schema;
mod threads;
mod timeline;
mod workspaces;

use active_thread::{
    ClientActiveThreadCancelTurnRequest, ClientActiveThreadCancelTurnResult,
    ClientActiveThreadClearResult, ClientActiveThreadEventRequest, ClientActiveThreadEventResult,
    ClientActiveThreadOpenByIdRequest, ClientActiveThreadOpenRequest,
    ClientActiveThreadSendTextRequest, ClientActiveThreadSendTextResult,
    ClientActiveThreadSnapshot, ClientActiveThreadSnapshotRequest,
    ClientActiveThreadUnsubscribeRequest, ClientActiveThreadUnsubscribeResult,
    ClientEnsureWorkspaceDraftRequest, ClientFfiActiveThreadState,
    ClientPrepareVoiceComposerSnapshotRequest,
};
use composer::{
    ClientComposerAttachmentFromPathRequest, ClientComposerAttachmentsUpdateRequest,
    ClientComposerCapabilitiesUpdateRequest, ClientComposerCapabilityMenuVisibilityRequest,
    ClientComposerCapabilityTargetRequest, ClientComposerDomainTransitionRequest,
    ClientComposerDraftLifecycleTransitionRequest, ClientComposerFilterMcpRowsRequest,
    ClientComposerFilterMcpRowsResult, ClientComposerFilterSkillRowsRequest,
    ClientComposerMcpCapabilityFromRowRequest, ClientComposerMcpPickerRowsRequest,
    ClientComposerMcpPickerRowsResult, ClientComposerMcpToggleRequest,
    ClientComposerMcpToggleResult, ClientComposerSkillCapabilityFromRowRequest,
    ClientComposerSkillPickerRowsRequest, ClientComposerSkillRowsForTargetRequest,
    ClientComposerSkillToggleRequest, ClientComposerSkillToggleResult,
    ClientComposerSubmissionPlanRequest, composer_attachment_from_path_request,
    composer_capability_menu, composer_capability_target, composer_domain_transition,
    composer_draft_lifecycle_transition, composer_mcp_picker_rows, composer_skill_picker_rows,
    composer_skill_rows_for_target, composer_submission_plan, filter_mcp_picker_rows,
    filter_skill_picker_rows, mcp_capability_from_row, skill_capability_from_row,
    toggle_mcp_picker_selection, toggle_skill_picker_selection, update_composer_attachments,
    update_composer_capabilities,
};
use contracts::{
    ClientEvent, ClientGatewayConnectRequest, ClientGatewayConnectResult,
    ClientGatewaySettingsGetRequest, ClientGatewaySettingsUpdateRequest,
    ClientVoiceInputPlanRequest, ClientVoiceInputPlanResult,
    reduce_gateway_ws_events_to_client_events,
};
use diagnostics::{ClientDiagnosticEvent, ClientFfiDiagnostics};
use gateway::{
    AddAndActivateRemoteGatewayRegistryPlan, AddRemoteGatewayPlan, PlanActivateGatewayRequest,
    PlanAddRemoteGatewayRequest, PlanDeleteRemoteGatewayRequest, PlanSetGatewayWorkspaceRequest,
    PlanUpdateRemoteGatewayRequest, RemoteGatewayValidationRequest, gateway_settings_error_code,
    gateway_settings_get_for_bridge, gateway_settings_update_for_bridge,
    plan_activate_gateway_registry_request, plan_add_and_activate_remote_gateway_registry_request,
    plan_add_remote_gateway_request, plan_delete_remote_gateway_registry_request,
    plan_set_gateway_workspace_registry_request, plan_update_remote_gateway_registry_request,
    provider_list_transcription_models_for_bridge, validate_remote_gateway_request,
    voice_input_plan_for_bridge,
};
use pending_requests::{
    ClientPendingRequestPresentationRequest, ClientPendingRequestPresentationResult,
    ClientPendingRequestResponsePlanRequest, ClientPendingRequestResponsePlanResult,
    pending_request_presentation_for_bridge, plan_pending_request_response_for_bridge,
};
use pioneer_client::{
    agents_doc::content::{
        AgentsDocSaveErrorKind, agents_doc_get_params, agents_doc_save_error_kind,
        agents_doc_save_params,
    },
    gateway::{
        runtime::{self as client_gateway_runtime, GatewayProfileError},
        secrets::GatewayAuthTokenRef,
        setup::{
            ActivateGatewayRegistryPlan, DeleteRemoteGatewayRegistryPlan, RemoteGatewayValidation,
            SetGatewayWorkspaceRegistryPlan, UpdateRemoteGatewayRegistryPlan,
        },
    },
    providers::{
        list::{
            cli_runtime_list_models_params,
            provider_models_response_from_cli_runtime_models_response,
            runtime_id_from_cli_runtime_provider_key,
        },
        presentation::{
            ProviderModelDisplayKey, ProviderModelDisplayResolution, ReasoningEffortRowsRequest,
            ReasoningEffortRowsResponse, provider_model_display_key,
            provider_model_display_models_params, reasoning_effort_rows_from_request,
            resolve_provider_model_display_from_response,
        },
    },
    runtime::ClientRuntime,
    timeline::semantic::{TopLevelPageMergeMode, WorkPageMergeMode},
    workspaces::{
        actions::WorkspaceBootstrapSuccessReduction,
        bootstrap::{WorkspaceBootstrapRequest, bootstrap_workspace_catalog},
    },
};
use pioneer_protocol::{
    CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse, CLIRuntimeListParams,
    CLIRuntimeListResponse, CLIRuntimeRefreshParams, CLIRuntimeRefreshResponse,
    CLIRuntimeRequestRespondParams, CLIRuntimeRequestRespondResponse, CLIRuntimeReviewStartParams,
    CLIRuntimeReviewStartResponse, CLIRuntimeThreadBindingGetParams,
    CLIRuntimeThreadBindingGetResponse, CLIRuntimeThreadCompactParams,
    CLIRuntimeThreadCompactResponse, CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse,
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ThreadAgentsDocArchiveParams, ThreadAgentsDocArchiveResponse, ThreadAgentsDocGetParams,
    ThreadAgentsDocGetResponse, ThreadAgentsDocSaveParams, ThreadAgentsDocSaveResponse,
    ThreadTimelinePageParams, ThreadTimelinePageResponse, TimelinePageAnchor,
    TurnPermissionRequestRespondParams, TurnPermissionRequestRespondResponse, TurnWorkPageParams,
    TurnWorkPageResponse, VoiceAudioFormat, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionStartParams,
    VoiceSessionStartResponse, VoiceStatusParams, VoiceStatusResponse,
};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    ffi::{CStr, CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::Mutex,
};
use threads::{
    ClientThreadTreeLevel, ClientThreadTreeQueryData, ThreadTreeLevelRequest,
    ThreadTreeRefreshRequest, client_thread_tree_level, refresh_thread_tree,
};
use timeline::{thread_timeline_page, turn_work_page};
use workspaces::{
    WorkspaceCreateRequest, WorkspaceCreateResult, WorkspaceRenameRequest, WorkspaceRenameResult,
    WorkspaceSwitchRequest, WorkspaceSwitchResult, create_workspace, rename_workspace,
    switch_workspace,
};

const FFI_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct PioneerClientFfi {
    runtime: ClientFfiRuntime,
}

#[derive(Default)]
struct ClientFfiRuntime {
    config: Mutex<Option<ClientFfiConfig>>,
    client_runtime: ClientRuntime,
    active_thread: ClientFfiActiveThreadState,
    active_connection_id: Mutex<Option<u64>>,
    diagnostics: ClientFfiDiagnostics,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientFfiConfig {
    pub app_data_dir: Option<String>,
    pub locale: Option<String>,
    pub platform: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientFfiInitializeResult {
    pub initialized: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientFfiGatewayDisconnectResult {
    pub disconnected: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientFfiVoiceAudioChunkParams {
    pub session_id: String,
    pub sequence: u64,
    pub audio_format: VoiceAudioFormat,
    pub captured_at_unix_ms: Option<u64>,
    pub duration_ms: Option<u32>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientFfiVoiceAudioChunkResult {
    pub sent: bool,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum FfiResponse<T> {
    Ok {
        value: T,
    },
    Error {
        message: String,
        code: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClientFfiError {
    message: String,
    code: &'static str,
}

impl ClientFfiError {
    pub(crate) const GENERIC_CODE: &'static str = "pioneer_client_ffi_error";

    pub(crate) fn new(message: impl Into<String>, code: &'static str) -> Self {
        Self {
            message: message.into(),
            code,
        }
    }
}

impl PioneerClientFfi {
    fn new() -> Self {
        install_rustls_crypto_provider();

        Self {
            runtime: ClientFfiRuntime::default(),
        }
    }
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

impl ClientFfiRuntime {
    fn initialize(&self, config_json: &str) -> Result<ClientFfiInitializeResult, String> {
        let config = if config_json.trim().is_empty() {
            ClientFfiConfig::default()
        } else {
            serde_json::from_str::<ClientFfiConfig>(config_json)
                .map_err(|error| format!("invalid client ffi config: {error}"))?
        };

        let _client_contract_count = pioneer_client::schema::public_client_schema_contracts().len();
        *self
            .config
            .lock()
            .map_err(|_| "client ffi config lock is poisoned".to_owned())? = Some(config);

        Ok(ClientFfiInitializeResult { initialized: true })
    }

    fn gateway_validate_remote(&self, input_json: &str) -> Result<RemoteGatewayValidation, String> {
        let request = serde_json::from_str::<RemoteGatewayValidationRequest>(input_json)
            .map_err(|error| format!("invalid gateway validation request: {error}"))?;

        validate_remote_gateway_request(&request).map_err(|error| error.to_string())
    }

    fn gateway_plan_add_remote(&self, input_json: &str) -> Result<AddRemoteGatewayPlan, String> {
        let request = serde_json::from_str::<PlanAddRemoteGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway add remote planning request: {error}"))?;

        plan_add_remote_gateway_request(request, gateway_auth_token_ref_for_endpoint)
            .map_err(|error| error.to_string())
    }

    fn gateway_plan_add_and_activate_remote_registry(
        &self,
        input_json: &str,
    ) -> Result<AddAndActivateRemoteGatewayRegistryPlan, String> {
        let request = serde_json::from_str::<PlanAddRemoteGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway add remote registry request: {error}"))?;

        plan_add_and_activate_remote_gateway_registry_request(
            request,
            gateway_auth_token_ref_for_endpoint,
        )
        .map_err(|error| error.to_string())
    }

    fn gateway_plan_activate_registry(
        &self,
        input_json: &str,
    ) -> Result<ActivateGatewayRegistryPlan, String> {
        let request = serde_json::from_str::<PlanActivateGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway activation request: {error}"))?;

        plan_activate_gateway_registry_request(request).map_err(|error| error.to_string())
    }

    fn gateway_plan_update_remote_registry(
        &self,
        input_json: &str,
    ) -> Result<UpdateRemoteGatewayRegistryPlan, String> {
        let request = serde_json::from_str::<PlanUpdateRemoteGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway update remote request: {error}"))?;

        plan_update_remote_gateway_registry_request(request, gateway_auth_token_ref_for_endpoint)
            .map_err(|error| error.to_string())
    }

    fn gateway_plan_delete_remote_registry(
        &self,
        input_json: &str,
    ) -> Result<DeleteRemoteGatewayRegistryPlan, String> {
        let request = serde_json::from_str::<PlanDeleteRemoteGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway delete remote request: {error}"))?;

        plan_delete_remote_gateway_registry_request(request).map_err(|error| error.to_string())
    }

    fn gateway_plan_set_workspace_registry(
        &self,
        input_json: &str,
    ) -> Result<SetGatewayWorkspaceRegistryPlan, String> {
        let request = serde_json::from_str::<PlanSetGatewayWorkspaceRequest>(input_json)
            .map_err(|error| format!("invalid gateway set workspace request: {error}"))?;

        plan_set_gateway_workspace_registry_request(request).map_err(|error| error.to_string())
    }

    fn gateway_connect(&self, input_json: &str) -> Result<ClientGatewayConnectResult, String> {
        let request = serde_json::from_str::<ClientGatewayConnectRequest>(input_json)
            .map_err(|error| format!("invalid gateway connect request: {error}"))?;

        let timings = request
            .timings
            .to_gateway_ws_timings()
            .map_err(|error| error.to_string())?;

        let plan = client_gateway_runtime::plan_gateway_connect_spec(
            &request.endpoint,
            request.auth_token,
            timings,
        );

        let connection_id = self
            .client_runtime
            .ws_command_sender()
            .connect_with_retry(plan.into())
            .map_err(|error| format!("{error:#}"))?;

        *self
            .active_connection_id
            .lock()
            .map_err(|_| "client ffi connection lock is poisoned".to_owned())? =
            Some(connection_id);

        Ok(ClientGatewayConnectResult { connection_id })
    }

    fn gateway_next_events(&self) -> Result<Vec<ClientEvent>, String> {
        loop {
            let active_connection_id = *self
                .active_connection_id
                .lock()
                .map_err(|_| "client ffi connection lock is poisoned".to_owned())?;

            if active_connection_id.is_none() {
                return Ok(Vec::new());
            }

            let Some(first_event) = self.client_runtime.recv_ws_event() else {
                return Ok(Vec::new());
            };

            let active_connection_id = *self
                .active_connection_id
                .lock()
                .map_err(|_| "client ffi connection lock is poisoned".to_owned())?;

            let events = self
                .client_runtime
                .drain_applicable_ws_events(active_connection_id, Some(first_event));
            let events = reduce_gateway_ws_events_to_client_events(events, Default::default());

            if !events.is_empty() {
                return Ok(events);
            }
        }
    }

    fn gateway_disconnect(&self) -> Result<ClientFfiGatewayDisconnectResult, String> {
        self.client_runtime
            .ws_command_sender()
            .disconnect()
            .map_err(|error| format!("{error:#}"))?;
        *self
            .active_connection_id
            .lock()
            .map_err(|_| "client ffi connection lock is poisoned".to_owned())? = None;
        Ok(ClientFfiGatewayDisconnectResult { disconnected: true })
    }

    fn gateway_settings_get(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::GatewaySettingsGetResponse, ClientFfiError> {
        let request = serde_json::from_str::<ClientGatewaySettingsGetRequest>(input_json).map_err(
            |error| {
                ClientFfiError::new(
                    format!("invalid gateway settings get request: {error}"),
                    gateway::INVALID_GATEWAY_SETTINGS_REQUEST_CODE,
                )
            },
        )?;
        self.require_initialized_and_connected()?;

        gateway_settings_get_for_bridge(&self.client_runtime.ws_command_sender(), request).map_err(
            |error| {
                let message = format!("{error:#}");
                ClientFfiError::new(
                    message.clone(),
                    gateway_settings_error_code(message.as_str()),
                )
            },
        )
    }

    fn gateway_settings_update(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::GatewaySettingsUpdateResponse, ClientFfiError> {
        let request = serde_json::from_str::<ClientGatewaySettingsUpdateRequest>(input_json)
            .map_err(|error| {
                ClientFfiError::new(
                    format!("invalid gateway settings update request: {error}"),
                    gateway::INVALID_GATEWAY_SETTINGS_REQUEST_CODE,
                )
            })?;
        self.require_initialized_and_connected()?;

        gateway_settings_update_for_bridge(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| {
                let message = format!("{error:#}");
                ClientFfiError::new(
                    message.clone(),
                    gateway_settings_error_code(message.as_str()),
                )
            })
    }

    fn require_initialized_and_connected(&self) -> Result<u64, ClientFfiError> {
        self.require_initialized()?;

        self.active_connection_id
            .lock()
            .map_err(|_| {
                ClientFfiError::new(
                    "client ffi connection lock is poisoned",
                    ClientFfiError::GENERIC_CODE,
                )
            })?
            .ok_or_else(|| {
                ClientFfiError::new(
                    "no active Gateway connection",
                    gateway::GATEWAY_DISCONNECTED_CODE,
                )
            })
    }

    fn require_initialized(&self) -> Result<(), ClientFfiError> {
        let initialized = self
            .config
            .lock()
            .map_err(|_| {
                ClientFfiError::new(
                    "client ffi config lock is poisoned",
                    ClientFfiError::GENERIC_CODE,
                )
            })?
            .is_some();
        if !initialized {
            return Err(ClientFfiError::new(
                "client ffi is not initialized",
                gateway::CLIENT_NOT_INITIALIZED_CODE,
            ));
        }
        Ok(())
    }

    fn workspace_bootstrap(
        &self,
        input_json: &str,
    ) -> Result<WorkspaceBootstrapSuccessReduction, String> {
        let request = serde_json::from_str::<WorkspaceBootstrapRequest>(input_json)
            .map_err(|error| format!("invalid workspace bootstrap request: {error}"))?;

        bootstrap_workspace_catalog(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| error.to_string())
    }

    fn workspace_switch(&self, input_json: &str) -> Result<WorkspaceSwitchResult, String> {
        let request = serde_json::from_str::<WorkspaceSwitchRequest>(input_json)
            .map_err(|error| format!("invalid workspace switch request: {error}"))?;

        switch_workspace(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn workspace_create(&self, input_json: &str) -> Result<WorkspaceCreateResult, String> {
        let request = serde_json::from_str::<WorkspaceCreateRequest>(input_json)
            .map_err(|error| format!("invalid workspace create request: {error}"))?;

        create_workspace(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn workspace_rename(&self, input_json: &str) -> Result<WorkspaceRenameResult, String> {
        let request = serde_json::from_str::<WorkspaceRenameRequest>(input_json)
            .map_err(|error| format!("invalid workspace rename request: {error}"))?;

        rename_workspace(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn provider_list(&self, input_json: &str) -> Result<ProviderListResponse, String> {
        let params = serde_json::from_str::<ProviderListParams>(input_json)
            .map_err(|error| format!("invalid provider list params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .provider_list(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_list(&self, input_json: &str) -> Result<CLIRuntimeListResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeListParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime list params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_list(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_refresh(&self, input_json: &str) -> Result<CLIRuntimeRefreshResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeRefreshParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime refresh params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_refresh(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_list_models(
        &self,
        input_json: &str,
    ) -> Result<CLIRuntimeListModelsResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeListModelsParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime list models params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_list_models(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_thread_binding_get(
        &self,
        input_json: &str,
    ) -> Result<CLIRuntimeThreadBindingGetResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeThreadBindingGetParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime thread binding get params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_thread_binding_get(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_thread_compact(
        &self,
        input_json: &str,
    ) -> Result<CLIRuntimeThreadCompactResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeThreadCompactParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime thread compact params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_thread_compact(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_turn_steer(
        &self,
        input_json: &str,
    ) -> Result<CLIRuntimeTurnSteerResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeTurnSteerParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime turn steer params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_turn_steer(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_review_start(
        &self,
        input_json: &str,
    ) -> Result<CLIRuntimeReviewStartResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeReviewStartParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime review start params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_review_start(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn cli_runtime_request_respond(
        &self,
        input_json: &str,
    ) -> Result<CLIRuntimeRequestRespondResponse, String> {
        let params = serde_json::from_str::<CLIRuntimeRequestRespondParams>(input_json)
            .map_err(|error| format!("invalid CLI runtime request respond params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .cli_runtime_request_respond(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn turn_permission_request_respond(
        &self,
        input_json: &str,
    ) -> Result<TurnPermissionRequestRespondResponse, String> {
        let params = serde_json::from_str::<TurnPermissionRequestRespondParams>(input_json)
            .map_err(|error| format!("invalid turn permission request respond params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .turn_permission_request_respond(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn voice_status(&self, input_json: &str) -> Result<VoiceStatusResponse, String> {
        let params = serde_json::from_str::<VoiceStatusParams>(input_json)
            .map_err(|error| format!("invalid voice status params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .voice_status(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn voice_session_start(&self, input_json: &str) -> Result<VoiceSessionStartResponse, String> {
        let params = serde_json::from_str::<VoiceSessionStartParams>(input_json)
            .map_err(|error| format!("invalid voice session start params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .voice_session_start(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn voice_audio_chunk(
        &self,
        input_json: &str,
        pcm_chunk: &[u8],
    ) -> Result<ClientFfiVoiceAudioChunkResult, String> {
        let params = serde_json::from_str::<ClientFfiVoiceAudioChunkParams>(input_json)
            .map_err(|error| format!("invalid voice audio chunk params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .send_voice_audio_chunk(
                params.session_id,
                params.sequence,
                params.audio_format,
                params.captured_at_unix_ms,
                params.duration_ms,
                pcm_chunk.to_vec(),
            )
            .map_err(|error| format!("{error:#}"))?;

        Ok(ClientFfiVoiceAudioChunkResult { sent: true })
    }

    fn voice_session_finalize(
        &self,
        input_json: &str,
    ) -> Result<VoiceSessionFinalizeResponse, String> {
        let params = serde_json::from_str::<VoiceSessionFinalizeParams>(input_json)
            .map_err(|error| format!("invalid voice session finalize params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .voice_session_finalize(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn voice_session_cancel(&self, input_json: &str) -> Result<VoiceSessionCancelResponse, String> {
        let params = serde_json::from_str::<VoiceSessionCancelParams>(input_json)
            .map_err(|error| format!("invalid voice session cancel params: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .voice_session_cancel(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn pending_request_response_plan(
        &self,
        input_json: &str,
    ) -> Result<ClientPendingRequestResponsePlanResult, String> {
        let request = serde_json::from_str::<ClientPendingRequestResponsePlanRequest>(input_json)
            .map_err(|error| {
            format!("invalid pending request response plan request: {error}")
        })?;

        plan_pending_request_response_for_bridge(request)
    }

    fn pending_request_presentation(
        &self,
        input_json: &str,
    ) -> Result<ClientPendingRequestPresentationResult, String> {
        let request = serde_json::from_str::<ClientPendingRequestPresentationRequest>(input_json)
            .map_err(|error| {
            format!("invalid pending request presentation request: {error}")
        })?;

        pending_request_presentation_for_bridge(request)
    }

    fn provider_list_models(&self, input_json: &str) -> Result<ProviderListModelsResponse, String> {
        let params = serde_json::from_str::<ProviderListModelsParams>(input_json)
            .map_err(|error| format!("invalid provider list models params: {error}"))?;

        if let Some(runtime_id) = runtime_id_from_cli_runtime_provider_key(params.provider.as_str())
        {
            let response = self
                .client_runtime
                .ws_command_sender()
                .cli_runtime_list_models(cli_runtime_list_models_params(
                    params.workspace_id,
                    runtime_id.to_owned(),
                ))
                .map_err(|error| format!("{error:#}"))?;

            return Ok(provider_models_response_from_cli_runtime_models_response(
                params.provider,
                response,
            ));
        }

        self.client_runtime
            .ws_command_sender()
            .provider_list_models(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn provider_list_transcription_models(
        &self,
        input_json: &str,
    ) -> Result<ProviderListModelsResponse, ClientFfiError> {
        let params =
            serde_json::from_str::<ProviderListModelsParams>(input_json).map_err(|error| {
                ClientFfiError::new(
                    format!("invalid provider transcription models params: {error}"),
                    gateway::INVALID_TRANSCRIPTION_MODELS_REQUEST_CODE,
                )
            })?;
        self.require_initialized_and_connected()?;

        provider_list_transcription_models_for_bridge(
            &self.client_runtime.ws_command_sender(),
            params,
        )
        .map_err(|error| ClientFfiError::new(format!("{error:#}"), ClientFfiError::GENERIC_CODE))
    }

    fn voice_input_settings_plan(
        &self,
        input_json: &str,
    ) -> Result<ClientVoiceInputPlanResult, ClientFfiError> {
        let request =
            serde_json::from_str::<ClientVoiceInputPlanRequest>(input_json).map_err(|error| {
                ClientFfiError::new(
                    format!("invalid Voice Input plan request: {error}"),
                    gateway::INVALID_VOICE_INPUT_PLAN_REQUEST_CODE,
                )
            })?;
        self.require_initialized()?;

        Ok(voice_input_plan_for_bridge(request))
    }

    fn provider_model_display(
        &self,
        input_json: &str,
    ) -> Result<ProviderModelDisplayResolution, String> {
        let request = serde_json::from_str::<ProviderModelDisplayKey>(input_json)
            .map_err(|error| format!("invalid provider model display request: {error}"))?;
        let key = provider_model_display_key(
            Some(request.workspace_id.as_str()),
            Some(request.provider.as_str()),
            Some(request.model.as_str()),
        )
        .ok_or_else(|| "invalid provider model display request: empty selection".to_owned())?;
        let response = if let Some(runtime_id) =
            runtime_id_from_cli_runtime_provider_key(key.provider.as_str())
        {
            let cli_response = self
                .client_runtime
                .ws_command_sender()
                .cli_runtime_list_models(cli_runtime_list_models_params(
                    key.workspace_id.clone(),
                    runtime_id.to_owned(),
                ))
                .map_err(|error| format!("{error:#}"))?;
            provider_models_response_from_cli_runtime_models_response(
                key.provider.clone(),
                cli_response,
            )
        } else {
            self.client_runtime
                .ws_command_sender()
                .provider_list_models(provider_model_display_models_params(&key))
                .map_err(|error| format!("{error:#}"))?
        };

        Ok(resolve_provider_model_display_from_response(
            &key, &response,
        ))
    }

    fn reasoning_effort_rows(
        &self,
        input_json: &str,
    ) -> Result<ReasoningEffortRowsResponse, String> {
        let request = serde_json::from_str::<ReasoningEffortRowsRequest>(input_json)
            .map_err(|error| format!("invalid reasoning effort rows request: {error}"))?;
        Ok(reasoning_effort_rows_from_request(request))
    }

    fn composer_permission_mode_options(
        &self,
    ) -> Result<Vec<pioneer_client::composer::permissions::ComposerPermissionModeOption>, String>
    {
        Ok(
            pioneer_client::composer::permissions::composer_permission_mode_options()
                .into_iter()
                .collect(),
        )
    }

    fn composer_attachment_from_path(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::attachments::ComposerAttachment, String> {
        let request = serde_json::from_str::<ClientComposerAttachmentFromPathRequest>(input_json)
            .map_err(|error| format!("invalid composer attachment request: {error}"))?;

        composer_attachment_from_path_request(request).map_err(|error| format!("{error:#}"))
    }

    fn composer_attachments_update(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::attachments::ComposerAttachment>, String> {
        let request = serde_json::from_str::<ClientComposerAttachmentsUpdateRequest>(input_json)
            .map_err(|error| format!("invalid composer attachments update request: {error}"))?;

        Ok(update_composer_attachments(request))
    }

    fn composer_skill_picker_rows(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::capabilities::SelectableSkillCapability>, String>
    {
        let request = serde_json::from_str::<ClientComposerSkillPickerRowsRequest>(input_json)
            .map_err(|error| format!("invalid composer skill picker request: {error}"))?;

        composer_skill_picker_rows(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn composer_mcp_picker_rows(
        &self,
        input_json: &str,
    ) -> Result<ClientComposerMcpPickerRowsResult, String> {
        let request = serde_json::from_str::<ClientComposerMcpPickerRowsRequest>(input_json)
            .map_err(|error| format!("invalid composer mcp picker request: {error}"))?;

        composer_mcp_picker_rows(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn composer_capabilities_update(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::capabilities::ComposerCapability>, String> {
        let parse_error = |error| format!("invalid composer capabilities update request: {error}");
        let request = serde_json::from_str::<ClientComposerCapabilitiesUpdateRequest>(input_json)
            .map_err(parse_error)?;

        Ok(update_composer_capabilities(request))
    }

    fn composer_capability_target(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::capabilities::ComposerCapabilityTarget, String> {
        let request = serde_json::from_str::<ClientComposerCapabilityTargetRequest>(input_json)
            .map_err(|error| format!("invalid composer capability target request: {error}"))?;

        Ok(composer_capability_target(request))
    }

    fn composer_capability_menu_visibility(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::capabilities::ComposerCapabilityMenuVisibility, String>
    {
        let request =
            serde_json::from_str::<ClientComposerCapabilityMenuVisibilityRequest>(input_json)
                .map_err(|error| {
                    format!("invalid composer capability menu visibility request: {error}")
                })?;

        Ok(composer_capability_menu(request))
    }

    fn composer_submission_plan(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::capabilities::ComposerSubmissionPlan, String> {
        let request = serde_json::from_str::<ClientComposerSubmissionPlanRequest>(input_json)
            .map_err(|error| format!("invalid composer submission plan request: {error}"))?;

        Ok(composer_submission_plan(request))
    }

    fn composer_domain_transition(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::state_machine::ComposerDomainTransition, String> {
        let request = serde_json::from_str::<ClientComposerDomainTransitionRequest>(input_json)
            .map_err(|error| format!("invalid composer domain transition request: {error}"))?;

        Ok(composer_domain_transition(request))
    }

    fn composer_draft_lifecycle_transition(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::draft::ComposerDraftLifecycleTransition, String> {
        let request =
            serde_json::from_str::<ClientComposerDraftLifecycleTransitionRequest>(input_json)
                .map_err(|error| {
                    format!("invalid composer draft lifecycle transition request: {error}")
                })?;

        Ok(composer_draft_lifecycle_transition(request))
    }

    fn composer_skill_rows_for_target(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::capabilities::SelectableSkillCapability>, String>
    {
        let request = serde_json::from_str::<ClientComposerSkillRowsForTargetRequest>(input_json)
            .map_err(|error| format!("invalid composer skill target request: {error}"))?;

        Ok(composer_skill_rows_for_target(request))
    }

    fn composer_skill_capability_from_row(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::capabilities::ComposerCapability, String> {
        let request =
            serde_json::from_str::<ClientComposerSkillCapabilityFromRowRequest>(input_json)
                .map_err(|error| format!("invalid composer skill capability request: {error}"))?;

        Ok(skill_capability_from_row(request))
    }

    fn composer_mcp_capability_from_row(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::capabilities::ComposerCapability, String> {
        let request = serde_json::from_str::<ClientComposerMcpCapabilityFromRowRequest>(input_json)
            .map_err(|error| format!("invalid composer mcp capability request: {error}"))?;

        Ok(mcp_capability_from_row(request))
    }

    fn composer_skill_toggle(
        &self,
        input_json: &str,
    ) -> Result<ClientComposerSkillToggleResult, String> {
        let request = serde_json::from_str::<ClientComposerSkillToggleRequest>(input_json)
            .map_err(|error| format!("invalid composer skill toggle request: {error}"))?;

        Ok(toggle_skill_picker_selection(request))
    }

    fn composer_mcp_toggle(
        &self,
        input_json: &str,
    ) -> Result<ClientComposerMcpToggleResult, String> {
        let request = serde_json::from_str::<ClientComposerMcpToggleRequest>(input_json)
            .map_err(|error| format!("invalid composer mcp toggle request: {error}"))?;

        Ok(toggle_mcp_picker_selection(request))
    }

    fn composer_filter_skill_rows(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::capabilities::SelectableSkillCapability>, String>
    {
        let request = serde_json::from_str::<ClientComposerFilterSkillRowsRequest>(input_json)
            .map_err(|error| format!("invalid composer skill row filter request: {error}"))?;

        Ok(filter_skill_picker_rows(request))
    }

    fn composer_filter_mcp_rows(
        &self,
        input_json: &str,
    ) -> Result<ClientComposerFilterMcpRowsResult, String> {
        let request = serde_json::from_str::<ClientComposerFilterMcpRowsRequest>(input_json)
            .map_err(|error| format!("invalid composer mcp row filter request: {error}"))?;

        Ok(filter_mcp_picker_rows(request))
    }

    fn thread_tree_refresh(&self, input_json: &str) -> Result<ClientThreadTreeQueryData, String> {
        let request = serde_json::from_str::<ThreadTreeRefreshRequest>(input_json)
            .map_err(|error| format!("invalid thread tree refresh request: {error}"))?;
        let active_thread_id = request.active_thread_id.clone();

        let mut result = refresh_thread_tree(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| format!("{error:#}"))?;
        self.active_thread
            .apply_thread_tree_snapshot(&result.snapshot)
            .map_err(|error| format!("{error:#}"))?;
        result.composer_model_selection = self
            .active_thread
            .resolve_composer_model_selection(
                active_thread_id.as_deref(),
                Some(result.snapshot.workspace_id.as_str()),
            )
            .map_err(|error| format!("{error:#}"))?;

        Ok(result)
    }

    fn thread_tree_level(&self, input_json: &str) -> Result<ClientThreadTreeLevel, String> {
        let request = serde_json::from_str::<ThreadTreeLevelRequest>(input_json)
            .map_err(|error| format!("invalid thread tree level request: {error}"))?;

        Ok(client_thread_tree_level(request))
    }

    fn thread_timeline_page(
        &self,
        input_json: &str,
    ) -> Result<ThreadTimelinePageResponse, ClientFfiError> {
        let params =
            serde_json::from_str::<ThreadTimelinePageParams>(input_json).map_err(|error| {
                ClientFfiError::new(
                    format!("invalid thread timeline page params: {error}"),
                    timeline::TIMELINE_ERROR_VALIDATION,
                )
            })?;

        let merge_mode = thread_timeline_page_merge_mode(&params.anchor);
        let page = thread_timeline_page(&self.client_runtime.ws_command_sender(), params)?;
        self.active_thread
            .apply_thread_timeline_page(page.clone(), merge_mode)
            .map_err(|error| {
                ClientFfiError::new(format!("{error:#}"), timeline::TIMELINE_ERROR_VALIDATION)
            })?;

        Ok(page)
    }

    fn turn_work_page(&self, input_json: &str) -> Result<TurnWorkPageResponse, ClientFfiError> {
        let params = serde_json::from_str::<TurnWorkPageParams>(input_json).map_err(|error| {
            ClientFfiError::new(
                format!("invalid turn work page params: {error}"),
                timeline::TIMELINE_ERROR_VALIDATION,
            )
        })?;

        let merge_mode = turn_work_page_merge_mode(&params.anchor);
        let page = turn_work_page(&self.client_runtime.ws_command_sender(), params)?;
        self.active_thread
            .apply_turn_work_page(page.clone(), merge_mode)
            .map_err(|error| {
                ClientFfiError::new(format!("{error:#}"), timeline::TIMELINE_ERROR_VALIDATION)
            })?;

        Ok(page)
    }

    fn agents_doc_get(&self, input_json: &str) -> Result<ThreadAgentsDocGetResponse, String> {
        let request = serde_json::from_str::<ThreadAgentsDocGetParams>(input_json)
            .map_err(|error| format!("invalid agents doc get request: {error}"))?;
        let params =
            agents_doc_get_params(request.workspace_id.as_str(), request.folder_id.as_deref());

        self.client_runtime
            .ws_command_sender()
            .thread_agents_doc_get(params)
            .map_err(|error| format!("{error:#}"))
    }

    fn agents_doc_save(&self, input_json: &str) -> Result<ThreadAgentsDocSaveResponse, String> {
        let request = serde_json::from_str::<ThreadAgentsDocSaveParams>(input_json)
            .map_err(|error| format!("invalid agents doc save request: {error}"))?;
        let params = agents_doc_save_params(
            request.workspace_id.as_str(),
            request.folder_id.as_deref(),
            request.content.as_str(),
            request.expected_version,
            request.save_reason,
        );

        self.client_runtime
            .ws_command_sender()
            .thread_agents_doc_save(params)
            .map_err(|error| {
                let message = format!("{error:#}");
                match agents_doc_save_error_kind(message.as_str()) {
                    AgentsDocSaveErrorKind::VersionConflict => "version conflict".to_owned(),
                    AgentsDocSaveErrorKind::Other => message,
                }
            })
    }

    fn agents_doc_archive(
        &self,
        input_json: &str,
    ) -> Result<ThreadAgentsDocArchiveResponse, String> {
        let request = serde_json::from_str::<ThreadAgentsDocArchiveParams>(input_json)
            .map_err(|error| format!("invalid agents doc archive request: {error}"))?;

        self.client_runtime
            .ws_command_sender()
            .thread_agents_doc_archive(request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_open(&self, input_json: &str) -> Result<ClientActiveThreadSnapshot, String> {
        let request = serde_json::from_str::<ClientActiveThreadOpenRequest>(input_json)
            .map_err(|error| format!("invalid active thread open request: {error}"))?;

        self.active_thread
            .open_thread(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_open_by_id(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadSnapshot, String> {
        let request = serde_json::from_str::<ClientActiveThreadOpenByIdRequest>(input_json)
            .map_err(|error| format!("invalid active thread open by id request: {error}"))?;

        self.active_thread
            .open_thread_by_id(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_ensure_workspace_draft(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadSnapshot, String> {
        let request = serde_json::from_str::<ClientEnsureWorkspaceDraftRequest>(input_json)
            .map_err(|error| format!("invalid active thread draft request: {error}"))?;

        self.active_thread
            .ensure_workspace_draft(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_open_or_create_new(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadSnapshot, String> {
        let request = serde_json::from_str::<ClientEnsureWorkspaceDraftRequest>(input_json)
            .map_err(|error| format!("invalid active thread new request: {error}"))?;

        self.active_thread
            .open_or_create_new_thread(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_snapshot(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadSnapshot, String> {
        let request = serde_json::from_str::<ClientActiveThreadSnapshotRequest>(input_json)
            .map_err(|error| format!("invalid active thread snapshot request: {error}"))?;

        self.active_thread
            .snapshot(request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_apply_event(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadEventResult, String> {
        let request = serde_json::from_str::<ClientActiveThreadEventRequest>(input_json)
            .map_err(|error| format!("invalid active thread event request: {error}"))?;

        self.active_thread
            .apply_event(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_send_text(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadSendTextResult, String> {
        let request = serde_json::from_str::<ClientActiveThreadSendTextRequest>(input_json)
            .map_err(|error| format!("invalid active thread send text request: {error}"))?;

        self.active_thread
            .send_text_turn(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn prepare_voice_composer_snapshot(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::turn_prepare::PreparedVoiceComposerSnapshot, String> {
        let request = serde_json::from_str::<ClientPrepareVoiceComposerSnapshotRequest>(input_json)
            .map_err(|error| format!("invalid prepare voice composer snapshot request: {error}"))?;

        self.active_thread
            .prepare_voice_composer_snapshot(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_cancel_turn(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadCancelTurnResult, String> {
        let request = serde_json::from_str::<ClientActiveThreadCancelTurnRequest>(input_json)
            .map_err(|error| format!("invalid active thread cancel turn request: {error}"))?;

        self.active_thread
            .cancel_turn(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_clear(&self) -> Result<ClientActiveThreadClearResult, String> {
        self.active_thread
            .clear(&self.client_runtime)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_unsubscribe_or_close(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadUnsubscribeResult, String> {
        let request = serde_json::from_str::<ClientActiveThreadUnsubscribeRequest>(input_json)
            .map_err(|error| format!("invalid active thread unsubscribe request: {error}"))?;

        self.active_thread
            .unsubscribe_or_close_thread(&self.client_runtime, request)
            .map_err(|error| format!("{error:#}"))
    }

    fn diagnostics_drain(&self) -> Result<Vec<ClientDiagnosticEvent>, String> {
        self.diagnostics.drain()
    }
}

fn thread_timeline_page_merge_mode(anchor: &TimelinePageAnchor) -> TopLevelPageMergeMode {
    match anchor {
        TimelinePageAnchor::Before { .. } => TopLevelPageMergeMode::MergeBefore,
        TimelinePageAnchor::After { .. } => TopLevelPageMergeMode::MergeAfter,
        TimelinePageAnchor::Newest
        | TimelinePageAnchor::Oldest
        | TimelinePageAnchor::Around { .. } => TopLevelPageMergeMode::Merge,
    }
}

fn turn_work_page_merge_mode(anchor: &TimelinePageAnchor) -> WorkPageMergeMode {
    match anchor {
        TimelinePageAnchor::Before { .. } => WorkPageMergeMode::MergeBefore,
        TimelinePageAnchor::After { .. } => WorkPageMergeMode::MergeAfter,
        TimelinePageAnchor::Newest
        | TimelinePageAnchor::Oldest
        | TimelinePageAnchor::Around { .. } => WorkPageMergeMode::Reset,
    }
}

fn ffi_client_json_typed_response<T, F>(
    ptr: *mut PioneerClientFfi,
    input_json: *const c_char,
    operation_name: &'static str,
    operation: F,
) -> *mut c_char
where
    T: Serialize,
    F: FnOnce(&ClientFfiRuntime, &str) -> Result<T, ClientFfiError>,
{
    let client = match unsafe { ffi_ref(ptr) } {
        Ok(client) => client,
        Err(error) => return into_c_string(to_json_response::<()>(Err(error))),
    };
    let input_json = match unsafe { read_c_string(input_json) } {
        Ok(input_json) => input_json,
        Err(error) => return into_c_string(to_json_response::<()>(Err(error))),
    };

    into_ffi_typed_response_with_diagnostics(&client.runtime.diagnostics, operation_name, || {
        operation(&client.runtime, input_json.as_str())
    })
}

fn ffi_client_json_response<T, F>(
    ptr: *mut PioneerClientFfi,
    input_json: *const c_char,
    operation_name: &'static str,
    operation: F,
) -> *mut c_char
where
    T: Serialize,
    F: FnOnce(&ClientFfiRuntime, &str) -> Result<T, String>,
{
    let client = match unsafe { ffi_ref(ptr) } {
        Ok(client) => client,
        Err(error) => return into_c_string(to_json_response::<()>(Err(error))),
    };
    let input_json = match unsafe { read_c_string(input_json) } {
        Ok(input_json) => input_json,
        Err(error) => return into_c_string(to_json_response::<()>(Err(error))),
    };

    into_ffi_response_with_diagnostics(&client.runtime.diagnostics, operation_name, || {
        operation(&client.runtime, input_json.as_str())
    })
}

fn ffi_client_response<T, F>(
    ptr: *mut PioneerClientFfi,
    operation_name: &'static str,
    operation: F,
) -> *mut c_char
where
    T: Serialize,
    F: FnOnce(&ClientFfiRuntime) -> Result<T, String>,
{
    let client = match unsafe { ffi_ref(ptr) } {
        Ok(client) => client,
        Err(error) => return into_c_string(to_json_response::<()>(Err(error))),
    };

    into_ffi_response_with_diagnostics(&client.runtime.diagnostics, operation_name, || {
        operation(&client.runtime)
    })
}

macro_rules! ffi_client_json_method {
    ($export_name:ident, $runtime_method:ident) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $export_name(
            ptr: *mut PioneerClientFfi,
            input_json: *const c_char,
        ) -> *mut c_char {
            ffi_client_json_response(
                ptr,
                input_json,
                stringify!($runtime_method),
                |runtime, input_json| runtime.$runtime_method(input_json),
            )
        }
    };
}

macro_rules! ffi_client_json_typed_method {
    ($export_name:ident, $runtime_method:ident) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $export_name(
            ptr: *mut PioneerClientFfi,
            input_json: *const c_char,
        ) -> *mut c_char {
            ffi_client_json_typed_response(
                ptr,
                input_json,
                stringify!($runtime_method),
                |runtime, input_json| runtime.$runtime_method(input_json),
            )
        }
    };
}

#[unsafe(no_mangle)]
pub extern "C" fn pioneer_client_ffi_version() -> *mut c_char {
    into_ffi_response(|| Ok(FFI_VERSION))
}

#[unsafe(no_mangle)]
pub extern "C" fn pioneer_client_ffi_client_create() -> *mut PioneerClientFfi {
    catch_unwind(AssertUnwindSafe(|| {
        Box::into_raw(Box::new(PioneerClientFfi::new()))
    }))
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_client_destroy(ptr: *mut PioneerClientFfi) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if ptr.is_null() {
            return;
        }

        // SAFETY: ownership is transferred back from the raw pointer exactly once
        // by the native wrapper when its Nitro object is deallocated.
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }));
}

ffi_client_json_method!(pioneer_client_ffi_client_initialize, initialize);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_validate_remote,
    gateway_validate_remote
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_add_remote,
    gateway_plan_add_remote
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_add_and_activate_remote_registry,
    gateway_plan_add_and_activate_remote_registry
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_activate_registry,
    gateway_plan_activate_registry
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_update_remote_registry,
    gateway_plan_update_remote_registry
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_delete_remote_registry,
    gateway_plan_delete_remote_registry
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_set_workspace_registry,
    gateway_plan_set_workspace_registry
);
ffi_client_json_method!(pioneer_client_ffi_gateway_connect, gateway_connect);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_settings_get,
    gateway_settings_get
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_settings_update,
    gateway_settings_update
);
ffi_client_json_method!(pioneer_client_ffi_workspace_bootstrap, workspace_bootstrap);
ffi_client_json_method!(pioneer_client_ffi_workspace_switch, workspace_switch);
ffi_client_json_method!(pioneer_client_ffi_workspace_create, workspace_create);
ffi_client_json_method!(pioneer_client_ffi_workspace_rename, workspace_rename);
ffi_client_json_method!(pioneer_client_ffi_provider_list, provider_list);
ffi_client_json_method!(pioneer_client_ffi_cli_runtime_list, cli_runtime_list);
ffi_client_json_method!(pioneer_client_ffi_cli_runtime_refresh, cli_runtime_refresh);
ffi_client_json_method!(
    pioneer_client_ffi_cli_runtime_list_models,
    cli_runtime_list_models
);
ffi_client_json_method!(
    pioneer_client_ffi_cli_runtime_thread_binding_get,
    cli_runtime_thread_binding_get
);
ffi_client_json_method!(
    pioneer_client_ffi_cli_runtime_thread_compact,
    cli_runtime_thread_compact
);
ffi_client_json_method!(
    pioneer_client_ffi_cli_runtime_turn_steer,
    cli_runtime_turn_steer
);
ffi_client_json_method!(
    pioneer_client_ffi_cli_runtime_review_start,
    cli_runtime_review_start
);
ffi_client_json_method!(
    pioneer_client_ffi_cli_runtime_request_respond,
    cli_runtime_request_respond
);
ffi_client_json_method!(
    pioneer_client_ffi_turn_permission_request_respond,
    turn_permission_request_respond
);
ffi_client_json_method!(pioneer_client_ffi_voice_status, voice_status);
ffi_client_json_method!(pioneer_client_ffi_voice_session_start, voice_session_start);
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_voice_audio_chunk(
    ptr: *mut PioneerClientFfi,
    input_json: *const c_char,
    pcm_ptr: *const u8,
    pcm_len: usize,
) -> *mut c_char {
    let client = match unsafe { ffi_ref(ptr) } {
        Ok(client) => client,
        Err(error) => return into_c_string(to_json_response::<()>(Err(error))),
    };
    let input_json = match unsafe { read_c_string(input_json) } {
        Ok(input_json) => input_json,
        Err(error) => return into_c_string(to_json_response::<()>(Err(error))),
    };
    let pcm_chunk: &[u8] = if pcm_len == 0 {
        &[]
    } else if pcm_ptr.is_null() {
        return into_c_string(to_json_response::<()>(Err(
            "received null pcm chunk pointer".to_owned(),
        )));
    } else {
        // SAFETY: the native bridge passes an ArrayBuffer pointer that stays
        // alive for the duration of this synchronous FFI call.
        unsafe { std::slice::from_raw_parts(pcm_ptr, pcm_len) }
    };

    into_ffi_response_with_diagnostics(&client.runtime.diagnostics, "voice_audio_chunk", || {
        client
            .runtime
            .voice_audio_chunk(input_json.as_str(), pcm_chunk)
    })
}
ffi_client_json_method!(
    pioneer_client_ffi_voice_session_finalize,
    voice_session_finalize
);
ffi_client_json_method!(
    pioneer_client_ffi_voice_session_cancel,
    voice_session_cancel
);
ffi_client_json_method!(
    pioneer_client_ffi_pending_request_response_plan,
    pending_request_response_plan
);
ffi_client_json_method!(
    pioneer_client_ffi_pending_request_presentation,
    pending_request_presentation
);
ffi_client_json_method!(
    pioneer_client_ffi_provider_list_models,
    provider_list_models
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_provider_list_transcription_models,
    provider_list_transcription_models
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_voice_input_settings_plan,
    voice_input_settings_plan
);
ffi_client_json_method!(
    pioneer_client_ffi_provider_model_display,
    provider_model_display
);
ffi_client_json_method!(
    pioneer_client_ffi_reasoning_effort_rows,
    reasoning_effort_rows
);
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_composer_permission_mode_options(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "composer_permission_mode_options", |runtime| {
        runtime.composer_permission_mode_options()
    })
}
ffi_client_json_method!(
    pioneer_client_ffi_composer_attachment_from_path,
    composer_attachment_from_path
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_attachments_update,
    composer_attachments_update
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_picker_rows,
    composer_skill_picker_rows
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_mcp_picker_rows,
    composer_mcp_picker_rows
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_capabilities_update,
    composer_capabilities_update
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_capability_target,
    composer_capability_target
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_capability_menu_visibility,
    composer_capability_menu_visibility
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_submission_plan,
    composer_submission_plan
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_domain_transition,
    composer_domain_transition
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_draft_lifecycle_transition,
    composer_draft_lifecycle_transition
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_rows_for_target,
    composer_skill_rows_for_target
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_capability_from_row,
    composer_skill_capability_from_row
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_mcp_capability_from_row,
    composer_mcp_capability_from_row
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_toggle,
    composer_skill_toggle
);
ffi_client_json_method!(pioneer_client_ffi_composer_mcp_toggle, composer_mcp_toggle);
ffi_client_json_method!(
    pioneer_client_ffi_composer_filter_skill_rows,
    composer_filter_skill_rows
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_filter_mcp_rows,
    composer_filter_mcp_rows
);
ffi_client_json_method!(pioneer_client_ffi_thread_tree_refresh, thread_tree_refresh);
ffi_client_json_method!(pioneer_client_ffi_thread_tree_level, thread_tree_level);
ffi_client_json_typed_method!(
    pioneer_client_ffi_thread_timeline_page,
    thread_timeline_page
);
ffi_client_json_typed_method!(pioneer_client_ffi_turn_work_page, turn_work_page);
ffi_client_json_method!(pioneer_client_ffi_agents_doc_get, agents_doc_get);
ffi_client_json_method!(pioneer_client_ffi_agents_doc_save, agents_doc_save);
ffi_client_json_method!(pioneer_client_ffi_agents_doc_archive, agents_doc_archive);
ffi_client_json_method!(pioneer_client_ffi_active_thread_open, active_thread_open);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_open_by_id,
    active_thread_open_by_id
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_ensure_workspace_draft,
    active_thread_ensure_workspace_draft
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_open_or_create_new,
    active_thread_open_or_create_new
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_snapshot,
    active_thread_snapshot
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_apply_event,
    active_thread_apply_event
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_send_text,
    active_thread_send_text
);
ffi_client_json_method!(
    pioneer_client_ffi_prepare_voice_composer_snapshot,
    prepare_voice_composer_snapshot
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_cancel_turn,
    active_thread_cancel_turn
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_unsubscribe_or_close,
    active_thread_unsubscribe_or_close
);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_active_thread_clear(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "active_thread_clear", |runtime| {
        runtime.active_thread_clear()
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_gateway_next_events(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "gateway_next_events", |runtime| {
        runtime.gateway_next_events()
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_gateway_disconnect(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "gateway_disconnect", |runtime| {
        runtime.gateway_disconnect()
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_diagnostics_drain(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "diagnostics_drain", |runtime| {
        runtime.diagnostics_drain()
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_string_destroy(value: *mut c_char) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if value.is_null() {
            return;
        }

        // SAFETY: strings returned by this crate are allocated with
        // `CString::into_raw`; this reclaims and drops that allocation.
        unsafe {
            drop(CString::from_raw(value));
        }
    }));
}

unsafe fn ffi_ref<'a>(ptr: *mut PioneerClientFfi) -> Result<&'a PioneerClientFfi, String> {
    if ptr.is_null() {
        return Err("received null client pointer".to_owned());
    }

    // SAFETY: the pointer is created by `pioneer_client_ffi_client_create` and
    // remains valid until `pioneer_client_ffi_client_destroy`.
    unsafe { ptr.as_ref() }.ok_or_else(|| "received invalid client pointer".to_owned())
}

unsafe fn read_c_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("received null string pointer".to_owned());
    }

    // SAFETY: callers pass a valid, NUL-terminated string pointer owned by the
    // native bridge for the duration of this call.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(str::to_owned)
        .map_err(|error| format!("received non-utf8 string: {error}"))
}

fn gateway_auth_token_ref_for_endpoint(endpoint_id: &str) -> Result<String, GatewayProfileError> {
    GatewayAuthTokenRef::for_endpoint_id(endpoint_id)
        .map(GatewayAuthTokenRef::into_string)
        .map_err(|error| GatewayProfileError::InvalidAuthTokenRef {
            endpoint_id: endpoint_id.to_owned(),
            reason: error.to_string(),
        })
}

fn to_json_response<T: Serialize>(result: Result<T, String>) -> String {
    match result {
        Ok(value) => serialize_json_response(serde_json::to_string(&FfiResponse::Ok { value })),
        Err(message) => to_json_error_response(message, "pioneer_client_ffi_error"),
    }
}

fn to_json_typed_response<T: Serialize>(result: Result<T, ClientFfiError>) -> String {
    match result {
        Ok(value) => serialize_json_response(serde_json::to_string(&FfiResponse::Ok { value })),
        Err(error) => serialize_json_response(serde_json::to_string(&FfiResponse::<()>::Error {
            message: error.message,
            code: Some(error.code.to_owned()),
        })),
    }
}

fn to_json_error_response(message: String, code: &'static str) -> String {
    serialize_json_response(serde_json::to_string(&FfiResponse::<()>::Error {
        message,
        code: Some(code.to_owned()),
    }))
}

fn serialize_json_response(response: Result<String, serde_json::Error>) -> String {
    response.unwrap_or_else(|error| {
        format!(
            r#"{{"status":"error","message":"failed to serialize ffi response: {}","code":"pioneer_client_ffi_serialize_error"}}"#,
            sanitize_c_string(error.to_string())
        )
    })
}

fn into_ffi_response<T, F>(operation: F) -> *mut c_char
where
    T: Serialize,
    F: FnOnce() -> Result<T, String>,
{
    into_c_string(ffi_response_json(operation))
}

fn into_ffi_response_with_diagnostics<T, F>(
    diagnostics: &ClientFfiDiagnostics,
    operation_name: &'static str,
    operation: F,
) -> *mut c_char
where
    T: Serialize,
    F: FnOnce() -> Result<T, String>,
{
    into_c_string(ffi_response_json_with_diagnostics(
        diagnostics,
        operation_name,
        operation,
    ))
}

fn into_ffi_typed_response_with_diagnostics<T, F>(
    diagnostics: &ClientFfiDiagnostics,
    operation_name: &'static str,
    operation: F,
) -> *mut c_char
where
    T: Serialize,
    F: FnOnce() -> Result<T, ClientFfiError>,
{
    into_c_string(ffi_typed_response_json_with_diagnostics(
        diagnostics,
        operation_name,
        operation,
    ))
}

fn ffi_response_json<T, F>(operation: F) -> String
where
    T: Serialize,
    F: FnOnce() -> Result<T, String>,
{
    catch_unwind(AssertUnwindSafe(|| to_json_response(operation()))).unwrap_or_else(|payload| {
        to_json_error_response(
            format!("panic in pioneer client ffi: {}", panic_message(payload)),
            "pioneer_client_ffi_panic",
        )
    })
}

fn ffi_typed_response_json_with_diagnostics<T, F>(
    diagnostics: &ClientFfiDiagnostics,
    operation_name: &'static str,
    operation: F,
) -> String
where
    T: Serialize,
    F: FnOnce() -> Result<T, ClientFfiError>,
{
    catch_unwind(AssertUnwindSafe(|| {
        let response = operation();
        if let Err(error) = &response {
            diagnostics.record_error(operation_name, error.message.clone(), error.code);
        }
        to_json_typed_response(response)
    }))
    .unwrap_or_else(|payload| {
        let message = format!("panic in pioneer client ffi: {}", panic_message(payload));
        diagnostics.record_error(operation_name, message.clone(), "pioneer_client_ffi_panic");
        to_json_error_response(message, "pioneer_client_ffi_panic")
    })
}

fn ffi_response_json_with_diagnostics<T, F>(
    diagnostics: &ClientFfiDiagnostics,
    operation_name: &'static str,
    operation: F,
) -> String
where
    T: Serialize,
    F: FnOnce() -> Result<T, String>,
{
    catch_unwind(AssertUnwindSafe(|| to_json_response(operation()))).unwrap_or_else(|payload| {
        let message = format!("panic in pioneer client ffi: {}", panic_message(payload));
        diagnostics.record_error(operation_name, message.clone(), "pioneer_client_ffi_panic");
        to_json_error_response(message, "pioneer_client_ffi_panic")
    })
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        sanitize_c_string((*message).to_owned())
    } else if let Some(message) = payload.downcast_ref::<String>() {
        sanitize_c_string(message.clone())
    } else {
        "unknown panic payload".to_owned()
    }
}

fn sanitize_c_string(value: String) -> String {
    value.replace('\0', "\\u0000")
}

fn into_c_string(value: String) -> *mut c_char {
    match CString::new(value) {
        Ok(value) => value.into_raw(),
        Err(error) => {
            let sanitized =
                sanitize_c_string(String::from_utf8_lossy(&error.into_vec()).into_owned());
            CString::new(sanitized)
                .map(CString::into_raw)
                .unwrap_or(ptr::null_mut())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_response<T: for<'de> Deserialize<'de>>(json: &str) -> T {
        #[derive(Deserialize)]
        #[serde(tag = "status", rename_all = "lowercase")]
        enum TestResponse<T> {
            Ok {
                value: T,
            },
            Error {
                message: String,
                code: Option<String>,
            },
        }

        match serde_json::from_str::<TestResponse<T>>(json).expect("response json") {
            TestResponse::Ok { value } => value,
            TestResponse::Error { message, code } => {
                panic!("unexpected ffi error: {message} {code:?}")
            }
        }
    }

    #[test]
    fn initialize_accepts_shell_config() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .initialize(r#"{"platform":"ios","locale":"en","app_data_dir":"/tmp/pioneer"}"#)
            .expect("initialize");

        assert!(result.initialized);
    }

    #[test]
    fn ffi_response_is_tagged_json() {
        let response = to_json_response::<serde_json::Value>(Ok(serde_json::json!({"value": 1})));
        let value: serde_json::Value = decode_response(response.as_str());

        assert_eq!(value["value"], 1);
    }

    #[test]
    fn composer_permission_mode_options_use_shared_client_contract() {
        let runtime = ClientFfiRuntime::default();
        let options = runtime
            .composer_permission_mode_options()
            .expect("composer permission options");

        assert_eq!(options.len(), 3);
        assert_eq!(
            options[0].mode,
            pioneer_protocol::TurnPermissionMode::FullAccess
        );
        assert_eq!(
            options[1].mode,
            pioneer_protocol::TurnPermissionMode::AutoAcceptEdits
        );
        assert_eq!(
            options[2].mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
    }

    #[test]
    fn composer_presentation_and_submission_contracts_cross_the_json_boundary() {
        let runtime = ClientFfiRuntime::default();
        let presentation_target = runtime
            .composer_capability_target(r#"{"provider":"cli_runtime:codex","runtimes":[]}"#)
            .expect("presentation target");
        assert!(presentation_target.is_cli());
        assert!(!presentation_target.policy().supports_mcp_tools);
        let visibility = runtime
            .composer_capability_menu_visibility(
                serde_json::json!({ "target": presentation_target })
                    .to_string()
                    .as_str(),
            )
            .expect("menu visibility");
        assert!(visibility.skills);
        assert!(visibility.mcp);
        assert!(visibility.any);

        let skill_rows = runtime
            .composer_skill_rows_for_target(
                serde_json::json!({
                    "target": {
                        "kind": "native",
                        "supports_skills": true,
                        "supports_mcp_tools": true
                    },
                    "rows": [
                        {
                            "key": "skill:BBBBBBBBBBBBBBBBBBBBB",
                            "skill_id": "BBBBBBBBBBBBBBBBBBBBB",
                            "label": "pioneer/browser",
                            "display_name": "Browser",
                            "description": "User-controlled bundled browser",
                            "owner": "pioneer",
                            "slug": "browser",
                            "source_kind": "system",
                            "selectable": true,
                            "unavailable_reason": null
                        },
                        {
                            "key": "skill:WWWWWWWWWWWWWWWWWWWWW",
                            "skill_id": "WWWWWWWWWWWWWWWWWWWWW",
                            "label": "writer",
                            "display_name": "Writer",
                            "description": "User-installed writer",
                            "owner": null,
                            "slug": "writer",
                            "source_kind": "user",
                            "selectable": true,
                            "unavailable_reason": null
                        }
                    ]
                })
                .to_string()
                .as_str(),
            )
            .expect("skill picker rows");
        assert_eq!(skill_rows.len(), 2);
        assert_eq!(skill_rows[0].key, "skill:BBBBBBBBBBBBBBBBBBBBB");
        assert_eq!(skill_rows[1].key, "skill:WWWWWWWWWWWWWWWWWWWWW");
        let selected_skill = runtime
            .composer_skill_capability_from_row(
                serde_json::json!({ "row": skill_rows[0] })
                    .to_string()
                    .as_str(),
            )
            .expect("skill row conversion");
        assert_eq!(selected_skill.id, "skill:BBBBBBBBBBBBBBBBBBBBB");
        assert!(matches!(
            selected_skill.kind,
            pioneer_client::composer::capabilities::ComposerCapabilityKind::Skill {
                ref skill_id,
                ref owner,
                ref slug,
                ..
            } if skill_id.as_str() == "BBBBBBBBBBBBBBBBBBBBB"
                && owner.as_deref() == Some("pioneer")
                && slug == "browser"
        ));

        let capabilities = serde_json::json!([
            {
                "id": "mcp-server:workspace:appstoreconnect",
                "label": "appstoreconnect",
                "kind": {
                    "McpServer": {
                        "name": "appstoreconnect",
                        "scope_kind": "workspace"
                    }
                }
            },
            {
                "id": "skill:BBBBBBBBBBBBBBBBBBBBB",
                "label": "pioneer/browser",
                "kind": {
                    "Skill": {
                        "skill_id": "BBBBBBBBBBBBBBBBBBBBB",
                        "owner": "pioneer",
                        "slug": "browser",
                        "source_kind": "system"
                    }
                }
            }
        ]);
        let text = runtime
            .composer_submission_plan(
                serde_json::json!({
                    "provider": "cli_runtime:codex",
                    "text": "inspect releases",
                    "has_attachments": false,
                    "capabilities": capabilities.clone(),
                })
                .to_string()
                .as_str(),
            )
            .expect("text submission plan");
        let voice = runtime
            .composer_submission_plan(
                serde_json::json!({
                    "provider": "cli_runtime:codex",
                    "text": "",
                    "has_attachments": true,
                    "capabilities": capabilities,
                })
                .to_string()
                .as_str(),
            )
            .expect("voice submission plan");

        assert_eq!(text.capabilities, voice.capabilities);
        assert_eq!(text.removed, voice.removed);
        assert_eq!(text.capabilities.len(), 2);
        assert_eq!(
            text.capabilities[0].id,
            "mcp-server:workspace:appstoreconnect"
        );
        assert_eq!(text.capabilities[1].id, "skill:BBBBBBBBBBBBBBBBBBBBB");
        assert!(text.removed.is_empty());
        assert!(text.has_composer_payload);
        assert!(voice.has_composer_payload);

        let missing_skill_id = runtime.composer_submission_plan(
            serde_json::json!({
                "provider": "openai",
                "text": "missing id",
                "capabilities": [{
                    "id": "skill:BBBBBBBBBBBBBBBBBBBBB",
                    "label": "browser",
                    "kind": {
                        "Skill": {
                            "slug": "browser",
                            "source_kind": "system"
                        }
                    }
                }]
            })
            .to_string()
            .as_str(),
        );
        assert!(
            missing_skill_id.is_err(),
            "skill capability without exact id must be rejected"
        );
    }

    #[test]
    fn composer_domain_transition_crosses_the_mobile_json_boundary() {
        let runtime = ClientFfiRuntime::default();
        let transition = runtime
            .composer_domain_transition(
                serde_json::json!({
                    "state": {
                        "attachments": [],
                        "capabilities": [{
                            "id": "mcp-server:workspace:appstoreconnect",
                            "label": "appstoreconnect",
                            "kind": {
                                "McpServer": {
                                    "name": "appstoreconnect",
                                    "scope_kind": "workspace"
                                }
                            }
                        }],
                        "selected_mode": "Agent",
                        "mode_manually_selected": false,
                        "selected_provider": "cli_runtime:codex",
                        "capability_target": {
                            "kind": "cli",
                            "supports_skills": true,
                            "supports_mcp_tools": true
                        },
                        "selected_model": "gpt-5.6-sol",
                        "selected_reasoning_effort": null,
                        "selected_permission_mode": "full_access",
                        "model_manually_selected": false
                    },
                    "action": {
                        "SetReasoningEffortFromUser": {
                            "effort": " max "
                        }
                    }
                })
                .to_string()
                .as_str(),
            )
            .expect("composer domain transition");

        assert!(transition.changed);
        assert!(transition.model_selection_changed);
        assert_eq!(
            transition.state.selected_reasoning_effort.as_deref(),
            Some("max")
        );
        assert!(transition.state.model_manually_selected);
        assert_eq!(transition.state.capabilities.len(), 1);
        assert_eq!(
            transition.state.capabilities[0].id,
            "mcp-server:workspace:appstoreconnect"
        );
    }

    #[test]
    fn composer_draft_lifecycle_crosses_the_mobile_json_boundary() {
        let runtime = ClientFfiRuntime::default();
        let draft = serde_json::json!({
            "text": "inspect releases",
            "domain": {
                "attachments": [],
                "capabilities": [],
                "selected_mode": "Agent",
                "mode_manually_selected": false,
                "selected_provider": "cli_runtime:codex",
                "capability_target": {
                    "kind": "cli",
                    "supports_skills": true,
                    "supports_mcp_tools": true
                },
                "selected_model": "gpt-5.6-sol",
                "selected_reasoning_effort": "max",
                "selected_permission_mode": "supervised",
                "model_manually_selected": true
            }
        });
        let transition = runtime
            .composer_draft_lifecycle_transition(
                serde_json::json!({
                    "state": { "drafts": {} },
                    "action": {
                        "SwitchThread": {
                            "current_thread_id": null,
                            "current_draft": null,
                            "target_thread_id": "thread-a",
                            "fallback": draft
                        }
                    }
                })
                .to_string()
                .as_str(),
            )
            .expect("composer draft lifecycle transition");

        assert!(transition.changed);
        assert_eq!(
            transition
                .restored_draft
                .as_ref()
                .map(|draft| draft.text.as_str()),
            Some("inspect releases")
        );
        assert!(transition.state.drafts.contains_key("thread-a"));
    }

    #[test]
    fn gateway_validation_uses_shared_request_contract() {
        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .gateway_validate_remote(r#"{"address":"127.0.0.1:23000","timeout_ms":0}"#)
            .expect_err("zero timeout should fail");

        assert!(error.contains("timeout must be positive"));
    }

    #[test]
    fn gateway_settings_ffi_rejects_null_and_invalid_json_safely() {
        let client = pioneer_client_ffi_client_create();
        assert!(!client.is_null());

        let response_ptr =
            unsafe { pioneer_client_ffi_gateway_settings_get(client, std::ptr::null()) };
        assert!(!response_ptr.is_null());
        let response = unsafe { CString::from_raw(response_ptr) }
            .into_string()
            .expect("UTF-8 FFI response");
        let response: serde_json::Value =
            serde_json::from_str(response.as_str()).expect("JSON FFI response");
        assert_eq!(response["status"], "error");
        assert_eq!(response["code"], ClientFfiError::GENERIC_CODE);

        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .gateway_settings_update("{")
            .expect_err("invalid settings JSON");
        assert_eq!(error.code, gateway::INVALID_GATEWAY_SETTINGS_REQUEST_CODE);

        unsafe { pioneer_client_ffi_client_destroy(client) };
    }

    #[test]
    fn gateway_settings_ffi_requires_initialized_active_gateway() {
        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .gateway_settings_get("{}")
            .expect_err("uninitialized client");
        assert_eq!(error.code, gateway::CLIENT_NOT_INITIALIZED_CODE);

        runtime.initialize("{}").expect("initialize client");
        let error = runtime
            .gateway_settings_get("{}")
            .expect_err("disconnected client");
        assert_eq!(error.code, gateway::GATEWAY_DISCONNECTED_CODE);
    }

    #[test]
    fn gateway_settings_ffi_preserves_voice_conflict_error_code() {
        let response = to_json_typed_response::<()>(Err(ClientFfiError::new(
            "voice input cannot be reconfigured while a voice session is active",
            gateway::VOICE_RECONFIGURATION_BUSY_CODE,
        )));
        let response: serde_json::Value =
            serde_json::from_str(response.as_str()).expect("typed FFI response");

        assert_eq!(response["status"], "error");
        assert_eq!(response["code"], gateway::VOICE_RECONFIGURATION_BUSY_CODE);
    }

    #[test]
    fn transcription_models_ffi_validates_input_and_active_gateway() {
        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .provider_list_transcription_models("{")
            .expect_err("invalid catalog JSON");
        assert_eq!(
            error.code,
            gateway::INVALID_TRANSCRIPTION_MODELS_REQUEST_CODE
        );

        runtime.initialize("{}").expect("initialize client");
        let error = runtime
            .provider_list_transcription_models(
                r#"{"workspace_id":"workspace-1","provider":"local"}"#,
            )
            .expect_err("disconnected catalog request");
        assert_eq!(error.code, gateway::GATEWAY_DISCONNECTED_CODE);
    }

    #[test]
    fn voice_input_plan_ffi_is_pure_after_initialization() {
        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .voice_input_settings_plan(r#"{"operation":"unknown"}"#)
            .expect_err("unknown planner operation");
        assert_eq!(error.code, gateway::INVALID_VOICE_INPUT_PLAN_REQUEST_CODE);

        runtime.initialize("{}").expect("initialize client");
        let result = runtime
            .voice_input_settings_plan(
                r#"{
                    "operation":"status_reduction",
                    "current":{
                        "enabled":false,
                        "runtime":{"phase":"disabled","effective_enabled":false}
                    }
                }"#,
            )
            .expect("pure status reduction does not require a connection");
        let ClientVoiceInputPlanResult::StatusReduction { reduction } = result else {
            panic!("status reduction result")
        };
        assert_eq!(
            reduction.presentation,
            pioneer_client::settings::voice::VoiceInputRuntimePresentation::Disabled
        );
    }

    #[test]
    fn cli_runtime_refresh_validates_bridge_input() {
        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .cli_runtime_refresh(r#"{"workspace_id":42}"#)
            .expect_err("non-string workspace id should fail");

        assert!(error.contains("invalid CLI runtime refresh params"));
    }

    #[test]
    fn gateway_add_remote_planning_returns_shared_plan_without_persistence() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_add_remote(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": null,
                        "local": {
                            "id": "local",
                            "name": "Local",
                            "address": "127.0.0.1:17878",
                            "kind": "local",
                            "auth_token_ref": null,
                            "workspace_id": null,
                            "service_name": null
                        },
                        "remotes": []
                    },
                    "name": " Remote ",
                    "address": "127.0.0.1:23000",
                    "auth_token": " token ",
                    "new_endpoint_id": "remote-one",
                    "default_remote_name": "Remote 1"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan add remote");

        assert_eq!(result.endpoint.id, "remote-one");
        assert_eq!(
            result.endpoint.auth_token_ref.as_deref(),
            Some("remote-one")
        );
        assert_eq!(
            result
                .token_write
                .as_ref()
                .map(|write| write.token.as_str()),
            Some("token")
        );
    }

    #[test]
    fn gateway_add_and_activate_remote_registry_plan_returns_shared_next_registry() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_add_and_activate_remote_registry(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": null,
                        "remotes": []
                    },
                    "name": " Remote ",
                    "address": "127.0.0.1:23000",
                    "auth_token": " token ",
                    "new_endpoint_id": "remote-one",
                    "default_remote_name": "Remote 1"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan add remote registry");

        assert_eq!(result.endpoint.id, "remote-one");
        assert_eq!(
            result.registry.active_gateway_id.as_deref(),
            Some("remote-one")
        );
        assert!(result.registry.local.is_none());
        assert_eq!(result.registry.remotes.len(), 1);
        assert_eq!(
            result
                .token_write
                .as_ref()
                .map(|write| write.token.as_str()),
            Some("token")
        );
    }

    #[test]
    fn gateway_activate_registry_plan_returns_shared_next_registry() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_activate_registry(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": null,
                        "remotes": [{
                            "id": "remote-one",
                            "name": "Remote",
                            "address": "127.0.0.1:23000",
                            "kind": "remote",
                            "auth_token_ref": null,
                            "workspace_id": null,
                            "service_name": null
                        }]
                    },
                    "gateway_id": "remote-one"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan activate gateway registry");

        assert_eq!(result.endpoint.id, "remote-one");
        assert_eq!(
            result.registry.active_gateway_id.as_deref(),
            Some("remote-one")
        );
    }

    #[test]
    fn gateway_update_remote_registry_plan_returns_token_write() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_update_remote_registry(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": "remote-one",
                        "remotes": [{
                            "id": "remote-one",
                            "name": "Remote",
                            "address": "127.0.0.1:23000",
                            "kind": "remote",
                            "auth_token_ref": null,
                            "workspace_id": null,
                            "service_name": null
                        }]
                    },
                    "gateway_id": "remote-one",
                    "name": "Renamed",
                    "address": "127.0.0.1:24000",
                    "auth_token_update": {
                        "mode": "replace",
                        "token": " token "
                    },
                    "default_remote_name": "Remote 1"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan update remote registry");

        assert_eq!(result.endpoint.name, "Renamed");
        assert_eq!(result.endpoint.address, "127.0.0.1:24000");
        assert_eq!(
            result
                .token_write
                .as_ref()
                .map(|write| write.token.as_str()),
            Some("token")
        );
    }

    #[test]
    fn gateway_delete_remote_registry_plan_returns_fallback() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_delete_remote_registry(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": "remote-one",
                        "remotes": [
                            {
                                "id": "remote-one",
                                "name": "One",
                                "address": "127.0.0.1:23000",
                                "kind": "remote",
                                "auth_token_ref": "remote-one",
                                "workspace_id": null,
                                "service_name": null
                            },
                            {
                                "id": "remote-two",
                                "name": "Two",
                                "address": "127.0.0.1:24000",
                                "kind": "remote",
                                "auth_token_ref": null,
                                "workspace_id": null,
                                "service_name": null
                            }
                        ]
                    },
                    "gateway_id": "remote-one"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan delete remote registry");

        assert!(result.deleted_active);
        assert_eq!(result.endpoint.id, "remote-one");
        assert_eq!(result.deleted_token_ref.as_deref(), Some("remote-one"));
        assert_eq!(
            result.registry.active_gateway_id.as_deref(),
            Some("remote-two")
        );
    }

    #[test]
    fn gateway_set_workspace_registry_plan_returns_shared_next_registry() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_set_workspace_registry(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": "remote-one",
                        "remotes": [{
                            "id": "remote-one",
                            "name": "Remote",
                            "address": "127.0.0.1:23000",
                            "kind": "remote",
                            "auth_token_ref": null,
                            "workspace_id": null,
                            "service_name": null
                        }]
                    },
                    "gateway_id": "remote-one",
                    "workspace_id": "ws-selected"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan gateway workspace registry");

        assert_eq!(result.endpoint.id, "remote-one");
        assert_eq!(result.endpoint.workspace_id.as_deref(), Some("ws-selected"));
        assert_eq!(
            result.registry.remotes[0].workspace_id.as_deref(),
            Some("ws-selected")
        );
    }

    #[test]
    fn ffi_boundary_converts_panic_to_error_response() {
        let response = ffi_response_json::<(), _>(|| panic!("boom"));
        let error = serde_json::from_str::<serde_json::Value>(response.as_str()).expect("json");

        assert_eq!(error["status"], "error");
        assert_eq!(error["code"], "pioneer_client_ffi_panic");
        assert!(error["message"].as_str().unwrap().contains("boom"));
    }
}
