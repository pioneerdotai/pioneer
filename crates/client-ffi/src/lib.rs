//! C ABI boundary for native Pioneer client shells.
//!
//! This crate intentionally owns ABI/runtime glue and shell-boundary DTOs.
//! Client domain logic remains in `pioneer-client`.

mod active_thread;
mod composer;
mod contracts;
mod gateway;
#[cfg(feature = "schema")]
pub mod schema;
mod threads;
mod workspaces;

use active_thread::{
    ClientActiveThreadCancelTurnRequest, ClientActiveThreadCancelTurnResult,
    ClientActiveThreadClearResult, ClientActiveThreadEventRequest, ClientActiveThreadOpenRequest,
    ClientActiveThreadSendTextRequest, ClientActiveThreadSendTextResult,
    ClientActiveThreadSnapshot, ClientActiveThreadSnapshotRequest, ClientFfiActiveThreadState,
};
use composer::{
    ClientComposerAttachmentFromPathRequest, ClientComposerAttachmentsUpdateRequest,
    ClientComposerCapabilitiesUpdateRequest, ClientComposerFilterMcpRowsRequest,
    ClientComposerFilterMcpRowsResult, ClientComposerFilterSkillRowsRequest,
    ClientComposerMcpCapabilityFromRowRequest, ClientComposerMcpPickerRowsRequest,
    ClientComposerMcpPickerRowsResult, ClientComposerMcpToggleRequest,
    ClientComposerMcpToggleResult, ClientComposerSkillCapabilityFromRowRequest,
    ClientComposerSkillPickerRowsRequest, ClientComposerSkillToggleRequest,
    ClientComposerSkillToggleResult, composer_attachment_from_path_request,
    composer_mcp_picker_rows, composer_skill_picker_rows, filter_mcp_picker_rows,
    filter_skill_picker_rows, mcp_capability_from_row, skill_capability_from_row,
    toggle_mcp_picker_selection, toggle_skill_picker_selection, update_composer_attachments,
    update_composer_capabilities,
};
use contracts::{
    ClientEvent, ClientGatewayConnectRequest, ClientGatewayConnectResult,
    reduce_gateway_ws_events_to_client_events,
};
use gateway::{
    AddAndActivateRemoteGatewayRegistryPlan, AddRemoteGatewayPlan, PlanActivateGatewayRequest,
    PlanAddRemoteGatewayRequest, PlanDeleteRemoteGatewayRequest, PlanSetGatewayWorkspaceRequest,
    PlanUpdateRemoteGatewayRequest, RemoteGatewayValidationRequest,
    plan_activate_gateway_registry_request, plan_add_and_activate_remote_gateway_registry_request,
    plan_add_remote_gateway_request, plan_delete_remote_gateway_registry_request,
    plan_set_gateway_workspace_registry_request, plan_update_remote_gateway_registry_request,
    validate_remote_gateway_request,
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
            ProviderModelDisplayKey, ProviderModelDisplayResolution, provider_model_display_key,
            provider_model_display_models_params, resolve_provider_model_display_from_response,
        },
    },
    runtime::ClientRuntime,
    workspaces::{
        actions::WorkspaceBootstrapSuccessReduction,
        bootstrap::{WorkspaceBootstrapRequest, bootstrap_workspace_catalog},
    },
};
use pioneer_protocol::{
    CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse, CLIRuntimeListParams,
    CLIRuntimeListResponse, CLIRuntimeRequestRespondParams, CLIRuntimeRequestRespondResponse,
    CLIRuntimeReviewStartParams, CLIRuntimeReviewStartResponse, CLIRuntimeThreadBindingGetParams,
    CLIRuntimeThreadBindingGetResponse, CLIRuntimeThreadCompactParams,
    CLIRuntimeThreadCompactResponse, CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse,
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ThreadAgentsDocArchiveParams, ThreadAgentsDocArchiveResponse, ThreadAgentsDocGetParams,
    ThreadAgentsDocGetResponse, ThreadAgentsDocSaveParams, ThreadAgentsDocSaveResponse,
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
    ) -> Result<ClientActiveThreadSnapshot, String> {
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
}

fn ffi_client_json_response<T, F>(
    ptr: *mut PioneerClientFfi,
    input_json: *const c_char,
    operation: F,
) -> *mut c_char
where
    T: Serialize,
    F: FnOnce(&ClientFfiRuntime, &str) -> Result<T, String>,
{
    into_ffi_response(|| {
        let client = unsafe { ffi_ref(ptr)? };
        let input_json = unsafe { read_c_string(input_json)? };
        operation(&client.runtime, input_json.as_str())
    })
}

fn ffi_client_response<T, F>(ptr: *mut PioneerClientFfi, operation: F) -> *mut c_char
where
    T: Serialize,
    F: FnOnce(&ClientFfiRuntime) -> Result<T, String>,
{
    into_ffi_response(|| {
        let client = unsafe { ffi_ref(ptr)? };
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
            ffi_client_json_response(ptr, input_json, |runtime, input_json| {
                runtime.$runtime_method(input_json)
            })
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
ffi_client_json_method!(pioneer_client_ffi_workspace_bootstrap, workspace_bootstrap);
ffi_client_json_method!(pioneer_client_ffi_workspace_switch, workspace_switch);
ffi_client_json_method!(pioneer_client_ffi_workspace_create, workspace_create);
ffi_client_json_method!(pioneer_client_ffi_workspace_rename, workspace_rename);
ffi_client_json_method!(pioneer_client_ffi_provider_list, provider_list);
ffi_client_json_method!(pioneer_client_ffi_cli_runtime_list, cli_runtime_list);
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
    pioneer_client_ffi_provider_list_models,
    provider_list_models
);
ffi_client_json_method!(
    pioneer_client_ffi_provider_model_display,
    provider_model_display
);
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
ffi_client_json_method!(pioneer_client_ffi_agents_doc_get, agents_doc_get);
ffi_client_json_method!(pioneer_client_ffi_agents_doc_save, agents_doc_save);
ffi_client_json_method!(pioneer_client_ffi_agents_doc_archive, agents_doc_archive);
ffi_client_json_method!(pioneer_client_ffi_active_thread_open, active_thread_open);
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
    pioneer_client_ffi_active_thread_cancel_turn,
    active_thread_cancel_turn
);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_active_thread_clear(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, |runtime| runtime.active_thread_clear())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_gateway_next_events(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, |runtime| runtime.gateway_next_events())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_gateway_disconnect(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, |runtime| runtime.gateway_disconnect())
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
    let response = match result {
        Ok(value) => serde_json::to_string(&FfiResponse::Ok { value }),
        Err(message) => serde_json::to_string(&FfiResponse::<()>::Error {
            message,
            code: Some("pioneer_client_ffi_error".to_owned()),
        }),
    };

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

fn ffi_response_json<T, F>(operation: F) -> String
where
    T: Serialize,
    F: FnOnce() -> Result<T, String>,
{
    catch_unwind(AssertUnwindSafe(|| to_json_response(operation()))).unwrap_or_else(|payload| {
        to_json_response::<()>(Err(format!(
            "panic in pioneer client ffi: {}",
            panic_message(payload)
        )))
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
    fn gateway_validation_uses_shared_request_contract() {
        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .gateway_validate_remote(r#"{"address":"127.0.0.1:23000","timeout_ms":0}"#)
            .expect_err("zero timeout should fail");

        assert!(error.contains("timeout must be positive"));
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
        assert_eq!(error["code"], "pioneer_client_ffi_error");
        assert!(error["message"].as_str().unwrap().contains("boom"));
    }
}
