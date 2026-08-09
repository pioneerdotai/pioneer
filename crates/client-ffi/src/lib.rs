//! C ABI boundary for native Pioneer client shells.
//!
//! This crate intentionally owns ABI/runtime glue and shell-boundary DTOs.
//! Client domain logic remains in `pioneer-client`.

mod active_thread;
mod artifacts;
mod auth;
mod avatars;
mod composer;
mod contracts;
mod diagnostics;
mod gateway;
mod invitation;
mod pending_requests;
mod presentation;
#[cfg(feature = "schema")]
pub mod schema;
mod skills;
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
use artifacts::{
    ClientArtifactDownloadCancelResult, ClientArtifactDownloadOperationRequest,
    ClientArtifactDownloadProgressResult, ClientArtifactDownloadRequest,
    ClientArtifactDownloadResult, ClientArtifactTargetRequest, ClientArtifactViewOpenResult,
};
use auth::{
    ClientAuthDeviceActivateRequest, ClientAuthRefreshRequest,
    ClientGatewaySessionReplaceAccessRequest, ClientGatewaySessionReplaceAccessResult,
    auth_exchange_error, auth_exchange_runtime,
};
use avatars::{
    ClientAgentAvatarCacheRequest, ClientAgentAvatarCacheResult, ClientFfiAvatarCache,
    ClientMemberAvatarCacheRequest, ClientMemberAvatarCacheResult,
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
    ClientEvent, ClientGatewaySettingsGetRequest, ClientGatewaySettingsUpdateRequest,
    ClientVoiceInputPlanRequest, ClientVoiceInputPlanResult,
    reduce_gateway_ws_events_to_client_events,
};
use diagnostics::{ClientDiagnosticEvent, ClientFfiDiagnostics};
use gateway::{
    AddAndActivateRemoteGatewayRegistryPlan, AddRemoteGatewayPlan, LoadGatewayRegistryRequest,
    LoadGatewayRegistryResult, PlanActivateGatewayRequest, PlanAddRemoteGatewayRequest,
    PlanDeleteRemoteGatewayRequest, PlanSetGatewayWorkspaceRequest, PlanUpdateRemoteGatewayRequest,
    RemoteGatewayValidationRequest, gateway_settings_error_code, gateway_settings_get_for_bridge,
    gateway_settings_update_for_bridge, load_gateway_registry_request,
    plan_activate_gateway_registry_request, plan_add_and_activate_remote_gateway_registry_request,
    plan_add_remote_gateway_request, plan_delete_remote_gateway_registry_request,
    plan_set_gateway_workspace_registry_request, plan_update_remote_gateway_registry_request,
    provider_list_transcription_models_for_bridge, validate_remote_gateway_request,
    voice_input_plan_for_bridge,
};
use invitation::{
    ClientInvitationAcceptRequest, ClientInvitationAcceptResult, ClientInvitationAccessResult,
    ClientInvitationCommitCleanupRequest, ClientInvitationCommitFailureResult,
    ClientInvitationCommitRequest, ClientInvitationPresentationRequest,
    ClientInvitationPresentationResult, ClientInvitationPreviewRequest,
    ClientInvitationPreviewResult, ClientInvitationRefreshWrite, ClientInvitationRegistryWrite,
};
use pending_requests::{
    ClientPendingRequestPresentationRequest, ClientPendingRequestPresentationResult,
    ClientPendingRequestResponsePlanRequest, ClientPendingRequestResponsePlanResult,
    pending_request_presentation_for_bridge, plan_pending_request_response_for_bridge,
};
use pioneer_client::gateway::invitation::InvitationSessionCommitState;
use pioneer_client::{
    agents_doc::content::{
        AgentsDocSaveErrorKind, agents_doc_get_params, agents_doc_save_error_kind,
        agents_doc_save_params,
    },
    gateway::setup::{
        ActivateGatewayRegistryPlan, DeleteRemoteGatewayRegistryPlan, RemoteGatewayValidation,
        SetGatewayWorkspaceRegistryPlan, UpdateRemoteGatewayRegistryPlan,
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
    timeline::rows::{MessageRevisionPagePresentation, project_message_revision_page},
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
    ThreadReadParams, ThreadReadResponse, ThreadTimelinePageParams, ThreadTimelinePageResponse,
    TimelinePageAnchor, TurnMessageDeleteParams, TurnMessageDeleteResponse, TurnMessageEditParams,
    TurnMessageEditResponse, TurnMessageRevisionsPageParams, TurnMessageRevisionsPageResponse,
    TurnPermissionRequestRespondParams, TurnPermissionRequestRespondResponse,
    TurnWorkItemsGetParams, TurnWorkItemsGetResponse, TurnWorkPageParams, TurnWorkPageResponse,
    VoiceAudioFormat, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionStartParams,
    VoiceSessionStartResponse, VoiceStatusParams, VoiceStatusResponse,
};
use presentation::{
    ClientCurrentPrincipalPresentationRequest, ClientInvitationListRowRequest,
    ClientMemberPresentationRequest, ClientThreadCreateVisibilityRequest,
    ClientThreadScopeMutationPlanRequest, ClientThreadScopePresentationRequest, current_principal,
    invitation_list_row, member_presentation, principal_capabilities, session_list_row,
    thread_create_visibility, thread_scope, thread_scope_mutation_plan,
};
use serde::{Deserialize, Serialize};
use skills::{
    ClientComposerSkillChipsRequest, ClientComposerSkillPackPickerRequest,
    ClientComposerSkillSelectionToggleRequest, composer_skill_chips, composer_skill_pack_picker,
    composer_skill_selection_toggle,
};
use std::{
    any::Any,
    collections::HashMap,
    ffi::{CStr, CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use threads::{
    ClientThreadTreeLevel, ClientThreadTreeQueryData, ThreadTreeLevelRequest,
    ThreadTreeRefreshRequest, client_thread_tree_level, refresh_thread_tree,
};
use timeline::{thread_timeline_page, turn_work_items_get, turn_work_page};
use workspaces::{
    WorkspaceCreateRequest, WorkspaceCreateResult, WorkspaceRenameRequest, WorkspaceRenameResult,
    WorkspaceSwitchRequest, WorkspaceSwitchResult, create_workspace, rename_workspace,
    switch_workspace,
};
use zeroize::Zeroizing;

const FFI_VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_OUTSTANDING_INVITATION_COMMITS: usize = 16;

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
    gateway_session_lifecycles:
        Mutex<HashMap<String, pioneer_client::gateway::session_lifecycle::SessionLifecycle>>,
    artifact_downloads: artifacts::ClientFfiArtifactDownloads,
    avatar_cache: ClientFfiAvatarCache,
    invitation_commit_sequence: AtomicU64,
    invitation_commits:
        Mutex<HashMap<String, pioneer_client::gateway::invitation::InvitationSessionCommit>>,
}

fn contains_gateway_connection_epoch_boundary(events: &[ClientEvent]) -> bool {
    events
        .iter()
        .any(|event| matches!(event, ClientEvent::GatewayConnectionChanged(_)))
}

fn contains_session_termination(events: &[ClientEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            ClientEvent::GatewayNotification(
                pioneer_protocol::GatewayNotification::AuthSessionRevoked(_)
            )
        )
    })
}

fn contains_avatar_authorization_boundary(events: &[ClientEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            ClientEvent::GatewayNotification(
                pioneer_protocol::GatewayNotification::AccessChanged(_)
                    | pioneer_protocol::GatewayNotification::MemberChanged(_)
                    | pioneer_protocol::GatewayNotification::WorkspaceMembersChanged(_)
            )
        )
    })
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
    code: String,
}

impl ClientFfiError {
    pub(crate) const GENERIC_CODE: &'static str = "pioneer_client_ffi_error";

    pub(crate) fn new(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: code.into(),
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

        plan_add_remote_gateway_request(request).map_err(|error| error.to_string())
    }

    fn gateway_load_registry_v3(
        &self,
        input_json: &str,
    ) -> Result<LoadGatewayRegistryResult, String> {
        let request = serde_json::from_str::<LoadGatewayRegistryRequest>(input_json)
            .map_err(|_| "invalid Gateway registry load request".to_owned())?;
        load_gateway_registry_request(request).map_err(|error| error.to_string())
    }

    fn gateway_plan_add_and_activate_remote_registry(
        &self,
        input_json: &str,
    ) -> Result<AddAndActivateRemoteGatewayRegistryPlan, String> {
        let request = serde_json::from_str::<PlanAddRemoteGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway add remote registry request: {error}"))?;

        plan_add_and_activate_remote_gateway_registry_request(request)
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

        plan_update_remote_gateway_registry_request(request).map_err(|error| error.to_string())
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

    fn gateway_session_lifecycle_reduce(
        &self,
        input_json: &str,
    ) -> Result<auth::ClientGatewaySessionLifecycleResult, ClientFfiError> {
        self.require_initialized()?;
        let request =
            serde_json::from_str::<auth::ClientGatewaySessionLifecycleRequest>(input_json)
                .map_err(|_| {
                    ClientFfiError::new(
                        "invalid Gateway session lifecycle request",
                        auth::INVALID_AUTH_REQUEST_CODE,
                    )
                })?;
        let endpoint_id = request.endpoint_id.trim();
        if endpoint_id.is_empty() || endpoint_id.len() > 128 {
            return Err(ClientFfiError::new(
                "invalid Gateway session lifecycle endpoint id",
                auth::INVALID_AUTH_REQUEST_CODE,
            ));
        }
        let mut lifecycles = self.gateway_session_lifecycles.lock().map_err(|_| {
            ClientFfiError::new(
                "Gateway session lifecycle lock is poisoned",
                ClientFfiError::GENERIC_CODE,
            )
        })?;
        let release_lifecycle = matches!(
            &request.event,
            pioneer_client::gateway::session_lifecycle::SessionLifecycleEvent::NoStoredSession
        );
        let result = {
            let lifecycle = lifecycles.entry(endpoint_id.to_owned()).or_default();
            let effect = lifecycle.reduce(request.event);
            auth::ClientGatewaySessionLifecycleResult {
                state: lifecycle.state().clone(),
                effect,
            }
        };
        if release_lifecycle {
            lifecycles.remove(endpoint_id);
        }
        Ok(result)
    }

    fn gateway_device_activation_presentation(
        &self,
        input_json: &str,
    ) -> Result<auth::ClientDeviceActivationPresentationResult, ClientFfiError> {
        self.require_initialized()?;
        let request =
            serde_json::from_str::<auth::ClientDeviceActivationPresentationRequest>(input_json)
                .map_err(|_| {
                    ClientFfiError::new(
                        "invalid activation presentation request",
                        auth::INVALID_AUTH_REQUEST_CODE,
                    )
                })?;
        auth::ClientDeviceActivationPresentationResult::from_request(request)
            .map_err(|message| ClientFfiError::new(message, auth::INVALID_AUTH_REQUEST_CODE))
    }

    fn gateway_device_activation_parse(
        &self,
        input_json: &str,
    ) -> Result<auth::ClientDeviceActivationParseResult, ClientFfiError> {
        self.require_initialized()?;
        let request = serde_json::from_str::<auth::ClientDeviceActivationParseRequest>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid activation URI request",
                    auth::INVALID_AUTH_REQUEST_CODE,
                )
            })?;
        auth::ClientDeviceActivationParseResult::from_request(request)
            .map_err(|message| ClientFfiError::new(message, auth::INVALID_AUTH_REQUEST_CODE))
    }

    fn gateway_auth_refresh(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthRefreshGrant, ClientFfiError> {
        self.require_initialized()?;
        let request =
            serde_json::from_str::<ClientAuthRefreshRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid auth refresh request",
                    auth::INVALID_AUTH_REQUEST_CODE,
                )
            })?;
        let (runtime, client) = auth_exchange_runtime(request.timeout_ms)
            .map_err(|message| ClientFfiError::new(message, auth::AUTH_EXCHANGE_RUNTIME_CODE))?;
        runtime
            .block_on(client.refresh(
                &request.gateway_base_url,
                request.credential.expose_secret(),
                request.params,
            ))
            .map_err(auth_exchange_error)
    }

    fn gateway_auth_device_activate(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthSessionGrant, ClientFfiError> {
        self.require_initialized()?;
        let request =
            serde_json::from_str::<ClientAuthDeviceActivateRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid device activation request",
                    auth::INVALID_AUTH_REQUEST_CODE,
                )
            })?;
        let (runtime, client) = auth_exchange_runtime(request.timeout_ms)
            .map_err(|message| ClientFfiError::new(message, auth::AUTH_EXCHANGE_RUNTIME_CODE))?;
        runtime
            .block_on(client.activate_device(
                &request.gateway_base_url,
                request.credential.expose_secret(),
                request.params,
            ))
            .map_err(auth_exchange_error)
    }

    fn gateway_auth_session_cleanup(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthSessionRevokeResponse, ClientFfiError> {
        self.require_initialized()?;
        let request = serde_json::from_str::<auth::ClientAuthSessionCleanupRequest>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid auth session cleanup request",
                    auth::INVALID_AUTH_REQUEST_CODE,
                )
            })?;
        let (runtime, client) = auth_exchange_runtime(request.timeout_ms)
            .map_err(|message| ClientFfiError::new(message, auth::AUTH_EXCHANGE_RUNTIME_CODE))?;
        runtime
            .block_on(client.cleanup_session_once(
                &request.gateway_base_url,
                request.access_token.expose_secret(),
                request.session_id,
            ))
            .map_err(auth_exchange_error)
    }

    fn gateway_auth_me(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthMeResponse, ClientFfiError> {
        parse_empty_auth_request(input_json)?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .auth_me()
            .map_err(normal_auth_error)
    }

    fn gateway_auth_session_list(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthSessionListResponse, ClientFfiError> {
        parse_empty_auth_request(input_json)?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .auth_session_list()
            .map_err(normal_auth_error)
    }

    fn gateway_auth_session_revoke(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthSessionRevokeResponse, ClientFfiError> {
        let params = serde_json::from_str::<pioneer_protocol::AuthSessionRevokeParams>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid session revoke request",
                    auth::INVALID_AUTH_REQUEST_CODE,
                )
            })?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .auth_session_revoke(params)
            .map_err(normal_auth_error)
    }

    fn gateway_auth_logout(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthLogoutResponse, ClientFfiError> {
        parse_empty_auth_request(input_json)?;
        self.require_initialized_and_connected()?;
        let response = self
            .client_runtime
            .ws_command_sender()
            .auth_logout()
            .map_err(normal_auth_error)?;
        if let Ok(runtime_home) = self.native_cache_runtime_home() {
            self.avatar_cache.invalidate_all(runtime_home.as_path());
        }
        self.invitation_commits
            .lock()
            .map_err(|_| invitation_commit_lock_error())?
            .clear();
        Ok(response)
    }

    fn gateway_auth_device_create(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthDeviceCreateResponse, ClientFfiError> {
        parse_empty_auth_request(input_json)?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .auth_device_create()
            .map_err(normal_auth_error)
    }

    fn invitation_presentation(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationPresentationResult, ClientFfiError> {
        self.require_initialized()?;
        let request = serde_json::from_str::<ClientInvitationPresentationRequest>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid invitation presentation request",
                    invitation::INVALID_INVITATION_REQUEST_CODE,
                )
            })?;
        ClientInvitationPresentationResult::from_request(request).map_err(|message| {
            ClientFfiError::new(message, invitation::INVALID_INVITATION_REQUEST_CODE)
        })
    }

    fn invitation_preview(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationPreviewResult, ClientFfiError> {
        self.require_initialized()?;
        let request =
            serde_json::from_str::<ClientInvitationPreviewRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid invitation preview request",
                    invitation::INVALID_INVITATION_REQUEST_CODE,
                )
            })?;
        let presentation = invitation::parse_preview(&request).map_err(|message| {
            ClientFfiError::new(message, invitation::INVALID_INVITATION_REQUEST_CODE)
        })?;
        let (runtime, client) = auth_exchange_runtime(request.timeout_ms)
            .map_err(|message| ClientFfiError::new(message, auth::AUTH_EXCHANGE_RUNTIME_CODE))?;
        runtime
            .block_on(client.preview_invitation(&presentation))
            .map_err(invitation::exchange_error)
    }

    fn invitation_accept(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationAcceptResult, ClientFfiError> {
        self.require_initialized()?;
        let request =
            serde_json::from_str::<ClientInvitationAcceptRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid invitation accept request",
                    invitation::INVALID_INVITATION_REQUEST_CODE,
                )
            })?;
        let presentation = invitation::parse_accept(&request).map_err(|message| {
            ClientFfiError::new(message, invitation::INVALID_INVITATION_REQUEST_CODE)
        })?;
        {
            let commits = self
                .invitation_commits
                .lock()
                .map_err(|_| invitation_commit_lock_error())?;
            if !invitation_commit_capacity_available(commits.len()) {
                return Err(invitation_commit_unavailable());
            }
        }
        let (runtime, client) = auth_exchange_runtime(request.timeout_ms)
            .map_err(|message| ClientFfiError::new(message, auth::AUTH_EXCHANGE_RUNTIME_CODE))?;
        let accepted = runtime
            .block_on(client.accept_invitation(&presentation, request.params))
            .map_err(invitation::exchange_error)?;
        let commit = pioneer_client::gateway::invitation::InvitationSessionCommit::new(
            &presentation,
            accepted,
            request.expected_installation_id.as_str(),
        )
        .map_err(|_| {
            ClientFfiError::new(
                "invalid invitation session grant",
                invitation::INVALID_INVITATION_REQUEST_CODE,
            )
        })?;
        let state = commit.state().into();
        let sequence = self
            .invitation_commit_sequence
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        let commit_id = format!("invitation_commit_{sequence}");
        let mut commits = match self.invitation_commits.lock() {
            Ok(commits) => commits,
            Err(_) => {
                cleanup_untracked_invitation_commit(&runtime, &client, commit);
                return Err(invitation_commit_lock_error());
            }
        };
        if !invitation_commit_capacity_available(commits.len()) {
            drop(commits);
            cleanup_untracked_invitation_commit(&runtime, &client, commit);
            return Err(invitation_commit_unavailable());
        }
        commits.insert(commit_id.clone(), commit);
        Ok(ClientInvitationAcceptResult { commit_id, state })
    }

    fn invitation_commit_take_refresh(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationRefreshWrite, ClientFfiError> {
        self.require_initialized()?;
        let request = parse_invitation_commit_request(input_json)?;
        let mut commits = self
            .invitation_commits
            .lock()
            .map_err(|_| invitation_commit_lock_error())?;
        let commit = commits
            .get_mut(request.commit_id.as_str())
            .ok_or_else(invitation_commit_unavailable)?;
        commit
            .take_refresh_for_secure_storage()
            .map(ClientInvitationRefreshWrite::from)
            .map_err(|_| invitation_commit_unavailable())
    }

    fn invitation_commit_secure_storage_committed(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationRegistryWrite, ClientFfiError> {
        self.require_initialized()?;
        let request = parse_invitation_commit_request(input_json)?;
        let mut commits = self
            .invitation_commits
            .lock()
            .map_err(|_| invitation_commit_lock_error())?;
        let commit = commits
            .get_mut(request.commit_id.as_str())
            .ok_or_else(invitation_commit_unavailable)?;
        commit
            .secure_storage_committed()
            .map(ClientInvitationRegistryWrite::from)
            .map_err(|_| invitation_commit_unavailable())
    }

    fn invitation_commit_registry_committed(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationAccessResult, ClientFfiError> {
        self.require_initialized()?;
        let request = parse_invitation_commit_request(input_json)?;
        let mut commits = self
            .invitation_commits
            .lock()
            .map_err(|_| invitation_commit_lock_error())?;
        let access = commits
            .get_mut(request.commit_id.as_str())
            .ok_or_else(invitation_commit_unavailable)?
            .registry_committed()
            .map_err(|_| invitation_commit_unavailable())?;
        commits.remove(request.commit_id.as_str());
        Ok(access.into())
    }

    fn invitation_commit_registry_failed(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationCommitFailureResult, ClientFfiError> {
        self.require_initialized()?;
        let request = parse_invitation_commit_request(input_json)?;
        let mut commits = self
            .invitation_commits
            .lock()
            .map_err(|_| invitation_commit_lock_error())?;
        let commit = commits
            .get_mut(request.commit_id.as_str())
            .ok_or_else(invitation_commit_unavailable)?;
        commit
            .registry_failed()
            .map_err(|_| invitation_commit_unavailable())?;
        commits.remove(request.commit_id.as_str());
        Ok(ClientInvitationCommitFailureResult {
            released: true,
            cleanup_attempted: false,
        })
    }

    fn invitation_commit_secure_storage_failed(
        &self,
        input_json: &str,
    ) -> Result<ClientInvitationCommitFailureResult, ClientFfiError> {
        self.require_initialized()?;
        let request = serde_json::from_str::<ClientInvitationCommitCleanupRequest>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid invitation cleanup request",
                    invitation::INVALID_INVITATION_REQUEST_CODE,
                )
            })?;
        {
            let commits = self
                .invitation_commits
                .lock()
                .map_err(|_| invitation_commit_lock_error())?;
            let commit = commits
                .get(request.commit_id.as_str())
                .ok_or_else(invitation_commit_unavailable)?;
            if commit.state() != InvitationSessionCommitState::AwaitingSecureStorage {
                return Err(invitation_commit_unavailable());
            }
        }
        let (runtime, client) = auth_exchange_runtime(request.timeout_ms)
            .map_err(|message| ClientFfiError::new(message, auth::AUTH_EXCHANGE_RUNTIME_CODE))?;
        let commit = {
            let mut commits = self
                .invitation_commits
                .lock()
                .map_err(|_| invitation_commit_lock_error())?;
            let commit = commits
                .get(request.commit_id.as_str())
                .ok_or_else(invitation_commit_unavailable)?;
            if commit.state() != InvitationSessionCommitState::AwaitingSecureStorage {
                return Err(invitation_commit_unavailable());
            }
            commits
                .remove(request.commit_id.as_str())
                .expect("invitation commit verified under the same lock")
        };
        let cleanup = commit
            .secure_storage_failed()
            .map_err(|_| invitation_commit_unavailable())?;
        runtime.block_on(client.cleanup_invitation_session_best_effort(cleanup));
        Ok(ClientInvitationCommitFailureResult {
            released: true,
            cleanup_attempted: true,
        })
    }

    fn invitation_create(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::InvitationCreateResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "invitation create")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .invitation_create(params)
            .map_err(administration_rpc_error)
    }

    fn invitation_list(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::InvitationListResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "invitation list")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .invitation_list(params)
            .map_err(administration_rpc_error)
    }

    fn invitation_revoke(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::InvitationRevokeResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "invitation revoke")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .invitation_revoke(params)
            .map_err(administration_rpc_error)
    }

    fn member_list(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::MemberListResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "member list")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .member_list(params)
            .map_err(administration_rpc_error)
    }

    fn member_avatar_cache(
        &self,
        input_json: &str,
    ) -> Result<ClientMemberAvatarCacheResult, ClientFfiError> {
        self.require_initialized_and_connected()?;
        let request =
            serde_json::from_str::<ClientMemberAvatarCacheRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid member avatar cache request",
                    "avatar_invalid_request",
                )
            })?;
        self.avatar_cache.resolve(
            &self.client_runtime.ws_command_sender(),
            self.native_cache_runtime_home()?,
            request,
        )
    }

    fn agent_avatar_cache(
        &self,
        input_json: &str,
    ) -> Result<ClientAgentAvatarCacheResult, ClientFfiError> {
        self.require_initialized_and_connected()?;
        let request =
            serde_json::from_str::<ClientAgentAvatarCacheRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid Agent avatar cache request",
                    "avatar_invalid_request",
                )
            })?;
        self.avatar_cache.resolve_agent(
            &self.client_runtime.ws_command_sender(),
            self.native_cache_runtime_home()?,
            request,
        )
    }

    fn member_suspend(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::MemberMutationResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "member suspend")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .member_suspend(params)
            .map_err(administration_rpc_error)
    }

    fn member_restore(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::MemberMutationResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "member restore")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .member_restore(params)
            .map_err(administration_rpc_error)
    }

    fn member_remove(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::MemberMutationResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "member remove")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .member_remove(params)
            .map_err(administration_rpc_error)
    }

    fn member_device_create(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::MemberDeviceCreateResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "member device create")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .member_device_create(params)
            .map_err(administration_rpc_error)
    }

    fn workspace_member_list(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::WorkspaceMemberListResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "workspace member list")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .workspace_member_list(params)
            .map_err(administration_rpc_error)
    }

    fn workspace_member_add(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::WorkspaceMemberMutationResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "workspace member add")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .workspace_member_add(params)
            .map_err(administration_rpc_error)
    }

    fn workspace_member_remove(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::WorkspaceMemberMutationResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "workspace member remove")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .workspace_member_remove(params)
            .map_err(administration_rpc_error)
    }

    fn thread_participants_list(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::ThreadParticipantsResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "thread participants list")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .thread_participants_list(params)
            .map_err(administration_rpc_error)
    }

    fn thread_update(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::ThreadUpdateResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "thread update")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .thread_update(params)
            .map_err(administration_rpc_error)
    }

    fn thread_participant_add(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::ThreadParticipantsResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "thread participant add")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .thread_participant_add(params)
            .map_err(administration_rpc_error)
    }

    fn thread_participant_remove(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::ThreadParticipantsResponse, ClientFfiError> {
        let params = parse_normal_params(input_json, "thread participant remove")?;
        self.require_initialized_and_connected()?;
        self.client_runtime
            .ws_command_sender()
            .thread_participant_remove(params)
            .map_err(administration_rpc_error)
    }

    fn gateway_session_replace_access(
        &self,
        input_json: &str,
    ) -> Result<ClientGatewaySessionReplaceAccessResult, ClientFfiError> {
        self.require_initialized()?;
        let request = serde_json::from_str::<ClientGatewaySessionReplaceAccessRequest>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid access replacement request",
                    auth::INVALID_AUTH_REQUEST_CODE,
                )
            })?;
        let spec = request
            .into_session_spec()
            .map_err(|message| ClientFfiError::new(message, auth::INVALID_AUTH_REQUEST_CODE))?;
        let connection_id = self
            .client_runtime
            .ws_command_sender()
            .replace_access_and_wait(spec.into_connect_spec())
            .map_err(normal_auth_error)?;
        if let Ok(runtime_home) = self.native_cache_runtime_home() {
            self.avatar_cache.invalidate_all(runtime_home.as_path());
        }
        self.active_thread
            .begin_authorization_epoch()
            .map_err(|error| {
                ClientFfiError::new(format!("{error:#}"), ClientFfiError::GENERIC_CODE)
            })?;
        *self.active_connection_id.lock().map_err(|_| {
            ClientFfiError::new(
                "client ffi connection lock is poisoned",
                ClientFfiError::GENERIC_CODE,
            )
        })? = Some(connection_id);
        Ok(ClientGatewaySessionReplaceAccessResult { connection_id })
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
                if contains_session_termination(events.as_slice())
                    || contains_avatar_authorization_boundary(events.as_slice())
                {
                    if let Ok(runtime_home) = self.native_cache_runtime_home() {
                        self.avatar_cache.invalidate_all(runtime_home.as_path());
                    }
                }
                if contains_session_termination(events.as_slice()) {
                    self.invitation_commits
                        .lock()
                        .map_err(|_| "invitation commit state is unavailable".to_owned())?
                        .clear();
                }
                if contains_gateway_connection_epoch_boundary(events.as_slice()) {
                    self.active_thread
                        .begin_authorization_epoch()
                        .map_err(|error| format!("{error:#}"))?;
                }
                return Ok(events);
            }
        }
    }

    fn gateway_disconnect(&self) -> Result<ClientFfiGatewayDisconnectResult, String> {
        self.artifact_downloads.cancel_all();
        if let Ok(runtime_home) = self.native_cache_runtime_home() {
            self.avatar_cache.invalidate_all(runtime_home.as_path());
        }
        self.client_runtime
            .ws_command_sender()
            .disconnect()
            .map_err(|error| format!("{error:#}"))?;
        self.active_thread
            .begin_authorization_epoch()
            .map_err(|error| format!("{error:#}"))?;
        *self
            .active_connection_id
            .lock()
            .map_err(|_| "client ffi connection lock is poisoned".to_owned())? = None;
        self.invitation_commits
            .lock()
            .map_err(|_| "invitation commit state is unavailable".to_owned())?
            .clear();
        Ok(ClientFfiGatewayDisconnectResult { disconnected: true })
    }

    fn artifact_view_open(
        &self,
        input_json: &str,
    ) -> Result<ClientArtifactViewOpenResult, ClientFfiError> {
        self.require_initialized_and_connected()?;
        let request =
            serde_json::from_str::<ClientArtifactTargetRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid artifact view request",
                    artifacts::INVALID_ARTIFACT_ACTION_CODE,
                )
            })?;
        artifacts::open_artifact_view(&self.client_runtime.ws_command_sender(), request)
    }

    fn artifact_download(
        &self,
        input_json: &str,
    ) -> Result<ClientArtifactDownloadResult, ClientFfiError> {
        self.require_initialized_and_connected()?;
        let request =
            serde_json::from_str::<ClientArtifactDownloadRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid artifact download request",
                    artifacts::INVALID_ARTIFACT_ACTION_CODE,
                )
            })?;
        let runtime_home = self.native_cache_runtime_home()?;
        artifacts::download_artifact(
            &self.client_runtime.ws_command_sender(),
            &self.artifact_downloads,
            runtime_home,
            request,
        )
    }

    fn artifact_download_progress(
        &self,
        input_json: &str,
    ) -> Result<ClientArtifactDownloadProgressResult, ClientFfiError> {
        self.require_initialized()?;
        let request = serde_json::from_str::<ClientArtifactDownloadOperationRequest>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid artifact download progress request",
                    artifacts::INVALID_ARTIFACT_ACTION_CODE,
                )
            })?;
        self.artifact_downloads.progress(request)
    }

    fn artifact_download_cancel(
        &self,
        input_json: &str,
    ) -> Result<ClientArtifactDownloadCancelResult, ClientFfiError> {
        self.require_initialized()?;
        let request = serde_json::from_str::<ClientArtifactDownloadOperationRequest>(input_json)
            .map_err(|_| {
                ClientFfiError::new(
                    "invalid artifact download cancel request",
                    artifacts::INVALID_ARTIFACT_ACTION_CODE,
                )
            })?;
        self.artifact_downloads.cancel(request)
    }

    fn native_cache_runtime_home(&self) -> Result<std::path::PathBuf, ClientFfiError> {
        let config = self.config.lock().map_err(|_| {
            ClientFfiError::new(
                "client ffi config lock is poisoned",
                ClientFfiError::GENERIC_CODE,
            )
        })?;
        let app_data_dir = config
            .as_ref()
            .and_then(|config| config.app_data_dir.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ClientFfiError::new(
                    "native app data directory is required for private file caches",
                    artifacts::ARTIFACT_RECONFIGURATION_CODE,
                )
            })?;
        let runtime_home = std::path::PathBuf::from(app_data_dir);
        if !runtime_home.is_absolute()
            || runtime_home
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(ClientFfiError::new(
                "native app data directory must be an absolute normalized path",
                artifacts::ARTIFACT_RECONFIGURATION_CODE,
            ));
        }
        Ok(runtime_home)
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

        // Voice sessions are scoped to both workspace and thread on the active
        // connection. Always restore that scope first so a recent workspace
        // switch or access-token rotation cannot start Voice on a stale socket.
        self.active_thread
            .ensure_thread_subscription(
                &self.client_runtime,
                params.context.thread_id.as_str(),
                params.context.workspace_id.clone(),
            )
            .map_err(|error| format!("{error:#}"))?;

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

    fn composer_turn_mode_options(&self) -> Result<Vec<pioneer_protocol::ThreadMode>, String> {
        Ok(
            pioneer_client::composer::model_selection::composer_turn_mode_options()
                .into_iter()
                .collect(),
        )
    }

    fn principal_presentation_capabilities(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::authorization::PrincipalPresentationCapabilities, String> {
        let auth = serde_json::from_str::<pioneer_protocol::AuthMeResponse>(input_json)
            .map_err(|_| "invalid current principal presentation request".to_owned())?;
        Ok(principal_capabilities(auth))
    }

    fn current_principal_presentation(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::authorization::CurrentPrincipalPresentation, String> {
        let request = serde_json::from_str::<ClientCurrentPrincipalPresentationRequest>(input_json)
            .map_err(|_| "invalid current principal presentation request".to_owned())?;
        Ok(current_principal(request))
    }

    fn session_list_row_presentation(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::authorization::SessionListRowPresentation, String> {
        let item = serde_json::from_str::<pioneer_protocol::AuthSessionListItem>(input_json)
            .map_err(|_| "invalid session list row presentation request".to_owned())?;
        Ok(session_list_row(item))
    }

    fn thread_scope_presentation(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::threads::scope::ThreadScopePresentation, String> {
        let request = serde_json::from_str::<ClientThreadScopePresentationRequest>(input_json)
            .map_err(|_| "invalid thread scope presentation request".to_owned())?;
        Ok(thread_scope(request))
    }

    fn thread_create_visibility_plan(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::threads::scope::ThreadCreateVisibilityPlan, String> {
        let request = serde_json::from_str::<ClientThreadCreateVisibilityRequest>(input_json)
            .map_err(|_| "invalid thread create visibility request".to_owned())?;
        Ok(thread_create_visibility(request))
    }

    fn thread_scope_mutation_plan(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::threads::scope::ThreadScopeMutationPlan, String> {
        let request = serde_json::from_str::<ClientThreadScopeMutationPlanRequest>(input_json)
            .map_err(|_| "invalid thread scope mutation plan request".to_owned())?;
        Ok(thread_scope_mutation_plan(request))
    }

    fn member_presentation(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::administration::MemberListRow, String> {
        let request = serde_json::from_str::<ClientMemberPresentationRequest>(input_json)
            .map_err(|_| "invalid member presentation request".to_owned())?;
        Ok(member_presentation(request))
    }

    fn invitation_list_row(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::administration::InvitationListRow, String> {
        let request = serde_json::from_str::<ClientInvitationListRowRequest>(input_json)
            .map_err(|_| "invalid invitation list row request".to_owned())?;
        Ok(invitation_list_row(request))
    }

    fn administration_conflict_refetch(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::administration::AdministrationRefetch>, String> {
        let action = serde_json::from_str::<pioneer_client::administration::AdministrationAction>(
            input_json,
        )
        .map_err(|_| "invalid administration conflict action".to_owned())?;
        Ok(pioneer_client::administration::conflict_refetch(&action))
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

    fn composer_skill_pack_picker(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::skill_selection::ComposerSkillPickerProjection, String>
    {
        let request = serde_json::from_str::<ClientComposerSkillPackPickerRequest>(input_json)
            .map_err(|error| format!("invalid composer skill pack picker request: {error}"))?;
        composer_skill_pack_picker(&self.client_runtime.ws_command_sender(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn composer_skill_selection_toggle(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::skill_selection::ComposerSkillSelectionReduction, String>
    {
        let request = serde_json::from_str::<ClientComposerSkillSelectionToggleRequest>(input_json)
            .map_err(|error| format!("invalid composer skill selection toggle request: {error}"))?;
        Ok(composer_skill_selection_toggle(request))
    }

    fn composer_skill_chips(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::skill_selection::ComposerSkillChip>, String> {
        let request = serde_json::from_str::<ClientComposerSkillChipsRequest>(input_json)
            .map_err(|error| format!("invalid composer skill chips request: {error}"))?;
        Ok(composer_skill_chips(request))
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

    fn turn_message_edit(
        &self,
        input_json: &str,
    ) -> Result<TurnMessageEditResponse, ClientFfiError> {
        let params =
            serde_json::from_str::<TurnMessageEditParams>(input_json).map_err(|error| {
                ClientFfiError::new(
                    format!("invalid Turn message edit params: {error}"),
                    timeline::TURN_MESSAGE_ERROR_INVALID_INPUT,
                )
            })?;
        timeline::turn_message_edit(&self.client_runtime.ws_command_sender(), params)
    }

    fn turn_message_delete(
        &self,
        input_json: &str,
    ) -> Result<TurnMessageDeleteResponse, ClientFfiError> {
        let params =
            serde_json::from_str::<TurnMessageDeleteParams>(input_json).map_err(|error| {
                ClientFfiError::new(
                    format!("invalid Turn message delete params: {error}"),
                    timeline::TURN_MESSAGE_ERROR_INVALID_INPUT,
                )
            })?;
        timeline::turn_message_delete(&self.client_runtime.ws_command_sender(), params)
    }

    fn turn_message_revisions_page(
        &self,
        input_json: &str,
    ) -> Result<TurnMessageRevisionsPageResponse, ClientFfiError> {
        let params = serde_json::from_str::<TurnMessageRevisionsPageParams>(input_json).map_err(
            |error| {
                ClientFfiError::new(
                    format!("invalid Turn message revisions params: {error}"),
                    timeline::TURN_MESSAGE_ERROR_INVALID_INPUT,
                )
            },
        )?;
        timeline::turn_message_revisions_page(&self.client_runtime.ws_command_sender(), params)
    }

    fn message_revision_page_presentation(
        &self,
        input_json: &str,
    ) -> Result<MessageRevisionPagePresentation, String> {
        let response = serde_json::from_str::<TurnMessageRevisionsPageResponse>(input_json)
            .map_err(|error| format!("invalid Turn message revisions page response: {error}"))?;
        Ok(project_message_revision_page(response))
    }

    fn thread_read(&self, input_json: &str) -> Result<ThreadReadResponse, ClientFfiError> {
        let params = serde_json::from_str::<ThreadReadParams>(input_json).map_err(|error| {
            ClientFfiError::new(
                format!("invalid thread read params: {error}"),
                timeline::THREAD_READ_ERROR,
            )
        })?;
        timeline::thread_read(&self.client_runtime.ws_command_sender(), params)
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

    fn turn_work_items_get(
        &self,
        input_json: &str,
    ) -> Result<TurnWorkItemsGetResponse, ClientFfiError> {
        let params =
            serde_json::from_str::<TurnWorkItemsGetParams>(input_json).map_err(|error| {
                ClientFfiError::new(
                    format!("invalid turn work items params: {error}"),
                    timeline::TIMELINE_ERROR_VALIDATION,
                )
            })?;

        let response = turn_work_items_get(&self.client_runtime.ws_command_sender(), params)?;
        self.active_thread
            .apply_turn_work_items_get_response(response.clone())
            .map_err(|error| {
                ClientFfiError::new(format!("{error:#}"), timeline::TIMELINE_ERROR_VALIDATION)
            })?;

        Ok(response)
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

fn parse_empty_auth_request(input_json: &str) -> Result<(), ClientFfiError> {
    let value = serde_json::from_str::<serde_json::Value>(input_json).map_err(|_| {
        ClientFfiError::new("invalid auth request", auth::INVALID_AUTH_REQUEST_CODE)
    })?;
    if value.as_object().is_none_or(|object| !object.is_empty()) {
        return Err(ClientFfiError::new(
            "auth request must be an empty object",
            auth::INVALID_AUTH_REQUEST_CODE,
        ));
    }
    Ok(())
}

const INVALID_ADMINISTRATION_REQUEST_CODE: &str = "invalid_administration_request";

fn parse_normal_params<T>(input_json: &str, operation: &str) -> Result<T, ClientFfiError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_str(input_json).map_err(|_| {
        ClientFfiError::new(
            format!("invalid {operation} request"),
            INVALID_ADMINISTRATION_REQUEST_CODE,
        )
    })
}

fn parse_invitation_commit_request(
    input_json: &str,
) -> Result<ClientInvitationCommitRequest, ClientFfiError> {
    let request =
        serde_json::from_str::<ClientInvitationCommitRequest>(input_json).map_err(|_| {
            ClientFfiError::new(
                "invalid invitation commit request",
                invitation::INVALID_INVITATION_REQUEST_CODE,
            )
        })?;
    if request.commit_id.is_empty() || request.commit_id.len() > 128 {
        return Err(invitation_commit_unavailable());
    }
    Ok(request)
}

fn invitation_commit_unavailable() -> ClientFfiError {
    ClientFfiError::new(
        "invitation commit is unavailable",
        invitation::INVITATION_COMMIT_UNAVAILABLE_CODE,
    )
}

fn invitation_commit_lock_error() -> ClientFfiError {
    ClientFfiError::new(
        "invitation commit state is unavailable",
        ClientFfiError::GENERIC_CODE,
    )
}

fn invitation_commit_capacity_available(current: usize) -> bool {
    current < MAX_OUTSTANDING_INVITATION_COMMITS
}

fn cleanup_untracked_invitation_commit(
    runtime: &tokio::runtime::Runtime,
    client: &pioneer_client::transport::ws::auth_exchange::AuthExchangeClient,
    mut commit: pioneer_client::gateway::invitation::InvitationSessionCommit,
) {
    let Ok(refresh) = commit.take_refresh_for_secure_storage() else {
        return;
    };
    drop(refresh);
    let Ok(cleanup) = commit.secure_storage_failed() else {
        return;
    };
    runtime.block_on(client.cleanup_invitation_session_best_effort(cleanup));
}

fn administration_rpc_error(error: anyhow::Error) -> ClientFfiError {
    let code = pioneer_client::rpc::json_rpc_response_error(&error)
        .and_then(|response| response.machine_code())
        .filter(|code| {
            code.len() <= 64
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        })
        .unwrap_or(ClientFfiError::GENERIC_CODE);
    ClientFfiError::new("Gateway administration request failed", code)
}

fn normal_auth_error(error: anyhow::Error) -> ClientFfiError {
    let message = format!("{error:#}");
    let code = [
        "session_revoked",
        "session_expired",
        "session_compromised",
        "gateway_identity_mismatch",
        "invalid_credential",
        "device_activation_consumed",
        "device_activation_expired",
    ]
    .into_iter()
    .find(|code| message.contains(code))
    .unwrap_or(ClientFfiError::GENERIC_CODE);
    ClientFfiError::new(message, code)
}

fn turn_work_page_merge_mode(anchor: &TimelinePageAnchor) -> WorkPageMergeMode {
    match anchor {
        TimelinePageAnchor::Newest | TimelinePageAnchor::After { .. } => {
            WorkPageMergeMode::MergeAfter
        }
        TimelinePageAnchor::Oldest | TimelinePageAnchor::Before { .. } => {
            WorkPageMergeMode::MergeBefore
        }
        TimelinePageAnchor::Around { .. } => WorkPageMergeMode::Reset,
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
    pioneer_client_ffi_gateway_load_registry_v3,
    gateway_load_registry_v3
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
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_session_lifecycle_reduce,
    gateway_session_lifecycle_reduce
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_device_activation_presentation,
    gateway_device_activation_presentation
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_device_activation_parse,
    gateway_device_activation_parse
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_auth_refresh,
    gateway_auth_refresh
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_auth_device_activate,
    gateway_auth_device_activate
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_auth_session_cleanup,
    gateway_auth_session_cleanup
);
ffi_client_json_typed_method!(pioneer_client_ffi_gateway_auth_me, gateway_auth_me);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_auth_session_list,
    gateway_auth_session_list
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_auth_session_revoke,
    gateway_auth_session_revoke
);
ffi_client_json_typed_method!(pioneer_client_ffi_gateway_auth_logout, gateway_auth_logout);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_auth_device_create,
    gateway_auth_device_create
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_invitation_presentation,
    invitation_presentation
);
ffi_client_json_typed_method!(pioneer_client_ffi_invitation_preview, invitation_preview);
ffi_client_json_typed_method!(pioneer_client_ffi_invitation_accept, invitation_accept);
ffi_client_json_typed_method!(
    pioneer_client_ffi_invitation_commit_take_refresh,
    invitation_commit_take_refresh
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_invitation_commit_secure_storage_committed,
    invitation_commit_secure_storage_committed
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_invitation_commit_registry_committed,
    invitation_commit_registry_committed
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_invitation_commit_secure_storage_failed,
    invitation_commit_secure_storage_failed
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_invitation_commit_registry_failed,
    invitation_commit_registry_failed
);
ffi_client_json_typed_method!(pioneer_client_ffi_invitation_create, invitation_create);
ffi_client_json_typed_method!(pioneer_client_ffi_invitation_list, invitation_list);
ffi_client_json_typed_method!(pioneer_client_ffi_invitation_revoke, invitation_revoke);
ffi_client_json_typed_method!(pioneer_client_ffi_member_list, member_list);
ffi_client_json_typed_method!(pioneer_client_ffi_member_avatar_cache, member_avatar_cache);
ffi_client_json_typed_method!(pioneer_client_ffi_agent_avatar_cache, agent_avatar_cache);
ffi_client_json_typed_method!(pioneer_client_ffi_member_suspend, member_suspend);
ffi_client_json_typed_method!(pioneer_client_ffi_member_restore, member_restore);
ffi_client_json_typed_method!(pioneer_client_ffi_member_remove, member_remove);
ffi_client_json_typed_method!(
    pioneer_client_ffi_member_device_create,
    member_device_create
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_workspace_member_list,
    workspace_member_list
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_workspace_member_add,
    workspace_member_add
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_workspace_member_remove,
    workspace_member_remove
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_thread_participants_list,
    thread_participants_list
);
ffi_client_json_typed_method!(pioneer_client_ffi_thread_update, thread_update);
ffi_client_json_typed_method!(
    pioneer_client_ffi_thread_participant_add,
    thread_participant_add
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_thread_participant_remove,
    thread_participant_remove
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_session_replace_access,
    gateway_session_replace_access
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_settings_get,
    gateway_settings_get
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_settings_update,
    gateway_settings_update
);
ffi_client_json_typed_method!(pioneer_client_ffi_artifact_view_open, artifact_view_open);
ffi_client_json_typed_method!(pioneer_client_ffi_artifact_download, artifact_download);
ffi_client_json_typed_method!(
    pioneer_client_ffi_artifact_download_progress,
    artifact_download_progress
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_artifact_download_cancel,
    artifact_download_cancel
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_composer_turn_mode_options(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "composer_turn_mode_options", |runtime| {
        runtime.composer_turn_mode_options()
    })
}
ffi_client_json_method!(
    pioneer_client_ffi_principal_presentation_capabilities,
    principal_presentation_capabilities
);
ffi_client_json_method!(
    pioneer_client_ffi_current_principal_presentation,
    current_principal_presentation
);
ffi_client_json_method!(
    pioneer_client_ffi_session_list_row_presentation,
    session_list_row_presentation
);
ffi_client_json_method!(
    pioneer_client_ffi_thread_scope_presentation,
    thread_scope_presentation
);
ffi_client_json_method!(
    pioneer_client_ffi_thread_create_visibility_plan,
    thread_create_visibility_plan
);
ffi_client_json_method!(
    pioneer_client_ffi_thread_scope_mutation_plan,
    thread_scope_mutation_plan
);
ffi_client_json_method!(pioneer_client_ffi_member_presentation, member_presentation);
ffi_client_json_method!(pioneer_client_ffi_invitation_list_row, invitation_list_row);
ffi_client_json_method!(
    pioneer_client_ffi_administration_conflict_refetch,
    administration_conflict_refetch
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
    pioneer_client_ffi_composer_skill_pack_picker,
    composer_skill_pack_picker
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_selection_toggle,
    composer_skill_selection_toggle
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_chips,
    composer_skill_chips
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
ffi_client_json_typed_method!(pioneer_client_ffi_turn_message_edit, turn_message_edit);
ffi_client_json_typed_method!(pioneer_client_ffi_turn_message_delete, turn_message_delete);
ffi_client_json_typed_method!(
    pioneer_client_ffi_turn_message_revisions_page,
    turn_message_revisions_page
);
ffi_client_json_method!(
    pioneer_client_ffi_message_revision_page_presentation,
    message_revision_page_presentation
);
ffi_client_json_typed_method!(pioneer_client_ffi_thread_read, thread_read);
ffi_client_json_typed_method!(pioneer_client_ffi_turn_work_page, turn_work_page);
ffi_client_json_typed_method!(pioneer_client_ffi_turn_work_items_get, turn_work_items_get);
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
        // `CString::into_raw`; this reclaims that allocation. The buffer can
        // contain direct-return auth credentials, so overwrite its contents
        // before releasing the allocation.
        unsafe {
            let mut bytes = CString::from_raw(value).into_bytes();
            bytes.fill(0);
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

unsafe fn read_c_string(ptr: *const c_char) -> Result<Zeroizing<String>, String> {
    if ptr.is_null() {
        return Err("received null string pointer".to_owned());
    }

    // SAFETY: callers pass a valid, NUL-terminated string pointer owned by the
    // native bridge for the duration of this call.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|value| Zeroizing::new(value.to_owned()))
        .map_err(|error| format!("received non-utf8 string: {error}"))
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
            diagnostics.record_error(operation_name, error.message.clone(), error.code.as_str());
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
    use pioneer_client::state::client_state::GatewayConnectionState;

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
    fn outstanding_invitation_commit_ownership_is_bounded() {
        assert!(invitation_commit_capacity_available(
            MAX_OUTSTANDING_INVITATION_COMMITS - 1
        ));
        assert!(!invitation_commit_capacity_available(
            MAX_OUTSTANDING_INVITATION_COMMITS
        ));
    }

    #[test]
    fn malformed_normal_administration_request_has_a_domain_neutral_code() {
        let error = parse_normal_params::<pioneer_protocol::MemberListParams>(
            r#"{"unknown":true}"#,
            "member list",
        )
        .expect_err("unknown administration fields must be rejected");

        assert_eq!(error.code, INVALID_ADMINISTRATION_REQUEST_CODE);
        assert_eq!(error.message, "invalid member list request");
    }

    #[test]
    fn ffi_response_is_tagged_json() {
        let response = to_json_response::<serde_json::Value>(Ok(serde_json::json!({"value": 1})));
        let value: serde_json::Value = decode_response(response.as_str());

        assert_eq!(value["value"], 1);
    }

    #[test]
    fn revision_page_presentation_crosses_the_mobile_json_boundary_without_file_inputs() {
        let runtime = ClientFfiRuntime::default();
        let presentation = runtime
            .message_revision_page_presentation(
                serde_json::json!({
                    "workspace_id": "workspace-a",
                    "thread_id": "thread-a",
                    "turn_id": "turn-a",
                    "revisions": [{
                        "turn_id": "turn-a",
                        "revision": 2,
                        "change_kind": "edit",
                        "changed_by": { "kind": "system" },
                        "created_at": 1_700_000_000_000_i64,
                        "input": [
                            { "type": "text", "text": "updated" },
                            { "type": "file", "url": "private://must-not-cross" }
                        ],
                        "mentions": []
                    }],
                    "next_cursor": null
                })
                .to_string()
                .as_str(),
            )
            .expect("revision presentation");

        assert_eq!(presentation.revisions[0].text.as_deref(), Some("updated"));
        assert!(
            !serde_json::to_string(&presentation)
                .expect("serialize")
                .contains("private://")
        );
    }

    #[test]
    fn auth_ffi_error_codes_are_machine_readable_and_diagnostics_are_redacted() {
        let diagnostics = ClientFfiDiagnostics::default();
        let secret = "prf_auth-response-must-not-enter-diagnostics";
        let response = ffi_typed_response_json_with_diagnostics::<(), _>(
            &diagnostics,
            "gateway_auth_refresh",
            || {
                Err(ClientFfiError::new(
                    format!("refresh_token={secret}"),
                    "session_compromised",
                ))
            },
        );
        let response: serde_json::Value =
            serde_json::from_str(response.as_str()).expect("typed auth response");
        assert_eq!(response["code"], "session_compromised");

        let events = diagnostics.drain().expect("diagnostics drain");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].code.as_deref(), Some("session_compromised"));
        assert!(!events[0].message.contains(secret));
        assert!(events[0].message.contains("[redacted]"));
    }

    #[test]
    fn invalid_auth_ffi_input_never_echoes_the_supplied_credential() {
        let runtime = ClientFfiRuntime::default();
        runtime.initialize("{}").expect("initialize client");
        let secret = "prf_input-must-not-be-echoed";
        let error = runtime
            .gateway_auth_refresh(
                serde_json::json!({
                    "gateway_base_url": "http://localhost:17878/",
                    "credential": secret,
                    "params": {"unexpected": true}
                })
                .to_string()
                .as_str(),
            )
            .expect_err("invalid refresh request");

        assert_eq!(error.code, auth::INVALID_AUTH_REQUEST_CODE);
        assert!(!error.message.contains(secret));
    }

    #[test]
    fn no_stored_session_releases_the_endpoint_lifecycle_entry() {
        let runtime = ClientFfiRuntime::default();
        runtime.initialize("{}").expect("initialize client");
        let endpoint_id = "remote-lifecycle-test";
        runtime
            .gateway_session_lifecycle_reduce(
                serde_json::json!({
                    "endpoint_id": endpoint_id,
                    "event": {
                        "kind": "stored_session_loaded",
                        "data": {
                            "gateway_id": "G00000000000000000001",
                            "device_id": "D00000000000000000001",
                            "session_id": "S00000000000000000001",
                            "refresh_generation": 0,
                            "refresh_expires_at_unix": 1_900_000_000_u64
                        }
                    }
                })
                .to_string()
                .as_str(),
            )
            .expect("store lifecycle");
        assert_eq!(
            runtime
                .gateway_session_lifecycles
                .lock()
                .expect("lifecycle lock")
                .len(),
            1
        );

        runtime
            .gateway_session_lifecycle_reduce(
                serde_json::json!({
                    "endpoint_id": endpoint_id,
                    "event": {"kind": "no_stored_session"}
                })
                .to_string()
                .as_str(),
            )
            .expect("release lifecycle");
        assert!(
            runtime
                .gateway_session_lifecycles
                .lock()
                .expect("lifecycle lock")
                .is_empty()
        );
    }

    #[test]
    fn turn_work_boundary_anchors_preserve_loaded_native_ranges() {
        assert_eq!(
            turn_work_page_merge_mode(&TimelinePageAnchor::Newest),
            WorkPageMergeMode::MergeAfter
        );
        assert_eq!(
            turn_work_page_merge_mode(&TimelinePageAnchor::Oldest),
            WorkPageMergeMode::MergeBefore
        );
        assert_eq!(
            turn_work_page_merge_mode(&TimelinePageAnchor::Around {
                cursor: pioneer_protocol::TimelineCursor {
                    value: "cursor".to_owned(),
                },
            }),
            WorkPageMergeMode::Reset
        );
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
            .gateway_validate_remote(r#"{"gateway_base_url":"127.0.0.1:23000","timeout_ms":0}"#)
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
                        "version": 3,
                        "active_gateway_id": null,
                        "local": {
                            "id": "local",
                            "name": "Local",
                            "gateway_base_url": "http://127.0.0.1:17878/",
                            "kind": "local",
                            "workspace_id": null,
                            "service_name": null
                        },
                        "remotes": []
                    },
                    "name": " Remote ",
                    "gateway_base_url": "http://127.0.0.1:23000/",
                    "new_endpoint_id": "remote-one",
                    "default_remote_name": "Remote 1"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan add remote");

        assert_eq!(result.endpoint.id, "remote-one");
        assert!(result.endpoint.session_ref.is_none());
        assert!(result.previous_endpoint.is_none());
    }

    #[test]
    fn gateway_add_and_activate_remote_registry_plan_returns_shared_next_registry() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_add_and_activate_remote_registry(
                serde_json::json!({
                    "registry": {
                        "version": 3,
                        "active_gateway_id": null,
                        "remotes": []
                    },
                    "name": " Remote ",
                    "gateway_base_url": "http://127.0.0.1:23000/",
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
        assert!(result.endpoint.session_ref.is_none());
    }

    #[test]
    fn gateway_activate_registry_plan_returns_shared_next_registry() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_activate_registry(
                serde_json::json!({
                    "registry": {
                        "version": 3,
                        "active_gateway_id": null,
                        "remotes": [{
                            "id": "remote-one",
                            "name": "Remote",
                            "gateway_base_url": "http://127.0.0.1:23000/",
                            "kind": "remote",
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
    fn gateway_update_remote_registry_plan_updates_profile_only() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_update_remote_registry(
                serde_json::json!({
                    "registry": {
                        "version": 3,
                        "active_gateway_id": "remote-one",
                        "remotes": [{
                            "id": "remote-one",
                            "name": "Remote",
                            "gateway_base_url": "http://127.0.0.1:23000/",
                            "kind": "remote",
                            "workspace_id": null,
                            "service_name": null
                        }]
                    },
                    "gateway_id": "remote-one",
                    "name": "Renamed",
                    "gateway_base_url": "http://127.0.0.1:24000/",
                    "default_remote_name": "Remote 1"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan update remote registry");

        assert_eq!(result.endpoint.name, "Renamed");
        assert_eq!(
            result.endpoint.gateway_base_url.as_str(),
            "http://127.0.0.1:24000/"
        );
        assert_eq!(
            result.previous_endpoint.gateway_base_url.as_str(),
            "http://127.0.0.1:23000/"
        );
    }

    #[test]
    fn gateway_delete_remote_registry_plan_returns_fallback() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_delete_remote_registry(
                serde_json::json!({
                    "registry": {
                        "version": 3,
                        "active_gateway_id": "remote-one",
                        "remotes": [
                            {
                                "id": "remote-one",
                                "name": "One",
                                "gateway_base_url": "http://127.0.0.1:23000/",
                                "kind": "remote",
                                "workspace_id": null,
                                "service_name": null
                            },
                            {
                                "id": "remote-two",
                                "name": "Two",
                                "gateway_base_url": "http://127.0.0.1:24000/",
                                "kind": "remote",
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
                        "version": 3,
                        "active_gateway_id": "remote-one",
                        "remotes": [{
                            "id": "remote-one",
                            "name": "Remote",
                            "gateway_base_url": "http://127.0.0.1:23000/",
                            "kind": "remote",
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

    #[test]
    fn gateway_connection_event_starts_a_new_authorization_revision_epoch() {
        assert!(contains_gateway_connection_epoch_boundary(&[
            ClientEvent::GatewayConnectionChanged(contracts::ClientGatewayConnectionEvent {
                connection_state: GatewayConnectionState::Connected,
                gateway_error: None,
            }),
        ]));
        assert!(!contains_gateway_connection_epoch_boundary(&[
            ClientEvent::Error(contracts::ClientErrorEvent {
                message: "unrelated".to_owned(),
                code: None,
            }),
        ]));
    }

    #[test]
    fn authorization_and_directory_events_invalidate_private_avatar_cache() {
        let access_changed =
            ClientEvent::GatewayNotification(pioneer_protocol::GatewayNotification::AccessChanged(
                pioneer_protocol::AccessChangedNotification {
                    authorization_revision: 7,
                    workspace_id: "workspace-one".to_owned(),
                    thread_id: None,
                    change: pioneer_protocol::AccessChangeKind::WorkspaceMembership,
                },
            ));
        let member_changed =
            ClientEvent::GatewayNotification(pioneer_protocol::GatewayNotification::MemberChanged(
                pioneer_protocol::MemberChangedNotification {
                    revision: 8,
                    principal_id: pioneer_protocol::PrincipalId::new("P00000000000000000001")
                        .unwrap(),
                },
            ));

        assert!(contains_avatar_authorization_boundary(&[
            access_changed,
            member_changed,
        ]));
        assert!(!contains_avatar_authorization_boundary(&[
            ClientEvent::Error(contracts::ClientErrorEvent {
                message: "unrelated".to_owned(),
                code: None,
            }),
        ]));
    }
}
