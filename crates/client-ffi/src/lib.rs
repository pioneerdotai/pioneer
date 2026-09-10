//! C ABI boundary for native Pioneer client shells.
//!
//! This crate intentionally owns ABI/runtime glue and shell-boundary DTOs.
//! Client domain logic remains in `pioneer-client`.

mod active_thread;
mod administration_activation;
mod artifacts;
mod auth;
mod avatars;
mod client_binding;
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
mod telemetry;
mod thread_files;
mod threads;
mod workspaces;

use active_thread::{
    ClientActiveThreadClearResult, ClientActiveThreadOpenByIdRequest,
    ClientActiveThreadOpenRequest, ClientActiveThreadSendTextRequest,
    ClientActiveThreadSendTextResult, ClientEnsureWorkspaceDraftRequest,
    ClientPrepareVoiceComposerSnapshotRequest,
};
use artifacts::{
    ClientArtifactDownloadRequest, ClientArtifactDownloadResult, ClientArtifactTargetRequest,
    ClientArtifactViewOpenResult,
};
use avatars::{
    ClientAgentAvatarCacheRequest, ClientAgentAvatarCacheResult, ClientFfiAvatarCache,
    ClientMemberAvatarCacheRequest, ClientMemberAvatarCacheResult,
};
use composer::{
    ClientComposerAttachmentFromPathRequest, ClientComposerCapabilityMenuVisibilityRequest,
    ClientComposerCapabilityTargetRequest, ClientComposerFilterMcpRowsRequest,
    ClientComposerFilterMcpRowsResult, ClientComposerSkillRowsForTargetRequest,
    ClientComposerSubmissionPlanRequest, composer_attachment_from_path_request,
    composer_capability_menu, composer_capability_target, composer_skill_rows_for_target,
    composer_submission_plan, filter_mcp_picker_rows,
};
use diagnostics::{ClientDiagnosticEvent, ClientFfiDiagnostics};
use gateway::{
    LoadGatewayRegistryRequest, LoadGatewayRegistryResult, load_gateway_registry_request,
};
use invitation::{ClientInvitationPresentationRequest, ClientInvitationPresentationResult};
use pending_requests::{
    ClientPendingRequestPresentationRequest, ClientPendingRequestPresentationResult,
    pending_request_presentation_for_bridge,
};

use pioneer_client::{
    core::{ClientCore, ClientScope, ClientSubscription},
    workspaces::{
        actions::WorkspaceBootstrapSuccessReduction, bootstrap::WorkspaceBootstrapRequest,
    },
};

use pioneer_protocol::{
    VoiceAudioFormat, VoiceSessionCancelResponse, VoiceSessionFinalizeResponse,
    VoiceSessionStartResponse,
};
use presentation::{
    ClientArtifactPresentationPolicyRequest, ClientCurrentPrincipalPresentationRequest,
    ClientMemberPresentationRequest, ClientThreadCreateVisibilityRequest,
    artifact_presentation_policy, current_principal, member_presentation, session_list_row,
    thread_create_visibility,
};
pub use presentation::{principal_capabilities, thread_capabilities};
use serde::{Deserialize, Serialize};
use skills::{
    ClientComposerSkillChipsRequest, ClientComposerSkillPackPickerRequest, composer_skill_chips,
    composer_skill_pack_picker,
};
use std::{
    collections::HashMap,
    ffi::{CStr, CString, c_char},
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::{Arc, Mutex},
};
use thread_files::{ClientThreadFileViewOpenRequest, ClientThreadFileViewOpenResult};
use threads::{
    ClientThreadTreeLevel, ThreadTreeLevelRequest, ThreadTreeRefreshRequest,
    client_thread_tree_level,
};
use workspaces::{
    WorkspaceCreateRequest, WorkspaceCreateResult, WorkspaceRenameRequest, WorkspaceRenameResult,
    WorkspaceSwitchRequest, WorkspaceSwitchResult,
};
use zeroize::Zeroizing;

const FFI_VERSION: &str = env!("CARGO_PKG_VERSION");
#[cfg(test)]
const MAX_OUTSTANDING_INVITATION_COMMITS: usize =
    pioneer_client::gateway::invitation_commits::MAX_OUTSTANDING_INVITATION_COMMITS;

pub struct PioneerClientFfi {
    runtime: ClientFfiRuntime,
}

struct ClientFfiRuntime {
    config: Mutex<Option<ClientFfiConfig>>,
    core: Arc<ClientCore>,
    client_subscriptions: Mutex<HashMap<ClientScope, ClientSubscription>>,
    observed_scopes: Mutex<std::collections::HashSet<ClientScope>>,

    diagnostics: ClientFfiDiagnostics,
    avatar_cache: ClientFfiAvatarCache,
}

fn mobile_process_core() -> Arc<ClientCore> {
    #[cfg(not(test))]
    {
        static CORE: std::sync::OnceLock<Arc<ClientCore>> = std::sync::OnceLock::new();
        CORE.get_or_init(ClientCore::shared).clone()
    }
    #[cfg(test)]
    ClientCore::shared()
}

impl Default for ClientFfiRuntime {
    fn default() -> Self {
        let core = mobile_process_core();
        Self {
            core,
            config: Default::default(),
            client_subscriptions: Default::default(),
            observed_scopes: Default::default(),

            diagnostics: Default::default(),
            avatar_cache: Default::default(),
        }
    }
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
    pub boundary_version: u32,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientFfiGatewayDisconnectResult {
    pub disconnected: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientFfiVoiceAudioChunkParams {
    pub operation: pioneer_client::composer::store::ComposerOperationIdentity,
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
                .map_err(|_| "invalid client ffi config".to_owned())?
        };

        let _client_contract_count = pioneer_client::schema::public_client_schema_contracts().len();
        *self
            .config
            .lock()
            .map_err(|_| "client ffi config lock is poisoned".to_owned())? = Some(config);

        Ok(ClientFfiInitializeResult {
            initialized: true,
            boundary_version: 2,
        })
    }

    fn mobile_startup_record(
        &self,
        input_json: &str,
    ) -> Result<telemetry::ClientMobileStartupRecordResult, String> {
        self.require_initialized().map_err(|error| error.message)?;
        telemetry::record_mobile_startup(input_json)
    }

    fn client_intent_dispatch(
        &self,
        input_json: &str,
    ) -> Result<client_binding::ClientTransitionDto, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request = serde_json::from_str::<client_binding::ClientIntentDispatchDto>(input_json)
            .map_err(|_| "invalid Client intent request".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        let transition = self.core.dispatch(request.intent);
        Ok(client_binding::transition_dto(transition))
    }

    fn observe_client_scope(&self, scope: ClientScope) -> Result<(), String> {
        let mut scopes = self
            .observed_scopes
            .lock()
            .map_err(|_| "Client scope registry poisoned".to_owned())?;
        if !scopes.contains(&scope) && scopes.len() >= 128 {
            return Err("Client scope capacity exceeded".into());
        }
        scopes.insert(scope);
        Ok(())
    }
    fn client_scope_acquire(&self, input_json: &str) -> Result<bool, String> {
        let request: client_binding::ClientScopeLeaseRequestDto = serde_json::from_str(input_json)
            .map_err(|_| "invalid Client scope lease".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        self.ensure_client_subscription(request.scope)?;
        Ok(true)
    }
    fn client_scope_release(&self, input_json: &str) -> Result<bool, String> {
        let request: client_binding::ClientScopeLeaseRequestDto = serde_json::from_str(input_json)
            .map_err(|_| "invalid Client scope lease".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        self.observed_scopes
            .lock()
            .map_err(|_| "Client scope registry poisoned".to_owned())?
            .remove(&request.scope);
        let removed = self
            .client_subscriptions
            .lock()
            .map_err(|_| "Client subscription registry poisoned".to_owned())?
            .remove(&request.scope);
        let released = removed.is_some();
        drop(removed);
        Ok(released)
    }

    fn client_scoped_snapshot(
        &self,
        input_json: &str,
    ) -> Result<Option<client_binding::ClientScopedSnapshotDto>, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request =
            serde_json::from_str::<client_binding::ClientScopedSnapshotRequestDto>(input_json)
                .map_err(|_| "invalid Client scoped snapshot request".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        self.observe_client_scope(request.scope.clone())?;
        Ok(self
            .core
            .snapshot_if_newer(&request.scope, request.after_revision)
            .map(client_binding::snapshot_dto))
    }

    fn client_change_batch(
        &self,
        input_json: &str,
    ) -> Result<client_binding::ClientChangeBatchDto, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request =
            serde_json::from_str::<client_binding::ClientChangeBatchRequestDto>(input_json)
                .map_err(|_| "invalid Client change batch request".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        if request.maximum_items == 0 || request.maximum_items > 256 {
            return Err("Client change batch maximum_items must be between 1 and 256".to_owned());
        }
        self.ensure_client_subscription(request.scope.clone())?;

        let subscriptions = self
            .client_subscriptions
            .lock()
            .map_err(|_| "Client subscription registry lock is poisoned".to_owned())?;
        let subscription = subscriptions
            .get(&request.scope)
            .ok_or_else(|| "Client scope subscription is unavailable".to_owned())?;
        let mut changes = Vec::new();
        for _ in 0..request.maximum_items {
            let Some(event) = subscription.try_next() else {
                break;
            };
            changes.push(client_binding::change_dto(event));
        }
        Ok(client_binding::ClientChangeBatchDto {
            schema_version: client_binding::CLIENT_BINDING_SCHEMA_VERSION,
            changes,
        })
    }

    fn client_wait_publications(
        &self,
        input_json: &str,
    ) -> Result<client_binding::ClientProcessChangeBatchDto, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request: client_binding::ClientPublicationWaitRequestDto =
            serde_json::from_str(input_json).map_err(|_| "invalid publication wait".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        let batch = self.core.wait_for_publications(request.after_sequence);
        let retained = self
            .observed_scopes
            .lock()
            .map_err(|_| "Client subscription registry poisoned")?;
        Ok(client_binding::ClientProcessChangeBatchDto {
            closed: batch.closed,
            effects: batch.effects,
            schema_version: client_binding::CLIENT_BINDING_SCHEMA_VERSION,
            sequence: batch.sequence,
            resnapshot: batch.resnapshot,
            changes: batch
                .changes
                .iter()
                .map(|change| client_binding::ClientProcessChangeSetDto {
                    timeline_changes: change.timeline_changes().iter().filter(|delta| retained.contains(&ClientScope::Timeline { thread_id: delta.thread_id.clone() })).cloned().collect(),
                    sequence: change.sequence(),
                    predecessor: change.predecessor(),
                    snapshots: change
                        .publications()
                        .iter()
                        .filter(|publication| retained.contains(publication.scope()))
                        .cloned()
                        .map(|publication| {
                            let incremental = matches!(publication.scope(), pioneer_client::core::ClientScope::Timeline { thread_id }
                                if change.timeline_changes().iter().any(|delta| &delta.thread_id == thread_id));
                            client_binding::publication_dto(publication, incremental)
                        })
                        .collect(),
                })
                .collect(),
        })
    }

    fn client_shutdown(&self, input_json: &str) -> Result<bool, String> {
        if input_json != "{}" {
            return Err("invalid Client shutdown request".into());
        }
        self.core.shutdown();
        self.observed_scopes
            .lock()
            .map_err(|_| "Client scope registry poisoned")?
            .clear();
        self.client_subscriptions
            .lock()
            .map_err(|_| "Client subscription registry poisoned")?
            .clear();
        Ok(true)
    }

    fn client_effect_complete(
        &self,
        input_json: &str,
    ) -> Result<client_binding::ClientTransitionDto, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request = serde_json::from_str::<client_binding::ClientEffectCompletionDto>(input_json)
            .map_err(|_| "invalid Client effect completion".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        Ok(client_binding::transition_dto(
            self.core.complete_effect(request.completion),
        ))
    }

    fn client_effect_cancel(
        &self,
        input_json: &str,
    ) -> Result<client_binding::ClientTransitionDto, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request =
            serde_json::from_str::<client_binding::ClientEffectCancellationDto>(input_json)
                .map_err(|_| "invalid Client effect cancellation".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        Ok(client_binding::transition_dto(
            self.core.cancel_effect(request.cancellation),
        ))
    }

    fn client_sequence_gap_resnapshot(
        &self,
        input_json: &str,
    ) -> Result<Option<client_binding::ClientScopedSnapshotDto>, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request =
            serde_json::from_str::<client_binding::ClientSequenceGapResnapshotDto>(input_json)
                .map_err(|_| "invalid Client sequence-gap resnapshot".to_owned())?;
        client_binding::validate_schema_version(request.schema_version)?;
        self.observe_client_scope(request.scope.clone())?;
        Ok(self
            .core
            .snapshot(&request.scope)
            .map(client_binding::snapshot_dto))
    }

    fn ensure_client_subscription(&self, scope: ClientScope) -> Result<(), String> {
        self.require_initialized().map_err(|error| error.message)?;
        if self.core.is_stopped() {
            return Err("Client is closed".into());
        }
        self.observe_client_scope(scope.clone())?;
        let mut subscriptions = self
            .client_subscriptions
            .lock()
            .map_err(|_| "Client subscription registry lock is poisoned".to_owned())?;
        if subscriptions.contains_key(&scope) {
            return Ok(());
        }
        if subscriptions.len() >= 256 {
            return Err("Client scope capacity exceeded".into());
        }
        let capacity = NonZeroUsize::new(64).expect("fixed Client queue capacity is non-zero");
        let subscription = self.core.subscribe(scope.clone(), capacity);
        subscriptions.insert(scope, subscription);
        Ok(())
    }

    fn gateway_load_registry_v3(
        &self,
        input_json: &str,
    ) -> Result<LoadGatewayRegistryResult, String> {
        let request = serde_json::from_str::<LoadGatewayRegistryRequest>(input_json)
            .map_err(|_| "invalid Gateway registry load request".to_owned())?;
        load_gateway_registry_request(request).map_err(|error| error.to_string())
    }

    fn gateway_session_validate(
        &self,
        input_json: &str,
    ) -> Result<auth::ClientGatewaySessionValidationResult, String> {
        let request = serde_json::from_str(input_json)
            .map_err(|_| "invalid Gateway session validation request".to_owned())?;
        Ok(auth::validate_gateway_session(request))
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
        if let auth::ClientDeviceActivationPresentationRequest::Current { generation } = request {
            let presentation = self
                .core
                .device_activation_presentation(generation)
                .ok_or_else(|| {
                    ClientFfiError::new(
                        "activation presentation is no longer available",
                        "activation_stale",
                    )
                })?;
            return auth::ClientDeviceActivationPresentationResult::from_presentation(presentation)
                .map_err(|message| ClientFfiError::new(message, auth::INVALID_AUTH_REQUEST_CODE));
        }
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

    fn gateway_auth_me(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthMeResponse, ClientFfiError> {
        parse_empty_auth_request(input_json)?;
        self.require_initialized_and_connected()?;
        self.core.refresh_current_auth().map_err(normal_auth_error)
    }

    fn gateway_authorization_capabilities(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::AuthorizationCapabilitySnapshot, ClientFfiError> {
        let params =
            serde_json::from_str::<pioneer_protocol::AuthorizationCapabilitiesParams>(input_json)
                .map_err(|_| {
                ClientFfiError::new(
                    "invalid authorization capabilities request",
                    "invalid_capability_scope",
                )
            })?;
        self.require_initialized_and_connected()?;
        self.core
            .refresh_identity_authorization(params)
            .map(|(_, snapshot)| snapshot)
            .map_err(|(_, error)| normal_auth_error(error))
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

    fn invitation_create(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::InvitationCreateResponse, ClientFfiError> {
        if let Ok(request) = serde_json::from_str::<
            administration_activation::AdministrationActivationRequest,
        >(input_json)
        {
            request.validate()?;
            self.require_initialized_and_connected()?;
            let operation = self.core.take_administration_activation_operation(request.generation,
                pioneer_client::administration::operations::AdministrationActivationKind::Invitation).map_err(administration_rpc_error)?;
            return match self.core.execute_prepared_administration_command(operation).map_err(administration_rpc_error)? {
                pioneer_client::administration::operations::AdministrationCompletion::InvitationCreated(response) => Ok(response),
                _ => Err(ClientFfiError::new("invalid administration completion", "administration_completion_mismatch")),
            };
        }
        let params = parse_normal_params(input_json, "invitation create")?;
        self.require_initialized_and_connected()?;
        match self.core.execute_administration_command(
            pioneer_client::administration::operations::AdministrationCommand::CreateInvitation(params),
        ).map_err(administration_rpc_error)? {
            pioneer_client::administration::operations::AdministrationCompletion::InvitationCreated(response) => Ok(response),
            _ => Err(ClientFfiError::new("invalid administration completion", "administration_completion_mismatch")),
        }
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
            &self.core,
            &self.core.transport_runtime().ws_command_sender(),
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
            &self.core,
            &self.core.transport_runtime().ws_command_sender(),
            self.native_cache_runtime_home()?,
            request,
        )
    }

    fn member_device_create(
        &self,
        input_json: &str,
    ) -> Result<pioneer_protocol::MemberDeviceCreateResponse, ClientFfiError> {
        if let Ok(request) = serde_json::from_str::<
            administration_activation::AdministrationActivationRequest,
        >(input_json)
        {
            request.validate()?;
            self.require_initialized_and_connected()?;
            let operation = self.core.take_administration_activation_operation(request.generation,
                pioneer_client::administration::operations::AdministrationActivationKind::RecoveryDevice).map_err(administration_rpc_error)?;
            return match self.core.execute_prepared_administration_command(operation).map_err(administration_rpc_error)? {
                pioneer_client::administration::operations::AdministrationCompletion::RecoveryDeviceCreated(response) => Ok(response),
                _ => Err(ClientFfiError::new("invalid administration completion", "administration_completion_mismatch")),
            };
        }
        let params = parse_normal_params(input_json, "member device create")?;
        self.require_initialized_and_connected()?;
        match self.core.execute_administration_command(
            pioneer_client::administration::operations::AdministrationCommand::CreateRecoveryDevice(params),
        ).map_err(administration_rpc_error)? {
            pioneer_client::administration::operations::AdministrationCompletion::RecoveryDeviceCreated(response) => Ok(response),
            _ => Err(ClientFfiError::new("invalid administration completion", "administration_completion_mismatch")),
        }
    }

    fn gateway_transport_reserve(&self, input_json: &str) -> Result<u64, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request: client_binding::ClientTransportReserveRequestDto =
            serde_json::from_str(input_json).map_err(|error| error.to_string())?;
        client_binding::validate_schema_version(request.schema_version)?;
        self.core
            .reserve_gateway_transport(request.exclusive)
            .map_err(|error| error.to_string())
    }

    fn gateway_transport_wait(&self, input_json: &str) -> Result<bool, String> {
        self.require_initialized().map_err(|error| error.message)?;
        let request: client_binding::ClientTransportLeaseRequestDto =
            serde_json::from_str(input_json).map_err(|error| error.to_string())?;
        client_binding::validate_schema_version(request.schema_version)?;
        Ok(self.core.wait_gateway_transport(request.lease_id))
    }

    fn gateway_transport_release(&self, input_json: &str) -> Result<bool, String> {
        let request: client_binding::ClientTransportLeaseRequestDto =
            serde_json::from_str(input_json).map_err(|error| error.to_string())?;
        client_binding::validate_schema_version(request.schema_version)?;
        Ok(self.core.release_gateway_transport(request.lease_id))
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
        artifacts::open_artifact_view(&self.core, request)
    }

    fn thread_file_view_open(
        &self,
        input_json: &str,
    ) -> Result<ClientThreadFileViewOpenResult, ClientFfiError> {
        self.require_initialized_and_connected()?;
        let request =
            serde_json::from_str::<ClientThreadFileViewOpenRequest>(input_json).map_err(|_| {
                ClientFfiError::new(
                    "invalid workspace file view request",
                    thread_files::INVALID_THREAD_FILE_ACTION_CODE,
                )
            })?;
        thread_files::open_thread_file_view(
            &self.core.transport_runtime().ws_command_sender(),
            request,
        )
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
        artifacts::download_artifact(&self.core, runtime_home, request)
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

    fn require_initialized_and_connected(&self) -> Result<u64, ClientFfiError> {
        self.require_initialized()?;

        self.core.current_auth_ticket().1.ok_or_else(|| {
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
            .map_err(|_| "invalid workspace bootstrap request".to_owned())?;

        self.core
            .bootstrap_workspace_catalog(request.persisted_workspace_id)
            .map_err(|error| error.to_string())
    }

    fn workspace_switch(&self, input_json: &str) -> Result<WorkspaceSwitchResult, String> {
        let request = serde_json::from_str::<WorkspaceSwitchRequest>(input_json)
            .map_err(|_| "invalid workspace switch request".to_owned())?;

        self.core
            .switch_workspace(request.workspace_id)
            .map_err(|error| format!("{error:#}"))
    }

    fn workspace_create(&self, input_json: &str) -> Result<WorkspaceCreateResult, String> {
        let request = serde_json::from_str::<WorkspaceCreateRequest>(input_json)
            .map_err(|_| "invalid workspace create request".to_owned())?;

        self.core
            .create_and_select_workspace(request.name)
            .map_err(|error| format!("{error:#}"))
    }

    fn workspace_rename(&self, input_json: &str) -> Result<WorkspaceRenameResult, String> {
        let request = serde_json::from_str::<WorkspaceRenameRequest>(input_json)
            .map_err(|_| "invalid workspace rename request".to_owned())?;

        self.core
            .rename_workspace(request.workspace_id, request.name)
            .map_err(|error| format!("{error:#}"))
    }

    fn composer_voice_capture_plan(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::store::ComposerOperationPlan, String> {
        let identity = serde_json::from_str::<
            pioneer_client::composer::store::ComposerOperationIdentity,
        >(input_json)
        .map_err(|_| "invalid voice operation identity".to_owned())?;
        self.core
            .prepare_composer_voice_capture(identity)
            .map_err(|error| format!("{error:#}"))
    }
    fn voice_session_start(&self, input_json: &str) -> Result<VoiceSessionStartResponse, String> {
        let request = serde_json::from_str::<
            pioneer_client::composer::voice::ComposerVoiceStartRequest,
        >(input_json)
        .map_err(|_| "invalid voice session start params".to_owned())?;
        self.core
            .start_composer_voice_session(request)
            .map_err(|error| format!("{error:#}"))
    }

    fn voice_audio_chunk(
        &self,
        input_json: &str,
        pcm_chunk: &[u8],
    ) -> Result<ClientFfiVoiceAudioChunkResult, String> {
        let params = serde_json::from_str::<ClientFfiVoiceAudioChunkParams>(input_json)
            .map_err(|_| "invalid voice audio chunk params".to_owned())?;

        self.core
            .send_composer_voice_audio_chunk(
                &params.operation,
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
        let request = serde_json::from_str::<
            pioneer_client::composer::voice::ComposerVoiceFinalizeRequest,
        >(input_json)
        .map_err(|_| "invalid voice session finalize params".to_owned())?;
        self.core
            .finalize_composer_voice_session(request)
            .map_err(|error| format!("{error:#}"))
    }

    fn voice_session_cancel(&self, input_json: &str) -> Result<VoiceSessionCancelResponse, String> {
        let request = serde_json::from_str::<
            pioneer_client::composer::voice::ComposerVoiceCancelRequest,
        >(input_json)
        .map_err(|_| "invalid voice session cancel params".to_owned())?;
        self.core
            .cancel_composer_voice_session(request)
            .map_err(|error| format!("{error:#}"))
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

    fn principal_presentation_capabilities(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::authorization::PrincipalPresentationCapabilities, String> {
        let snapshot =
            serde_json::from_str::<pioneer_protocol::AuthorizationCapabilitySnapshot>(input_json)
                .map_err(|_| "invalid capability snapshot".to_owned())?;
        Ok(principal_capabilities(snapshot))
    }

    fn artifact_presentation_policy(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::artifacts::presentation::ArtifactPresentationPolicy, String> {
        let request = serde_json::from_str::<ClientArtifactPresentationPolicyRequest>(input_json)
            .map_err(|_| "invalid artifact presentation request".to_owned())?;
        Ok(artifact_presentation_policy(request))
    }

    fn current_principal_presentation(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::authorization::CurrentPrincipalPresentation, String> {
        let request = serde_json::from_str::<ClientCurrentPrincipalPresentationRequest>(input_json)
            .map_err(|_| "invalid current principal presentation request".to_owned())?;
        current_principal(request)
    }

    fn session_list_row_presentation(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::authorization::SessionListRowPresentation, String> {
        let item = serde_json::from_str::<pioneer_protocol::AuthSessionListItem>(input_json)
            .map_err(|_| "invalid session list row presentation request".to_owned())?;
        Ok(session_list_row(item))
    }

    fn thread_create_visibility_plan(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::threads::scope::ThreadCreateVisibilityPlan, String> {
        let request = serde_json::from_str::<ClientThreadCreateVisibilityRequest>(input_json)
            .map_err(|_| "invalid thread create visibility request".to_owned())?;
        Ok(thread_create_visibility(request))
    }

    fn member_presentation(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::administration::MemberListRow, String> {
        let request = serde_json::from_str::<ClientMemberPresentationRequest>(input_json)
            .map_err(|_| "invalid member presentation request".to_owned())?;
        Ok(member_presentation(request))
    }

    fn composer_attachment_from_path(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::attachments::ComposerAttachment, String> {
        let request = serde_json::from_str::<ClientComposerAttachmentFromPathRequest>(input_json)
            .map_err(|_| "invalid composer attachment request".to_owned())?;

        composer_attachment_from_path_request(request).map_err(|error| format!("{error:#}"))
    }

    fn composer_skill_pack_picker(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::skill_selection::ComposerSkillPickerProjection, String>
    {
        let request = serde_json::from_str::<ClientComposerSkillPackPickerRequest>(input_json)
            .map_err(|_| "invalid composer skill pack picker request".to_owned())?;
        Ok(composer_skill_pack_picker(&self.core, request))
    }

    fn composer_skill_chips(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::skill_selection::ComposerSkillChip>, String> {
        let request = serde_json::from_str::<ClientComposerSkillChipsRequest>(input_json)
            .map_err(|_| "invalid composer skill chips request".to_owned())?;
        Ok(composer_skill_chips(request))
    }

    fn composer_capability_target(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::capabilities::ComposerCapabilityTarget, String> {
        let request = serde_json::from_str::<ClientComposerCapabilityTargetRequest>(input_json)
            .map_err(|_| "invalid composer capability target request".to_owned())?;

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
            .map_err(|_| "invalid composer submission plan request".to_owned())?;

        Ok(composer_submission_plan(request))
    }

    fn composer_skill_rows_for_target(
        &self,
        input_json: &str,
    ) -> Result<Vec<pioneer_client::composer::capabilities::SelectableSkillCapability>, String>
    {
        let request = serde_json::from_str::<ClientComposerSkillRowsForTargetRequest>(input_json)
            .map_err(|_| "invalid composer skill target request".to_owned())?;

        Ok(composer_skill_rows_for_target(request))
    }

    fn composer_filter_mcp_rows(
        &self,
        input_json: &str,
    ) -> Result<ClientComposerFilterMcpRowsResult, String> {
        let request = serde_json::from_str::<ClientComposerFilterMcpRowsRequest>(input_json)
            .map_err(|_| "invalid composer mcp row filter request".to_owned())?;

        Ok(filter_mcp_picker_rows(request))
    }

    fn thread_tree_refresh(&self, input_json: &str) -> Result<(), String> {
        let request = serde_json::from_str::<ThreadTreeRefreshRequest>(input_json)
            .map_err(|_| "invalid thread tree refresh request".to_owned())?;
        self.core
            .refresh_workspace_tree(&request.workspace_id)
            .map(|_| ())
            .map_err(|_| "thread_tree_refresh_failed".to_owned())
    }

    fn thread_tree_level(&self, input_json: &str) -> Result<ClientThreadTreeLevel, String> {
        let request = serde_json::from_str::<ThreadTreeLevelRequest>(input_json)
            .map_err(|_| "invalid thread tree level request".to_owned())?;

        Ok(client_thread_tree_level(request))
    }

    fn active_thread_open(&self, input_json: &str) -> Result<(), String> {
        let request = serde_json::from_str::<ClientActiveThreadOpenRequest>(input_json)
            .map_err(|_| "invalid active thread open request".to_owned())?;

        active_thread::open_thread(&self.core, self.core.transport_runtime(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_open_by_id(&self, input_json: &str) -> Result<(), String> {
        let request = serde_json::from_str::<ClientActiveThreadOpenByIdRequest>(input_json)
            .map_err(|_| "invalid active thread open by id request".to_owned())?;

        active_thread::open_thread_by_id(&self.core, self.core.transport_runtime(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_open_or_create_new(&self, input_json: &str) -> Result<String, String> {
        let request = serde_json::from_str::<ClientEnsureWorkspaceDraftRequest>(input_json)
            .map_err(|_| "invalid active thread new request".to_owned())?;

        active_thread::open_or_create_new_thread(&self.core, self.core.transport_runtime(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_send_text(
        &self,
        input_json: &str,
    ) -> Result<ClientActiveThreadSendTextResult, String> {
        let request = serde_json::from_str::<ClientActiveThreadSendTextRequest>(input_json)
            .map_err(|_| "invalid active thread send text request".to_owned())?;

        active_thread::send_text_turn(&self.core, self.core.transport_runtime(), request)
            .map_err(|error| format!("{error:#}"))
    }

    fn prepare_voice_composer_snapshot(
        &self,
        input_json: &str,
    ) -> Result<pioneer_client::composer::turn_prepare::PreparedVoiceComposerSnapshot, String> {
        let request = serde_json::from_str::<ClientPrepareVoiceComposerSnapshotRequest>(input_json)
            .map_err(|_| "invalid prepare voice composer snapshot request".to_owned())?;

        active_thread::prepare_voice_composer_snapshot(
            &self.core,
            self.core.transport_runtime(),
            request,
        )
        .map_err(|error| format!("{error:#}"))
    }

    fn active_thread_clear(&self) -> Result<ClientActiveThreadClearResult, String> {
        active_thread::clear(&self.core, self.core.transport_runtime())
            .map_err(|error| format!("{error:#}"))
    }

    fn diagnostics_drain(&self) -> Result<Vec<ClientDiagnosticEvent>, String> {
        self.diagnostics.drain()
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

#[cfg(test)]
fn invitation_commit_capacity_available(current: usize) -> bool {
    current < MAX_OUTSTANDING_INVITATION_COMMITS
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

// Session transport and auth control are consumed from scoped Client publications.

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
        "nickname_unavailable",
        "avatar_invalid",
    ]
    .into_iter()
    .find(|code| message.contains(code))
    .unwrap_or(ClientFfiError::GENERIC_CODE);
    ClientFfiError::new(message, code)
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
        /// Calls a JSON Client operation through an owned native Client handle.
        ///
        /// # Safety
        ///
        /// `ptr` must be null or a live handle returned by
        /// `pioneer_client_ffi_client_create`; `input_json` must be null or
        /// point to a valid NUL-terminated string for the duration of the call.
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
        /// Calls a typed-error JSON operation through an owned native Client handle.
        ///
        /// # Safety
        ///
        /// `ptr` must be null or a live handle returned by
        /// `pioneer_client_ffi_client_create`; `input_json` must be null or
        /// point to a valid NUL-terminated string for the duration of the call.
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

/// # Safety
/// `ptr` must be null or a live handle returned by `client_create`.
/// No call may still use the handle, and it must be destroyed only once.
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
    pioneer_client_ffi_client_intent_dispatch,
    client_intent_dispatch
);
ffi_client_json_method!(
    pioneer_client_ffi_client_scope_acquire,
    client_scope_acquire
);
ffi_client_json_method!(
    pioneer_client_ffi_client_scope_release,
    client_scope_release
);
ffi_client_json_method!(
    pioneer_client_ffi_client_scoped_snapshot,
    client_scoped_snapshot
);
ffi_client_json_method!(pioneer_client_ffi_client_change_batch, client_change_batch);
ffi_client_json_method!(
    pioneer_client_ffi_client_wait_publications,
    client_wait_publications
);
ffi_client_json_method!(pioneer_client_ffi_client_shutdown, client_shutdown);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_session_validate,
    gateway_session_validate
);
ffi_client_json_method!(
    pioneer_client_ffi_client_effect_complete,
    client_effect_complete
);
ffi_client_json_method!(
    pioneer_client_ffi_client_effect_cancel,
    client_effect_cancel
);
ffi_client_json_method!(
    pioneer_client_ffi_client_sequence_gap_resnapshot,
    client_sequence_gap_resnapshot
);
ffi_client_json_method!(
    pioneer_client_ffi_mobile_startup_record,
    mobile_startup_record
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_load_registry_v3,
    gateway_load_registry_v3
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_device_activation_presentation,
    gateway_device_activation_presentation
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_device_activation_parse,
    gateway_device_activation_parse
);
ffi_client_json_typed_method!(pioneer_client_ffi_gateway_auth_me, gateway_auth_me);
ffi_client_json_typed_method!(
    pioneer_client_ffi_gateway_authorization_capabilities,
    gateway_authorization_capabilities
);
ffi_client_json_typed_method!(
    pioneer_client_ffi_invitation_presentation,
    invitation_presentation
);
ffi_client_json_typed_method!(pioneer_client_ffi_invitation_create, invitation_create);
ffi_client_json_typed_method!(pioneer_client_ffi_member_avatar_cache, member_avatar_cache);
ffi_client_json_typed_method!(pioneer_client_ffi_agent_avatar_cache, agent_avatar_cache);
ffi_client_json_typed_method!(
    pioneer_client_ffi_member_device_create,
    member_device_create
);
ffi_client_json_typed_method!(pioneer_client_ffi_artifact_view_open, artifact_view_open);
ffi_client_json_typed_method!(
    pioneer_client_ffi_thread_file_view_open,
    thread_file_view_open
);
ffi_client_json_typed_method!(pioneer_client_ffi_artifact_download, artifact_download);
ffi_client_json_method!(pioneer_client_ffi_workspace_bootstrap, workspace_bootstrap);
ffi_client_json_method!(pioneer_client_ffi_workspace_switch, workspace_switch);
ffi_client_json_method!(pioneer_client_ffi_workspace_create, workspace_create);
ffi_client_json_method!(pioneer_client_ffi_workspace_rename, workspace_rename);
ffi_client_json_method!(pioneer_client_ffi_voice_session_start, voice_session_start);
ffi_client_json_method!(
    pioneer_client_ffi_composer_voice_capture_plan,
    composer_voice_capture_plan
);
/// # Safety
/// `ptr` must be a live Client handle; `input_json` must be a valid NUL-terminated
/// UTF-8 allocation. `pcm_ptr` must be readable for `pcm_len` bytes until return.
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
    pioneer_client_ffi_pending_request_presentation,
    pending_request_presentation
);
ffi_client_json_method!(
    pioneer_client_ffi_principal_presentation_capabilities,
    principal_presentation_capabilities
);
ffi_client_json_method!(
    pioneer_client_ffi_artifact_presentation_policy,
    artifact_presentation_policy
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
    pioneer_client_ffi_thread_create_visibility_plan,
    thread_create_visibility_plan
);
ffi_client_json_method!(pioneer_client_ffi_member_presentation, member_presentation);
ffi_client_json_method!(
    pioneer_client_ffi_composer_attachment_from_path,
    composer_attachment_from_path
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_pack_picker,
    composer_skill_pack_picker
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_skill_chips,
    composer_skill_chips
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
    pioneer_client_ffi_composer_skill_rows_for_target,
    composer_skill_rows_for_target
);
ffi_client_json_method!(
    pioneer_client_ffi_composer_filter_mcp_rows,
    composer_filter_mcp_rows
);
ffi_client_json_method!(pioneer_client_ffi_thread_tree_refresh, thread_tree_refresh);
ffi_client_json_method!(pioneer_client_ffi_thread_tree_level, thread_tree_level);
ffi_client_json_method!(pioneer_client_ffi_active_thread_open, active_thread_open);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_open_by_id,
    active_thread_open_by_id
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_open_or_create_new,
    active_thread_open_or_create_new
);
ffi_client_json_method!(
    pioneer_client_ffi_active_thread_send_text,
    active_thread_send_text
);
ffi_client_json_method!(
    pioneer_client_ffi_prepare_voice_composer_snapshot,
    prepare_voice_composer_snapshot
);

/// # Safety
/// `ptr` must be a live Client handle for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_active_thread_clear(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "active_thread_clear", |runtime| {
        runtime.active_thread_clear()
    })
}

/// # Safety
/// `ptr` must be a live Client handle for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_diagnostics_drain(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, "diagnostics_drain", |runtime| {
        runtime.diagnostics_drain()
    })
}

/// # Safety
/// `value` must be null or an unmodified string returned by this ABI.
/// It must be freed exactly once and must not be accessed after this call.
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
            zeroize::Zeroize::zeroize(&mut bytes);
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
    let value = unsafe { CStr::from_ptr(ptr) };
    if value.to_bytes().len() > 8 * 1024 * 1024 {
        return Err("Client input capacity exceeded".into());
    }
    value
        .to_str()
        .map(|value| Zeroizing::new(value.to_owned()))
        .map_err(|_| "Client input is not UTF-8".to_owned())
}

fn to_json_response<T: Serialize>(result: Result<T, String>) -> String {
    match result {
        Ok(value) => serialize_json_response(serde_json::to_string(&FfiResponse::Ok { value })),
        Err(_) => {
            to_json_error_response("Client request failed".into(), "pioneer_client_ffi_error")
        }
    }
}

fn to_json_typed_response<T: Serialize>(result: Result<T, ClientFfiError>) -> String {
    match result {
        Ok(value) => serialize_json_response(serde_json::to_string(&FfiResponse::Ok { value })),
        Err(error) => serialize_json_response(serde_json::to_string(&FfiResponse::<()>::Error {
            message: "Client request failed".into(),
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
    response.unwrap_or_else(|_| r#"{"status":"error","message":"failed to serialize ffi response","code":"pioneer_client_ffi_serialize_error"}"#.to_owned())
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
    catch_unwind(AssertUnwindSafe(|| to_json_response(operation()))).unwrap_or_else(|_payload| {
        to_json_error_response(
            "panic in pioneer client ffi".to_owned(),
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
            diagnostics.record_error(
                operation_name,
                "Client request failed".into(),
                error.code.as_str(),
            );
        }
        to_json_typed_response(response)
    }))
    .unwrap_or_else(|_payload| {
        let message = "panic in pioneer client ffi".to_owned();
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
    catch_unwind(AssertUnwindSafe(|| to_json_response(operation()))).unwrap_or_else(|_payload| {
        let message = "panic in pioneer client ffi".to_owned();
        diagnostics.record_error(operation_name, message.clone(), "pioneer_client_ffi_panic");
        to_json_error_response(message, "pioneer_client_ffi_panic")
    })
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
    fn abi_buffers_have_one_owner_and_invalid_inputs_return_payload_free_errors() {
        unsafe {
            for _ in 0..256 {
                let response = pioneer_client_ffi_version();
                assert!(!response.is_null());
                let value: serde_json::Value =
                    serde_json::from_slice(CStr::from_ptr(response).to_bytes()).unwrap();
                assert_eq!(value["status"], "ok");
                pioneer_client_ffi_string_destroy(response);
            }
            pioneer_client_ffi_string_destroy(std::ptr::null_mut());
            let invalid = CString::new(vec![0xff, 0xfe]).unwrap();
            assert_eq!(
                read_c_string(invalid.as_ptr()).unwrap_err(),
                "Client input is not UTF-8"
            );
            let oversized = CString::new(vec![b'x'; 8 * 1024 * 1024 + 1]).unwrap();
            assert_eq!(
                read_c_string(oversized.as_ptr()).unwrap_err(),
                "Client input capacity exceeded"
            );
            let client = pioneer_client_ffi_client_create();
            let payload = CString::new("{secret_payload:invalid").unwrap();
            for response in [
                pioneer_client_ffi_client_initialize(client, payload.as_ptr()),
                pioneer_client_ffi_client_initialize(client, std::ptr::null()),
                pioneer_client_ffi_client_initialize(std::ptr::null_mut(), payload.as_ptr()),
            ] {
                let encoded = CStr::from_ptr(response).to_str().unwrap();
                assert!(!encoded.contains("secret_payload"));
                let value: serde_json::Value = serde_json::from_str(encoded).unwrap();
                assert_eq!(value["status"], "error");
                assert!(value["code"].is_string());
                pioneer_client_ffi_string_destroy(response);
            }
            pioneer_client_ffi_client_destroy(client);
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
    fn rejected_scope_acquisition_does_not_retain_a_boundary_observer() {
        let runtime = ClientFfiRuntime::default();
        let request = r#"{"schema_version":1,"scope":{"kind":"settings"}}"#;
        assert!(runtime.client_scope_acquire(request).is_err());
        assert!(runtime.observed_scopes.lock().unwrap().is_empty());
        runtime.initialize("{}").unwrap();
        runtime.client_shutdown("{}").unwrap();
        assert!(runtime.client_scope_acquire(request).is_err());
        assert!(runtime.observed_scopes.lock().unwrap().is_empty());
        assert!(runtime.client_subscriptions.lock().unwrap().is_empty());
    }

    #[test]
    fn thread_snapshot_reads_are_pure_and_explicit_scope_release_retires_the_queue() {
        let runtime = ClientFfiRuntime::default();
        runtime.initialize(r#"{"platform":"ios"}"#).unwrap();
        let scope = ClientScope::Thread {
            thread_id: "synthetic".into(),
        };
        runtime
            .client_scoped_snapshot(
                r#"{"schema_version":1,"scope":{"kind":"thread","thread_id":"synthetic"}}"#,
            )
            .unwrap();
        assert!(runtime.client_subscriptions.lock().unwrap().is_empty());
        runtime.ensure_client_subscription(scope.clone()).unwrap();
        runtime.client_intent_dispatch(r#"{"schema_version":1,"intent":{"kind":"set_scope_demand","scope":{"kind":"thread","thread_id":"synthetic"},"demand":"suspended","generation":1}}"#).unwrap();
        assert!(
            runtime
                .client_scope_release(
                    r#"{"schema_version":1,"scope":{"kind":"thread","thread_id":"synthetic"}}"#
                )
                .unwrap()
        );
        assert!(
            !runtime
                .client_subscriptions
                .lock()
                .unwrap()
                .contains_key(&scope)
        );
    }

    #[test]
    fn client_binding_routes_versioned_intents_and_exact_scope_subscription() {
        let runtime = ClientFfiRuntime::default();
        runtime.initialize(r#"{"platform":"ios"}"#).unwrap();
        let intent = serde_json::json!({
            "schema_version": client_binding::CLIENT_BINDING_SCHEMA_VERSION,
            "intent": {
                "kind": "set_scope_demand",
                "scope": { "kind": "settings" },
                "demand": "visible",
                "generation": 1
            }
        })
        .to_string();
        assert_eq!(
            runtime.client_intent_dispatch(&intent).unwrap().outcome,
            pioneer_client::core::ClientTransitionOutcome::Changed
        );
        assert_eq!(
            runtime.client_intent_dispatch(&intent).unwrap().outcome,
            pioneer_client::core::ClientTransitionOutcome::Noop
        );

        let snapshot_request = serde_json::json!({
            "schema_version": client_binding::CLIENT_BINDING_SCHEMA_VERSION,
            "scope": { "kind": "settings" },
            "after_revision": null
        })
        .to_string();
        assert!(
            runtime
                .client_scoped_snapshot(&snapshot_request)
                .unwrap()
                .is_none()
        );
        assert!(
            runtime
                .client_scoped_snapshot(&snapshot_request)
                .unwrap()
                .is_none()
        );
        assert!(runtime.client_subscriptions.lock().unwrap().is_empty());
        runtime
            .client_scope_acquire(r#"{"schema_version":1,"scope":{"kind":"settings"}}"#)
            .unwrap();
        assert_eq!(runtime.client_subscriptions.lock().unwrap().len(), 1);

        let batch = runtime
            .client_change_batch(
                serde_json::json!({
                    "schema_version": client_binding::CLIENT_BINDING_SCHEMA_VERSION,
                    "scope": { "kind": "settings" },
                    "maximum_items": 4
                })
                .to_string()
                .as_str(),
            )
            .unwrap();
        assert!(batch.changes.is_empty());
    }

    #[test]
    fn timeline_wire_binding_and_direct_client_share_plans_and_scope_retirement() {
        use pioneer_client::core::{ClientCore, ClientIntent};
        use pioneer_client::timeline::controller::{TimelineDemand, TimelineIntent};
        let mut runtime = ClientFfiRuntime::default();
        runtime.initialize(r#"{"platform":"ios"}"#).unwrap();
        runtime.core = Arc::new(ClientCore::new());
        let direct = ClientCore::new();
        for core in [&direct, runtime.core.as_ref()] {
            core.upsert_thread(serde_json::from_value(serde_json::json!({
                "created_at":1,"id":"a","mode":"Chat","model":"synthetic-model","model_provider":"synthetic-provider","origin_kind":"user","preview":"","sidebar_visibility":"visible","status":"Idle","turns":[],"updated_at":2,"workspace_id":"workspace"
            })).unwrap());
            core.apply_thread_timeline_page(serde_json::from_value(serde_json::json!({
                "workspaceId":"workspace","threadId":"a","projectionVersion":1,
                "blocks":[{"workspaceId":"workspace","threadId":"a","blockId":"message","turnId":"turn","sortKey":"1","kind":{"kind":"user_message","text":"synthetic","mode":"Message"}}],
                "page":{"hasMoreBefore":false,"hasMoreAfter":false}
            })).unwrap(), pioneer_client::timeline::semantic::TopLevelPageMergeMode::Reset);
            core.dispatch(ClientIntent::SetScopeDemand {
                scope: ClientScope::Timeline {
                    thread_id: "a".into(),
                },
                demand: pioneer_client::core::ClientDemand::Visible,
                generation: pioneer_client::core::ClientGeneration::new(1),
            });
        }
        let snapshot = direct.thread_presentation_snapshot("a").unwrap();
        assert_eq!(
            snapshot.timeline().source_revision(),
            direct.thread_snapshot("a").unwrap().timeline_revision(),
            "fixture must publish its latest source"
        );
        let input = TimelineIntent::Update {
            demand: TimelineDemand {
                thread_id: "a".into(),
                consumer_id: "visible".into(),
                generation: 1,
                source_revision: snapshot.timeline().source_revision(),
                row_ids: snapshot
                    .timeline()
                    .rows()
                    .iter()
                    .map(|r| r.id().as_str().to_owned())
                    .collect(),
                threshold: 3,
                before: true,
                after: true,
                work: true,
                presented_rows: false,
                scroll_generation: 0,
                latest_user_turn_id: Some("turn".into()),
                viewed_through_turn_id: Some("turn".into()),
                read_requires_unread: false,
                prefetch_on_visibility: true,
                boundary_request_limit: 1,
            },
        };
        let envelope = client_binding::ClientIntentDispatchDto {
            schema_version: 1,
            intent: ClientIntent::Timeline {
                intent: input.clone(),
            },
        };
        let wire = serde_json::to_string(&envelope).unwrap();
        let decoded: client_binding::ClientIntentDispatchDto = serde_json::from_str(&wire).unwrap();
        let ClientIntent::Timeline { intent } = decoded.intent else {
            panic!()
        };
        let plan = direct.plan_timeline_intent(input.clone(), 0);
        assert_eq!(plan.reads.len(), 1);
        assert_eq!(plan, runtime.core.plan_timeline_intent(intent, 0));
        // The actual bridge dispatch of equal demand is a no-op: no fake plan is executed.
        assert_eq!(
            runtime.client_intent_dispatch(&wire).unwrap(),
            client_binding::transition_dto(direct.dispatch(envelope.intent))
        );
        let exit = ClientIntent::Timeline {
            intent: TimelineIntent::Exit {
                thread_id: "a".into(),
                consumer_id: "visible".into(),
                generation: 1,
            },
        };
        let wire = serde_json::to_string(&client_binding::ClientIntentDispatchDto {
            schema_version: 1,
            intent: exit.clone(),
        })
        .unwrap();
        assert_eq!(
            runtime.client_intent_dispatch(&wire).unwrap(),
            client_binding::transition_dto(direct.dispatch(exit))
        );
        assert_eq!(
            direct.plan_timeline_intent(input.clone(), 1),
            Default::default()
        );
        assert_eq!(
            runtime.core.plan_timeline_intent(input, 1),
            Default::default()
        );
    }

    #[test]
    fn navigation_binding_matches_direct_rust_for_selection_destinations_and_fences() {
        use pioneer_client::navigation::{
            AdministrationRoute, NavigationIntent, SemanticDestination, SettingsRoute,
            TaskThreadLineage,
        };
        use pioneer_client::providers::selectors::ProviderFilter;
        let runtime = ClientFfiRuntime::default();
        runtime.initialize(r#"{"platform":"ios"}"#).unwrap();
        let direct = pioneer_client::core::ClientCore::shared();
        let intents = vec![
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            NavigationIntent::RememberDraft {
                workspace_id: "workspace".into(),
                thread_id: Some("draft".into()),
            },
            NavigationIntent::SelectThread {
                workspace_id: Some("workspace".into()),
                thread_id: Some("parent".into()),
            },
            NavigationIntent::PushTaskThread {
                entry: TaskThreadLineage::new(
                    "parent".into(),
                    "child".into(),
                    "workspace".into(),
                    "Task".into(),
                ),
            },
            NavigationIntent::PopTaskThread,
            NavigationIntent::PopTaskThread,
            NavigationIntent::Navigate {
                destination: SemanticDestination::Providers {
                    filter: ProviderFilter::Connected,
                },
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Administration {
                    route: AdministrationRoute::Invitations,
                },
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Mcp {
                    server_id: Some("server".into()),
                },
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Skills { skill_id: None },
            },
            NavigationIntent::Navigate {
                destination: SemanticDestination::Settings {
                    route: SettingsRoute::Memory,
                },
            },
            NavigationIntent::PromoteThread {
                thread_id: "draft".into(),
            },
            NavigationIntent::PromoteThread {
                thread_id: "draft".into(),
            },
            NavigationIntent::SelectThread {
                workspace_id: None,
                thread_id: Some(" ".into()),
            },
            NavigationIntent::Reset,
            NavigationIntent::Reset,
        ];
        for intent in intents {
            let request = client_binding::ClientIntentDispatchDto {
                schema_version: 1,
                intent: pioneer_client::core::ClientIntent::Navigation {
                    intent,
                    expected_revision: None,
                },
            };
            let direct_result = direct.dispatch(request.intent.clone());
            let ffi_result = runtime
                .client_intent_dispatch(&serde_json::to_string(&request).unwrap())
                .unwrap();
            assert_eq!(ffi_result, client_binding::transition_dto(direct_result));
            let ffi = runtime
                .client_scoped_snapshot(r#"{"schema_version":1,"scope":{"kind":"navigation"}}"#)
                .unwrap()
                .unwrap();
            assert_eq!(
                ffi,
                client_binding::snapshot_dto(direct.snapshot(&ClientScope::Navigation).unwrap())
            );
        }
        let stale = r#"{"schema_version":1,"intent":{"kind":"navigation","intent":{"kind":"reset"},"expected_revision":0}}"#;
        assert_eq!(
            runtime.client_intent_dispatch(stale).unwrap().outcome,
            pioneer_client::core::ClientTransitionOutcome::Stale
        );
        for core in [&direct, &runtime.core] {
            core.activate_thread(Some("protected"), Some("workspace"));
            core.begin_authorization_epoch(None);
            assert_eq!(core.navigation_snapshot().active_thread_id(), None);
            assert!(
                core.snapshot(&ClientScope::Navigation)
                    .unwrap()
                    .typed::<pioneer_client::navigation::ClientNavigationState>()
                    .is_some()
            );
        }
    }

    #[test]
    fn client_binding_rejects_unknown_schema_versions() {
        let runtime = ClientFfiRuntime::default();
        runtime.initialize("{}").unwrap();
        let error = runtime
            .client_scoped_snapshot(
                serde_json::json!({
                    "schema_version": client_binding::CLIENT_BINDING_SCHEMA_VERSION + 1,
                    "scope": { "kind": "navigation" },
                    "after_revision": null
                })
                .to_string()
                .as_str(),
            )
            .expect_err("unknown binding versions must fail closed");
        assert!(error.contains("unsupported Client binding schema version"));
    }

    #[test]
    fn session_demand_replay_preserves_direct_outcomes_revisions_and_effects() {
        use pioneer_client::{
            core::ClientIntent,
            gateway::session_driver::{SessionDemand, SessionVisibility},
        };
        let direct = ClientCore::shared();
        let ffi = ClientFfiRuntime::default();
        ffi.initialize("{}").unwrap();
        for (generation, visibility, online) in [
            (1, SessionVisibility::Inactive, true),
            (1, SessionVisibility::Inactive, true),
            (0, SessionVisibility::Foreground, true),
            (1, SessionVisibility::Foreground, true),
            (2, SessionVisibility::Background, true),
            (3, SessionVisibility::Foreground, false),
            (4, SessionVisibility::Foreground, true),
        ] {
            let intent = ClientIntent::SessionDemand {
                demand: SessionDemand {
                    endpoint_id: None,
                    generation,
                    visibility,
                    network_available: online,
                },
            };
            let expected = client_binding::transition_dto(direct.dispatch(intent.clone()));
            let actual = ffi
                .client_intent_dispatch(
                    &serde_json::json!({"schema_version": 1, "intent": intent}).to_string(),
                )
                .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(ffi.core.gateway_session(), direct.gateway_session());
        }
        ffi.client_shutdown("{}").unwrap();
    }

    #[test]
    fn client_binding_replays_independent_core_outputs_and_bounded_delivery() {
        use pioneer_client::core::*;
        let direct = ClientCore::shared();
        let runtime = ClientFfiRuntime::default();
        runtime.initialize("{}").unwrap();
        assert!(!Arc::ptr_eq(&direct, &runtime.core));
        let scope = ClientScope::Settings;
        let subscription = direct.subscribe(scope.clone(), NonZeroUsize::new(64).unwrap());
        let request = serde_json::json!({"schema_version": 1, "scope": scope}).to_string();
        assert!(runtime.client_scoped_snapshot(&request).unwrap().is_none());
        runtime.client_scope_acquire(&request).unwrap();
        let authority = ClientMutationAuthority::for_test();
        let revisions = |n| {
            ClientRevisions::new(
                DomainRevision::new(n),
                PresentationRevision::new(n),
                ContentRevision::new(n),
                ScopedRevision::new(n),
            )
        };
        let operation = ClientOperationId::new("refresh-settings").unwrap();
        let plan = ClientEffectPlan::new(
            operation.clone(),
            ClientGeneration::new(1),
            pioneer_client::notifications::effects::ClientEffect::RefreshGatewaySettings,
        );
        let compare = |left: ClientTransition, right: ClientTransition| {
            assert_eq!(
                client_binding::transition_dto(left),
                client_binding::transition_dto(right)
            );
        };
        compare(
            direct.transition(&authority, vec![], vec![plan.clone()]),
            runtime.core.transition(&authority, vec![], vec![plan]),
        );
        for (generation, demand) in [
            (2, ClientDemand::Visible),
            (2, ClientDemand::Visible),
            (1, ClientDemand::Visible),
            (2, ClientDemand::Suspended),
        ] {
            let intent = ClientIntent::SetScopeDemand {
                scope: scope.clone(),
                generation: ClientGeneration::new(generation),
                demand,
            };
            let expected = client_binding::transition_dto(direct.dispatch(intent.clone()));
            let actual = runtime
                .client_intent_dispatch(
                    &serde_json::json!({"schema_version": 1, "intent": intent}).to_string(),
                )
                .unwrap();
            assert_eq!(expected, actual);
        }
        for generation in [0, 1, 1] {
            let cancellation =
                ClientEffectCancellation::new(operation.clone(), ClientGeneration::new(generation));
            let expected =
                client_binding::transition_dto(direct.cancel_effect(cancellation.clone()));
            let actual = runtime
                .client_effect_cancel(
                    &serde_json::json!({"schema_version": 1, "cancellation": cancellation})
                        .to_string(),
                )
                .unwrap();
            assert_eq!(expected, actual);
        }
        let completion = ClientEffectCompletion::new(
            operation,
            ClientGeneration::new(1),
            ClientEffectResult::Completed,
        );
        assert_eq!(
            client_binding::transition_dto(direct.complete_effect(completion.clone())),
            runtime
                .client_effect_complete(
                    &serde_json::json!({"schema_version": 1, "completion": completion}).to_string()
                )
                .unwrap()
        );

        for n in 1..=70 {
            compare(
                direct.publish(&authority, scope.clone(), revisions(n), Arc::new(n), vec![]),
                runtime
                    .core
                    .publish(&authority, scope.clone(), revisions(n), Arc::new(n), vec![]),
            );
        }
        let batch_request =
            serde_json::json!({"schema_version": 1, "scope": scope, "maximum_items": 256})
                .to_string();
        let actual = runtime.client_change_batch(&batch_request).unwrap();
        let mut expected = Vec::new();
        while let Some(event) = subscription.try_next() {
            expected.push(client_binding::change_dto(event));
        }
        assert_eq!(actual.changes, expected);
        assert!(
            matches!(actual.changes.as_slice(), [client_binding::ClientChangeDto::ResnapshotRequired { latest_sequence, .. }] if *latest_sequence == direct.snapshot(&scope).unwrap().snapshot().sequence())
        );
        assert!(
            runtime
                .client_change_batch(&batch_request)
                .unwrap()
                .changes
                .is_empty()
        );
        let expected_snapshot = client_binding::snapshot_dto(direct.snapshot(&scope).unwrap());
        assert_eq!(
            runtime.client_sequence_gap_resnapshot(&request).unwrap(),
            Some(expected_snapshot)
        );
        assert!(
            runtime
                .client_scoped_snapshot(
                    &serde_json::json!({"schema_version": 1, "scope": scope, "after_revision": 70})
                        .to_string()
                )
                .unwrap()
                .is_none()
        );

        for n in 71..=73 {
            compare(
                direct.publish(&authority, scope.clone(), revisions(n), Arc::new(n), vec![]),
                runtime
                    .core
                    .publish(&authority, scope.clone(), revisions(n), Arc::new(n), vec![]),
            );
        }
        let actual = runtime.client_change_batch(&batch_request).unwrap();
        let expected = (0..3)
            .map(|_| client_binding::change_dto(subscription.try_next().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(actual.changes, expected);
        for limit in [0, 257] {
            assert!(runtime.client_change_batch(&serde_json::json!({"schema_version": 1, "scope": scope, "maximum_items": limit}).to_string()).is_err());
        }
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
        assert_eq!(events[0].message, "Client request failed");
        assert!(!response.to_string().contains(secret));
    }

    #[test]
    fn session_demand_rejects_malformed_input_without_work_or_credential_echo() {
        let runtime = ClientFfiRuntime::default();
        runtime.initialize("{}").unwrap();
        for input in [
            r#"{"schema_version":1,"intent":{"kind":"session_demand","demand":{"endpoint_id":"endpoint","visibility":"unknown","generation":1,"network_available":true}}}"#,
            r#"{"schema_version":1,"intent":{"kind":"session_demand","demand":{"endpoint_id":"endpoint","visibility":"foreground","generation":1,"network_available":true,"credential":"synthetic-secret"}}}"#,
        ] {
            let error = runtime.client_intent_dispatch(input).unwrap_err();
            assert_eq!(error, "invalid Client intent request");
            assert!(!error.contains("synthetic-secret"));
        }
        assert!(runtime.core.gateway_session().connections.is_empty());
        assert!(runtime.core.gateway_session().session("endpoint").is_none());
        let error = runtime
            .client_effect_complete(r#"{"schema_version":1,"completion":"synthetic-secret"}"#)
            .unwrap_err();
        assert!(!error.contains("synthetic-secret"));
        runtime.client_shutdown("{}").unwrap();
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
    fn ffi_boundary_converts_panic_to_error_response() {
        let response = ffi_response_json::<(), _>(|| panic!("boom"));
        let error = serde_json::from_str::<serde_json::Value>(response.as_str()).expect("json");

        assert_eq!(error["status"], "error");
        assert_eq!(error["code"], "pioneer_client_ffi_panic");
        assert_eq!(error["message"], "panic in pioneer client ffi");
        assert!(!serde_json::to_string(&error).unwrap().contains("boom"));
    }

    #[test]
    fn transport_lease_boundary_rejects_malformed_versions_and_releases_on_shutdown() {
        let runtime = ClientFfiRuntime::default();
        runtime.initialize("{}").unwrap();
        assert!(
            runtime
                .gateway_transport_reserve(r#"{"schema_version":2,"exclusive":true}"#)
                .is_err()
        );
        assert!(
            runtime
                .gateway_transport_reserve(r#"{"schema_version":1,"exclusive":true,"extra":1}"#)
                .is_err()
        );
        let lease = runtime
            .gateway_transport_reserve(r#"{"schema_version":1,"exclusive":true}"#)
            .unwrap();
        let request = serde_json::json!({"schema_version":1,"lease_id":lease}).to_string();
        assert!(runtime.gateway_transport_wait(&request).unwrap());
        runtime.client_shutdown("{}").unwrap();
        assert!(!runtime.gateway_transport_release(&request).unwrap());
        assert!(
            runtime
                .gateway_transport_reserve(r#"{"schema_version":1,"exclusive":true}"#)
                .is_err()
        );
    }
}

ffi_client_json_method!(
    pioneer_client_ffi_gateway_transport_reserve,
    gateway_transport_reserve
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_transport_wait,
    gateway_transport_wait
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_transport_release,
    gateway_transport_release
);

#[cfg(test)]
mod timeline_publication_tests;

#[cfg(test)]
mod workspace_publication_tests;

#[cfg(test)]
mod provider_runtime_tests;

#[cfg(test)]
mod catalog_binding_tests;

#[cfg(test)]
mod document_binding_tests;

#[cfg(test)]
mod onboarding_binding_tests;

#[cfg(test)]
mod settings_binding_tests;
