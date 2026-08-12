mod client;
mod command_sender;

use crate::transport::http::{GatewayHttpAccess, GatewayHttpAuthorityError};
use anyhow::{Context, Result, anyhow};
use pioneer_protocol::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactCapabilitiesParams,
    ArtifactCapabilitiesResponse, ArtifactDeleteParams, ArtifactDeleteResponse, ArtifactGetParams,
    ArtifactGetResponse, ArtifactListForMessageParams, ArtifactListForThreadParams,
    ArtifactListForTurnParams, ArtifactListParams, ArtifactListResponse, ArtifactRestoreParams,
    ArtifactRestoreResponse, ArtifactUploadAbortParams, ArtifactUploadAbortResponse,
    ArtifactUploadChunkAckNotification, ArtifactUploadFinishParams, ArtifactUploadFinishResponse,
    ArtifactUploadStartParams, ArtifactUploadStartResponse, ArtifactViewGrantCreateParams,
    ArtifactViewGrantCreateResponse, AuthDeviceCreateResponse, AuthLogoutResponse, AuthMeResponse,
    AuthProfileUpdateParams, AuthProfileUpdateResponse, AuthSessionListResponse,
    AuthSessionRevokeParams, AuthSessionRevokeResponse, AuthorizationCapabilitiesParams,
    AuthorizationCapabilitySnapshot, CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse,
    CLIRuntimeListParams, CLIRuntimeListResponse, CLIRuntimeLoginCancelParams,
    CLIRuntimeLoginCancelResponse, CLIRuntimeLoginStartParams, CLIRuntimeLoginStartResponse,
    CLIRuntimeProxyDeleteParams, CLIRuntimeProxyDeleteResponse, CLIRuntimeProxySetParams,
    CLIRuntimeProxySetResponse, CLIRuntimeRefreshParams, CLIRuntimeRefreshResponse,
    CLIRuntimeRequestRespondParams, CLIRuntimeRequestRespondResponse, CLIRuntimeReviewStartParams,
    CLIRuntimeReviewStartResponse, CLIRuntimeStatusParams, CLIRuntimeStatusResponse,
    CLIRuntimeThreadBindingGetParams, CLIRuntimeThreadBindingGetResponse,
    CLIRuntimeThreadCompactParams, CLIRuntimeThreadCompactResponse, CLIRuntimeThreadForkParams,
    CLIRuntimeThreadForkResponse, CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse,
    GatewaySettingsGetResponse, GatewaySettingsUpdate, GatewaySettingsUpdateResponse,
    InvitationCreateParams, InvitationCreateResponse, InvitationListParams, InvitationListResponse,
    InvitationRevokeParams, InvitationRevokeResponse, McpInstallParams, McpInstallResponse,
    McpListParams, McpListResponse, McpPolicySetParams, McpPolicySetResponse,
    McpServerDetailsParams, McpServerDetailsResponse, McpServerRestartParams,
    McpServerRestartResponse, McpUninstallParams, McpUninstallResponse, MemberDeviceCreateParams,
    MemberDeviceCreateResponse, MemberListParams, MemberListResponse, MemberMutationResponse,
    MemberRemoveParams, MemberRestoreParams, MemberSuspendParams, ProviderConfigureParams,
    ProviderConfigureResponse, ProviderDeleteApiKeyParams, ProviderDeleteApiKeyResponse,
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ProviderSetApiKeyParams, ProviderSetApiKeyResponse, SkillListParams, SkillListResponse,
    SkillsHealthParams, SkillsHealthResponse, SkillsInstallParams, SkillsInstallResponse,
    SkillsPackInstallParams, SkillsPackInstallResponse, SkillsPackUninstallParams,
    SkillsPackUninstallResponse, SkillsPackUpdateParams, SkillsPackUpdateResponse,
    SkillsPolicyListParams, SkillsPolicyListResponse, SkillsPolicySetParams,
    SkillsPolicySetResponse, SkillsUninstallParams, SkillsUninstallResponse, SkillsUpdateParams,
    SkillsUpdateResponse, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadChunkAckNotification, SkillsUploadFinishParams, SkillsUploadFinishResponse,
    SkillsUploadStartParams, SkillsUploadStartResponse, TaskAcceptParams, TaskAcceptResponse,
    TaskCancelParams, TaskCancelResponse, TaskReviseParams, TaskReviseResponse,
    ThreadAgentsDocArchiveParams, ThreadAgentsDocArchiveResponse, ThreadAgentsDocGetParams,
    ThreadAgentsDocGetResponse, ThreadAgentsDocResolveForThreadParams,
    ThreadAgentsDocResolveForThreadResponse, ThreadAgentsDocSaveParams,
    ThreadAgentsDocSaveResponse, ThreadFolderCreateParams, ThreadFolderCreateResponse,
    ThreadFolderDeleteParams, ThreadFolderDeleteResponse, ThreadFolderMoveParams,
    ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse, ThreadMoveParams,
    ThreadMoveResponse, ThreadParticipantMutationParams, ThreadParticipantsListParams,
    ThreadParticipantsResponse, ThreadStartParams, ThreadStartResponse, ThreadTimelinePageParams,
    ThreadTimelinePageResponse, ThreadTreeParams, ThreadTreeResponse, ThreadUnsubscribeResponse,
    ThreadUpdateParams, ThreadUpdateResponse, TurnCancelParams, TurnCancelResponse, TurnGetParams,
    TurnGetResponse, TurnItemsParams, TurnItemsResponse, TurnPermissionRequestRespondParams,
    TurnPermissionRequestRespondResponse, TurnStartParams, TurnStartResponse,
    TurnWorkItemsGetParams, TurnWorkItemsGetResponse, TurnWorkPageParams, TurnWorkPageResponse,
    VoiceAudioFormat, VoiceSessionCancelParams, VoiceSessionCancelResponse,
    VoiceSessionFinalizeParams, VoiceSessionFinalizeResponse, VoiceSessionStartParams,
    VoiceSessionStartResponse, VoiceStatusParams, VoiceStatusResponse, WorkspaceCreateParams,
    WorkspaceCreateResponse, WorkspaceDefaultResponse, WorkspaceListResponse,
    WorkspaceMemberAddParams, WorkspaceMemberListParams, WorkspaceMemberListResponse,
    WorkspaceMemberMutationResponse, WorkspaceMemberRemoveParams, WorkspaceSelectParams,
    WorkspaceSelectResponse, WorkspaceUpdateParams, WorkspaceUpdateResponse,
};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use super::client::{GatewayWsCommand, spawn_worker};
use super::{GatewayWsConnectSpec, GatewayWsEvent};

const UPLOAD_CHUNK_ACK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct GatewayWsCommandSender {
    command_tx: UnboundedSender<GatewayWsCommand>,
    next_connection_id: Arc<AtomicU64>,
    session_access: Arc<Mutex<Option<GatewayHttpAccess>>>,
}

#[derive(Clone)]
pub struct GatewayWsClient {
    command_sender: GatewayWsCommandSender,
    event_rx: Arc<Mutex<Receiver<GatewayWsEvent>>>,
}
