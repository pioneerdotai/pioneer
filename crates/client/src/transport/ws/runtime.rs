mod client;
mod command_sender;

use crate::rpc::RPC_REQUEST_TIMEOUT;
use anyhow::{Context, Result, anyhow};
use pioneer_protocol::{
    ArtifactBindParams, ArtifactBindResponse, ArtifactCapabilitiesParams,
    ArtifactCapabilitiesResponse, ArtifactDeleteParams, ArtifactDeleteResponse,
    ArtifactDownloadAbortParams, ArtifactDownloadAbortResponse, ArtifactDownloadChunkParams,
    ArtifactDownloadChunkResponse, ArtifactDownloadFinishParams, ArtifactDownloadFinishResponse,
    ArtifactDownloadStartParams, ArtifactDownloadStartResponse, ArtifactGetParams,
    ArtifactGetResponse, ArtifactListForMessageParams, ArtifactListForThreadParams,
    ArtifactListForTurnParams, ArtifactListParams, ArtifactListResponse, ArtifactReadParams,
    ArtifactReadResponse, ArtifactRestoreParams, ArtifactRestoreResponse,
    ArtifactUploadAbortParams, ArtifactUploadAbortResponse, ArtifactUploadChunkAckNotification,
    ArtifactUploadFinishParams, ArtifactUploadFinishResponse, ArtifactUploadStartParams,
    ArtifactUploadStartResponse, CLIRuntimeListModelsParams, CLIRuntimeListModelsResponse,
    CLIRuntimeListParams, CLIRuntimeListResponse, CLIRuntimeLoginCancelParams,
    CLIRuntimeLoginCancelResponse, CLIRuntimeLoginStartParams, CLIRuntimeLoginStartResponse,
    CLIRuntimeRefreshParams, CLIRuntimeRefreshResponse, CLIRuntimeRequestRespondParams,
    CLIRuntimeRequestRespondResponse, CLIRuntimeReviewStartParams, CLIRuntimeReviewStartResponse,
    CLIRuntimeStatusParams, CLIRuntimeStatusResponse, CLIRuntimeThreadBindingGetParams,
    CLIRuntimeThreadBindingGetResponse, CLIRuntimeThreadCompactParams,
    CLIRuntimeThreadCompactResponse, CLIRuntimeThreadForkParams, CLIRuntimeThreadForkResponse,
    CLIRuntimeTurnSteerParams, CLIRuntimeTurnSteerResponse, GatewaySettingsGetResponse,
    GatewaySettingsUpdate, GatewaySettingsUpdateResponse, McpInstallParams, McpInstallResponse,
    McpListParams, McpListResponse, McpPolicySetParams, McpPolicySetResponse,
    McpServerDetailsParams, McpServerDetailsResponse, McpServerRestartParams,
    McpServerRestartResponse, McpUninstallParams, McpUninstallResponse, ProviderDeleteApiKeyParams,
    ProviderDeleteApiKeyResponse, ProviderListModelsParams, ProviderListModelsResponse,
    ProviderListParams, ProviderListResponse, ProviderSetApiKeyParams, ProviderSetApiKeyResponse,
    SkillListParams, SkillListResponse, SkillsHealthParams, SkillsHealthResponse,
    SkillsInstallParams, SkillsInstallResponse, SkillsPolicyListParams, SkillsPolicyListResponse,
    SkillsPolicySetParams, SkillsPolicySetResponse, SkillsUninstallParams, SkillsUninstallResponse,
    SkillsUpdateParams, SkillsUpdateResponse, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadChunkAckNotification, SkillsUploadFinishParams, SkillsUploadFinishResponse,
    SkillsUploadStartParams, SkillsUploadStartResponse, TaskAcceptParams, TaskAcceptResponse,
    TaskCancelParams, TaskCancelResponse, TaskReviseParams, TaskReviseResponse,
    ThreadAgentsDocArchiveParams, ThreadAgentsDocArchiveResponse, ThreadAgentsDocGetParams,
    ThreadAgentsDocGetResponse, ThreadAgentsDocResolveForThreadParams,
    ThreadAgentsDocResolveForThreadResponse, ThreadAgentsDocSaveParams,
    ThreadAgentsDocSaveResponse, ThreadFolderCreateParams, ThreadFolderCreateResponse,
    ThreadFolderDeleteParams, ThreadFolderDeleteResponse, ThreadFolderMoveParams,
    ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse, ThreadMoveParams,
    ThreadMoveResponse, ThreadStartParams, ThreadStartResponse, ThreadTimelinePageParams,
    ThreadTimelinePageResponse, ThreadTreeParams, ThreadTreeResponse, ThreadUnsubscribeResponse,
    ThreadUpdateParams, ThreadUpdateResponse, TurnCancelParams, TurnCancelResponse, TurnGetParams,
    TurnGetResponse, TurnItemsParams, TurnItemsResponse, TurnPermissionRequestRespondParams,
    TurnPermissionRequestRespondResponse, TurnStartParams, TurnStartResponse, TurnWorkPageParams,
    TurnWorkPageResponse, WorkspaceCreateParams, WorkspaceCreateResponse, WorkspaceDefaultResponse,
    WorkspaceListResponse, WorkspaceSelectParams, WorkspaceSelectResponse, WorkspaceUpdateParams,
    WorkspaceUpdateResponse,
};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use super::client::{GatewayWsCommand, spawn_worker};
use super::download::ArtifactDownloadChunkPayload;
use super::{GatewayWsConnectSpec, GatewayWsEvent};

const UPLOAD_CHUNK_ACK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct GatewayWsCommandSender {
    command_tx: UnboundedSender<GatewayWsCommand>,
    next_connection_id: Arc<AtomicU64>,
    artifact_cache_root: Arc<Mutex<Option<PathBuf>>>,
}

#[derive(Clone)]
pub struct GatewayWsClient {
    command_sender: GatewayWsCommandSender,
    event_rx: Arc<Mutex<Receiver<GatewayWsEvent>>>,
}
