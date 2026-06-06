mod client;
mod command_sender;

use crate::state;
use anyhow::{Context, Result, anyhow};
use pioneer_client::rpc::RPC_REQUEST_TIMEOUT;
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
    ArtifactUploadStartResponse, GatewaySettingsGetResponse, GatewaySettingsUpdate,
    GatewaySettingsUpdateResponse, McpInstallParams, McpInstallResponse, McpListParams,
    McpListResponse, McpPolicySetParams, McpPolicySetResponse, McpServerDetailsParams,
    McpServerDetailsResponse, McpServerRestartParams, McpServerRestartResponse, McpUninstallParams,
    McpUninstallResponse, ProviderDeleteApiKeyParams, ProviderDeleteApiKeyResponse,
    ProviderListModelsParams, ProviderListModelsResponse, ProviderListParams, ProviderListResponse,
    ProviderSetApiKeyParams, ProviderSetApiKeyResponse, SkillListParams, SkillListResponse,
    SkillsHealthParams, SkillsHealthResponse, SkillsInstallParams, SkillsInstallResponse,
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
    ThreadFolderMoveResponse, ThreadGetParams, ThreadGetResponse, ThreadHistoryParams,
    ThreadHistoryResponse, ThreadMoveParams, ThreadMoveResponse, ThreadStartParams,
    ThreadStartResponse, ThreadTreeParams, ThreadTreeResponse, ThreadUnsubscribeResponse,
    ThreadUpdateParams, ThreadUpdateResponse, TurnCancelParams, TurnCancelResponse, TurnGetParams,
    TurnGetResponse, TurnItemsParams, TurnItemsResponse, TurnStartParams, TurnStartResponse,
    TurnTimelineParams, TurnTimelineResponse, WorkspaceCreateParams, WorkspaceCreateResponse,
    WorkspaceDefaultResponse, WorkspaceListResponse, WorkspaceSelectParams,
    WorkspaceSelectResponse, WorkspaceUpdateParams, WorkspaceUpdateResponse,
};
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pioneer_client::artifacts::download::{
    ARTIFACT_DOWNLOAD_CACHE_MAX_AGE, ArtifactDownloadCachePaths,
    build_artifact_download_cache_path, prune_artifact_download_cache,
    verify_artifact_download_file,
};
#[cfg(test)]
use pioneer_client::transport::ws::backoff::{duration_to_millis_u64, next_backoff};
use pioneer_client::transport::ws::client::{GatewayWsCommand, spawn_worker};
#[cfg(test)]
use pioneer_client::transport::ws::decode::process_text_payload;
use pioneer_client::transport::ws::download::ArtifactDownloadChunkPayload;
#[cfg(test)]
pub(super) use pioneer_client::transport::ws::frames::encode_artifact_upload_chunk_frame;
#[cfg(test)]
use pioneer_client::transport::ws::rpc::normalize_ws_url;
use pioneer_client::transport::ws::{GatewayWsConnectSpec, GatewayWsEvent};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

const UPLOAD_CHUNK_ACK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct GatewayWsCommandSender {
    command_tx: UnboundedSender<GatewayWsCommand>,
    next_connection_id: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct GatewayWsClient {
    command_sender: GatewayWsCommandSender,
    event_rx: Arc<Mutex<Receiver<GatewayWsEvent>>>,
}

#[cfg(test)]
mod tests;
