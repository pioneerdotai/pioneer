mod backoff;
mod client;
mod command_sender;
mod decode;
mod rpc;
mod worker;

use crate::gateway::timings::GatewayWsTimings;
use crate::gateway::types::GatewayEndpointKind;
use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use pioneer_protocol::generate_id;
use pioneer_protocol::{
    GatewayNotification, JSONRPC_VERSION, JsonRpcNotification, JsonRpcRequest, McpInstallParams,
    McpInstallResponse, McpListParams, McpListResponse, McpPolicySetParams, McpPolicySetResponse,
    McpServerDetailsParams, McpServerDetailsResponse, McpServerRestartParams,
    McpServerRestartResponse, McpUninstallParams, McpUninstallResponse, ProviderDeleteApiKeyParams,
    ProviderDeleteApiKeyResponse, ProviderListModelsParams, ProviderListModelsResponse,
    ProviderListParams, ProviderListResponse, ProviderSetApiKeyParams, ProviderSetApiKeyResponse,
    REQUEST_ID_LEN, RequestId, SkillListParams, SkillListResponse, SkillsHealthParams,
    SkillsHealthResponse, SkillsInstallParams, SkillsInstallResponse, SkillsPolicySetParams,
    SkillsPolicySetResponse, SkillsUninstallParams, SkillsUninstallResponse, SkillsUpdateParams,
    SkillsUpdateResponse, SkillsUploadAbortParams, SkillsUploadAbortResponse,
    SkillsUploadChunkAckNotification, SkillsUploadChunkHeader, SkillsUploadFinishParams,
    SkillsUploadFinishResponse, SkillsUploadStartParams, SkillsUploadStartResponse,
    ThreadFolderCreateParams, ThreadFolderCreateResponse, ThreadFolderDeleteParams,
    ThreadFolderDeleteResponse, ThreadFolderMoveParams, ThreadFolderMoveResponse,
    ThreadHistoryParams, ThreadHistoryResponse, ThreadMoveParams, ThreadMoveResponse,
    ThreadStartParams, ThreadStartResponse, ThreadTreeParams, ThreadTreeResponse,
    ThreadUnsubscribeParams, ThreadUnsubscribeResponse, TurnCancelParams, TurnCancelResponse,
    TurnGetParams, TurnGetResponse, TurnItemsParams, TurnItemsResponse, TurnStartParams,
    TurnStartResponse, TurnTimelineParams, TurnTimelineResponse, WorkspaceDefaultParams,
    WorkspaceDefaultResponse, constants::events, constants::methods,
};
use pioneer_skills::is_qualified_skill_slug;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::Request;

use backoff::{duration_to_millis_u64, jitter_delay, next_backoff};
use decode::{
    fail_pending_requests, fail_pending_upload_chunks, process_text_payload, upload_ack_key,
};
use rpc::{build_ws_request, normalize_ws_url};
use worker::spawn_worker;

const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const RPC_UNSUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(1);
const UPLOAD_CHUNK_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const UPLOAD_FRAME_MAGIC: &[u8; 4] = b"PSU1";

#[derive(Clone, Debug)]
pub struct GatewayWsConnectSpec {
    pub endpoint_id: String,
    pub endpoint_name: String,
    pub endpoint_kind: GatewayEndpointKind,
    pub address: String,
    pub auth_token: Option<String>,
    pub timings: GatewayWsTimings,
}

#[derive(Clone, Debug)]
pub enum GatewayWsEvent {
    Connecting {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
    },
    Connected {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        address: String,
    },
    Reconnecting {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        attempt: u32,
        delay_ms: u64,
        reason: String,
    },
    Disconnected {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        reason: String,
    },
    ConnectFailed {
        connection_id: u64,
        endpoint_id: String,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        error: String,
    },
    Notification {
        connection_id: u64,
        notification: GatewayNotification,
    },
}

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

enum GatewayWsCommand {
    Connect {
        connection_id: u64,
        spec: GatewayWsConnectSpec,
        initial_result_tx: Option<Sender<std::result::Result<(), String>>>,
        retry_initial_failure: bool,
    },
    Request {
        request_id: String,
        payload: String,
        response_tx: Sender<std::result::Result<JsonValue, String>>,
    },
    BinaryUploadChunk {
        upload_id: String,
        offset: u64,
        payload: Vec<u8>,
        response_tx: Sender<std::result::Result<SkillsUploadChunkAckNotification, String>>,
    },
    Disconnect,
    Shutdown,
}

#[cfg(test)]
mod tests;
