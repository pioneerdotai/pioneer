//! Codex app-server runtime implementation boundary.

#[path = "codex_schema.rs"]
mod schema;
pub use schema::{
    CODEX_MCP_SCHEMA_TRANSFORM_CONTRACT_VERSION, CodexMcpSchemaTransformer,
    CodexMcpSchemaTransformerError,
};

#[path = "codex_mcp_config.rs"]
mod mcp_config;
pub use mcp_config::{
    CodexManagedMcpConfigArtifact, CodexManagedMcpConfigError, CodexManagedMcpConfigInput,
    CodexManagedMcpConfigLimits, CodexManagedMcpSemanticInput, CodexManagedMcpToolIdentity,
    codex_config_value_fingerprint, codex_managed_mcp_semantic_restart_fingerprint,
    serialize_codex_managed_mcp_config,
};

use crate::driver::{
    JsonlRpcDecodeError, JsonlRpcError, JsonlRpcId, JsonlRpcIncomingMessage, JsonlRpcNotification,
    JsonlRpcRequest, JsonlRpcResponse, read_jsonl_rpc_message, write_jsonl_rpc_message,
    write_jsonl_rpc_request,
};
use crate::input::CLIRuntimeTurnInputItem;
use crate::process::{
    CLIAgentProcessSpawnConfig, SensitiveEnvironment, expand_home_path, spawn_cli_agent_process,
};
use pioneer_protocol::normalize_metadata_reasoning_effort;
use pioneer_runtime_events::{
    OrderedEventIngress, OrderedIngressClass, OrderedIngressConfig, OrderedIngressEvent,
    OrderedIngressOffer,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufRead, AsyncWrite, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 64;
const DEFAULT_INCOMING_QUEUE_CAPACITY: usize = 256;
const DEFAULT_SERVER_REQUEST_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const CODEX_SERVER_REQUEST_HANDLER_UNAVAILABLE_CODE: i64 = -32000;
const CODEX_SERVER_REQUEST_TIMEOUT_CODE: i64 = -32001;
const CODEX_SERVER_REQUEST_DUPLICATE_ID_CODE: i64 = -32600;
const LOGIN_SESSION_TIMEOUT: Duration = Duration::from_secs(600);
const CODEX_HOME_OVERLAY_POLICY_VERSION: u32 = 1;
const CODEX_SHARED_HOME_ENTRY_NAMES_V1: &[&str] = &[
    "sessions",
    "archived_sessions",
    "sqlite",
    "shell_snapshots",
    "skills",
];
const CODEX_PRIVATE_HOME_ENTRY_NAMES: &[&str] = &["auth.json", "models_cache.json"];
const CODEX_GENERATION_LOCAL_ENTRY_NAMES_V1: &[&str] = &["bootstrap", "log", "cache", "tmp"];
const CODEX_GENERATION_OVERLAY_MARKER: &str = ".pioneer-codex-overlay.json";
const CODEX_PHASE_00_CONTROLLED_CONFIG_V1: &[u8] = br#"[features]
apps = false
enable_mcp_apps = false
plugins = false
remote_plugin = false
skill_mcp_dependency_install = false
"#;
static CODEX_GENERATION_OVERLAY_NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct CodexJsonlRpcClient {
    command_tx: mpsc::Sender<CodexJsonlRpcCommand>,
    next_request_id: Arc<AtomicI64>,
    notification_rx: Arc<StdMutex<Option<mpsc::Receiver<CodexJsonlRpcNotificationEvent>>>>,
    server_request_rx: Arc<StdMutex<Option<mpsc::Receiver<CodexJsonlRpcServerRequest>>>>,
    diagnostic_rx: Arc<StdMutex<Option<mpsc::Receiver<CodexJsonlRpcClientDiagnostic>>>>,
}

impl CodexJsonlRpcClient {
    pub fn new<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncBufRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Self::new_with_channel_capacity(
            reader,
            writer,
            DEFAULT_INCOMING_QUEUE_CAPACITY,
            DEFAULT_INCOMING_QUEUE_CAPACITY,
            DEFAULT_COMMAND_QUEUE_CAPACITY,
        )
    }

    pub fn new_with_channel_capacity<R, W>(
        reader: R,
        writer: W,
        notification_capacity: usize,
        server_request_capacity: usize,
        diagnostic_capacity: usize,
    ) -> Self
    where
        R: AsyncBufRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Self::new_with_server_request_policy(
            reader,
            writer,
            notification_capacity,
            server_request_capacity,
            diagnostic_capacity,
            DEFAULT_SERVER_REQUEST_TIMEOUT,
        )
    }

    fn new_with_server_request_policy<R, W>(
        reader: R,
        writer: W,
        notification_capacity: usize,
        server_request_capacity: usize,
        diagnostic_capacity: usize,
        server_request_timeout: Duration,
    ) -> Self
    where
        R: AsyncBufRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let server_request_capacity = server_request_capacity.max(1);
        let server_request_timeout = if server_request_timeout.is_zero() {
            Duration::from_millis(1)
        } else {
            server_request_timeout
        };
        let (command_tx, command_rx) = mpsc::channel(DEFAULT_COMMAND_QUEUE_CAPACITY);
        let (incoming_tx, incoming_rx) = mpsc::channel(DEFAULT_INCOMING_QUEUE_CAPACITY);
        let (notification_tx, notification_rx) = mpsc::channel(notification_capacity.max(1));
        let (server_request_tx, server_request_rx) = mpsc::channel(server_request_capacity);
        let (diagnostic_tx, diagnostic_rx) = mpsc::channel(diagnostic_capacity.max(1));
        let notification_ingress = OrderedEventIngress::spawn(
            notification_tx,
            OrderedIngressConfig {
                flush_interval: CODEX_NOTIFICATION_PROGRESS_FLUSH_INTERVAL,
                max_pending_coalesced_keys: CODEX_NOTIFICATION_MAX_PENDING_PROGRESS_KEYS,
                max_pending_durable_events: CODEX_NOTIFICATION_MAX_PENDING_DURABLE_EVENTS,
            },
        );

        tokio::spawn(run_codex_jsonl_rpc_reader(reader, incoming_tx));
        tokio::spawn(run_codex_jsonl_rpc_worker(
            writer,
            command_rx,
            incoming_rx,
            CodexJsonlRpcEventSinks {
                notification_ingress,
                server_request_tx,
                diagnostic_tx,
            },
            server_request_capacity,
            server_request_timeout,
        ));

        Self {
            command_tx,
            next_request_id: Arc::new(AtomicI64::new(0)),
            notification_rx: Arc::new(StdMutex::new(Some(notification_rx))),
            server_request_rx: Arc::new(StdMutex::new(Some(server_request_rx))),
            diagnostic_rx: Arc::new(StdMutex::new(Some(diagnostic_rx))),
        }
    }

    pub async fn request<TResponse, TParams>(
        &self,
        method: &str,
        params: &TParams,
        timeout: Duration,
    ) -> Result<TResponse, CodexJsonlRpcClientError>
    where
        TResponse: DeserializeOwned,
        TParams: Serialize + ?Sized,
    {
        let params =
            serde_json::to_value(params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: method.to_owned(),
                message: error.to_string(),
            })?;
        let result = self.request_value(method, Some(params), timeout).await?;
        serde_json::from_value(result).map_err(|error| CodexJsonlRpcClientError::Decode {
            method: method.to_owned(),
            message: error.to_string(),
        })
    }

    pub async fn request_value(
        &self,
        method: &str,
        params: Option<JsonValue>,
        timeout: Duration,
    ) -> Result<JsonValue, CodexJsonlRpcClientError> {
        let id = self.next_request_id();
        self.request_value_with_id(id, method, params, timeout)
            .await
    }

    pub async fn notify(
        &self,
        method: &str,
        params: Option<JsonValue>,
    ) -> Result<(), CodexJsonlRpcClientError> {
        let notification = JsonlRpcNotification::new(method, params);
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CodexJsonlRpcCommand::Notify {
                notification,
                response_tx,
            })
            .await
            .map_err(|_| CodexJsonlRpcClientError::TransportClosed {
                message: "codex jsonl-rpc worker is closed".to_owned(),
            })?;

        response_rx
            .await
            .map_err(|_| CodexJsonlRpcClientError::TransportClosed {
                message: "codex jsonl-rpc notification channel closed".to_owned(),
            })?
    }

    pub async fn shutdown(&self) -> Result<(), CodexJsonlRpcClientError> {
        self.command_tx
            .send(CodexJsonlRpcCommand::Shutdown)
            .await
            .map_err(|_| CodexJsonlRpcClientError::TransportClosed {
                message: "codex jsonl-rpc worker is closed".to_owned(),
            })
    }

    pub fn take_notification_receiver(
        &self,
    ) -> Option<mpsc::Receiver<CodexJsonlRpcNotificationEvent>> {
        self.notification_rx
            .lock()
            .expect("notification receiver mutex should not be poisoned")
            .take()
    }

    pub fn take_server_request_receiver(
        &self,
    ) -> Option<mpsc::Receiver<CodexJsonlRpcServerRequest>> {
        self.server_request_rx
            .lock()
            .expect("server request receiver mutex should not be poisoned")
            .take()
    }

    pub fn take_diagnostic_receiver(
        &self,
    ) -> Option<mpsc::Receiver<CodexJsonlRpcClientDiagnostic>> {
        self.diagnostic_rx
            .lock()
            .expect("diagnostic receiver mutex should not be poisoned")
            .take()
    }

    pub async fn respond_to_server_request(
        &self,
        id: JsonlRpcId,
        result: JsonValue,
    ) -> Result<(), CodexJsonlRpcClientError> {
        self.answer_server_request(id, CodexJsonlRpcServerRequestAnswer::Result(result))
            .await
    }

    pub async fn fail_server_request(
        &self,
        id: JsonlRpcId,
        code: i64,
        message: impl Into<String>,
        data: Option<JsonValue>,
    ) -> Result<(), CodexJsonlRpcClientError> {
        self.answer_server_request(
            id,
            CodexJsonlRpcServerRequestAnswer::Error {
                code,
                message: message.into(),
                data,
            },
        )
        .await
    }

    fn next_request_id(&self) -> JsonlRpcId {
        JsonlRpcId::Number(self.next_request_id.fetch_add(1, Ordering::SeqCst))
    }

    async fn request_value_with_id(
        &self,
        id: JsonlRpcId,
        method: &str,
        params: Option<JsonValue>,
        timeout: Duration,
    ) -> Result<JsonValue, CodexJsonlRpcClientError> {
        let request = JsonlRpcRequest::new(id.clone(), method, params);
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx
            .send(CodexJsonlRpcCommand::Request {
                request,
                response_tx,
            })
            .await
            .map_err(|_| CodexJsonlRpcClientError::TransportClosed {
                message: "codex jsonl-rpc worker is closed".to_owned(),
            })?;

        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CodexJsonlRpcClientError::TransportClosed {
                message: "codex jsonl-rpc response channel closed".to_owned(),
            }),
            Err(_) => {
                let _ = self
                    .command_tx
                    .send(CodexJsonlRpcCommand::Cancel { id: id.clone() })
                    .await;
                Err(CodexJsonlRpcClientError::RequestTimeout {
                    id,
                    method: method.to_owned(),
                    timeout,
                })
            }
        }
    }

    async fn answer_server_request(
        &self,
        id: JsonlRpcId,
        answer: CodexJsonlRpcServerRequestAnswer,
    ) -> Result<(), CodexJsonlRpcClientError> {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(CodexJsonlRpcCommand::RespondToServerRequest {
                id,
                answer,
                response_tx,
            })
            .await
            .map_err(|_| CodexJsonlRpcClientError::TransportClosed {
                message: "codex jsonl-rpc worker is closed".to_owned(),
            })?;

        response_rx
            .await
            .map_err(|_| CodexJsonlRpcClientError::TransportClosed {
                message: "codex jsonl-rpc response channel closed".to_owned(),
            })?
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CodexJsonlRpcClientError {
    Native(CodexJsonlRpcNativeError),
    RequestTimeout {
        id: JsonlRpcId,
        method: String,
        timeout: Duration,
    },
    TransportClosed {
        message: String,
    },
    Decode {
        method: String,
        message: String,
    },
    Encode {
        method: String,
        message: String,
    },
    DuplicateRequestId {
        id: JsonlRpcId,
    },
    ServerRequestNotPending {
        id: JsonlRpcId,
    },
}

impl fmt::Display for CodexJsonlRpcClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => write!(
                f,
                "codex app-server error {}: {}",
                error.code, error.message
            ),
            Self::RequestTimeout {
                method, timeout, ..
            } => write!(
                f,
                "timed out waiting for codex `{method}` response after {}ms",
                timeout.as_millis()
            ),
            Self::TransportClosed { message } => f.write_str(message),
            Self::Decode { method, message } => {
                write!(f, "failed to decode codex `{method}` response: {message}")
            }
            Self::Encode { method, message } => {
                write!(f, "failed to encode codex `{method}` request: {message}")
            }
            Self::DuplicateRequestId { id } => {
                write!(f, "duplicate codex jsonl-rpc request id `{id}`")
            }
            Self::ServerRequestNotPending { id } => {
                write!(f, "codex server request `{id}` is not pending")
            }
        }
    }
}

impl Error for CodexJsonlRpcClientError {}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexJsonlRpcNativeError {
    pub id: Option<JsonlRpcId>,
    pub code: i64,
    pub message: String,
    pub data: Option<JsonValue>,
}

#[derive(Clone)]
pub struct CodexAppServerClient {
    rpc: CodexJsonlRpcClient,
}

impl CodexAppServerClient {
    pub fn new(rpc: CodexJsonlRpcClient) -> Self {
        Self { rpc }
    }

    pub fn rpc(&self) -> &CodexJsonlRpcClient {
        &self.rpc
    }

    pub async fn initialize(
        &self,
        timeout: Duration,
    ) -> Result<CodexInitializeSnapshot, CodexJsonlRpcClientError> {
        let params = CodexInitializeParams::pioneer_desktop();
        let params_value =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "initialize".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("initialize", Some(params_value), timeout)
            .await?;
        let response: CodexInitializeResponse =
            serde_json::from_value(result.clone()).map_err(|error| {
                CodexJsonlRpcClientError::Decode {
                    method: "initialize".to_owned(),
                    message: error.to_string(),
                }
            })?;

        self.rpc
            .notify("initialized", Some(serde_json::json!({})))
            .await?;
        Ok(response.into_snapshot(result))
    }

    pub async fn account_read(
        &self,
        timeout: Duration,
    ) -> Result<CodexAccountReadResponse, CodexJsonlRpcClientError> {
        self.rpc
            .request("account/read", &serde_json::json!({}), timeout)
            .await
    }

    /// Reads merged configuration without touching MCP status or starting a
    /// native thread. The snapshot retains only bounded isolation evidence.
    pub async fn config_read(
        &self,
        cwd: &str,
        timeout: Duration,
    ) -> Result<CodexConfigReadSnapshot, CodexJsonlRpcClientError> {
        if cwd.trim().is_empty() {
            return Err(CodexJsonlRpcClientError::Encode {
                method: "config/read".to_owned(),
                message: "config/read cwd must not be empty".to_owned(),
            });
        }
        let params = CodexConfigReadParams {
            cwd: cwd.to_owned(),
            include_layers: true,
        };
        let response: CodexConfigReadWireResponse =
            self.rpc.request("config/read", &params, timeout).await?;
        decode_codex_config_read_response(response).map_err(|message| {
            CodexJsonlRpcClientError::Decode {
                method: "config/read".to_owned(),
                message,
            }
        })
    }

    pub async fn list_models_page(
        &self,
        cursor: Option<String>,
        timeout: Duration,
    ) -> Result<CodexModelListPage, CodexJsonlRpcClientError> {
        let params = CodexModelListParams { cursor };
        let response: CodexModelListResponse =
            self.rpc.request("model/list", &params, timeout).await?;
        Ok(response.into_page())
    }

    pub async fn list_all_models(
        &self,
        timeout: Duration,
    ) -> Result<Vec<CodexModelSnapshot>, CodexJsonlRpcClientError> {
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();
        let mut models = Vec::new();

        loop {
            let page = self.list_models_page(cursor.clone(), timeout).await?;
            models.extend(page.models);

            let Some(next_cursor) = page
                .next_cursor
                .as_deref()
                .map(str::trim)
                .filter(|cursor| !cursor.is_empty())
                .map(str::to_owned)
            else {
                break;
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(CodexJsonlRpcClientError::Decode {
                    method: "model/list".to_owned(),
                    message: format!("Codex model/list returned duplicate cursor `{next_cursor}`"),
                });
            }
            cursor = Some(next_cursor);
        }

        Ok(models)
    }

    pub async fn thread_read_raw(
        &self,
        thread_id: &str,
        include_turns: bool,
        timeout: Duration,
    ) -> Result<JsonValue, CodexJsonlRpcClientError> {
        self.rpc
            .request_value(
                "thread/read",
                Some(json!({
                    "threadId": thread_id,
                    "includeTurns": include_turns,
                })),
                timeout,
            )
            .await
    }

    pub async fn thread_read_metadata(
        &self,
        thread_id: &str,
        timeout: Duration,
    ) -> Result<CodexThreadMetadataSnapshot, CodexJsonlRpcClientError> {
        let raw = self.thread_read_raw(thread_id, false, timeout).await?;
        let response: CodexThreadReadMetadataResponse =
            serde_json::from_value(raw).map_err(|error| CodexJsonlRpcClientError::Decode {
                method: "thread/read".to_owned(),
                message: error.to_string(),
            })?;
        if response.thread.id != thread_id {
            return Err(CodexJsonlRpcClientError::Decode {
                method: "thread/read".to_owned(),
                message: "thread/read returned a different native thread id".to_owned(),
            });
        }
        if response.thread.cwd.trim().is_empty()
            || !Path::new(response.thread.cwd.as_str()).is_absolute()
        {
            return Err(CodexJsonlRpcClientError::Decode {
                method: "thread/read".to_owned(),
                message: "thread/read returned an invalid persisted cwd".to_owned(),
            });
        }
        Ok(CodexThreadMetadataSnapshot {
            native_thread_id: response.thread.id,
            cwd: response.thread.cwd,
        })
    }

    pub async fn thread_start(
        &self,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot, CodexJsonlRpcClientError> {
        let params =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "thread/start".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("thread/start", Some(params), timeout)
            .await?;
        decode_codex_thread_open_response("thread/start", result)
    }

    pub async fn thread_resume(
        &self,
        thread_id: impl Into<String>,
        params: CodexThreadStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot, CodexJsonlRpcClientError> {
        let params = serde_json::to_value(CodexThreadResumeParams {
            thread_id: thread_id.into(),
            start: params,
        })
        .map_err(|error| CodexJsonlRpcClientError::Encode {
            method: "thread/resume".to_owned(),
            message: error.to_string(),
        })?;
        let result = self
            .rpc
            .request_value("thread/resume", Some(params), timeout)
            .await?;
        decode_codex_thread_open_response("thread/resume", result)
    }

    pub async fn thread_compact_start(
        &self,
        params: CodexThreadCompactStartParams,
        timeout: Duration,
    ) -> Result<CodexThreadCompactStartSnapshot, CodexJsonlRpcClientError> {
        let native_thread_id = params.thread_id.clone();
        let params =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "thread/compact/start".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("thread/compact/start", Some(params), timeout)
            .await?;
        Ok(CodexThreadCompactStartSnapshot {
            native_thread_id,
            raw: result,
        })
    }

    pub async fn turn_start(
        &self,
        params: CodexTurnStartParams,
        timeout: Duration,
    ) -> Result<CodexTurnStartSnapshot, CodexJsonlRpcClientError> {
        let native_thread_id = params.thread_id.clone();
        let params =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "turn/start".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("turn/start", Some(params), timeout)
            .await?;
        decode_codex_turn_start_response("turn/start", native_thread_id.as_str(), result)
    }

    pub async fn thread_name_set(
        &self,
        params: CodexThreadNameSetParams,
        timeout: Duration,
    ) -> Result<CodexThreadNameSetSnapshot, CodexJsonlRpcClientError> {
        let native_thread_id = params.thread_id.clone();
        let params =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "thread/name/set".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("thread/name/set", Some(params), timeout)
            .await?;
        Ok(CodexThreadNameSetSnapshot {
            native_thread_id,
            raw: result,
        })
    }

    pub async fn thread_fork(
        &self,
        params: CodexThreadForkParams,
        timeout: Duration,
    ) -> Result<CodexThreadOpenSnapshot, CodexJsonlRpcClientError> {
        let params =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "thread/fork".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("thread/fork", Some(params), timeout)
            .await?;
        decode_codex_thread_open_response("thread/fork", result)
    }

    pub async fn review_start(
        &self,
        params: CodexReviewStartParams,
        timeout: Duration,
    ) -> Result<CodexReviewStartSnapshot, CodexJsonlRpcClientError> {
        let native_thread_id = params.thread_id.clone();
        let params =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "review/start".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("review/start", Some(params), timeout)
            .await?;
        decode_codex_review_start_response("review/start", native_thread_id.as_str(), result)
    }

    pub async fn interrupt_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        timeout: Duration,
    ) -> Result<JsonValue, CodexJsonlRpcClientError> {
        self.rpc
            .request_value(
                "turn/interrupt",
                Some(json!({
                    "threadId": thread_id,
                    "turnId": turn_id
                })),
                timeout,
            )
            .await
    }

    pub async fn turn_steer(
        &self,
        params: CodexTurnSteerParams,
        timeout: Duration,
    ) -> Result<CodexTurnSteerSnapshot, CodexJsonlRpcClientError> {
        let native_thread_id = params.thread_id.clone();
        let params =
            serde_json::to_value(&params).map_err(|error| CodexJsonlRpcClientError::Encode {
                method: "turn/steer".to_owned(),
                message: error.to_string(),
            })?;
        let result = self
            .rpc
            .request_value("turn/steer", Some(params), timeout)
            .await?;
        let response: CodexTurnSteerResponse =
            serde_json::from_value(result.clone()).map_err(|error| {
                CodexJsonlRpcClientError::Decode {
                    method: "turn/steer".to_owned(),
                    message: error.to_string(),
                }
            })?;

        Ok(CodexTurnSteerSnapshot {
            native_thread_id,
            native_turn_id: response.turn_id,
            raw: result,
        })
    }

    pub async fn respond_command_approval(
        &self,
        id: JsonlRpcId,
        decision: CodexCommandApprovalDecision,
    ) -> Result<(), CodexJsonlRpcClientError> {
        self.rpc
            .respond_to_server_request(id, codex_command_approval_response(decision))
            .await
    }

    pub async fn login_start(
        &self,
        login_type: &str,
        timeout: Duration,
    ) -> Result<CodexLoginStartResponse, CodexJsonlRpcClientError> {
        let result = self
            .rpc
            .request_value(
                "account/login/start",
                Some(serde_json::json!({ "type": login_type })),
                timeout,
            )
            .await?;
        let response: CodexLoginStartResponse =
            serde_json::from_value(result.clone()).map_err(|error| {
                CodexJsonlRpcClientError::Decode {
                    method: "account/login/start".to_owned(),
                    message: error.to_string(),
                }
            })?;
        Ok(response.with_raw(result))
    }

    pub async fn login_cancel(
        &self,
        login_id: &str,
        timeout: Duration,
    ) -> Result<JsonValue, CodexJsonlRpcClientError> {
        self.rpc
            .request_value(
                "account/login/cancel",
                Some(serde_json::json!({ "loginId": login_id })),
                timeout,
            )
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexInitializeParams {
    pub client_info: CodexClientInfo,
    pub capabilities: CodexInitializeCapabilities,
}

impl CodexInitializeParams {
    pub fn pioneer_desktop() -> Self {
        Self {
            client_info: CodexClientInfo {
                name: "pioneer_desktop".to_owned(),
                title: "Pioneer".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
            capabilities: CodexInitializeCapabilities {
                experimental_api: true,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexClientInfo {
    pub name: String,
    pub title: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexInitializeCapabilities {
    pub experimental_api: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexInitializeResponse {
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub platform_family: Option<String>,
    #[serde(default)]
    pub platform_os: Option<String>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

impl CodexInitializeResponse {
    fn into_snapshot(self, raw: JsonValue) -> CodexInitializeSnapshot {
        CodexInitializeSnapshot {
            user_agent: self.user_agent,
            version: self.version,
            platform_family: self.platform_family,
            platform_os: self.platform_os,
            extra: self.extra,
            raw,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexInitializeSnapshot {
    pub user_agent: Option<String>,
    pub version: Option<String>,
    pub platform_family: Option<String>,
    pub platform_os: Option<String>,
    pub extra: BTreeMap<String, JsonValue>,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexConfigReadParams {
    pub cwd: String,
    pub include_layers: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexConfigReadSnapshot {
    pub effective: CodexConfigIsolationEvidence,
    pub layers: Vec<CodexConfigLayerIsolationEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexConfigIsolationEvidence {
    pub mcp_server_names: Vec<String>,
    pub mcp_servers_fingerprint: String,
    pub plugin_names: Vec<String>,
    pub marketplace_names: Vec<String>,
    pub app_names: Vec<String>,
    pub trusted_project_names: Vec<String>,
    pub isolation_features: CodexConfigIsolationFeatures,
}

impl CodexConfigIsolationEvidence {
    pub fn has_forbidden_extension_entries(&self) -> bool {
        !self.plugin_names.is_empty()
            || !self.marketplace_names.is_empty()
            || !self.app_names.is_empty()
            || !self.trusted_project_names.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexConfigIsolationFeatures {
    pub apps: bool,
    pub enable_mcp_apps: bool,
    pub plugins: bool,
    pub remote_plugin: bool,
    pub skill_mcp_dependency_install: bool,
}

impl CodexConfigIsolationFeatures {
    pub fn all_disabled(self) -> bool {
        !self.apps
            && !self.enable_mcp_apps
            && !self.plugins
            && !self.remote_plugin
            && !self.skill_mcp_dependency_install
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexConfigLayerIsolationEvidence {
    pub source: CodexConfigLayerSourceKind,
    pub source_fingerprint: String,
    pub version: String,
    pub mcp_server_names: Vec<String>,
    pub mcp_servers_fingerprint: String,
    pub plugin_names: Vec<String>,
    pub marketplace_names: Vec<String>,
    pub app_names: Vec<String>,
    pub trusted_project_names: Vec<String>,
}

impl CodexConfigLayerIsolationEvidence {
    pub fn has_forbidden_entries(&self) -> bool {
        !self.mcp_server_names.is_empty()
            || !self.plugin_names.is_empty()
            || !self.marketplace_names.is_empty()
            || !self.app_names.is_empty()
            || !self.trusted_project_names.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CodexConfigLayerSourceKind {
    Mdm,
    System,
    EnterpriseManaged,
    User,
    Project,
    SessionFlags,
    LegacyManagedConfigTomlFromFile,
    LegacyManagedConfigTomlFromMdm,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexConfigReadWireResponse {
    config: JsonValue,
    layers: Option<Vec<CodexConfigLayerWire>>,
    origins: BTreeMap<String, CodexConfigLayerMetadataWire>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexConfigLayerWire {
    config: JsonValue,
    name: JsonValue,
    version: String,
    #[serde(default)]
    disabled_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexConfigLayerMetadataWire {
    name: JsonValue,
    version: String,
}

#[derive(Debug, Deserialize)]
struct CodexThreadReadMetadataResponse {
    thread: CodexThreadReadMetadataWire,
}

#[derive(Debug, Deserialize)]
struct CodexThreadReadMetadataWire {
    id: String,
    cwd: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexThreadMetadataSnapshot {
    pub native_thread_id: String,
    pub cwd: String,
}

fn decode_codex_config_read_response(
    response: CodexConfigReadWireResponse,
) -> Result<CodexConfigReadSnapshot, String> {
    const MAX_LAYERS: usize = 128;
    const MAX_ORIGINS: usize = 512;
    let layers = response
        .layers
        .ok_or_else(|| "config/read omitted requested layer evidence".to_owned())?;
    if layers.len() > MAX_LAYERS {
        return Err("config/read returned too many config layers".to_owned());
    }
    if response.origins.len() > MAX_ORIGINS {
        return Err("config/read returned too many config origins".to_owned());
    }
    for (key, metadata) in response.origins {
        validate_codex_config_evidence_text("origin key", key.as_str())?;
        validate_codex_config_evidence_text("origin version", metadata.version.as_str())?;
        let _ = decode_codex_config_layer_source(metadata.name)?;
    }

    let effective = decode_codex_config_isolation_evidence(&response.config, true)?;
    let mut decoded_layers = Vec::with_capacity(layers.len());
    let mut source_fingerprints = HashSet::with_capacity(layers.len());
    for layer in layers {
        validate_codex_config_evidence_text("layer version", layer.version.as_str())?;
        if let Some(disabled_reason) = layer.disabled_reason.as_deref() {
            validate_codex_config_evidence_text("layer disabled reason", disabled_reason)?;
        }
        let source = decode_codex_config_layer_source(layer.name.clone())?;
        let source_fingerprint = codex_safe_json_fingerprint(&layer.name)?;
        if !source_fingerprints.insert(source_fingerprint.clone()) {
            return Err("config/read returned duplicate config layer identities".to_owned());
        }
        let evidence = decode_codex_config_isolation_evidence(&layer.config, false)?;
        decoded_layers.push(CodexConfigLayerIsolationEvidence {
            source,
            source_fingerprint,
            version: layer.version,
            mcp_server_names: evidence.mcp_server_names,
            mcp_servers_fingerprint: evidence.mcp_servers_fingerprint,
            plugin_names: evidence.plugin_names,
            marketplace_names: evidence.marketplace_names,
            app_names: evidence.app_names,
            trusted_project_names: evidence.trusted_project_names,
        });
    }
    Ok(CodexConfigReadSnapshot {
        effective,
        layers: decoded_layers,
    })
}

fn decode_codex_config_isolation_evidence(
    config: &JsonValue,
    require_effective_fields: bool,
) -> Result<CodexConfigIsolationEvidence, String> {
    let config = config
        .as_object()
        .ok_or_else(|| "config/read config evidence is not an object".to_owned())?;
    let mcp_server_names = decode_codex_named_config_entries(
        config.get("mcp_servers"),
        "mcp_servers",
        require_effective_fields,
        false,
    )?;
    let mcp_servers_fingerprint = codex_config_value_fingerprint(
        config
            .get("mcp_servers")
            .unwrap_or(&JsonValue::Object(serde_json::Map::new())),
    )
    .map_err(|error| format!("config/read MCP evidence fingerprint failed: {error}"))?;
    let plugin_names = decode_codex_named_config_entries(
        config.get("plugins"),
        "plugins",
        require_effective_fields,
        false,
    )?;
    let marketplace_names = decode_codex_named_config_entries(
        config.get("marketplaces"),
        "marketplaces",
        require_effective_fields,
        false,
    )?;
    let app_names = decode_codex_named_config_entries(
        config.get("apps"),
        "apps",
        require_effective_fields,
        true,
    )?;
    let trusted_project_names = decode_codex_named_config_entries(
        config.get("projects"),
        "projects",
        require_effective_fields,
        true,
    )?;
    let isolation_features = if require_effective_fields {
        let features = config
            .get("features")
            .and_then(JsonValue::as_object)
            .ok_or_else(|| "config/read omitted effective feature evidence".to_owned())?;
        CodexConfigIsolationFeatures {
            apps: decode_codex_required_bool(features, "apps")?,
            enable_mcp_apps: decode_codex_required_bool(features, "enable_mcp_apps")?,
            plugins: decode_codex_required_bool(features, "plugins")?,
            remote_plugin: decode_codex_required_bool(features, "remote_plugin")?,
            skill_mcp_dependency_install: decode_codex_required_bool(
                features,
                "skill_mcp_dependency_install",
            )?,
        }
    } else {
        CodexConfigIsolationFeatures {
            apps: false,
            enable_mcp_apps: false,
            plugins: false,
            remote_plugin: false,
            skill_mcp_dependency_install: false,
        }
    };
    Ok(CodexConfigIsolationEvidence {
        mcp_server_names,
        mcp_servers_fingerprint,
        plugin_names,
        marketplace_names,
        app_names,
        trusted_project_names,
        isolation_features,
    })
}

fn decode_codex_named_config_entries(
    value: Option<&JsonValue>,
    field: &str,
    required: bool,
    allow_null: bool,
) -> Result<Vec<String>, String> {
    const MAX_ENTRIES: usize = 256;
    let Some(value) = value else {
        return required
            .then(|| Err(format!("config/read omitted effective `{field}` evidence")))
            .unwrap_or_else(|| Ok(Vec::new()));
    };
    if allow_null && value.is_null() {
        return Ok(Vec::new());
    }
    let entries = value
        .as_object()
        .ok_or_else(|| format!("config/read `{field}` evidence is not an object"))?;
    if entries.len() > MAX_ENTRIES {
        return Err(format!(
            "config/read `{field}` evidence has too many entries"
        ));
    }
    let mut names = entries.keys().cloned().collect::<Vec<_>>();
    for name in &names {
        validate_codex_config_evidence_text(field, name.as_str())?;
    }
    names.sort();
    Ok(names)
}

fn decode_codex_required_bool(
    values: &serde_json::Map<String, JsonValue>,
    field: &str,
) -> Result<bool, String> {
    values
        .get(field)
        .and_then(JsonValue::as_bool)
        .ok_or_else(|| format!("config/read omitted boolean feature `{field}` evidence"))
}

fn decode_codex_config_layer_source(
    value: JsonValue,
) -> Result<CodexConfigLayerSourceKind, String> {
    let source = value
        .as_object()
        .ok_or_else(|| "config/read layer source is not an object".to_owned())?;
    let source_type = source
        .get("type")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "config/read layer source has no string type".to_owned())?;
    let required_string = |field: &str| -> Result<(), String> {
        let value = source
            .get(field)
            .and_then(JsonValue::as_str)
            .ok_or_else(|| format!("config/read layer source `{source_type}` omitted `{field}`"))?;
        validate_codex_config_evidence_text("layer source", value)
    };
    match source_type {
        "mdm" => {
            required_string("domain")?;
            required_string("key")?;
            Ok(CodexConfigLayerSourceKind::Mdm)
        }
        "system" => {
            required_string("file")?;
            Ok(CodexConfigLayerSourceKind::System)
        }
        "enterpriseManaged" => {
            required_string("id")?;
            required_string("name")?;
            Ok(CodexConfigLayerSourceKind::EnterpriseManaged)
        }
        "user" => {
            required_string("file")?;
            if let Some(profile) = source.get("profile").filter(|value| !value.is_null()) {
                let profile = profile
                    .as_str()
                    .ok_or_else(|| "config/read user layer profile is not a string".to_owned())?;
                validate_codex_config_evidence_text("layer profile", profile)?;
            }
            Ok(CodexConfigLayerSourceKind::User)
        }
        "project" => {
            required_string("dotCodexFolder")?;
            Ok(CodexConfigLayerSourceKind::Project)
        }
        "sessionFlags" => Ok(CodexConfigLayerSourceKind::SessionFlags),
        "legacyManagedConfigTomlFromFile" => {
            required_string("file")?;
            Ok(CodexConfigLayerSourceKind::LegacyManagedConfigTomlFromFile)
        }
        "legacyManagedConfigTomlFromMdm" => {
            Ok(CodexConfigLayerSourceKind::LegacyManagedConfigTomlFromMdm)
        }
        _ => Err("config/read returned an unsupported config layer source".to_owned()),
    }
}

fn validate_codex_config_evidence_text(label: &str, value: &str) -> Result<(), String> {
    const MAX_TEXT_BYTES: usize = 1024;
    if value.is_empty() || value.len() > MAX_TEXT_BYTES || value.contains('\0') {
        return Err(format!("config/read returned invalid {label} evidence"));
    }
    Ok(())
}

fn codex_safe_json_fingerprint(value: &JsonValue) -> Result<String, String> {
    let serialized = serde_json::to_vec(value)
        .map_err(|_| "config/read layer source could not be fingerprinted".to_owned())?;
    Ok(codex_overlay_identity_component(
        "sha256",
        String::from_utf8_lossy(&serialized).as_ref(),
    ))
}

#[derive(Debug, Clone)]
pub struct CodexAccountProbeConfig {
    pub executable: String,
    pub home_path: String,
    pub shadow_home_path: Option<String>,
    pub cwd: Option<PathBuf>,
    pub home_dir: Option<PathBuf>,
    pub env: SensitiveEnvironment,
    pub initialize_timeout: Duration,
    pub request_timeout: Duration,
    pub shutdown_grace: Duration,
    pub stderr_ring_lines: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexAccountProbeSnapshot {
    pub status: CodexAccountProbeStatus,
    pub message: Option<String>,
    pub user_agent: Option<String>,
    pub version: Option<String>,
    pub account: Option<CodexAccountSnapshot>,
    pub requires_openai_auth: bool,
    pub diagnostics: Vec<CodexProbeDiagnostic>,
    pub stderr: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexAccountProbeStatus {
    Ready,
    NeedsAuth,
    MissingBinary,
    SpawnFailed,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexAccountSnapshot {
    pub authenticated: bool,
    pub account_id: Option<String>,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub plan: Option<String>,
    pub auth_method: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexProbeDiagnostic {
    pub level: CodexProbeDiagnosticLevel,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexProbeDiagnosticLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexHomeLayoutMode {
    Direct,
    AuthOverlay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHomeLayout {
    pub mode: CodexHomeLayoutMode,
    pub shared_home_path: PathBuf,
    pub effective_home_path: PathBuf,
    pub continuation_key: String,
}

/// Versioned, default-deny policy for state projected into a managed Codex
/// process home. New Codex root entries remain private unless a later policy
/// version names them explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexHomeOverlayPolicy {
    pub version: u32,
    pub shared_directory_names: &'static [&'static str],
    pub account_file_names: &'static [&'static str],
    pub local_directory_names: &'static [&'static str],
}

impl CodexHomeOverlayPolicy {
    pub const fn v1() -> Self {
        Self {
            version: CODEX_HOME_OVERLAY_POLICY_VERSION,
            shared_directory_names: CODEX_SHARED_HOME_ENTRY_NAMES_V1,
            account_file_names: CODEX_PRIVATE_HOME_ENTRY_NAMES,
            local_directory_names: CODEX_GENERATION_LOCAL_ENTRY_NAMES_V1,
        }
    }
}

/// Stable identity of one managed Codex home overlay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexGenerationOverlayIdentity {
    pub workspace_id: String,
    pub runtime_id: String,
    pub logical_thread_id: String,
    pub gateway_boot_id: String,
    pub process_generation: u64,
}

impl CodexGenerationOverlayIdentity {
    pub fn new(
        workspace_id: impl Into<String>,
        runtime_id: impl Into<String>,
        logical_thread_id: impl Into<String>,
        gateway_boot_id: impl Into<String>,
        process_generation: u64,
    ) -> Result<Self, CodexShadowHomeError> {
        let identity = Self {
            workspace_id: workspace_id.into(),
            runtime_id: runtime_id.into(),
            logical_thread_id: logical_thread_id.into(),
            gateway_boot_id: gateway_boot_id.into(),
            process_generation,
        };
        for (field, value) in [
            ("workspace_id", identity.workspace_id.as_str()),
            ("runtime_id", identity.runtime_id.as_str()),
            ("logical_thread_id", identity.logical_thread_id.as_str()),
            ("gateway_boot_id", identity.gateway_boot_id.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(CodexShadowHomeError::new(format!(
                    "Codex generation overlay {field} must not be empty"
                )));
            }
        }
        if process_generation == 0 {
            return Err(CodexShadowHomeError::new(
                "Codex generation overlay process_generation must be greater than zero",
            ));
        }
        Ok(identity)
    }
}

/// Filesystem contract for one materialized generation-scoped Codex home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexGenerationOverlayDescriptor {
    pub identity: CodexGenerationOverlayIdentity,
    pub policy_version: u32,
    pub managed_root_path: PathBuf,
    pub effective_home_path: PathBuf,
    pub shared_home_path: PathBuf,
    pub continuation_key: String,
    pub mcp_artifact: Option<CodexGenerationMcpArtifactDescriptor>,
    materialization_nonce: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexGenerationMcpArtifactDescriptor {
    pub config_path: PathBuf,
    pub config_artifact_digest: String,
    pub effective_mcp_servers_fingerprint: String,
    pub semantic_restart_fingerprint: String,
    pub bootstrap_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CodexGenerationOverlayMarker {
    policy_version: u32,
    identity: CodexGenerationOverlayIdentity,
    materialization_nonce: String,
    #[serde(default)]
    mcp_artifact: Option<CodexGenerationMcpArtifactMarker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CodexGenerationMcpArtifactMarker {
    config_artifact_digest: String,
    effective_mcp_servers_fingerprint: String,
    semantic_restart_fingerprint: String,
    bootstrap_relative_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexShadowHomeError {
    detail: String,
}

impl CodexShadowHomeError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    fn with_context(action: &str, path: &Path, error: impl fmt::Display) -> Self {
        Self::new(format!("{action} `{}` failed: {error}", path.display()))
    }

    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
}

impl fmt::Display for CodexShadowHomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail.as_str())
    }
}

impl Error for CodexShadowHomeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CodexShadowLinkState {
    Missing,
    NotSymlink,
    Symlink { target: PathBuf },
}

fn resolve_codex_home_layout(
    home_path: &str,
    shadow_home_path: Option<&str>,
    home_dir: Option<&Path>,
) -> Result<CodexHomeLayout, CodexShadowHomeError> {
    let shared_home_path = resolve_codex_home_path(home_path, home_dir)?;
    let continuation_key = format!("codex:home:{}", shared_home_path.display());
    let Some(shadow_home_path) = shadow_home_path
        .map(str::trim)
        .filter(|path| !path.is_empty())
    else {
        return Ok(CodexHomeLayout {
            mode: CodexHomeLayoutMode::Direct,
            effective_home_path: shared_home_path.clone(),
            shared_home_path,
            continuation_key,
        });
    };

    let effective_home_path = resolve_codex_home_path(shadow_home_path, home_dir)?;
    Ok(CodexHomeLayout {
        mode: CodexHomeLayoutMode::AuthOverlay,
        shared_home_path,
        effective_home_path,
        continuation_key,
    })
}

fn materialize_codex_shadow_home(layout: &CodexHomeLayout) -> Result<(), CodexShadowHomeError> {
    if layout.mode == CodexHomeLayoutMode::Direct {
        return Ok(());
    }
    if layout.shared_home_path == layout.effective_home_path {
        return Err(CodexShadowHomeError::new(
            "Codex shadow home path must be different from CODEX_HOME",
        ));
    }

    ensure_codex_directory_no_follow(layout.shared_home_path.as_path(), false)?;
    ensure_codex_directory_no_follow(layout.effective_home_path.as_path(), true)?;
    let policy = CodexHomeOverlayPolicy::v1();
    for entry_name in policy.shared_directory_names {
        create_dir_all_for_codex_home(layout.shared_home_path.join(entry_name).as_path())?;
    }

    for entry_name in policy.account_file_names {
        ensure_codex_private_shadow_entry(layout.effective_home_path.as_path(), entry_name)?;
    }

    for entry_name in policy.shared_directory_names {
        ensure_codex_shadow_symlink(
            layout.shared_home_path.as_path(),
            layout.effective_home_path.as_path(),
            entry_name,
        )?;
    }

    Ok(())
}

pub fn codex_app_server_process_config(
    config: &CodexAccountProbeConfig,
) -> Result<CLIAgentProcessSpawnConfig, CodexShadowHomeError> {
    let layout = resolve_codex_home_layout(
        config.home_path.as_str(),
        config.shadow_home_path.as_deref(),
        config.home_dir.as_deref(),
    )?;
    materialize_codex_shadow_home(&layout)?;

    let mut process_config = CLIAgentProcessSpawnConfig::codex_app_server(
        &config.executable,
        layout.effective_home_path.to_string_lossy().into_owned(),
    )
    .with_stderr_ring_lines(config.stderr_ring_lines);
    if let Some(cwd) = config.cwd.as_ref() {
        process_config = process_config.with_cwd(cwd);
    }
    if let Some(home_dir) = config.home_dir.as_ref() {
        process_config = process_config.with_home_dir(home_dir);
    }
    process_config = process_config.with_environment(&config.env);
    Ok(process_config)
}

/// Builds the app-server command for a managed, generation-scoped Codex home.
///
/// `fallback_managed_root` is used when the runtime has no configured auth
/// shadow home. A configured shadow home remains an account-material source;
/// its other entries are never projected into the generation overlay.
pub fn codex_generation_app_server_process_config(
    config: &CodexAccountProbeConfig,
    fallback_managed_root: &Path,
    identity: CodexGenerationOverlayIdentity,
) -> Result<(CLIAgentProcessSpawnConfig, CodexGenerationOverlayDescriptor), CodexShadowHomeError> {
    let shared_home_path =
        resolve_codex_home_path(config.home_path.as_str(), config.home_dir.as_deref())?;
    let configured_account_home = config
        .shadow_home_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(|path| resolve_codex_home_path(path, config.home_dir.as_deref()))
        .transpose()?;
    let account_home_path = configured_account_home
        .clone()
        .unwrap_or_else(|| shared_home_path.clone());
    let managed_root_path = match configured_account_home {
        Some(path) => path.join("pioneer-generation-overlays"),
        None => resolve_absolute_codex_path(fallback_managed_root)?,
    };

    let descriptor = materialize_codex_generation_overlay(
        shared_home_path,
        account_home_path,
        managed_root_path,
        identity,
    )?;
    let mut process_config = CLIAgentProcessSpawnConfig::codex_app_server(
        &config.executable,
        descriptor
            .effective_home_path
            .to_string_lossy()
            .into_owned(),
    )
    .with_env(
        "CODEX_SQLITE_HOME",
        descriptor.shared_home_path.to_string_lossy().into_owned(),
    )
    .with_stderr_ring_lines(config.stderr_ring_lines);
    if let Some(cwd) = config.cwd.as_ref() {
        process_config = process_config.with_cwd(cwd);
    }
    if let Some(home_dir) = config.home_dir.as_ref() {
        process_config = process_config.with_home_dir(home_dir);
    }
    process_config = process_config.with_environment(&config.env);
    Ok((process_config, descriptor))
}

pub fn cleanup_codex_generation_overlay(
    descriptor: &CodexGenerationOverlayDescriptor,
) -> Result<(), CodexShadowHomeError> {
    let managed_root = normalize_absolute_codex_path(descriptor.managed_root_path.as_path())?;
    let overlay_path = normalize_absolute_codex_path(descriptor.effective_home_path.as_path())?;
    if overlay_path == managed_root || !overlay_path.starts_with(managed_root.as_path()) {
        return Err(CodexShadowHomeError::new(
            "Codex generation overlay cleanup path escapes the managed root",
        ));
    }
    validate_existing_codex_directory_chain_no_follow(managed_root.as_path())?;
    validate_existing_codex_directory_chain_no_follow(
        overlay_path
            .parent()
            .ok_or_else(|| CodexShadowHomeError::new("Codex overlay has no parent directory"))?,
    )?;
    match fs::symlink_metadata(overlay_path.as_path()) {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(CodexShadowHomeError::with_context(
                "inspect Codex generation overlay for cleanup",
                overlay_path.as_path(),
                error,
            ));
        }
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(CodexShadowHomeError::new(format!(
                "Codex generation overlay cleanup target `{}` is not a real directory",
                overlay_path.display()
            )));
        }
        Ok(_) => {}
    }
    let marker = read_codex_generation_overlay_marker(
        overlay_path.join(CODEX_GENERATION_OVERLAY_MARKER).as_path(),
    )?;
    if marker.policy_version != descriptor.policy_version
        || marker.identity != descriptor.identity
        || marker.materialization_nonce != descriptor.materialization_nonce
    {
        return Err(CodexShadowHomeError::new(
            "Codex generation overlay cleanup refused a replacement path",
        ));
    }
    fs::remove_dir_all(overlay_path.as_path()).map_err(|error| {
        CodexShadowHomeError::with_context(
            "remove Codex generation overlay",
            overlay_path.as_path(),
            error,
        )
    })
}

/// Fence and stage one exact managed MCP config in an already-created,
/// generation-scoped overlay. The provider process must not be spawned until
/// this function returns successfully.
pub fn stage_codex_generation_mcp_config(
    descriptor: &mut CodexGenerationOverlayDescriptor,
    artifact: &CodexManagedMcpConfigArtifact,
    bootstrap_path: Option<&Path>,
) -> Result<CodexGenerationMcpArtifactDescriptor, CodexShadowHomeError> {
    validate_sha256_identity(
        artifact.artifact_digest.as_str(),
        "Codex MCP config artifact digest",
    )?;
    validate_sha256_identity(
        artifact.semantic_restart_fingerprint.as_str(),
        "Codex MCP semantic restart fingerprint",
    )?;
    if sha256_hex_bytes(artifact.config_toml.as_bytes()) != artifact.artifact_digest {
        return Err(CodexShadowHomeError::new(
            "Codex MCP config artifact digest does not match its contents",
        ));
    }
    let overlay_path = normalize_absolute_codex_path(descriptor.effective_home_path.as_path())?;
    let managed_root = normalize_absolute_codex_path(descriptor.managed_root_path.as_path())?;
    if overlay_path == managed_root || !overlay_path.starts_with(managed_root.as_path()) {
        return Err(CodexShadowHomeError::new(
            "Codex MCP config staging path escapes the managed root",
        ));
    }
    validate_existing_codex_directory_chain_no_follow(overlay_path.as_path())?;

    let bootstrap_path = match (artifact.enabled_tools.is_empty(), bootstrap_path) {
        (true, None) => None,
        (true, Some(_)) => {
            return Err(CodexShadowHomeError::new(
                "empty Codex MCP projection must not retain a bootstrap artifact",
            ));
        }
        (false, None) => {
            return Err(CodexShadowHomeError::new(
                "non-empty Codex MCP projection requires a bootstrap artifact",
            ));
        }
        (false, Some(path)) => {
            let path = normalize_absolute_codex_path(path)?;
            let bootstrap_root = overlay_path.join("bootstrap");
            if path == bootstrap_root || !path.starts_with(bootstrap_root.as_path()) {
                return Err(CodexShadowHomeError::new(
                    "Codex MCP bootstrap artifact is outside its generation overlay",
                ));
            }
            let parent = path.parent().ok_or_else(|| {
                CodexShadowHomeError::new("Codex MCP bootstrap artifact has no parent")
            })?;
            validate_existing_codex_directory_chain_no_follow(parent)?;
            let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| {
                CodexShadowHomeError::with_context(
                    "inspect Codex MCP bootstrap artifact",
                    path.as_path(),
                    error,
                )
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(CodexShadowHomeError::new(
                    "Codex MCP bootstrap artifact must be a real file",
                ));
            }
            Some(path)
        }
    };
    let bootstrap_relative_path = bootstrap_path
        .as_deref()
        .map(|path| {
            path.strip_prefix(overlay_path.as_path())
                .map(Path::to_path_buf)
                .map_err(|_| {
                    CodexShadowHomeError::new(
                        "Codex MCP bootstrap artifact escaped its generation overlay",
                    )
                })
        })
        .transpose()?;
    let staged_marker = CodexGenerationMcpArtifactMarker {
        config_artifact_digest: artifact.artifact_digest.clone(),
        effective_mcp_servers_fingerprint: artifact.effective_mcp_servers_fingerprint.clone(),
        semantic_restart_fingerprint: artifact.semantic_restart_fingerprint.clone(),
        bootstrap_relative_path,
    };

    let marker_path = overlay_path.join(CODEX_GENERATION_OVERLAY_MARKER);
    let mut marker = read_codex_generation_overlay_marker(marker_path.as_path())?;
    if marker.policy_version != descriptor.policy_version
        || marker.identity != descriptor.identity
        || marker.materialization_nonce != descriptor.materialization_nonce
    {
        return Err(CodexShadowHomeError::new(
            "Codex MCP config staging refused a stale generation overlay",
        ));
    }
    let config_path = overlay_path.join("config.toml");
    match marker.mcp_artifact.as_ref() {
        Some(existing) if existing != &staged_marker => {
            return Err(CodexShadowHomeError::new(
                "Codex MCP generation overlay already contains a different projection",
            ));
        }
        Some(_) => {
            if digest_codex_private_file(config_path.as_path())? != artifact.artifact_digest {
                return Err(CodexShadowHomeError::new(
                    "Codex MCP staged config was replaced after materialization",
                ));
            }
        }
        None => {
            overwrite_codex_private_file(config_path.as_path(), artifact.config_toml.as_bytes())?;
            marker.mcp_artifact = Some(staged_marker.clone());
            let serialized = serde_json::to_vec(&marker).map_err(|error| {
                CodexShadowHomeError::new(format!(
                    "serialize staged Codex generation marker failed: {error}"
                ))
            })?;
            overwrite_codex_private_file(marker_path.as_path(), serialized.as_slice())?;
        }
    }

    let staged = CodexGenerationMcpArtifactDescriptor {
        config_path,
        config_artifact_digest: artifact.artifact_digest.clone(),
        effective_mcp_servers_fingerprint: artifact.effective_mcp_servers_fingerprint.clone(),
        semantic_restart_fingerprint: artifact.semantic_restart_fingerprint.clone(),
        bootstrap_path,
    };
    descriptor.mcp_artifact = Some(staged.clone());
    Ok(staged)
}

/// Re-check the exact staged generation artifact immediately before a
/// pre-thread config attestation. This never follows a replacement overlay or
/// accepts a marker/config mismatch.
pub fn verify_codex_generation_mcp_config(
    descriptor: &CodexGenerationOverlayDescriptor,
) -> Result<CodexGenerationMcpArtifactDescriptor, CodexShadowHomeError> {
    let expected = descriptor.mcp_artifact.as_ref().ok_or_else(|| {
        CodexShadowHomeError::new("Codex generation overlay has no staged MCP artifact")
    })?;
    let overlay_path = normalize_absolute_codex_path(descriptor.effective_home_path.as_path())?;
    let managed_root = normalize_absolute_codex_path(descriptor.managed_root_path.as_path())?;
    if overlay_path == managed_root || !overlay_path.starts_with(managed_root.as_path()) {
        return Err(CodexShadowHomeError::new(
            "Codex MCP artifact verification path escapes the managed root",
        ));
    }
    validate_existing_codex_directory_chain_no_follow(overlay_path.as_path())?;
    let marker = read_codex_generation_overlay_marker(
        overlay_path.join(CODEX_GENERATION_OVERLAY_MARKER).as_path(),
    )?;
    if marker.policy_version != descriptor.policy_version
        || marker.identity != descriptor.identity
        || marker.materialization_nonce != descriptor.materialization_nonce
    {
        return Err(CodexShadowHomeError::new(
            "Codex MCP artifact verification refused a stale generation overlay",
        ));
    }
    let staged = marker.mcp_artifact.ok_or_else(|| {
        CodexShadowHomeError::new("Codex generation marker has no staged MCP artifact")
    })?;
    let bootstrap_path = staged
        .bootstrap_relative_path
        .as_deref()
        .map(|relative| overlay_path.join(relative));
    if staged.config_artifact_digest != expected.config_artifact_digest
        || staged.effective_mcp_servers_fingerprint != expected.effective_mcp_servers_fingerprint
        || staged.semantic_restart_fingerprint != expected.semantic_restart_fingerprint
        || bootstrap_path != expected.bootstrap_path
    {
        return Err(CodexShadowHomeError::new(
            "Codex MCP generation marker does not match the staged descriptor",
        ));
    }
    let config_path = overlay_path.join("config.toml");
    if config_path != expected.config_path
        || digest_codex_private_file(config_path.as_path())? != expected.config_artifact_digest
    {
        return Err(CodexShadowHomeError::new(
            "Codex MCP staged config failed pre-thread digest verification",
        ));
    }
    Ok(expected.clone())
}

fn materialize_codex_generation_overlay(
    shared_home_path: PathBuf,
    account_home_path: PathBuf,
    managed_root_path: PathBuf,
    identity: CodexGenerationOverlayIdentity,
) -> Result<CodexGenerationOverlayDescriptor, CodexShadowHomeError> {
    let shared_home_path = normalize_absolute_codex_path(shared_home_path.as_path())?;
    let account_home_path = normalize_absolute_codex_path(account_home_path.as_path())?;
    let managed_root_path = normalize_absolute_codex_path(managed_root_path.as_path())?;
    if managed_root_path.starts_with(shared_home_path.as_path()) {
        return Err(CodexShadowHomeError::new(
            "Codex generation overlay root must not be inside the shared state home",
        ));
    }

    ensure_codex_directory_no_follow(shared_home_path.as_path(), false)?;
    ensure_codex_directory_no_follow(account_home_path.as_path(), true)?;
    ensure_codex_directory_no_follow(managed_root_path.as_path(), true)?;

    let policy = CodexHomeOverlayPolicy::v1();
    let overlay_path = managed_root_path
        .join(format!("policy-v{}", policy.version))
        .join(codex_overlay_identity_component(
            "workspace",
            identity.workspace_id.as_str(),
        ))
        .join(codex_overlay_identity_component(
            "runtime",
            identity.runtime_id.as_str(),
        ))
        .join(codex_overlay_identity_component(
            "thread",
            identity.logical_thread_id.as_str(),
        ))
        .join(codex_overlay_identity_component(
            "boot",
            identity.gateway_boot_id.as_str(),
        ))
        .join(format!("generation-{:020}", identity.process_generation));
    let overlay_existed = match fs::symlink_metadata(overlay_path.as_path()) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(CodexShadowHomeError::new(format!(
                "Codex generation overlay path `{}` is not a real directory",
                overlay_path.display()
            )));
        }
        Ok(_) => true,
        Err(error) if error.kind() == ErrorKind::NotFound => false,
        Err(error) => {
            return Err(CodexShadowHomeError::with_context(
                "inspect Codex generation overlay",
                overlay_path.as_path(),
                error,
            ));
        }
    };
    ensure_codex_directory_no_follow(overlay_path.as_path(), true)?;

    let marker_path = overlay_path.join(CODEX_GENERATION_OVERLAY_MARKER);
    let marker = if overlay_existed {
        let marker =
            read_codex_generation_overlay_marker(marker_path.as_path()).map_err(|error| {
                CodexShadowHomeError::new(format!(
                    "stale or unmanaged Codex generation overlay `{}`: {error}",
                    overlay_path.display()
                ))
            })?;
        if marker.policy_version != policy.version || marker.identity != identity {
            return Err(CodexShadowHomeError::new(format!(
                "stale Codex generation overlay identity at `{}`",
                overlay_path.display()
            )));
        }
        marker
    } else {
        let marker = CodexGenerationOverlayMarker {
            policy_version: policy.version,
            identity: identity.clone(),
            materialization_nonce: codex_generation_overlay_nonce(&identity),
            mcp_artifact: None,
        };
        let serialized = serde_json::to_vec(&marker).map_err(|error| {
            CodexShadowHomeError::new(format!(
                "serialize Codex generation overlay marker failed: {error}"
            ))
        })?;
        write_new_codex_private_file(marker_path.as_path(), serialized.as_slice())?;
        marker
    };

    ensure_codex_private_file(
        overlay_path.join("config.toml").as_path(),
        CODEX_PHASE_00_CONTROLLED_CONFIG_V1,
    )?;
    for entry_name in policy.local_directory_names {
        ensure_codex_directory_no_follow(overlay_path.join(entry_name).as_path(), true)?;
    }
    for entry_name in policy.shared_directory_names {
        let source = shared_home_path.join(entry_name);
        ensure_codex_directory_no_follow(source.as_path(), false)?;
        ensure_codex_shadow_symlink(
            shared_home_path.as_path(),
            overlay_path.as_path(),
            entry_name,
        )?;
    }
    for entry_name in policy.account_file_names {
        materialize_codex_account_file(
            account_home_path.join(entry_name).as_path(),
            overlay_path.join(entry_name).as_path(),
        )?;
    }

    let mcp_artifact =
        marker
            .mcp_artifact
            .as_ref()
            .map(|artifact| CodexGenerationMcpArtifactDescriptor {
                config_path: overlay_path.join("config.toml"),
                config_artifact_digest: artifact.config_artifact_digest.clone(),
                effective_mcp_servers_fingerprint: artifact
                    .effective_mcp_servers_fingerprint
                    .clone(),
                semantic_restart_fingerprint: artifact.semantic_restart_fingerprint.clone(),
                bootstrap_path: artifact
                    .bootstrap_relative_path
                    .as_deref()
                    .map(|path| overlay_path.join(path)),
            });
    Ok(CodexGenerationOverlayDescriptor {
        identity,
        policy_version: policy.version,
        managed_root_path,
        effective_home_path: overlay_path,
        continuation_key: format!("codex:home:{}", shared_home_path.display()),
        shared_home_path,
        mcp_artifact,
        materialization_nonce: marker.materialization_nonce,
    })
}

fn codex_overlay_identity_component(label: &str, value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(label.len() + 1 + digest.len() * 2);
    encoded.push_str(label);
    encoded.push('-');
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn codex_generation_overlay_nonce(identity: &CodexGenerationOverlayIdentity) -> String {
    let counter = CODEX_GENERATION_OVERLAY_NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let input = format!(
        "{}:{}:{}:{}:{}:{nanos}:{}:{counter}",
        identity.workspace_id,
        identity.runtime_id,
        identity.logical_thread_id,
        identity.gateway_boot_id,
        identity.process_generation,
        std::process::id()
    );
    codex_overlay_identity_component("materialization", input.as_str())
}

fn resolve_codex_home_path(
    raw: &str,
    home_dir: Option<&Path>,
) -> Result<PathBuf, CodexShadowHomeError> {
    let expanded = expand_home_path(raw, home_dir).map_err(|error| {
        CodexShadowHomeError::new(format!("invalid Codex home path: {error:#}"))
    })?;
    if expanded.is_absolute() {
        return Ok(expanded);
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(expanded))
        .map_err(|error| {
            CodexShadowHomeError::new(format!("resolve current directory failed: {error}"))
        })
}

fn resolve_absolute_codex_path(path: &Path) -> Result<PathBuf, CodexShadowHomeError> {
    if path.is_absolute() {
        return normalize_absolute_codex_path(path);
    }
    let current_dir = std::env::current_dir().map_err(|error| {
        CodexShadowHomeError::new(format!("resolve current directory failed: {error}"))
    })?;
    normalize_absolute_codex_path(current_dir.join(path).as_path())
}

fn normalize_absolute_codex_path(path: &Path) -> Result<PathBuf, CodexShadowHomeError> {
    if !path.is_absolute() {
        return Err(CodexShadowHomeError::new(format!(
            "Codex managed path `{}` must be absolute",
            path.display()
        )));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                return Err(CodexShadowHomeError::new(format!(
                    "Codex managed path `{}` contains a parent traversal",
                    path.display()
                )));
            }
        }
    }
    Ok(normalized)
}

fn ensure_codex_directory_no_follow(
    path: &Path,
    owner_only: bool,
) -> Result<bool, CodexShadowHomeError> {
    let path = resolve_absolute_codex_path(path)?;
    let mut current = PathBuf::new();
    let mut created_leaf = false;
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => {
                current.push(component.as_os_str());
                continue;
            }
            Component::CurDir => continue,
            Component::ParentDir => unreachable!("path was normalized above"),
            Component::Normal(part) => current.push(part),
        }
        loop {
            match fs::symlink_metadata(current.as_path()) {
                Ok(metadata)
                    if metadata.file_type().is_symlink()
                        && !codex_trusted_platform_root_symlink(current.as_path()) =>
                {
                    return Err(CodexShadowHomeError::new(format!(
                        "Codex managed directory `{}` must not traverse a symlink",
                        current.display()
                    )));
                }
                Ok(metadata)
                    if metadata.file_type().is_symlink()
                        && codex_trusted_platform_root_symlink(current.as_path()) =>
                {
                    break;
                }
                Ok(metadata) if metadata.is_dir() => break,
                Ok(_) => {
                    return Err(CodexShadowHomeError::new(format!(
                        "Codex managed directory component `{}` is not a directory",
                        current.display()
                    )));
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    match fs::create_dir(current.as_path()) {
                        Ok(()) => {
                            if current == path {
                                created_leaf = true;
                            }
                            break;
                        }
                        Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                        Err(error) => {
                            return Err(CodexShadowHomeError::with_context(
                                "create Codex managed directory",
                                current.as_path(),
                                error,
                            ));
                        }
                    }
                }
                Err(error) => {
                    return Err(CodexShadowHomeError::with_context(
                        "inspect Codex managed directory",
                        current.as_path(),
                        error,
                    ));
                }
            }
        }
    }
    if owner_only {
        set_codex_owner_only_directory_permissions(path.as_path())?;
    }
    Ok(created_leaf)
}

fn validate_existing_codex_directory_chain_no_follow(
    path: &Path,
) -> Result<(), CodexShadowHomeError> {
    let path = resolve_absolute_codex_path(path)?;
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => {
                current.push(component.as_os_str());
                continue;
            }
            Component::CurDir => continue,
            Component::ParentDir => unreachable!("path was normalized above"),
            Component::Normal(part) => current.push(part),
        }
        let metadata = fs::symlink_metadata(current.as_path()).map_err(|error| {
            CodexShadowHomeError::with_context(
                "validate Codex managed directory chain",
                current.as_path(),
                error,
            )
        })?;
        if (metadata.file_type().is_symlink()
            && !codex_trusted_platform_root_symlink(current.as_path()))
            || (!metadata.file_type().is_symlink() && !metadata.is_dir())
        {
            return Err(CodexShadowHomeError::new(format!(
                "Codex managed directory chain `{}` is not a real directory",
                current.display()
            )));
        }
    }
    Ok(())
}

/// macOS exposes a small set of immutable system roots as compatibility
/// symlinks (for example `/var -> /private/var`). These precede every
/// application-owned component and are not part of the managed Pioneer tree.
/// All other symlinks, including a configured managed root, remain rejected.
fn codex_trusted_platform_root_symlink(path: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        if !matches!(path.to_str(), Some("/var" | "/tmp" | "/etc")) {
            return false;
        }
        return fs::canonicalize(path)
            .ok()
            .and_then(|resolved| fs::metadata(resolved).ok())
            .is_some_and(|metadata| metadata.is_dir());
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        false
    }
}

fn ensure_codex_private_file(
    path: &Path,
    initial_contents: &[u8],
) -> Result<(), CodexShadowHomeError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(CodexShadowHomeError::new(format!(
                "Codex private file `{}` must be a real file",
                path.display()
            )));
        }
        Ok(_) => set_codex_owner_only_file_permissions(path),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            write_new_codex_private_file(path, initial_contents)
        }
        Err(error) => Err(CodexShadowHomeError::with_context(
            "inspect Codex private file",
            path,
            error,
        )),
    }
}

fn overwrite_codex_private_file(path: &Path, contents: &[u8]) -> Result<(), CodexShadowHomeError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        CodexShadowHomeError::with_context("inspect Codex private file for staging", path, error)
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CodexShadowHomeError::new(format!(
            "Codex private file `{}` must be a real file",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|error| {
        CodexShadowHomeError::with_context("open Codex private file for staging", path, error)
    })?;
    file.write_all(contents).map_err(|error| {
        CodexShadowHomeError::with_context("write Codex private file for staging", path, error)
    })?;
    file.sync_all().map_err(|error| {
        CodexShadowHomeError::with_context("sync Codex private file for staging", path, error)
    })?;
    set_codex_owner_only_file_permissions(path)
}

fn digest_codex_private_file(path: &Path) -> Result<String, CodexShadowHomeError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        CodexShadowHomeError::with_context("inspect staged Codex private file", path, error)
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err(CodexShadowHomeError::new(format!(
            "staged Codex private file `{}` is invalid",
            path.display()
        )));
    }
    let mut contents = Vec::with_capacity(metadata.len() as usize);
    open_codex_file_no_follow(path)?
        .take(1024 * 1024 + 1)
        .read_to_end(&mut contents)
        .map_err(|error| {
            CodexShadowHomeError::with_context("read staged Codex private file", path, error)
        })?;
    Ok(sha256_hex_bytes(contents.as_slice()))
}

fn validate_sha256_identity(value: &str, label: &str) -> Result<(), CodexShadowHomeError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CodexShadowHomeError::new(format!("invalid {label}")));
    }
    Ok(())
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn write_new_codex_private_file(path: &Path, contents: &[u8]) -> Result<(), CodexShadowHomeError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|error| {
        CodexShadowHomeError::with_context("create Codex private file", path, error)
    })?;
    file.write_all(contents).map_err(|error| {
        CodexShadowHomeError::with_context("write Codex private file", path, error)
    })?;
    file.sync_all().map_err(|error| {
        CodexShadowHomeError::with_context("sync Codex private file", path, error)
    })?;
    set_codex_owner_only_file_permissions(path)
}

fn open_codex_file_no_follow(path: &Path) -> Result<File, CodexShadowHomeError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .map_err(|error| CodexShadowHomeError::with_context("open Codex managed file", path, error))
}

fn read_codex_generation_overlay_marker(
    path: &Path,
) -> Result<CodexGenerationOverlayMarker, CodexShadowHomeError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        CodexShadowHomeError::with_context("inspect Codex generation overlay marker", path, error)
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CodexShadowHomeError::new(format!(
            "Codex generation overlay marker `{}` must be a real file",
            path.display()
        )));
    }
    let mut serialized = Vec::new();
    open_codex_file_no_follow(path)?
        .take(64 * 1024)
        .read_to_end(&mut serialized)
        .map_err(|error| {
            CodexShadowHomeError::with_context("read Codex generation overlay marker", path, error)
        })?;
    serde_json::from_slice(serialized.as_slice()).map_err(|error| {
        CodexShadowHomeError::new(format!(
            "decode Codex generation overlay marker `{}` failed: {error}",
            path.display()
        ))
    })
}

fn materialize_codex_account_file(
    source: &Path,
    destination: &Path,
) -> Result<(), CodexShadowHomeError> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(CodexShadowHomeError::new(format!(
                "Codex account file destination `{}` must be a real file",
                destination.display()
            )));
        }
        Ok(_) => return set_codex_owner_only_file_permissions(destination),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(CodexShadowHomeError::with_context(
                "inspect Codex account file destination",
                destination,
                error,
            ));
        }
    }
    let source_metadata = match fs::symlink_metadata(source) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(CodexShadowHomeError::with_context(
                "inspect Codex account file source",
                source,
                error,
            ));
        }
    };
    if source_metadata.file_type().is_symlink() || !source_metadata.is_file() {
        return Err(CodexShadowHomeError::new(format!(
            "Codex account file source `{}` must be a real file",
            source.display()
        )));
    }
    let mut source_file = open_codex_file_no_follow(source)?;
    let mut destination_options = OpenOptions::new();
    destination_options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        destination_options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW);
    }
    let mut destination_file = destination_options.open(destination).map_err(|error| {
        CodexShadowHomeError::with_context(
            "create Codex account materialization",
            destination,
            error,
        )
    })?;
    std::io::copy(&mut source_file, &mut destination_file).map_err(|error| {
        CodexShadowHomeError::with_context("materialize Codex account file", destination, error)
    })?;
    destination_file.sync_all().map_err(|error| {
        CodexShadowHomeError::with_context("sync Codex account file", destination, error)
    })?;
    set_codex_owner_only_file_permissions(destination)
}

#[cfg(unix)]
fn set_codex_owner_only_directory_permissions(path: &Path) -> Result<(), CodexShadowHomeError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
        CodexShadowHomeError::with_context("set Codex directory permissions", path, error)
    })
}

#[cfg(not(unix))]
fn set_codex_owner_only_directory_permissions(_path: &Path) -> Result<(), CodexShadowHomeError> {
    Ok(())
}

#[cfg(unix)]
fn set_codex_owner_only_file_permissions(path: &Path) -> Result<(), CodexShadowHomeError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|error| {
        CodexShadowHomeError::with_context("set Codex file permissions", path, error)
    })
}

#[cfg(not(unix))]
fn set_codex_owner_only_file_permissions(_path: &Path) -> Result<(), CodexShadowHomeError> {
    Ok(())
}

fn create_dir_all_for_codex_home(path: &Path) -> Result<(), CodexShadowHomeError> {
    ensure_codex_directory_no_follow(path, false).map(|_| ())
}

fn ensure_codex_private_shadow_entry(
    shadow_home_path: &Path,
    entry_name: &str,
) -> Result<(), CodexShadowHomeError> {
    let private_path = shadow_home_path.join(entry_name);
    if matches!(
        read_codex_shadow_link_state(private_path.as_path())?,
        CodexShadowLinkState::Symlink { .. }
    ) {
        return Err(CodexShadowHomeError::new(format!(
            "Codex shadow private file `{}` must be a real file, not a symlink",
            private_path.display()
        )));
    }
    Ok(())
}

fn ensure_codex_shadow_symlink(
    shared_home_path: &Path,
    shadow_home_path: &Path,
    entry_name: &str,
) -> Result<(), CodexShadowHomeError> {
    let target = shared_home_path.join(entry_name);
    let link = shadow_home_path.join(entry_name);
    let target_metadata = fs::symlink_metadata(target.as_path()).map_err(|error| {
        CodexShadowHomeError::with_context(
            "inspect Codex shared home allowlist target",
            target.as_path(),
            error,
        )
    })?;
    if target_metadata.file_type().is_symlink() || !target_metadata.is_dir() {
        return Err(CodexShadowHomeError::new(format!(
            "Codex shared home allowlist target `{}` must be a real directory",
            target.display()
        )));
    }
    match read_codex_shadow_link_state(link.as_path())? {
        CodexShadowLinkState::Missing => {
            create_codex_shadow_symlink(target.as_path(), link.as_path())
        }
        CodexShadowLinkState::NotSymlink => Err(CodexShadowHomeError::new(format!(
            "Cannot create Codex shadow home because `{}` already exists and is not a symlink",
            link.display()
        ))),
        CodexShadowLinkState::Symlink {
            target: existing_target,
        } => {
            let resolved_existing = if existing_target.is_absolute() {
                existing_target
            } else {
                link.parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(existing_target)
            };
            if resolved_existing == target {
                return Ok(());
            }
            Err(CodexShadowHomeError::new(format!(
                "Cannot create Codex shadow home because `{}` points outside the allowlisted target",
                link.display()
            )))
        }
    }
}

fn read_codex_shadow_link_state(path: &Path) -> Result<CodexShadowLinkState, CodexShadowHomeError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let target = fs::read_link(path).map_err(|error| {
                CodexShadowHomeError::with_context("read Codex shadow home symlink", path, error)
            })?;
            Ok(CodexShadowLinkState::Symlink { target })
        }
        Ok(_) => Ok(CodexShadowLinkState::NotSymlink),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(CodexShadowLinkState::Missing),
        Err(error) => Err(CodexShadowHomeError::with_context(
            "inspect Codex shadow home entry",
            path,
            error,
        )),
    }
}

#[cfg(unix)]
fn create_codex_shadow_symlink(target: &Path, link: &Path) -> Result<(), CodexShadowHomeError> {
    std::os::unix::fs::symlink(target, link).map_err(|error| {
        CodexShadowHomeError::with_context("create Codex shadow home symlink", link, error)
    })
}

#[cfg(windows)]
fn create_codex_shadow_symlink(target: &Path, link: &Path) -> Result<(), CodexShadowHomeError> {
    let result = if target.is_dir() {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        std::os::windows::fs::symlink_file(target, link)
    };
    result.map_err(|error| {
        CodexShadowHomeError::with_context("create Codex shadow home symlink", link, error)
    })
}

pub struct CodexProbe;

impl CodexProbe {
    pub async fn account_read(config: CodexAccountProbeConfig) -> CodexAccountProbeSnapshot {
        let process_config = match codex_app_server_process_config(&config) {
            Ok(process_config) => process_config,
            Err(error) => return shadow_home_account_probe_snapshot(error),
        };

        let mut process = match spawn_cli_agent_process(&process_config) {
            Ok(process) => process,
            Err(error) => return spawn_error_probe_snapshot(&config, error),
        };
        let stderr_ring = process.stderr();

        let (stdout, stdin) = match process.take_stdio() {
            Ok(stdio) => stdio,
            Err(error) => {
                let message = format!("failed to open Codex app-server stdio pipes: {error:#}");
                let _ = process.terminate_with_grace(config.shutdown_grace).await;
                return CodexAccountProbeSnapshot {
                    status: CodexAccountProbeStatus::SpawnFailed,
                    message: Some(message.clone()),
                    user_agent: None,
                    version: None,
                    account: None,
                    requires_openai_auth: false,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.stdio_unavailable".to_owned(),
                        message,
                    }],
                    stderr: stderr_ring.lines().await,
                };
            }
        };

        let rpc = CodexJsonlRpcClient::new(BufReader::new(stdout), stdin);
        let client = CodexAppServerClient::new(rpc.clone());

        let initialize = match client.initialize(config.initialize_timeout).await {
            Ok(initialize) => initialize,
            Err(error) => {
                let stderr =
                    shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
                let message = format!("Codex app-server initialize failed: {error}");
                return CodexAccountProbeSnapshot {
                    status: CodexAccountProbeStatus::Error,
                    message: Some(message.clone()),
                    user_agent: None,
                    version: None,
                    account: None,
                    requires_openai_auth: false,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.initialize_failed".to_owned(),
                        message,
                    }],
                    stderr,
                };
            }
        };

        let user_agent = initialize.user_agent.clone();
        let version = initialize.version.clone().or_else(|| {
            user_agent
                .as_deref()
                .and_then(parse_codex_version_from_user_agent)
        });

        let account_response = match client.account_read(config.request_timeout).await {
            Ok(response) => response,
            Err(error) => {
                let stderr =
                    shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
                let message = format!("Codex account/read failed: {error}");
                return CodexAccountProbeSnapshot {
                    status: CodexAccountProbeStatus::Error,
                    message: Some(message.clone()),
                    user_agent,
                    version,
                    account: None,
                    requires_openai_auth: false,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.account_read_failed".to_owned(),
                        message,
                    }],
                    stderr,
                };
            }
        };

        let account = account_response
            .account
            .as_ref()
            .map(codex_account_snapshot_from_value);
        let status = if account.is_some() {
            CodexAccountProbeStatus::Ready
        } else if account_response.requires_openai_auth {
            CodexAccountProbeStatus::NeedsAuth
        } else {
            CodexAccountProbeStatus::Ready
        };
        let (message, diagnostics) = if status == CodexAccountProbeStatus::NeedsAuth {
            let message = "Codex CLI is not authenticated".to_owned();
            (
                Some(message.clone()),
                vec![CodexProbeDiagnostic {
                    level: CodexProbeDiagnosticLevel::Warning,
                    code: "codex_probe.needs_auth".to_owned(),
                    message,
                }],
            )
        } else {
            (None, Vec::new())
        };

        let stderr = shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
        CodexAccountProbeSnapshot {
            status,
            message,
            user_agent,
            version,
            account,
            requires_openai_auth: account_response.requires_openai_auth,
            diagnostics,
            stderr,
        }
    }

    pub async fn model_list(config: CodexAccountProbeConfig) -> CodexModelListProbeSnapshot {
        let process_config = match codex_app_server_process_config(&config) {
            Ok(process_config) => process_config,
            Err(error) => return shadow_home_model_list_probe_snapshot(error),
        };

        let mut process = match spawn_cli_agent_process(&process_config) {
            Ok(process) => process,
            Err(error) => return model_list_spawn_error_probe_snapshot(&config, error),
        };
        let stderr_ring = process.stderr();

        let (stdout, stdin) = match process.take_stdio() {
            Ok(stdio) => stdio,
            Err(error) => {
                let message = format!("failed to open Codex app-server stdio pipes: {error:#}");
                let _ = process.terminate_with_grace(config.shutdown_grace).await;
                return CodexModelListProbeSnapshot {
                    status: CodexModelListProbeStatus::SpawnFailed,
                    message: Some(message.clone()),
                    user_agent: None,
                    version: None,
                    models: Vec::new(),
                    requires_openai_auth: false,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.stdio_unavailable".to_owned(),
                        message,
                    }],
                    stderr: stderr_ring.lines().await,
                };
            }
        };

        let rpc = CodexJsonlRpcClient::new(BufReader::new(stdout), stdin);
        let client = CodexAppServerClient::new(rpc.clone());

        let initialize = match client.initialize(config.initialize_timeout).await {
            Ok(initialize) => initialize,
            Err(error) => {
                let stderr =
                    shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
                let message = format!("Codex app-server initialize failed: {error}");
                return CodexModelListProbeSnapshot {
                    status: CodexModelListProbeStatus::Error,
                    message: Some(message.clone()),
                    user_agent: None,
                    version: None,
                    models: Vec::new(),
                    requires_openai_auth: false,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.initialize_failed".to_owned(),
                        message,
                    }],
                    stderr,
                };
            }
        };
        let user_agent = initialize.user_agent.clone();
        let version = initialize.version.clone().or_else(|| {
            user_agent
                .as_deref()
                .and_then(parse_codex_version_from_user_agent)
        });

        let account_response = match client.account_read(config.request_timeout).await {
            Ok(response) => response,
            Err(error) => {
                let stderr =
                    shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
                let message = format!("Codex account/read failed before model/list: {error}");
                return CodexModelListProbeSnapshot {
                    status: CodexModelListProbeStatus::Error,
                    message: Some(message.clone()),
                    user_agent,
                    version,
                    models: Vec::new(),
                    requires_openai_auth: false,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.account_read_failed".to_owned(),
                        message,
                    }],
                    stderr,
                };
            }
        };

        if account_response.account.is_none() && account_response.requires_openai_auth {
            let stderr = shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
            let message = "Codex CLI is not authenticated".to_owned();
            return CodexModelListProbeSnapshot {
                status: CodexModelListProbeStatus::NeedsAuth,
                message: Some(message.clone()),
                user_agent,
                version,
                models: Vec::new(),
                requires_openai_auth: true,
                diagnostics: vec![CodexProbeDiagnostic {
                    level: CodexProbeDiagnosticLevel::Warning,
                    code: "codex_probe.needs_auth".to_owned(),
                    message,
                }],
                stderr,
            };
        }

        let models = match client.list_all_models(config.request_timeout).await {
            Ok(models) => models,
            Err(error) => {
                let stderr =
                    shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
                let message = format!("Codex model/list failed: {error}");
                return CodexModelListProbeSnapshot {
                    status: CodexModelListProbeStatus::Error,
                    message: Some(message.clone()),
                    user_agent,
                    version,
                    models: Vec::new(),
                    requires_openai_auth: account_response.requires_openai_auth,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.model_list_failed".to_owned(),
                        message,
                    }],
                    stderr,
                };
            }
        };

        let stderr = shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
        CodexModelListProbeSnapshot {
            status: CodexModelListProbeStatus::Ready,
            message: None,
            user_agent,
            version,
            models,
            requires_openai_auth: account_response.requires_openai_auth,
            diagnostics: Vec::new(),
            stderr,
        }
    }

    pub async fn login_start(
        config: CodexAccountProbeConfig,
        login_type: impl Into<String>,
    ) -> CodexLoginStartSnapshot {
        let login_type = login_type.into();
        let process_config = match codex_app_server_process_config(&config) {
            Ok(process_config) => process_config,
            Err(error) => return shadow_home_login_start_snapshot(login_type, error),
        };

        let mut process = match spawn_cli_agent_process(&process_config) {
            Ok(process) => process,
            Err(error) => return login_start_spawn_error_snapshot(&config, login_type, error),
        };
        let stderr_ring = process.stderr();
        let (stdout, stdin) = match process.take_stdio() {
            Ok(stdio) => stdio,
            Err(error) => {
                let message = format!("failed to open Codex app-server stdio pipes: {error:#}");
                let _ = process.terminate_with_grace(config.shutdown_grace).await;
                return CodexLoginStartSnapshot {
                    status: CodexLoginStartStatus::SpawnFailed,
                    login_type,
                    message: Some(message.clone()),
                    response: None,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.stdio_unavailable".to_owned(),
                        message,
                    }],
                    stderr: stderr_ring.lines().await,
                };
            }
        };

        let rpc = CodexJsonlRpcClient::new(BufReader::new(stdout), stdin);
        let client = CodexAppServerClient::new(rpc.clone());
        let mut notifications = rpc.take_notification_receiver();

        if let Err(error) = client.initialize(config.initialize_timeout).await {
            let stderr = shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
            let message = format!("Codex app-server initialize failed: {error}");
            return CodexLoginStartSnapshot {
                status: CodexLoginStartStatus::Error,
                login_type,
                message: Some(message.clone()),
                response: None,
                diagnostics: vec![CodexProbeDiagnostic {
                    level: CodexProbeDiagnosticLevel::Error,
                    code: "codex_probe.initialize_failed".to_owned(),
                    message,
                }],
                stderr,
            };
        }

        let response = match client
            .login_start(login_type.as_str(), config.request_timeout)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let stderr =
                    shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;
                let message = format!("Codex account/login/start failed: {error}");
                return CodexLoginStartSnapshot {
                    status: CodexLoginStartStatus::Error,
                    login_type,
                    message: Some(message.clone()),
                    response: None,
                    diagnostics: vec![CodexProbeDiagnostic {
                        level: CodexProbeDiagnosticLevel::Error,
                        code: "codex_probe.login_start_failed".to_owned(),
                        message,
                    }],
                    stderr,
                };
            }
        };

        let stderr_ring_for_task = stderr_ring.clone();
        let shutdown_grace = config.shutdown_grace;
        let login_id = response.login_id.clone();
        let rpc_for_task = rpc.clone();
        tokio::spawn(async move {
            wait_for_codex_login_completion(
                rpc_for_task,
                process,
                stderr_ring_for_task,
                shutdown_grace,
                login_id,
                notifications.take(),
            )
            .await;
        });

        CodexLoginStartSnapshot {
            status: CodexLoginStartStatus::Started,
            login_type,
            message: None,
            response: Some(response),
            diagnostics: Vec::new(),
            stderr: Vec::new(),
        }
    }

    pub async fn login_cancel(
        config: CodexAccountProbeConfig,
        login_id: impl Into<String>,
    ) -> CodexLoginCancelSnapshot {
        let login_id = login_id.into();
        let process_config = match codex_app_server_process_config(&config) {
            Ok(process_config) => process_config,
            Err(error) => {
                return CodexLoginCancelSnapshot {
                    login_id,
                    cancelled: false,
                    message: Some(error.to_string()),
                };
            }
        };

        let mut process = match spawn_cli_agent_process(&process_config) {
            Ok(process) => process,
            Err(error) => {
                return CodexLoginCancelSnapshot {
                    login_id,
                    cancelled: false,
                    message: Some(format!("failed to spawn Codex app-server: {error:#}")),
                };
            }
        };
        let stderr_ring = process.stderr();
        let (stdout, stdin) = match process.take_stdio() {
            Ok(stdio) => stdio,
            Err(error) => {
                let _ = process.terminate_with_grace(config.shutdown_grace).await;
                return CodexLoginCancelSnapshot {
                    login_id,
                    cancelled: false,
                    message: Some(format!(
                        "failed to open Codex app-server stdio pipes: {error:#}"
                    )),
                };
            }
        };

        let rpc = CodexJsonlRpcClient::new(BufReader::new(stdout), stdin);
        let client = CodexAppServerClient::new(rpc.clone());
        let result = match client.initialize(config.initialize_timeout).await {
            Ok(_) => client
                .login_cancel(login_id.as_str(), config.request_timeout)
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        };
        let _ = shutdown_probe_process(&rpc, &mut process, &stderr_ring, &config).await;

        CodexLoginCancelSnapshot {
            login_id,
            cancelled: result.is_ok(),
            message: result.err().map(|error| error.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexAccountReadResponse {
    #[serde(default)]
    pub account: Option<JsonValue>,
    #[serde(default)]
    pub requires_openai_auth: bool,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadStartParams {
    pub cwd: String,
    pub approval_policy: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ephemeral: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
}

impl fmt::Debug for CodexThreadStartParams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexThreadStartParams")
            .field("cwd", &self.cwd)
            .field("approval_policy", &self.approval_policy)
            .field("ephemeral", &self.ephemeral)
            .field("sandbox", &self.sandbox)
            .field("permissions", &self.permissions)
            .field("model", &self.model)
            .field("service_tier", &self.service_tier)
            .finish()
    }
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexThreadResumeParams {
    pub thread_id: String,
    #[serde(flatten)]
    pub start: CodexThreadStartParams,
}

impl fmt::Debug for CodexThreadResumeParams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexThreadResumeParams")
            .field("thread_id", &self.thread_id)
            .field("start", &self.start)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexThreadOpenSnapshot {
    pub native_thread_id: String,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadCompactStartParams {
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexThreadCompactStartSnapshot {
    pub native_thread_id: String,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexTurnStartParams {
    pub thread_id: String,
    pub input: Vec<CLIRuntimeTurnInputItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub personality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collaboration_mode: Option<CodexCollaborationMode>,
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub struct CodexCollaborationMode {
    mode: CodexCollaborationModeKind,
    settings: CodexCollaborationModeSettings,
}

impl CodexCollaborationMode {
    pub fn default_with_developer_instructions(
        model: String,
        reasoning_effort: Option<String>,
        developer_instructions: String,
    ) -> Self {
        Self {
            mode: CodexCollaborationModeKind::Default,
            settings: CodexCollaborationModeSettings {
                model,
                reasoning_effort,
                developer_instructions,
            },
        }
    }
}

impl fmt::Debug for CodexCollaborationMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexCollaborationMode")
            .field("mode", &self.mode)
            .field("model", &self.settings.model)
            .field("reasoning_effort", &self.settings.reasoning_effort)
            .field("developer_instructions", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CodexCollaborationModeKind {
    Default,
}

#[derive(Clone, PartialEq, Serialize)]
struct CodexCollaborationModeSettings {
    model: String,
    reasoning_effort: Option<String>,
    developer_instructions: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexTurnStartSnapshot {
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadNameSetParams {
    pub thread_id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexThreadNameSetSnapshot {
    pub native_thread_id: String,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexThreadForkParams {
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexTurnSteerParams {
    pub thread_id: String,
    pub expected_turn_id: String,
    pub input: Vec<CLIRuntimeTurnInputItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexTurnSteerSnapshot {
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexTurnSteerResponse {
    turn_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CodexReviewDelivery {
    Inline,
    Detached,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CodexReviewTarget {
    UncommittedChanges,
    BaseBranch {
        branch: String,
    },
    Commit {
        sha: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    Custom {
        instructions: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexReviewStartParams {
    pub thread_id: String,
    pub delivery: CodexReviewDelivery,
    pub target: CodexReviewTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexReviewStartSnapshot {
    pub native_thread_id: String,
    pub review_thread_id: String,
    pub native_turn_id: Option<String>,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexCommandApprovalRequest {
    pub native_request_id: String,
    pub native_request_id_json: JsonValue,
    pub approval_id: Option<String>,
    pub command: Option<String>,
    pub argv: Vec<String>,
    pub command_actions: Vec<JsonValue>,
    pub cwd: Option<String>,
    pub reason: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_item_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub raw: JsonValue,
}

impl CodexCommandApprovalRequest {
    pub fn display_command(&self) -> Option<String> {
        self.command
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .or_else(|| (!self.argv.is_empty()).then(|| self.argv.join(" ")))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CodexCommandApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
    Other(JsonValue),
}

impl CodexCommandApprovalDecision {
    pub fn as_codex_value(&self) -> JsonValue {
        match self {
            Self::Accept => JsonValue::String("accept".to_owned()),
            Self::AcceptForSession => JsonValue::String("acceptForSession".to_owned()),
            Self::Decline => JsonValue::String("decline".to_owned()),
            Self::Cancel => JsonValue::String("cancel".to_owned()),
            Self::Other(value) => value.clone(),
        }
    }
}

pub fn codex_command_approval_response(decision: CodexCommandApprovalDecision) -> JsonValue {
    json!({ "decision": decision.as_codex_value() })
}

pub fn decode_codex_command_approval_request(
    request: &CodexJsonlRpcServerRequest,
) -> CodexCommandApprovalRequest {
    let raw = request.params.clone().unwrap_or(JsonValue::Null);
    CodexCommandApprovalRequest {
        native_request_id: request.id.to_string(),
        native_request_id_json: jsonl_rpc_id_value(&request.id),
        approval_id: optional_string_field(&raw, &["approvalId", "approval_id"]),
        command: optional_string_field(&raw, &["command", "cmd", "shellCommand"]),
        argv: optional_string_array_field(&raw, &["argv", "args", "commandArgv"]),
        command_actions: optional_json_array_field(&raw, &["commandActions", "command_actions"]),
        cwd: optional_string_field(&raw, &["cwd", "workingDirectory", "working_directory"]),
        reason: optional_string_field(&raw, &["reason", "message"]),
        native_thread_id: optional_string_field(&raw, &["threadId", "thread_id"]),
        native_turn_id: optional_string_field(&raw, &["turnId", "turn_id"]),
        native_item_id: optional_string_field(&raw, &["itemId", "item_id"]),
        started_at_ms: optional_i64_field(&raw, &["startedAtMs", "started_at_ms"]),
        raw,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexFileChangeApprovalRequest {
    pub native_request_id: String,
    pub native_request_id_json: JsonValue,
    pub grant_root: Option<String>,
    pub changed_files: Vec<String>,
    pub summary: Option<String>,
    pub diff: Option<String>,
    pub reason: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_item_id: Option<String>,
    pub started_at_ms: Option<i64>,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CodexFileChangeApprovalDecision {
    Accept,
    AcceptForSession,
    Decline,
    Cancel,
}

impl CodexFileChangeApprovalDecision {
    pub fn as_codex_value(&self) -> JsonValue {
        match self {
            Self::Accept => JsonValue::String("accept".to_owned()),
            Self::AcceptForSession => JsonValue::String("acceptForSession".to_owned()),
            Self::Decline => JsonValue::String("decline".to_owned()),
            Self::Cancel => JsonValue::String("cancel".to_owned()),
        }
    }
}

pub fn codex_file_change_approval_response(decision: CodexFileChangeApprovalDecision) -> JsonValue {
    json!({ "decision": decision.as_codex_value() })
}

pub fn decode_codex_file_change_approval_request(
    request: &CodexJsonlRpcServerRequest,
) -> CodexFileChangeApprovalRequest {
    let raw = request.params.clone().unwrap_or(JsonValue::Null);
    CodexFileChangeApprovalRequest {
        native_request_id: request.id.to_string(),
        native_request_id_json: jsonl_rpc_id_value(&request.id),
        grant_root: optional_string_field(&raw, &["grantRoot", "grant_root"]),
        changed_files: optional_changed_file_paths(
            &raw,
            &[
                "changedFiles",
                "changed_files",
                "files",
                "paths",
                "affectedPaths",
                "affected_paths",
            ],
        ),
        summary: optional_string_field(&raw, &["summary", "title", "message"]),
        diff: optional_preserved_string_field(
            &raw,
            &["diff", "patch", "unifiedDiff", "unified_diff"],
        ),
        reason: optional_string_field(&raw, &["reason", "message"]),
        native_thread_id: optional_string_field(&raw, &["threadId", "thread_id"]),
        native_turn_id: optional_string_field(&raw, &["turnId", "turn_id"]),
        native_item_id: optional_string_field(&raw, &["itemId", "item_id"]),
        started_at_ms: optional_i64_field(&raw, &["startedAtMs", "started_at_ms"]),
        raw,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexUserInputRequest {
    pub native_request_id: String,
    pub native_request_id_json: JsonValue,
    pub questions: Vec<CodexUserInputQuestion>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_item_id: Option<String>,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexUserInputQuestion {
    pub id: String,
    pub header: String,
    pub question: String,
    pub options: Vec<CodexUserInputOption>,
    pub is_other: bool,
    pub is_secret: bool,
    pub raw: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexUserInputOption {
    pub label: String,
    pub description: String,
}

pub fn codex_user_input_response(answers: BTreeMap<String, Vec<String>>) -> JsonValue {
    let answers = answers
        .into_iter()
        .map(|(question_id, answers)| (question_id, json!({ "answers": answers })))
        .collect::<serde_json::Map<_, _>>();
    json!({ "answers": answers })
}

pub fn decode_codex_user_input_request(
    request: &CodexJsonlRpcServerRequest,
) -> CodexUserInputRequest {
    let raw = request.params.clone().unwrap_or(JsonValue::Null);
    CodexUserInputRequest {
        native_request_id: request.id.to_string(),
        native_request_id_json: jsonl_rpc_id_value(&request.id),
        questions: codex_user_input_questions_from_value(&raw),
        native_thread_id: optional_string_field(&raw, &["threadId", "thread_id"]),
        native_turn_id: optional_string_field(&raw, &["turnId", "turn_id"]),
        native_item_id: optional_string_field(&raw, &["itemId", "item_id"]),
        raw,
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexThreadOpenResponse {
    #[serde(default)]
    thread: Option<CodexThreadOpenThread>,
    #[serde(default, alias = "thread_id")]
    thread_id: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default, flatten)]
    extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexThreadOpenThread {
    #[serde(default, alias = "thread_id")]
    id: Option<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default, flatten)]
    extra: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexTurnStartResponse {
    #[serde(default, alias = "turn_id")]
    turn_id: Option<String>,
    #[serde(default)]
    turn: Option<CodexTurnStartTurnResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexTurnStartTurnResponse {
    id: String,
}

fn decode_codex_thread_open_response(
    method: &str,
    raw: JsonValue,
) -> Result<CodexThreadOpenSnapshot, CodexJsonlRpcClientError> {
    let response: CodexThreadOpenResponse =
        serde_json::from_value(raw.clone()).map_err(|error| CodexJsonlRpcClientError::Decode {
            method: method.to_owned(),
            message: error.to_string(),
        })?;

    let CodexThreadOpenResponse {
        thread,
        thread_id,
        id,
        cwd,
        model,
        ..
    } = response;
    let (thread_id_from_thread, cwd_from_thread, model_from_thread) = thread
        .map(|thread| (thread.id, thread.cwd, thread.model))
        .unwrap_or((None, None, None));
    let native_thread_id = thread_id_from_thread
        .or(thread_id)
        .or(id)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CodexJsonlRpcClientError::Decode {
            method: method.to_owned(),
            message: "Codex thread open response did not include thread.id".to_owned(),
        })?;

    Ok(CodexThreadOpenSnapshot {
        native_thread_id,
        cwd: cwd_from_thread.or(cwd),
        model: model_from_thread.or(model),
        raw,
    })
}

fn decode_codex_turn_start_response(
    method: &str,
    native_thread_id: &str,
    raw: JsonValue,
) -> Result<CodexTurnStartSnapshot, CodexJsonlRpcClientError> {
    let response: CodexTurnStartResponse =
        serde_json::from_value(raw.clone()).map_err(|error| CodexJsonlRpcClientError::Decode {
            method: method.to_owned(),
            message: error.to_string(),
        })?;
    let native_turn_id = response
        .turn
        .map(|turn| turn.id)
        .or(response.turn_id)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CodexJsonlRpcClientError::Decode {
            method: method.to_owned(),
            message: "Codex turn/start response did not include turn.id".to_owned(),
        })?;

    Ok(CodexTurnStartSnapshot {
        native_thread_id: native_thread_id.to_owned(),
        native_turn_id,
        raw,
    })
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexReviewStartResponse {
    #[serde(default)]
    turn: Option<CodexReviewStartTurn>,
    #[serde(default, alias = "review_thread_id")]
    review_thread_id: Option<String>,
    #[serde(default, alias = "thread_id")]
    thread_id: Option<String>,
    #[serde(default, alias = "turn_id")]
    turn_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexReviewStartTurn {
    #[serde(default, alias = "turn_id")]
    id: Option<String>,
    #[serde(default, alias = "thread_id")]
    thread_id: Option<String>,
}

fn decode_codex_review_start_response(
    method: &str,
    fallback_thread_id: &str,
    raw: JsonValue,
) -> Result<CodexReviewStartSnapshot, CodexJsonlRpcClientError> {
    let response: CodexReviewStartResponse =
        serde_json::from_value(raw.clone()).map_err(|error| CodexJsonlRpcClientError::Decode {
            method: method.to_owned(),
            message: error.to_string(),
        })?;

    let turn_thread_id = response
        .turn
        .as_ref()
        .and_then(|turn| normalize_optional_string(turn.thread_id.as_deref()));
    let review_thread_id = normalize_optional_string(response.review_thread_id.as_deref())
        .or(turn_thread_id.clone())
        .or_else(|| normalize_optional_string(response.thread_id.as_deref()))
        .unwrap_or_else(|| fallback_thread_id.to_owned());
    let native_thread_id = turn_thread_id.unwrap_or_else(|| fallback_thread_id.to_owned());
    let native_turn_id = response
        .turn
        .and_then(|turn| normalize_optional_string(turn.id.as_deref()))
        .or_else(|| normalize_optional_string(response.turn_id.as_deref()));

    Ok(CodexReviewStartSnapshot {
        native_thread_id,
        review_thread_id,
        native_turn_id,
        raw,
    })
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexModelListProbeSnapshot {
    pub status: CodexModelListProbeStatus,
    pub message: Option<String>,
    pub user_agent: Option<String>,
    pub version: Option<String>,
    pub models: Vec<CodexModelSnapshot>,
    pub requires_openai_auth: bool,
    pub diagnostics: Vec<CodexProbeDiagnostic>,
    pub stderr: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexModelListProbeStatus {
    Ready,
    NeedsAuth,
    MissingBinary,
    SpawnFailed,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexModelListPage {
    pub models: Vec<CodexModelSnapshot>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexModelSnapshot {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub family: Option<String>,
    pub active: Option<bool>,
    pub effort_options: Vec<String>,
    pub input_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
    pub supports_reasoning: Option<bool>,
    pub supports_vision: Option<bool>,
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CodexModelListParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexModelListResponse {
    #[serde(default)]
    data: Vec<CodexModelListModel>,
    #[serde(default)]
    models: Vec<CodexModelListModel>,
    #[serde(default, alias = "next_cursor")]
    next_cursor: Option<String>,
    #[serde(default, flatten)]
    extra: BTreeMap<String, JsonValue>,
}

impl CodexModelListResponse {
    fn into_page(self) -> CodexModelListPage {
        let models = self
            .data
            .into_iter()
            .chain(self.models)
            .filter_map(CodexModelListModel::into_snapshot)
            .collect();

        CodexModelListPage {
            models,
            next_cursor: self.next_cursor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CodexModelListModel {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default, alias = "name", alias = "display_name")]
    display_name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    family: Option<String>,
    #[serde(default, alias = "isActive")]
    active: Option<bool>,
    #[serde(default)]
    supported_reasoning_efforts: Vec<CodexReasoningEffort>,
    #[serde(default)]
    input_modalities: Vec<String>,
    #[serde(default)]
    output_modalities: Vec<String>,
    #[serde(default)]
    supports_reasoning: Option<bool>,
    #[serde(default)]
    supports_vision: Option<bool>,
    #[serde(default, alias = "maxTokens", alias = "contextWindow")]
    max_input_tokens: Option<u64>,
    #[serde(default, alias = "maxOutputToken", alias = "outputTokenLimit")]
    max_output_tokens: Option<u64>,
    #[serde(default, flatten)]
    extra: BTreeMap<String, JsonValue>,
}

impl CodexModelListModel {
    fn into_snapshot(self) -> Option<CodexModelSnapshot> {
        let id = self.model.or(self.id)?.trim().to_owned();
        if id.is_empty() {
            return None;
        }

        let effort_options = self
            .supported_reasoning_efforts
            .into_iter()
            .filter_map(CodexReasoningEffort::into_value)
            .collect::<Vec<_>>();
        let supports_reasoning = self
            .supports_reasoning
            .or_else(|| (!effort_options.is_empty()).then_some(true));
        let supports_vision = self.supports_vision.or_else(|| {
            self.input_modalities
                .iter()
                .any(|modality| matches!(modality.as_str(), "image" | "vision"))
                .then_some(true)
        });

        Some(CodexModelSnapshot {
            id,
            name: self.display_name,
            description: self.description,
            family: self.family,
            active: self.active,
            effort_options,
            input_modalities: self.input_modalities,
            output_modalities: self.output_modalities,
            supports_reasoning,
            supports_vision,
            max_input_tokens: self.max_input_tokens,
            max_output_tokens: self.max_output_tokens,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
enum CodexReasoningEffort {
    String(String),
    Object {
        #[serde(default, rename = "reasoningEffort", alias = "id")]
        reasoning_effort: Option<String>,
        #[serde(default, flatten)]
        extra: BTreeMap<String, JsonValue>,
    },
}

impl CodexReasoningEffort {
    fn into_value(self) -> Option<String> {
        let value = match self {
            Self::String(value) => value,
            Self::Object {
                reasoning_effort, ..
            } => reasoning_effort?,
        };
        normalize_metadata_reasoning_effort(value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexLoginStartSnapshot {
    pub status: CodexLoginStartStatus,
    pub login_type: String,
    pub message: Option<String>,
    pub response: Option<CodexLoginStartResponse>,
    pub diagnostics: Vec<CodexProbeDiagnostic>,
    pub stderr: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexLoginStartStatus {
    Started,
    MissingBinary,
    SpawnFailed,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexLoginCancelSnapshot {
    pub login_id: String,
    pub cancelled: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLoginStartResponse {
    #[serde(default, rename = "type")]
    pub login_type: Option<String>,
    #[serde(default)]
    pub login_id: Option<String>,
    #[serde(default)]
    pub verification_url: Option<String>,
    #[serde(default)]
    pub user_code: Option<String>,
    #[serde(default, alias = "authUrl", alias = "url")]
    pub auth_url: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default, skip)]
    pub raw: JsonValue,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, JsonValue>,
}

impl CodexLoginStartResponse {
    fn with_raw(mut self, raw: JsonValue) -> Self {
        self.raw = raw;
        self
    }
}

fn spawn_error_probe_snapshot(
    config: &CodexAccountProbeConfig,
    error: anyhow::Error,
) -> CodexAccountProbeSnapshot {
    let is_missing_binary = error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.kind() == ErrorKind::NotFound)
    });
    let status = if is_missing_binary {
        CodexAccountProbeStatus::MissingBinary
    } else {
        CodexAccountProbeStatus::SpawnFailed
    };
    let code = if is_missing_binary {
        "codex_probe.missing_binary"
    } else {
        "codex_probe.spawn_failed"
    };
    let message = if is_missing_binary {
        format!("Codex CLI binary `{}` was not found", config.executable)
    } else {
        format!("failed to spawn Codex app-server: {error:#}")
    };

    CodexAccountProbeSnapshot {
        status,
        message: Some(message.clone()),
        user_agent: None,
        version: None,
        account: None,
        requires_openai_auth: false,
        diagnostics: vec![CodexProbeDiagnostic {
            level: CodexProbeDiagnosticLevel::Error,
            code: code.to_owned(),
            message,
        }],
        stderr: Vec::new(),
    }
}

fn shadow_home_probe_diagnostic(error: &CodexShadowHomeError) -> CodexProbeDiagnostic {
    CodexProbeDiagnostic {
        level: CodexProbeDiagnosticLevel::Error,
        code: "codex_probe.shadow_home_failed".to_owned(),
        message: error.to_string(),
    }
}

fn shadow_home_account_probe_snapshot(error: CodexShadowHomeError) -> CodexAccountProbeSnapshot {
    CodexAccountProbeSnapshot {
        status: CodexAccountProbeStatus::Error,
        message: Some(error.to_string()),
        user_agent: None,
        version: None,
        account: None,
        requires_openai_auth: false,
        diagnostics: vec![shadow_home_probe_diagnostic(&error)],
        stderr: Vec::new(),
    }
}

fn shadow_home_model_list_probe_snapshot(
    error: CodexShadowHomeError,
) -> CodexModelListProbeSnapshot {
    CodexModelListProbeSnapshot {
        status: CodexModelListProbeStatus::Error,
        message: Some(error.to_string()),
        user_agent: None,
        version: None,
        models: Vec::new(),
        requires_openai_auth: false,
        diagnostics: vec![shadow_home_probe_diagnostic(&error)],
        stderr: Vec::new(),
    }
}

fn shadow_home_login_start_snapshot(
    login_type: String,
    error: CodexShadowHomeError,
) -> CodexLoginStartSnapshot {
    CodexLoginStartSnapshot {
        status: CodexLoginStartStatus::Error,
        login_type,
        message: Some(error.to_string()),
        response: None,
        diagnostics: vec![shadow_home_probe_diagnostic(&error)],
        stderr: Vec::new(),
    }
}

fn model_list_spawn_error_probe_snapshot(
    config: &CodexAccountProbeConfig,
    error: anyhow::Error,
) -> CodexModelListProbeSnapshot {
    let is_missing_binary = error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.kind() == ErrorKind::NotFound)
    });
    let status = if is_missing_binary {
        CodexModelListProbeStatus::MissingBinary
    } else {
        CodexModelListProbeStatus::SpawnFailed
    };
    let code = if is_missing_binary {
        "codex_probe.missing_binary"
    } else {
        "codex_probe.spawn_failed"
    };
    let message = if is_missing_binary {
        format!("Codex CLI binary `{}` was not found", config.executable)
    } else {
        format!("failed to spawn Codex app-server: {error:#}")
    };

    CodexModelListProbeSnapshot {
        status,
        message: Some(message.clone()),
        user_agent: None,
        version: None,
        models: Vec::new(),
        requires_openai_auth: false,
        diagnostics: vec![CodexProbeDiagnostic {
            level: CodexProbeDiagnosticLevel::Error,
            code: code.to_owned(),
            message,
        }],
        stderr: Vec::new(),
    }
}

fn login_start_spawn_error_snapshot(
    config: &CodexAccountProbeConfig,
    login_type: String,
    error: anyhow::Error,
) -> CodexLoginStartSnapshot {
    let is_missing_binary = error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.kind() == ErrorKind::NotFound)
    });
    let status = if is_missing_binary {
        CodexLoginStartStatus::MissingBinary
    } else {
        CodexLoginStartStatus::SpawnFailed
    };
    let code = if is_missing_binary {
        "codex_probe.missing_binary"
    } else {
        "codex_probe.spawn_failed"
    };
    let message = if is_missing_binary {
        format!("Codex CLI binary `{}` was not found", config.executable)
    } else {
        format!("failed to spawn Codex app-server: {error:#}")
    };

    CodexLoginStartSnapshot {
        status,
        login_type,
        message: Some(message.clone()),
        response: None,
        diagnostics: vec![CodexProbeDiagnostic {
            level: CodexProbeDiagnosticLevel::Error,
            code: code.to_owned(),
            message,
        }],
        stderr: Vec::new(),
    }
}

async fn wait_for_codex_login_completion(
    rpc: CodexJsonlRpcClient,
    mut process: crate::process::CLIAgentProcess,
    stderr_ring: crate::process::StderrRing,
    shutdown_grace: Duration,
    login_id: Option<String>,
    mut notifications: Option<mpsc::Receiver<CodexJsonlRpcNotificationEvent>>,
) {
    if let Some(notifications) = notifications.as_mut() {
        let wait = async {
            while let Some(notification) = notifications.recv().await {
                if notification.method != "account/login/completed" {
                    continue;
                }
                let matches_login_id = match (login_id.as_deref(), notification.params.as_ref()) {
                    (Some(login_id), Some(params)) => {
                        params.get("loginId").and_then(JsonValue::as_str) == Some(login_id)
                    }
                    (Some(_), None) => false,
                    (None, _) => true,
                };
                if matches_login_id {
                    break;
                }
            }
        };
        let _ = tokio::time::timeout(LOGIN_SESSION_TIMEOUT, wait).await;
    } else {
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
    let _ =
        shutdown_probe_process_with_grace(&rpc, &mut process, &stderr_ring, shutdown_grace).await;
}

async fn shutdown_probe_process(
    rpc: &CodexJsonlRpcClient,
    process: &mut crate::process::CLIAgentProcess,
    stderr_ring: &crate::process::StderrRing,
    config: &CodexAccountProbeConfig,
) -> Vec<String> {
    shutdown_probe_process_with_grace(rpc, process, stderr_ring, config.shutdown_grace).await
}

async fn shutdown_probe_process_with_grace(
    rpc: &CodexJsonlRpcClient,
    process: &mut crate::process::CLIAgentProcess,
    stderr_ring: &crate::process::StderrRing,
    shutdown_grace: Duration,
) -> Vec<String> {
    let _ = rpc.shutdown().await;
    let _ = process.terminate_with_grace(shutdown_grace).await;
    stderr_ring.lines().await
}

fn codex_account_snapshot_from_value(account: &JsonValue) -> CodexAccountSnapshot {
    CodexAccountSnapshot {
        authenticated: true,
        account_id: first_string_field(account, &["id", "accountId"]),
        email: first_string_field(account, &["email"]),
        display_name: first_string_field(account, &["displayName", "name"]),
        plan: first_string_field(account, &["planType", "plan"]),
        auth_method: first_string_field(account, &["type", "authMethod"]),
    }
}

fn first_string_field(value: &JsonValue, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
    })
}

fn optional_string_field(value: &JsonValue, keys: &[&str]) -> Option<String> {
    first_string_field(value, keys)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn optional_preserved_string_field(value: &JsonValue, keys: &[&str]) -> Option<String> {
    first_string_field(value, keys).filter(|value| !value.is_empty())
}

fn optional_i64_field(value: &JsonValue, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(JsonValue::as_i64))
}

fn optional_string_array_field(value: &JsonValue, keys: &[&str]) -> Vec<String> {
    keys.iter()
        .find_map(|key| {
            value.get(*key).and_then(|candidate| {
                candidate.as_array().map(|items| {
                    items
                        .iter()
                        .filter_map(JsonValue::as_str)
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
            })
        })
        .unwrap_or_default()
}

fn optional_changed_file_paths(value: &JsonValue, keys: &[&str]) -> Vec<String> {
    keys.iter()
        .find_map(|key| {
            value.get(*key).and_then(|candidate| match candidate {
                JsonValue::String(path) => {
                    let path = path.trim();
                    (!path.is_empty()).then(|| vec![path.to_owned()])
                }
                JsonValue::Array(items) => {
                    let paths = items
                        .iter()
                        .filter_map(|item| match item {
                            JsonValue::String(path) => Some(path.as_str()),
                            JsonValue::Object(map) => map
                                .get("path")
                                .or_else(|| map.get("file"))
                                .or_else(|| map.get("name"))
                                .and_then(JsonValue::as_str),
                            _ => None,
                        })
                        .map(str::trim)
                        .filter(|path| !path.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>();
                    (!paths.is_empty()).then_some(paths)
                }
                _ => None,
            })
        })
        .unwrap_or_default()
}

fn optional_json_array_field(value: &JsonValue, keys: &[&str]) -> Vec<JsonValue> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(JsonValue::as_array).cloned())
        .unwrap_or_default()
}

fn codex_user_input_questions_from_value(value: &JsonValue) -> Vec<CodexUserInputQuestion> {
    value
        .get("questions")
        .and_then(JsonValue::as_array)
        .map(|questions| {
            questions
                .iter()
                .enumerate()
                .filter_map(|(index, question)| {
                    let id = optional_string_field(question, &["id"])
                        .unwrap_or_else(|| format!("question_{}", index + 1));
                    let question_text =
                        optional_string_field(question, &["question", "text", "label"])
                            .unwrap_or_default();
                    if id.trim().is_empty() && question_text.trim().is_empty() {
                        return None;
                    }
                    Some(CodexUserInputQuestion {
                        id,
                        header: optional_string_field(question, &["header"]).unwrap_or_default(),
                        question: question_text,
                        options: codex_user_input_options_from_value(question),
                        is_other: optional_bool_field(question, &["isOther", "is_other"])
                            .unwrap_or(false),
                        is_secret: optional_bool_field(question, &["isSecret", "is_secret"])
                            .unwrap_or(false),
                        raw: question.clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn codex_user_input_options_from_value(value: &JsonValue) -> Vec<CodexUserInputOption> {
    value
        .get("options")
        .and_then(JsonValue::as_array)
        .map(|options| {
            options
                .iter()
                .filter_map(|option| {
                    let label = optional_string_field(option, &["label", "value"])?;
                    Some(CodexUserInputOption {
                        label,
                        description: optional_string_field(option, &["description"])
                            .unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn optional_bool_field(value: &JsonValue, keys: &[&str]) -> Option<bool> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(JsonValue::as_bool))
}

fn jsonl_rpc_id_value(id: &JsonlRpcId) -> JsonValue {
    serde_json::to_value(id).unwrap_or_else(|_| JsonValue::String(id.to_string()))
}

fn parse_codex_version_from_user_agent(user_agent: &str) -> Option<String> {
    let (_, rest) = user_agent.split_once('/')?;
    let version = rest.split_whitespace().next()?.trim();
    if version.is_empty() {
        None
    } else {
        Some(version.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexJsonlRpcNotificationEvent {
    pub method: String,
    pub params: Option<JsonValue>,
    pub raw: JsonValue,
}

impl From<JsonlRpcNotification> for CodexJsonlRpcNotificationEvent {
    fn from(notification: JsonlRpcNotification) -> Self {
        Self {
            method: notification.method,
            params: notification.params,
            raw: notification.raw,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexJsonlRpcServerRequest {
    pub id: JsonlRpcId,
    pub method: String,
    pub params: Option<JsonValue>,
    pub raw: JsonValue,
}

impl From<JsonlRpcRequest> for CodexJsonlRpcServerRequest {
    fn from(request: JsonlRpcRequest) -> Self {
        Self {
            id: request.id,
            method: request.method,
            params: request.params,
            raw: request.raw,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexJsonlRpcClientDiagnostic {
    pub kind: CodexJsonlRpcClientDiagnosticKind,
    pub message: String,
    pub method: Option<String>,
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexJsonlRpcClientDiagnosticKind {
    NotificationChannelFull,
    NotificationChannelClosed,
    ServerRequestChannelFull,
    ServerRequestChannelClosed,
    DuplicateServerRequestId,
}

enum CodexJsonlRpcServerRequestAnswer {
    Result(JsonValue),
    Error {
        code: i64,
        message: String,
        data: Option<JsonValue>,
    },
}

enum CodexJsonlRpcCommand {
    Request {
        request: JsonlRpcRequest,
        response_tx: oneshot::Sender<Result<JsonValue, CodexJsonlRpcClientError>>,
    },
    Notify {
        notification: JsonlRpcNotification,
        response_tx: oneshot::Sender<Result<(), CodexJsonlRpcClientError>>,
    },
    RespondToServerRequest {
        id: JsonlRpcId,
        answer: CodexJsonlRpcServerRequestAnswer,
        response_tx: oneshot::Sender<Result<(), CodexJsonlRpcClientError>>,
    },
    Cancel {
        id: JsonlRpcId,
    },
    Shutdown,
}

enum CodexJsonlRpcIncoming {
    Message(JsonlRpcIncomingMessage),
    DecodeError(JsonlRpcDecodeError),
    Closed,
}

struct PendingCodexJsonlRpcRequest {
    response_tx: oneshot::Sender<Result<JsonValue, CodexJsonlRpcClientError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexServerRequestTerminalState {
    DuplicateId,
    Evicted,
    IngressClosed,
    TimedOut,
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
struct PendingCodexServerRequest {
    deadline: Instant,
}

struct CodexJsonlRpcEventSinks {
    notification_ingress: OrderedEventIngress<CodexJsonlRpcNotificationEvent>,
    server_request_tx: mpsc::Sender<CodexJsonlRpcServerRequest>,
    diagnostic_tx: mpsc::Sender<CodexJsonlRpcClientDiagnostic>,
}

impl OrderedIngressEvent for CodexJsonlRpcNotificationEvent {
    type Key = String;

    fn ingress_class(&self) -> OrderedIngressClass<Self::Key> {
        if codex_notification_is_progress(self.method.as_str()) {
            OrderedIngressClass::Coalesced(codex_notification_progress_key(self))
        } else {
            OrderedIngressClass::Durable
        }
    }

    fn coalesce(&mut self, newer: Self) {
        merge_codex_progress_notification(self, newer);
    }
}

#[derive(Default)]
struct PendingCodexJsonlRpcRequests {
    pending: HashMap<JsonlRpcId, PendingCodexJsonlRpcRequest>,
}

impl PendingCodexJsonlRpcRequests {
    fn contains(&self, id: &JsonlRpcId) -> bool {
        self.pending.contains_key(id)
    }

    fn insert(&mut self, id: JsonlRpcId, request: PendingCodexJsonlRpcRequest) {
        self.pending.insert(id, request);
    }

    fn remove(&mut self, id: &JsonlRpcId) -> Option<PendingCodexJsonlRpcRequest> {
        self.pending.remove(id)
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn fail_all(&mut self, error: CodexJsonlRpcClientError) -> usize {
        let count = self.pending.len();
        for (_, pending) in self.pending.drain() {
            let _ = pending.response_tx.send(Err(error.clone()));
        }
        count
    }
}

const CODEX_NOTIFICATION_PROGRESS_FLUSH_INTERVAL: Duration = Duration::from_millis(25);
const CODEX_NOTIFICATION_MAX_PENDING_PROGRESS_KEYS: usize = 4096;
const CODEX_NOTIFICATION_MAX_PENDING_DURABLE_EVENTS: usize = 1024;
const CODEX_NOTIFICATION_MAX_PENDING_PROGRESS_BYTES: usize = 64 * 1024;

fn codex_notification_is_progress(method: &str) -> bool {
    matches!(
        method,
        "thread/tokenUsage/updated"
            | "account/rateLimits/updated"
            | "turn/plan/updated"
            | "turn/diff/updated"
            | "item/agentMessage/delta"
            | "item/reasoning/textDelta"
            | "item/reasoning/summaryTextDelta"
            | "item/reasoning/summaryPartAdded"
            | "item/plan/delta"
            | "item/commandExecution/outputDelta"
            | "item/fileChange/outputDelta"
            | "item/fileChange/patchUpdated"
    )
}

fn codex_notification_progress_key(notification: &CodexJsonlRpcNotificationEvent) -> String {
    let params = notification.params.as_ref();
    let part = |names: &[&str]| {
        names.iter().find_map(|name| {
            params
                .and_then(|params| params.get(*name))
                .and_then(JsonValue::as_str)
        })
    };
    format!(
        "{}:{}:{}:{}",
        notification.method,
        part(&["threadId", "thread_id"]).unwrap_or_default(),
        part(&["turnId", "turn_id"]).unwrap_or_default(),
        part(&["itemId", "item_id", "callId", "call_id"]).unwrap_or_default(),
    )
}

fn merge_codex_progress_notification(
    pending: &mut CodexJsonlRpcNotificationEvent,
    next: CodexJsonlRpcNotificationEvent,
) {
    if codex_notification_progress_is_snapshot(pending.method.as_str()) {
        *pending = next;
        return;
    }

    let next_delta = next
        .params
        .as_ref()
        .and_then(|params| params.get("delta").or_else(|| params.get("text")))
        .and_then(JsonValue::as_str);
    let Some(next_delta) = next_delta else {
        *pending = next;
        return;
    };
    let mut delta = pending
        .params
        .as_ref()
        .and_then(|params| params.get("delta").or_else(|| params.get("text")))
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned();
    delta.push_str(next_delta);
    let truncated =
        truncate_codex_progress_delta(&mut delta, CODEX_NOTIFICATION_MAX_PENDING_PROGRESS_BYTES);

    let mut params = next.params.unwrap_or_else(|| json!({}));
    if let Some(object) = params.as_object_mut() {
        object.insert("delta".to_owned(), JsonValue::String(delta));
        object.insert("coalesced".to_owned(), JsonValue::Bool(true));
        if truncated {
            object.insert("truncated".to_owned(), JsonValue::Bool(true));
        }
    }
    pending.params = Some(params.clone());
    pending.raw = next.raw;
    if let Some(raw) = pending.raw.as_object_mut() {
        raw.insert("params".to_owned(), params);
    }
}

fn codex_notification_progress_is_snapshot(method: &str) -> bool {
    matches!(
        method,
        "thread/tokenUsage/updated"
            | "account/rateLimits/updated"
            | "turn/plan/updated"
            | "turn/diff/updated"
            | "item/reasoning/summaryPartAdded"
            | "item/fileChange/patchUpdated"
    )
}

fn truncate_codex_progress_delta(delta: &mut String, max_bytes: usize) -> bool {
    if delta.len() <= max_bytes {
        return false;
    }
    let mut boundary = max_bytes.min(delta.len());
    while boundary > 0 && !delta.is_char_boundary(boundary) {
        boundary -= 1;
    }
    delta.truncate(boundary);
    true
}

async fn run_codex_jsonl_rpc_reader<R>(
    mut reader: R,
    incoming_tx: mpsc::Sender<CodexJsonlRpcIncoming>,
) where
    R: AsyncBufRead + Send + Unpin + 'static,
{
    loop {
        match read_jsonl_rpc_message(&mut reader).await {
            Ok(Some(message)) => {
                if incoming_tx
                    .send(CodexJsonlRpcIncoming::Message(message))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Ok(None) => {
                let _ = incoming_tx.send(CodexJsonlRpcIncoming::Closed).await;
                return;
            }
            Err(error) => {
                let _ = incoming_tx
                    .send(CodexJsonlRpcIncoming::DecodeError(error))
                    .await;
                return;
            }
        }
    }
}

async fn run_codex_jsonl_rpc_worker<W>(
    mut writer: W,
    mut command_rx: mpsc::Receiver<CodexJsonlRpcCommand>,
    mut incoming_rx: mpsc::Receiver<CodexJsonlRpcIncoming>,
    event_sinks: CodexJsonlRpcEventSinks,
    max_pending_server_requests: usize,
    server_request_timeout: Duration,
) where
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut pending = PendingCodexJsonlRpcRequests::default();
    let mut pending_server_requests = HashMap::<JsonlRpcId, PendingCodexServerRequest>::new();
    let close_error = loop {
        let next_server_request_deadline = pending_server_requests
            .values()
            .map(|pending| pending.deadline)
            .min();
        tokio::select! {
            command = command_rx.recv() => {
                match command {
                    Some(CodexJsonlRpcCommand::Request { request, response_tx }) => {
                        let id = request.id.clone();
                        if pending.contains(&id) {
                            let _ = response_tx.send(Err(CodexJsonlRpcClientError::DuplicateRequestId { id }));
                            continue;
                        }

                        pending.insert(id.clone(), PendingCodexJsonlRpcRequest { response_tx });
                        if let Err(error) = write_jsonl_rpc_request(&mut writer, &request).await {
                            let write_error = CodexJsonlRpcClientError::TransportClosed {
                                message: format!("failed to write codex jsonl-rpc request: {error}"),
                            };
                            if let Some(pending_request) = pending.remove(&id) {
                                let _ = pending_request.response_tx.send(Err(write_error.clone()));
                            }
                            break write_error;
                        }
                    }
                    Some(CodexJsonlRpcCommand::Notify { notification, response_tx }) => {
                        if let Err(error) = write_jsonl_rpc_message(&mut writer, &notification).await {
                            let write_error = CodexJsonlRpcClientError::TransportClosed {
                                message: format!("failed to write codex jsonl-rpc notification: {error}"),
                            };
                            let _ = response_tx.send(Err(write_error.clone()));
                            break write_error;
                        }

                        let _ = response_tx.send(Ok(()));
                    }
                    Some(CodexJsonlRpcCommand::RespondToServerRequest { id, answer, response_tx }) => {
                        if pending_server_requests.remove(&id).is_none() {
                            let _ = response_tx.send(Err(CodexJsonlRpcClientError::ServerRequestNotPending { id }));
                            continue;
                        }

                        let response = server_request_answer_to_response(id, answer);
                        if let Err(error) = write_jsonl_rpc_message(&mut writer, &response).await {
                            let write_error = CodexJsonlRpcClientError::TransportClosed {
                                message: format!("failed to write codex server request response: {error}"),
                            };
                            let _ = response_tx.send(Err(write_error.clone()));
                            break write_error;
                        }

                        let _ = response_tx.send(Ok(()));
                    }
                    Some(CodexJsonlRpcCommand::Cancel { id }) => {
                        let _ = pending.remove(&id);
                    }
                    Some(CodexJsonlRpcCommand::Shutdown) => {
                        complete_all_codex_server_requests(
                            &mut writer,
                            &mut pending_server_requests,
                            CodexServerRequestTerminalState::Shutdown,
                        ).await;
                        break CodexJsonlRpcClientError::TransportClosed {
                            message: "codex jsonl-rpc client shut down".to_owned(),
                        };
                    }
                    None => {
                        complete_all_codex_server_requests(
                            &mut writer,
                            &mut pending_server_requests,
                            CodexServerRequestTerminalState::Shutdown,
                        ).await;
                        break CodexJsonlRpcClientError::TransportClosed {
                            message: "codex jsonl-rpc command channel closed".to_owned(),
                        };
                    }
                }
            }
            incoming = incoming_rx.recv() => {
                match incoming {
                    Some(CodexJsonlRpcIncoming::Message(message)) => {
                        handle_codex_jsonl_rpc_incoming(
                            message,
                            &mut pending,
                            &mut pending_server_requests,
                            &event_sinks,
                            max_pending_server_requests,
                            server_request_timeout,
                            &mut writer,
                        ).await;
                    }
                    Some(CodexJsonlRpcIncoming::DecodeError(error)) => {
                        complete_all_codex_server_requests(
                            &mut writer,
                            &mut pending_server_requests,
                            CodexServerRequestTerminalState::Shutdown,
                        ).await;
                        break CodexJsonlRpcClientError::TransportClosed {
                            message: format!("failed to decode codex jsonl-rpc message: {error}"),
                        };
                    }
                    Some(CodexJsonlRpcIncoming::Closed) => {
                        complete_all_codex_server_requests(
                            &mut writer,
                            &mut pending_server_requests,
                            CodexServerRequestTerminalState::Shutdown,
                        ).await;
                        break CodexJsonlRpcClientError::TransportClosed {
                            message: "codex jsonl-rpc stdout closed".to_owned(),
                        };
                    }
                    None => {
                        complete_all_codex_server_requests(
                            &mut writer,
                            &mut pending_server_requests,
                            CodexServerRequestTerminalState::Shutdown,
                        ).await;
                        break CodexJsonlRpcClientError::TransportClosed {
                            message: "codex jsonl-rpc reader closed".to_owned(),
                        };
                    }
                }
            }
            _ = wait_for_codex_server_request_deadline(next_server_request_deadline), if next_server_request_deadline.is_some() => {
                complete_expired_codex_server_requests(
                    &mut writer,
                    &mut pending_server_requests,
                    Instant::now(),
                ).await;
            }
        }
    };

    if !pending.is_empty() {
        let _ = pending.fail_all(close_error);
    }
    event_sinks.notification_ingress.close();
}

async fn handle_codex_jsonl_rpc_incoming(
    message: JsonlRpcIncomingMessage,
    pending: &mut PendingCodexJsonlRpcRequests,
    pending_server_requests: &mut HashMap<JsonlRpcId, PendingCodexServerRequest>,
    event_sinks: &CodexJsonlRpcEventSinks,
    max_pending_server_requests: usize,
    server_request_timeout: Duration,
    writer: &mut (impl AsyncWrite + Send + Unpin),
) {
    match message {
        JsonlRpcIncomingMessage::Response(response) => {
            let Some(id) = response.id.clone() else {
                return;
            };
            let Some(pending_request) = pending.remove(&id) else {
                return;
            };

            if let Some(error) = response.error {
                let _ = pending_request
                    .response_tx
                    .send(Err(CodexJsonlRpcClientError::Native(
                        CodexJsonlRpcNativeError {
                            id: Some(id),
                            code: error.code,
                            message: error.message,
                            data: error.data,
                        },
                    )));
                return;
            }

            let result = response.result.unwrap_or(JsonValue::Null);
            let _ = pending_request.response_tx.send(Ok(result));
        }
        JsonlRpcIncomingMessage::Notification(notification) => {
            dispatch_notification(notification.into(), event_sinks).await;
        }
        JsonlRpcIncomingMessage::ServerRequest(request) => {
            dispatch_server_request(
                request.into(),
                pending_server_requests,
                event_sinks,
                max_pending_server_requests,
                server_request_timeout,
                writer,
            )
            .await;
        }
    }
}

async fn dispatch_notification(
    notification: CodexJsonlRpcNotificationEvent,
    event_sinks: &CodexJsonlRpcEventSinks,
) {
    match event_sinks.notification_ingress.offer(notification).await {
        OrderedIngressOffer::Accepted => {}
        OrderedIngressOffer::CoalescedKeyLimit(notification) => emit_diagnostic(
            event_sinks,
            CodexJsonlRpcClientDiagnostic {
                kind: CodexJsonlRpcClientDiagnosticKind::NotificationChannelFull,
                message:
                    "codex progress coalescer reached its key limit; transient progress dropped"
                        .to_owned(),
                method: Some(notification.method),
                raw: Some(notification.raw),
            },
        ),
        OrderedIngressOffer::Closed(notification) => emit_diagnostic(
            event_sinks,
            CodexJsonlRpcClientDiagnostic {
                kind: CodexJsonlRpcClientDiagnosticKind::NotificationChannelClosed,
                message: "codex jsonl-rpc notification ingress is closed".to_owned(),
                method: Some(notification.method),
                raw: Some(notification.raw),
            },
        ),
    }
}

async fn dispatch_server_request(
    request: CodexJsonlRpcServerRequest,
    pending_server_requests: &mut HashMap<JsonlRpcId, PendingCodexServerRequest>,
    event_sinks: &CodexJsonlRpcEventSinks,
    max_pending_server_requests: usize,
    server_request_timeout: Duration,
    writer: &mut (impl AsyncWrite + Send + Unpin),
) {
    if pending_server_requests.contains_key(&request.id) {
        emit_diagnostic(
            event_sinks,
            CodexJsonlRpcClientDiagnostic {
                kind: CodexJsonlRpcClientDiagnosticKind::DuplicateServerRequestId,
                message: format!(
                    "duplicate codex jsonl-rpc server request id `{}`; request rejected",
                    request.id
                ),
                method: Some(request.method.clone()),
                raw: Some(request.raw.clone()),
            },
        );
        pending_server_requests.remove(&request.id);
        let _ = write_codex_server_request_terminal_error(
            writer,
            request.id,
            CodexServerRequestTerminalState::DuplicateId,
        )
        .await;
        return;
    }

    if pending_server_requests.len() >= max_pending_server_requests {
        emit_diagnostic(
            event_sinks,
            CodexJsonlRpcClientDiagnostic {
                kind: CodexJsonlRpcClientDiagnosticKind::ServerRequestChannelFull,
                message: "codex jsonl-rpc pending server request limit reached".to_owned(),
                method: Some(request.method.clone()),
                raw: Some(request.raw.clone()),
            },
        );
        let _ = write_codex_server_request_terminal_error(
            writer,
            request.id,
            CodexServerRequestTerminalState::Evicted,
        )
        .await;
        return;
    }

    pending_server_requests.insert(
        request.id.clone(),
        PendingCodexServerRequest {
            deadline: Instant::now() + server_request_timeout,
        },
    );
    if let Err(error) = event_sinks.server_request_tx.send(request).await {
        let request = error.0;
        pending_server_requests.remove(&request.id);
        emit_diagnostic(
            event_sinks,
            CodexJsonlRpcClientDiagnostic {
                kind: CodexJsonlRpcClientDiagnosticKind::ServerRequestChannelClosed,
                message: "codex jsonl-rpc server request ingress is closed".to_owned(),
                method: Some(request.method),
                raw: Some(request.raw),
            },
        );
        let _ = write_codex_server_request_terminal_error(
            writer,
            request.id,
            CodexServerRequestTerminalState::IngressClosed,
        )
        .await;
    }
}

async fn wait_for_codex_server_request_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

async fn complete_expired_codex_server_requests(
    writer: &mut (impl AsyncWrite + Send + Unpin),
    pending: &mut HashMap<JsonlRpcId, PendingCodexServerRequest>,
    now: Instant,
) {
    let expired = pending
        .iter()
        .filter_map(|(id, request)| (request.deadline <= now).then_some(id.clone()))
        .collect::<Vec<_>>();
    for id in expired {
        pending.remove(&id);
        let _ = write_codex_server_request_terminal_error(
            writer,
            id,
            CodexServerRequestTerminalState::TimedOut,
        )
        .await;
    }
}

async fn complete_all_codex_server_requests(
    writer: &mut (impl AsyncWrite + Send + Unpin),
    pending: &mut HashMap<JsonlRpcId, PendingCodexServerRequest>,
    terminal: CodexServerRequestTerminalState,
) {
    let ids = pending.drain().map(|(id, _)| id).collect::<Vec<_>>();
    for id in ids {
        let _ = write_codex_server_request_terminal_error(writer, id, terminal).await;
    }
}

async fn write_codex_server_request_terminal_error(
    writer: &mut (impl AsyncWrite + Send + Unpin),
    id: JsonlRpcId,
    terminal: CodexServerRequestTerminalState,
) -> serde_json::Result<()> {
    let (code, message) = match terminal {
        CodexServerRequestTerminalState::DuplicateId => (
            CODEX_SERVER_REQUEST_DUPLICATE_ID_CODE,
            "duplicate server request id",
        ),
        CodexServerRequestTerminalState::Evicted => (
            CODEX_SERVER_REQUEST_HANDLER_UNAVAILABLE_CODE,
            "server request capacity exhausted",
        ),
        CodexServerRequestTerminalState::IngressClosed => (
            CODEX_SERVER_REQUEST_HANDLER_UNAVAILABLE_CODE,
            "server request handler unavailable",
        ),
        CodexServerRequestTerminalState::TimedOut => (
            CODEX_SERVER_REQUEST_TIMEOUT_CODE,
            "server request timed out",
        ),
        CodexServerRequestTerminalState::Shutdown => (
            CODEX_SERVER_REQUEST_HANDLER_UNAVAILABLE_CODE,
            "server request cancelled during shutdown",
        ),
    };
    write_jsonl_rpc_message(
        writer,
        &server_request_answer_to_response(
            id,
            CodexJsonlRpcServerRequestAnswer::Error {
                code,
                message: message.to_owned(),
                data: None,
            },
        ),
    )
    .await
}

fn emit_diagnostic(
    event_sinks: &CodexJsonlRpcEventSinks,
    diagnostic: CodexJsonlRpcClientDiagnostic,
) {
    let _ = event_sinks.diagnostic_tx.try_send(diagnostic);
}

fn server_request_answer_to_response(
    id: JsonlRpcId,
    answer: CodexJsonlRpcServerRequestAnswer,
) -> JsonlRpcResponse {
    match answer {
        CodexJsonlRpcServerRequestAnswer::Result(result) => JsonlRpcResponse {
            id: Some(id),
            result: Some(result),
            error: None,
            extra: BTreeMap::new(),
            raw: JsonValue::Null,
        },
        CodexJsonlRpcServerRequestAnswer::Error {
            code,
            message,
            data,
        } => JsonlRpcResponse {
            id: Some(id),
            result: None,
            error: Some(JsonlRpcError {
                code,
                message,
                data,
                extra: BTreeMap::new(),
            }),
            extra: BTreeMap::new(),
            raw: JsonValue::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

    fn client_pair() -> (
        CodexJsonlRpcClient,
        BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
    ) {
        client_pair_with_capacities(
            DEFAULT_INCOMING_QUEUE_CAPACITY,
            DEFAULT_INCOMING_QUEUE_CAPACITY,
            DEFAULT_COMMAND_QUEUE_CAPACITY,
        )
    }

    fn client_pair_with_capacities(
        notification_capacity: usize,
        server_request_capacity: usize,
        diagnostic_capacity: usize,
    ) -> (
        CodexJsonlRpcClient,
        BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
    ) {
        let (client_stream, server_stream) = tokio::io::duplex(8192);
        let (client_read, client_write) = split(client_stream);
        let (server_read, server_write) = split(server_stream);
        let client = CodexJsonlRpcClient::new_with_channel_capacity(
            BufReader::new(client_read),
            client_write,
            notification_capacity,
            server_request_capacity,
            diagnostic_capacity,
        );
        (client, BufReader::new(server_read), server_write)
    }

    fn client_pair_with_server_request_timeout(
        timeout: Duration,
    ) -> (
        CodexJsonlRpcClient,
        BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
    ) {
        let (client_stream, server_stream) = tokio::io::duplex(8192);
        let (client_read, client_write) = split(client_stream);
        let (server_read, server_write) = split(server_stream);
        let client = CodexJsonlRpcClient::new_with_server_request_policy(
            BufReader::new(client_read),
            client_write,
            DEFAULT_INCOMING_QUEUE_CAPACITY,
            DEFAULT_INCOMING_QUEUE_CAPACITY,
            DEFAULT_COMMAND_QUEUE_CAPACITY,
            timeout,
        );
        (client, BufReader::new(server_read), server_write)
    }

    async fn read_server_line(
        reader: &mut BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    ) -> JsonValue {
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .expect("server should read request");
        serde_json::from_str(&line).expect("request should be json")
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn codex_account_probe_ready_from_fake_server() {
        let root = unique_temp_dir("codex-account-ready");
        let script = root.join("fake-codex");
        write_unix_script(
            script.as_path(),
            r#"#!/bin/sh
read initialize
printf '%s\n' '{"id":0,"result":{"userAgent":"codex/1.2.3 darwin","platformFamily":"unix"}}'
read initialized
read account
printf '%s\n' '{"id":1,"result":{"account":{"type":"chatgpt","email":"alex@example.com","displayName":"Alex","planType":"pro","id":"acct_123"},"requiresOpenaiAuth":false}}'
while read line; do :; done
"#,
        );

        let snapshot = CodexProbe::account_read(codex_account_probe_config(&root, &script)).await;

        assert_eq!(snapshot.status, CodexAccountProbeStatus::Ready);
        assert_eq!(snapshot.version.as_deref(), Some("1.2.3"));
        assert!(!snapshot.requires_openai_auth);
        let account = snapshot.account.expect("account snapshot");
        assert!(account.authenticated);
        assert_eq!(account.email.as_deref(), Some("alex@example.com"));
        assert_eq!(account.display_name.as_deref(), Some("Alex"));
        assert_eq!(account.plan.as_deref(), Some("pro"));
        assert_eq!(account.auth_method.as_deref(), Some("chatgpt"));
        assert_eq!(account.account_id.as_deref(), Some("acct_123"));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn codex_account_probe_needs_auth_from_fake_server() {
        let root = unique_temp_dir("codex-account-needs-auth");
        let script = root.join("fake-codex");
        write_unix_script(
            script.as_path(),
            r#"#!/bin/sh
read initialize
printf '%s\n' '{"id":0,"result":{"userAgent":"codex/2.0.0 darwin"}}'
read initialized
read account
printf '%s\n' '{"id":1,"result":{"requiresOpenaiAuth":true}}'
while read line; do :; done
"#,
        );

        let snapshot = CodexProbe::account_read(codex_account_probe_config(&root, &script)).await;

        assert_eq!(snapshot.status, CodexAccountProbeStatus::NeedsAuth);
        assert_eq!(snapshot.version.as_deref(), Some("2.0.0"));
        assert!(snapshot.requires_openai_auth);
        assert!(snapshot.account.is_none());
        assert_eq!(snapshot.diagnostics[0].code, "codex_probe.needs_auth");
    }

    #[tokio::test]
    async fn codex_account_probe_missing_binary() {
        let root = unique_temp_dir("codex-account-missing-binary");
        let missing_binary = root.join("definitely-missing-codex");

        let snapshot =
            CodexProbe::account_read(codex_account_probe_config(&root, &missing_binary)).await;

        assert_eq!(snapshot.status, CodexAccountProbeStatus::MissingBinary);
        assert_eq!(snapshot.diagnostics[0].code, "codex_probe.missing_binary");
        assert!(snapshot.account.is_none());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn codex_account_probe_spawn_failure() {
        let root = unique_temp_dir("codex-account-spawn-failure");
        let not_executable = root.join("not-executable-codex");
        fs::write(not_executable.as_path(), "#!/bin/sh\n").expect("write non-executable file");

        let snapshot =
            CodexProbe::account_read(codex_account_probe_config(&root, &not_executable)).await;

        assert_eq!(snapshot.status, CodexAccountProbeStatus::SpawnFailed);
        assert_eq!(snapshot.diagnostics[0].code, "codex_probe.spawn_failed");
        assert!(snapshot.account.is_none());
    }

    #[tokio::test]
    async fn codex_model_list_one_page_maps_metadata() {
        let (rpc, mut server_reader, mut server_writer) = client_pair();
        let client = CodexAppServerClient::new(rpc);
        let request =
            tokio::spawn(async move { client.list_all_models(Duration::from_secs(2)).await });

        let first_request = read_server_line(&mut server_reader).await;
        assert_eq!(first_request["method"], json!("model/list"));
        assert_eq!(first_request["params"], json!({}));
        server_writer
            .write_all(
                json!({
                    "id": first_request["id"],
                    "result": {
                        "data": [
                            {
                                "id": "gpt-5.4",
                                "model": "gpt-5.4",
                                "displayName": "GPT-5.4",
                                "description": "Flagship reasoning model",
                                "family": "gpt-5",
                                "active": true,
                                "supportedReasoningEfforts": [
                                    { "reasoningEffort": "low" },
                                    { "reasoningEffort": "high" }
                                ],
                                "inputModalities": ["text", "image"],
                                "outputModalities": ["text"],
                                "maxInputTokens": 128000,
                                "maxOutputTokens": 8192
                            }
                        ]
                    }
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .expect("write model response");
        server_writer.write_all(b"\n").await.expect("write newline");

        let models = request
            .await
            .expect("model list task should join")
            .expect("model list should succeed");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "gpt-5.4");
        assert_eq!(models[0].name.as_deref(), Some("GPT-5.4"));
        assert_eq!(models[0].effort_options, vec!["low", "high"]);
        assert_eq!(models[0].supports_reasoning, Some(true));
        assert_eq!(models[0].supports_vision, Some(true));
        assert_eq!(models[0].max_input_tokens, Some(128000));
        assert_eq!(models[0].max_output_tokens, Some(8192));
    }

    #[test]
    fn codex_model_entry_supports_string_and_object_reasoning_efforts() {
        let model: CodexModelListModel = serde_json::from_value(json!({
            "id": "gpt-5.4",
            "supportedReasoningEfforts": [
                "low",
                { "reasoningEffort": "Extra High" },
                { "id": "maximum" },
                "ultra",
                " "
            ]
        }))
        .expect("model metadata should decode");

        let snapshot = model.into_snapshot().expect("model snapshot");
        assert_eq!(
            snapshot.effort_options,
            vec!["low", "xhigh", "max", "ultra"]
        );
        assert_eq!(snapshot.supports_reasoning, Some(true));
    }

    #[tokio::test]
    async fn codex_model_list_multiple_pages_follow_cursor() {
        let (rpc, mut server_reader, mut server_writer) = client_pair();
        let client = CodexAppServerClient::new(rpc);
        let request =
            tokio::spawn(async move { client.list_all_models(Duration::from_secs(2)).await });

        let first_request = read_server_line(&mut server_reader).await;
        assert_eq!(first_request["method"], json!("model/list"));
        assert_eq!(first_request["params"], json!({}));
        server_writer
            .write_all(
                json!({
                    "id": first_request["id"],
                    "result": {
                        "data": [{ "model": "gpt-5.4", "displayName": "GPT-5.4" }],
                        "nextCursor": "cursor_2"
                    }
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .expect("write first model response");
        server_writer.write_all(b"\n").await.expect("write newline");

        let second_request = read_server_line(&mut server_reader).await;
        assert_eq!(second_request["method"], json!("model/list"));
        assert_eq!(second_request["params"], json!({"cursor": "cursor_2"}));
        server_writer
            .write_all(
                json!({
                    "id": second_request["id"],
                    "result": {
                        "data": [{ "model": "o4-mini", "displayName": "o4-mini" }]
                    }
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .expect("write second model response");
        server_writer.write_all(b"\n").await.expect("write newline");

        let models = request
            .await
            .expect("model list task should join")
            .expect("model list should succeed");
        assert_eq!(
            models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            vec!["gpt-5.4", "o4-mini"]
        );
    }

    #[tokio::test]
    async fn request_map_resolves_multiple_concurrent_requests_by_id() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        let first = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value(
                        "model/list",
                        Some(json!({"page": 1})),
                        Duration::from_secs(2),
                    )
                    .await
            })
        };
        let second = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value("account/read", Some(json!({})), Duration::from_secs(2))
                    .await
            })
        };

        let first_request = read_server_line(&mut server_reader).await;
        let second_request = read_server_line(&mut server_reader).await;
        assert_eq!(first_request["id"], json!(0));
        assert_eq!(second_request["id"], json!(1));

        server_writer
            .write_all(b"{\"id\":1,\"result\":{\"account\":\"ok\"}}\n")
            .await
            .expect("write second response");
        server_writer
            .write_all(b"{\"id\":0,\"result\":{\"models\":[\"gpt-5.4\"]}}\n")
            .await
            .expect("write first response");

        assert_eq!(
            first.await.expect("first join").expect("first response"),
            json!({"models": ["gpt-5.4"]})
        );
        assert_eq!(
            second.await.expect("second join").expect("second response"),
            json!({"account": "ok"})
        );
    }

    #[tokio::test]
    async fn request_map_returns_native_error() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value("account/read", Some(json!({})), Duration::from_secs(2))
                    .await
            })
        };

        let server_request = read_server_line(&mut server_reader).await;
        assert_eq!(server_request["id"], json!(0));
        server_writer
            .write_all(b"{\"id\":0,\"error\":{\"code\":123,\"message\":\"Needs auth\",\"data\":{\"login\":true}}}\n")
            .await
            .expect("write error response");

        let error = request
            .await
            .expect("request join")
            .expect_err("native error expected");
        assert!(matches!(
            error,
            CodexJsonlRpcClientError::Native(CodexJsonlRpcNativeError { code: 123, .. })
        ));
    }

    #[tokio::test]
    async fn request_map_timeout_removes_pending_request() {
        let (client, mut server_reader, mut server_writer) = client_pair();

        let error = client
            .request_value(
                "thread/read",
                Some(json!({"threadId": "thr_1"})),
                Duration::from_millis(20),
            )
            .await
            .expect_err("timeout expected");
        assert!(matches!(
            error,
            CodexJsonlRpcClientError::RequestTimeout { .. }
        ));

        let first_request = read_server_line(&mut server_reader).await;
        assert_eq!(first_request["id"], json!(0));
        server_writer
            .write_all(b"{\"id\":0,\"result\":{\"late\":true}}\n")
            .await
            .expect("late response should write");

        let second = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value("account/read", Some(json!({})), Duration::from_secs(2))
                    .await
            })
        };
        let second_request = read_server_line(&mut server_reader).await;
        assert_eq!(second_request["id"], json!(1));
        server_writer
            .write_all(b"{\"id\":1,\"result\":{\"ok\":true}}\n")
            .await
            .expect("write second response");

        assert_eq!(
            second.await.expect("second join").expect("second response"),
            json!({"ok": true})
        );
    }

    #[tokio::test]
    async fn request_map_rejects_duplicate_request_id() {
        let (client, mut server_reader, _server_writer) = client_pair();
        let first = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value_with_id(
                        JsonlRpcId::Number(7),
                        "thread/read",
                        Some(json!({"threadId": "thr_1"})),
                        Duration::from_secs(2),
                    )
                    .await
            })
        };

        let first_request = read_server_line(&mut server_reader).await;
        assert_eq!(first_request["id"], json!(7));

        let duplicate = client
            .request_value_with_id(
                JsonlRpcId::Number(7),
                "thread/read",
                Some(json!({"threadId": "thr_2"})),
                Duration::from_secs(2),
            )
            .await
            .expect_err("duplicate request id expected");
        assert!(matches!(
            duplicate,
            CodexJsonlRpcClientError::DuplicateRequestId {
                id: JsonlRpcId::Number(7)
            }
        ));

        client.shutdown().await.expect("shutdown should send");
        let _ = first.await.expect("first join");
    }

    #[tokio::test]
    async fn request_map_shutdown_fails_pending_requests_without_hanging() {
        let (client, mut server_reader, _server_writer) = client_pair();
        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value("thread/read", Some(json!({})), Duration::from_secs(30))
                    .await
            })
        };

        let server_request = read_server_line(&mut server_reader).await;
        assert_eq!(server_request["id"], json!(0));
        client.shutdown().await.expect("shutdown should send");

        let error = tokio::time::timeout(Duration::from_secs(2), request)
            .await
            .expect("request should finish")
            .expect("request join")
            .expect_err("pending request should fail");
        assert!(matches!(
            error,
            CodexJsonlRpcClientError::TransportClosed { .. }
        ));
    }

    #[tokio::test]
    async fn request_map_stdout_close_fails_pending_request_as_transport_closed() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value("thread/read", Some(json!({})), Duration::from_secs(30))
                    .await
            })
        };

        let server_request = read_server_line(&mut server_reader).await;
        assert_eq!(server_request["id"], json!(0));
        server_writer
            .shutdown()
            .await
            .expect("fake stdout should close");
        drop(server_writer);

        let error = tokio::time::timeout(Duration::from_secs(2), request)
            .await
            .expect("request should finish")
            .expect("request join")
            .expect_err("pending request should fail");
        assert!(matches!(
            error,
            CodexJsonlRpcClientError::TransportClosed { .. }
        ));
    }

    #[tokio::test]
    async fn notifications_deliver_unknown_method_with_raw_payload() {
        let (client, _server_reader, mut server_writer) = client_pair();
        let mut notifications = client
            .take_notification_receiver()
            .expect("notification receiver should be available");

        server_writer
            .write_all(
                b"{\"method\":\"future/notification\",\"params\":{\"value\":1},\"future\":true}\n",
            )
            .await
            .expect("write notification");

        let notification = tokio::time::timeout(Duration::from_secs(2), notifications.recv())
            .await
            .expect("notification should arrive")
            .expect("notification channel should stay open");

        assert_eq!(notification.method, "future/notification");
        assert_eq!(notification.params, Some(json!({"value": 1})));
        assert_eq!(
            notification.raw,
            json!({"method": "future/notification", "params": {"value": 1}, "future": true})
        );
    }

    #[tokio::test]
    async fn notification_ingress_preserves_events_without_blocking_request_responses() {
        let (client, mut server_reader, mut server_writer) = client_pair_with_capacities(1, 1, 4);
        let mut notifications = client
            .take_notification_receiver()
            .expect("notification receiver should be available");
        let mut diagnostics = client
            .take_diagnostic_receiver()
            .expect("diagnostic receiver should be available");

        server_writer
            .write_all(
                b"{\"method\":\"n/one\",\"params\":{}}\n{\"method\":\"n/two\",\"params\":{}}\n",
            )
            .await
            .expect("write notifications");

        let queued_notification = notifications
            .recv()
            .await
            .expect("first notification should remain queued");
        assert_eq!(queued_notification.method, "n/one");

        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value("account/read", Some(json!({})), Duration::from_secs(2))
                    .await
            })
        };
        let server_request = read_server_line(&mut server_reader).await;
        assert_eq!(server_request["id"], json!(0));
        server_writer
            .write_all(b"{\"id\":0,\"result\":{\"ok\":true}}\n")
            .await
            .expect("write response");

        assert_eq!(
            request.await.expect("request join").expect("response"),
            json!({"ok": true})
        );

        let queued_notification = notifications
            .recv()
            .await
            .expect("second notification should remain queued");
        assert_eq!(queued_notification.method, "n/two");
        assert!(
            tokio::time::timeout(Duration::from_millis(25), diagnostics.recv())
                .await
                .is_err(),
            "normal backpressure must not be reported as dropped lifecycle data"
        );
    }

    #[tokio::test]
    async fn notification_ingress_survives_delta_storm_and_retains_terminal_events() {
        let (client, mut server_reader, mut server_writer) = client_pair_with_capacities(1, 1, 4);
        let mut notifications = client
            .take_notification_receiver()
            .expect("notification receiver should be available");
        let mut diagnostics = client
            .take_diagnostic_receiver()
            .expect("diagnostic receiver should be available");

        let mut payload = String::new();
        for _ in 0..10_000 {
            payload.push_str(
                "{\"method\":\"item/agentMessage/delta\",\"params\":{\"threadId\":\"thread_1\",\"turnId\":\"turn_1\",\"itemId\":\"item_1\",\"delta\":\"x\"}}\n",
            );
        }
        payload.push_str(
            "{\"method\":\"item/completed\",\"params\":{\"threadId\":\"thread_1\",\"turnId\":\"turn_1\",\"item\":{\"id\":\"item_1\",\"type\":\"agentMessage\",\"text\":\"done\"}}}\n",
        );
        payload.push_str(
            "{\"method\":\"turn/completed\",\"params\":{\"threadId\":\"thread_1\",\"turn\":{\"id\":\"turn_1\",\"status\":\"completed\"}}}\n",
        );
        server_writer
            .write_all(payload.as_bytes())
            .await
            .expect("write delta storm");

        let request = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .request_value("account/read", Some(json!({})), Duration::from_secs(2))
                    .await
            })
        };
        let server_request = read_server_line(&mut server_reader).await;
        server_writer
            .write_all(b"{\"id\":0,\"result\":{\"ok\":true}}\n")
            .await
            .expect("write response");
        assert_eq!(server_request["id"], json!(0));
        assert_eq!(
            request.await.expect("request join").expect("response"),
            json!({"ok": true})
        );

        let mut progress_batches = 0usize;
        loop {
            let notification = notifications.recv().await.expect("notification");
            if notification.method == "item/completed" {
                break;
            }
            assert_eq!(notification.method, "item/agentMessage/delta");
            progress_batches = progress_batches.saturating_add(1);
        }
        assert!(progress_batches > 0);
        assert_eq!(
            notifications.recv().await.expect("turn completion").method,
            "turn/completed"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), diagnostics.recv())
                .await
                .is_err(),
            "delta coalescing must not report lifecycle loss"
        );
    }

    #[tokio::test]
    async fn server_requests_can_be_answered_once() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = client
            .take_server_request_receiver()
            .expect("server request receiver should be available");

        server_writer
            .write_all(b"{\"method\":\"command/approval/request\",\"id\":42,\"params\":{\"command\":\"ls\"},\"future\":true}\n")
            .await
            .expect("write server request");

        let request = tokio::time::timeout(Duration::from_secs(2), server_requests.recv())
            .await
            .expect("server request should arrive")
            .expect("server request channel should stay open");
        assert_eq!(request.id, JsonlRpcId::Number(42));
        assert_eq!(request.method, "command/approval/request");
        assert_eq!(request.params, Some(json!({"command": "ls"})));
        assert_eq!(
            request.raw,
            json!({"method": "command/approval/request", "id": 42, "params": {"command": "ls"}, "future": true})
        );

        client
            .respond_to_server_request(request.id.clone(), json!({"decision": "accept"}))
            .await
            .expect("server request response should write");
        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!(42));
        assert_eq!(response["result"], json!({"decision": "accept"}));

        let error = client
            .respond_to_server_request(request.id, json!({"decision": "accept"}))
            .await
            .expect_err("server request can only be answered once");
        assert!(matches!(
            error,
            CodexJsonlRpcClientError::ServerRequestNotPending {
                id: JsonlRpcId::Number(42)
            }
        ));
    }

    #[tokio::test]
    async fn command_approval_request_decodes_permissively() {
        let request = CodexJsonlRpcServerRequest {
            id: JsonlRpcId::Number(42),
            method: "item/commandExecution/requestApproval".to_owned(),
            params: Some(json!({
                "approvalId": "approval-1",
                "command": "cargo check",
                "argv": ["cargo", "check"],
                "commandActions": [
                    { "type": "read", "path": "/tmp/Cargo.toml" }
                ],
                "cwd": "/tmp/project",
                "reason": "requires write access",
                "threadId": "codex-thread-1",
                "turnId": "codex-turn-1",
                "itemId": "codex-item-1",
                "startedAtMs": 1234,
                "futureField": true
            })),
            raw: json!({
                "id": 42,
                "method": "item/commandExecution/requestApproval"
            }),
        };

        let decoded = decode_codex_command_approval_request(&request);

        assert_eq!(decoded.native_request_id, "42");
        assert_eq!(decoded.native_request_id_json, json!(42));
        assert_eq!(decoded.approval_id.as_deref(), Some("approval-1"));
        assert_eq!(decoded.command.as_deref(), Some("cargo check"));
        assert_eq!(decoded.argv, vec!["cargo", "check"]);
        assert_eq!(decoded.cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(decoded.reason.as_deref(), Some("requires write access"));
        assert_eq!(decoded.native_thread_id.as_deref(), Some("codex-thread-1"));
        assert_eq!(decoded.native_turn_id.as_deref(), Some("codex-turn-1"));
        assert_eq!(decoded.native_item_id.as_deref(), Some("codex-item-1"));
        assert_eq!(decoded.started_at_ms, Some(1234));
        assert_eq!(decoded.raw["futureField"], json!(true));
    }

    #[tokio::test]
    async fn command_approval_accept_writes_codex_decision_payload() {
        assert_command_approval_decision_response(
            CodexCommandApprovalDecision::Accept,
            json!("accept"),
        )
        .await;
    }

    #[tokio::test]
    async fn command_approval_accept_for_session_writes_codex_decision_payload() {
        assert_command_approval_decision_response(
            CodexCommandApprovalDecision::AcceptForSession,
            json!("acceptForSession"),
        )
        .await;
    }

    #[tokio::test]
    async fn command_approval_decline_writes_codex_decision_payload() {
        assert_command_approval_decision_response(
            CodexCommandApprovalDecision::Decline,
            json!("decline"),
        )
        .await;
    }

    #[tokio::test]
    async fn command_approval_cancel_writes_decision_and_can_interrupt_turn() {
        let (rpc, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = rpc
            .take_server_request_receiver()
            .expect("server request receiver should be available");
        let client = CodexAppServerClient::new(rpc);

        server_writer
            .write_all(
                b"{\"method\":\"item/commandExecution/requestApproval\",\"id\":\"approval-1\",\"params\":{\"command\":\"rm -rf target\",\"threadId\":\"thread-1\",\"turnId\":\"turn-1\",\"itemId\":\"item-1\"}}\n",
            )
            .await
            .expect("write command approval request");

        let request = tokio::time::timeout(Duration::from_secs(2), server_requests.recv())
            .await
            .expect("server request should arrive")
            .expect("server request channel should stay open");

        client
            .respond_command_approval(request.id, CodexCommandApprovalDecision::Cancel)
            .await
            .expect("command approval response should write");

        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!("approval-1"));
        assert_eq!(response["result"], json!({"decision": "cancel"}));

        let interrupt = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .interrupt_turn("thread-1", "turn-1", Duration::from_secs(2))
                    .await
            }
        });

        let interrupt_request = read_server_line(&mut server_reader).await;
        assert_eq!(interrupt_request["method"], json!("turn/interrupt"));
        assert_eq!(
            interrupt_request["params"],
            json!({"threadId": "thread-1", "turnId": "turn-1"})
        );
        server_writer
            .write_all(
                json!({
                    "id": interrupt_request["id"],
                    "result": { "interrupted": true }
                })
                .to_string()
                .as_bytes(),
            )
            .await
            .expect("write interrupt response");
        server_writer.write_all(b"\n").await.expect("write newline");

        assert_eq!(
            interrupt
                .await
                .expect("interrupt task should join")
                .expect("interrupt request should succeed"),
            json!({ "interrupted": true })
        );
    }

    async fn assert_command_approval_decision_response(
        decision: CodexCommandApprovalDecision,
        expected_decision: JsonValue,
    ) {
        let (rpc, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = rpc
            .take_server_request_receiver()
            .expect("server request receiver should be available");
        let client = CodexAppServerClient::new(rpc);

        server_writer
            .write_all(
                b"{\"method\":\"item/commandExecution/requestApproval\",\"id\":42,\"params\":{\"command\":\"ls\",\"threadId\":\"thread-1\",\"turnId\":\"turn-1\",\"itemId\":\"item-1\"}}\n",
            )
            .await
            .expect("write command approval request");

        let request = tokio::time::timeout(Duration::from_secs(2), server_requests.recv())
            .await
            .expect("server request should arrive")
            .expect("server request channel should stay open");

        client
            .respond_command_approval(request.id, decision)
            .await
            .expect("command approval response should write");

        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!(42));
        assert_eq!(response["result"], json!({ "decision": expected_decision }));
    }

    #[tokio::test]
    async fn file_change_approval_request_decodes_permissively() {
        let request = CodexJsonlRpcServerRequest {
            id: JsonlRpcId::String("file-approval-1".to_owned()),
            method: "item/fileChange/requestApproval".to_owned(),
            params: Some(json!({
                "grantRoot": "/tmp/project",
                "changedFiles": [
                    "src/lib.rs",
                    { "path": "README.md" }
                ],
                "diff": "--- a/src/lib.rs\n+++ b/src/lib.rs\n",
                "reason": "needs write access",
                "threadId": "codex-thread-1",
                "turnId": "codex-turn-1",
                "itemId": "codex-item-1",
                "startedAtMs": 1234,
                "futureField": true
            })),
            raw: json!({
                "id": "file-approval-1",
                "method": "item/fileChange/requestApproval"
            }),
        };

        let decoded = decode_codex_file_change_approval_request(&request);

        assert_eq!(decoded.native_request_id, "file-approval-1");
        assert_eq!(decoded.native_request_id_json, json!("file-approval-1"));
        assert_eq!(decoded.grant_root.as_deref(), Some("/tmp/project"));
        assert_eq!(decoded.changed_files, vec!["src/lib.rs", "README.md"]);
        assert_eq!(decoded.reason.as_deref(), Some("needs write access"));
        assert_eq!(decoded.native_thread_id.as_deref(), Some("codex-thread-1"));
        assert_eq!(decoded.native_turn_id.as_deref(), Some("codex-turn-1"));
        assert_eq!(decoded.native_item_id.as_deref(), Some("codex-item-1"));
        assert_eq!(decoded.started_at_ms, Some(1234));
        assert_eq!(decoded.raw["futureField"], json!(true));
    }

    #[tokio::test]
    async fn file_change_approval_accept_writes_codex_decision_payload() {
        assert_file_change_approval_decision_response(
            CodexFileChangeApprovalDecision::Accept,
            json!("accept"),
        )
        .await;
    }

    #[tokio::test]
    async fn file_change_approval_decline_writes_codex_decision_payload() {
        assert_file_change_approval_decision_response(
            CodexFileChangeApprovalDecision::Decline,
            json!("decline"),
        )
        .await;
    }

    #[tokio::test]
    async fn file_change_approval_cancel_writes_codex_decision_payload() {
        assert_file_change_approval_decision_response(
            CodexFileChangeApprovalDecision::Cancel,
            json!("cancel"),
        )
        .await;
    }

    async fn assert_file_change_approval_decision_response(
        decision: CodexFileChangeApprovalDecision,
        expected_decision: JsonValue,
    ) {
        let (rpc, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = rpc
            .take_server_request_receiver()
            .expect("server request receiver should be available");

        server_writer
            .write_all(
                b"{\"method\":\"item/fileChange/requestApproval\",\"id\":84,\"params\":{\"grantRoot\":\"/tmp/project\",\"threadId\":\"thread-1\",\"turnId\":\"turn-1\",\"itemId\":\"item-1\",\"startedAtMs\":1}}\n",
            )
            .await
            .expect("write file change approval request");

        let request = tokio::time::timeout(Duration::from_secs(2), server_requests.recv())
            .await
            .expect("server request should arrive")
            .expect("server request channel should stay open");

        rpc.respond_to_server_request(request.id, codex_file_change_approval_response(decision))
            .await
            .expect("file change approval response should write");

        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!(84));
        assert_eq!(response["result"], json!({ "decision": expected_decision }));
    }

    #[tokio::test]
    async fn user_input_request_decodes_questions_permissively() {
        let request = CodexJsonlRpcServerRequest {
            id: JsonlRpcId::Number(99),
            method: "item/tool/requestUserInput".to_owned(),
            params: Some(json!({
                "threadId": "codex-thread-1",
                "turnId": "codex-turn-1",
                "itemId": "codex-item-1",
                "questions": [
                    {
                        "id": "choice",
                        "header": "Mode",
                        "question": "Choose mode",
                        "isOther": true,
                        "options": [
                            { "label": "Fast", "description": "Prefer speed" }
                        ]
                    },
                    {
                        "question": "Missing id still maps"
                    }
                ]
            })),
            raw: json!({
                "id": 99,
                "method": "item/tool/requestUserInput"
            }),
        };

        let decoded = decode_codex_user_input_request(&request);

        assert_eq!(decoded.native_request_id, "99");
        assert_eq!(decoded.native_request_id_json, json!(99));
        assert_eq!(decoded.native_thread_id.as_deref(), Some("codex-thread-1"));
        assert_eq!(decoded.native_turn_id.as_deref(), Some("codex-turn-1"));
        assert_eq!(decoded.native_item_id.as_deref(), Some("codex-item-1"));
        assert_eq!(decoded.questions.len(), 2);
        assert_eq!(decoded.questions[0].id, "choice");
        assert_eq!(decoded.questions[0].header, "Mode");
        assert!(decoded.questions[0].is_other);
        assert_eq!(decoded.questions[0].options[0].label, "Fast");
        assert_eq!(decoded.questions[1].id, "question_2");
    }

    #[tokio::test]
    async fn user_input_response_writes_codex_answers_payload() {
        let (rpc, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = rpc
            .take_server_request_receiver()
            .expect("server request receiver should be available");

        server_writer
            .write_all(
                b"{\"method\":\"item/tool/requestUserInput\",\"id\":99,\"params\":{\"threadId\":\"thread-1\",\"turnId\":\"turn-1\",\"itemId\":\"item-1\",\"questions\":[{\"id\":\"q1\",\"header\":\"Question\",\"question\":\"Answer?\"}]}}\n",
            )
            .await
            .expect("write user input request");

        let request = tokio::time::timeout(Duration::from_secs(2), server_requests.recv())
            .await
            .expect("server request should arrive")
            .expect("server request channel should stay open");
        let answers = BTreeMap::from([("q1".to_owned(), vec!["yes".to_owned()])]);
        rpc.respond_to_server_request(request.id, codex_user_input_response(answers))
            .await
            .expect("user input response should write");

        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!(99));
        assert_eq!(
            response["result"],
            json!({"answers": {"q1": {"answers": ["yes"]}}})
        );
    }

    #[tokio::test]
    async fn unknown_server_request_method_is_visible_and_can_receive_error_response() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = client
            .take_server_request_receiver()
            .expect("server request receiver should be available");

        server_writer
            .write_all(b"{\"method\":\"future/request\",\"id\":\"srv_1\",\"params\":{\"x\":1}}\n")
            .await
            .expect("write server request");

        let request = tokio::time::timeout(Duration::from_secs(2), server_requests.recv())
            .await
            .expect("server request should arrive")
            .expect("server request channel should stay open");
        assert_eq!(request.id, JsonlRpcId::String("srv_1".to_owned()));
        assert_eq!(request.method, "future/request");
        assert_eq!(request.params, Some(json!({"x": 1})));

        client
            .fail_server_request(
                request.id,
                -32601,
                "unknown server request",
                Some(json!({"method": "future/request"})),
            )
            .await
            .expect("error response should write");

        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!("srv_1"));
        assert_eq!(response["error"]["code"], json!(-32601));
        assert_eq!(
            response["error"]["message"],
            json!("unknown server request")
        );
        assert_eq!(
            response["error"]["data"],
            json!({"method": "future/request"})
        );
    }

    #[tokio::test]
    async fn codex_server_request_timeout_writes_one_bounded_error() {
        let (client, mut server_reader, mut server_writer) =
            client_pair_with_server_request_timeout(Duration::from_millis(20));
        let mut server_requests = client
            .take_server_request_receiver()
            .expect("server request receiver should be available");
        server_writer
            .write_all(b"{\"method\":\"future/request\",\"id\":91,\"params\":{}}\n")
            .await
            .expect("write server request");
        let request = server_requests.recv().await.expect("server request");
        let response =
            tokio::time::timeout(Duration::from_secs(1), read_server_line(&mut server_reader))
                .await
                .expect("timeout response should be bounded");
        assert_eq!(response["id"], json!(91));
        assert_eq!(
            response["error"]["code"],
            json!(CODEX_SERVER_REQUEST_TIMEOUT_CODE)
        );
        assert!(matches!(
            client
                .respond_to_server_request(request.id, json!({"late": true}))
                .await,
            Err(CodexJsonlRpcClientError::ServerRequestNotPending { .. })
        ));
    }

    #[tokio::test]
    async fn codex_server_request_shutdown_drains_pending_with_error() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = client
            .take_server_request_receiver()
            .expect("server request receiver should be available");
        server_writer
            .write_all(b"{\"method\":\"future/request\",\"id\":92,\"params\":{}}\n")
            .await
            .expect("write server request");
        let _request = server_requests.recv().await.expect("server request");
        client
            .shutdown()
            .await
            .expect("shutdown command should enqueue");
        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!(92));
        assert_eq!(
            response["error"]["code"],
            json!(CODEX_SERVER_REQUEST_HANDLER_UNAVAILABLE_CODE)
        );
    }

    #[tokio::test]
    async fn codex_server_request_closed_ingress_receives_error() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        drop(
            client
                .take_server_request_receiver()
                .expect("server request receiver should be available"),
        );
        server_writer
            .write_all(b"{\"method\":\"future/request\",\"id\":93,\"params\":{}}\n")
            .await
            .expect("write server request");
        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!(93));
        assert_eq!(
            response["error"]["code"],
            json!(CODEX_SERVER_REQUEST_HANDLER_UNAVAILABLE_CODE)
        );
    }

    #[tokio::test]
    async fn codex_server_request_duplicate_id_terminalizes_original_lane() {
        let (client, mut server_reader, mut server_writer) = client_pair();
        let mut server_requests = client
            .take_server_request_receiver()
            .expect("server request receiver should be available");
        server_writer
            .write_all(
                b"{\"method\":\"future/one\",\"id\":94,\"params\":{}}\n{\"method\":\"future/two\",\"id\":94,\"params\":{}}\n",
            )
            .await
            .expect("write duplicate requests");
        let original = server_requests.recv().await.expect("original request");
        let response = read_server_line(&mut server_reader).await;
        assert_eq!(response["id"], json!(94));
        assert_eq!(
            response["error"]["code"],
            json!(CODEX_SERVER_REQUEST_DUPLICATE_ID_CODE)
        );
        assert!(matches!(
            client
                .respond_to_server_request(original.id, json!({"late": true}))
                .await,
            Err(CodexJsonlRpcClientError::ServerRequestNotPending { .. })
        ));
    }

    struct FakeCodexAppServer {
        client: CodexAppServerClient,
        server_reader: BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        server_writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    }

    impl FakeCodexAppServer {
        fn new() -> Self {
            let (rpc, server_reader, server_writer) = client_pair();
            Self {
                client: CodexAppServerClient::new(rpc),
                server_reader,
                server_writer,
            }
        }

        async fn read_message(&mut self) -> JsonValue {
            read_server_line(&mut self.server_reader).await
        }

        async fn write_result_response(&mut self, id: JsonValue, result: JsonValue) {
            let payload = json!({ "id": id, "result": result }).to_string();
            self.server_writer
                .write_all(format!("{payload}\n").as_bytes())
                .await
                .expect("fake server should write result response");
        }

        async fn write_error_response(&mut self, id: JsonValue, code: i64, message: &str) {
            let payload = json!({
                "id": id,
                "error": {
                    "code": code,
                    "message": message
                }
            })
            .to_string();
            self.server_writer
                .write_all(format!("{payload}\n").as_bytes())
                .await
                .expect("fake server should write error response");
        }

        async fn write_notification(&mut self, method: &str, params: JsonValue) {
            let payload = json!({
                "method": method,
                "params": params
            })
            .to_string();
            self.server_writer
                .write_all(format!("{payload}\n").as_bytes())
                .await
                .expect("fake server should write notification");
        }
    }

    fn empty_codex_config_read_result() -> JsonValue {
        json!({
            "config": {
                "mcp_servers": {},
                "plugins": {},
                "marketplaces": {},
                "apps": null,
                "projects": null,
                "features": {
                    "apps": false,
                    "enable_mcp_apps": false,
                    "plugins": false,
                    "remote_plugin": false,
                    "skill_mcp_dependency_install": false
                }
            },
            "origins": {},
            "layers": [
                {
                    "name": { "type": "user", "file": "/managed/config.toml", "profile": null },
                    "version": "sha256:user",
                    "config": {
                        "features": {
                            "apps": false,
                            "enable_mcp_apps": false,
                            "plugins": false,
                            "remote_plugin": false,
                            "skill_mcp_dependency_install": false
                        }
                    }
                },
                {
                    "name": { "type": "project", "dotCodexFolder": "/workspace/.codex" },
                    "version": "sha256:project",
                    "config": {}
                }
            ]
        })
    }

    #[tokio::test]
    async fn codex_config_read_sends_exact_read_only_request_and_decodes_safe_evidence() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let read = tokio::spawn(async move {
            client
                .config_read("/workspace", Duration::from_secs(2))
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("config/read"));
        assert_eq!(
            request["params"],
            json!({ "cwd": "/workspace", "includeLayers": true })
        );
        fake.write_result_response(request["id"].clone(), empty_codex_config_read_result())
            .await;

        let snapshot = read
            .await
            .expect("config/read task should join")
            .expect("config/read should decode");
        assert!(snapshot.effective.mcp_server_names.is_empty());
        assert!(snapshot.effective.isolation_features.all_disabled());
        assert_eq!(snapshot.layers.len(), 2);
        assert_eq!(snapshot.layers[0].source, CodexConfigLayerSourceKind::User);
        assert_eq!(
            snapshot.layers[1].source,
            CodexConfigLayerSourceKind::Project
        );
    }

    #[tokio::test]
    async fn codex_config_read_rejects_malformed_or_missing_security_evidence() {
        for malformed in [
            json!({ "config": {}, "origins": {}, "layers": [] }),
            json!({
                "config": {
                    "mcp_servers": [],
                    "plugins": {},
                    "marketplaces": {},
                    "apps": null,
                    "projects": null,
                    "features": {}
                },
                "origins": {},
                "layers": []
            }),
            json!({
                "config": {
                    "mcp_servers": {},
                    "plugins": {},
                    "marketplaces": {},
                    "apps": null,
                    "projects": null,
                    "features": {
                        "apps": false,
                        "enable_mcp_apps": false,
                        "plugins": false,
                        "remote_plugin": false,
                        "skill_mcp_dependency_install": false
                    }
                },
                "origins": {}
            }),
        ] {
            let mut fake = FakeCodexAppServer::new();
            let client = fake.client.clone();
            let read = tokio::spawn(async move {
                client
                    .config_read("/workspace", Duration::from_secs(2))
                    .await
            });
            let request = fake.read_message().await;
            fake.write_result_response(request["id"].clone(), malformed)
                .await;
            let error = read
                .await
                .expect("config/read task should join")
                .expect_err("malformed config evidence must fail closed");
            assert!(matches!(error, CodexJsonlRpcClientError::Decode { .. }));
        }
    }

    #[tokio::test]
    async fn codex_config_read_timeout_is_bounded() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let read = tokio::spawn(async move {
            client
                .config_read("/workspace", Duration::from_millis(20))
                .await
        });
        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("config/read"));
        let error = read
            .await
            .expect("config/read task should join")
            .expect_err("missing config/read response must time out");
        assert!(matches!(
            error,
            CodexJsonlRpcClientError::RequestTimeout { .. }
        ));
    }

    #[tokio::test]
    async fn codex_config_read_thread_metadata_uses_persisted_cwd() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let read = tokio::spawn(async move {
            client
                .thread_read_metadata("native-thread", Duration::from_secs(2))
                .await
        });
        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/read"));
        assert_eq!(
            request["params"],
            json!({ "threadId": "native-thread", "includeTurns": false })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({ "thread": { "id": "native-thread", "cwd": "/persisted/workspace" } }),
        )
        .await;
        let metadata = read
            .await
            .expect("thread/read task should join")
            .expect("thread metadata should decode");
        assert_eq!(metadata.cwd, "/persisted/workspace");
    }

    #[tokio::test]
    async fn initialize_success_sends_initialize_then_initialized_and_captures_snapshot() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let initialize =
            tokio::spawn(async move { client.initialize(Duration::from_secs(2)).await });

        let initialize_request = fake.read_message().await;
        assert_eq!(initialize_request["method"], json!("initialize"));
        assert_eq!(initialize_request["id"], json!(0));
        assert_eq!(
            initialize_request["params"]["clientInfo"]["name"],
            json!("pioneer_desktop")
        );
        assert_eq!(
            initialize_request["params"]["clientInfo"]["title"],
            json!("Pioneer")
        );
        assert_eq!(
            initialize_request["params"]["clientInfo"]["version"],
            json!(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            initialize_request["params"]["capabilities"]["experimentalApi"],
            json!(true)
        );

        fake.write_result_response(
            initialize_request["id"].clone(),
            json!({
                "userAgent": "codex-test/1.2.3",
                "version": "1.2.3",
                "platformFamily": "macos",
                "platformOs": "darwin",
                "futureField": true
            }),
        )
        .await;

        let initialized = fake.read_message().await;
        assert_eq!(initialized, json!({"method": "initialized", "params": {}}));

        let snapshot = initialize
            .await
            .expect("initialize task should join")
            .expect("initialize should succeed");
        assert_eq!(snapshot.user_agent, Some("codex-test/1.2.3".to_owned()));
        assert_eq!(snapshot.version, Some("1.2.3".to_owned()));
        assert_eq!(snapshot.platform_family, Some("macos".to_owned()));
        assert_eq!(snapshot.platform_os, Some("darwin".to_owned()));
        assert_eq!(snapshot.extra.get("futureField"), Some(&json!(true)));
    }

    #[tokio::test]
    async fn initialize_error_is_classified_and_does_not_send_initialized() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let initialize =
            tokio::spawn(async move { client.initialize(Duration::from_secs(2)).await });

        let initialize_request = fake.read_message().await;
        assert_eq!(initialize_request["method"], json!("initialize"));
        fake.write_error_response(initialize_request["id"].clone(), -32000, "not logged in")
            .await;

        let error = initialize
            .await
            .expect("initialize task should join")
            .expect_err("initialize should fail");
        assert!(matches!(
            error,
            CodexJsonlRpcClientError::Native(CodexJsonlRpcNativeError { code: -32000, .. })
        ));

        let extra_message =
            tokio::time::timeout(Duration::from_millis(50), fake.read_message()).await;
        assert!(
            extra_message.is_err(),
            "initialized notification must not be sent after initialize error"
        );
    }

    #[tokio::test]
    async fn initialize_allows_unknown_notifications_after_lifecycle() {
        let mut fake = FakeCodexAppServer::new();
        let mut notifications = fake
            .client
            .rpc()
            .take_notification_receiver()
            .expect("notification receiver should be available");
        let client = fake.client.clone();
        let initialize =
            tokio::spawn(async move { client.initialize(Duration::from_secs(2)).await });

        let initialize_request = fake.read_message().await;
        fake.write_result_response(
            initialize_request["id"].clone(),
            json!({"userAgent": "codex-test/1.2.3"}),
        )
        .await;
        let initialized = fake.read_message().await;
        assert_eq!(initialized["method"], json!("initialized"));
        initialize
            .await
            .expect("initialize task should join")
            .expect("initialize should succeed");

        fake.write_notification("future/changed", json!({"ok": true}))
            .await;
        let notification = tokio::time::timeout(Duration::from_secs(2), notifications.recv())
            .await
            .expect("notification should arrive")
            .expect("notification channel should stay open");
        assert_eq!(notification.method, "future/changed");
        assert_eq!(notification.params, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn codex_thread_start_maps_request_and_response() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let start = tokio::spawn(async move {
            client
                .thread_start(
                    CodexThreadStartParams {
                        cwd: "/tmp/project".to_owned(),
                        approval_policy: "on-request".to_owned(),
                        ephemeral: false,
                        sandbox: Some("workspace-write".to_owned()),
                        permissions: None,
                        model: Some("gpt-5".to_owned()),
                        service_tier: Some("priority".to_owned()),
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/start"));
        assert_eq!(
            request["params"],
            json!({
                "cwd": "/tmp/project",
                "approvalPolicy": "on-request",
                "sandbox": "workspace-write",
                "model": "gpt-5",
                "serviceTier": "priority"
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "thread": {
                    "id": "codex-thread-started",
                    "cwd": "/tmp/project",
                    "model": "gpt-5"
                }
            }),
        )
        .await;

        let snapshot = start
            .await
            .expect("thread start task should join")
            .expect("thread start should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-started");
        assert_eq!(snapshot.cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(snapshot.model.as_deref(), Some("gpt-5"));
    }

    #[test]
    fn codex_turn_collaboration_mode_debug_redacts_developer_instructions() {
        let canary = "codex-developer-instruction-canary";
        let params = CodexCollaborationMode::default_with_developer_instructions(
            "gpt-5".to_owned(),
            Some("high".to_owned()),
            canary.to_owned(),
        );

        let debug = format!("{params:?}");
        assert!(!debug.contains(canary));
        assert!(debug.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn codex_skill_turn_start_serializes_structured_installed_path() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let turn_start = tokio::spawn(async move {
            client
                .turn_start(
                    CodexTurnStartParams {
                        thread_id: "codex-thread-existing".to_owned(),
                        input: vec![
                            CLIRuntimeTurnInputItem::Text {
                                text: "Run tests".to_owned(),
                            },
                            CLIRuntimeTurnInputItem::Skill {
                                name: "proposal-51-sentinel".to_owned(),
                                path: "/configured/skills/proposal-51-sentinel/SKILL.md".to_owned(),
                            },
                        ],
                        cwd: Some("/tmp/project".to_owned()),
                        approval_policy: Some("on-request".to_owned()),
                        sandbox_policy: Some(json!({
                            "type": "workspaceWrite",
                            "writableRoots": ["/tmp/project"],
                            "networkAccess": true
                        })),
                        permissions: None,
                        model: Some("gpt-5".to_owned()),
                        effort: Some("ultra".to_owned()),
                        personality: None,
                        summary: Some("concise".to_owned()),
                        collaboration_mode: Some(
                            CodexCollaborationMode::default_with_developer_instructions(
                                "gpt-5".to_owned(),
                                Some("ultra".to_owned()),
                                "Pioneer elevated instructions".to_owned(),
                            ),
                        ),
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("turn/start"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing",
                "input": [
                    {"type": "text", "text": "Run tests"},
                    {
                        "type": "skill",
                        "name": "proposal-51-sentinel",
                        "path": "/configured/skills/proposal-51-sentinel/SKILL.md"
                    }
                ],
                "cwd": "/tmp/project",
                "approvalPolicy": "on-request",
                "sandboxPolicy": {
                    "type": "workspaceWrite",
                    "writableRoots": ["/tmp/project"],
                    "networkAccess": true
                },
                "model": "gpt-5",
                "effort": "ultra",
                "summary": "concise",
                "collaborationMode": {
                    "mode": "default",
                    "settings": {
                        "model": "gpt-5",
                        "reasoning_effort": "ultra",
                        "developer_instructions": "Pioneer elevated instructions"
                    }
                }
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "turn": {
                    "id": "codex-turn-started",
                    "status": "inProgress"
                }
            }),
        )
        .await;

        let snapshot = turn_start
            .await
            .expect("turn start task should join")
            .expect("turn start should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-existing");
        assert_eq!(snapshot.native_turn_id, "codex-turn-started");
    }

    #[tokio::test]
    async fn codex_security_thread_start_serializes_danger_full_access() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let start = tokio::spawn(async move {
            client
                .thread_start(
                    CodexThreadStartParams {
                        cwd: "/tmp/project".to_owned(),
                        approval_policy: "never".to_owned(),
                        ephemeral: false,
                        sandbox: Some("danger-full-access".to_owned()),
                        permissions: None,
                        model: None,
                        service_tier: None,
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/start"));
        assert_eq!(
            request["params"],
            json!({
                "cwd": "/tmp/project",
                "approvalPolicy": "never",
                "sandbox": "danger-full-access"
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({ "thread": { "id": "codex-thread-full-access" } }),
        )
        .await;

        start
            .await
            .expect("thread start task should join")
            .expect("thread start should succeed");
    }

    #[tokio::test]
    async fn codex_security_turn_start_serializes_permissions_profile() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let turn_start = tokio::spawn(async move {
            client
                .turn_start(
                    CodexTurnStartParams {
                        thread_id: "codex-thread-existing".to_owned(),
                        input: vec![CLIRuntimeTurnInputItem::Text {
                            text: "Inspect project".to_owned(),
                        }],
                        cwd: Some("/tmp/project".to_owned()),
                        approval_policy: Some("on-request".to_owned()),
                        sandbox_policy: None,
                        permissions: Some(":read-only".to_owned()),
                        model: None,
                        effort: None,
                        personality: None,
                        summary: None,
                        collaboration_mode: None,
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("turn/start"));
        assert_eq!(request["params"]["approvalPolicy"], json!("on-request"));
        assert_eq!(request["params"]["permissions"], json!(":read-only"));
        assert_eq!(request["params"].get("sandboxPolicy"), None);
        fake.write_result_response(
            request["id"].clone(),
            json!({ "turn": { "id": "codex-turn-read-only" } }),
        )
        .await;

        turn_start
            .await
            .expect("turn start task should join")
            .expect("turn start should succeed");
    }

    #[tokio::test]
    async fn codex_turn_start_omits_effort_when_not_selected() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let turn_start = tokio::spawn(async move {
            client
                .turn_start(
                    CodexTurnStartParams {
                        thread_id: "codex-thread-existing".to_owned(),
                        input: vec![CLIRuntimeTurnInputItem::Text {
                            text: "Run tests".to_owned(),
                        }],
                        cwd: None,
                        approval_policy: None,
                        sandbox_policy: None,
                        permissions: None,
                        model: Some("gpt-5".to_owned()),
                        effort: None,
                        personality: None,
                        summary: None,
                        collaboration_mode: None,
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("turn/start"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing",
                "input": [{"type": "text", "text": "Run tests"}],
                "model": "gpt-5"
            })
        );
        assert!(request["params"].get("effort").is_none());
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "turn": {
                    "id": "codex-turn-started",
                    "status": "inProgress"
                }
            }),
        )
        .await;

        let snapshot = turn_start
            .await
            .expect("turn start task should join")
            .expect("turn start should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-existing");
        assert_eq!(snapshot.native_turn_id, "codex-turn-started");
    }

    #[tokio::test]
    async fn codex_review_start_maps_uncommitted_changes_request_and_response() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let review = tokio::spawn(async move {
            client
                .review_start(
                    CodexReviewStartParams {
                        thread_id: "codex-thread-existing".to_owned(),
                        delivery: CodexReviewDelivery::Inline,
                        target: CodexReviewTarget::UncommittedChanges,
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("review/start"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing",
                "delivery": "inline",
                "target": {
                    "type": "uncommittedChanges"
                }
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "turn": {
                    "id": "codex-review-turn-1",
                    "threadId": "codex-thread-existing"
                },
                "reviewThreadId": "codex-thread-existing"
            }),
        )
        .await;

        let snapshot = review
            .await
            .expect("review start task should join")
            .expect("review start should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-existing");
        assert_eq!(snapshot.review_thread_id, "codex-thread-existing");
        assert_eq!(
            snapshot.native_turn_id.as_deref(),
            Some("codex-review-turn-1")
        );
    }

    #[tokio::test]
    async fn codex_review_start_maps_custom_instruction_target() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let review = tokio::spawn(async move {
            client
                .review_start(
                    CodexReviewStartParams {
                        thread_id: "codex-thread-existing".to_owned(),
                        delivery: CodexReviewDelivery::Detached,
                        target: CodexReviewTarget::Custom {
                            instructions: "Review only security-sensitive changes".to_owned(),
                        },
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("review/start"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing",
                "delivery": "detached",
                "target": {
                    "type": "custom",
                    "instructions": "Review only security-sensitive changes"
                }
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "turn": {
                    "id": "codex-review-turn-custom",
                    "threadId": "codex-review-thread-custom"
                },
                "reviewThreadId": "codex-review-thread-custom"
            }),
        )
        .await;

        let snapshot = review
            .await
            .expect("review start task should join")
            .expect("review start should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-review-thread-custom");
        assert_eq!(snapshot.review_thread_id, "codex-review-thread-custom");
        assert_eq!(
            snapshot.native_turn_id.as_deref(),
            Some("codex-review-turn-custom")
        );
    }

    #[tokio::test]
    async fn codex_compaction_start_maps_request_and_response() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let compact = tokio::spawn(async move {
            client
                .thread_compact_start(
                    CodexThreadCompactStartParams {
                        thread_id: "codex-thread-compact".to_owned(),
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/compact/start"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-compact"
            })
        );
        fake.write_result_response(request["id"].clone(), json!({}))
            .await;

        let snapshot = compact
            .await
            .expect("compaction start task should join")
            .expect("compaction start should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-compact");
        assert_eq!(snapshot.raw, json!({}));
    }

    #[tokio::test]
    async fn codex_compaction_start_surfaces_native_error() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let compact = tokio::spawn(async move {
            client
                .thread_compact_start(
                    CodexThreadCompactStartParams {
                        thread_id: "codex-thread-missing".to_owned(),
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/compact/start"));
        fake.write_error_response(request["id"].clone(), -32000, "thread is not loaded")
            .await;

        let error = compact
            .await
            .expect("compaction start task should join")
            .expect_err("native compaction error should propagate");
        assert!(error.to_string().contains("thread is not loaded"));
    }

    #[tokio::test]
    async fn codex_thread_name_set_maps_request_and_response() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let rename = tokio::spawn(async move {
            client
                .thread_name_set(
                    CodexThreadNameSetParams {
                        thread_id: "codex-thread-existing".to_owned(),
                        name: "Bug bash notes".to_owned(),
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/name/set"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing",
                "name": "Bug bash notes"
            })
        );
        fake.write_result_response(request["id"].clone(), json!({}))
            .await;

        let snapshot = rename
            .await
            .expect("rename task should join")
            .expect("rename should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-existing");
        assert_eq!(snapshot.raw, json!({}));
    }

    #[tokio::test]
    async fn codex_thread_fork_maps_request_and_response() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let fork = tokio::spawn(async move {
            client
                .thread_fork(
                    CodexThreadForkParams {
                        thread_id: "codex-thread-existing".to_owned(),
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/fork"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing"
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "thread": {
                    "id": "codex-thread-fork",
                    "sessionId": "codex-thread-existing",
                    "forkedFromId": "codex-thread-existing"
                }
            }),
        )
        .await;

        let snapshot = fork
            .await
            .expect("fork task should join")
            .expect("fork should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-fork");
    }

    #[tokio::test]
    async fn codex_turn_steer_maps_request_and_response() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let steer = tokio::spawn(async move {
            client
                .turn_steer(
                    CodexTurnSteerParams {
                        thread_id: "codex-thread-existing".to_owned(),
                        expected_turn_id: "codex-turn-running".to_owned(),
                        input: vec![CLIRuntimeTurnInputItem::Text {
                            text: "Focus on the failing test".to_owned(),
                        }],
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("turn/steer"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing",
                "expectedTurnId": "codex-turn-running",
                "input": [
                    {
                        "type": "text",
                        "text": "Focus on the failing test"
                    }
                ]
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "turnId": "codex-turn-running"
            }),
        )
        .await;

        let snapshot = steer
            .await
            .expect("steer task should join")
            .expect("steer should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-existing");
        assert_eq!(snapshot.native_turn_id, "codex-turn-running");
    }

    #[tokio::test]
    async fn codex_thread_resume_maps_request_and_response() {
        let mut fake = FakeCodexAppServer::new();
        let client = fake.client.clone();
        let resume = tokio::spawn(async move {
            client
                .thread_resume(
                    "codex-thread-existing",
                    CodexThreadStartParams {
                        cwd: "/tmp/project".to_owned(),
                        approval_policy: "never".to_owned(),
                        ephemeral: false,
                        sandbox: Some("danger-full-access".to_owned()),
                        permissions: None,
                        model: None,
                        service_tier: None,
                    },
                    Duration::from_secs(2),
                )
                .await
        });

        let request = fake.read_message().await;
        assert_eq!(request["method"], json!("thread/resume"));
        assert_eq!(
            request["params"],
            json!({
                "threadId": "codex-thread-existing",
                "cwd": "/tmp/project",
                "approvalPolicy": "never",
                "sandbox": "danger-full-access"
            })
        );
        fake.write_result_response(
            request["id"].clone(),
            json!({
                "threadId": "codex-thread-existing",
                "cwd": "/tmp/project",
                "model": "gpt-5.1"
            }),
        )
        .await;

        let snapshot = resume
            .await
            .expect("thread resume task should join")
            .expect("thread resume should succeed");
        assert_eq!(snapshot.native_thread_id, "codex-thread-existing");
        assert_eq!(snapshot.cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(snapshot.model.as_deref(), Some("gpt-5.1"));
    }

    fn codex_account_probe_config(root: &Path, executable: &Path) -> CodexAccountProbeConfig {
        CodexAccountProbeConfig {
            executable: executable.to_string_lossy().into_owned(),
            home_path: root.join("codex-home").to_string_lossy().into_owned(),
            shadow_home_path: None,
            cwd: Some(root.to_path_buf()),
            home_dir: None,
            env: SensitiveEnvironment::new(),
            initialize_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            shutdown_grace: Duration::from_millis(100),
            stderr_ring_lines: 16,
        }
    }

    fn codex_overlay_identity(thread_id: &str, generation: u64) -> CodexGenerationOverlayIdentity {
        CodexGenerationOverlayIdentity::new(
            "workspace-a",
            "codex-personal",
            thread_id,
            "gateway-boot-a",
            generation,
        )
        .expect("overlay identity should be valid")
    }

    fn codex_overlay_mcp_artifact(
        bootstrap_path: &Path,
        manifest_byte: char,
    ) -> CodexManagedMcpConfigArtifact {
        let hash = manifest_byte.to_string().repeat(64);
        serialize_codex_managed_mcp_config(CodexManagedMcpConfigInput {
            semantic: CodexManagedMcpSemanticInput {
                canonical_manifest_hash: hash.clone(),
                provider_manifest_hash: hash.clone(),
                provider_contract_fingerprint: "f".repeat(64),
                overlay_policy_version: CodexHomeOverlayPolicy::v1().version,
                tools: vec![CodexManagedMcpToolIdentity {
                    canonical_callable_name: "mcp__server__tool".to_owned(),
                    canonical_schema_fingerprint: "1".repeat(64),
                    transformed_schema_fingerprint: "2".repeat(64),
                    transform_contract_fingerprint: "3".repeat(64),
                    transformed_fingerprint: "4".repeat(64),
                }],
            },
            limits: CodexManagedMcpConfigLimits { max_tools: 512 },
            helper_path: Some(PathBuf::from("/opt/pioneer/pioneer")),
            bootstrap_path: Some(bootstrap_path.to_path_buf()),
        })
        .expect("managed overlay MCP artifact")
    }

    #[cfg(unix)]
    fn write_unix_script(path: &Path, content: &str) {
        fs::write(path, content).expect("write fake executable");
        let mut permissions = fs::metadata(path).expect("script metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("script permissions");
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "pioneer-cli-agent-runtime-{prefix}-{nanos}-{}",
            std::process::id()
        ));
        fs::create_dir_all(path.as_path()).expect("create temp dir");
        path
    }

    #[test]
    fn codex_home_direct_layout_uses_shared_home_as_effective_home() {
        let root = unique_temp_dir("codex-home-direct");
        let user_home = root.join("user-home");
        fs::create_dir_all(user_home.as_path()).expect("create user home");

        let layout = resolve_codex_home_layout("~/.codex", None, Some(user_home.as_path()))
            .expect("direct layout should resolve");

        assert_eq!(layout.mode, CodexHomeLayoutMode::Direct);
        assert_eq!(layout.shared_home_path, user_home.join(".codex"));
        assert_eq!(layout.effective_home_path, user_home.join(".codex"));
        assert_eq!(
            layout.continuation_key,
            format!("codex:home:{}", user_home.join(".codex").display())
        );
        materialize_codex_shadow_home(&layout).expect("direct layout materialization is a noop");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn codex_app_server_process_config_uses_plain_app_server() {
        let root = unique_temp_dir("codex-app-server-plain");
        let executable = root.join("fake-codex");
        let config = codex_account_probe_config(root.as_path(), executable.as_path());

        let process_config =
            codex_app_server_process_config(&config).expect("process config should resolve");

        assert_eq!(process_config.args, vec!["app-server".to_owned()]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_home_auth_overlay_projects_only_v1_allowlist_and_private_auth() {
        let root = unique_temp_dir("codex-home-auth-overlay");
        let shared = root.join("shared");
        let shadow = root.join("shadow");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        fs::create_dir_all(shadow.as_path()).expect("create shadow home");
        fs::write(shared.join("config.toml"), "shared-config").expect("write shared config");
        fs::create_dir_all(shared.join("plugins")).expect("create user plugins");
        fs::create_dir_all(shared.join("cache")).expect("create user cache");
        fs::write(shared.join("auth.json"), "shared-auth").expect("write shared auth");
        fs::write(shared.join("models_cache.json"), "shared-models")
            .expect("write shared models cache");
        fs::write(shadow.join("auth.json"), "shadow-auth").expect("write shadow auth");
        fs::write(shadow.join("models_cache.json"), "shadow-models")
            .expect("write shadow models cache");

        let layout = resolve_codex_home_layout(
            shared.to_string_lossy().as_ref(),
            Some(shadow.to_string_lossy().as_ref()),
            None,
        )
        .expect("overlay layout should resolve");
        materialize_codex_shadow_home(&layout).expect("overlay should materialize");

        assert_eq!(layout.mode, CodexHomeLayoutMode::AuthOverlay);
        assert_symlink_target(
            shadow.join("sessions").as_path(),
            shared.join("sessions").as_path(),
        );
        assert_symlink_target(
            shadow.join("skills").as_path(),
            shared.join("skills").as_path(),
        );
        assert!(!shadow.join("cache").exists());
        assert!(!shadow.join("plugins").exists());
        assert!(!shadow.join("config.toml").exists());
        assert!(
            !fs::symlink_metadata(shadow.join("auth.json"))
                .expect("shadow auth metadata")
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read_to_string(shadow.join("auth.json")).expect("read shadow auth"),
            "shadow-auth"
        );
        assert!(
            !fs::symlink_metadata(shadow.join("models_cache.json"))
                .expect("shadow models metadata")
                .file_type()
                .is_symlink()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_home_auth_overlay_rejects_existing_non_symlink_shared_entry() {
        let root = unique_temp_dir("codex-home-conflict");
        let shared = root.join("shared");
        let shadow = root.join("shadow");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        fs::create_dir_all(shadow.join("sessions")).expect("create conflicting shadow sessions");

        let layout = resolve_codex_home_layout(
            shared.to_string_lossy().as_ref(),
            Some(shadow.to_string_lossy().as_ref()),
            None,
        )
        .expect("overlay layout should resolve");
        let error = materialize_codex_shadow_home(&layout)
            .expect_err("existing real shared entry should block materialization");

        assert!(
            error
                .to_string()
                .contains("already exists and is not a symlink")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_home_auth_overlay_rejects_private_auth_symlink() {
        let root = unique_temp_dir("codex-home-private-auth");
        let shared = root.join("shared");
        let shadow = root.join("shadow");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        fs::create_dir_all(shadow.as_path()).expect("create shadow home");
        fs::write(shared.join("auth.json"), "shared-auth").expect("write shared auth");
        std::os::unix::fs::symlink(shared.join("auth.json"), shadow.join("auth.json"))
            .expect("create bad private auth symlink");

        let layout = resolve_codex_home_layout(
            shared.to_string_lossy().as_ref(),
            Some(shadow.to_string_lossy().as_ref()),
            None,
        )
        .expect("overlay layout should resolve");
        let error = materialize_codex_shadow_home(&layout)
            .expect_err("private auth symlink should block materialization");

        assert!(error.to_string().contains("must be a real file"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_overlay_policy_is_versioned_owner_only_and_default_deny() {
        let root = unique_temp_dir("codex-overlay-policy");
        let shared = root.join("shared");
        let account = root.join("account");
        let managed = root.join("managed");
        fs::create_dir_all(shared.join("plugins")).expect("create plugins");
        fs::create_dir_all(shared.join("marketplaces")).expect("create marketplaces");
        fs::create_dir_all(shared.join("worktrees")).expect("create worktrees");
        fs::write(shared.join("config.toml"), "[mcp_servers.host]").expect("write host config");
        fs::create_dir_all(account.as_path()).expect("create account home");
        fs::write(account.join("auth.json"), "account-secret").expect("write account auth");
        fs::write(account.join("models_cache.json"), "models").expect("write account models");
        let mut config = codex_account_probe_config(root.as_path(), Path::new("codex"));
        config.home_path = shared.to_string_lossy().into_owned();
        config.shadow_home_path = Some(account.to_string_lossy().into_owned());

        let (process, descriptor) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-a", 1),
        )
        .expect("generation overlay should materialize");
        let overlay = descriptor.effective_home_path.as_path();

        assert_eq!(descriptor.policy_version, 1);
        assert_eq!(CodexHomeOverlayPolicy::v1().version, 1);
        assert_eq!(
            process.env.expose("CODEX_SQLITE_HOME"),
            Some(shared.to_string_lossy().as_ref())
        );
        assert_eq!(
            process.home_path.as_deref(),
            Some(overlay.to_string_lossy().as_ref())
        );
        for entry_name in CODEX_SHARED_HOME_ENTRY_NAMES_V1 {
            assert_symlink_target(
                overlay.join(entry_name).as_path(),
                shared.join(entry_name).as_path(),
            );
        }
        for denied in ["plugins", "marketplaces", "worktrees", "host-unknown"] {
            assert!(
                !overlay.join(denied).exists(),
                "denied host entry `{denied}` was projected"
            );
        }
        assert_eq!(
            fs::read_to_string(overlay.join("config.toml")).expect("read generated config"),
            std::str::from_utf8(CODEX_PHASE_00_CONTROLLED_CONFIG_V1)
                .expect("controlled config is UTF-8")
        );
        assert_eq!(
            fs::read_to_string(overlay.join("auth.json")).expect("read account material"),
            "account-secret"
        );
        assert_eq!(
            fs::metadata(overlay)
                .expect("overlay metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(overlay.join("config.toml"))
                .expect("config metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        cleanup_codex_generation_overlay(&descriptor).expect("clean overlay");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_overlay_concurrent_threads_and_generations_are_disjoint_and_idempotent() {
        let root = unique_temp_dir("codex-overlay-concurrency");
        let shared = root.join("shared");
        let managed = root.join("managed");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        let mut config = codex_account_probe_config(root.as_path(), Path::new("codex"));
        config.home_path = shared.to_string_lossy().into_owned();

        let (_, first) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-a", 1),
        )
        .expect("first overlay");
        let (_, first_again) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-a", 1),
        )
        .expect("idempotent overlay");
        let (_, second_thread) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-b", 2),
        )
        .expect("second-thread overlay");
        let (_, second_generation) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-a", 3),
        )
        .expect("second-generation overlay");

        assert_eq!(first, first_again);
        assert_ne!(first.effective_home_path, second_thread.effective_home_path);
        assert_ne!(
            first.effective_home_path,
            second_generation.effective_home_path
        );
        for descriptor in [&first, &second_thread, &second_generation] {
            assert_symlink_target(
                descriptor.effective_home_path.join("sessions").as_path(),
                shared.join("sessions").as_path(),
            );
        }

        cleanup_codex_generation_overlay(&first).expect("clean first overlay");
        cleanup_codex_generation_overlay(&first_again).expect("idempotent cleanup");
        cleanup_codex_generation_overlay(&second_thread).expect("clean second thread");
        cleanup_codex_generation_overlay(&second_generation).expect("clean second generation");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_overlay_mcp_artifacts_are_disjoint_with_shared_resume_continuity() {
        let root = unique_temp_dir("codex-overlay-mcp-concurrency");
        let shared = root.join("shared");
        let managed = root.join("managed");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        let mut config = codex_account_probe_config(root.as_path(), Path::new("codex"));
        config.home_path = shared.to_string_lossy().into_owned();

        let (_, mut thread_a) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-a", 1),
        )
        .expect("thread A overlay");
        let (_, mut thread_b) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-b", 1),
        )
        .expect("thread B overlay");
        let bootstrap_a = thread_a
            .effective_home_path
            .join("bootstrap/session-a/bootstrap.json");
        let bootstrap_b = thread_b
            .effective_home_path
            .join("bootstrap/session-b/bootstrap.json");
        fs::create_dir_all(bootstrap_a.parent().expect("bootstrap A parent"))
            .expect("create bootstrap A parent");
        fs::create_dir_all(bootstrap_b.parent().expect("bootstrap B parent"))
            .expect("create bootstrap B parent");
        fs::write(bootstrap_a.as_path(), "bootstrap-a").expect("write bootstrap A");
        fs::write(bootstrap_b.as_path(), "bootstrap-b").expect("write bootstrap B");

        let artifact_a = codex_overlay_mcp_artifact(bootstrap_a.as_path(), 'a');
        let artifact_b = codex_overlay_mcp_artifact(bootstrap_b.as_path(), 'a');
        let staged_a = stage_codex_generation_mcp_config(
            &mut thread_a,
            &artifact_a,
            Some(bootstrap_a.as_path()),
        )
        .expect("stage thread A MCP artifact");
        let staged_b = stage_codex_generation_mcp_config(
            &mut thread_b,
            &artifact_b,
            Some(bootstrap_b.as_path()),
        )
        .expect("stage thread B MCP artifact");
        let staged_a_again = stage_codex_generation_mcp_config(
            &mut thread_a,
            &artifact_a,
            Some(bootstrap_a.as_path()),
        )
        .expect("identical staging is idempotent");

        assert_eq!(staged_a, staged_a_again);
        assert_ne!(staged_a.config_path, staged_b.config_path);
        assert_ne!(staged_a.bootstrap_path, staged_b.bootstrap_path);
        assert_eq!(
            staged_a.semantic_restart_fingerprint, staged_b.semantic_restart_fingerprint,
            "generation-local paths must not affect semantic session equality"
        );
        assert_ne!(
            staged_a.config_artifact_digest, staged_b.config_artifact_digest,
            "generation-local paths remain part of the staged artifact identity"
        );
        let config_a = fs::read_to_string(staged_a.config_path.as_path()).expect("read config A");
        let config_b = fs::read_to_string(staged_b.config_path.as_path()).expect("read config B");
        assert!(config_a.contains(bootstrap_a.to_string_lossy().as_ref()));
        assert!(!config_a.contains(bootstrap_b.to_string_lossy().as_ref()));
        assert!(config_b.contains(bootstrap_b.to_string_lossy().as_ref()));
        assert!(!config_b.contains(bootstrap_a.to_string_lossy().as_ref()));
        for descriptor in [&thread_a, &thread_b] {
            for shared_entry in ["sessions", "archived_sessions", "sqlite"] {
                assert_symlink_target(
                    descriptor.effective_home_path.join(shared_entry).as_path(),
                    shared.join(shared_entry).as_path(),
                );
            }
            for forbidden in ["plugins", "marketplaces", "worktrees", "host-config"] {
                assert!(!descriptor.effective_home_path.join(forbidden).exists());
            }
        }

        cleanup_codex_generation_overlay(&thread_a).expect("clean thread A");
        cleanup_codex_generation_overlay(&thread_b).expect("clean thread B");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_overlay_mcp_collision_and_external_bootstrap_fail_closed() {
        let root = unique_temp_dir("codex-overlay-mcp-collision");
        let shared = root.join("shared");
        let managed = root.join("managed");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        let mut config = codex_account_probe_config(root.as_path(), Path::new("codex"));
        config.home_path = shared.to_string_lossy().into_owned();
        let (_, mut descriptor) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-a", 1),
        )
        .expect("generation overlay");
        let bootstrap = descriptor
            .effective_home_path
            .join("bootstrap/session/bootstrap.json");
        fs::create_dir_all(bootstrap.parent().expect("bootstrap parent"))
            .expect("create bootstrap parent");
        fs::write(bootstrap.as_path(), "bootstrap").expect("write bootstrap");
        let first = codex_overlay_mcp_artifact(bootstrap.as_path(), 'a');
        stage_codex_generation_mcp_config(&mut descriptor, &first, Some(bootstrap.as_path()))
            .expect("stage first artifact");

        let replacement = codex_overlay_mcp_artifact(bootstrap.as_path(), 'b');
        let collision = stage_codex_generation_mcp_config(
            &mut descriptor,
            &replacement,
            Some(bootstrap.as_path()),
        )
        .expect_err("same generation cannot accept another projection");
        assert!(collision.to_string().contains("different projection"));

        let (_, mut external_descriptor) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-b", 1),
        )
        .expect("external-bootstrap overlay");
        let external_bootstrap = root.join("outside-bootstrap.json");
        fs::write(external_bootstrap.as_path(), "outside").expect("write outside bootstrap");
        let external_artifact = codex_overlay_mcp_artifact(external_bootstrap.as_path(), 'a');
        let outside = stage_codex_generation_mcp_config(
            &mut external_descriptor,
            &external_artifact,
            Some(external_bootstrap.as_path()),
        )
        .expect_err("bootstrap outside the generation overlay must fail");
        assert!(
            outside
                .to_string()
                .contains("outside its generation overlay")
        );

        cleanup_codex_generation_overlay(&descriptor).expect("clean first overlay");
        cleanup_codex_generation_overlay(&external_descriptor)
            .expect("clean external-bootstrap overlay");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_overlay_mcp_attestation_rejects_config_drift_before_thread_start() {
        let root = unique_temp_dir("codex-overlay-mcp-attestation");
        let shared = root.join("shared");
        let managed = root.join("managed");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        let mut config = codex_account_probe_config(root.as_path(), Path::new("codex"));
        config.home_path = shared.to_string_lossy().into_owned();
        let (_, mut descriptor) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-a", 1),
        )
        .expect("generation overlay");
        let bootstrap = descriptor
            .effective_home_path
            .join("bootstrap/session/bootstrap.json");
        fs::create_dir_all(bootstrap.parent().expect("bootstrap parent"))
            .expect("create bootstrap parent");
        fs::write(bootstrap.as_path(), "bootstrap").expect("write bootstrap");
        let artifact = codex_overlay_mcp_artifact(bootstrap.as_path(), 'a');
        stage_codex_generation_mcp_config(&mut descriptor, &artifact, Some(bootstrap.as_path()))
            .expect("stage artifact");
        verify_codex_generation_mcp_config(&descriptor).expect("exact artifact verifies");

        fs::write(
            descriptor.effective_home_path.join("config.toml"),
            "[mcp_servers.malicious]\ncommand = '/sentinel'\n",
        )
        .expect("replace staged config");
        let error = verify_codex_generation_mcp_config(&descriptor)
            .expect_err("staged config drift must fail before config/read");
        assert!(error.to_string().contains("digest verification"));

        cleanup_codex_generation_overlay(&descriptor).expect("clean overlay");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_overlay_stale_cleanup_cannot_delete_replacement_path() {
        let root = unique_temp_dir("codex-overlay-replacement");
        let shared = root.join("shared");
        let managed = root.join("managed");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        let mut config = codex_account_probe_config(root.as_path(), Path::new("codex"));
        config.home_path = shared.to_string_lossy().into_owned();
        let identity = codex_overlay_identity("thread-a", 1);

        let (_, stale_descriptor) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            identity.clone(),
        )
        .expect("first materialization");
        cleanup_codex_generation_overlay(&stale_descriptor).expect("clean first materialization");
        let (_, replacement_descriptor) =
            codex_generation_app_server_process_config(&config, managed.as_path(), identity)
                .expect("replacement materialization");

        let error = cleanup_codex_generation_overlay(&stale_descriptor)
            .expect_err("stale cleanup must not remove replacement");
        assert!(error.to_string().contains("replacement path"));
        assert!(replacement_descriptor.effective_home_path.exists());
        cleanup_codex_generation_overlay(&replacement_descriptor).expect("clean replacement");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    #[cfg(unix)]
    fn codex_overlay_rejects_stale_directory_and_symlinked_managed_root() {
        let root = unique_temp_dir("codex-overlay-stale");
        let shared = root.join("shared");
        let managed = root.join("managed");
        fs::create_dir_all(shared.as_path()).expect("create shared home");
        let mut config = codex_account_probe_config(root.as_path(), Path::new("codex"));
        config.home_path = shared.to_string_lossy().into_owned();
        let identity = codex_overlay_identity("thread-a", 1);
        let (_, descriptor) = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            identity.clone(),
        )
        .expect("initial materialization");
        cleanup_codex_generation_overlay(&descriptor).expect("clean initial overlay");
        fs::create_dir_all(descriptor.effective_home_path.as_path())
            .expect("create stale unmarked directory");
        let error =
            codex_generation_app_server_process_config(&config, managed.as_path(), identity)
                .expect_err("stale unmarked directory must fail closed");
        assert!(error.to_string().contains("stale or unmanaged"));
        fs::remove_dir_all(managed.as_path()).expect("remove stale managed root");

        let outside = root.join("outside");
        fs::create_dir_all(outside.as_path()).expect("create outside directory");
        std::os::unix::fs::symlink(outside.as_path(), managed.as_path())
            .expect("create managed-root symlink");
        let error = codex_generation_app_server_process_config(
            &config,
            managed.as_path(),
            codex_overlay_identity("thread-b", 2),
        )
        .expect_err("symlinked managed root must fail closed");
        assert!(error.to_string().contains("must not traverse a symlink"));
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    fn assert_symlink_target(link: &Path, expected_target: &Path) {
        let metadata = fs::symlink_metadata(link).expect("symlink metadata");
        assert!(
            metadata.file_type().is_symlink(),
            "{} is not a symlink",
            link.display()
        );
        let target = fs::read_link(link).expect("read symlink");
        assert_eq!(target, expected_target);
    }
}
