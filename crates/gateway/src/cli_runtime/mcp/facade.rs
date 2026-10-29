use super::coordinator::{
    CliMcpActivationGeneration, CliMcpCoordinator, CliMcpCoordinatorError,
    CliMcpProjectionFingerprint, CliMcpProjectionGeneration, CliMcpProjectionReadiness,
};
use super::grants::CliMcpBoundGrant;
use super::limits::{
    CliMcpFacadeLimits, CliMcpFacadeProjectionLimits, CliMcpLimitConfigurationError,
    RESPONSE_ENVELOPE_RESERVE_BYTES,
};
use crate::turn_mcp::invoker::{
    NativeMcpCallIdentity, TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode,
    TurnMcpInvocationOrigin, TurnMcpInvoker,
};
use crate::turn_mcp::result::CanonicalMcpToolResult;
use async_trait::async_trait;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, watch};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const JSON_RPC_VERSION: &str = "2.0";
const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
const MCP_SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"];
const SERVER_NAME: &str = "pioneer";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_REQUEST_ID_BYTES: usize = 256;
const MAX_SAFE_DESCRIPTION_CHARS: usize = 4_096;
const MAX_UNKNOWN_CONTENT_CHARS: usize = 4_096;

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;
const FACADE_NOT_INITIALIZED: i64 = -32001;
const FACADE_INACTIVE: i64 = -32002;
const FACADE_STALE: i64 = -32003;
const FACADE_PROJECTION_MISMATCH: i64 = -32004;
const FACADE_CALL_FAILED: i64 = -32005;
const FACADE_BUSY: i64 = -32006;
const FACADE_LIMIT_EXCEEDED: i64 = -32007;
const FACADE_SHUTTING_DOWN: i64 = -32008;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CliMcpFacadeTool {
    name: String,
    description: Option<String>,
    input_schema: JsonValue,
    annotations: JsonValue,
}

impl CliMcpFacadeTool {
    pub(crate) fn new(
        name: impl Into<String>,
        description: Option<String>,
        input_schema: JsonValue,
        annotations: JsonValue,
    ) -> Result<Self, CliMcpFacadeBuildError> {
        let name = name.into();
        if name.trim().is_empty() || name.len() > 128 || name.chars().any(char::is_control) {
            return Err(CliMcpFacadeBuildError::InvalidToolName);
        }
        if !input_schema.is_object() || !annotations.is_object() {
            return Err(CliMcpFacadeBuildError::InvalidToolSchema);
        }
        validate_annotations(&annotations)?;
        let description = description.map(|value| safe_description(value.as_str()));
        Ok(Self {
            name,
            description,
            input_schema: canonical_json(&input_schema),
            annotations: canonical_json(&annotations),
        })
    }

    fn as_wire_value(&self) -> JsonValue {
        let mut value = JsonMap::new();
        value.insert("name".to_owned(), JsonValue::String(self.name.clone()));
        if let Some(description) = &self.description {
            value.insert(
                "description".to_owned(),
                JsonValue::String(description.clone()),
            );
        }
        value.insert("inputSchema".to_owned(), self.input_schema.clone());
        if self
            .annotations
            .as_object()
            .is_some_and(|annotations| !annotations.is_empty())
        {
            value.insert("annotations".to_owned(), self.annotations.clone());
        }
        JsonValue::Object(value)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CliMcpFacadeProjection {
    tools: Arc<[CliMcpFacadeTool]>,
    tool_names: Arc<HashSet<String>>,
    list_result: JsonValue,
    fingerprint: CliMcpProjectionFingerprint,
}

impl CliMcpFacadeProjection {
    pub(crate) fn new(
        tools: Vec<CliMcpFacadeTool>,
        limits: CliMcpFacadeProjectionLimits,
    ) -> Result<Self, CliMcpFacadeBuildError> {
        if limits.max_tools == 0 || limits.max_list_result_bytes == 0 {
            return Err(CliMcpFacadeBuildError::InvalidLimit);
        }
        if tools.len() > limits.max_tools {
            return Err(CliMcpFacadeBuildError::TooManyTools {
                actual: tools.len(),
                maximum: limits.max_tools,
            });
        }
        let mut tool_names = HashSet::with_capacity(tools.len());
        for tool in &tools {
            if !tool_names.insert(tool.name.clone()) {
                return Err(CliMcpFacadeBuildError::DuplicateToolName);
            }
        }

        let wire_tools = tools
            .iter()
            .map(CliMcpFacadeTool::as_wire_value)
            .collect::<Vec<_>>();
        let fingerprint_bytes = serde_json::to_vec(&canonical_json(&json!({
            "tools": wire_tools.clone(),
        })))
        .map_err(|_| CliMcpFacadeBuildError::Serialization)?;
        let fingerprint =
            CliMcpProjectionFingerprint::new(hex::encode(Sha256::digest(fingerprint_bytes)))
                .map_err(|_| CliMcpFacadeBuildError::Serialization)?;
        let list_result = json!({
            "tools": wire_tools,
            "_meta": {
                "pioneer/projectionFingerprint": fingerprint.as_str(),
                "pioneer/paginationMode": "single-page",
            }
        });
        let encoded_bytes = serde_json::to_vec(&list_result)
            .map_err(|_| CliMcpFacadeBuildError::Serialization)?
            .len();
        if encoded_bytes > limits.max_list_result_bytes {
            return Err(CliMcpFacadeBuildError::ListResultTooLarge {
                actual: encoded_bytes,
                maximum: limits.max_list_result_bytes,
            });
        }

        Ok(Self {
            tools: tools.into(),
            tool_names: Arc::new(tool_names),
            list_result,
            fingerprint,
        })
    }

    pub(crate) fn fingerprint(&self) -> &CliMcpProjectionFingerprint {
        &self.fingerprint
    }

    pub(crate) fn tools(&self) -> &[CliMcpFacadeTool] {
        &self.tools
    }

    pub(crate) fn contains_tool(&self, name: &str) -> bool {
        self.tool_names.contains(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliMcpFacadeBuildError {
    InvalidLimit,
    InvalidToolName,
    InvalidToolSchema,
    DuplicateToolName,
    TooManyTools { actual: usize, maximum: usize },
    ListResultTooLarge { actual: usize, maximum: usize },
    Serialization,
}

impl fmt::Display for CliMcpFacadeBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit => formatter.write_str("CLI MCP facade limit must be positive"),
            Self::InvalidToolName => formatter.write_str("invalid CLI MCP facade tool name"),
            Self::InvalidToolSchema => {
                formatter.write_str("CLI MCP facade tool schema/annotations must be objects")
            }
            Self::DuplicateToolName => formatter.write_str("duplicate CLI MCP facade tool name"),
            Self::TooManyTools { actual, maximum } => write!(
                formatter,
                "CLI MCP projection has {actual} tools; one-page maximum is {maximum}"
            ),
            Self::ListResultTooLarge { actual, maximum } => write!(
                formatter,
                "CLI MCP tools/list is {actual} bytes; one-page maximum is {maximum}"
            ),
            Self::Serialization => {
                formatter.write_str("failed to encode CLI MCP facade projection")
            }
        }
    }
}

impl std::error::Error for CliMcpFacadeBuildError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliMcpFacadeConfigurationError {
    Limits(CliMcpLimitConfigurationError),
    ProjectionSerialization,
    ProjectionTooLarge,
}

impl fmt::Display for CliMcpFacadeConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limits(error) => error.fmt(formatter),
            Self::ProjectionSerialization => {
                formatter.write_str("failed to encode CLI MCP facade projection")
            }
            Self::ProjectionTooLarge => {
                formatter.write_str("CLI MCP tools/list exceeds the configured facade result limit")
            }
        }
    }
}

impl std::error::Error for CliMcpFacadeConfigurationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CliMcpFacadeShutdownOutcome {
    pub(crate) drained: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CliMcpFacadeRequestContext {
    pub(crate) bound_grant: CliMcpBoundGrant,
    pub(crate) projection_generation: CliMcpProjectionGeneration,
    pub(crate) activation_generation: Option<CliMcpActivationGeneration>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CliMcpProgressNotification {
    pub(crate) progress_token: JsonValue,
    pub(crate) progress: f64,
    pub(crate) total: f64,
    pub(crate) message: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CliMcpProgressSendError;

#[async_trait]
pub(crate) trait CliMcpProgressSink: Send + Sync {
    async fn send_progress(
        &self,
        notification: CliMcpProgressNotification,
    ) -> Result<(), CliMcpProgressSendError>;
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct CliMcpNoopProgressSink;

#[cfg(test)]
#[async_trait]
impl CliMcpProgressSink for CliMcpNoopProgressSink {
    async fn send_progress(
        &self,
        _notification: CliMcpProgressNotification,
    ) -> Result<(), CliMcpProgressSendError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CliMcpRequestId {
    Number(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CliMcpLedgerKey {
    session_generation: u64,
    turn_id: String,
    request_id: CliMcpRequestId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliMcpLedgerIdentity {
    callable: String,
    arguments_hash: String,
    native_call: NativeMcpCallIdentity,
}

type CliMcpCachedOutcome = Result<JsonValue, FacadeProtocolError>;

enum CliMcpLedgerEntryState {
    InFlight,
    Completed { completed_at: Instant },
}

struct CliMcpLedgerEntry {
    identity: CliMcpLedgerIdentity,
    cancellation: CancellationToken,
    outcome: watch::Sender<Option<Arc<CliMcpCachedOutcome>>>,
    state: CliMcpLedgerEntryState,
}

#[derive(Default)]
struct CliMcpLedgerState {
    entries: HashMap<CliMcpLedgerKey, CliMcpLedgerEntry>,
}

struct CliMcpInvocationLedger {
    state: StdMutex<CliMcpLedgerState>,
    max_entries: usize,
    completed_ttl: std::time::Duration,
    active_leaders: AtomicUsize,
    drained: Notify,
}

enum CliMcpLedgerBegin {
    Leader(CliMcpLedgerLeaderReservation),
    Follower(watch::Receiver<Option<Arc<CliMcpCachedOutcome>>>),
}

struct CliMcpLedgerLeaderReservation {
    ledger: Arc<CliMcpInvocationLedger>,
    key: CliMcpLedgerKey,
    completed: bool,
}

impl CliMcpLedgerLeaderReservation {
    fn complete(mut self, outcome: CliMcpCachedOutcome) {
        self.ledger.finish(&self.key, outcome);
        self.completed = true;
    }
}

impl Drop for CliMcpLedgerLeaderReservation {
    fn drop(&mut self) {
        if !self.completed {
            // Losing the response future is not an execution cancellation ACK.
            self.ledger.finish(
                &self.key,
                Err(FacadeProtocolError::new(
                    FACADE_CALL_FAILED,
                    "MCP response was not confirmed",
                    "response_unconfirmed",
                )),
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliMcpLedgerBeginError {
    ConflictingDuplicate,
    CapacityExhausted,
}

impl CliMcpInvocationLedger {
    fn new(limits: &CliMcpFacadeLimits) -> Self {
        Self {
            state: StdMutex::new(CliMcpLedgerState::default()),
            max_entries: limits.max_ledger_entries,
            completed_ttl: limits.completed_entry_ttl,
            active_leaders: AtomicUsize::new(0),
            drained: Notify::new(),
        }
    }

    fn begin(
        self: &Arc<Self>,
        key: CliMcpLedgerKey,
        identity: CliMcpLedgerIdentity,
        cancellation: CancellationToken,
    ) -> Result<CliMcpLedgerBegin, CliMcpLedgerBeginError> {
        let mut state = lock_ledger_state(&self.state);
        purge_expired_entries(&mut state, self.completed_ttl);
        if let Some(entry) = state.entries.get(&key) {
            if entry.identity != identity {
                return Err(CliMcpLedgerBeginError::ConflictingDuplicate);
            }
            return Ok(CliMcpLedgerBegin::Follower(entry.outcome.subscribe()));
        }
        if state.entries.len() >= self.max_entries {
            evict_oldest_completed(&mut state);
        }
        if state.entries.len() >= self.max_entries {
            return Err(CliMcpLedgerBeginError::CapacityExhausted);
        }
        let (outcome, _) = watch::channel(None);
        state.entries.insert(
            key.clone(),
            CliMcpLedgerEntry {
                identity,
                cancellation,
                outcome,
                state: CliMcpLedgerEntryState::InFlight,
            },
        );
        self.active_leaders.fetch_add(1, Ordering::AcqRel);
        Ok(CliMcpLedgerBegin::Leader(CliMcpLedgerLeaderReservation {
            ledger: self.clone(),
            key,
            completed: false,
        }))
    }

    fn finish(&self, key: &CliMcpLedgerKey, outcome: CliMcpCachedOutcome) {
        let cached = Arc::new(outcome);
        {
            let mut state = lock_ledger_state(&self.state);
            if let Some(entry) = state.entries.get_mut(key) {
                // Keep the terminal outcome even when no duplicate request is
                // currently subscribed. `watch::Sender::send` discards the
                // value when the initial receiver has already been dropped,
                // which would make a later replay wait until its timeout.
                entry.outcome.send_replace(Some(cached));
                entry.state = CliMcpLedgerEntryState::Completed {
                    completed_at: Instant::now(),
                };
            }
        }
        if self.active_leaders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }

    fn cancel_request(&self, session_generation: u64, request_id: &CliMcpRequestId) {
        let state = lock_ledger_state(&self.state);
        for (key, entry) in &state.entries {
            if key.session_generation == session_generation
                && &key.request_id == request_id
                && matches!(entry.state, CliMcpLedgerEntryState::InFlight)
            {
                entry.cancellation.cancel();
            }
        }
    }

    fn cancel_all(&self) {
        let state = lock_ledger_state(&self.state);
        for entry in state.entries.values() {
            if matches!(entry.state, CliMcpLedgerEntryState::InFlight) {
                entry.cancellation.cancel();
            }
        }
    }

    async fn wait_drained(&self, maximum: std::time::Duration) -> bool {
        let wait = async {
            loop {
                let notified = self.drained.notified();
                if self.active_leaders.load(Ordering::Acquire) == 0 {
                    return;
                }
                notified.await;
            }
        };
        timeout(maximum, wait).await.is_ok()
    }

    fn clear(&self) {
        lock_ledger_state(&self.state).entries.clear();
    }
}

fn lock_ledger_state(
    state: &StdMutex<CliMcpLedgerState>,
) -> std::sync::MutexGuard<'_, CliMcpLedgerState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn purge_expired_entries(state: &mut CliMcpLedgerState, ttl: std::time::Duration) {
    let now = Instant::now();
    state.entries.retain(|_, entry| match &entry.state {
        CliMcpLedgerEntryState::InFlight => true,
        CliMcpLedgerEntryState::Completed { completed_at, .. } => {
            now.saturating_duration_since(*completed_at) < ttl
        }
    });
}

fn evict_oldest_completed(state: &mut CliMcpLedgerState) {
    let oldest = state
        .entries
        .iter()
        .filter_map(|(key, entry)| match &entry.state {
            CliMcpLedgerEntryState::Completed { completed_at, .. } => {
                Some((key.clone(), *completed_at))
            }
            CliMcpLedgerEntryState::InFlight => None,
        })
        .min_by_key(|(_, completed_at)| *completed_at)
        .map(|(key, _)| key);
    if let Some(key) = oldest {
        state.entries.remove(&key);
    }
}

impl CliMcpRequestId {
    fn parse(value: &JsonValue) -> Result<Self, ()> {
        match value {
            JsonValue::Number(value) => value.as_i64().map(Self::Number).ok_or(()),
            JsonValue::String(value) if value.len() <= MAX_REQUEST_ID_BYTES => {
                Ok(Self::String(value.clone()))
            }
            _ => Err(()),
        }
    }

    fn as_json(&self) -> JsonValue {
        match self {
            Self::Number(value) => JsonValue::Number((*value).into()),
            Self::String(value) => JsonValue::String(value.clone()),
        }
    }

    fn provider_call_id(&self) -> String {
        match self {
            Self::Number(value) => format!("number:{value}"),
            Self::String(value) => format!("string:{value}"),
        }
    }
}

#[derive(Debug, Clone)]
struct ParsedRequest {
    id: Option<CliMcpRequestId>,
    method: String,
    params: Option<JsonValue>,
}

// Host-only typed delivery receipts. This task scope follows the invocation
// future (no detached work) and never trusts result JSON or tool names.
tokio::task_local! {
    static CUA_DELIVERIES: std::cell::RefCell<Option<crate::mcp_service::CuaCliDeliveryGuard>>;
}

pub(crate) fn defer_cua_delivery(
    guard: crate::mcp_service::CuaCliDeliveryGuard,
) -> Result<(), crate::mcp_service::CuaCliDeliveryGuard> {
    let mut guard = Some(guard);
    if CUA_DELIVERIES
        .try_with(|receipts| {
            receipts.replace(guard.take());
        })
        .is_ok()
    {
        Ok(())
    } else {
        Err(guard.unwrap())
    }
}

/// Selected at adapter construction; instance names and request data do not
/// select the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMcpCallContract {
    Codex,
    Claude,
    OpenCode,
}

pub(crate) struct CliMcpToolFacade {
    coordinator: Arc<CliMcpCoordinator>,
    invoker: Arc<dyn TurnMcpInvoker>,
    call_contract: CliMcpCallContract,
    projection: CliMcpFacadeProjection,
    progress_sink: Arc<dyn CliMcpProgressSink>,
    limits: CliMcpFacadeLimits,
    initialization_state: AtomicU8,
    ledger: Arc<CliMcpInvocationLedger>,
    admission: Arc<Semaphore>,
    queued_calls: AtomicUsize,
    shutting_down: AtomicBool,
}

impl CliMcpToolFacade {
    pub(crate) fn try_new(
        coordinator: Arc<CliMcpCoordinator>,
        invoker: Arc<dyn TurnMcpInvoker>,
        projection: CliMcpFacadeProjection,
        progress_sink: Arc<dyn CliMcpProgressSink>,
        limits: CliMcpFacadeLimits,
        call_contract: CliMcpCallContract,
    ) -> Result<Arc<Self>, CliMcpFacadeConfigurationError> {
        limits
            .validate()
            .map_err(CliMcpFacadeConfigurationError::Limits)?;
        let list_result_bytes = serde_json::to_vec(&projection.list_result)
            .map_err(|_| CliMcpFacadeConfigurationError::ProjectionSerialization)?
            .len();
        if list_result_bytes
            .checked_add(RESPONSE_ENVELOPE_RESERVE_BYTES)
            .is_none_or(|bytes| bytes > limits.max_frame_bytes)
        {
            return Err(CliMcpFacadeConfigurationError::ProjectionTooLarge);
        }
        Ok(Arc::new(Self {
            coordinator,
            invoker,
            call_contract,
            projection,
            progress_sink,
            ledger: Arc::new(CliMcpInvocationLedger::new(&limits)),
            admission: Arc::new(Semaphore::new(limits.max_active_calls)),
            limits,
            initialization_state: AtomicU8::new(0),
            queued_calls: AtomicUsize::new(0),
            shutting_down: AtomicBool::new(false),
        }))
    }

    pub(crate) async fn handle_bytes(
        &self,
        context: &CliMcpFacadeRequestContext,
        bytes: &[u8],
    ) -> Option<Vec<u8>> {
        CUA_DELIVERIES
            .scope(
                std::cell::RefCell::new(None),
                self.handle_bytes_with_delivery(context, bytes),
            )
            .await
    }

    async fn handle_bytes_with_delivery(
        &self,
        context: &CliMcpFacadeRequestContext,
        bytes: &[u8],
    ) -> Option<Vec<u8>> {
        if bytes.len() > self.limits.max_frame_bytes {
            return encode_response(&json_rpc_error(
                None,
                FACADE_LIMIT_EXCEEDED,
                "MCP request frame exceeds the configured limit",
                Some(json!({"kind": "frame_too_large"})),
            ));
        }
        let message = match serde_json::from_slice::<JsonValue>(bytes) {
            Ok(message) => message,
            Err(_) => {
                return encode_response(&json_rpc_error(None, PARSE_ERROR, "parse error", None));
            }
        };
        let response = self.handle_message(context, message).await?;
        let encoded = encode_response(&response)?;
        if encoded.len() <= self.limits.max_frame_bytes {
            if response.get("result").is_some() {
                CUA_DELIVERIES.with(|receipts| {
                    if let Some(receipt) = receipts.borrow_mut().as_mut() {
                        receipt.commit();
                    }
                });
            }
            return Some(encoded);
        }
        // Execution has already completed. Preserve correlation, but never
        // suggest repeating a mutation whose result could not be delivered.
        let error = json_rpc_error(
            response.get("id").cloned(),
            FACADE_LIMIT_EXCEEDED,
            "MCP response frame exceeds the configured limit",
            Some(
                json!({"kind": "response_frame_too_large", "retryable": false,
                "outcome": "unknown", "noReplay": true}),
            ),
        );
        let encoded = encode_response(&error)?;
        (encoded.len() <= self.limits.max_frame_bytes).then_some(encoded)
    }

    pub(crate) async fn shutdown(&self) -> CliMcpFacadeShutdownOutcome {
        if !self.shutting_down.swap(true, Ordering::AcqRel) {
            self.admission.close();
            self.ledger.cancel_all();
        }
        let drained = self
            .ledger
            .wait_drained(self.limits.shutdown_drain_duration)
            .await;
        self.ledger.clear();
        CliMcpFacadeShutdownOutcome { drained }
    }

    pub(crate) async fn handle_message(
        &self,
        context: &CliMcpFacadeRequestContext,
        message: JsonValue,
    ) -> Option<JsonValue> {
        let request = match parse_request(message) {
            Ok(request) => request,
            Err(id) => {
                return Some(json_rpc_error(
                    id,
                    INVALID_REQUEST,
                    "invalid JSON-RPC request",
                    None,
                ));
            }
        };
        let is_notification = request.id.is_none();
        let response = match request.method.as_str() {
            "initialize" => self.initialize(&request),
            "notifications/initialized" => {
                self.initialized(&request);
                return None;
            }
            "notifications/cancelled" => {
                self.cancel(context, &request).await;
                return None;
            }
            "ping" => self.ping(&request),
            "tools/list" => self.list(context, &request).await,
            "tools/call" => self.call(context, &request).await,
            _ if is_notification => return None,
            _ => Err(FacadeProtocolError::new(
                METHOD_NOT_FOUND,
                "method not found",
                "method_not_found",
            )),
        };
        if is_notification {
            return None;
        }
        let id = request.id.as_ref().map(CliMcpRequestId::as_json);
        Some(match response {
            Ok(result) => json!({
                "jsonrpc": JSON_RPC_VERSION,
                "id": id,
                "result": result,
            }),
            Err(error) => json_rpc_error(
                id,
                error.code,
                error.message,
                Some(json!({"kind": error.kind})),
            ),
        })
    }

    fn initialize(&self, request: &ParsedRequest) -> Result<JsonValue, FacadeProtocolError> {
        let params = required_object(request.params.as_ref())?;
        let requested_version = params
            .get("protocolVersion")
            .and_then(JsonValue::as_str)
            .ok_or_else(invalid_params)?;
        let client_info = params
            .get("clientInfo")
            .and_then(JsonValue::as_object)
            .ok_or_else(invalid_params)?;
        if client_info
            .get("name")
            .and_then(JsonValue::as_str)
            .is_none()
            || client_info
                .get("version")
                .and_then(JsonValue::as_str)
                .is_none()
            || params
                .get("capabilities")
                .and_then(JsonValue::as_object)
                .is_none()
        {
            return Err(invalid_params());
        }
        self.initialization_state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                FacadeProtocolError::new(
                    INVALID_REQUEST,
                    "facade is already initialized",
                    "already_initialized",
                )
            })?;
        let negotiated = MCP_SUPPORTED_PROTOCOL_VERSIONS
            .contains(&requested_version)
            .then_some(requested_version)
            .unwrap_or(MCP_PROTOCOL_VERSION);
        Ok(json!({
            "protocolVersion": negotiated,
            "capabilities": {
                "tools": {},
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION,
            },
            "instructions": "Only tools explicitly selected for this Pioneer session are available.",
        }))
    }

    fn initialized(&self, request: &ParsedRequest) {
        if params_are_empty(request.params.as_ref()) {
            let _ = self.initialization_state.compare_exchange(
                1,
                2,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }

    fn ping(&self, request: &ParsedRequest) -> Result<JsonValue, FacadeProtocolError> {
        self.require_negotiated()?;
        if !params_are_empty(request.params.as_ref()) {
            return Err(invalid_params());
        }
        Ok(json!({}))
    }

    async fn list(
        &self,
        context: &CliMcpFacadeRequestContext,
        request: &ParsedRequest,
    ) -> Result<JsonValue, FacadeProtocolError> {
        self.require_initialized()?;
        let params = optional_object(request.params.as_ref())?;
        if params
            .and_then(|params| params.get("cursor"))
            .is_some_and(|cursor| !cursor.is_null())
        {
            return Err(FacadeProtocolError::new(
                INVALID_PARAMS,
                "this projection is single-page and does not accept a cursor",
                "pagination_not_supported",
            ));
        }
        let authorization = self
            .coordinator
            .authorize_list(&context.bound_grant, context.projection_generation)
            .await
            .map_err(coordinator_error)?;
        if authorization.fingerprint != *self.projection.fingerprint() {
            return Err(FacadeProtocolError::new(
                FACADE_PROJECTION_MISMATCH,
                "staged MCP projection does not match the facade list",
                "projection_fingerprint_mismatch",
            ));
        }
        match authorization.readiness {
            CliMcpProjectionReadiness::Preparing | CliMcpProjectionReadiness::Ready => {
                self.coordinator
                    .mark_projection_ready(
                        &context.bound_grant,
                        context.projection_generation,
                        self.projection.fingerprint(),
                    )
                    .await
                    .map_err(coordinator_error)?;
            }
            CliMcpProjectionReadiness::Active => {}
        }
        Ok(self.projection.list_result.clone())
    }

    async fn call(
        &self,
        context: &CliMcpFacadeRequestContext,
        request: &ParsedRequest,
    ) -> Result<JsonValue, FacadeProtocolError> {
        self.require_initialized()?;
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(shutting_down_error());
        }
        let request_id = request.id.clone().ok_or_else(|| {
            FacadeProtocolError::new(INVALID_REQUEST, "tools/call requires an id", "missing_id")
        })?;
        let activation_generation = context.activation_generation.ok_or_else(|| {
            FacadeProtocolError::new(
                FACADE_INACTIVE,
                "MCP tools/call is unavailable before active turn binding",
                "turn_not_active",
            )
        })?;
        let params = required_object(request.params.as_ref())?;
        let name = params
            .get("name")
            .and_then(JsonValue::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(invalid_params)?;
        let authorization = self
            .coordinator
            .authorize_call(&context.bound_grant, activation_generation)
            .await
            .map_err(coordinator_error)?;
        if authorization.fingerprint != *self.projection.fingerprint()
            || authorization.projection_generation != context.projection_generation
        {
            return Err(FacadeProtocolError::new(
                FACADE_PROJECTION_MISMATCH,
                "active MCP projection does not match the facade list",
                "projection_fingerprint_mismatch",
            ));
        }
        if !self.projection.contains_tool(name) {
            return Err(FacadeProtocolError::new(
                INVALID_PARAMS,
                "tool is not present in the frozen projection",
                "tool_unbound",
            ));
        }
        if params.get("task").is_some_and(|task| !task.is_null()) {
            return Err(FacadeProtocolError::new(
                INVALID_PARAMS,
                "asynchronous MCP tasks are not supported",
                "tasks_not_supported",
            ));
        }
        let wire_arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let (arguments, native_call) = parse_native_call(
            self.call_contract,
            params.get("_meta"),
            wire_arguments,
            &authorization.native_thread_id,
            &authorization.native_turn_id,
        )?;
        if !arguments.is_object() {
            return Err(invalid_params());
        }
        let admission_deadline = tokio::time::Instant::now() + self.limits.max_queue_wait;
        let progress_token = parse_progress_token(params.get("_meta"))?;
        let cancellation = authorization.cancellation.child_token();
        let process_instance = &context.bound_grant.scope().process_instance;
        let key = CliMcpLedgerKey {
            session_generation: process_instance.generation(),
            turn_id: authorization.turn_id.clone(),
            request_id: request_id.clone(),
        };
        let identity = CliMcpLedgerIdentity {
            callable: name.to_owned(),
            arguments_hash: json_fingerprint(&arguments)?,
            native_call: native_call.clone(),
        };
        let leader = match self.ledger.begin(key, identity, cancellation.clone()) {
            Ok(CliMcpLedgerBegin::Follower(receiver)) => {
                return wait_for_cached_outcome(receiver).await;
            }
            Ok(CliMcpLedgerBegin::Leader(leader)) => leader,
            Err(CliMcpLedgerBeginError::ConflictingDuplicate) => {
                return Err(FacadeProtocolError::new(
                    INVALID_REQUEST,
                    "MCP request id was reused with different input",
                    "conflicting_duplicate_request_id",
                ));
            }
            Err(CliMcpLedgerBeginError::CapacityExhausted) => {
                return Err(FacadeProtocolError::new(
                    FACADE_BUSY,
                    "CLI MCP invocation ledger capacity is exhausted",
                    "ledger_capacity_exhausted",
                ));
            }
        };

        // Keep the existing call capacity reservation through final progress,
        // even though the replay reply becomes available before that await.
        let permit = self
            .acquire_call_permit(&cancellation, admission_deadline)
            .await;
        let outcome = match &permit {
            Ok(_) => {
                self.execute_leader_call(
                    context,
                    request_id,
                    name,
                    arguments,
                    progress_token.clone(),
                    native_call,
                    admission_deadline,
                    authorization.thread_id,
                    authorization.turn_id,
                    cancellation,
                )
                .await
            }
            Err(error) => Err(error.clone()),
        };
        leader.complete(outcome.clone());
        // Cache the complete replay response before optional transport work.
        // This confirms execution/result availability, never first delivery.
        if outcome.is_ok()
            && let Some(progress_token) = progress_token
        {
            let _ = timeout(
                self.limits.max_queue_wait,
                self.progress_sink
                    .send_progress(CliMcpProgressNotification {
                        progress_token,
                        progress: 1.0,
                        total: 1.0,
                        message: "MCP tool call completed",
                    }),
            )
            .await;
        }
        drop(permit);
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_leader_call(
        &self,
        context: &CliMcpFacadeRequestContext,
        request_id: CliMcpRequestId,
        name: &str,
        arguments: JsonValue,
        progress_token: Option<JsonValue>,
        native_call: NativeMcpCallIdentity,
        admission_deadline: tokio::time::Instant,
        thread_id: String,
        turn_id: String,
        cancellation: CancellationToken,
    ) -> Result<JsonValue, FacadeProtocolError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_error());
        }

        if let Some(progress_token) = progress_token.clone() {
            let send = self
                .progress_sink
                .send_progress(CliMcpProgressNotification {
                    progress_token,
                    progress: 0.0,
                    total: 1.0,
                    message: "MCP tool call started",
                });
            let sent = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(cancelled_error()),
                result = tokio::time::timeout_at(admission_deadline, send) => match result {
                    Ok(result) => result.is_ok(),
                    Err(_) => return Err(admission_timeout_error()),
                }
            };
            if !sent {
                cancellation.cancel();
                return Err(FacadeProtocolError::new(
                    INTERNAL_ERROR,
                    "failed to send MCP progress",
                    "progress_transport_failed",
                ));
            }
        }

        let process_instance = &context.bound_grant.scope().process_instance;
        let invocation = TurnMcpInvocation {
            workspace_id: process_instance.key().workspace_id.clone(),
            thread_id,
            turn_id,
            runtime_id: Some(process_instance.key().runtime_id.clone()),
            session_generation: Some(process_instance.generation()),
            provider_call_id: request_id.provider_call_id(),
            native_call: Some(native_call),
            admission_deadline: Some(admission_deadline),
            canonical_callable_name: name.to_owned(),
            arguments,
            origin: TurnMcpInvocationOrigin::CliFacade,
        };
        // The invoker owns cancellation and the execution outcome once admitted.
        // Dropping it here can lose a committed mutation or its history write.
        // A late client cancellation does not replace an already obtained result.
        let result = self
            .invoker
            .invoke(invocation, cancellation)
            .await
            .map_err(invocation_error)?;

        let result = canonical_call_result(result)?;
        Ok(result)
    }

    async fn acquire_call_permit(
        &self,
        cancellation: &CancellationToken,
        admission_deadline: tokio::time::Instant,
    ) -> Result<OwnedSemaphorePermit, FacadeProtocolError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(shutting_down_error());
        }
        match self.admission.clone().try_acquire_owned() {
            Ok(permit) => return Ok(permit),
            Err(tokio::sync::TryAcquireError::Closed) => return Err(shutting_down_error()),
            Err(tokio::sync::TryAcquireError::NoPermits) => {}
        }
        reserve_queue_slot(&self.queued_calls, self.limits.max_queued_calls)?;
        let _queued = QueuedCallGuard {
            queued: &self.queued_calls,
        };
        let acquire = self.admission.clone().acquire_owned();
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(cancelled_error()),
            outcome = tokio::time::timeout_at(admission_deadline, acquire) => match outcome {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(_)) => Err(shutting_down_error()),
                Err(_) => Err(admission_timeout_error()),
            }
        }
    }

    async fn cancel(&self, context: &CliMcpFacadeRequestContext, request: &ParsedRequest) {
        if self.initialization_state.load(Ordering::Acquire) < 1 {
            return;
        }
        let Ok(params) = required_object(request.params.as_ref()) else {
            return;
        };
        let Some(request_id) = params
            .get("requestId")
            .and_then(|value| CliMcpRequestId::parse(value).ok())
        else {
            return;
        };
        self.ledger.cancel_request(
            context.bound_grant.scope().process_instance.generation(),
            &request_id,
        );
    }

    fn require_negotiated(&self) -> Result<(), FacadeProtocolError> {
        if self.initialization_state.load(Ordering::Acquire) >= 1 {
            Ok(())
        } else {
            Err(FacadeProtocolError::new(
                FACADE_NOT_INITIALIZED,
                "MCP facade has not negotiated initialize",
                "not_initialized",
            ))
        }
    }

    fn require_initialized(&self) -> Result<(), FacadeProtocolError> {
        if self.initialization_state.load(Ordering::Acquire) == 2 {
            Ok(())
        } else {
            Err(FacadeProtocolError::new(
                FACADE_NOT_INITIALIZED,
                "MCP facade has not received notifications/initialized",
                "not_initialized",
            ))
        }
    }
}

struct QueuedCallGuard<'a> {
    queued: &'a AtomicUsize,
}

impl Drop for QueuedCallGuard<'_> {
    fn drop(&mut self) {
        self.queued.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone)]
struct FacadeProtocolError {
    code: i64,
    message: &'static str,
    kind: &'static str,
}

impl FacadeProtocolError {
    const fn new(code: i64, message: &'static str, kind: &'static str) -> Self {
        Self {
            code,
            message,
            kind,
        }
    }
}

async fn wait_for_cached_outcome(
    mut receiver: watch::Receiver<Option<Arc<CliMcpCachedOutcome>>>,
) -> CliMcpCachedOutcome {
    loop {
        if let Some(outcome) = receiver.borrow().clone() {
            return (*outcome).clone();
        }
        if receiver.changed().await.is_err() {
            return Err(shutting_down_error());
        }
    }
}

fn reserve_queue_slot(queued: &AtomicUsize, maximum: usize) -> Result<(), FacadeProtocolError> {
    let mut current = queued.load(Ordering::Acquire);
    loop {
        if current >= maximum {
            return Err(FacadeProtocolError::new(
                FACADE_BUSY,
                "CLI MCP facade queue capacity is exhausted",
                "queue_capacity_exhausted",
            ));
        }
        match queued.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(()),
            Err(observed) => current = observed,
        }
    }
}

fn json_fingerprint(value: &JsonValue) -> Result<String, FacadeProtocolError> {
    let bytes = serde_json::to_vec(&canonical_json(value)).map_err(|_| invalid_params())?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn admission_timeout_error() -> FacadeProtocolError {
    invocation_error(TurnMcpInvocationError::new(
        TurnMcpInvocationErrorCode::TimedOut,
        "MCP native admission timed out",
    ))
}

fn cancelled_error() -> FacadeProtocolError {
    FacadeProtocolError::new(
        FACADE_CALL_FAILED,
        "MCP tool call was cancelled",
        "cancelled",
    )
}

fn shutting_down_error() -> FacadeProtocolError {
    FacadeProtocolError::new(
        FACADE_SHUTTING_DOWN,
        "CLI MCP facade is shutting down",
        "shutting_down",
    )
}

fn parse_request(message: JsonValue) -> Result<ParsedRequest, Option<JsonValue>> {
    let object = message.as_object().ok_or(None)?;
    let raw_id = object.get("id");
    let id = raw_id
        .map(CliMcpRequestId::parse)
        .transpose()
        .map_err(|()| None)?;
    if object.get("jsonrpc").and_then(JsonValue::as_str) != Some(JSON_RPC_VERSION)
        || object.contains_key("result")
        || object.contains_key("error")
    {
        return Err(id.as_ref().map(CliMcpRequestId::as_json));
    }
    let method = object
        .get("method")
        .and_then(JsonValue::as_str)
        .filter(|method| !method.is_empty())
        .ok_or_else(|| id.as_ref().map(CliMcpRequestId::as_json))?;
    let params = object.get("params").cloned();
    if params
        .as_ref()
        .is_some_and(|params| !params.is_null() && !params.is_object())
    {
        return Err(id.as_ref().map(CliMcpRequestId::as_json));
    }
    Ok(ParsedRequest {
        id,
        method: method.to_owned(),
        params,
    })
}

fn optional_object(
    params: Option<&JsonValue>,
) -> Result<Option<&JsonMap<String, JsonValue>>, FacadeProtocolError> {
    match params {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Object(params)) => Ok(Some(params)),
        _ => Err(invalid_params()),
    }
}

fn required_object(
    params: Option<&JsonValue>,
) -> Result<&JsonMap<String, JsonValue>, FacadeProtocolError> {
    optional_object(params)?.ok_or_else(invalid_params)
}

fn params_are_empty(params: Option<&JsonValue>) -> bool {
    match params {
        None | Some(JsonValue::Null) => true,
        Some(JsonValue::Object(params)) => params.is_empty(),
        _ => false,
    }
}

fn parse_progress_token(
    meta: Option<&JsonValue>,
) -> Result<Option<JsonValue>, FacadeProtocolError> {
    let Some(meta) = meta else {
        return Ok(None);
    };
    let meta = meta.as_object().ok_or_else(invalid_params)?;
    let Some(token) = meta.get("progressToken") else {
        return Ok(None);
    };
    match token {
        JsonValue::Number(value) if value.as_i64().is_some() => Ok(Some(token.clone())),
        JsonValue::String(value) if value.len() <= MAX_REQUEST_ID_BYTES => Ok(Some(token.clone())),
        _ => Err(invalid_params()),
    }
}

fn coordinator_error(error: CliMcpCoordinatorError) -> FacadeProtocolError {
    use CliMcpCoordinatorError as Error;
    match error {
        Error::CallsNotActive | Error::MissingTurn | Error::InvalidTransition => {
            FacadeProtocolError::new(FACADE_INACTIVE, "MCP turn is not active", "turn_not_active")
        }
        Error::StaleProjectionGeneration | Error::StaleActivationGeneration => {
            FacadeProtocolError::new(FACADE_STALE, "MCP generation is stale", "stale_generation")
        }
        Error::ProjectionFingerprintMismatch => FacadeProtocolError::new(
            FACADE_PROJECTION_MISMATCH,
            "MCP projection fingerprint mismatch",
            "projection_fingerprint_mismatch",
        ),
        Error::Grant(_) | Error::MissingProjection => FacadeProtocolError::new(
            FACADE_STALE,
            "MCP session grant is unavailable",
            "session_grant_unavailable",
        ),
        Error::InvalidIdentity | Error::GenerationExhausted => FacadeProtocolError::new(
            INTERNAL_ERROR,
            "MCP facade coordinator failed",
            "coordinator_failed",
        ),
    }
}

fn parse_native_call(
    contract: CliMcpCallContract,
    meta: Option<&JsonValue>,
    wire_arguments: JsonValue,
    native_thread: &str,
    native_turn: &str,
) -> Result<(JsonValue, NativeMcpCallIdentity), FacadeProtocolError> {
    let invalid = || {
        FacadeProtocolError::new(
            INVALID_PARAMS,
            "managed MCP native identity is missing or invalid",
            "invalid_native_identity",
        )
    };
    let required = |object: &JsonMap<String, JsonValue>, name: &str| {
        object
            .get(name)
            .and_then(JsonValue::as_str)
            .filter(|s| {
                !s.is_empty()
                    && s.trim() == *s
                    && s.len() <= 512
                    && !s.chars().any(char::is_control)
            })
            .map(str::to_owned)
            .ok_or_else(invalid)
    };
    let (arguments, item) = match contract {
        CliMcpCallContract::Codex => {
            let identity = parse_codex_native_call(meta)?;
            if identity.native_thread_id != native_thread {
                return Err(invocation_error(TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::ScopeMismatch,
                    "Codex MCP request belongs to a different native thread",
                )));
            }
            return Ok((wire_arguments, identity));
        }
        CliMcpCallContract::Claude => {
            let meta = meta.and_then(JsonValue::as_object).ok_or_else(invalid)?;
            (wire_arguments, required(meta, "claudecode/toolUseId")?)
        }
        CliMcpCallContract::OpenCode => {
            // Created only by the owned executor after native input decoding.
            // Original arguments stay nested, including any names like these.
            let envelope = wire_arguments.as_object().ok_or_else(invalid)?;
            if envelope.len() != 2 {
                return Err(invalid());
            }
            let identity = envelope
                .get("__pioneer_mcp")
                .and_then(JsonValue::as_object)
                .ok_or_else(invalid)?;
            if identity.len() != 3 {
                return Err(invalid());
            }
            let session = required(identity, "sessionID")?;
            let meta = meta.and_then(JsonValue::as_object).ok_or_else(invalid)?;
            if session != native_thread || required(meta, "ai.opencode/sessionID")? != session {
                return Err(invocation_error(TurnMcpInvocationError::new(
                    TurnMcpInvocationErrorCode::ScopeMismatch,
                    "OpenCode MCP session does not match its native binding",
                )));
            }
            let message = required(identity, "messageID")?;
            let call = required(identity, "id")?;
            let arguments = envelope.get("arguments").cloned().ok_or_else(invalid)?;
            (arguments, format!("{message}:tool:{call}"))
        }
    };
    Ok((
        arguments,
        NativeMcpCallIdentity {
            native_thread_id: native_thread.into(),
            native_turn_id: native_turn.into(),
            native_item_id: item,
        },
    ))
}

// Codex rust-v0.164.0: mcp_tool_call.rs and turn_metadata.rs serialize
// this as an object, with callId identical to the native McpToolCall item id.
fn parse_codex_native_call(
    meta: Option<&JsonValue>,
) -> Result<NativeMcpCallIdentity, FacadeProtocolError> {
    let invalid = || {
        FacadeProtocolError::new(
            INVALID_PARAMS,
            "Codex MCP call requires consistent callId, threadId and turn metadata",
            "invalid_native_metadata",
        )
    };
    let meta = meta.and_then(JsonValue::as_object).ok_or_else(invalid)?;
    let required = |object: &JsonMap<String, JsonValue>, field: &str| {
        object
            .get(field)
            .and_then(JsonValue::as_str)
            .filter(|value| {
                !value.trim().is_empty()
                    && value.trim() == *value
                    && value.len() <= 512
                    && !value.chars().any(char::is_control)
            })
            .map(str::to_owned)
            .ok_or_else(invalid)
    };
    let native_item_id = required(meta, "callId")?;
    let native_thread_id = required(meta, "threadId")?;
    let turn_meta = meta
        .get("x-codex-turn-metadata")
        .and_then(JsonValue::as_object)
        .ok_or_else(invalid)?;
    if required(turn_meta, "thread_id")? != native_thread_id {
        return Err(invalid());
    }
    Ok(NativeMcpCallIdentity {
        native_thread_id,
        native_turn_id: required(turn_meta, "turn_id")?,
        native_item_id,
    })
}

fn invocation_error(error: TurnMcpInvocationError) -> FacadeProtocolError {
    let (code, message) = match error.code {
        TurnMcpInvocationErrorCode::InvalidRequest => (INVALID_PARAMS, "invalid MCP tool call"),
        TurnMcpInvocationErrorCode::TurnNotActive
        | TurnMcpInvocationErrorCode::SessionBindingUnavailable => {
            (FACADE_INACTIVE, "MCP turn is not active")
        }
        TurnMcpInvocationErrorCode::SessionGenerationStale
        | TurnMcpInvocationErrorCode::ScopeMismatch => (FACADE_STALE, "MCP session is stale"),
        TurnMcpInvocationErrorCode::ProjectionUnavailable
        | TurnMcpInvocationErrorCode::ToolUnbound
        | TurnMcpInvocationErrorCode::ToolDrift => {
            (FACADE_PROJECTION_MISMATCH, "MCP tool projection changed")
        }
        TurnMcpInvocationErrorCode::Cancelled => (FACADE_CALL_FAILED, "MCP tool call cancelled"),
        TurnMcpInvocationErrorCode::TimedOut => (FACADE_CALL_FAILED, "MCP tool call timed out"),
        _ => (FACADE_CALL_FAILED, "MCP tool call failed"),
    };
    FacadeProtocolError::new(code, message, error.reason_code())
}

fn canonical_call_result(result: CanonicalMcpToolResult) -> Result<JsonValue, FacadeProtocolError> {
    let content = canonical_content(result.content)?;
    let mut value = JsonMap::new();
    value.insert("content".to_owned(), JsonValue::Array(content));
    if let Some(structured_content) = result.structured_content {
        value.insert("structuredContent".to_owned(), structured_content);
    }
    value.insert("isError".to_owned(), JsonValue::Bool(result.is_error));
    let mut meta = match result.meta {
        None => JsonMap::new(),
        Some(JsonValue::Object(meta)) => meta,
        Some(_) => {
            return Err(FacadeProtocolError::new(
                INTERNAL_ERROR,
                "MCP tool result metadata is invalid",
                "result_invalid",
            ));
        }
    };
    let pioneer = meta
        .entry("pioneer".to_owned())
        .or_insert_with(|| json!({}));
    if let Some(pioneer) = pioneer.as_object_mut() {
        pioneer.insert("durationMs".to_owned(), result.duration_ms.into());
    }
    value.insert("_meta".to_owned(), JsonValue::Object(meta));
    Ok(JsonValue::Object(value))
}

fn canonical_content(content: JsonValue) -> Result<Vec<JsonValue>, FacadeProtocolError> {
    match content {
        JsonValue::String(text) => Ok(vec![json!({"type": "text", "text": text})]),
        JsonValue::Array(content) => content
            .into_iter()
            .map(canonical_content_block)
            .collect::<Result<Vec<_>, _>>(),
        _ => Err(FacadeProtocolError::new(
            INTERNAL_ERROR,
            "MCP tool result content is invalid",
            "result_invalid",
        )),
    }
}

fn canonical_content_block(content: JsonValue) -> Result<JsonValue, FacadeProtocolError> {
    let Some(object) = content.as_object() else {
        return Ok(unsupported_content(content));
    };
    let valid = match object.get("type").and_then(JsonValue::as_str) {
        Some("text") => object.get("text").and_then(JsonValue::as_str).is_some(),
        Some("image") | Some("audio") => {
            object.get("data").and_then(JsonValue::as_str).is_some()
                && object.get("mimeType").and_then(JsonValue::as_str).is_some()
        }
        Some("resource") => object.get("resource").is_some_and(JsonValue::is_object),
        Some("resource_link") => {
            object.get("uri").and_then(JsonValue::as_str).is_some()
                && object.get("name").and_then(JsonValue::as_str).is_some()
        }
        _ => false,
    };
    if valid {
        Ok(content)
    } else {
        Ok(unsupported_content(content))
    }
}

fn unsupported_content(content: JsonValue) -> JsonValue {
    let encoded = serde_json::to_string(&content).unwrap_or_else(|_| "null".to_owned());
    let bounded = bound_chars(encoded.as_str(), MAX_UNKNOWN_CONTENT_CHARS);
    json!({
        "type": "text",
        "text": format!("[Unsupported MCP content] {bounded}"),
    })
}

fn safe_description(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\r' | '\t') {
                '\u{fffd}'
            } else {
                character
            }
        })
        .collect::<String>();
    bound_chars(sanitized.as_str(), MAX_SAFE_DESCRIPTION_CHARS)
}

fn validate_annotations(annotations: &JsonValue) -> Result<(), CliMcpFacadeBuildError> {
    let annotations = annotations
        .as_object()
        .ok_or(CliMcpFacadeBuildError::InvalidToolSchema)?;
    for (key, value) in annotations {
        let valid = match key.as_str() {
            "title" => value
                .as_str()
                .is_some_and(|title| title.len() <= 512 && !title.chars().any(char::is_control)),
            "readOnlyHint" | "destructiveHint" | "idempotentHint" | "openWorldHint" => {
                value.is_boolean()
            }
            _ => false,
        };
        if !valid {
            return Err(CliMcpFacadeBuildError::InvalidToolSchema);
        }
    }
    Ok(())
}

fn bound_chars(value: &str, maximum: usize) -> String {
    let mut characters = value.chars();
    let bounded = characters.by_ref().take(maximum).collect::<String>();
    if characters.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut canonical = JsonMap::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_json(&object[key]));
            }
            JsonValue::Object(canonical)
        }
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        _ => value.clone(),
    }
}

fn invalid_params() -> FacadeProtocolError {
    FacadeProtocolError::new(
        INVALID_PARAMS,
        "invalid method parameters",
        "invalid_params",
    )
}

fn json_rpc_error(
    id: Option<JsonValue>,
    code: i64,
    message: &'static str,
    data: Option<JsonValue>,
) -> JsonValue {
    let mut error = JsonMap::new();
    error.insert("code".to_owned(), code.into());
    error.insert("message".to_owned(), message.into());
    if let Some(data) = data {
        error.insert("data".to_owned(), data);
    }
    json!({
        "jsonrpc": JSON_RPC_VERSION,
        "id": id,
        "error": error,
    })
}

fn encode_response(response: &JsonValue) -> Option<Vec<u8>> {
    let mut encoded = serde_json::to_vec(response).ok()?;
    encoded.push(b'\n');
    Some(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
    use crate::cli_runtime::mcp::grants::{
        CliMcpConnectionId, CliMcpGrantScope, CliMcpManifestHash,
    };
    use crate::cli_runtime::session_instance::CliSessionInstanceId;
    use crate::turn_mcp::invoker::TurnMcpInvocationError;
    use async_trait::async_trait;
    use pioneer_cli_mcp_bridge::{AttachRequest, BridgeGeneration};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct RecordingInvoker {
        calls: AtomicUsize,
        invocations: std::sync::Mutex<Vec<TurnMcpInvocation>>,
        result: CanonicalMcpToolResult,
    }

    struct CancellationInvoker {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl TurnMcpInvoker for CancellationInvoker {
        async fn invoke(
            &self,
            _invocation: TurnMcpInvocation,
            cancellation: CancellationToken,
        ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            cancellation.cancelled().await;
            Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::Cancelled,
                "cancelled",
            ))
        }
    }

    #[async_trait]
    impl TurnMcpInvoker for RecordingInvoker {
        async fn invoke(
            &self,
            invocation: TurnMcpInvocation,
            _cancellation: CancellationToken,
        ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.invocations
                .lock()
                .expect("recording invoker should not be poisoned")
                .push(invocation);
            Ok(self.result.clone())
        }
    }

    fn fixture_tool(name: &str) -> CliMcpFacadeTool {
        CliMcpFacadeTool::new(
            name,
            Some(format!("{name} description")),
            json!({"type": "object", "properties": {}}),
            json!({"readOnlyHint": true}),
        )
        .expect("tool")
    }

    fn initialize_message(id: i64) -> JsonValue {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "fixture", "version": "1"}
            }
        })
    }

    fn process_instance(runtime_id: &str) -> CliSessionInstanceId {
        CliSessionInstanceId::unmanaged_for_test(
            CLIAgentRuntimeSessionKey::new("workspace", runtime_id, "thread").expect("key"),
            1,
        )
        .expect("instance")
    }

    async fn fixture(
        tools: Vec<CliMcpFacadeTool>,
        result: CanonicalMcpToolResult,
    ) -> (
        Arc<CliMcpToolFacade>,
        Arc<RecordingInvoker>,
        CliMcpFacadeRequestContext,
        Arc<CliMcpCoordinator>,
    ) {
        let invoker = Arc::new(RecordingInvoker {
            calls: AtomicUsize::new(0),
            invocations: std::sync::Mutex::new(Vec::new()),
            result,
        });
        let (facade, context, coordinator) =
            fixture_with_invoker(tools, invoker.clone(), CliMcpFacadeLimits::default()).await;
        (facade, invoker, context, coordinator)
    }

    async fn fixture_with_invoker(
        tools: Vec<CliMcpFacadeTool>,
        invoker: Arc<dyn TurnMcpInvoker>,
        limits: CliMcpFacadeLimits,
    ) -> (
        Arc<CliMcpToolFacade>,
        CliMcpFacadeRequestContext,
        Arc<CliMcpCoordinator>,
    ) {
        // Shared facade cases use Claude metadata independently of the instance name.
        fixture_with_runtime(tools, invoker, limits, "codex", CliMcpCallContract::Claude).await
    }

    async fn fixture_with_runtime(
        tools: Vec<CliMcpFacadeTool>,
        invoker: Arc<dyn TurnMcpInvoker>,
        limits: CliMcpFacadeLimits,
        runtime_id: &str,
        contract: CliMcpCallContract,
    ) -> (
        Arc<CliMcpToolFacade>,
        CliMcpFacadeRequestContext,
        Arc<CliMcpCoordinator>,
    ) {
        let coordinator = Arc::new(CliMcpCoordinator::default());
        let projection =
            CliMcpFacadeProjection::new(tools, CliMcpFacadeProjectionLimits::default())
                .expect("projection");
        let scope = CliMcpGrantScope::new(
            process_instance(runtime_id),
            CliMcpManifestHash::new("a".repeat(64)).expect("manifest"),
        );
        let issued = coordinator
            .issue_grant(scope.clone(), now_ms().saturating_add(60_000))
            .await
            .expect("grant");
        let projection_reservation = coordinator
            .stage_projection(&issued.grant_ref(), projection.fingerprint().clone())
            .await
            .expect("stage");
        let request = AttachRequest {
            session_id: issued.bridge_session_id.clone(),
            generation: BridgeGeneration::new(1).expect("generation"),
            nonce: issued.nonce.clone(),
        };
        let bound_grant = coordinator
            .attach(&request, &scope, CliMcpConnectionId::for_test(1))
            .await
            .expect("attach");
        let facade = CliMcpToolFacade::try_new(
            coordinator.clone(),
            invoker,
            projection,
            Arc::new(CliMcpNoopProgressSink),
            limits,
            contract,
        )
        .expect("facade");
        (
            facade,
            CliMcpFacadeRequestContext {
                bound_grant,
                projection_generation: projection_reservation.generation,
                activation_generation: None,
            },
            coordinator,
        )
    }

    async fn initialize(facade: &CliMcpToolFacade, context: &CliMcpFacadeRequestContext) {
        let response = facade
            .handle_message(context, initialize_message(1))
            .await
            .expect("response");
        assert_eq!(response["result"]["capabilities"], json!({"tools": {}}));
        facade
            .handle_message(
                context,
                json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            )
            .await;
    }

    async fn activate(
        facade: &CliMcpToolFacade,
        context: &mut CliMcpFacadeRequestContext,
        coordinator: &CliMcpCoordinator,
        turn_id: &str,
    ) {
        facade
            .handle_message(
                context,
                json!({"jsonrpc": "2.0", "id": 90, "method": "tools/list"}),
            )
            .await
            .expect("list");
        let turn = coordinator
            .reserve_turn(
                &context.bound_grant.grant_ref(),
                context.projection_generation,
                "thread",
                turn_id,
            )
            .await
            .expect("turn");
        coordinator
            .activate_turn(
                &context.bound_grant,
                turn.activation_generation,
                format!("native-thread-{turn_id}"),
                format!("native-turn-{turn_id}"),
            )
            .await
            .expect("activate");
        context.activation_generation = Some(turn.activation_generation);
    }

    fn call_message(id: JsonValue, arguments: JsonValue) -> JsonValue {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": "read", "arguments": arguments,
                "_meta": {"claudecode/toolUseId": format!("toolu_{id}")}
            }
        })
    }

    fn basic_result(content: JsonValue) -> CanonicalMcpToolResult {
        CanonicalMcpToolResult {
            content,
            structured_content: None,
            is_error: false,
            duration_ms: 7,
            meta: None,
        }
    }

    #[tokio::test]
    async fn cli_mcp_facade_initialize_is_tool_only_and_unknown_methods_fail() {
        let (facade, _, context, _) = fixture(Vec::new(), basic_result(json!([]))).await;
        initialize(&facade, &context).await;
        for method in [
            "resources/list",
            "prompts/list",
            "sampling/createMessage",
            "elicitation/create",
            "roots/list",
        ] {
            let response = facade
                .handle_message(
                    &context,
                    json!({"jsonrpc": "2.0", "id": 2, "method": method}),
                )
                .await
                .expect("response");
            assert_eq!(response["error"]["code"], METHOD_NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn cli_mcp_tools_list_empty_projection_is_explicit() {
        let (facade, _, context, _) = fixture(Vec::new(), basic_result(json!([]))).await;
        initialize(&facade, &context).await;
        let response = facade
            .handle_message(
                &context,
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            )
            .await
            .expect("list");
        assert_eq!(response["result"]["tools"], json!([]));
    }

    #[tokio::test]
    async fn cli_mcp_tools_list_is_exact_stable_and_single_page() {
        let (facade, _, context, _) = fixture(
            vec![fixture_tool("first"), fixture_tool("second")],
            basic_result(json!([])),
        )
        .await;
        initialize(&facade, &context).await;
        let request = json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"});
        let first = facade
            .handle_message(&context, request.clone())
            .await
            .expect("list");
        let second = facade
            .handle_message(&context, request)
            .await
            .expect("list");
        assert_eq!(first, second);
        assert_eq!(first["result"]["tools"][0]["name"], "first");
        assert_eq!(first["result"]["tools"][1]["name"], "second");
        assert!(first["result"].get("nextCursor").is_none());
        let cursor = facade
            .handle_message(
                &context,
                json!({
                    "jsonrpc": "2.0", "id": 3, "method": "tools/list",
                    "params": {"cursor": "partial"}
                }),
            )
            .await
            .expect("cursor error");
        assert_eq!(cursor["error"]["code"], INVALID_PARAMS);
    }

    #[tokio::test]
    async fn cli_mcp_tools_call_is_inactive_before_exact_activation() {
        let (facade, invoker, context, _) =
            fixture(vec![fixture_tool("read")], basic_result(json!([]))).await;
        initialize(&facade, &context).await;
        let response = facade
            .handle_message(
                &context,
                json!({
                    "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": "read", "arguments": {},
                        "_meta": {"claudecode/toolUseId": "toolu_inactive"}
                    }
                }),
            )
            .await
            .expect("inactive response");
        assert_eq!(response["error"]["code"], FACADE_INACTIVE);
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cli_mcp_tools_call_active_exact_binding_reaches_only_shared_invoker() {
        let (facade, invoker, mut context, coordinator) = fixture(
            vec![fixture_tool("read")],
            CanonicalMcpToolResult {
                content: json!([{"type": "text", "text": "ok"}]),
                structured_content: Some(json!({"value": 1})),
                is_error: false,
                duration_ms: 4,
                meta: None,
            },
        )
        .await;
        initialize(&facade, &context).await;
        let preparing_list = facade
            .handle_message(
                &context,
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            )
            .await
            .expect("list");
        let turn = coordinator
            .reserve_turn(
                &context.bound_grant.grant_ref(),
                context.projection_generation,
                "thread",
                "turn",
            )
            .await
            .expect("turn");
        coordinator
            .activate_turn(
                &context.bound_grant,
                turn.activation_generation,
                "native-thread",
                "native-turn",
            )
            .await
            .expect("activate");
        context.activation_generation = Some(turn.activation_generation);

        let response = facade
            .handle_message(
                &context,
                json!({
                    "jsonrpc": "2.0", "id": "call-1", "method": "tools/call",
                    "params": {"name": "read", "arguments": {"key": "value"},
                        "_meta": {"claudecode/toolUseId": "toolu_call-1"}
                    }
                }),
            )
            .await
            .expect("call");
        assert_eq!(response["result"]["content"][0]["text"], "ok");
        assert_eq!(response["result"]["structuredContent"], json!({"value": 1}));
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 1);

        let active_list = facade
            .handle_message(
                &context,
                json!({"jsonrpc": "2.0", "id": 3, "method": "tools/list"}),
            )
            .await
            .expect("active list");
        assert_eq!(active_list["result"], preparing_list["result"]);
    }

    #[tokio::test]
    async fn cli_mcp_tools_call_uses_executing_child_thread_not_continuation_session_thread() {
        let (facade, invoker, mut context, coordinator) = fixture(
            vec![fixture_tool("read")],
            basic_result(json!([{"type": "text", "text": "child-ok"}])),
        )
        .await;
        initialize(&facade, &context).await;
        facade
            .handle_message(
                &context,
                json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            )
            .await
            .expect("list");
        let turn = coordinator
            .reserve_turn(
                &context.bound_grant.grant_ref(),
                context.projection_generation,
                "detached-child-thread",
                "detached-child-turn",
            )
            .await
            .expect("detached child turn");
        coordinator
            .activate_turn(
                &context.bound_grant,
                turn.activation_generation,
                "native-continuation-thread",
                "native-child-turn",
            )
            .await
            .expect("activate detached child");
        context.activation_generation = Some(turn.activation_generation);

        let response = facade
            .handle_message(
                &context,
                json!({
                    "jsonrpc": "2.0", "id": "child-call", "method": "tools/call",
                    "params": {"name": "read", "arguments": {},
                        "_meta": {"claudecode/toolUseId": "toolu_child-call"}
                    }
                }),
            )
            .await
            .expect("child call");
        assert_eq!(response["result"]["content"][0]["text"], "child-ok");
        let invocations = invoker
            .invocations
            .lock()
            .expect("recording invoker should not be poisoned");
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].thread_id, "detached-child-thread");
        assert_eq!(invocations[0].turn_id, "detached-child-turn");
        assert_ne!(
            invocations[0].thread_id,
            context.bound_grant.scope().process_instance.key().thread_id
        );
    }

    #[tokio::test]
    async fn cli_mcp_ledger_coalesces_and_caches_same_id_same_input() {
        let (facade, invoker, mut context, coordinator) = fixture(
            vec![fixture_tool("read")],
            basic_result(json!([{"type": "text", "text": "once"}])),
        )
        .await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn-one").await;
        let request = call_message(json!("same-id"), json!({"b": 2, "a": 1}));
        let first = facade
            .handle_message(&context, request.clone())
            .await
            .expect("first");
        let second = facade
            .handle_message(&context, request)
            .await
            .expect("cached");
        assert_eq!(first, second);
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 1);

        let conflict = facade
            .handle_message(&context, call_message(json!("same-id"), json!({"a": 9})))
            .await
            .expect("conflict");
        assert_eq!(
            conflict["error"]["data"]["kind"],
            "conflicting_duplicate_request_id"
        );
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cli_mcp_ledger_same_request_id_in_new_turn_is_new_invocation() {
        let (facade, invoker, mut context, coordinator) = fixture(
            vec![fixture_tool("read")],
            basic_result(json!([{"type": "text", "text": "ok"}])),
        )
        .await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn-one").await;
        let request = call_message(json!(7), json!({}));
        facade
            .handle_message(&context, request.clone())
            .await
            .expect("first turn");
        coordinator
            .terminal_turn(
                &context.bound_grant,
                context.activation_generation.expect("activation"),
            )
            .await
            .expect("terminal");
        context.activation_generation = None;
        activate(&facade, &mut context, &coordinator, "turn-two").await;
        facade
            .handle_message(&context, request)
            .await
            .expect("second turn");
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cli_mcp_limits_reject_frame_before_json_allocation() {
        let invoker = Arc::new(RecordingInvoker {
            calls: AtomicUsize::new(0),
            invocations: std::sync::Mutex::new(Vec::new()),
            result: basic_result(json!([])),
        });
        let mut limits = CliMcpFacadeLimits::default();
        limits.max_frame_bytes = 2_048;
        let (facade, context, _) = fixture_with_invoker(Vec::new(), invoker.clone(), limits).await;
        let response = facade
            .handle_bytes(&context, &vec![b'x'; 2_049])
            .await
            .expect("response");
        let response: JsonValue = serde_json::from_slice(&response).expect("json");
        assert_eq!(response["error"]["data"]["kind"], "frame_too_large");
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[tokio::test]
    async fn oversized_final_response_keeps_correlation_without_reexecution() {
        // A transport receipt, not a simulated OS/grant success: the real
        // facade must keep this pending until the escaped envelope fits.
        struct Receipt {
            committed: Arc<std::sync::atomic::AtomicBool>,
            revoked: Arc<AtomicUsize>,
        }
        impl pioneer_tools::ToolDeliveryReceipt for Receipt {
            fn commit(&self) {
                self.committed.store(true, AtomicOrdering::SeqCst);
            }
        }
        impl Drop for Receipt {
            fn drop(&mut self) {
                if !self.committed.load(AtomicOrdering::SeqCst) {
                    self.revoked.fetch_add(1, AtomicOrdering::SeqCst);
                }
            }
        }
        struct DeliveryInvoker {
            calls: AtomicUsize,
            committed: Arc<std::sync::atomic::AtomicBool>,
            revoked: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl TurnMcpInvoker for DeliveryInvoker {
            async fn invoke(
                &self,
                invocation: TurnMcpInvocation,
                _: CancellationToken,
            ) -> Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
                self.calls.fetch_add(1, AtomicOrdering::SeqCst);
                if invocation
                    .arguments
                    .get("action")
                    .and_then(JsonValue::as_bool)
                    == Some(true)
                {
                    if self.committed.load(AtomicOrdering::SeqCst) {
                        return Ok(basic_result(
                            json!([{"type":"text", "text":"transport-authorized action"}]),
                        ));
                    }
                    return Err(TurnMcpInvocationError::new(
                        crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::ResultInvalid,
                        "fresh observation required",
                    ));
                }
                let mut guard =
                    crate::mcp_service::CuaCliDeliveryGuard::new(None, invocation.turn_id);
                guard.hold_receipt(Some(Arc::new(Receipt {
                    committed: self.committed.clone(),
                    revoked: self.revoked.clone(),
                })));
                assert!(defer_cua_delivery(guard).is_ok());
                // Canonical payload fits the facade ceiling, but the actual
                // escaped 256-byte ID makes the final response exceed it.
                Ok(basic_result(
                    json!([{"type":"text", "text":"x".repeat(2500)}]),
                ))
            }
        }
        let invoker = Arc::new(DeliveryInvoker {
            calls: AtomicUsize::new(0),
            committed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            revoked: Arc::new(AtomicUsize::new(0)),
        });
        let mut limits = CliMcpFacadeLimits::default();
        limits.max_frame_bytes = 4096;
        let (facade, mut context, coordinator) =
            fixture_with_invoker(vec![fixture_tool("read")], invoker.clone(), limits).await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "large-result").await;
        let id = "\u{1}".repeat(256);
        let mut message = call_message(json!(id), json!({}));
        message["params"]["_meta"]["claudecode/toolUseId"] = json!("toolu_large_observation");
        let request = serde_json::to_vec(&message).unwrap();
        for _ in 0..2 {
            let encoded = facade.handle_bytes(&context, &request).await.unwrap();
            assert!(encoded.len() <= 4096);
            let response: JsonValue = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(response["id"], id);
            assert_eq!(
                response["error"]["data"]["kind"],
                "response_frame_too_large"
            );
            assert_eq!(response["error"]["data"]["noReplay"], true);
        }
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(invoker.revoked.load(AtomicOrdering::SeqCst), 1);
        assert!(!invoker.committed.load(AtomicOrdering::SeqCst));
        let action =
            serde_json::to_vec(&call_message(json!("new-action"), json!({"action":true}))).unwrap();
        let encoded = facade.handle_bytes(&context, &action).await.unwrap();
        let response: JsonValue = serde_json::from_slice(&encoded).unwrap();
        assert!(response.get("error").is_some());
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 2);
        let fresh =
            serde_json::to_vec(&call_message(json!("fresh-observation"), json!({}))).unwrap();
        let encoded = facade.handle_bytes(&context, &fresh).await.unwrap();
        let response: JsonValue = serde_json::from_slice(&encoded).unwrap();
        assert!(response.get("result").is_some());
        assert!(invoker.committed.load(AtomicOrdering::SeqCst));
        let action =
            serde_json::to_vec(&call_message(json!("after-fresh"), json!({"action":true})))
                .unwrap();
        let encoded = facade.handle_bytes(&context, &action).await.unwrap();
        let response: JsonValue = serde_json::from_slice(&encoded).unwrap();
        assert!(response.get("result").is_some());
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 4);
    }

    #[tokio::test]
    async fn cli_mcp_large_result_is_not_rejected_after_completion() {
        let text = "x".repeat(2 * 1024 * 1024);
        let (facade, invoker, mut context, coordinator) = fixture(
            vec![fixture_tool("read")],
            basic_result(json!([{
                "type": "text",
                "text": text,
            }])),
        )
        .await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn").await;

        let response = facade
            .handle_message(&context, call_message(json!(1), json!({})))
            .await
            .expect("large valid result should remain successful");

        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 1);
        assert!(response.get("error").is_none());
        assert_eq!(
            response["result"]["content"][0]["text"]
                .as_str()
                .map(str::len),
            Some(2 * 1024 * 1024)
        );
    }

    #[tokio::test]
    async fn cli_mcp_cancel_propagates_and_shutdown_drains_ledger() {
        let invoker = Arc::new(CancellationInvoker {
            calls: AtomicUsize::new(0),
        });
        let (facade, mut context, coordinator) = fixture_with_invoker(
            vec![fixture_tool("read")],
            invoker.clone(),
            CliMcpFacadeLimits::default(),
        )
        .await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn").await;
        let call_facade = facade.clone();
        let call_context = context.clone();
        let call = tokio::spawn(async move {
            call_facade
                .handle_message(&call_context, call_message(json!("cancel-me"), json!({})))
                .await
                .expect("call response")
        });
        wait_for_calls(&invoker.calls, 1).await;
        let duplicate_facade = facade.clone();
        let duplicate_context = context.clone();
        let duplicate = tokio::spawn(async move {
            duplicate_facade
                .handle_message(
                    &duplicate_context,
                    call_message(json!("cancel-me"), json!({})),
                )
                .await
                .expect("duplicate response")
        });
        tokio::task::yield_now().await;
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 1);
        facade
            .handle_message(
                &context,
                json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": "cancel-me", "reason": "test"}
                }),
            )
            .await;
        let response = call.await.expect("join");
        assert_eq!(response["error"]["data"]["kind"], "cancelled");
        assert_eq!(
            duplicate.await.expect("join")["error"]["data"]["kind"],
            "cancelled"
        );
        assert_eq!(invoker.calls.load(AtomicOrdering::SeqCst), 1);
        assert!(facade.shutdown().await.drained);
    }

    #[tokio::test]
    async fn cli_mcp_shutdown_cancels_inflight_and_rejects_late_calls() {
        let invoker = Arc::new(CancellationInvoker {
            calls: AtomicUsize::new(0),
        });
        let (facade, mut context, coordinator) = fixture_with_invoker(
            vec![fixture_tool("read")],
            invoker.clone(),
            CliMcpFacadeLimits::default(),
        )
        .await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn").await;
        let call_facade = facade.clone();
        let call_context = context.clone();
        let call = tokio::spawn(async move {
            call_facade
                .handle_message(&call_context, call_message(json!(1), json!({})))
                .await
                .expect("call")
        });
        wait_for_calls(&invoker.calls, 1).await;
        assert!(facade.shutdown().await.drained);
        assert_eq!(
            call.await.expect("join")["error"]["data"]["kind"],
            "cancelled"
        );
        let late = facade
            .handle_message(&context, call_message(json!(2), json!({})))
            .await
            .expect("late");
        assert_eq!(late["error"]["data"]["kind"], "shutting_down");
    }

    #[tokio::test]
    async fn cli_mcp_cancel_connection_revoke_reaches_inflight_invoker() {
        let invoker = Arc::new(CancellationInvoker {
            calls: AtomicUsize::new(0),
        });
        let (facade, mut context, coordinator) = fixture_with_invoker(
            vec![fixture_tool("read")],
            invoker.clone(),
            CliMcpFacadeLimits::default(),
        )
        .await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn").await;
        let call_facade = facade.clone();
        let call_context = context.clone();
        let call = tokio::spawn(async move {
            call_facade
                .handle_message(&call_context, call_message(json!(1), json!({})))
                .await
                .expect("call")
        });
        wait_for_calls(&invoker.calls, 1).await;
        coordinator
            .revoke_connection(&context.bound_grant)
            .await
            .expect("revoke");
        assert_eq!(
            call.await.expect("join")["error"]["data"]["kind"],
            "cancelled"
        );
        assert!(facade.shutdown().await.drained);
    }

    #[tokio::test]
    async fn cli_mcp_limits_bound_queue_and_release_every_reservation() {
        let invoker = Arc::new(CancellationInvoker {
            calls: AtomicUsize::new(0),
        });
        let mut limits = CliMcpFacadeLimits::default();
        limits.max_active_calls = 1;
        limits.max_queued_calls = 1;
        limits.max_ledger_entries = 4;
        let (facade, mut context, coordinator) =
            fixture_with_invoker(vec![fixture_tool("read")], invoker.clone(), limits).await;
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn").await;

        let first_facade = facade.clone();
        let first_context = context.clone();
        let first = tokio::spawn(async move {
            first_facade
                .handle_message(&first_context, call_message(json!(1), json!({})))
                .await
                .expect("first")
        });
        wait_for_calls(&invoker.calls, 1).await;
        let second_facade = facade.clone();
        let second_context = context.clone();
        let second = tokio::spawn(async move {
            second_facade
                .handle_message(&second_context, call_message(json!(2), json!({})))
                .await
                .expect("second")
        });
        wait_for_atomic(&facade.queued_calls, 1).await;
        let saturated = facade
            .handle_message(&context, call_message(json!(3), json!({})))
            .await
            .expect("saturation");
        assert_eq!(
            saturated["error"]["data"]["kind"],
            "queue_capacity_exhausted"
        );

        facade
            .handle_message(
                &context,
                json!({
                    "jsonrpc": "2.0", "method": "notifications/cancelled",
                    "params": {"requestId": 1}
                }),
            )
            .await;
        assert_eq!(
            first.await.expect("join")["error"]["data"]["kind"],
            "cancelled"
        );
        wait_for_calls(&invoker.calls, 2).await;
        facade
            .handle_message(
                &context,
                json!({
                    "jsonrpc": "2.0", "method": "notifications/cancelled",
                    "params": {"requestId": 2}
                }),
            )
            .await;
        assert_eq!(
            second.await.expect("join")["error"]["data"]["kind"],
            "cancelled"
        );
        assert_eq!(facade.queued_calls.load(Ordering::Acquire), 0);
        assert!(facade.shutdown().await.drained);
    }

    #[test]
    fn cli_mcp_facade_projection_rejects_partial_one_page_surface() {
        let error = CliMcpFacadeProjection::new(
            vec![fixture_tool("one"), fixture_tool("two")],
            CliMcpFacadeProjectionLimits {
                max_tools: 1,
                max_list_result_bytes: 1024,
            },
        )
        .expect_err("oversized projection");
        assert!(matches!(error, CliMcpFacadeBuildError::TooManyTools { .. }));
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        timeout(std::time::Duration::from_secs(1), async {
            while calls.load(AtomicOrdering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("invoker entry");
    }

    async fn wait_for_atomic(value: &AtomicUsize, expected: usize) {
        timeout(std::time::Duration::from_secs(1), async {
            while value.load(Ordering::Acquire) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("atomic state");
    }

    // One facade-level owner for the shared admission/replay properties. Each
    // case uses its production correlator and ledger; only final execution is fake.
    enum ExactItems {
        Claude(Arc<crate::cli_runtime::claude_session::ClaudeNativeMcpCorrelationLedger>),
        OpenCode(Arc<super::super::native_correlation::NativeMcpCorrelationLedger>),
    }

    impl ExactItems {
        fn item_id(&self, call: &str) -> String {
            match self {
                Self::Claude(_) => format!("toolu_{call}"),
                Self::OpenCode(_) => format!("msg_assistant:tool:{call}"),
            }
        }

        fn register_in(
            &self,
            call: &str,
            arguments: JsonValue,
            callable: &str,
            thread: &str,
            turn: &str,
        ) {
            let item = self.item_id(call);
            let fingerprint =
                crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&arguments).unwrap();
            match self {
                Self::Claude(ledger) => ledger
                    .register(
                        &crate::cli_runtime::claude_mcp::ClaudeNativeMcpItemBinding {
                            native_thread_id: thread.into(),
                            native_turn_id: turn.into(),
                            native_item_id: item,
                            canonical_callable_name: callable.into(),
                            arguments_fingerprint: fingerprint,
                            metadata: pioneer_protocol::ToolMetadata::empty(),
                        },
                    )
                    .unwrap(),
                Self::OpenCode(ledger) => ledger
                    .register(super::super::native_correlation::NativeMcpItemBinding {
                        native_thread_id: thread.into(),
                        native_turn_id: turn.into(),
                        native_item_id: item,
                        canonical_callable_name: callable.into(),
                        arguments_fingerprint: fingerprint,
                    })
                    .unwrap(),
            }
        }

        fn register(&self, call: &str, arguments: JsonValue) {
            self.register_in(
                call,
                arguments,
                "read",
                "native-thread-turn",
                "native-turn-turn",
            );
        }

        fn invoker(
            &self,
            inner: Arc<dyn TurnMcpInvoker>,
            coordinator: Arc<CliMcpCoordinator>,
            bound: CliMcpBoundGrant,
        ) -> Arc<dyn TurnMcpInvoker> {
            match self {
                Self::Claude(ledger) => Arc::new(
                    crate::cli_runtime::claude_session::ClaudeCorrelatingTurnMcpInvoker {
                        inner,
                        native_items: ledger.clone(),
                        coordinator,
                        bound_grant: bound,
                    },
                ),
                Self::OpenCode(ledger) => Arc::new(
                    super::super::native_correlation::CorrelatingTurnMcpInvoker {
                        inner,
                        native_items: ledger.clone(),
                        opencode_binding: Some((coordinator, bound)),
                        codex_binding: None,
                    },
                ),
            }
        }
    }

    fn exact_call(
        contract: CliMcpCallContract,
        rpc: &str,
        call: &str,
        arguments: JsonValue,
    ) -> JsonValue {
        let (arguments, meta) = match contract {
            CliMcpCallContract::Claude => (
                arguments,
                json!({"claudecode/toolUseId": format!("toolu_{call}")}),
            ),
            CliMcpCallContract::OpenCode => (
                json!({
                    "__pioneer_mcp": {"sessionID": "native-thread-turn", "messageID": "msg_assistant", "id": call},
                    "arguments": arguments,
                }),
                json!({"ai.opencode/sessionID": "native-thread-turn"}),
            ),
            CliMcpCallContract::Codex => unreachable!(),
        };
        json!({"jsonrpc":"2.0", "id":rpc, "method":"tools/call", "params":{"name":"read", "arguments":arguments, "_meta":meta}})
    }

    async fn exact_fixture(
        contract: CliMcpCallContract,
        runtime_id: &str,
        limits: CliMcpFacadeLimits,
    ) -> (
        Arc<CliMcpToolFacade>,
        Arc<RecordingInvoker>,
        CliMcpFacadeRequestContext,
        Arc<CliMcpCoordinator>,
        ExactItems,
    ) {
        let recording = Arc::new(RecordingInvoker {
            calls: AtomicUsize::new(0),
            invocations: StdMutex::new(Vec::new()),
            result: basic_result(json!([])),
        });
        let (mut facade, mut context, coordinator) = fixture_with_runtime(
            vec![fixture_tool("read"), fixture_tool("other")],
            recording.clone(),
            limits,
            runtime_id,
            contract,
        )
        .await;
        let items = match contract {
            CliMcpCallContract::Claude => ExactItems::Claude(Arc::default()),
            CliMcpCallContract::OpenCode => ExactItems::OpenCode(Arc::default()),
            CliMcpCallContract::Codex => unreachable!(),
        };
        let facade_mut = Arc::get_mut(&mut facade).unwrap();
        facade_mut.invoker = items.invoker(
            recording.clone(),
            coordinator.clone(),
            context.bound_grant.clone(),
        );
        initialize(&facade, &context).await;
        activate(&facade, &mut context, &coordinator, "turn").await;
        (facade, recording, context, coordinator, items)
    }

    fn recorded_ids(invoker: &RecordingInvoker) -> Vec<String> {
        invoker
            .invocations
            .lock()
            .unwrap()
            .iter()
            .map(|call| call.provider_call_id.clone())
            .collect()
    }

    async fn cancel_exact(
        facade: &CliMcpToolFacade,
        context: &CliMcpFacadeRequestContext,
        rpc: &str,
    ) {
        facade.handle_message(context, json!({"jsonrpc":"2.0", "method":"notifications/cancelled", "params":{"requestId":rpc}})).await;
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_wait_for_own_identity_despite_delay_foreign_notifications_and_reordered_calls()
     {
        use futures_util::poll;
        use tokio::time::advance;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            // Names deliberately disagree with provider kinds, including Codex's
            // usual name; neither metadata nor the name selects the adapter.
            for runtime_id in ["work", "codex"] {
                let (facade, invoker, context, _, items) =
                    exact_fixture(contract, runtime_id, CliMcpFacadeLimits::default()).await;
                let mut b = Box::pin(
                    facade.handle_message(&context, exact_call(contract, "rpc-B", "B", json!({}))),
                );
                assert!(poll!(b.as_mut()).is_pending());
                advance(std::time::Duration::from_millis(300)).await;
                assert!(poll!(b.as_mut()).is_pending()); // OpenCode's old 250ms window.
                items.register("foreign", json!({"different":true}));
                assert!(poll!(b.as_mut()).is_pending()); // Claude's old single wakeup failed here.
                advance(std::time::Duration::from_secs(2)).await;
                assert!(poll!(b.as_mut()).is_pending()); // No independent two-second Claude window.
                let mut a = Box::pin(
                    facade.handle_message(&context, exact_call(contract, "rpc-A", "A", json!({}))),
                );
                assert!(poll!(a.as_mut()).is_pending());
                items.register("A", json!({}));
                assert!(poll!(b.as_mut()).is_pending());
                assert!(a.await.unwrap().get("result").is_some());
                items.register("B", json!({}));
                assert!(b.await.unwrap().get("result").is_some());

                assert_eq!(
                    recorded_ids(&invoker),
                    [items.item_id("A"), items.item_id("B")]
                );
                // An earlier native item whose RPC has not arrived remains
                // unused even after the later identical call completes.
                items.register("first", json!({}));
                items.register("later", json!({}));
                for call in ["later", "first"] {
                    assert!(
                        facade
                            .handle_message(&context, exact_call(contract, call, call, json!({})))
                            .await
                            .unwrap()
                            .get("result")
                            .is_some()
                    );
                }
                assert!(
                    invoker
                        .invocations
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|call| call.runtime_id.as_deref() == Some(runtime_id))
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_timeout_and_cancel_do_not_consume_or_steal_late_items() {
        use futures_util::poll;
        use tokio::time::advance;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_queue_wait = std::time::Duration::from_secs(1);
            let (facade, invoker, context, _, items) =
                exact_fixture(contract, "work", limits).await;
            let mut a = Box::pin(
                facade.handle_message(&context, exact_call(contract, "timeout-A", "A", json!({}))),
            );
            assert!(poll!(a.as_mut()).is_pending());
            advance(std::time::Duration::from_secs(1)).await;
            let timed_out = a.await.unwrap();
            assert_eq!(timed_out["error"]["data"]["kind"], "timed_out");
            assert_eq!(timed_out["error"]["code"], FACADE_CALL_FAILED);
            items.register("A", json!({}));
            let mut b = Box::pin(
                facade.handle_message(&context, exact_call(contract, "cancel-B", "B", json!({}))),
            );
            assert!(poll!(b.as_mut()).is_pending());
            cancel_exact(&facade, &context, "cancel-B").await;
            assert_eq!(b.await.unwrap()["error"]["data"]["kind"], "cancelled");
            items.register("B", json!({}));
            let mut c = Box::pin(
                facade.handle_message(&context, exact_call(contract, "new-C", "C", json!({}))),
            );
            assert!(poll!(c.as_mut()).is_pending());
            items.register("C", json!({}));
            assert!(c.await.unwrap().get("result").is_some());
            // Neither a timeout nor cancellation before claim consumes A/B.
            for call in ["A", "B"] {
                assert!(
                    facade
                        .handle_message(
                            &context,
                            exact_call(contract, &format!("fresh-{call}"), call, json!({}))
                        )
                        .await
                        .unwrap()
                        .get("result")
                        .is_some()
                );
            }
            assert_eq!(
                recorded_ids(&invoker),
                [items.item_id("C"), items.item_id("A"), items.item_id("B")]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_replay_and_cache_eviction_never_reexecute_used_native_items() {
        use futures_util::poll;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_active_calls = 1;
            limits.max_queued_calls = 1;
            limits.max_ledger_entries = 2;
            let (facade, invoker, context, _, items) =
                exact_fixture(contract, "work", limits).await;
            let request = exact_call(contract, "rpc-A", "A", json!({}));
            let mut leader = Box::pin(facade.handle_message(&context, request.clone()));
            let mut follower = Box::pin(facade.handle_message(&context, request.clone()));
            assert!(poll!(leader.as_mut()).is_pending());
            assert!(poll!(follower.as_mut()).is_pending());
            assert_eq!(
                facade
                    .handle_message(
                        &context,
                        exact_call(contract, "rpc-A", "foreign", json!({}))
                    )
                    .await
                    .unwrap()["error"]["data"]["kind"],
                "conflicting_duplicate_request_id"
            );
            items.register("A", json!({}));
            let result = leader.await.unwrap();
            assert_eq!(follower.await.unwrap(), result);
            assert_eq!(
                facade
                    .handle_message(&context, request.clone())
                    .await
                    .unwrap(),
                result
            );
            assert_eq!(
                facade
                    .handle_message(
                        &context,
                        exact_call(contract, "rpc-A", "foreign", json!({}))
                    )
                    .await
                    .unwrap()["error"]["data"]["kind"],
                "conflicting_duplicate_request_id"
            );
            for call in ["B", "C"] {
                items.register(call, json!({}));
                assert!(
                    facade
                        .handle_message(&context, exact_call(contract, call, call, json!({})))
                        .await
                        .unwrap()
                        .get("result")
                        .is_some()
                );
            }
            items.register("A", json!({})); // Re-registration preserves its used mark.
            assert_eq!(
                facade.handle_message(&context, request).await.unwrap()["error"]["data"]["kind"],
                "invalid_request"
            );
            {
                // Result TTL uses std time; expire completed entries directly.
                let mut state = lock_ledger_state(&facade.ledger.state);
                for entry in state.entries.values_mut() {
                    if let CliMcpLedgerEntryState::Completed { completed_at } = &mut entry.state {
                        *completed_at = Instant::now() - facade.limits.completed_entry_ttl;
                    }
                }
            }
            assert_eq!(
                facade
                    .handle_message(&context, exact_call(contract, "C", "C", json!({})))
                    .await
                    .unwrap()["error"]["data"]["kind"],
                "invalid_request"
            );
            items.register("D", json!({}));
            assert!(
                facade
                    .handle_message(&context, exact_call(contract, "D", "D", json!({})))
                    .await
                    .unwrap()
                    .get("result")
                    .is_some()
            );
            assert_eq!(
                recorded_ids(&invoker),
                [
                    items.item_id("A"),
                    items.item_id("B"),
                    items.item_id("C"),
                    items.item_id("D")
                ]
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_reject_invalid_identity_content_and_scope_before_execution() {
        use futures_util::poll;
        use tokio::time::advance;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_queue_wait = std::time::Duration::from_secs(1);
            let (facade, invoker, context, _, items) =
                exact_fixture(contract, "work", limits).await;
            let mut missing = exact_call(contract, "missing", "A", json!({}));
            missing["params"].as_object_mut().unwrap().remove("_meta");
            assert_eq!(
                facade.handle_message(&context, missing).await.unwrap()["error"]["data"]["kind"],
                "invalid_native_identity"
            );
            let mut malformed = exact_call(contract, "malformed", "A", json!({}));
            match contract {
                CliMcpCallContract::Claude => {
                    malformed["params"]["_meta"]["claudecode/toolUseId"] = json!(17)
                }
                CliMcpCallContract::OpenCode => {
                    malformed["params"]["arguments"]["__pioneer_mcp"]["id"] = json!(17)
                }
                _ => unreachable!(),
            }
            assert_eq!(
                facade.handle_message(&context, malformed).await.unwrap()["error"]["data"]["kind"],
                "invalid_native_identity"
            );
            for (call, callable, args) in [
                ("tool", "other", json!({})),
                ("args", "read", json!({"different":true})),
            ] {
                items.register(call, json!({}));
                let mut request = exact_call(contract, call, call, args);
                request["params"]["name"] = json!(callable);
                assert_eq!(
                    facade.handle_message(&context, request).await.unwrap()["error"]["data"]["kind"],
                    "invalid_request"
                );
            }
            items.register_in(
                "foreign",
                json!({}),
                "read",
                "foreign-thread",
                "foreign-turn",
            );
            let mut foreign = Box::pin(facade.handle_message(
                &context,
                exact_call(contract, "foreign", "foreign", json!({})),
            ));
            assert!(poll!(foreign.as_mut()).is_pending());
            advance(std::time::Duration::from_secs(1)).await;
            assert_eq!(foreign.await.unwrap()["error"]["data"]["kind"], "timed_out");
            if contract == CliMcpCallContract::OpenCode {
                let mut forged = exact_call(contract, "forged", "A", json!({}));
                forged["params"]["arguments"]["__pioneer_mcp"]["sessionID"] = json!("foreign");
                assert_eq!(
                    facade.handle_message(&context, forged).await.unwrap()["error"]["data"]["kind"],
                    "scope_mismatch"
                );
                let raw = call_message(json!("raw"), json!({"id":"A","sessionID":"model"}));
                assert_eq!(
                    facade.handle_message(&context, raw).await.unwrap()["error"]["data"]["kind"],
                    "invalid_native_identity"
                );
            }
            assert!(recorded_ids(&invoker).is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn opencode_envelope_is_removed_before_replay_and_execution_without_argument_collisions()
    {
        let contract = CliMcpCallContract::OpenCode;
        let (facade, invoker, context, _, items) =
            exact_fixture(contract, "work", CliMcpFacadeLimits::default()).await;
        let arguments =
            json!({"__pioneer_mcp":{"sessionID":"legal-model-field"},"arguments":{"id":"legal"}});
        items.register("A", arguments.clone());
        let request = exact_call(contract, "rpc-A", "A", arguments.clone());
        let result = facade
            .handle_message(&context, request.clone())
            .await
            .unwrap();
        assert!(result.get("result").is_some());
        assert_eq!(
            facade.handle_message(&context, request).await.unwrap(),
            result
        );
        assert_eq!(invoker.invocations.lock().unwrap()[0].arguments, arguments);
        assert_eq!(recorded_ids(&invoker), [items.item_id("A")]);
    }

    struct StalledProgress;
    #[async_trait]
    impl CliMcpProgressSink for StalledProgress {
        async fn send_progress(
            &self,
            _: CliMcpProgressNotification,
        ) -> Result<(), CliMcpProgressSendError> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_queue_progress_and_confirmation_share_one_deadline() {
        use futures_util::poll;
        use tokio::time::advance;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            for progress in [false, true] {
                let mut limits = CliMcpFacadeLimits::default();
                limits.max_active_calls = 1;
                limits.max_queued_calls = 1;
                limits.max_queue_wait = std::time::Duration::from_secs(1);
                let (mut facade, invoker, context, _, items) =
                    exact_fixture(contract, "work", limits).await;
                if progress {
                    Arc::get_mut(&mut facade).unwrap().progress_sink = Arc::new(StalledProgress);
                }
                let mut a = Box::pin(
                    facade.handle_message(&context, exact_call(contract, "A", "A", json!({}))),
                );
                let mut request_b = exact_call(contract, "B", "B", json!({}));
                if progress {
                    request_b["params"]["_meta"]["progressToken"] = json!("B");
                }
                let mut b = Box::pin(facade.handle_message(&context, request_b));
                assert!(poll!(a.as_mut()).is_pending());
                assert!(poll!(b.as_mut()).is_pending());
                assert_eq!(facade.queued_calls.load(Ordering::Acquire), 1);
                advance(std::time::Duration::from_millis(600)).await;
                items.register("A", json!({}));
                assert!(a.await.unwrap().get("result").is_some());
                assert!(poll!(b.as_mut()).is_pending());
                advance(std::time::Duration::from_millis(399)).await;
                assert!(poll!(b.as_mut()).is_pending());
                advance(std::time::Duration::from_millis(1)).await;
                assert_eq!(b.await.unwrap()["error"]["data"]["kind"], "timed_out");
                assert_eq!(facade.admission.available_permits(), 1);
                assert_eq!(facade.queued_calls.load(Ordering::Acquire), 0);
                assert_eq!(recorded_ids(&invoker), [items.item_id("A")]);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_admitted_execution_outlives_admission_deadline() {
        use futures_util::poll;
        use tokio::time::advance;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_queue_wait = std::time::Duration::from_secs(1);
            let (mut facade, _, context, coordinator, items) =
                exact_fixture(contract, "work", limits).await;
            let execution = Arc::new(CancellationInvoker {
                calls: AtomicUsize::new(0),
            });
            Arc::get_mut(&mut facade).unwrap().invoker =
                items.invoker(execution.clone(), coordinator, context.bound_grant.clone());
            items.register("long", json!({}));
            let mut call = Box::pin(
                facade.handle_message(&context, exact_call(contract, "long", "long", json!({}))),
            );
            assert!(poll!(call.as_mut()).is_pending());
            assert_eq!(execution.calls.load(AtomicOrdering::SeqCst), 1);
            advance(std::time::Duration::from_secs(2)).await;
            assert!(poll!(call.as_mut()).is_pending());
            cancel_exact(&facade, &context, "long").await;
            assert_eq!(call.await.unwrap()["error"]["data"]["kind"], "cancelled");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_revalidate_native_turn_after_wait_without_transferring_invocation() {
        use futures_util::poll;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            let (facade, invoker, context, coordinator, items) =
                exact_fixture(contract, "work", CliMcpFacadeLimits::default()).await;
            let mut old = Box::pin(
                facade.handle_message(&context, exact_call(contract, "old", "A", json!({}))),
            );
            assert!(poll!(old.as_mut()).is_pending());
            coordinator
                .retarget_turn(
                    &context.bound_grant,
                    context.activation_generation.unwrap(),
                    "native-thread-turn",
                    "new-native-turn",
                )
                .await
                .unwrap();
            items.register("A", json!({}));
            assert_eq!(
                old.await.unwrap()["error"]["data"]["kind"],
                "scope_mismatch"
            );
            let mut new = Box::pin(
                facade.handle_message(&context, exact_call(contract, "new", "B", json!({}))),
            );
            assert!(poll!(new.as_mut()).is_pending());
            items.register_in(
                "B",
                json!({}),
                "read",
                "native-thread-turn",
                "new-native-turn",
            );
            assert!(new.await.unwrap().get("result").is_some());
            assert_eq!(recorded_ids(&invoker), [items.item_id("B")]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exact_adapters_lifecycle_cancels_waiters_and_releases_capacity() {
        use futures_util::poll;
        for contract in [CliMcpCallContract::Claude, CliMcpCallContract::OpenCode] {
            for stop in [
                "cancel",
                "terminal",
                "revoke",
                "replacement",
                "shutdown",
                "drop",
            ] {
                let mut limits = CliMcpFacadeLimits::default();
                limits.max_active_calls = 1;
                limits.max_queued_calls = 1;
                let (facade, invoker, context, coordinator, _) =
                    exact_fixture(contract, "work", limits).await;
                let mut a = Box::pin(
                    facade.handle_message(&context, exact_call(contract, "A", "A", json!({}))),
                );
                let mut b = Box::pin(
                    facade.handle_message(&context, exact_call(contract, "B", "B", json!({}))),
                );
                assert!(poll!(a.as_mut()).is_pending());
                assert!(poll!(b.as_mut()).is_pending());
                if stop == "drop" {
                    drop(a);
                    drop(b);
                } else if stop == "shutdown" {
                    let mut shutdown = Box::pin(facade.shutdown());
                    assert!(poll!(shutdown.as_mut()).is_pending());
                    assert_eq!(a.await.unwrap()["error"]["data"]["kind"], "cancelled");
                    assert_eq!(b.await.unwrap()["error"]["data"]["kind"], "cancelled");
                    assert!(shutdown.await.drained);
                } else {
                    match stop {
                        "cancel" => {
                            cancel_exact(&facade, &context, "A").await;
                            cancel_exact(&facade, &context, "B").await;
                        }
                        "terminal" => coordinator
                            .terminal_turn(
                                &context.bound_grant,
                                context.activation_generation.unwrap(),
                            )
                            .await
                            .unwrap(),
                        "revoke" => {
                            coordinator
                                .revoke_connection(&context.bound_grant)
                                .await
                                .unwrap();
                        }
                        "replacement" => {
                            coordinator
                                .revoke_process(&context.bound_grant.scope().process_instance)
                                .await;
                        }
                        _ => unreachable!(),
                    }
                    assert_eq!(a.await.unwrap()["error"]["data"]["kind"], "cancelled");
                    assert_eq!(b.await.unwrap()["error"]["data"]["kind"], "cancelled");
                }
                assert_eq!(
                    facade.admission.available_permits(),
                    1,
                    "{contract:?} {stop}"
                );
                assert_eq!(facade.queued_calls.load(Ordering::Acquire), 0);
                assert!(recorded_ids(&invoker).is_empty());
                if stop == "terminal" {
                    assert_eq!(
                        facade
                            .handle_message(&context, exact_call(contract, "late", "A", json!({})))
                            .await
                            .unwrap()["error"]["data"]["kind"],
                        "turn_not_active"
                    );
                }
            }
        }
    }

    mod codex_regressions {
        use super::*;
        fn codex_meta(call: &str, thread: &str, turn: &str) -> JsonValue {
            json!({
                "callId": call, "threadId": thread,
                "x-codex-turn-metadata": {"thread_id": thread, "turn_id": turn, "codex_version": "0.164.0"}
            })
        }

        // These regressions cross the real facade ledger/admission and correlator.
        // Only the final shared invoker is fake; request metadata matches Codex 0.164.
        async fn codex_fixture(
            limits: CliMcpFacadeLimits,
        ) -> (
            Arc<CliMcpToolFacade>,
            Arc<RecordingInvoker>,
            CliMcpFacadeRequestContext,
            Arc<CliMcpCoordinator>,
            Arc<super::super::super::native_correlation::NativeMcpCorrelationLedger>,
        ) {
            correlating_fixture("codex", true, limits).await
        }

        async fn correlating_fixture(
            runtime_id: &str,
            codex_adapter: bool,
            limits: CliMcpFacadeLimits,
        ) -> (
            Arc<CliMcpToolFacade>,
            Arc<RecordingInvoker>,
            CliMcpFacadeRequestContext,
            Arc<CliMcpCoordinator>,
            Arc<super::super::super::native_correlation::NativeMcpCorrelationLedger>,
        ) {
            use super::super::super::native_correlation::{
                CodexMcpCorrelationBinding, CorrelatingTurnMcpInvoker, NativeMcpCorrelationLedger,
            };
            assert!(codex_adapter);
            let recording = Arc::new(RecordingInvoker {
                calls: AtomicUsize::new(0),
                invocations: StdMutex::new(Vec::new()),
                result: basic_result(json!([])),
            });
            let (mut facade, mut context, coordinator) = fixture_with_runtime(
                vec![fixture_tool("read"), fixture_tool("other")],
                recording.clone(),
                limits,
                runtime_id,
                CliMcpCallContract::Codex,
            )
            .await;
            let items = Arc::new(NativeMcpCorrelationLedger::default());
            let facade_mut = Arc::get_mut(&mut facade).unwrap();
            facade_mut.invoker = Arc::new(CorrelatingTurnMcpInvoker {
                inner: recording.clone(),
                native_items: items.clone(),
                opencode_binding: None,
                codex_binding: Some(CodexMcpCorrelationBinding {
                    coordinator: coordinator.clone(),
                    bound_grant: context.bound_grant.clone(),
                }),
            });
            initialize(&facade, &context).await;
            activate(&facade, &mut context, &coordinator, "turn").await;
            (facade, recording, context, coordinator, items)
        }

        fn codex_call(rpc: &str, item: &str, turn: &str) -> JsonValue {
            let mut request = call_message(json!(rpc), json!({}));
            request["params"]["_meta"] = codex_meta(item, "native-thread-turn", turn);
            request
        }

        fn codex_item(
            item: &str,
            turn: &str,
        ) -> super::super::super::native_correlation::NativeMcpItemBinding {
            super::super::super::native_correlation::NativeMcpItemBinding {
                native_thread_id: "native-thread-turn".into(),
                native_turn_id: turn.into(),
                native_item_id: item.into(),
                canonical_callable_name: "read".into(),
                arguments_fingerprint: crate::cli_runtime::codex_mcp::canonical_value_fingerprint(
                    &json!({}),
                )
                .unwrap(),
            }
        }

        fn executed_ids(recording: &RecordingInvoker) -> Vec<String> {
            recording
                .invocations
                .lock()
                .unwrap()
                .iter()
                .map(|call| call.provider_call_id.clone())
                .collect()
        }

        async fn cancel_rpc(
            facade: &CliMcpToolFacade,
            context: &CliMcpFacadeRequestContext,
            rpc: &str,
        ) {
            facade.handle_message(context, json!({
            "jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": rpc}
        })).await;
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_exact_items_survive_delay_reordering_and_identical_arguments() {
            use futures_util::poll;
            use tokio::time::advance;
            for runtime_id in ["codex", "codex_work", "work"] {
                let (facade, recording, context, _, items) =
                    correlating_fixture(runtime_id, true, CliMcpFacadeLimits::default()).await;
                let mut delayed = Box::pin(facade.handle_message(
                    &context,
                    codex_call("rpc-delayed", "delayed", "native-turn-turn"),
                ));
                assert!(poll!(delayed.as_mut()).is_pending());
                advance(std::time::Duration::from_millis(300)).await;
                // The old 250ms claim would already return session_binding_unavailable.
                assert!(poll!(delayed.as_mut()).is_pending());
                items
                    .register(codex_item("delayed", "native-turn-turn"))
                    .unwrap();
                assert!(delayed.await.unwrap().get("result").is_some());
                let mut absent = codex_call("missing-meta", "missing", "native-turn-turn");
                absent["params"].as_object_mut().unwrap().remove("_meta");
                assert_eq!(
                    facade.handle_message(&context, absent).await.unwrap()["error"]["data"]["kind"],
                    "invalid_native_metadata",
                    "{runtime_id}"
                );
                assert_eq!(executed_ids(&recording), ["delayed"]);
                // A is visible first but has not been requested. B/C must not steal it,
                // and their completion must not imply that A has already executed.
                items.register(codex_item("A", "native-turn-turn")).unwrap();
                let mut c = Box::pin(
                    facade.handle_message(&context, codex_call("rpc-C", "C", "native-turn-turn")),
                );
                let mut b = Box::pin(
                    facade.handle_message(&context, codex_call("rpc-B", "B", "native-turn-turn")),
                );
                assert!(poll!(c.as_mut()).is_pending());
                assert!(poll!(b.as_mut()).is_pending());
                items.register(codex_item("B", "native-turn-turn")).unwrap();
                assert!(poll!(c.as_mut()).is_pending());
                assert!(b.await.unwrap().get("result").is_some());
                items.register(codex_item("C", "native-turn-turn")).unwrap();
                assert!(c.await.unwrap().get("result").is_some());
                assert!(
                    facade
                        .handle_message(&context, codex_call("rpc-A", "A", "native-turn-turn"))
                        .await
                        .unwrap()
                        .get("result")
                        .is_some()
                );
                assert_eq!(executed_ids(&recording), ["delayed", "B", "C", "A"]);
                assert!(
                    recording
                        .invocations
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|call| call.runtime_id.as_deref() == Some(runtime_id))
                );
            }
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_timeout_and_cancel_do_not_steal_later_calls_or_consume_items() {
            use futures_util::poll;
            use tokio::time::advance;
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_queue_wait = std::time::Duration::from_secs(1);
            let (facade, recording, context, _, items) = codex_fixture(limits).await;
            let mut timed_out = Box::pin(
                facade.handle_message(&context, codex_call("timeout", "A", "native-turn-turn")),
            );
            assert!(poll!(timed_out.as_mut()).is_pending());
            advance(std::time::Duration::from_secs(1)).await;
            let timeout = timed_out.await.unwrap();
            assert_eq!(timeout["error"]["data"]["kind"], "timed_out");
            assert_eq!(timeout["error"]["code"], FACADE_CALL_FAILED);
            items.register(codex_item("A", "native-turn-turn")).unwrap();
            let mut cancelled = Box::pin(
                facade.handle_message(&context, codex_call("cancel", "C", "native-turn-turn")),
            );
            assert!(poll!(cancelled.as_mut()).is_pending());
            cancel_rpc(&facade, &context, "cancel").await;
            assert_eq!(
                cancelled.await.unwrap()["error"]["data"]["kind"],
                "cancelled"
            );
            items.register(codex_item("C", "native-turn-turn")).unwrap();
            let mut b = Box::pin(
                facade.handle_message(&context, codex_call("new-B", "B", "native-turn-turn")),
            );
            assert!(poll!(b.as_mut()).is_pending());
            items.register(codex_item("B", "native-turn-turn")).unwrap();
            assert!(b.await.unwrap().get("result").is_some());
            // A timeout/cancel before claim is not evidence of execution. A fresh
            // transport request may still use exactly that unconsumed native item.
            for item in ["A", "C"] {
                assert!(
                    facade
                        .handle_message(
                            &context,
                            codex_call(&format!("retry-{item}"), item, "native-turn-turn")
                        )
                        .await
                        .unwrap()
                        .get("result")
                        .is_some()
                );
            }
            assert_eq!(executed_ids(&recording), ["B", "A", "C"]);
            assert_eq!(
                facade.admission.available_permits(),
                facade.limits.max_active_calls
            );
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_replay_checks_identity_and_eviction_does_not_reexecute() {
            use futures_util::poll;
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_active_calls = 1;
            limits.max_queued_calls = 1;
            limits.max_ledger_entries = 2;
            let (facade, recording, context, _, items) = codex_fixture(limits).await;
            let request = codex_call("rpc-A", "A", "native-turn-turn");
            let mut leader = Box::pin(facade.handle_message(&context, request.clone()));
            let mut follower = Box::pin(facade.handle_message(&context, request.clone()));
            assert!(poll!(leader.as_mut()).is_pending());
            assert!(poll!(follower.as_mut()).is_pending());
            let conflicting = facade
                .handle_message(&context, codex_call("rpc-A", "B", "native-turn-turn"))
                .await
                .unwrap();
            assert_eq!(
                conflicting["error"]["data"]["kind"],
                "conflicting_duplicate_request_id"
            );
            items.register(codex_item("A", "native-turn-turn")).unwrap();
            let completed = leader.await.unwrap();
            assert_eq!(follower.await.unwrap(), completed);
            assert_eq!(
                facade
                    .handle_message(&context, request.clone())
                    .await
                    .unwrap(),
                completed
            );
            assert_eq!(
                facade
                    .handle_message(&context, codex_call("rpc-A", "A", "other-native-turn"))
                    .await
                    .unwrap()["error"]["data"]["kind"],
                "conflicting_duplicate_request_id"
            );
            for item in ["B", "C"] {
                items
                    .register(codex_item(item, "native-turn-turn"))
                    .unwrap();
                assert!(
                    facade
                        .handle_message(
                            &context,
                            codex_call(&format!("rpc-{item}"), item, "native-turn-turn")
                        )
                        .await
                        .unwrap()
                        .get("result")
                        .is_some()
                );
            }
            // Capacity evicted A's full result. Duplicate lifecycle registration
            // must retain its independent used mark.
            items.register(codex_item("A", "native-turn-turn")).unwrap();
            assert_eq!(
                facade.handle_message(&context, request).await.unwrap()["error"]["data"]["kind"],
                "invalid_request"
            );
            // The cache uses std::time::Instant; expire the existing completed
            // entry explicitly without sleeping or changing production clocks.
            {
                let mut ledger = lock_ledger_state(&facade.ledger.state);
                for entry in ledger.entries.values_mut() {
                    if let CliMcpLedgerEntryState::Completed { completed_at } = &mut entry.state {
                        *completed_at = Instant::now() - facade.limits.completed_entry_ttl;
                    }
                }
            }
            assert_eq!(
                facade
                    .handle_message(&context, codex_call("rpc-C", "C", "native-turn-turn"))
                    .await
                    .unwrap()["error"]["data"]["kind"],
                "invalid_request"
            );
            items.register(codex_item("D", "native-turn-turn")).unwrap();
            assert!(
                facade
                    .handle_message(&context, codex_call("rpc-D", "D", "native-turn-turn"))
                    .await
                    .unwrap()
                    .get("result")
                    .is_some()
            );
            assert_eq!(executed_ids(&recording), ["A", "B", "C", "D"]);
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_rejects_invalid_metadata_scope_and_item_content() {
            let (facade, recording, context, _, items) =
                codex_fixture(CliMcpFacadeLimits::default()).await;
            let mut absent = codex_call("absent", "A", "native-turn-turn");
            absent["params"].as_object_mut().unwrap().remove("_meta");
            assert_eq!(
                facade.handle_message(&context, absent).await.unwrap()["error"]["data"]["kind"],
                "invalid_native_metadata"
            );
            let valid = codex_meta("A", "native-thread-turn", "native-turn-turn");
            let mut missing_call = valid.clone();
            missing_call.as_object_mut().unwrap().remove("callId");
            let mut bad_call = valid.clone();
            bad_call["callId"] = json!(17);
            let mut empty_turn = valid.clone();
            empty_turn["x-codex-turn-metadata"]["turn_id"] = json!(" ");
            let mut bad_thread = valid.clone();
            bad_thread["x-codex-turn-metadata"]["thread_id"] = json!("different");
            let mut serialized = valid.clone();
            serialized["x-codex-turn-metadata"] = json!(valid["x-codex-turn-metadata"].to_string());
            let mut missing_thread = valid.clone();
            missing_thread.as_object_mut().unwrap().remove("threadId");
            for meta in [
                JsonValue::Null,
                missing_call,
                bad_call,
                empty_turn,
                bad_thread,
                serialized,
                missing_thread,
            ] {
                let mut request = codex_call("bad", "A", "native-turn-turn");
                request["params"]["_meta"] = meta;
                let response = facade.handle_message(&context, request).await.unwrap();
                assert_eq!(response["error"]["data"]["kind"], "invalid_native_metadata");
            }
            let mut foreign = codex_call("foreign", "A", "native-turn-turn");
            foreign["params"]["_meta"] = codex_meta("A", "foreign-thread", "native-turn-turn");
            assert_eq!(
                facade.handle_message(&context, foreign).await.unwrap()["error"]["data"]["kind"],
                "scope_mismatch"
            );
            for (rpc, callable, args) in [
                ("tool", "other", json!({})),
                ("args", "read", json!({"different": true})),
            ] {
                items.register(codex_item(rpc, "native-turn-turn")).unwrap();
                let mut request = codex_call(rpc, rpc, "native-turn-turn");
                request["params"]["name"] = json!(callable);
                request["params"]["arguments"] = args;
                assert_eq!(
                    facade.handle_message(&context, request).await.unwrap()["error"]["data"]["kind"],
                    "invalid_request"
                );
            }
            assert!(executed_ids(&recording).is_empty());
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_queue_and_confirmation_share_one_deadline_and_release_capacity() {
            use futures_util::poll;
            use tokio::time::advance;
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_active_calls = 1;
            limits.max_queued_calls = 1;
            limits.max_queue_wait = std::time::Duration::from_secs(1);
            let (facade, recording, context, _, items) = codex_fixture(limits).await;
            let mut a = Box::pin(
                facade.handle_message(&context, codex_call("rpc-A", "A", "native-turn-turn")),
            );
            let mut b = Box::pin(
                facade.handle_message(&context, codex_call("rpc-B", "B", "native-turn-turn")),
            );
            assert!(poll!(a.as_mut()).is_pending());
            assert!(poll!(b.as_mut()).is_pending());
            assert_eq!(facade.queued_calls.load(Ordering::Acquire), 1);
            advance(std::time::Duration::from_millis(600)).await;
            items.register(codex_item("A", "native-turn-turn")).unwrap();
            assert!(a.await.unwrap().get("result").is_some());
            assert!(poll!(b.as_mut()).is_pending());
            assert_eq!(facade.queued_calls.load(Ordering::Acquire), 0);
            advance(std::time::Duration::from_millis(399)).await;
            assert!(poll!(b.as_mut()).is_pending());
            advance(std::time::Duration::from_millis(1)).await;
            assert_eq!(b.await.unwrap()["error"]["data"]["kind"], "timed_out");
            assert_eq!(facade.admission.available_permits(), 1);
            assert_eq!(executed_ids(&recording), ["A"]);
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_admitted_execution_outlives_admission_deadline() {
            use super::super::super::native_correlation::{
                CodexMcpCorrelationBinding, CorrelatingTurnMcpInvoker,
            };
            use futures_util::poll;
            use tokio::time::advance;
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_queue_wait = std::time::Duration::from_secs(1);
            let (mut facade, _, context, coordinator, items) = codex_fixture(limits).await;
            let execution = Arc::new(CancellationInvoker {
                calls: AtomicUsize::new(0),
            });
            Arc::get_mut(&mut facade).unwrap().invoker = Arc::new(CorrelatingTurnMcpInvoker {
                inner: execution.clone(),
                native_items: items.clone(),
                opencode_binding: None,
                codex_binding: Some(CodexMcpCorrelationBinding {
                    coordinator,
                    bound_grant: context.bound_grant.clone(),
                }),
            });
            items
                .register(codex_item("long-call", "native-turn-turn"))
                .unwrap();
            let mut call = Box::pin(facade.handle_message(
                &context,
                codex_call("long-rpc", "long-call", "native-turn-turn"),
            ));
            assert!(poll!(call.as_mut()).is_pending());
            assert_eq!(execution.calls.load(AtomicOrdering::SeqCst), 1);
            advance(std::time::Duration::from_secs(2)).await;
            assert!(poll!(call.as_mut()).is_pending());
            cancel_rpc(&facade, &context, "long-rpc").await;
            assert_eq!(call.await.unwrap()["error"]["data"]["kind"], "cancelled");
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_waits_for_retarget_without_borrowing_old_turn_or_resetting_deadline()
        {
            use futures_util::poll;
            use tokio::time::advance;
            let mut limits = CliMcpFacadeLimits::default();
            limits.max_queue_wait = std::time::Duration::from_secs(1);
            let (facade, recording, context, coordinator, items) = codex_fixture(limits).await;
            items
                .register(codex_item("shared-id", "native-turn-turn"))
                .unwrap();
            let mut b = Box::pin(
                facade.handle_message(&context, codex_call("rpc-B", "shared-id", "native-B")),
            );
            let mut missing = Box::pin(
                facade.handle_message(&context, codex_call("rpc-missing", "missing", "native-B")),
            );
            assert!(poll!(b.as_mut()).is_pending());
            assert!(poll!(missing.as_mut()).is_pending());
            advance(std::time::Duration::from_millis(600)).await;
            // Even an exact B item must wait until the normal coordinator retarget.
            items.register(codex_item("shared-id", "native-B")).unwrap();
            assert!(poll!(b.as_mut()).is_pending());
            coordinator
                .retarget_turn(
                    &context.bound_grant,
                    context.activation_generation.unwrap(),
                    "native-thread-turn",
                    "native-B",
                )
                .await
                .unwrap();
            assert!(b.await.unwrap().get("result").is_some());
            assert!(poll!(missing.as_mut()).is_pending());
            coordinator
                .retarget_turn(
                    &context.bound_grant,
                    context.activation_generation.unwrap(),
                    "native-thread-turn",
                    "native-B",
                )
                .await
                .unwrap();
            assert_eq!(
                facade
                    .handle_message(&context, codex_call("duplicate-B", "shared-id", "native-B"))
                    .await
                    .unwrap()["error"]["data"]["kind"],
                "invalid_request"
            );
            advance(std::time::Duration::from_millis(400)).await;
            assert_eq!(missing.await.unwrap()["error"]["data"]["kind"], "timed_out");
            assert_eq!(executed_ids(&recording), ["shared-id"]);
        }

        #[tokio::test(start_paused = true)]
        async fn codex_facade_lifecycle_cancels_native_and_queue_waits() {
            use futures_util::poll;
            for stop in ["cancel", "terminal", "revoke", "replacement", "shutdown"] {
                let mut limits = CliMcpFacadeLimits::default();
                limits.max_active_calls = 1;
                limits.max_queued_calls = 1;
                let (facade, recording, context, coordinator, items) = codex_fixture(limits).await;
                let mut a = Box::pin(
                    facade.handle_message(&context, codex_call("rpc-A", "A", "native-turn-turn")),
                );
                let mut b = Box::pin(
                    facade.handle_message(&context, codex_call("rpc-B", "B", "native-turn-turn")),
                );
                assert!(poll!(a.as_mut()).is_pending());
                assert!(poll!(b.as_mut()).is_pending());
                match stop {
                    "cancel" => {
                        cancel_rpc(&facade, &context, "rpc-A").await;
                        cancel_rpc(&facade, &context, "rpc-B").await;
                    }
                    "terminal" => coordinator
                        .terminal_turn(&context.bound_grant, context.activation_generation.unwrap())
                        .await
                        .unwrap(),
                    "revoke" => {
                        coordinator
                            .revoke_connection(&context.bound_grant)
                            .await
                            .unwrap();
                    }
                    "replacement" => {
                        coordinator
                            .revoke_process(&context.bound_grant.scope().process_instance)
                            .await;
                    }
                    "shutdown" => {
                        let mut shutdown = Box::pin(facade.shutdown());
                        assert!(poll!(shutdown.as_mut()).is_pending());
                        assert_eq!(
                            a.as_mut().await.unwrap()["error"]["data"]["kind"],
                            "cancelled"
                        );
                        assert_eq!(
                            b.as_mut().await.unwrap()["error"]["data"]["kind"],
                            "cancelled"
                        );
                        assert!(shutdown.await.drained);
                    }
                    _ => unreachable!(),
                }
                if stop != "shutdown" {
                    assert_eq!(a.await.unwrap()["error"]["data"]["kind"], "cancelled");
                    assert_eq!(b.await.unwrap()["error"]["data"]["kind"], "cancelled");
                }
                assert_eq!(facade.admission.available_permits(), 1, "{stop}");
                assert_eq!(facade.queued_calls.load(Ordering::Acquire), 0, "{stop}");
                assert!(executed_ids(&recording).is_empty(), "{stop}");
                if stop == "cancel" {
                    items.register(codex_item("A", "native-turn-turn")).unwrap();
                    assert!(
                        facade
                            .handle_message(
                                &context,
                                codex_call("fresh-A", "A", "native-turn-turn")
                            )
                            .await
                            .unwrap()
                            .get("result")
                            .is_some()
                    );
                } else if stop == "terminal" {
                    assert_eq!(
                        facade
                            .handle_message(&context, codex_call("late", "A", "native-turn-turn"))
                            .await
                            .unwrap()["error"]["data"]["kind"],
                        "turn_not_active"
                    );
                }
            }
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis()
            .try_into()
            .expect("milliseconds")
    }
}
