use crate::apply_patch::CommitObserver;
use crate::apply_patch::history::{CommittedPatchChange, InvocationIdentity, StoredPatchRecord};
use crate::context::ToolOutcome;
use crate::error::ToolError;
use crate::output_policy::{
    ToolDisplayPayload, ToolOutputPolicySnapshot, ToolRecoveryView, ToolResultEnvelope,
    ToolResultView, ToolStoragePayload,
};
use pioneer_protocol::{
    ItemDeltaStream, ProtocolEventClass, StorageOutputPolicy, TimelineOutputPolicy, ToolMetadata,
    ToolOutputSummary, ToolRecoveryPolicySnapshot, TurnItemExecutionClass,
    TurnPermissionAuditEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use tokio::sync::{broadcast, oneshot};
use tracing::debug;

type NativePatchIdentityKey = (String, String, String);

static NATIVE_PATCH_OBSERVER_REGISTRY: OnceLock<
    Mutex<HashMap<NativePatchIdentityKey, Arc<dyn CommitObserver>>>,
> = OnceLock::new();

fn native_patch_observer_registry()
-> &'static Mutex<HashMap<NativePatchIdentityKey, Arc<dyn CommitObserver>>> {
    NATIVE_PATCH_OBSERVER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Publishes the process-local observer only as a handoff between the tool
/// executor and the gateway lifecycle consumer.  Durable recovery remains in
/// SQLite; this registry is bounded and is never treated as the source of
/// truth after restart.
pub fn native_patch_history_record(
    identity: &InvocationIdentity,
) -> Option<(StoredPatchRecord, Vec<CommittedPatchChange>)> {
    let key = (
        identity.thread_id.clone(),
        identity.turn_id.clone(),
        identity.invocation_id.clone(),
    );
    let observer = native_patch_observer_registry()
        .lock()
        .ok()?
        .get(&key)?
        .clone();
    observer.record(identity).ok().flatten()
}

pub fn register_native_patch_observer(
    identity: &InvocationIdentity,
    observer: Arc<dyn CommitObserver>,
) -> bool {
    let key = (
        identity.thread_id.clone(),
        identity.turn_id.clone(),
        identity.invocation_id.clone(),
    );
    if let Ok(mut registry) = native_patch_observer_registry().lock() {
        // A redelivered ItemStarted for the same invocation is an idempotent
        // lifecycle event. Keep the original observer (and its admission
        // permit) instead of replacing it with a second observer that could
        // race the first executor or lose its durable hand-off state.
        if registry.contains_key(&key) {
            return true;
        }
        // Never evict an active observer to make room for a new invocation:
        // eviction would orphan the bounded admission permit while its
        // executor is still allowed to run.  Durable intent allocation happens
        // later, under the executor's complete target lock set.
        if registry.len() >= 4096 {
            return false;
        }
        registry.insert(key, observer);
        true
    } else {
        false
    }
}

pub fn unregister_native_patch_observer(identity: &InvocationIdentity) {
    let key = (
        identity.thread_id.clone(),
        identity.turn_id.clone(),
        identity.invocation_id.clone(),
    );
    if let Ok(mut registry) = native_patch_observer_registry().lock() {
        registry.remove(&key);
    }
}

/// Releases all process-local native patch observers for a terminal turn.
/// SQLite intent/record rows remain the source of truth; this only drops the
/// bounded in-process hand-off objects and their history-capacity permits.
pub fn unregister_native_patch_observers_for_turn(thread_id: &str, turn_id: &str) {
    if let Ok(mut registry) = native_patch_observer_registry().lock() {
        registry.retain(|(thread, turn, _), _| thread != thread_id || turn != turn_id);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEventKind {
    CallStarted,
    Heartbeat,
    PermissionAudit,
    OutputDelta,
    CallCompleted,
    CallFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolEventPayload {
    CallStarted(ToolCallStartedEvent),
    Heartbeat,
    PermissionAudit(TurnPermissionAuditEvent),
    OutputDelta(ToolOutputDeltaEvent),
    CallCompleted(ToolCallCompletedEvent),
    CallFailed(ToolCallFailedEvent),
}

impl ToolEventPayload {
    pub fn kind(&self) -> ToolEventKind {
        match self {
            Self::CallStarted(_) => ToolEventKind::CallStarted,
            Self::Heartbeat => ToolEventKind::Heartbeat,
            Self::PermissionAudit(_) => ToolEventKind::PermissionAudit,
            Self::OutputDelta(_) => ToolEventKind::OutputDelta,
            Self::CallCompleted(_) => ToolEventKind::CallCompleted,
            Self::CallFailed(_) => ToolEventKind::CallFailed,
        }
    }

    pub fn event_class(&self) -> ProtocolEventClass {
        match self {
            Self::Heartbeat | Self::OutputDelta(_) => ProtocolEventClass::Progress,
            Self::CallStarted(_)
            | Self::PermissionAudit(_)
            | Self::CallCompleted(_)
            | Self::CallFailed(_) => ProtocolEventClass::Durable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallStartedEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    #[serde(default)]
    pub turn_item_execution_class: TurnItemExecutionClass,
    pub output_policy: ToolOutputPolicySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutputDeltaEvent {
    pub delta: ToolDeltaPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolDeltaPayload {
    OutputChunk {
        stream: ItemDeltaStream,
        text: String,
        truncated: bool,
    },
    Progress {
        stage: String,
        #[serde(default)]
        metadata: ToolMetadata,
    },
    ArtifactRef {
        label: String,
        uri: String,
        #[serde(default)]
        metadata: ToolMetadata,
    },
    Diagnostic {
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_class: Option<String>,
        #[serde(default)]
        metadata: ToolMetadata,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallCompletedEvent {
    pub success: bool,
    pub outcome: ToolOutcome,
    pub llm_view: ToolResultView,
    pub display: ToolDisplayPayload,
    pub storage: ToolStoragePayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<ToolRecoveryView>,
    pub output_policy: ToolOutputPolicySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallFailedEvent {
    pub error: String,
    pub outcome: ToolOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_view: Option<ToolResultView>,
    pub display: ToolDisplayPayload,
    pub storage: ToolStoragePayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<ToolRecoveryView>,
    pub output_policy: ToolOutputPolicySnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationContext {
    pub trace_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub attempt_id: u32,
    pub pipeline_stage: String,
    pub ts_unix_ms: i64,
    pub mono_ns: u64,
    pub event_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolEvent {
    pub schema_version: u16,
    pub call_id: String,
    pub tool_name: String,
    pub ts_unix_ms: i64,
    pub observation: ObservationContext,
    pub payload: ToolEventPayload,
}

impl ToolEvent {
    pub fn kind(&self) -> ToolEventKind {
        self.payload.kind()
    }
}

#[derive(Debug)]
pub struct DurableToolEventEnvelope {
    pub event: ToolEvent,
    committed_tx: Option<oneshot::Sender<Result<(), String>>>,
}

impl DurableToolEventEnvelope {
    pub fn acknowledge(mut self, result: Result<(), String>) {
        if let Some(committed_tx) = self.committed_tx.take() {
            let _ = committed_tx.send(result);
        }
    }
}

#[derive(Clone)]
pub struct ToolEventBus {
    tx: broadcast::Sender<ToolEvent>,
    durable_tx: Arc<Mutex<Option<tokio::sync::mpsc::Sender<DurableToolEventEnvelope>>>>,
    durable_installed: Arc<AtomicBool>,
    durable_capacity: usize,
    trace_sequences: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    thread_id: Arc<String>,
}

#[derive(Clone)]
pub struct ToolEventTrace {
    event_bus: ToolEventBus,
    trace_id: String,
    turn_id: String,
    tool_call_id: String,
    tool_name: String,
    sequence: Arc<AtomicU64>,
    thread_id: Arc<String>,
}

impl ToolEventTrace {
    pub fn trace_id(&self) -> &str {
        self.trace_id.as_str()
    }

    pub fn turn_id(&self) -> &str {
        self.turn_id.as_str()
    }

    pub fn tool_call_id(&self) -> &str {
        self.tool_call_id.as_str()
    }

    pub fn tool_name(&self) -> &str {
        self.tool_name.as_str()
    }

    /// Returns the trusted native patch identity and in-process observer for
    /// this invocation. The model cannot supply any of these values: the
    /// thread/turn/call identifiers come from the runtime trace.
    pub fn native_patch_context(&self) -> Option<(InvocationIdentity, Arc<dyn CommitObserver>)> {
        let identity = InvocationIdentity::new(
            self.thread_id.as_str(),
            self.turn_id.as_str(),
            self.tool_call_id.as_str(),
        )
        .ok()?;
        let observer = native_patch_observer_registry()
            .lock()
            .ok()
            .and_then(|registry| {
                registry
                    .get(&(
                        identity.thread_id.clone(),
                        identity.turn_id.clone(),
                        identity.invocation_id.clone(),
                    ))
                    .cloned()
            })?;
        Some((identity, observer))
    }

    fn next_context(&self, pipeline_stage: &str, attempt_id: u32) -> ObservationContext {
        let ts_unix_ms = now_unix_ms();
        let event_seq = self
            .sequence
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        ObservationContext {
            trace_id: self.trace_id.clone(),
            turn_id: self.turn_id.clone(),
            tool_call_id: self.tool_call_id.clone(),
            attempt_id,
            pipeline_stage: pipeline_stage.to_owned(),
            ts_unix_ms,
            mono_ns: monotonic_now_ns(),
            event_seq,
        }
    }

    pub fn emit_stage(
        &self,
        attempt_id: u32,
        pipeline_stage: &str,
        message: Option<String>,
        metadata: Option<JsonValue>,
    ) {
        debug!(
            target: "pioneer.tools.pipeline",
            trace_id = %self.trace_id,
            turn_id = %self.turn_id,
            tool_call_id = %self.tool_call_id,
            tool_name = %self.tool_name,
            attempt_id,
            stage = pipeline_stage,
            message = message.as_deref().unwrap_or(""),
            metadata = ?metadata,
            "tool pipeline stage"
        );
    }

    pub async fn emit_permission_audit(
        &self,
        attempt_id: u32,
        event: TurnPermissionAuditEvent,
    ) -> Result<(), ToolError> {
        self.event_bus
            .emit_durable(
                self,
                attempt_id,
                "permission.audit",
                ToolEventPayload::PermissionAudit(event),
            )
            .await
    }

    pub async fn emit_started(
        &self,
        attempt_id: u32,
        arguments: Option<JsonValue>,
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        turn_item_execution_class: TurnItemExecutionClass,
        output_policy: ToolOutputPolicySnapshot,
    ) -> Result<(), ToolError> {
        self.event_bus
            .emit_durable(
                self,
                attempt_id,
                "runtime.call.started",
                ToolEventPayload::CallStarted(ToolCallStartedEvent {
                    arguments,
                    recovery_policy,
                    turn_item_execution_class,
                    output_policy,
                }),
            )
            .await
    }

    /// Publish confirmed liveness without inventing visible tool output.
    pub fn emit_heartbeat(&self, attempt_id: u32) {
        self.event_bus.emit_progress(
            self,
            attempt_id,
            "runtime.call.heartbeat",
            ToolEventPayload::Heartbeat,
        );
    }

    pub fn emit_delta(&self, attempt_id: u32, text: String) {
        self.emit_output_chunk_delta(attempt_id, ItemDeltaStream::Generic, text, false);
    }

    pub fn emit_output_chunk_delta(
        &self,
        attempt_id: u32,
        stream: ItemDeltaStream,
        text: String,
        truncated: bool,
    ) {
        self.event_bus.emit_progress(
            self,
            attempt_id,
            "runtime.call.delta",
            ToolEventPayload::OutputDelta(ToolOutputDeltaEvent {
                delta: ToolDeltaPayload::OutputChunk {
                    stream,
                    text,
                    truncated,
                },
            }),
        );
    }

    pub fn emit_progress_delta(
        &self,
        attempt_id: u32,
        stage: impl Into<String>,
        metadata: Option<JsonValue>,
    ) {
        self.event_bus.emit_progress(
            self,
            attempt_id,
            "runtime.call.delta",
            ToolEventPayload::OutputDelta(ToolOutputDeltaEvent {
                delta: ToolDeltaPayload::Progress {
                    stage: stage.into(),
                    metadata: metadata
                        .map(ToolMetadata::from_json)
                        .unwrap_or_else(ToolMetadata::empty),
                },
            }),
        );
    }

    pub async fn emit_completed(
        &self,
        attempt_id: u32,
        envelope: &ToolResultEnvelope,
    ) -> Result<(), ToolError> {
        self.event_bus
            .emit_durable(
                self,
                attempt_id,
                "runtime.call.completed",
                ToolEventPayload::CallCompleted(ToolCallCompletedEvent {
                    success: envelope.success,
                    outcome: envelope.outcome.clone(),
                    llm_view: envelope.llm_view.clone(),
                    display: envelope.display.clone(),
                    storage: envelope.storage.clone(),
                    recovery: envelope.recovery.clone(),
                    output_policy: envelope.output_policy.clone(),
                }),
            )
            .await
    }

    pub async fn emit_failed_with_outcome(
        &self,
        attempt_id: u32,
        error: &str,
        outcome: &ToolOutcome,
        output_policy: ToolOutputPolicySnapshot,
    ) -> Result<(), ToolError> {
        let summary = ToolOutputSummary {
            title: "Tool execution failed".to_owned(),
            lines: vec![error.to_owned()],
            metadata: ToolMetadata::from_json(serde_json::json!({
                "errorClass": outcome.error_class.map(|class| format!("{class:?}")),
            })),
            truncated: false,
        };
        let display = display_for_policy(&output_policy, summary.clone());
        let storage = storage_for_policy(&output_policy, summary);
        let recovery = ToolRecoveryView {
            error_class: outcome.error_class.map(|class| format!("{class:?}")),
            retry_hint: outcome.retry_hint.clone(),
            incomplete_reason: outcome.incomplete_reason.clone(),
            diagnostic_summary: Some(error.to_owned()),
            diagnostic_excerpt: None,
            output_fingerprint: None,
            content_fingerprint: None,
            was_truncated: outcome.incomplete,
            continuation: None,
        };
        self.event_bus
            .emit_durable(
                self,
                attempt_id,
                "runtime.call.failed",
                ToolEventPayload::CallFailed(ToolCallFailedEvent {
                    error: error.to_owned(),
                    outcome: outcome.clone(),
                    llm_view: None,
                    display,
                    storage,
                    recovery: Some(recovery),
                    output_policy,
                }),
            )
            .await
    }
}

fn display_for_policy(
    output_policy: &ToolOutputPolicySnapshot,
    summary: ToolOutputSummary,
) -> ToolDisplayPayload {
    match output_policy.timeline {
        TimelineOutputPolicy::Full { .. } | TimelineOutputPolicy::Summary { .. } => {
            ToolDisplayPayload::Summary(summary)
        }
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            ToolDisplayPayload::Hidden
        }
    }
}

fn storage_for_policy(
    output_policy: &ToolOutputPolicySnapshot,
    summary: ToolOutputSummary,
) -> ToolStoragePayload {
    match output_policy.storage {
        StorageOutputPolicy::Full { .. } | StorageOutputPolicy::Summary { .. } => {
            ToolStoragePayload::Summary(summary)
        }
        StorageOutputPolicy::MetadataOnly => ToolStoragePayload::Metadata {
            metadata: summary.metadata,
        },
        StorageOutputPolicy::None => ToolStoragePayload::None,
    }
}

impl ToolEventBus {
    pub fn new(capacity: usize) -> Self {
        Self::with_thread_id(capacity, "unbound-thread")
    }

    /// Constructs a tool event bus bound to one trusted gateway thread. The
    /// binding is used only for native patch-history attribution; ordinary
    /// event delivery remains independent of it.
    pub fn with_thread_id(capacity: usize, thread_id: impl Into<String>) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            durable_tx: Arc::new(Mutex::new(None)),
            durable_installed: Arc::new(AtomicBool::new(false)),
            durable_capacity: capacity.max(1),
            trace_sequences: Arc::new(Mutex::new(HashMap::new())),
            thread_id: Arc::new(thread_id.into()),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ToolEvent> {
        self.tx.subscribe()
    }

    /// Installs the single lossless lifecycle consumer used by the native
    /// agent. Live observers remain on the lossy broadcast lane; start/audit/
    /// terminal tool events cannot be skipped by a lagging Gateway forwarder.
    pub fn take_durable_receiver(
        &self,
    ) -> Option<tokio::sync::mpsc::Receiver<DurableToolEventEnvelope>> {
        let mut durable_tx = self.durable_tx.lock().ok()?;
        if durable_tx.is_some() {
            return None;
        }
        if self.durable_installed.load(Ordering::SeqCst) {
            return None;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(self.durable_capacity);
        *durable_tx = Some(tx);
        self.durable_installed.store(true, Ordering::SeqCst);
        Some(rx)
    }

    /// Publishes the producer watermark for a native tool lifecycle lane.
    /// Traces may remain referenced by diagnostics after execution, so drain
    /// completion must not depend on every trace clone being dropped.
    pub fn close_durable_lane(&self) {
        if let Ok(mut durable_tx) = self.durable_tx.lock() {
            durable_tx.take();
        }
    }

    pub fn trace_with_id(
        &self,
        trace_id: impl Into<String>,
        turn_id: impl Into<String>,
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
    ) -> ToolEventTrace {
        let trace_id = trace_id.into();
        let sequence = {
            let mut guard = self.trace_sequences.lock().expect("trace sequences lock");
            guard
                .entry(trace_id.clone())
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .clone()
        };

        ToolEventTrace {
            event_bus: self.clone(),
            trace_id,
            turn_id: turn_id.into(),
            tool_call_id: tool_call_id.into(),
            tool_name: tool_name.into(),
            sequence,
            thread_id: self.thread_id.clone(),
        }
    }

    pub fn start_trace(
        &self,
        turn_id: impl Into<String>,
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
    ) -> ToolEventTrace {
        self.trace_with_id(self.new_trace_id(), turn_id, tool_call_id, tool_name)
    }

    pub fn new_trace_id(&self) -> String {
        static TRACE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let index = TRACE_COUNTER
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        format!("tr_{}_{}", now_unix_ms(), index)
    }

    fn build_event(
        &self,
        trace: &ToolEventTrace,
        attempt_id: u32,
        pipeline_stage: &str,
        payload: ToolEventPayload,
    ) -> ToolEvent {
        let observation = trace.next_context(pipeline_stage, attempt_id);
        ToolEvent {
            schema_version: 1,
            call_id: trace.tool_call_id.clone(),
            tool_name: trace.tool_name.clone(),
            ts_unix_ms: observation.ts_unix_ms,
            observation,
            payload,
        }
    }

    fn emit_progress(
        &self,
        trace: &ToolEventTrace,
        attempt_id: u32,
        pipeline_stage: &str,
        payload: ToolEventPayload,
    ) {
        debug_assert_eq!(payload.event_class(), ProtocolEventClass::Progress);
        let event = self.build_event(trace, attempt_id, pipeline_stage, payload);
        let _ = self.tx.send(event);
    }

    async fn emit_durable(
        &self,
        trace: &ToolEventTrace,
        attempt_id: u32,
        pipeline_stage: &str,
        payload: ToolEventPayload,
    ) -> Result<(), ToolError> {
        debug_assert_eq!(payload.event_class(), ProtocolEventClass::Durable);
        let event = self.build_event(trace, attempt_id, pipeline_stage, payload);
        let durable_tx = self
            .durable_tx
            .lock()
            .map_err(|_| ToolError::execution_failed("tool durable event lane lock poisoned"))?
            .clone();
        match (self.durable_installed.load(Ordering::SeqCst), durable_tx) {
            (true, Some(durable_tx)) => {
                let (committed_tx, committed_rx) = oneshot::channel();
                durable_tx
                    .send(DurableToolEventEnvelope {
                        event: event.clone(),
                        committed_tx: Some(committed_tx),
                    })
                    .await
                    .map_err(|_| {
                        ToolError::execution_failed(
                            "tool durable event lane closed before lifecycle event commit",
                        )
                    })?;
                committed_rx
                    .await
                    .map_err(|_| {
                        ToolError::execution_failed(
                            "tool durable event commit acknowledgement was dropped",
                        )
                    })?
                    .map_err(|reason| {
                        ToolError::execution_failed(format!(
                            "tool durable event commit was rejected: {reason}"
                        ))
                    })?;
            }
            (true, None) => {
                return Err(ToolError::execution_failed(
                    "tool durable event emitted after the producer watermark",
                ));
            }
            (false, None) => {}
            (false, Some(_)) => {
                return Err(ToolError::execution_failed(
                    "tool durable event lane entered an invalid installation state",
                ));
            }
        }
        let _ = self.tx.send(event);
        Ok(())
    }

    pub fn finish_trace(&self, trace_id: &str) {
        if let Ok(mut guard) = self.trace_sequences.lock() {
            guard.remove(trace_id);
        }
    }
}

impl Default for ToolEventBus {
    fn default() -> Self {
        Self::new(512)
    }
}

fn now_unix_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn monotonic_now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    let nanos = start.elapsed().as_nanos();
    u64::try_from(nanos).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output_policy::{ToolOutputProjectionKind, ToolResultView};
    use crate::output_projection::{ToolProjectionInput, project_tool_result};
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn emit_stage_does_not_publish_to_tool_event_bus() {
        let bus = ToolEventBus::new(4);
        let mut events = bus.subscribe();
        let trace = bus.start_trace("turn_1", "call_1", "grep_files");

        trace.emit_stage(
            1,
            "router.parse.started",
            Some("debug only".to_owned()),
            Some(serde_json::json!({ "arguments_length": 2 })),
        );

        assert!(
            timeout(Duration::from_millis(25), events.recv())
                .await
                .is_err(),
            "internal pipeline stages must not be delivered as tool events"
        );
    }

    #[test]
    fn output_delta_events_are_progress() {
        let event = ToolEventPayload::OutputDelta(ToolOutputDeltaEvent {
            delta: ToolDeltaPayload::OutputChunk {
                stream: ItemDeltaStream::Stdout,
                text: "chunk".to_owned(),
                truncated: false,
            },
        });

        assert_eq!(event.event_class(), ProtocolEventClass::Progress);
    }

    #[test]
    fn completed_calls_are_durable() {
        let event = ToolEventPayload::CallCompleted(ToolCallCompletedEvent {
            success: true,
            outcome: ToolOutcome::ok(),
            llm_view: ToolResultView::Empty,
            display: ToolDisplayPayload::default(),
            storage: ToolStoragePayload::default(),
            recovery: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("test_tool"),
        });

        assert_eq!(event.event_class(), ProtocolEventClass::Durable);
    }

    #[tokio::test]
    async fn native_durable_receiver_preserves_terminal_event_when_live_lane_lags() {
        let bus = ToolEventBus::new(1);
        let mut live = bus.subscribe();
        let mut durable = bus
            .take_durable_receiver()
            .expect("single durable receiver");
        let trace = bus.start_trace("turn_1", "call_1", "test_tool");
        let outcome = ToolOutcome::ok();
        let output_policy = ToolOutputPolicySnapshot::for_tool_name("test_tool");
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_1",
            tool_name: "test_tool",
            arguments: &serde_json::json!({}),
            raw_output_text: "ok",
            raw_output_json: &serde_json::json!({"ok": true}),
            success: true,
            outcome: &outcome,
            output_policy: &output_policy,
            output_projection: &ToolOutputProjectionKind::Builtin,
        });

        let started_trace = trace.clone();
        let started = tokio::spawn(async move {
            started_trace
                .emit_started(
                    1,
                    None,
                    None,
                    TurnItemExecutionClass::Standard,
                    output_policy,
                )
                .await
        });
        tokio::pin!(started);
        assert!(
            timeout(Duration::from_millis(20), &mut started)
                .await
                .is_err(),
            "durable producer must wait for the canonical commit acknowledgement"
        );
        for index in 0..32 {
            trace.emit_delta(1, format!("chunk-{index}"));
        }
        let completion_trace = trace.clone();
        let completion =
            tokio::spawn(async move { completion_trace.emit_completed(1, &envelope).await });
        tokio::pin!(completion);
        assert!(
            timeout(Duration::from_millis(20), &mut completion)
                .await
                .is_err(),
            "a full durable lane must apply backpressure instead of dropping terminal state"
        );

        let started_envelope = durable
            .recv()
            .await
            .expect("started event should remain queued");
        assert_eq!(started_envelope.event.kind(), ToolEventKind::CallStarted);
        started_envelope.acknowledge(Ok(()));
        started
            .await
            .expect("started publisher should not panic")
            .expect("started publisher should resume after commit ACK");
        let completed_envelope = durable
            .recv()
            .await
            .expect("completed event should remain queued");
        assert_eq!(
            completed_envelope.event.kind(),
            ToolEventKind::CallCompleted
        );
        completed_envelope.acknowledge(Ok(()));
        completion
            .await
            .expect("completion publisher should not panic")
            .expect("completion should resume after durable commit ACK");
        assert!(
            timeout(Duration::from_millis(20), durable.recv())
                .await
                .is_err(),
            "lossy output deltas must not consume durable queue capacity"
        );
        bus.close_durable_lane();
        assert!(
            durable.recv().await.is_none(),
            "producer watermark must close drain even while a trace clone remains alive"
        );
        assert!(
            trace
                .emit_started(
                    2,
                    None,
                    None,
                    TurnItemExecutionClass::Standard,
                    ToolOutputPolicySnapshot::for_tool_name("test_tool"),
                )
                .await
                .is_err(),
            "late durable lifecycle events must fail closed after the watermark"
        );
        assert!(matches!(
            live.recv().await,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        ));
    }
}
