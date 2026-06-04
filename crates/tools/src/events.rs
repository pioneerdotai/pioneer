use crate::context::ToolOutcome;
use crate::output_policy::{
    ToolDisplayPayload, ToolOutputPolicySnapshot, ToolRecoveryView, ToolResultEnvelope,
    ToolResultView, ToolStoragePayload,
};
use pioneer_protocol::{
    ItemDeltaStream, ProtocolEventClass, StorageOutputPolicy, TimelineOutputPolicy, ToolMetadata,
    ToolOutputSummary, ToolRecoveryPolicySnapshot,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;
use tokio::sync::broadcast;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEventKind {
    CallStarted,
    OutputDelta,
    CallCompleted,
    CallFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolEventPayload {
    CallStarted(ToolCallStartedEvent),
    OutputDelta(ToolOutputDeltaEvent),
    CallCompleted(ToolCallCompletedEvent),
    CallFailed(ToolCallFailedEvent),
}

impl ToolEventPayload {
    pub fn kind(&self) -> ToolEventKind {
        match self {
            Self::CallStarted(_) => ToolEventKind::CallStarted,
            Self::OutputDelta(_) => ToolEventKind::OutputDelta,
            Self::CallCompleted(_) => ToolEventKind::CallCompleted,
            Self::CallFailed(_) => ToolEventKind::CallFailed,
        }
    }

    pub fn event_class(&self) -> ProtocolEventClass {
        match self {
            Self::OutputDelta(_) => ProtocolEventClass::Progress,
            Self::CallStarted(_) | Self::CallCompleted(_) | Self::CallFailed(_) => {
                ProtocolEventClass::Durable
            }
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

#[derive(Clone)]
pub struct ToolEventBus {
    tx: broadcast::Sender<ToolEvent>,
    trace_sequences: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
}

#[derive(Clone)]
pub struct ToolEventTrace {
    event_bus: ToolEventBus,
    trace_id: String,
    turn_id: String,
    tool_call_id: String,
    tool_name: String,
    sequence: Arc<AtomicU64>,
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

    pub fn emit_started(
        &self,
        attempt_id: u32,
        arguments: Option<JsonValue>,
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
    ) {
        self.event_bus.emit_internal(
            self,
            attempt_id,
            "runtime.call.started",
            ToolEventPayload::CallStarted(ToolCallStartedEvent {
                arguments,
                recovery_policy,
                output_policy,
            }),
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
        self.event_bus.emit_internal(
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
        self.event_bus.emit_internal(
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

    pub fn emit_completed(&self, attempt_id: u32, envelope: &ToolResultEnvelope) {
        self.event_bus.emit_internal(
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
        );
    }

    pub fn emit_failed_with_outcome(
        &self,
        attempt_id: u32,
        error: &str,
        outcome: &ToolOutcome,
        output_policy: ToolOutputPolicySnapshot,
    ) {
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
        self.event_bus.emit_internal(
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
        );
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
        let (tx, _) = broadcast::channel(capacity);
        Self {
            tx,
            trace_sequences: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ToolEvent> {
        self.tx.subscribe()
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

    fn emit_internal(
        &self,
        trace: &ToolEventTrace,
        attempt_id: u32,
        pipeline_stage: &str,
        payload: ToolEventPayload,
    ) {
        let observation = trace.next_context(pipeline_stage, attempt_id);
        let _ = self.tx.send(ToolEvent {
            schema_version: 1,
            call_id: trace.tool_call_id.clone(),
            tool_name: trace.tool_name.clone(),
            ts_unix_ms: observation.ts_unix_ms,
            observation,
            payload,
        });
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
    async fn write_file_completed_event_carries_file_change_metadata() {
        let raw_output_json = serde_json::json!({
            "ok": true,
            "status": "completed",
            "operation": "created",
            "path": "docs/example.md",
            "resolved_path": "/tmp/project/docs/example.md",
            "bytes_written": 5,
            "sha256": "abc123",
            "file_observation": {
                "id": "write_file:call_write",
                "path": "/tmp/project/docs/example.md",
                "bytes": 5,
                "sha256": "abc123",
                "mtime_ms": 1234,
                "complete": true,
                "source_tool_call_id": "call_write"
            },
            "created_dirs": [],
            "changed_files": ["/tmp/project/docs/example.md"],
            "content": "SECRET_WRITE_FILE_CONTENT"
        });
        let outcome = ToolOutcome::ok();
        let output_policy = ToolOutputPolicySnapshot::for_tool_name("write_file");
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_write",
            tool_name: "write_file",
            arguments: &serde_json::json!({"path": "docs/example.md"}),
            raw_output_text: "write_file completed: created /tmp/project/docs/example.md, 5 bytes.",
            raw_output_json: &raw_output_json,
            success: true,
            outcome: &outcome,
            output_policy: &output_policy,
            output_projection: &ToolOutputProjectionKind::Builtin,
        });
        let bus = ToolEventBus::new(4);
        let mut events = bus.subscribe();
        let trace = bus.start_trace("turn_1", "call_write", "write_file");

        trace.emit_completed(1, &envelope);

        let event = timeout(Duration::from_millis(100), events.recv())
            .await
            .expect("event should arrive")
            .expect("event should decode");
        let ToolEventPayload::CallCompleted(completed) = event.payload else {
            panic!("write_file completion should emit CallCompleted");
        };
        let ToolDisplayPayload::Summary(display_summary) = &completed.display else {
            panic!("write_file display should be a summary");
        };
        let ToolStoragePayload::Metadata { metadata } = &completed.storage else {
            panic!("write_file storage should be metadata-only");
        };
        let display_metadata = display_summary.metadata.to_json();
        let storage_metadata = metadata.to_json();

        assert_eq!(event.tool_name, "write_file");
        assert_eq!(display_summary.title, "write_file created 1 file(s)");
        assert_eq!(
            display_metadata["changedFiles"][0],
            "/tmp/project/docs/example.md"
        );
        assert_eq!(storage_metadata["operation"], "created");
        assert_eq!(storage_metadata["bytesWritten"], 5);
        assert_eq!(storage_metadata["sha256"], "abc123");
        assert!(
            !serde_json::to_string(&completed)
                .unwrap()
                .contains("SECRET_WRITE_FILE_CONTENT")
        );
    }

    #[tokio::test]
    async fn edit_file_completed_event_carries_file_change_metadata() {
        let raw_output_json = serde_json::json!({
            "ok": true,
            "status": "completed",
            "operation": "edited",
            "path": "src/lib.rs",
            "resolved_path": "/tmp/project/src/lib.rs",
            "matches": 1,
            "matches_replaced": 1,
            "replace_all": false,
            "bytes_before": 44,
            "bytes_after": 45,
            "bytes_written": 45,
            "sha256_before": "before123",
            "sha256": "after456",
            "old_string_bytes": 31,
            "new_string_bytes": 31,
            "old_string_sha256": "oldhash",
            "new_string_sha256": "newhash",
            "line_ending_mode": "lf",
            "file_observation": {
                "id": "edit_file:call_edit",
                "path": "/tmp/project/src/lib.rs",
                "bytes": 45,
                "sha256": "after456",
                "mtime_ms": 1234,
                "complete": true,
                "source_tool_call_id": "call_edit"
            },
            "changed_files": ["/tmp/project/src/lib.rs"],
            "old_string": "SECRET_EDIT_OLD_SENTINEL",
            "new_string": "SECRET_EDIT_NEW_SENTINEL",
            "content": "SECRET_EDIT_FINAL_SENTINEL"
        });
        let outcome = ToolOutcome::ok();
        let output_policy = ToolOutputPolicySnapshot::for_tool_name("edit_file");
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_edit",
            tool_name: "edit_file",
            arguments: &serde_json::json!({"path": "src/lib.rs"}),
            raw_output_text: "edit_file completed: edited /tmp/project/src/lib.rs, replaced 1 occurrence, 45 bytes.",
            raw_output_json: &raw_output_json,
            success: true,
            outcome: &outcome,
            output_policy: &output_policy,
            output_projection: &ToolOutputProjectionKind::Builtin,
        });
        let bus = ToolEventBus::new(4);
        let mut events = bus.subscribe();
        let trace = bus.start_trace("turn_1", "call_edit", "edit_file");

        trace.emit_completed(1, &envelope);

        let event = timeout(Duration::from_millis(100), events.recv())
            .await
            .expect("event should arrive")
            .expect("event should decode");
        let ToolEventPayload::CallCompleted(completed) = event.payload else {
            panic!("edit_file completion should emit CallCompleted");
        };
        let ToolDisplayPayload::Summary(display_summary) = &completed.display else {
            panic!("edit_file display should be a summary");
        };
        let ToolStoragePayload::Metadata { metadata } = &completed.storage else {
            panic!("edit_file storage should be metadata-only");
        };
        let display_metadata = display_summary.metadata.to_json();
        let storage_metadata = metadata.to_json();

        assert_eq!(event.tool_name, "edit_file");
        assert_eq!(display_summary.title, "edit_file edited 1 file(s)");
        assert_eq!(
            display_metadata["changedFiles"][0],
            "/tmp/project/src/lib.rs"
        );
        assert_eq!(storage_metadata["operation"], "edited");
        assert_eq!(storage_metadata["matchesReplaced"], 1);
        assert_eq!(storage_metadata["replaceAll"], false);
        assert_eq!(storage_metadata["bytesBefore"], 44);
        assert_eq!(storage_metadata["bytesAfter"], 45);
        assert_eq!(storage_metadata["bytesWritten"], 45);
        assert_eq!(storage_metadata["sha256Before"], "before123");
        assert_eq!(storage_metadata["sha256"], "after456");
        assert_eq!(storage_metadata["oldStringSha256"], "oldhash");
        assert_eq!(storage_metadata["newStringSha256"], "newhash");
        assert_eq!(storage_metadata["lineEndingMode"], "lf");
        let completed_json = serde_json::to_string(&completed).unwrap();
        assert!(!completed_json.contains("SECRET_EDIT_OLD_SENTINEL"));
        assert!(!completed_json.contains("SECRET_EDIT_NEW_SENTINEL"));
        assert!(!completed_json.contains("SECRET_EDIT_FINAL_SENTINEL"));
    }
}
