use crate::{MarkdownDocument, SandboxPolicy, TaskEvent, TaskTurnItem, ThreadMode};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ByteRange {
    pub start: usize,
    pub end: usize,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(transparent)]
pub struct ToolMetadata {
    fields: BTreeMap<String, ToolMetadataValue>,
}

impl ToolMetadata {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_json(value: JsonValue) -> Self {
        match value {
            JsonValue::Object(map) => Self {
                fields: map
                    .into_iter()
                    .map(|(key, value)| {
                        let metadata_value =
                            ToolMetadataValue::from_json_with_key(value, Some(key.as_str()));
                        (key, metadata_value)
                    })
                    .collect(),
            },
            JsonValue::Null => Self::default(),
            value => {
                let mut fields = BTreeMap::new();
                fields.insert(
                    "value".to_owned(),
                    ToolMetadataValue::from_json_with_key(value, Some("value")),
                );
                Self { fields }
            }
        }
    }

    pub fn to_json(&self) -> JsonValue {
        JsonValue::Object(
            self.fields
                .iter()
                .map(|(key, value)| (key.clone(), value.to_json()))
                .collect(),
        )
    }

    pub fn get(&self, key: &str) -> Option<&ToolMetadataValue> {
        self.fields.get(key)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: ToolMetadataValue) {
        self.fields.insert(key.into(), value);
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl From<JsonValue> for ToolMetadata {
    fn from(value: JsonValue) -> Self {
        Self::from_json(value)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolMetadataValue {
    Null,
    Bool {
        value: bool,
    },
    Number {
        value: String,
    },
    String {
        value: String,
    },
    Array {
        values: Vec<ToolMetadataValue>,
    },
    Object {
        fields: BTreeMap<String, ToolMetadataValue>,
    },
    RedactedRaw {
        raw_kind: ToolMetadataRawKind,
        sha256: String,
        bytes: usize,
        value_kind: String,
    },
}

impl ToolMetadataValue {
    pub fn from_json(value: JsonValue) -> Self {
        Self::from_json_with_key(value, None)
    }

    fn from_json_with_key(value: JsonValue, key_hint: Option<&str>) -> Self {
        if key_hint.is_some_and(is_raw_like_metadata_key) && !value.is_null() {
            let serialized = serde_json::to_vec(&value).unwrap_or_default();
            return Self::RedactedRaw {
                raw_kind: ToolMetadataRawKind::from_key_hint(key_hint.unwrap_or_default()),
                sha256: sha256_hex(serialized.as_slice()),
                bytes: serialized.len(),
                value_kind: json_value_kind(&value).to_owned(),
            };
        }

        match value {
            JsonValue::Null => Self::Null,
            JsonValue::Bool(value) => Self::Bool { value },
            JsonValue::Number(value) => Self::Number {
                value: value.to_string(),
            },
            JsonValue::String(value) => Self::String { value },
            JsonValue::Array(values) => Self::Array {
                values: values.into_iter().map(Self::from_json).collect(),
            },
            JsonValue::Object(map) => Self::Object {
                fields: map
                    .into_iter()
                    .map(|(key, value)| {
                        let metadata_value = Self::from_json_with_key(value, Some(key.as_str()));
                        (key, metadata_value)
                    })
                    .collect(),
            },
        }
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            Self::Null => JsonValue::Null,
            Self::Bool { value } => JsonValue::Bool(*value),
            Self::Number { value } => metadata_number_to_json(value),
            Self::String { value } => JsonValue::String(value.clone()),
            Self::Array { values } => JsonValue::Array(
                values
                    .iter()
                    .map(ToolMetadataValue::to_json)
                    .collect::<Vec<_>>(),
            ),
            Self::Object { fields } => JsonValue::Object(
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), value.to_json()))
                    .collect(),
            ),
            Self::RedactedRaw {
                raw_kind,
                sha256,
                bytes,
                value_kind,
            } => serde_json::json!({
                "kind": "redacted_raw",
                "rawKind": raw_kind,
                "sha256": sha256,
                "bytes": bytes,
                "valueKind": value_kind,
            }),
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String { value } => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool { value } => Some(*value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number { value } => value.parse::<u64>().ok(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number { value } => value.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[ToolMetadataValue]> {
        match self {
            Self::Array { values } => Some(values.as_slice()),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, ToolMetadataValue>> {
        match self {
            Self::Object { fields } => Some(fields),
            _ => None,
        }
    }
}

fn metadata_number_to_json(value: &str) -> JsonValue {
    if let Ok(value) = value.parse::<u64>() {
        return JsonValue::Number(value.into());
    }
    if let Ok(value) = value.parse::<i64>() {
        return JsonValue::Number(value.into());
    }
    serde_json::Number::from_f64(value.parse::<f64>().unwrap_or_default())
        .map(JsonValue::Number)
        .unwrap_or(JsonValue::Null)
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolMetadataRawKind {
    Content,
    Body,
    Blob,
    Base64,
    Bytes,
    Data,
    Html,
    Image,
    Output,
    Screenshot,
    Stdout,
    Stderr,
    Text,
    Unknown,
}

impl ToolMetadataRawKind {
    fn from_key_hint(key: &str) -> Self {
        match normalize_metadata_key(key).as_str() {
            "content" => Self::Content,
            "body" => Self::Body,
            "blob" => Self::Blob,
            "base64" => Self::Base64,
            "bytes" => Self::Bytes,
            "data" | "dataurl" | "data_url" => Self::Data,
            "html" => Self::Html,
            "image" => Self::Image,
            "output" | "outputjson" | "output_json" => Self::Output,
            "screenshot" => Self::Screenshot,
            "stdout" => Self::Stdout,
            "stderr" => Self::Stderr,
            "text" => Self::Text,
            _ => Self::Unknown,
        }
    }
}

fn is_raw_like_metadata_key(key: &str) -> bool {
    matches!(
        normalize_metadata_key(key).as_str(),
        "content"
            | "body"
            | "blob"
            | "base64"
            | "bytes"
            | "data"
            | "dataurl"
            | "data_url"
            | "html"
            | "image"
            | "output"
            | "outputjson"
            | "output_json"
            | "screenshot"
            | "stdout"
            | "stderr"
            | "text"
    )
}

fn normalize_metadata_key(key: &str) -> String {
    key.chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn json_value_kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
pub struct TurnStartParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub input: Vec<UserInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<SandboxPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ThreadMode>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnStartResponse {
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnCancelParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnCancelResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnGetParams {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnGetResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
pub struct TurnItemsParams {
    pub thread_id: String,
    pub turn_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnItemsResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub events: Vec<TurnItemEvent>,
    pub last_sequence: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnTimelineParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default = "default_compose_tasks")]
    pub compose_tasks: bool,
    #[serde(default)]
    pub include_collapsed_task_events: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_child_items_per_task: Option<u32>,
}

impl Default for TurnTimelineParams {
    fn default() -> Self {
        Self {
            thread_id: String::new(),
            turn_id: String::new(),
            compose_tasks: true,
            include_collapsed_task_events: false,
            max_child_items_per_task: Some(100),
        }
    }
}

fn default_compose_tasks() -> bool {
    true
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimelineOriginKind {
    ParentTurn,
    TaskEvent,
    ChildTurn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimelineLane {
    Parent,
    Task,
    ChildAgent,
    ChildTool,
    ChildReasoning,
    ChildResult,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TimelineOrigin {
    pub kind: TimelineOriginKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_turn_item_id: Option<String>,
    pub origin_sequence: i64,
    pub occurred_at: i64,
    pub lane: TimelineLane,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimelinePayload {
    TurnItemEvent { event: TurnItemEvent },
    TaskEvent { event: TaskEvent },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TimelineItem {
    pub id: String,
    pub origin: TimelineOrigin,
    pub payload: TimelinePayload,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TurnTimelineResponse {
    pub thread_id: String,
    pub workspace_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub items: Vec<TimelineItem>,
    pub last_sequence: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnTimelineChangedReason {
    ParentTurnChanged,
    TaskEventChanged,
    ChildTurnChanged,
    CollapseStateHintChanged,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnTimelineChangedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_turn_id: Option<String>,
    pub reason: TurnTimelineChangedReason,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TurnItemEventPayload {
    #[serde(rename_all = "camelCase")]
    ItemStarted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemDelta {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream: Option<ItemDeltaStream>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<JsonValue>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown: Option<MarkdownDocument>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown_version: Option<u16>,
    },
    #[serde(rename_all = "camelCase")]
    ItemCompleted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemUpdated {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    #[serde(rename_all = "camelCase")]
    ItemTimeoutDetected {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        attempt_number: u32,
        reason: TurnItemTimeoutReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_job_id: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryOpened {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryAttached {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        recovery_item_id: String,
        recovery_item_type: TurnItemType,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        existing_status: RecoveryJobStatus,
        next_attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRetryScheduled {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        next_run_at_unix: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    ItemRetryAttemptStarted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoverySucceeded {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    #[serde(rename_all = "camelCase")]
    ItemRecoveryExhausted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        status: RecoveryJobStatus,
        error_message: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryScheduled {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        retry_hint: String,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryResolved {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        resolution: ToolRetryResolution,
        budgets: Vec<ToolRetryBudgetUsage>,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    ItemToolRetryExhausted {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        exhaustion_kind: ToolRetryExhaustionKind,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    TurnToolLoopBudgetExceeded {
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        limit_kind: ToolLoopBudgetLimitKind,
        limit: u32,
        observed: u32,
        action: ToolLoopBudgetAction,
        reason: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnItemEvent {
    pub sequence: i64,
    pub created_at: i64,
    pub payload: TurnItemEventPayload,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct Turn {
    pub id: String,
    pub status: TurnStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_manifest: Option<PromptManifest>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct PromptManifest {
    pub compiler_version: String,
    pub profile: PromptManifestProfile,
    #[serde(default)]
    pub section_ids: Vec<String>,
    pub fingerprint_stable: String,
    pub fingerprint_dynamic: String,
    pub fingerprint_full: String,
    #[serde(default)]
    pub diagnostics: Vec<PromptManifestDiagnostic>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestProfile {
    AssistantFull,
    AssistantMinimal,
    AssistantNone,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct PromptManifestDiagnostic {
    pub code: PromptManifestDiagnosticCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PromptManifestDiagnosticCode {
    MissingFile,
    FileReadError,
    FileTruncated,
    TotalBudgetTruncated,
    FileFilteredByProfile,
    DynamicSectionTruncated,
    DynamicSectionOmitted,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnStatus {
    InProgress,
    Completed,
    Failed,
    Interrupted,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemType {
    UserMessage,
    AgentMessage,
    Reasoning,
    SystemEvent,
    Task,
    CommandExecution,
    FileChange,
    WebSearch,
    WebFetch,
    Download,
    DynamicToolCall,
}

impl TurnItemType {
    pub const fn is_tool_item(self) -> bool {
        matches!(
            self,
            Self::CommandExecution
                | Self::FileChange
                | Self::WebSearch
                | Self::WebFetch
                | Self::Download
                | Self::DynamicToolCall
        )
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemAttemptStatus {
    Running,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
    Interrupted,
    Retrying,
    Exhausted,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TurnItemTimeoutReason {
    StartDeadlineExceeded,
    IdleDeadlineExceeded,
    HardDeadlineExceeded,
    LeaseExpired,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryJobStatus {
    Pending,
    Active,
    Succeeded,
    Failed,
    Exhausted,
    Cancelled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryTrigger {
    /// Turn-item execution exceeded timeout policy (start/idle/hard/lease).
    Timeout,
    /// Any provider/model/transport failure in LLM interaction path.
    ProviderError,
    /// Forward-compatibility fallback for unknown persisted values.
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAction {
    RetryAttempt,
    RetryWithBackoff,
    RestartTurn,
    Fallback,
    MarkFailed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRecoveryRetryClass {
    Never,
    Transient,
    Arguments,
    Session,
    Network,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolRecoveryIdempotencyMode {
    None,
    Safe,
    RequiresKey,
    SessionBound,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolRecoveryPolicySnapshot {
    pub retry_class: ToolRecoveryRetryClass,
    pub idempotency_mode: ToolRecoveryIdempotencyMode,
    pub max_attempts: u8,
    pub can_resume: bool,
    pub resolved_action: RecoveryAction,
    pub base_backoff_secs: u64,
    pub max_wall_clock_secs: u64,
    pub no_progress_limit: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutputPolicySnapshot {
    pub llm: LlmOutputPolicy,
    pub llm_retention: LlmRetentionPolicy,
    pub timeline: TimelineOutputPolicy,
    pub storage: StorageOutputPolicy,
    pub recovery: RecoveryOutputPolicy,
    pub deltas: DeltaOutputPolicy,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum LlmOutputPolicy {
    Full { max_bytes: usize },
    Structured { max_bytes: usize },
    SummaryOnly,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum LlmRetentionPolicy {
    UntilTurnTerminal { max_bytes: usize },
    DoNotRetain,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum TimelineOutputPolicy {
    Full { max_bytes: usize },
    Summary { max_chars: usize },
    MetadataOnly,
    Hidden,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum StorageOutputPolicy {
    Full { max_bytes: usize },
    Summary { max_chars: usize },
    MetadataOnly,
    None,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum RecoveryOutputPolicy {
    Evidence {
        include_exit_status: bool,
        include_error_class: bool,
        include_retry_hint: bool,
        diagnostic_excerpt: DiagnosticExcerptPolicy,
        include_fingerprints: bool,
    },
    MetadataOnly,
    None,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum DiagnosticExcerptPolicy {
    Disabled,
    ErrorsOnly { max_chars: usize },
    Output { max_chars: usize },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum DeltaOutputPolicy {
    PersistAndDisplay {
        max_chunk_bytes: usize,
        max_total_bytes: usize,
    },
    ProgressOnly,
    Disabled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutputSummary {
    pub title: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<String>,
    pub metadata: ToolMetadata,
    pub truncated: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolDisplayPayload {
    Shell {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stdout: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stderr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aggregated_output: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timed_out: Option<bool>,
        truncated: bool,
    },
    Summary(ToolOutputSummary),
    Progress {
        stage: String,
        metadata: ToolMetadata,
    },
    Hidden,
}

impl Default for ToolDisplayPayload {
    fn default() -> Self {
        Self::Hidden
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ToolStoragePayload {
    Shell {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stdout: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stderr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aggregated_output: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timed_out: Option<bool>,
        truncated: bool,
    },
    Summary(ToolOutputSummary),
    Metadata {
        metadata: ToolMetadata,
    },
    None,
}

impl Default for ToolStoragePayload {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolRecoveryView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_excerpt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_fingerprint: Option<String>,
    pub was_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<JsonValue>,
}

impl ToolOutputPolicySnapshot {
    pub fn for_tool_name(tool_name: &str) -> Self {
        match tool_name {
            "exec_command" | "write_stdin" => shell_output_policy_snapshot(),
            "web_fetch" => web_fetch_output_policy_snapshot(),
            "web_search" => web_search_output_policy_snapshot(),
            "download_url" | "download" => download_output_policy_snapshot(),
            "computer_use" => computer_use_output_policy_snapshot(),
            "read_file" | "read_skill" | "list_dir" | "grep_files" | "apply_patch"
            | "tool_search" | "tool_suggest" => model_only_metadata_policy_snapshot(),
            _ => dynamic_unknown_output_policy_snapshot(),
        }
    }
}

const DEFAULT_LLM_MAX_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_SUMMARY_CHARS: usize = 2_000;
const DEFAULT_SHELL_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_SHELL_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_DIAGNOSTIC_CHARS: usize = 4_000;

fn retained_llm_policy() -> LlmRetentionPolicy {
    LlmRetentionPolicy::UntilTurnTerminal {
        max_bytes: DEFAULT_LLM_MAX_BYTES,
    }
}

fn evidence_recovery_policy(diagnostic_excerpt: DiagnosticExcerptPolicy) -> RecoveryOutputPolicy {
    RecoveryOutputPolicy::Evidence {
        include_exit_status: true,
        include_error_class: true,
        include_retry_hint: true,
        diagnostic_excerpt,
        include_fingerprints: true,
    }
}

fn shell_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Full {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Full {
            max_bytes: DEFAULT_SHELL_TOTAL_BYTES,
        },
        storage: StorageOutputPolicy::Full {
            max_bytes: DEFAULT_SHELL_TOTAL_BYTES,
        },
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::ErrorsOnly {
            max_chars: DEFAULT_DIAGNOSTIC_CHARS,
        }),
        deltas: DeltaOutputPolicy::PersistAndDisplay {
            max_chunk_bytes: DEFAULT_SHELL_CHUNK_BYTES,
            max_total_bytes: DEFAULT_SHELL_TOTAL_BYTES,
        },
    }
}

fn model_only_metadata_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Full {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::Disabled,
    }
}

fn web_fetch_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Full {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn web_search_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn download_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn computer_use_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::Summary {
            max_chars: DEFAULT_SUMMARY_CHARS,
        },
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: evidence_recovery_policy(DiagnosticExcerptPolicy::Disabled),
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

fn dynamic_unknown_output_policy_snapshot() -> ToolOutputPolicySnapshot {
    ToolOutputPolicySnapshot {
        llm: LlmOutputPolicy::Structured {
            max_bytes: DEFAULT_LLM_MAX_BYTES,
        },
        llm_retention: retained_llm_policy(),
        timeline: TimelineOutputPolicy::MetadataOnly,
        storage: StorageOutputPolicy::MetadataOnly,
        recovery: RecoveryOutputPolicy::MetadataOnly,
        deltas: DeltaOutputPolicy::ProgressOnly,
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureClass {
    NetworkTransient,
    RateLimit,
    #[serde(rename = "provider_5xx")]
    Provider5xx,
    AuthExpired,
    ModelNotFound,
    PromptTooLong,
    MaxOutputTokens,
    StreamStall,
    StreamTruncated,
    InvalidRequest,
    PermissionDenied,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTransportKind {
    Stream,
    NonStream,
    Ws,
    Sse,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFailureStage {
    Connect,
    FirstChunk,
    MidStream,
    Finalize,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ProviderFailureDetails {
    pub provider: String,
    pub model: String,
    pub transport: ProviderTransportKind,
    pub class: ProviderFailureClass,
    pub stage: ProviderFailureStage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub is_recoverable_hint: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    InProgress,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ItemDeltaStream {
    AgentMessage,
    Stdout,
    Stderr,
    ToolProgress,
    FileChange,
    Generic,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SystemEventLevel {
    Info,
    Warning,
    Error,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolObservation {
    pub trace_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
    pub attempt_id: u32,
    pub pipeline_stage: String,
    pub ts_unix_ms: i64,
    pub mono_ns: u64,
    pub event_seq: u64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcomeStatus {
    Ok,
    RecoverableError,
    FatalError,
    PartialSuccess,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorClass {
    InvalidArguments,
    NotFound,
    PermissionDenied,
    CommandNotFound,
    Timeout,
    Cancelled,
    ExecutionFailed,
    NeedsNarrowing,
    Internal,
    OutputTruncated,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryErrorClass {
    InvalidArguments,
    NotFound,
    PermissionDenied,
    CommandNotFound,
    Timeout,
    Cancelled,
    ExecutionFailed,
    NeedsNarrowing,
    Internal,
    OutputTruncated,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryBudgetKind {
    Episode,
    ErrorClass,
    ToolName,
    FailureSignature,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ToolRetryBudgetUsage {
    pub kind: ToolRetryBudgetKind,
    pub used: u32,
    pub limit: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryResolution {
    Succeeded,
    NonRetryable,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolRetryExhaustionKind {
    TotalRetryRounds,
    ErrorClass,
    ToolName,
    FailureSignature,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolLoopBudgetLimitKind {
    AgentRounds,
    ToolCalls,
    ProviderReturnedToolsAfterToolsDisabled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolLoopBudgetAction {
    RequestFinalNoToolsRound,
    FailTurn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolOutcome {
    pub status: ToolOutcomeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<ToolErrorClass>,
    pub should_retry: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_hint: Option<String>,
    pub incomplete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WebSearchResultItem {
    pub rank: usize,
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct WebFetchLink {
    pub text: String,
    pub url: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TextElement {
    pub byte_range: ByteRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserInput {
    #[serde(rename_all = "camelCase")]
    Text {
        text: String,
        #[serde(default)]
        text_elements: Vec<TextElement>,
    },
    Image {
        url: String,
    },
    LocalImage {
        path: String,
    },
    File {
        url: String,
    },
    LocalFile {
        path: String,
    },
    Audio {
        url: String,
    },
    LocalAudio {
        path: String,
    },
    Video {
        url: String,
    },
    LocalVideo {
        path: String,
    },
    Skill {
        name: String,
        path: String,
    },
    Mention {
        name: String,
        path: String,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum UserMessageAttachment {
    Image { url: String },
    LocalImage { path: String },
    File { url: String },
    LocalFile { path: String },
    Audio { url: String },
    LocalAudio { path: String },
    Video { url: String },
    LocalVideo { path: String },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TurnItem {
    #[serde(rename_all = "camelCase")]
    UserMessage {
        id: String,
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<UserMessageAttachment>,
    },
    #[serde(rename_all = "camelCase")]
    AgentMessage {
        id: String,
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown: Option<MarkdownDocument>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown_version: Option<u16>,
    },
    #[serde(rename_all = "camelCase")]
    Reasoning {
        id: String,
        #[serde(default)]
        summary: Vec<String>,
        #[serde(default)]
        content: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    SystemEvent {
        id: String,
        level: SystemEventLevel,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<JsonValue>,
    },
    #[serde(rename_all = "camelCase")]
    Task {
        #[serde(flatten)]
        item: TaskTurnItem,
    },
    #[serde(rename_all = "camelCase")]
    CommandExecution {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default)]
        command: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    FileChange {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default)]
        changed_files: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stdout: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stderr: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    WebSearch {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        took_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result_count: Option<usize>,
        #[serde(default)]
        results: Vec<WebSearchResultItem>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    WebFetch {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status_code: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extract_mode: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolved_mode: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bytes_received: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elapsed_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        truncated: Option<JsonValue>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        word_count: Option<usize>,
        #[serde(default)]
        links: Vec<WebFetchLink>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    Download {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status_code: Option<u16>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        bytes_written: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sha256: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elapsed_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        truncated: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
    #[serde(rename_all = "camelCase")]
    DynamicToolCall {
        id: String,
        tool_name: String,
        arguments: JsonValue,
        status: ToolCallStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
        output_policy: ToolOutputPolicySnapshot,
        display: ToolDisplayPayload,
        storage: ToolStoragePayload,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recovery: Option<ToolRecoveryView>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        success: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        observation: Option<ToolObservation>,
    },
}

impl TurnItem {
    pub fn item_id(&self) -> &str {
        match self {
            Self::UserMessage { id, .. }
            | Self::AgentMessage { id, .. }
            | Self::Reasoning { id, .. }
            | Self::SystemEvent { id, .. }
            | Self::Task {
                item: TaskTurnItem { id, .. },
            }
            | Self::CommandExecution { id, .. }
            | Self::FileChange { id, .. }
            | Self::WebSearch { id, .. }
            | Self::WebFetch { id, .. }
            | Self::Download { id, .. }
            | Self::DynamicToolCall { id, .. } => id.as_str(),
        }
    }

    pub fn item_type(&self) -> TurnItemType {
        match self {
            Self::UserMessage { .. } => TurnItemType::UserMessage,
            Self::AgentMessage { .. } => TurnItemType::AgentMessage,
            Self::Reasoning { .. } => TurnItemType::Reasoning,
            Self::SystemEvent { .. } => TurnItemType::SystemEvent,
            Self::Task { .. } => TurnItemType::Task,
            Self::CommandExecution { .. } => TurnItemType::CommandExecution,
            Self::FileChange { .. } => TurnItemType::FileChange,
            Self::WebSearch { .. } => TurnItemType::WebSearch,
            Self::WebFetch { .. } => TurnItemType::WebFetch,
            Self::Download { .. } => TurnItemType::Download,
            Self::DynamicToolCall { .. } => TurnItemType::DynamicToolCall,
        }
    }

    pub fn is_tool_item(&self) -> bool {
        self.item_type().is_tool_item()
    }

    pub fn recovery_policy(&self) -> Option<&ToolRecoveryPolicySnapshot> {
        match self {
            Self::CommandExecution {
                recovery_policy, ..
            }
            | Self::FileChange {
                recovery_policy, ..
            }
            | Self::WebSearch {
                recovery_policy, ..
            }
            | Self::WebFetch {
                recovery_policy, ..
            }
            | Self::Download {
                recovery_policy, ..
            }
            | Self::DynamicToolCall {
                recovery_policy, ..
            } => recovery_policy.as_ref(),
            Self::UserMessage { .. }
            | Self::AgentMessage { .. }
            | Self::Reasoning { .. }
            | Self::SystemEvent { .. }
            | Self::Task { .. } => None,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnStartedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnCompletedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnFailedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemStartedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item: TurnItem,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemDeltaNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<ItemDeltaStream>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub markdown: Option<MarkdownDocument>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub markdown_version: Option<u16>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemCompletedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item: TurnItem,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemUpdatedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item: TurnItem,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemTimeoutDetectedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub attempt_number: u32,
    pub reason: TurnItemTimeoutReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_job_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoveryOpenedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoveryAttachedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub recovery_item_id: String,
    pub recovery_item_type: TurnItemType,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub existing_status: RecoveryJobStatus,
    pub next_attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRetryScheduledNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
    pub next_run_at_unix: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRetryAttemptStartedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoverySucceededNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemRecoveryExhaustedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub recovery_job_id: String,
    pub attempt_number: u32,
    pub status: RecoveryJobStatus,
    pub error_message: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemToolRetryScheduledNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub tool_retry_episode_id: String,
    pub tool_name: String,
    pub attempt_number: u32,
    pub error_class: ToolRetryErrorClass,
    pub retry_hint: String,
    #[serde(default)]
    pub budgets: Vec<ToolRetryBudgetUsage>,
    pub failure_signature_fingerprint: String,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemToolRetryResolvedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub tool_retry_episode_id: String,
    pub tool_name: String,
    pub attempt_number: u32,
    pub resolution: ToolRetryResolution,
    #[serde(default)]
    pub budgets: Vec<ToolRetryBudgetUsage>,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ItemToolRetryExhaustedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub tool_retry_episode_id: String,
    pub tool_name: String,
    pub attempt_number: u32,
    pub error_class: ToolRetryErrorClass,
    pub exhaustion_kind: ToolRetryExhaustionKind,
    #[serde(default)]
    pub budgets: Vec<ToolRetryBudgetUsage>,
    pub failure_signature_fingerprint: String,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnToolLoopBudgetExceededNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub limit_kind: ToolLoopBudgetLimitKind,
    pub limit: u32,
    pub observed: u32,
    pub action: ToolLoopBudgetAction,
    pub reason: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct TurnStatusChangedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub status: TurnStatus,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ContextCompressingNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub message: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
pub struct ContextCompressedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub compressed_tokens: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn turn_start_params_decode_text_input() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "input": [
                {
                    "type": "text",
                    "text": "hello"
                }
            ]
        }))
        .expect("params should decode");

        assert_eq!(params.thread_id, "thr_123");
        assert_eq!(params.turn_id, "turn_123");
        assert_eq!(params.input.len(), 1);
        assert!(matches!(
            params.input.first(),
            Some(UserInput::Text { text, .. }) if text == "hello"
        ));
    }

    #[test]
    fn turn_start_params_encode_user_input_tagged_enum() {
        let params = TurnStartParams {
            thread_id: "thr_123".to_owned(),
            turn_id: "turn_123".to_owned(),
            input: vec![UserInput::Image {
                url: "https://example.com/image.png".to_owned(),
            }],
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
        };

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "input": [
                    {
                        "type": "image",
                        "url": "https://example.com/image.png"
                    }
                ]
            })
        );
    }

    #[test]
    fn turn_cancel_params_roundtrip_optional_reason() {
        let params: TurnCancelParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "reason": "user clicked stop"
        }))
        .expect("params should decode");

        assert_eq!(params.thread_id, "thr_123");
        assert_eq!(params.turn_id, "turn_123");
        assert_eq!(params.reason.as_deref(), Some("user clicked stop"));

        let encoded = serde_json::to_value(TurnCancelParams {
            thread_id: "thr_123".to_owned(),
            turn_id: "turn_123".to_owned(),
            reason: None,
        })
        .expect("params should encode");
        assert_eq!(
            encoded,
            json!({
                "thread_id": "thr_123",
                "turn_id": "turn_123"
            })
        );
    }

    #[test]
    fn turn_start_params_encode_extended_attachment_input_variants() {
        let params = TurnStartParams {
            thread_id: "thr_123".to_owned(),
            turn_id: "turn_123".to_owned(),
            input: vec![
                UserInput::File {
                    url: "https://example.com/file.pdf".to_owned(),
                },
                UserInput::LocalFile {
                    path: "/tmp/file.pdf".to_owned(),
                },
                UserInput::Audio {
                    url: "https://example.com/sample.mp3".to_owned(),
                },
                UserInput::LocalAudio {
                    path: "/tmp/sample.wav".to_owned(),
                },
                UserInput::Video {
                    url: "https://example.com/clip.mp4".to_owned(),
                },
                UserInput::LocalVideo {
                    path: "/tmp/clip.mp4".to_owned(),
                },
            ],
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
        };

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({
                "thread_id": "thr_123",
                "turn_id": "turn_123",
                "input": [
                    { "type": "file", "url": "https://example.com/file.pdf" },
                    { "type": "localFile", "path": "/tmp/file.pdf" },
                    { "type": "audio", "url": "https://example.com/sample.mp3" },
                    { "type": "localAudio", "path": "/tmp/sample.wav" },
                    { "type": "video", "url": "https://example.com/clip.mp4" },
                    { "type": "localVideo", "path": "/tmp/clip.mp4" }
                ]
            })
        );
    }

    #[test]
    fn turn_start_params_round_trip_turn_id() {
        let params: TurnStartParams = serde_json::from_value(json!({
            "thread_id": "thr_123",
            "turn_id": "turn_abc"
        }))
        .expect("params should decode");
        assert_eq!(params.turn_id, "turn_abc");

        let encoded = serde_json::to_value(params).expect("params should encode");
        assert_eq!(
            encoded,
            json!({"thread_id": "thr_123", "turn_id": "turn_abc", "input": []})
        );
    }

    #[test]
    fn turn_start_params_require_turn_id() {
        let error = serde_json::from_value::<TurnStartParams>(json!({
            "thread_id": "thr_123",
            "input": []
        }))
        .expect_err("turn_id is required");
        assert!(error.to_string().contains("turn_id"));
    }

    #[test]
    fn turn_item_type_is_tool_item_matches_expected_variants() {
        assert!(TurnItemType::CommandExecution.is_tool_item());
        assert!(TurnItemType::DynamicToolCall.is_tool_item());
        assert!(!TurnItemType::AgentMessage.is_tool_item());
        assert!(!TurnItemType::SystemEvent.is_tool_item());
    }

    #[test]
    fn tool_recovery_policy_snapshot_uses_snake_case_enum_wire_values() {
        let snapshot = sample_snapshot();
        let value = serde_json::to_value(snapshot).expect("snapshot should serialize");

        assert_eq!(value["retryClass"], "network");
        assert_eq!(value["idempotencyMode"], "requires_key");
        assert_eq!(value["resolvedAction"], "retry_with_backoff");

        let decoded: ToolRecoveryPolicySnapshot =
            serde_json::from_value(value).expect("snapshot should deserialize");
        assert_eq!(decoded, sample_snapshot());
    }

    #[test]
    fn every_tool_turn_item_variant_carries_recovery_policy() {
        for item in sample_tool_items() {
            let value = serde_json::to_value(&item).expect("tool item should serialize");
            assert!(
                value.get("recoveryPolicy").is_some(),
                "missing recoveryPolicy in {value:?}"
            );
            assert!(
                value.get("outputPolicy").is_some(),
                "missing outputPolicy in {value:?}"
            );
            assert!(
                value.get("outputJson").is_none(),
                "tool item must not expose outputJson in {value:?}"
            );
            let decoded: TurnItem = serde_json::from_value(value).expect("tool item should decode");
            assert_eq!(decoded.recovery_policy(), Some(&sample_snapshot()));
        }
    }

    #[test]
    fn tool_output_policy_snapshot_round_trips() {
        let snapshot = ToolOutputPolicySnapshot::for_tool_name("web_fetch");
        let value = serde_json::to_value(&snapshot).expect("policy should serialize");
        assert_eq!(value["storage"]["mode"], "metadata_only");
        assert_eq!(value["deltas"]["mode"], "progress_only");

        let decoded: ToolOutputPolicySnapshot =
            serde_json::from_value(value).expect("policy should deserialize");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn tool_display_and_storage_payloads_round_trip() {
        let display = ToolDisplayPayload::Summary(ToolOutputSummary {
            title: "Read crates/tools/src/runtime.rs".to_owned(),
            lines: vec!["Read lines 240-310".to_owned(), "71 lines".to_owned()],
            metadata: ToolMetadata::from_json(json!({
                "path": "crates/tools/src/runtime.rs",
                "lineStart": 240,
                "lineEnd": 310,
                "bytes": 3890
            })),
            truncated: false,
        });
        let storage = ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(json!({
                "path": "crates/tools/src/runtime.rs",
                "contentHash": "sha256:test"
            })),
        };
        let recovery = ToolRecoveryView {
            error_class: Some("timeout".to_owned()),
            retry_hint: Some("retry".to_owned()),
            incomplete_reason: Some("tool timed out".to_owned()),
            diagnostic_summary: Some("stderr contained timeout".to_owned()),
            diagnostic_excerpt: Some("deadline exceeded".to_owned()),
            output_fingerprint: Some("sha256:output".to_owned()),
            content_fingerprint: Some("sha256:content".to_owned()),
            was_truncated: true,
            continuation: Some(json!({"sessionId": 7})),
        };

        let display_value = serde_json::to_value(&display).expect("display should serialize");
        let storage_value = serde_json::to_value(&storage).expect("storage should serialize");
        let recovery_value = serde_json::to_value(&recovery).expect("recovery should serialize");
        assert_eq!(
            serde_json::from_value::<ToolDisplayPayload>(display_value)
                .expect("display should deserialize"),
            display
        );
        assert_eq!(
            serde_json::from_value::<ToolStoragePayload>(storage_value)
                .expect("storage should deserialize"),
            storage
        );
        assert_eq!(
            serde_json::from_value::<ToolRecoveryView>(recovery_value)
                .expect("recovery should deserialize"),
            recovery
        );
    }

    #[test]
    fn tool_metadata_redacts_raw_like_fields_by_construction() {
        let metadata = ToolMetadata::from_json(json!({
            "url": "https://example.com",
            "body": "SECRET_BODY",
            "nested": {
                "base64": "SECRET_BLOB"
            }
        }));

        assert_eq!(
            metadata.get("url").and_then(ToolMetadataValue::as_str),
            Some("https://example.com")
        );
        assert!(matches!(
            metadata.get("body"),
            Some(ToolMetadataValue::RedactedRaw {
                raw_kind: ToolMetadataRawKind::Body,
                ..
            })
        ));
        let serialized = serde_json::to_string(&metadata).expect("metadata should serialize");
        assert!(!serialized.contains("SECRET_BODY"));
        assert!(!serialized.contains("SECRET_BLOB"));
    }

    #[test]
    fn tool_metadata_to_json_preserves_integer_precision() {
        let value = "9007199254740993";
        let metadata_value = ToolMetadataValue::Number {
            value: value.to_owned(),
        };

        assert_eq!(metadata_value.to_json(), json!(9007199254740993_u64));
    }

    #[test]
    fn generated_schema_documents_cover_typed_tool_output_contract() {
        let documents = crate::protocol_schema_documents();
        let schema_names = documents
            .iter()
            .map(|document| document.file_name)
            .collect::<Vec<_>>();

        for expected in [
            "tool_output_policy_snapshot.json",
            "tool_metadata.json",
            "tool_metadata_value.json",
            "tool_metadata_raw_kind.json",
            "tool_output_summary.json",
            "tool_display_payload.json",
            "tool_storage_payload.json",
            "tool_recovery_view.json",
        ] {
            assert!(
                schema_names.iter().any(|name| *name == expected),
                "missing schema document {expected}"
            );
        }

        let turn_item_schema = documents
            .iter()
            .find(|document| document.file_name == "turn_item.json")
            .expect("turn_item schema should be exported");
        let schema_json = serde_json::to_string(&turn_item_schema.schema)
            .expect("turn_item schema should serialize");
        for expected_field in ["outputPolicy", "display", "storage", "recovery"] {
            assert!(
                schema_json.contains(expected_field),
                "turn_item schema should include {expected_field}"
            );
        }
        assert!(
            !schema_json.contains("outputJson") && !schema_json.contains("output_json"),
            "turn_item schema must not expose generic raw output_json"
        );
    }

    #[test]
    fn non_tool_turn_items_do_not_require_recovery_policy() {
        let item = TurnItem::AgentMessage {
            id: "agent_1".to_owned(),
            text: "done".to_owned(),
            markdown: None,
            markdown_version: None,
        };

        let value = serde_json::to_value(&item).expect("agent item should serialize");
        assert!(value.get("recoveryPolicy").is_none());
        let decoded: TurnItem = serde_json::from_value(value).expect("agent item should decode");
        assert_eq!(decoded.recovery_policy(), None);
    }

    fn sample_snapshot() -> ToolRecoveryPolicySnapshot {
        ToolRecoveryPolicySnapshot {
            retry_class: ToolRecoveryRetryClass::Network,
            idempotency_mode: ToolRecoveryIdempotencyMode::RequiresKey,
            max_attempts: 4,
            can_resume: true,
            resolved_action: RecoveryAction::RetryWithBackoff,
            base_backoff_secs: 3,
            max_wall_clock_secs: 240,
            no_progress_limit: 3,
        }
    }

    fn sample_tool_items() -> Vec<TurnItem> {
        let recovery_policy = Some(sample_snapshot());
        let recovery = Some(ToolRecoveryView {
            error_class: None,
            retry_hint: None,
            incomplete_reason: None,
            diagnostic_summary: Some("sample".to_owned()),
            diagnostic_excerpt: None,
            output_fingerprint: None,
            content_fingerprint: None,
            was_truncated: false,
            continuation: None,
        });
        vec![
            TurnItem::CommandExecution {
                id: "cmd_1".to_owned(),
                tool_name: "exec_command".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                command: Vec::new(),
                cwd: None,
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::FileChange {
                id: "file_1".to_owned(),
                tool_name: "apply_patch".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("apply_patch"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                changed_files: Vec::new(),
                exit_code: None,
                stdout: None,
                stderr: None,
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::WebSearch {
                id: "search_1".to_owned(),
                tool_name: "web_search".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("web_search"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                query: None,
                provider: None,
                took_ms: None,
                result_count: None,
                results: Vec::new(),
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::WebFetch {
                id: "fetch_1".to_owned(),
                tool_name: "web_fetch".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                url: None,
                final_url: None,
                status_code: None,
                content_type: None,
                extract_mode: None,
                resolved_mode: None,
                bytes_received: None,
                elapsed_ms: None,
                truncated: None,
                title: None,
                word_count: None,
                links: Vec::new(),
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::Download {
                id: "download_1".to_owned(),
                tool_name: "download".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy: recovery_policy.clone(),
                output_policy: ToolOutputPolicySnapshot::for_tool_name("download"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery: recovery.clone(),
                url: None,
                final_url: None,
                status_code: None,
                path: None,
                bytes_written: None,
                sha256: None,
                content_type: None,
                elapsed_ms: None,
                truncated: None,
                success: None,
                outcome: None,
                observation: None,
            },
            TurnItem::DynamicToolCall {
                id: "dynamic_1".to_owned(),
                tool_name: "dynamic".to_owned(),
                arguments: json!({}),
                status: ToolCallStatus::InProgress,
                recovery_policy,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic"),
                display: ToolDisplayPayload::default(),
                storage: ToolStoragePayload::default(),
                recovery,
                success: None,
                outcome: None,
                observation: None,
            },
        ]
    }
}
