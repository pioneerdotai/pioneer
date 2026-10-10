use pioneer_protocol::ProviderFailureClass;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt::{Display, Formatter};
use std::time::Duration;

pub use pioneer_protocol::ReasoningEffort;

/// Optional provider-adapter classification for a concrete transport error.
///
/// Adapters only need to return this when they can map their own structured
/// error to Pioneer semantics. Unknown errors deliberately fall back to the
/// conservative provider-neutral classifier in the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderFailureClassification {
    /// Retains the registry's network fallback after the unsafe source is dropped.
    /// Internal transport fact, never serialized into public diagnostics.
    pub is_network_error: bool,
    pub error_reason: Option<pioneer_protocol::ProviderErrorReason>,
    pub request_id: Option<pioneer_protocol::ProviderRequestId>,
    pub class: ProviderFailureClass,
    pub http_status: Option<u16>,
    pub provider_code: Option<String>,
    pub retry_after_ms: Option<u64>,
}

impl ProviderFailureClassification {
    pub fn new(class: ProviderFailureClass) -> Self {
        Self {
            is_network_error: false,
            error_reason: None,
            request_id: None,
            class,
            http_status: None,
            provider_code: None,
            retry_after_ms: None,
        }
    }
}

/// Request-owned diagnostic facts, independent of payload chunks and progress.
/// Each streaming request gets a fresh handle; providers must never reuse it.
#[derive(Clone, Debug, Default)]
pub struct ProviderStreamDiagnostics {
    request_id: std::sync::Arc<std::sync::RwLock<Option<pioneer_protocol::ProviderRequestId>>>,
}

impl ProviderStreamDiagnostics {
    pub fn request_id(&self) -> Option<pioneer_protocol::ProviderRequestId> {
        self.request_id
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub(crate) fn set_request_id(&self, id: pioneer_protocol::ProviderRequestId) {
        *self
            .request_id
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(id);
    }
}

/// A provider stream and its request-owned facts, consumed by agent deadlines.
/// Facts do not produce chunks or reset the first/inter-chunk deadline.
pub struct ProviderStream {
    pub stream: futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>,
    pub diagnostics: ProviderStreamDiagnostics,
}

impl ProviderStream {
    pub fn new(
        stream: futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>,
    ) -> Self {
        Self {
            stream,
            diagnostics: ProviderStreamDiagnostics::default(),
        }
    }
}

/// A provider-controlled response exceeded a hard runtime boundary.
///
/// This error deliberately contains only bounded metadata.  It is used for
/// both transport bodies and the logical response envelope retained by the
/// agent, so callers can classify it as non-retryable without inspecting a
/// provider-supplied body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponseTooLarge {
    pub component: &'static str,
    pub limit: usize,
    pub observed: usize,
}

impl ProviderResponseTooLarge {
    pub const fn new(component: &'static str, limit: usize, observed: usize) -> Self {
        Self {
            component,
            limit,
            observed,
        }
    }
}

impl Display for ProviderResponseTooLarge {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "provider response exceeds {} limit (limit={} bytes, observed_at_least={} bytes)",
            self.component, self.limit, self.observed
        )
    }
}

impl std::error::Error for ProviderResponseTooLarge {}

/// An oversized HTTP error body. The status is retained even though the
/// untrusted body is discarded, so retry/cooldown policy can still distinguish
/// rate limits and provider failures from oversized successful responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderHttpErrorBodyTooLarge {
    pub status: u16,
    pub limit: ProviderResponseTooLarge,
}

impl Display for ProviderHttpErrorBodyTooLarge {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "provider HTTP error {}: {}",
            self.status, self.limit
        )
    }
}

impl std::error::Error for ProviderHttpErrorBodyTooLarge {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputContentType {
    Text,
    File,
    Image,
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InputTypeSupport {
    #[serde(default)]
    pub native: bool,
    #[serde(default)]
    pub file_upload: bool,
    #[serde(default)]
    pub data_url_inline: bool,
    #[serde(default)]
    pub text_fallback: bool,
}

impl InputTypeSupport {
    pub const fn disabled() -> Self {
        Self {
            native: false,
            file_upload: false,
            data_url_inline: false,
            text_fallback: false,
        }
    }

    pub const fn fallback_only() -> Self {
        Self {
            native: false,
            file_upload: false,
            data_url_inline: false,
            text_fallback: true,
        }
    }

    pub const fn native_inline_only() -> Self {
        Self {
            native: true,
            file_upload: false,
            data_url_inline: false,
            text_fallback: false,
        }
    }

    pub const fn file_upload_only() -> Self {
        Self {
            native: false,
            file_upload: true,
            data_url_inline: false,
            text_fallback: false,
        }
    }

    pub const fn data_url_inline_only() -> Self {
        Self {
            native: false,
            file_upload: false,
            data_url_inline: true,
            text_fallback: false,
        }
    }

    pub const fn is_supported(self) -> bool {
        self.native || self.file_upload || self.data_url_inline || self.text_fallback
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderInputCapabilities {
    #[serde(default = "default_text_input_capability")]
    pub text: bool,
    #[serde(default)]
    pub file: InputTypeSupport,
    #[serde(default)]
    pub image: InputTypeSupport,
    #[serde(default)]
    pub audio: InputTypeSupport,
    #[serde(default)]
    pub video: InputTypeSupport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderTimeoutPolicy {
    pub connect_timeout: Duration,
    pub first_chunk_timeout: Duration,
    pub inter_chunk_idle_timeout: Duration,
    pub non_stream_request_timeout: Duration,
    pub max_stream_duration: Option<Duration>,
}

impl ProviderTimeoutPolicy {
    pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;
    pub const DEFAULT_FIRST_CHUNK_TIMEOUT_SECS: u64 = 180;
    pub const DEFAULT_INTER_CHUNK_IDLE_TIMEOUT_SECS: u64 = 180;
    pub const DEFAULT_NON_STREAM_REQUEST_TIMEOUT_SECS: u64 = 120;
    /// Safety cap for one stream when no workload override was configured.
    /// This is independent of the Turn's soft checkpoint threshold/lifetime.
    pub const DEFAULT_MAX_STREAM_DURATION_SECS: u64 = 30 * 60;

    pub fn stream_attempt_duration(self) -> Duration {
        self.max_stream_duration
            .unwrap_or(Duration::from_secs(Self::DEFAULT_MAX_STREAM_DURATION_SECS))
            .max(Duration::from_millis(1))
    }

    pub fn from_secs(
        connect_timeout_secs: u64,
        first_chunk_timeout_secs: u64,
        inter_chunk_idle_timeout_secs: u64,
        non_stream_request_timeout_secs: u64,
        max_stream_duration_secs: Option<u64>,
    ) -> Self {
        Self {
            connect_timeout: Duration::from_secs(connect_timeout_secs.max(1)),
            first_chunk_timeout: Duration::from_secs(first_chunk_timeout_secs.max(1)),
            inter_chunk_idle_timeout: Duration::from_secs(inter_chunk_idle_timeout_secs.max(1)),
            non_stream_request_timeout: Duration::from_secs(non_stream_request_timeout_secs.max(1)),
            max_stream_duration: max_stream_duration_secs
                .map(|secs| Duration::from_secs(secs.max(1))),
        }
    }
}

/// In-memory ownership for one physical request. Every transport stage spends
/// the same monotonic allowance; none may create a new first-output budget.
#[derive(Clone, Debug)]
pub struct ProviderAttemptContext {
    pub first_output_deadline: tokio::time::Instant,
    pub deadline: tokio::time::Instant,
    pub cancellation: tokio_util::sync::CancellationToken,
}

impl ProviderAttemptContext {
    pub fn new(policy: ProviderTimeoutPolicy, streaming: bool) -> anyhow::Result<Self> {
        Self::from_started(policy, streaming, tokio::time::Instant::now())
    }

    pub fn from_started(
        policy: ProviderTimeoutPolicy,
        streaming: bool,
        started: tokio::time::Instant,
    ) -> anyhow::Result<Self> {
        let cap = if streaming {
            policy.stream_attempt_duration()
        } else {
            policy.non_stream_request_timeout
        }
        .max(Duration::from_millis(1));
        let deadline = started
            .checked_add(cap)
            .ok_or_else(|| anyhow::anyhow!("provider attempt timeout policy is out of range"))?;
        Ok(Self {
            first_output_deadline: started + policy.first_chunk_timeout.min(cap),
            deadline,
            cancellation: tokio_util::sync::CancellationToken::new(),
        })
    }
}

impl Default for ProviderTimeoutPolicy {
    fn default() -> Self {
        Self::from_secs(
            Self::DEFAULT_CONNECT_TIMEOUT_SECS,
            Self::DEFAULT_FIRST_CHUNK_TIMEOUT_SECS,
            Self::DEFAULT_INTER_CHUNK_IDLE_TIMEOUT_SECS,
            Self::DEFAULT_NON_STREAM_REQUEST_TIMEOUT_SECS,
            None,
        )
    }
}

impl ProviderInputCapabilities {
    pub fn disabled_for_all_file_types() -> Self {
        Self {
            text: true,
            file: InputTypeSupport::disabled(),
            image: InputTypeSupport::disabled(),
            audio: InputTypeSupport::disabled(),
            video: InputTypeSupport::disabled(),
        }
    }

    pub fn fallback_for_all_file_types() -> Self {
        Self {
            text: true,
            file: InputTypeSupport::fallback_only(),
            image: InputTypeSupport::fallback_only(),
            audio: InputTypeSupport::fallback_only(),
            video: InputTypeSupport::fallback_only(),
        }
    }

    pub fn support_for(&self, kind: InputContentType) -> InputTypeSupport {
        match kind {
            InputContentType::Text => InputTypeSupport {
                native: self.text,
                file_upload: false,
                data_url_inline: false,
                text_fallback: self.text,
            },
            InputContentType::File => self.file,
            InputContentType::Image => self.image,
            InputContentType::Audio => self.audio,
            InputContentType::Video => self.video,
        }
    }
}

impl Default for ProviderInputCapabilities {
    fn default() -> Self {
        Self {
            text: true,
            file: InputTypeSupport::default(),
            image: InputTypeSupport::default(),
            audio: InputTypeSupport::default(),
            video: InputTypeSupport::default(),
        }
    }
}

const fn default_text_input_capability() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum AttachmentDataSource {
    Bytes { base64_data: String },
    Path { path: String },
    Url { url: String },
    Reference { reference: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentArtifactContext {
    pub workspace_id: String,
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAttachment {
    pub mime_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    pub source: AttachmentDataSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<AttachmentArtifactContext>,
}

impl MessageAttachment {
    pub fn from_path(path: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Self {
            mime_type: mime_type.into(),
            name: None,
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Path { path: path.into() },
            artifact: None,
        }
    }

    pub fn from_url(url: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Self {
            mime_type: mime_type.into(),
            name: None,
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Url { url: url.into() },
            artifact: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageContentPart {
    Text { text: String },
    File { file: MessageAttachment },
    Image { image: MessageAttachment },
    Audio { audio: MessageAttachment },
    Video { video: MessageAttachment },
}

impl MessageContentPart {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn image(attachment: MessageAttachment) -> Self {
        Self::Image { image: attachment }
    }

    pub fn file(attachment: MessageAttachment) -> Self {
        Self::File { file: attachment }
    }

    pub fn audio(attachment: MessageAttachment) -> Self {
        Self::Audio { audio: attachment }
    }

    pub fn video(attachment: MessageAttachment) -> Self {
        Self::Video { video: attachment }
    }

    pub fn kind(&self) -> InputContentType {
        match self {
            Self::Text { .. } => InputContentType::Text,
            Self::File { .. } => InputContentType::File,
            Self::Image { .. } => InputContentType::Image,
            Self::Audio { .. } => InputContentType::Audio,
            Self::Video { .. } => InputContentType::Video,
        }
    }

    pub fn text_value(&self) -> Option<&str> {
        match self {
            Self::Text { text } => Some(text.as_str()),
            _ => None,
        }
    }
}

/// Provider-owned state that must be replayed unchanged only to its proven
/// provider/model target. The payload remains provider-owned; the history
/// projection may extract explicitly typed human-readable fields when a
/// completed round is moved to an incompatible target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderReplayState {
    pub provider: String,
    /// Exact model whose continuation contract produced this state. Legacy
    /// records omit it and are therefore portable history, not proof that an
    /// active continuation can be replayed to an arbitrary model exposed by
    /// the same provider.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub payload: JsonValue,
}

impl ProviderReplayState {
    pub fn new(provider: impl Into<String>, payload: JsonValue) -> Self {
        Self {
            provider: provider.into(),
            model: None,
            payload,
        }
    }

    pub fn for_model(
        provider: impl Into<String>,
        model: impl Into<String>,
        payload: JsonValue,
    ) -> Self {
        Self {
            provider: provider.into(),
            model: Some(model.into()),
            payload,
        }
    }

    pub fn is_compatible_with(&self, provider: &str, model: &str) -> bool {
        self.provider == provider && self.model.as_deref() == Some(model)
    }

    pub fn payload_for(&self, provider: &str) -> Option<&JsonValue> {
        (self.provider == provider).then_some(&self.payload)
    }
}

/// Runtime-only provenance. Provider wire serializers never emit this field,
/// and deserializing a provider response cannot manufacture trusted sources.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageSourceRef {
    pub scope: String,
    pub id: String,
    pub version: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageSourceAlias {
    pub represented_thread_id: String,
    pub represented_source: MessageSourceRef,
    pub thread_id: String,
    pub source: MessageSourceRef,
}
impl MessageSourceAlias {
    /// Exact canonical response -> model-facing UI copy. Never a turn-wide alias.
    pub fn is_response_copy(&self) -> bool {
        self.thread_id == self.represented_thread_id
            && self
                .represented_source
                .scope
                .strip_prefix("context:")
                .is_some_and(|turn| {
                    !turn.is_empty() && self.source.scope.strip_prefix("event:") == Some(turn)
                })
            && !self.source.version.is_empty()
            && !self.represented_source.version.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageSourceIdentity {
    pub thread_id: String,
    pub source: MessageSourceRef,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageProvenance {
    /// Exact Task command turn for a delivered outcome in the same source
    /// thread. Set by trusted canonical projection, never inferred from text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_turn_id: Option<String>,
    pub workspace_id: String,
    /// Canonical storage scope. Adopting work never moves its source records.
    pub thread_id: String,
    /// Explicit execution-context ownership assigned only by the accepted
    /// Gateway composition. None retains ownership in the canonical thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_thread: Option<String>,
    pub unit_id: String,
    pub sources: Vec<MessageSourceRef>,
    /// Exact canonical inputs represented by this message without contributing
    /// another provider-wire payload. Trusted Gateway metadata is the only
    /// authority allowed to establish these aliases.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_aliases: Vec<MessageSourceAlias>,
    /// Exact copies whose competing historical owners make suppression unsafe.
    /// This is evidence only, never a represented source or access grant.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ambiguous_input_aliases: Vec<MessageSourceIdentity>,
    pub complete: bool,
    pub protected_input: bool,
    pub inherited: bool,
}

/// Fixed-size canonical result fact. Missing/unknown version or status is Unknown,
/// never inferred from model-visible content. Owned by the tool execution boundary.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    Ok,
    RecoverableError,
    FatalError,
    PartialSuccess,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolResultOutcome {
    pub schema_version: u8,
    pub status: ToolResultStatus,
}

impl ToolResultOutcome {
    pub fn new(status: ToolResultStatus) -> Self {
        Self {
            schema_version: 1,
            status,
        }
    }
    pub fn is_error(self) -> Option<bool> {
        if self.schema_version != 1 {
            return None;
        }
        match self.status {
            ToolResultStatus::Ok => Some(false),
            ToolResultStatus::RecoverableError
            | ToolResultStatus::FatalError
            | ToolResultStatus::PartialSuccess => Some(true),
            ToolResultStatus::Unknown => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatMessage {
    #[serde(skip)]
    pub provenance: Option<MessageProvenance>,
    pub role: Role,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_parts: Vec<MessageContentPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ProviderToolCall>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_replay_state: Option<ProviderReplayState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_outcome: Option<ToolResultOutcome>,
}

impl ChatMessage {
    /// Safe content for APIs without an outcome flag. The typed envelope remains
    /// authoritative; preserve stored full content and never parse it for status.
    pub(crate) fn wire_content_is_empty(&self) -> bool {
        self.content.is_empty()
            && !(self.role == Role::Tool
                && self
                    .tool_outcome
                    .is_some_and(|outcome| outcome.is_error() == Some(true)))
    }

    pub(crate) fn wire_content(&self) -> String {
        if self.role == Role::Tool
            && let Some(outcome) = self.tool_outcome
            && outcome.is_error() == Some(true)
        {
            let label = match outcome.status {
                ToolResultStatus::RecoverableError => "recoverable_error",
                ToolResultStatus::FatalError => "fatal_error",
                ToolResultStatus::PartialSuccess => "partial_success",
                _ => return self.content.clone(),
            };
            return format!("Tool outcome: {label}.\n{}", self.content);
        }
        self.content.clone()
    }
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            provenance: None,
            role: Role::System,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            provider_replay_state: None,
            tool_outcome: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            provenance: None,
            role: Role::User,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            provider_replay_state: None,
            tool_outcome: None,
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            provenance: None,
            role: Role::Assistant,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            provider_replay_state: None,
            tool_outcome: None,
        }
    }

    pub fn tool(content: impl Into<String>) -> Self {
        Self {
            provenance: None,
            role: Role::Tool,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            provider_replay_state: None,
            tool_outcome: None,
        }
    }

    pub fn assistant_tool_calls(
        content: Option<impl Into<String>>,
        tool_calls: Vec<ProviderToolCall>,
    ) -> Self {
        Self::assistant_tool_calls_with_reasoning(content, None::<String>, tool_calls)
    }

    pub fn assistant_tool_calls_with_reasoning(
        content: Option<impl Into<String>>,
        reasoning_content: Option<impl Into<String>>,
        tool_calls: Vec<ProviderToolCall>,
    ) -> Self {
        Self::assistant_tool_calls_with_provider_state(content, reasoning_content, tool_calls, None)
    }

    pub fn assistant_tool_calls_with_provider_state(
        content: Option<impl Into<String>>,
        reasoning_content: Option<impl Into<String>>,
        tool_calls: Vec<ProviderToolCall>,
        provider_replay_state: Option<ProviderReplayState>,
    ) -> Self {
        Self {
            provenance: None,
            role: Role::Assistant,
            content: content.map(Into::into).unwrap_or_default(),
            reasoning_content: reasoning_content.map(Into::into),
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: Some(tool_calls),
            provider_replay_state,
            tool_outcome: None,
        }
    }

    pub fn tool_result(
        tool_call_id: impl Into<String>,
        name: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            provenance: None,
            role: Role::Tool,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: Some(tool_call_id.into()),
            name: Some(name.into()),
            tool_calls: None,
            provider_replay_state: None,
            tool_outcome: None,
        }
    }

    pub fn user_parts(parts: Vec<MessageContentPart>) -> Self {
        Self {
            provenance: None,
            role: Role::User,
            content: String::new(),
            reasoning_content: None,
            content_parts: parts,
            tool_call_id: None,
            name: None,
            tool_calls: None,
            provider_replay_state: None,
            tool_outcome: None,
        }
    }

    pub fn has_attachments(&self) -> bool {
        self.content_parts
            .iter()
            .any(|part| part.kind() != InputContentType::Text)
    }

    pub fn text_content_lossy(&self) -> String {
        let mut chunks = Vec::new();
        if !self.content.trim().is_empty() {
            chunks.push(self.content.trim().to_owned());
        }
        for part in &self.content_parts {
            if let Some(text) = part.text_value() {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    chunks.push(trimmed.to_owned());
                }
            }
        }
        chunks.join("\n\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelInputItem {
    Message {
        message: ChatMessage,
    },
    ToolResult {
        tool_call_id: String,
        name: String,
        content: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<JsonValue>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_outcome: Option<ToolResultOutcome>,
    },
}

impl ModelInputItem {
    pub fn message(message: ChatMessage) -> Self {
        Self::Message { message }
    }

    pub fn tool_result(
        tool_call_id: impl Into<String>,
        name: impl Into<String>,
        content: impl Into<String>,
        payload: Option<JsonValue>,
    ) -> Self {
        Self::ToolResult {
            tool_call_id: tool_call_id.into(),
            name: name.into(),
            content: content.into(),
            payload,
            tool_outcome: None,
        }
    }

    pub fn with_tool_outcome(mut self, outcome: ToolResultOutcome) -> Self {
        match &mut self {
            Self::Message { message } => message.tool_outcome = Some(outcome),
            Self::ToolResult { tool_outcome, .. } => *tool_outcome = Some(outcome),
        }
        self
    }

    pub fn into_chat_message(self) -> ChatMessage {
        match self {
            Self::Message { message } => message,
            Self::ToolResult {
                tool_call_id,
                name,
                content,
                payload,
                tool_outcome,
            } => {
                let content = payload
                    .map(|payload| canonical_json_string(&payload))
                    .unwrap_or(content);
                let mut message = ChatMessage::tool_result(tool_call_id, name, content);
                message.tool_outcome = tool_outcome;
                message
            }
        }
    }
}

fn canonical_json_string(value: &JsonValue) -> String {
    serde_json::to_string(&canonicalize_json(value)).unwrap_or_else(|_| value.to_string())
}

fn canonicalize_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();

            let mut ordered = serde_json::Map::new();
            for key in keys {
                if let Some(entry) = map.get(key.as_str()) {
                    ordered.insert(key, canonicalize_json(entry));
                }
            }
            JsonValue::Object(ordered)
        }
        JsonValue::Array(items) => {
            JsonValue::Array(items.iter().map(canonicalize_json).collect::<Vec<_>>())
        }
        _ => value.clone(),
    }
}

impl From<ChatMessage> for ModelInputItem {
    fn from(message: ChatMessage) -> Self {
        Self::Message { message }
    }
}

impl From<ModelInputItem> for ChatMessage {
    fn from(item: ModelInputItem) -> Self {
        item.into_chat_message()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledPromptPayload {
    pub stable_system_text: String,
    pub dynamic_system_text: String,
    pub boundary_marker: String,
    pub full_system_text: String,
}

impl CompiledPromptPayload {
    pub fn stable_system_text(&self) -> Option<String> {
        let trimmed = self.stable_system_text.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    }

    pub fn dynamic_system_text(&self) -> Option<String> {
        let trimmed = self.dynamic_system_text.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    }

    pub fn full_system_text(&self) -> Option<String> {
        let trimmed = self.full_system_text.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    }

    pub fn system_sections(&self) -> Vec<String> {
        let mut sections = Vec::new();

        if let Some(stable) = self.stable_system_text() {
            sections.push(stable);
        }

        if let Some(dynamic) = self.dynamic_system_text() {
            sections.push(dynamic);
        }

        if sections.is_empty()
            && let Some(full) = self.full_system_text()
        {
            sections.push(full);
        }

        sections
    }
}

#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub temperature: Option<f32>,
    /// Prepared generation reserve. Adapters must preserve this bound in their
    /// native field. For inclusive APIs it covers thinking + visible output;
    /// visible-only APIs need a separate prepared thinking reserve or an
    /// explicit restriction. Qualitative effort never invents token budgets.
    pub max_tokens: Option<u32>,
    pub tools: Option<Vec<ToolDefinition>>,
    pub tool_choice: Option<ToolChoice>,
    /// true permits multiple calls (it does not require them); false requires
    /// at most one call per response and must be enforced or rejected locally.
    pub parallel_tool_calls: Option<bool>,
    pub reasoning: Option<ReasoningConfig>,
    pub compiled_prompt: Option<CompiledPromptPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingRequest {
    pub model: String,
    pub input: Vec<String>,
}

impl EmbeddingRequest {
    pub fn new(model: impl Into<String>, input: Vec<String>) -> Self {
        Self {
            model: model.into(),
            input,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub usage: Option<TokenUsage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningConfig {
    /// Explicit off; differs from an omitted setting (server default).
    Disabled,
    /// Includes explicit `None`, serialized as the protocol's off control.
    Effort(ReasoningEffort),
}

impl ReasoningConfig {
    pub const fn disabled() -> Self {
        Self::Disabled
    }

    pub const fn effort(effort: ReasoningEffort) -> Self {
        Self::Effort(effort)
    }
}

impl ChatRequest {
    /// Deterministic names and typed failure text for context admission. This
    /// view must not be persisted or used as the tool router's canonical input.
    pub fn tool_wire_budget_view(&self) -> anyhow::Result<Self> {
        let manifest = crate::tools::wire::ToolWireManifest::new("_budget", self)?;
        let mut view = self.clone();
        manifest.project(&mut view)?;
        for message in &mut view.messages {
            if message.role == Role::Tool {
                message.content = message.wire_content();
            }
        }
        Ok(view)
    }
    pub fn non_system_messages(&self) -> Vec<ChatMessage> {
        self.messages
            .iter()
            .filter(|message| message.role != Role::System)
            .cloned()
            .collect::<Vec<_>>()
    }

    pub fn rendered_messages_with_compiled_prompt(&self) -> Vec<ChatMessage> {
        let Some(compiled_prompt) = self.compiled_prompt.as_ref() else {
            return self.messages.clone();
        };

        let mut rendered = Vec::new();

        if let Some(full_system_text) = compiled_prompt.full_system_text() {
            rendered.push(ChatMessage::system(full_system_text));
        }

        rendered.extend(self.non_system_messages());

        rendered
    }

    pub fn rendered_messages_with_compiled_sections(&self) -> Vec<ChatMessage> {
        let Some(compiled_prompt) = self.compiled_prompt.as_ref() else {
            return self.messages.clone();
        };

        let mut rendered = compiled_prompt
            .system_sections()
            .into_iter()
            .map(ChatMessage::system)
            .collect::<Vec<_>>();

        rendered.extend(self.non_system_messages());

        rendered
    }

    pub fn compiled_prompt_sections(&self) -> Option<Vec<String>> {
        self.compiled_prompt
            .as_ref()
            .map(CompiledPromptPayload::system_sections)
            .filter(|sections| !sections.is_empty())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    Tool { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCallIdentity {
    pub provider_call_id: String,
    pub turn_item_id: String,
    pub ordinal: u32,
}

/// Durable provider-facing assistant round plus the separate Pioneer domain
/// identity of every requested operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalProviderRoundEnvelope {
    pub version: u8,
    pub round_id: String,
    pub termination: ProviderTermination,
    pub message: ChatMessage,
    pub calls: Vec<ProviderCallIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "raw", rename_all = "snake_case")]
pub enum ProviderTermination {
    Complete,
    ToolCalls,
    Length,
    ContentFiltered,
    Safety,
    Cancelled,
    ProviderError,
    Unknown(String),
}

impl ProviderTermination {
    pub fn from_openai_reason(reason: &str) -> Self {
        match reason.trim().to_ascii_lowercase().as_str() {
            "stop" | "end_turn" | "stop_sequence" => Self::Complete,
            "tool_calls" | "function_call" | "tool_use" => Self::ToolCalls,
            "length" | "max_tokens" | "max_output_tokens" => Self::Length,
            "content_filter" | "content_filtered" | "recitation" => Self::ContentFiltered,
            "safety"
            | "blocked"
            | "blocklist"
            | "prohibited_content"
            | "spii"
            | "image_safety"
            | "refusal"
            | "guardrail_intervened" => Self::Safety,
            "cancelled" | "canceled" => Self::Cancelled,
            "error" | "provider_error" => Self::ProviderError,
            other => Self::Unknown(other.chars().take(64).collect()),
        }
    }

    pub fn permits_success(&self) -> bool {
        matches!(self, Self::Complete | Self::ToolCalls)
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(default)]
pub struct TokenUsage {
    /// Effective input including cache reads/writes once. Missing is not zero.
    pub input_tokens: Option<u64>,
    /// Output including reasoning once, according to the API category rules.
    pub output_tokens: Option<u64>,
    pub uncached_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_write_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub reported_total_tokens: Option<u64>,
    pub semantics: Option<String>,
    pub raw_usage: Option<serde_json::Value>,
    pub generation_id: Option<String>,
    pub reported_model: Option<String>,
    pub service_tier: Option<String>,
    pub request_id: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub api: Option<String>,
    pub api_version: Option<String>,
    pub route: Option<String>,
    pub physical_attempt_id: Option<String>,
    pub accounting: Option<serde_json::Value>,
}

impl TokenUsage {
    /// Stream fields are cumulative snapshots. Duplicate events replace values;
    /// partial native deltas preserve previously reported categories.
    pub fn update(&mut self, snapshot: &Self) {
        macro_rules! update {
            ($($field:ident),*) => {$(if snapshot.$field.is_some() {
                self.$field = snapshot.$field.clone();
            })*};
        }
        update!(
            input_tokens,
            output_tokens,
            uncached_input_tokens,
            cache_read_input_tokens,
            cache_write_input_tokens,
            reasoning_tokens,
            reported_total_tokens,
            reported_model,
            service_tier,
            semantics,
            generation_id,
            request_id,
            provider,
            model,
            api,
            api_version,
            route,
            physical_attempt_id,
            accounting
        );
        if let Some(raw) = &snapshot.raw_usage {
            let target = self.raw_usage.get_or_insert_with(|| serde_json::json!({}));
            if let (Some(target), Some(raw)) = (target.as_object_mut(), raw.as_object()) {
                fn merge(
                    target: &mut serde_json::Map<String, serde_json::Value>,
                    raw: &serde_json::Map<String, serde_json::Value>,
                ) {
                    for (key, value) in raw {
                        if let (Some(existing), Some(incoming)) = (
                            target
                                .get_mut(key)
                                .and_then(serde_json::Value::as_object_mut),
                            value.as_object(),
                        ) {
                            merge(existing, incoming);
                        } else {
                            target.insert(key.clone(), value.clone());
                        }
                    }
                }
                merge(target, raw);
            }
            self.raw_usage = self.raw_usage.as_ref().map(crate::usage::bounded_usage);
        }
        // Gemini thoughts are separate from candidates. A partial cumulative
        // snapshot must not replace the earlier candidate count with thoughts.
        if self.semantics.as_deref() == Some("inclusive_cache_separate_thoughts") {
            if let Some(raw) = &self.raw_usage {
                let candidates = raw
                    .get("candidatesTokenCount")
                    .and_then(serde_json::Value::as_u64);
                let thoughts = raw
                    .get("thoughtsTokenCount")
                    .and_then(serde_json::Value::as_u64);
                self.output_tokens = match (candidates, thoughts) {
                    (Some(c), t) => c.checked_add(t.unwrap_or(0)),
                    (None, Some(t)) => Some(t),
                    _ => None,
                };
            }
        }
        if self.semantics.as_deref() == Some("exclusive_cache") {
            self.input_tokens = self.uncached_input_tokens.and_then(|i| {
                i.checked_add(self.cache_read_input_tokens.unwrap_or(0))?
                    .checked_add(self.cache_write_input_tokens.unwrap_or(0))
            });
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChatResponse {
    pub text: String,
    pub usage: Option<TokenUsage>,
    pub termination: ProviderTermination,
    /// Raw reasoning/thinking content from thinking models (e.g. DeepSeek-R1,
    /// o1, Kimi K2.5). Preserved as an opaque pass-through so it can be sent
    /// back in subsequent API requests — some providers reject tool-call history
    /// that omits this field.
    pub reasoning_content: Option<String>,
    pub tool_calls: Vec<ProviderToolCall>,
    /// Opaque provider-native state (for example signed thinking blocks) that
    /// must be attached to the replayed assistant message unchanged.
    pub provider_replay_state: Option<ProviderReplayState>,
}

#[derive(Debug, Clone)]
pub struct StreamChunk {
    pub usage: Option<TokenUsage>,
    pub delta: String,
    pub reasoning_delta: Option<String>,
    pub tool_calls: Vec<ProviderToolCall>,
    /// A complete provider replay state snapshot. When present more than once,
    /// the latest snapshot replaces the previous one.
    pub provider_replay_state: Option<ProviderReplayState>,
    pub is_final: bool,
    pub termination: Option<ProviderTermination>,
}

impl StreamChunk {
    pub fn usage(usage: TokenUsage) -> Self {
        Self {
            usage: Some(usage),
            ..Self::delta("")
        }
    }

    pub fn with_usage(mut self, usage: Option<TokenUsage>) -> Self {
        self.usage = usage;
        self
    }

    pub fn delta(text: impl Into<String>) -> Self {
        Self {
            usage: None,
            delta: text.into(),
            reasoning_delta: None,
            tool_calls: Vec::new(),
            provider_replay_state: None,
            is_final: false,
            termination: None,
        }
    }

    pub fn reasoning(text: impl Into<String>) -> Self {
        Self {
            usage: None,
            delta: String::new(),
            reasoning_delta: Some(text.into()),
            tool_calls: Vec::new(),
            provider_replay_state: None,
            is_final: false,
            termination: None,
        }
    }

    pub fn tool_calls(tool_calls: Vec<ProviderToolCall>) -> Self {
        Self {
            usage: None,
            delta: String::new(),
            reasoning_delta: None,
            tool_calls,
            provider_replay_state: None,
            is_final: false,
            termination: None,
        }
    }

    pub fn provider_replay_state(state: ProviderReplayState) -> Self {
        Self {
            usage: None,
            delta: String::new(),
            reasoning_delta: None,
            tool_calls: Vec::new(),
            provider_replay_state: Some(state),
            is_final: false,
            termination: None,
        }
    }

    pub fn final_chunk_with(termination: ProviderTermination) -> Self {
        Self {
            usage: None,
            delta: String::new(),
            reasoning_delta: None,
            tool_calls: Vec::new(),
            provider_replay_state: None,
            is_final: true,
            termination: Some(termination),
        }
    }
}

/// Hard limits for data originating at a provider boundary.  These values
/// bound both the decoded transport body and the fields which the agent keeps
/// across a streamed or replayed round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderResponseLimits {
    pub max_transport_bytes: usize,
    pub max_text_bytes: usize,
    pub max_reasoning_bytes: usize,
    pub max_tool_calls: usize,
    pub max_tool_arguments_bytes: usize,
    pub max_replay_bytes: usize,
    pub max_retained_bytes: usize,
}

impl Default for ProviderResponseLimits {
    fn default() -> Self {
        Self {
            max_transport_bytes: 16 * 1024 * 1024,
            max_text_bytes: 4 * 1024 * 1024,
            max_reasoning_bytes: 4 * 1024 * 1024,
            max_tool_calls: 64,
            max_tool_arguments_bytes: 4 * 1024 * 1024,
            max_replay_bytes: 2 * 1024 * 1024,
            max_retained_bytes: 16 * 1024 * 1024,
        }
    }
}

impl ProviderResponseLimits {
    pub fn validate_chat_response(
        &self,
        response: &ChatResponse,
    ) -> Result<(), ProviderResponseTooLarge> {
        self.validate_accumulated(
            response.text.as_str(),
            response.reasoning_content.as_deref().unwrap_or_default(),
            response.tool_calls.as_slice(),
            response.provider_replay_state.as_ref(),
        )
    }

    pub fn validate_accumulated(
        &self,
        text: &str,
        reasoning: &str,
        tool_calls: &[ProviderToolCall],
        replay_state: Option<&ProviderReplayState>,
    ) -> Result<(), ProviderResponseTooLarge> {
        self.validate_text(text, "response_text")?;
        self.validate_reasoning(reasoning)?;
        self.validate_tool_calls(tool_calls)?;
        if let Some(state) = replay_state {
            self.validate_replay_state(state)?;
        }
        self.validate_retained_bytes(
            text.len()
                .saturating_add(reasoning.len())
                .saturating_add(tool_calls_bytes(tool_calls))
                .saturating_add(replay_state.map_or(0, replay_state_bytes)),
        )
    }

    pub fn validate_stream_chunk(
        &self,
        chunk: &StreamChunk,
    ) -> Result<(), ProviderResponseTooLarge> {
        self.validate_text(chunk.delta.as_str(), "stream_text_delta")?;
        if let Some(reasoning) = chunk.reasoning_delta.as_deref() {
            self.validate_reasoning(reasoning)?;
        }
        self.validate_tool_calls(chunk.tool_calls.as_slice())?;
        if let Some(state) = chunk.provider_replay_state.as_ref() {
            self.validate_replay_state(state)?;
        }
        self.validate_retained_bytes(
            chunk
                .delta
                .len()
                .saturating_add(chunk.reasoning_delta.as_deref().map_or(0, str::len))
                .saturating_add(tool_calls_bytes(chunk.tool_calls.as_slice()))
                .saturating_add(
                    chunk
                        .provider_replay_state
                        .as_ref()
                        .map_or(0, replay_state_bytes),
                ),
        )
    }

    pub fn validate_text(
        &self,
        text: &str,
        component: &'static str,
    ) -> Result<(), ProviderResponseTooLarge> {
        self.validate_text_bytes(text.len(), component)
    }

    pub fn validate_text_bytes(
        &self,
        bytes: usize,
        component: &'static str,
    ) -> Result<(), ProviderResponseTooLarge> {
        if bytes > self.max_text_bytes {
            return Err(ProviderResponseTooLarge::new(
                component,
                self.max_text_bytes,
                bytes,
            ));
        }
        Ok(())
    }

    pub fn validate_reasoning(&self, reasoning: &str) -> Result<(), ProviderResponseTooLarge> {
        self.validate_reasoning_bytes(reasoning.len())
    }

    pub fn validate_reasoning_bytes(&self, bytes: usize) -> Result<(), ProviderResponseTooLarge> {
        if bytes > self.max_reasoning_bytes {
            return Err(ProviderResponseTooLarge::new(
                "reasoning_content",
                self.max_reasoning_bytes,
                bytes,
            ));
        }
        Ok(())
    }

    pub fn validate_tool_calls(
        &self,
        calls: &[ProviderToolCall],
    ) -> Result<(), ProviderResponseTooLarge> {
        if calls.len() > self.max_tool_calls {
            return Err(ProviderResponseTooLarge::new(
                "tool_calls",
                self.max_tool_calls,
                calls.len(),
            ));
        }
        let arguments_bytes = calls.iter().fold(0usize, |total, call| {
            total.saturating_add(call.arguments.len())
        });
        if arguments_bytes > self.max_tool_arguments_bytes {
            return Err(ProviderResponseTooLarge::new(
                "tool_call_arguments",
                self.max_tool_arguments_bytes,
                arguments_bytes,
            ));
        }
        Ok(())
    }

    pub fn validate_replay_state(
        &self,
        state: &ProviderReplayState,
    ) -> Result<(), ProviderResponseTooLarge> {
        let bytes = replay_state_bytes(state);
        if bytes > self.max_replay_bytes {
            return Err(ProviderResponseTooLarge::new(
                "provider_replay_state",
                self.max_replay_bytes,
                bytes,
            ));
        }
        Ok(())
    }

    pub fn validate_retained_bytes(&self, bytes: usize) -> Result<(), ProviderResponseTooLarge> {
        if bytes > self.max_retained_bytes {
            return Err(ProviderResponseTooLarge::new(
                "retained_response_envelope",
                self.max_retained_bytes,
                bytes,
            ));
        }
        Ok(())
    }
}

fn tool_calls_bytes(calls: &[ProviderToolCall]) -> usize {
    calls.iter().fold(0usize, |total, call| {
        total
            .saturating_add(call.id.len())
            .saturating_add(call.name.len())
            .saturating_add(call.arguments.len())
    })
}

fn replay_state_bytes(state: &ProviderReplayState) -> usize {
    let mut counter = JsonByteCounter::default();
    if serde_json::to_writer(&mut counter, &state.payload).is_err() {
        return usize::MAX;
    }
    state
        .provider
        .len()
        .saturating_add(state.model.as_deref().map_or(0, str::len))
        .saturating_add(counter.bytes)
}

#[derive(Default)]
struct JsonByteCounter {
    bytes: usize,
}

impl std::io::Write for JsonByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderCapabilities {
    pub streaming: bool,
    pub vision: bool,
    pub tool_calling: bool,
    pub embeddings: bool,
    pub transcription: bool,
    pub input_types: ProviderInputCapabilities,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_message_constructors() {
        let sys = ChatMessage::system("Be helpful");
        assert_eq!(sys.role, Role::System);
        assert_eq!(sys.content, "Be helpful");
        assert!(sys.content_parts.is_empty());

        let user = ChatMessage::user("Hello");
        assert_eq!(user.role, Role::User);
        assert!(user.content_parts.is_empty());

        let asst = ChatMessage::assistant("Hi there");
        assert_eq!(asst.role, Role::Assistant);
        assert!(asst.content_parts.is_empty());

        let tool = ChatMessage::tool("result");
        assert_eq!(tool.role, Role::Tool);
        assert_eq!(tool.content, "result");
        assert!(tool.content_parts.is_empty());
        assert!(tool.tool_call_id.is_none());
        assert!(tool.name.is_none());
        assert!(tool.tool_calls.is_none());

        let assistant_with_calls = ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_file".to_owned(),
                arguments: "{\"path\":\"Cargo.toml\"}".to_owned(),
            }],
        );
        assert_eq!(assistant_with_calls.role, Role::Assistant);
        assert!(assistant_with_calls.tool_calls.is_some());

        let tool_result = ChatMessage::tool_result("call_1", "read_file", "ok");
        assert_eq!(tool_result.role, Role::Tool);
        assert_eq!(tool_result.tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(tool_result.name.as_deref(), Some("read_file"));
    }

    #[test]
    fn assistant_round_serialization_preserves_reasoning_calls_and_provider_state() {
        let message = ChatMessage::assistant_tool_calls_with_provider_state(
            Some("working"),
            Some("provider reasoning"),
            vec![
                ProviderToolCall {
                    id: "call_1".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{\"path\":\"a\"}".to_owned(),
                },
                ProviderToolCall {
                    id: "call_2".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{\"path\":\"b\"}".to_owned(),
                },
            ],
            Some(ProviderReplayState::new(
                "test-provider",
                serde_json::json!({"signature": "opaque-signature"}),
            )),
        );

        let encoded = serde_json::to_string(&message).expect("assistant round serializes");
        let decoded =
            serde_json::from_str::<ChatMessage>(&encoded).expect("assistant round deserializes");

        assert_eq!(decoded, message);
    }

    #[test]
    fn replay_compatibility_is_exact_and_legacy_payloads_remain_readable() {
        let legacy: ProviderReplayState = serde_json::from_value(serde_json::json!({
            "provider":"openrouter",
            "payload":{"opaque":"unchanged"}
        }))
        .unwrap();
        assert!(legacy.model.is_none());
        assert!(!legacy.is_compatible_with("openrouter", "any-model"));
        assert_eq!(
            serde_json::to_value(&legacy).unwrap(),
            serde_json::json!({
                "provider":"openrouter",
                "payload":{"opaque":"unchanged"}
            })
        );

        let scoped = ProviderReplayState::for_model(
            "openrouter",
            "exact-model",
            serde_json::json!({"opaque":"unchanged"}),
        );
        assert!(scoped.is_compatible_with("openrouter", "exact-model"));
        assert!(!scoped.is_compatible_with("openrouter", "other-model"));
        assert!(!scoped.is_compatible_with("other-provider", "exact-model"));
    }

    #[test]
    fn stream_chunk_constructors() {
        let chunk = StreamChunk::delta("hello");
        assert_eq!(chunk.delta, "hello");
        assert!(chunk.reasoning_delta.is_none());
        assert!(chunk.tool_calls.is_empty());
        assert!(!chunk.is_final);

        let reasoning = StreamChunk::reasoning("thinking...");
        assert!(reasoning.delta.is_empty());
        assert_eq!(reasoning.reasoning_delta.as_deref(), Some("thinking..."));
        assert!(reasoning.tool_calls.is_empty());
        assert!(!reasoning.is_final);

        let tool_calls = StreamChunk::tool_calls(vec![ProviderToolCall {
            id: "call_1".to_owned(),
            name: "shell".to_owned(),
            arguments: "{\"command\":\"pwd\"}".to_owned(),
        }]);
        assert!(tool_calls.delta.is_empty());
        assert!(tool_calls.reasoning_delta.is_none());
        assert_eq!(tool_calls.tool_calls.len(), 1);
        assert!(!tool_calls.is_final);

        let final_chunk = StreamChunk::final_chunk_with(ProviderTermination::Complete);
        assert!(final_chunk.delta.is_empty());
        assert!(final_chunk.reasoning_delta.is_none());
        assert!(final_chunk.tool_calls.is_empty());
        assert!(final_chunk.is_final);
    }

    #[test]
    fn provider_capabilities_default() {
        let caps = ProviderCapabilities::default();
        assert!(!caps.streaming);
        assert!(!caps.vision);
        assert!(!caps.tool_calling);
        assert!(caps.input_types.text);
        assert!(!caps.input_types.file.is_supported());
        assert!(!caps.input_types.image.is_supported());
        assert!(!caps.input_types.audio.is_supported());
        assert!(!caps.input_types.video.is_supported());
    }

    #[test]
    fn user_parts_message_reports_attachments() {
        let message = ChatMessage::user_parts(vec![
            MessageContentPart::text("Describe this image"),
            MessageContentPart::image(MessageAttachment::from_path("/tmp/snap.png", "image/png")),
        ]);
        assert!(message.has_attachments());
        assert_eq!(message.text_content_lossy(), "Describe this image");
    }

    #[test]
    fn role_serializes_lowercase() {
        let json = serde_json::to_string(&Role::System).unwrap();
        assert_eq!(json, "\"system\"");

        let json = serde_json::to_string(&Role::User).unwrap();
        assert_eq!(json, "\"user\"");

        let json = serde_json::to_string(&Role::Assistant).unwrap();
        assert_eq!(json, "\"assistant\"");
    }

    #[test]
    fn chat_response_default_usage() {
        let resp = ChatResponse {
            text: "Hello".into(),
            usage: None,
            reasoning_content: None,
            tool_calls: Vec::new(),
            provider_replay_state: None,
            termination: ProviderTermination::Complete,
        };
        assert!(resp.usage.is_none());
        assert!(resp.reasoning_content.is_none());
        assert!(resp.tool_calls.is_empty());
    }

    #[test]
    fn provider_response_limits_reject_oversized_logical_fields() {
        let mut limits = ProviderResponseLimits::default();
        limits.max_text_bytes = 3;
        let error = limits
            .validate_stream_chunk(&StreamChunk::delta("abcd"))
            .expect_err("stream text must be bounded before retention");
        assert_eq!(error.component, "stream_text_delta");
        assert_eq!(error.limit, 3);

        limits.max_text_bytes = usize::MAX;
        limits.max_tool_calls = 1;
        let calls = vec![
            ProviderToolCall {
                id: "one".to_owned(),
                name: "read_file".to_owned(),
                arguments: "{}".to_owned(),
            },
            ProviderToolCall {
                id: "two".to_owned(),
                name: "read_file".to_owned(),
                arguments: "{}".to_owned(),
            },
        ];
        let error = limits
            .validate_accumulated("", "", calls.as_slice(), None)
            .expect_err("tool-call count must be bounded");
        assert_eq!(error.component, "tool_calls");
    }

    #[test]
    fn provider_response_limits_bound_replay_and_retained_bytes() {
        let mut limits = ProviderResponseLimits::default();
        limits.max_replay_bytes = 4;
        let replay = ProviderReplayState::new("provider", serde_json::json!({"value": "large"}));
        let error = limits
            .validate_accumulated("", "", &[], Some(&replay))
            .expect_err("replay state must be bounded");
        assert_eq!(error.component, "provider_replay_state");

        limits.max_replay_bytes = usize::MAX;
        limits.max_retained_bytes = 2;
        let error = limits
            .validate_accumulated("abc", "", &[], None)
            .expect_err("retained response envelope must be bounded");
        assert_eq!(error.component, "retained_response_envelope");
    }

    #[test]
    fn provider_termination_preserves_common_cross_provider_reasons() {
        assert_eq!(
            ProviderTermination::from_openai_reason("stop_sequence"),
            ProviderTermination::Complete
        );
        assert_eq!(
            ProviderTermination::from_openai_reason("guardrail_intervened"),
            ProviderTermination::Safety
        );
        assert_eq!(
            ProviderTermination::from_openai_reason("new_provider_reason"),
            ProviderTermination::Unknown("new_provider_reason".to_owned())
        );
    }

    #[test]
    fn token_usage_with_values() {
        let usage = TokenUsage {
            input_tokens: Some(100),
            output_tokens: Some(50),
            ..Default::default()
        };
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.output_tokens, Some(50));
    }

    #[test]
    fn model_input_item_to_chat_message() {
        let item = ModelInputItem::tool_result(
            "call_1",
            "read_file",
            "ok",
            Some(serde_json::json!({"z": 2, "a": {"c": 3, "b": 1}})),
        );
        let message = item.into_chat_message();
        assert_eq!(message.role, Role::Tool);
        assert_eq!(message.tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(message.name.as_deref(), Some("read_file"));
        assert_eq!(message.content, "{\"a\":{\"b\":1,\"c\":3},\"z\":2}");
    }

    #[test]
    fn compiled_prompt_sections_preserve_stable_then_dynamic_order() {
        let compiled = CompiledPromptPayload {
            stable_system_text: "Stable".to_owned(),
            dynamic_system_text: "Dynamic".to_owned(),
            boundary_marker: "<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->".to_owned(),
            full_system_text: "Stable\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic".to_owned(),
        };
        assert_eq!(
            compiled.system_sections(),
            vec!["Stable".to_owned(), "Dynamic".to_owned()]
        );
    }

    #[test]
    fn rendered_messages_with_compiled_prompt_uses_canonical_full_text() {
        let request = ChatRequest {
            model: "test-model".to_owned(),
            messages: vec![
                ChatMessage::system("legacy prompt"),
                ChatMessage::user("Hello"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: Some(CompiledPromptPayload {
                stable_system_text: "Stable".to_owned(),
                dynamic_system_text: "Dynamic".to_owned(),
                boundary_marker: "<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->".to_owned(),
                full_system_text: "Stable\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic"
                    .to_owned(),
            }),
        };

        let rendered = request.rendered_messages_with_compiled_prompt();
        assert_eq!(rendered.len(), 2);
        assert_eq!(rendered[0].role, Role::System);
        assert_eq!(
            rendered[0].content,
            "Stable\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic"
        );
        assert_eq!(rendered[1].role, Role::User);
    }
}

#[cfg(test)]
mod provenance_tests {
    use super::*;
    #[test]
    fn runtime_provenance_survives_cloning_but_never_provider_serialization() {
        let provenance = MessageProvenance {
            logical_turn_id: Some("command-turn".into()),
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            context_thread: None,
            unit_id: "round".into(),
            sources: vec![MessageSourceRef {
                scope: "input:turn".into(),
                id: "source".into(),
                version: "input-revision:1".into(),
            }],
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            complete: true,
            protected_input: true,
            inherited: false,
        };
        let mut message = ChatMessage::user("text");
        message.provenance = Some(provenance.clone());
        assert_eq!(message.clone().provenance, Some(provenance.clone()));
        let mut wire = serde_json::to_value(message).unwrap();
        assert!(wire.get("provenance").is_none());
        wire["provenance"] = serde_json::to_value(provenance).unwrap();
        let received: ChatMessage = serde_json::from_value(wire).unwrap();
        assert_eq!(received.provenance, None);
    }
}

#[cfg(test)]
mod tool_outcome_contract_tests {
    use super::*;
    #[test]
    fn outcome_is_additive_fixed_size_and_never_inferred_from_content() {
        let legacy: ChatMessage = serde_json::from_value(serde_json::json!({"role":"tool","content":"{\"success\":false}","tool_call_id":"call","name":"read"})).unwrap();
        assert!(legacy.tool_outcome.is_none());
        assert_eq!(legacy.wire_content(), legacy.content);
        let future: ToolResultOutcome = serde_json::from_value(
            serde_json::json!({"schema_version":1,"status":"future_status"}),
        )
        .unwrap();
        assert_eq!(future.is_error(), None);
        let outcome = ToolResultOutcome::new(ToolResultStatus::RecoverableError);
        let message = ModelInputItem::tool_result("call", "read", "false", None)
            .with_tool_outcome(outcome)
            .into_chat_message();
        let cold: ChatMessage =
            serde_json::from_value(serde_json::to_value(&message).unwrap()).unwrap();
        assert_eq!(cold.tool_outcome, Some(outcome));
        assert_eq!(cold.content, "false");
        assert_eq!(
            cold.wire_content(),
            "Tool outcome: recoverable_error.\nfalse"
        );
        let mut empty = ChatMessage::tool_result("call", "read", "");
        empty.tool_outcome = Some(outcome);
        assert!(!empty.wire_content_is_empty());
        assert!(empty.wire_content().contains("recoverable_error"));
        assert!(serde_json::to_vec(&outcome).unwrap().len() < 80);
    }
}
