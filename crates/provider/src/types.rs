use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::time::Duration;

pub use pioneer_protocol::ReasoningEffort;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
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
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        }
    }

    pub fn tool(content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
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
        Self {
            role: Role::Assistant,
            content: content.map(Into::into).unwrap_or_default(),
            reasoning_content: reasoning_content.map(Into::into),
            content_parts: Vec::new(),
            tool_call_id: None,
            name: None,
            tool_calls: Some(tool_calls),
        }
    }

    pub fn tool_result(
        tool_call_id: impl Into<String>,
        name: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            role: Role::Tool,
            content: content.into(),
            reasoning_content: None,
            content_parts: Vec::new(),
            tool_call_id: Some(tool_call_id.into()),
            name: Some(name.into()),
            tool_calls: None,
        }
    }

    pub fn user_parts(parts: Vec<MessageContentPart>) -> Self {
        Self {
            role: Role::User,
            content: String::new(),
            reasoning_content: None,
            content_parts: parts,
            tool_call_id: None,
            name: None,
            tool_calls: None,
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
        }
    }

    pub fn into_chat_message(self) -> ChatMessage {
        match self {
            Self::Message { message } => message,
            Self::ToolResult {
                tool_call_id,
                name,
                content,
                payload,
            } => {
                let content = payload
                    .map(|payload| canonical_json_string(&payload))
                    .unwrap_or(content);
                ChatMessage::tool_result(tool_call_id, name, content)
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
    pub max_tokens: Option<u32>,
    pub tools: Option<Vec<ToolDefinition>>,
    pub tool_choice: Option<ToolChoice>,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningConfig {
    Disabled,
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

#[derive(Debug, Clone, Default)]
pub struct TokenUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ChatResponse {
    pub text: String,
    pub usage: Option<TokenUsage>,
    /// Raw reasoning/thinking content from thinking models (e.g. DeepSeek-R1,
    /// o1, Kimi K2.5). Preserved as an opaque pass-through so it can be sent
    /// back in subsequent API requests — some providers reject tool-call history
    /// that omits this field.
    pub reasoning_content: Option<String>,
    pub tool_calls: Vec<ProviderToolCall>,
}

#[derive(Debug, Clone)]
pub struct StreamChunk {
    pub delta: String,
    pub reasoning_delta: Option<String>,
    pub tool_calls: Vec<ProviderToolCall>,
    pub is_final: bool,
}

impl StreamChunk {
    pub fn delta(text: impl Into<String>) -> Self {
        Self {
            delta: text.into(),
            reasoning_delta: None,
            tool_calls: Vec::new(),
            is_final: false,
        }
    }

    pub fn reasoning(text: impl Into<String>) -> Self {
        Self {
            delta: String::new(),
            reasoning_delta: Some(text.into()),
            tool_calls: Vec::new(),
            is_final: false,
        }
    }

    pub fn tool_calls(tool_calls: Vec<ProviderToolCall>) -> Self {
        Self {
            delta: String::new(),
            reasoning_delta: None,
            tool_calls,
            is_final: false,
        }
    }

    pub fn final_chunk() -> Self {
        Self {
            delta: String::new(),
            reasoning_delta: None,
            tool_calls: Vec::new(),
            is_final: true,
        }
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

        let final_chunk = StreamChunk::final_chunk();
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
        };
        assert!(resp.usage.is_none());
        assert!(resp.reasoning_content.is_none());
        assert!(resp.tool_calls.is_empty());
    }

    #[test]
    fn token_usage_with_values() {
        let usage = TokenUsage {
            input_tokens: Some(100),
            output_tokens: Some(50),
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
