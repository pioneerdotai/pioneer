#[cfg(test)]
use crate::attachments::prepare_messages_for_provider;
use crate::{
    attachments::{
        PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes, attachment_data_url,
        ensure_no_unrendered_attachments, prepare_messages_for_provider_async,
    },
    tools::call::{StreamToolCallAccumulator, StreamToolCallDelta, StreamToolFunctionDelta},
    tools::parse::{parse_embedded_tool_payload, parse_tool_calls},
    tools::stream::{IncrementalLineDecoder, sse_data},
    types::{
        ChatMessage, ChatRequest, ChatResponse, InputContentType, InputTypeSupport,
        ProviderCapabilities, ProviderInputCapabilities, ProviderReplayState, ProviderTermination,
        ProviderTimeoutPolicy, Role, StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
    },
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

/// How to pass the credential in HTTP requests.
#[derive(Debug, Clone)]
pub enum AuthStyle {
    /// `Authorization: Bearer <credential>`
    Bearer,
    /// `x-api-key: <credential>`
    XApiKey,
    /// Custom header name: `<header>: <credential>`
    Custom(String),
}

/// A generic provider for any API that follows the OpenAI chat completions format.
///
/// Configurable with name, base URL, credential, auth style, and various
/// capability flags. Used as the backbone for 40+ providers that share the
/// same wire format but differ in endpoint and authentication.
pub struct OpenAiCompatibleProvider {
    name: String,
    base_url: String,
    credential: String,
    auth_style: AuthStyle,
    input_types: ProviderInputCapabilities,
    merge_system_into_user: bool,
    replay_reasoning_content: bool,
    timeout_policy: ProviderTimeoutPolicy,
    extra_headers: HashMap<String, String>,
    client: Client,
}

const COMPATIBLE_REPLAY_STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CompatibleAssistantReplayState {
    schema_version: u32,
    assistant_message: CompatibleAssistantReplayMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct CompatibleAssistantReplayMessage {
    content: Option<String>,
    pub(super) reasoning_content: Option<String>,
    tool_calls: Vec<ApiToolCall>,
}

// ── OpenAI-compatible API request types ─────────────────────────────────────

#[derive(Debug, Serialize)]
struct ApiChatRequest {
    model: String,
    messages: Vec<ApiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ApiToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<ApiToolChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parallel_tool_calls: Option<bool>,
    stream: bool,
}

#[derive(Debug, Serialize)]
struct ApiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<ApiMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ApiToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ApiMessageContent {
    Text(String),
    Parts(Vec<ApiContentPart>),
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ApiContentPart {
    Text { text: String },
    ImageUrl { image_url: ApiImageUrl },
    File { file: ApiFilePart },
    InputAudio { input_audio: ApiInputAudioPart },
    VideoUrl { video_url: ApiVideoUrl },
}

#[derive(Debug, Serialize)]
struct ApiImageUrl {
    url: String,
}

#[derive(Debug, Serialize)]
struct ApiFilePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    file_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filename: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApiInputAudioPart {
    data: String,
    format: String,
}

#[derive(Debug, Serialize)]
struct ApiVideoUrl {
    url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiToolDefinition {
    #[serde(rename = "type")]
    kind: String,
    function: ApiToolFunctionDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiToolFunctionDefinition {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum ApiToolChoice {
    Literal(String),
    Tool {
        #[serde(rename = "type")]
        kind: String,
        function: ApiToolChoiceFunction,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiToolChoiceFunction {
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: ApiToolCallFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiToolCallFunction {
    name: String,
    arguments: String,
}

// ── OpenAI-compatible API response types ────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ApiChatResponse {
    choices: Vec<ApiChoice>,
    #[serde(default)]
    usage: Option<ApiUsage>,
}

#[derive(Debug, Deserialize)]
struct ApiChoice {
    message: ApiResponseMessage,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiResponseMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    tool_calls: Option<serde_json::Value>,
    #[serde(default)]
    function_call: Option<serde_json::Value>,
}

impl ApiResponseMessage {
    fn effective_content(&self) -> String {
        self.content
            .as_ref()
            .map(|c| strip_think_tags(c))
            .unwrap_or_default()
    }
}

#[derive(Debug, Deserialize)]
struct ApiUsage {
    #[serde(default)]
    prompt_tokens: Option<u64>,
    #[serde(default)]
    completion_tokens: Option<u64>,
}

// ── SSE streaming response types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct StreamResponse {
    #[serde(default)]
    choices: Vec<StreamChoice>,
    #[serde(default)]
    error: Option<StreamError>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    #[serde(default)]
    delta: StreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct StreamDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<StreamToolCallDelta>>,
    #[serde(default)]
    function_call: Option<StreamToolFunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct StreamError {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    code: Option<serde_json::Value>,
    #[serde(default, rename = "type")]
    error_type: Option<String>,
}

impl StreamError {
    fn description(self) -> String {
        let mut parts = Vec::new();
        if let Some(message) = self.message.filter(|message| !message.is_empty()) {
            parts.push(message);
        }
        if let Some(error_type) = self.error_type.filter(|error_type| !error_type.is_empty()) {
            parts.push(format!("type={error_type}"));
        }
        if let Some(code) = self.code {
            let code = code
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| code.to_string());
            if !code.is_empty() {
                parts.push(format!("code={code}"));
            }
        }
        if parts.is_empty() {
            "unknown stream error".to_owned()
        } else {
            parts.join(", ")
        }
    }
}

// ── List models response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ModelsListResponse {
    data: Vec<CompatModelEntry>,
}

#[derive(Debug, Deserialize)]
struct CompatModelEntry {
    id: String,
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    owned_by: Option<String>,
}

/// Strip `<think>...</think>` tags from content returned by some thinking models.
fn strip_think_tags(text: &str) -> String {
    let mut result = text.to_owned();
    while let Some(start) = result.find("<think>") {
        if let Some(end) = result[start..].find("</think>") {
            result = format!("{}{}", &result[..start], &result[start + end + 8..]);
        } else {
            // Unclosed <think> tag — strip from tag to end
            result = result[..start].to_owned();
            break;
        }
    }
    result.trim().to_owned()
}

// ── Builder-style construction ──────────────────────────────────────────────

impl OpenAiCompatibleProvider {
    /// Create a new provider with the required fields.
    pub fn new(
        name: impl Into<String>,
        base_url: impl Into<String>,
        credential: impl Into<String>,
        auth_style: AuthStyle,
    ) -> Self {
        let base_url = base_url.into();
        let timeout_policy = ProviderTimeoutPolicy::default();
        Self {
            name: name.into(),
            base_url,
            credential: credential.into(),
            auth_style,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
            merge_system_into_user: false,
            replay_reasoning_content: true,
            timeout_policy,
            extra_headers: HashMap::new(),
            client: crate::http::build_client(timeout_policy),
        }
    }

    /// Enable or disable vision support.
    pub fn with_vision(mut self, supports_vision: bool) -> Self {
        self.input_types = if supports_vision {
            ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::disabled(),
                image: InputTypeSupport::data_url_inline_only(),
                audio: InputTypeSupport::disabled(),
                video: InputTypeSupport::disabled(),
            }
        } else {
            ProviderInputCapabilities::disabled_for_all_file_types()
        };
        self
    }

    pub fn with_input_capabilities(mut self, input_types: ProviderInputCapabilities) -> Self {
        self.input_types = input_types;
        self
    }

    /// When enabled, prepend system message content into the first user message.
    /// Useful for providers that do not support the `system` role.
    pub fn with_merge_system_into_user(mut self, merge: bool) -> Self {
        self.merge_system_into_user = merge;
        self
    }

    /// Include prior assistant `reasoning_content` in replayed chat history.
    pub fn with_reasoning_content_replay(mut self, replay: bool) -> Self {
        self.replay_reasoning_content = replay;
        self
    }

    /// Override the default request timeout.
    pub fn with_timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_policy.non_stream_request_timeout =
            std::time::Duration::from_secs(secs.max(1));
        self.client = crate::http::build_client(self.timeout_policy);
        self
    }

    pub fn with_timeout_policy(mut self, timeout_policy: ProviderTimeoutPolicy) -> Self {
        self.timeout_policy = timeout_policy;
        self.client = crate::http::build_client(timeout_policy);
        self
    }

    /// Add extra headers sent with every request.
    pub fn with_extra_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.extra_headers = headers;
        self
    }

    /// Set a custom User-Agent header.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.extra_headers
            .insert("User-Agent".to_string(), user_agent.into());
        self
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Resolve the chat completions endpoint URL.
    /// If the base_url already ends with `/chat/completions`, use it as-is.
    fn chat_completions_url(&self) -> String {
        if self.base_url.ends_with("/chat/completions") {
            self.base_url.clone()
        } else {
            let base = self.base_url.trim_end_matches('/');
            format!("{base}/chat/completions")
        }
    }

    /// Resolve the models endpoint URL.
    fn models_url(&self) -> String {
        let base = self.base_url.trim_end_matches('/');
        // Strip /chat/completions if the base_url points directly there
        let base = base.strip_suffix("/chat/completions").unwrap_or(base);
        let base = base.trim_end_matches('/');
        format!("{base}/models")
    }

    /// Build a GET request builder with the correct auth and extra headers applied.
    fn authorized_get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.client.get(url);

        match &self.auth_style {
            AuthStyle::Bearer => {
                builder = builder.header("Authorization", format!("Bearer {}", self.credential));
            }
            AuthStyle::XApiKey => {
                builder = builder.header("x-api-key", &self.credential);
            }
            AuthStyle::Custom(header_name) => {
                builder = builder.header(header_name, &self.credential);
            }
        }

        for (key, value) in &self.extra_headers {
            builder = builder.header(key, value);
        }

        builder
    }

    /// Build a request builder with the correct auth and extra headers applied.
    fn authorized_post(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.client.post(url);

        match &self.auth_style {
            AuthStyle::Bearer => {
                builder = builder.header("Authorization", format!("Bearer {}", self.credential));
            }
            AuthStyle::XApiKey => {
                builder = builder.header("x-api-key", &self.credential);
            }
            AuthStyle::Custom(header_name) => {
                builder = builder.header(header_name, &self.credential);
            }
        }

        for (key, value) in &self.extra_headers {
            builder = builder.header(key, value);
        }

        builder
    }

    fn audio_format_from_mime(mime: &str) -> String {
        let normalized = mime
            .split(';')
            .next()
            .unwrap_or(mime)
            .trim()
            .to_ascii_lowercase();
        let subtype = normalized
            .split('/')
            .nth(1)
            .unwrap_or(normalized.as_str())
            .trim();
        match subtype {
            "x-wav" => "wav".to_owned(),
            "mpga" => "mp3".to_owned(),
            "x-m4a" => "m4a".to_owned(),
            "x-aiff" => "aiff".to_owned(),
            other => other.to_owned(),
        }
    }

    fn looks_like_url(value: &str) -> bool {
        let value = value.trim().to_ascii_lowercase();
        value.starts_with("http://") || value.starts_with("https://") || value.starts_with("data:")
    }

    fn media_url_or_data_url(
        attachment: &crate::attachments::PreparedAttachment,
    ) -> Result<String> {
        match &attachment.source {
            PreparedAttachmentSource::Reference { reference } => Ok(reference.clone()),
            _ => attachment_data_url(attachment),
        }
    }

    fn build_file_part(
        attachment: &crate::attachments::PreparedAttachment,
    ) -> Result<ApiContentPart> {
        let file = match &attachment.source {
            PreparedAttachmentSource::Reference { reference } => {
                if Self::looks_like_url(reference.as_str()) {
                    ApiFilePart {
                        file_data: Some(reference.clone()),
                        file_id: None,
                        filename: Some(attachment.name.clone()),
                    }
                } else {
                    ApiFilePart {
                        file_data: None,
                        file_id: Some(reference.clone()),
                        filename: None,
                    }
                }
            }
            _ => ApiFilePart {
                file_data: Some(BASE64.encode(attachment_bytes(attachment)?)),
                file_id: None,
                filename: Some(attachment.name.clone()),
            },
        };
        Ok(ApiContentPart::File { file })
    }

    /// Convert our domain messages to the API wire format, optionally merging
    /// the system message into the first user message.
    fn convert_messages(&self, prepared: &PreparedProviderMessages) -> Result<Vec<ApiMessage>> {
        if self.merge_system_into_user {
            self.merge_system_messages(prepared)
        } else {
            self.direct_convert_for_provider(prepared)
        }
    }

    fn direct_convert(prepared: &PreparedProviderMessages) -> Result<Vec<ApiMessage>> {
        Self::direct_convert_with_options(prepared, false, "openai_compatible")
    }

    fn direct_convert_for_provider(
        &self,
        prepared: &PreparedProviderMessages,
    ) -> Result<Vec<ApiMessage>> {
        if self.replay_reasoning_content {
            Self::direct_convert_with_options(prepared, true, self.name.as_str())
        } else {
            Self::direct_convert(prepared)
        }
    }

    fn direct_convert_with_options(
        prepared: &PreparedProviderMessages,
        replay_reasoning_content: bool,
        provider_name: &str,
    ) -> Result<Vec<ApiMessage>> {
        let mut rendered = Vec::new();
        for (message_index, message) in prepared.messages.iter().enumerate() {
            let attachments = prepared
                .attachments_for_message(message_index)
                .collect::<Vec<_>>();
            if message.role == Role::Tool && !attachments.is_empty() {
                let tool_text = if message.content.is_empty() {
                    "{}".to_owned()
                } else {
                    message.content.clone()
                };
                rendered.push(Self::build_api_message(
                    "tool",
                    message,
                    &[],
                    Some(tool_text),
                    provider_name,
                    replay_reasoning_content,
                )?);
                rendered.push(Self::build_tool_context_message(
                    attachments.as_slice(),
                    provider_name,
                )?);
                continue;
            }

            rendered.push(Self::build_api_message(
                match message.role {
                    Role::System => "system",
                    Role::User => "user",
                    Role::Assistant => "assistant",
                    Role::Tool => "tool",
                },
                message,
                attachments.as_slice(),
                None,
                provider_name,
                replay_reasoning_content,
            )?);
        }
        Ok(rendered)
    }

    fn build_parts(
        text: &str,
        attachments: &[&crate::attachments::PreparedAttachment],
        include_text: bool,
        provider_name: &str,
    ) -> Result<Vec<ApiContentPart>> {
        let mut parts = Vec::new();
        if include_text && !text.is_empty() {
            parts.push(ApiContentPart::Text {
                text: text.to_owned(),
            });
        }

        for attachment in attachments {
            match attachment.kind {
                InputContentType::Image => {
                    parts.push(ApiContentPart::ImageUrl {
                        image_url: ApiImageUrl {
                            url: Self::media_url_or_data_url(attachment)?,
                        },
                    });
                }
                InputContentType::File => {
                    parts.push(Self::build_file_part(attachment)?);
                }
                InputContentType::Audio => {
                    parts.push(ApiContentPart::InputAudio {
                        input_audio: ApiInputAudioPart {
                            data: BASE64.encode(attachment_bytes(attachment)?),
                            format: Self::audio_format_from_mime(attachment.mime_type.as_str()),
                        },
                    });
                }
                InputContentType::Video => {
                    parts.push(ApiContentPart::VideoUrl {
                        video_url: ApiVideoUrl {
                            url: Self::media_url_or_data_url(attachment)?,
                        },
                    });
                }
                InputContentType::Text => {
                    return Err(anyhow!(
                        "provider `{provider_name}` received unexpected text attachment part"
                    ));
                }
            }
        }

        Ok(parts)
    }

    fn build_tool_context_message(
        attachments: &[&crate::attachments::PreparedAttachment],
        provider_name: &str,
    ) -> Result<ApiMessage> {
        let parts = Self::build_parts(
            "Analyze the attached tool media context and continue.",
            attachments,
            true,
            provider_name,
        )?;
        Ok(ApiMessage {
            role: "user".to_owned(),
            content: Some(ApiMessageContent::Parts(parts)),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
            name: None,
        })
    }

    fn build_content(
        message: &ChatMessage,
        attachments: &[&crate::attachments::PreparedAttachment],
        override_text: Option<String>,
        provider_name: &str,
    ) -> Result<Option<ApiMessageContent>> {
        if attachments.is_empty() {
            let text = override_text.unwrap_or_else(|| message.content.clone());
            if text.is_empty() && message.tool_calls.is_some() {
                return Ok(None);
            }
            return Ok(Some(ApiMessageContent::Text(text)));
        }

        let text = override_text.unwrap_or_else(|| message.content.clone());
        let parts = Self::build_parts(text.as_str(), attachments, true, provider_name)?;
        Ok(Some(ApiMessageContent::Parts(parts)))
    }

    fn build_api_message(
        role: &str,
        message: &ChatMessage,
        attachments: &[&crate::attachments::PreparedAttachment],
        override_text: Option<String>,
        provider_name: &str,
        replay_reasoning_content: bool,
    ) -> Result<ApiMessage> {
        if message.role == Role::Assistant
            && let Some(replay) = Self::decode_assistant_replay_state(message, provider_name)?
        {
            return Ok(ApiMessage {
                role: role.to_owned(),
                content: replay.content.map(ApiMessageContent::Text),
                reasoning_content: replay.reasoning_content,
                tool_calls: Some(replay.tool_calls),
                tool_call_id: None,
                name: None,
            });
        }

        Ok(ApiMessage {
            role: role.to_owned(),
            content: Self::build_content(message, attachments, override_text, provider_name)?,
            reasoning_content: if replay_reasoning_content && message.role == Role::Assistant {
                message.reasoning_content.clone()
            } else {
                None
            },
            tool_calls: message.tool_calls.as_deref().map(Self::convert_tool_calls),
            tool_call_id: message.tool_call_id.clone(),
            name: message.name.clone(),
        })
    }

    /// Merge system messages: collect all leading system messages and prepend
    /// their content to the first user message.
    fn merge_system_messages(
        &self,
        prepared: &PreparedProviderMessages,
    ) -> Result<Vec<ApiMessage>> {
        let messages = &prepared.messages;
        let mut system_parts: Vec<&str> = Vec::new();
        let mut rest_start = 0;

        for (i, msg) in messages.iter().enumerate() {
            if msg.role == Role::System {
                system_parts.push(&msg.content);
                rest_start = i + 1;
            } else {
                break;
            }
        }

        let system_prefix = system_parts.join("\n");
        let mut result = Vec::with_capacity(messages.len());

        let mut system_prepended = false;
        for (offset, msg) in messages[rest_start..].iter().enumerate() {
            let message_index = rest_start + offset;
            let attachments = prepared
                .attachments_for_message(message_index)
                .collect::<Vec<_>>();
            if msg.role == Role::Tool && !attachments.is_empty() {
                let tool_text = if msg.content.is_empty() {
                    "{}".to_owned()
                } else {
                    msg.content.clone()
                };
                result.push(Self::build_api_message(
                    "tool",
                    msg,
                    &[],
                    Some(tool_text),
                    self.name.as_str(),
                    self.replay_reasoning_content,
                )?);
                result.push(Self::build_tool_context_message(
                    attachments.as_slice(),
                    self.name.as_str(),
                )?);
                continue;
            }

            if !system_prepended && msg.role == Role::User && !system_prefix.is_empty() {
                let merged_text = if msg.content.is_empty() {
                    system_prefix.clone()
                } else {
                    format!("{system_prefix}\n\n{}", msg.content)
                };
                result.push(Self::build_api_message(
                    "user",
                    msg,
                    attachments.as_slice(),
                    Some(merged_text),
                    self.name.as_str(),
                    self.replay_reasoning_content,
                )?);
                system_prepended = true;
            } else {
                result.push(Self::build_api_message(
                    match msg.role {
                        Role::System => "user", // demote any later system messages too
                        Role::User => "user",
                        Role::Assistant => "assistant",
                        Role::Tool => "tool",
                    },
                    msg,
                    attachments.as_slice(),
                    None,
                    self.name.as_str(),
                    self.replay_reasoning_content,
                )?);
            }
        }

        // Edge case: only system messages, no user message to merge into.
        if !system_prefix.is_empty() && !system_prepended {
            result.insert(
                0,
                ApiMessage {
                    role: "user".into(),
                    content: Some(ApiMessageContent::Text(system_prefix)),
                    reasoning_content: None,
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
            );
        }

        Ok(result)
    }

    fn convert_tools(tools: &[ToolDefinition]) -> Vec<ApiToolDefinition> {
        tools
            .iter()
            .map(|tool| ApiToolDefinition {
                kind: "function".to_owned(),
                function: ApiToolFunctionDefinition {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    parameters: tool.parameters.clone(),
                },
            })
            .collect()
    }

    fn convert_tool_calls(tool_calls: &[crate::types::ProviderToolCall]) -> Vec<ApiToolCall> {
        tool_calls
            .iter()
            .map(|call| ApiToolCall {
                id: call.id.clone(),
                kind: "function".to_owned(),
                function: ApiToolCallFunction {
                    name: call.name.clone(),
                    arguments: call.arguments.clone(),
                },
            })
            .collect()
    }

    pub(super) fn assistant_replay_state(
        provider_name: &str,
        content: Option<String>,
        reasoning_content: Option<String>,
        tool_calls: &[crate::types::ProviderToolCall],
    ) -> ProviderReplayState {
        ProviderReplayState::new(
            provider_name,
            serde_json::to_value(CompatibleAssistantReplayState {
                schema_version: COMPATIBLE_REPLAY_STATE_SCHEMA_VERSION,
                assistant_message: CompatibleAssistantReplayMessage {
                    content,
                    reasoning_content,
                    tool_calls: Self::convert_tool_calls(tool_calls),
                },
            })
            .expect("compatible assistant replay state must serialize"),
        )
    }

    fn decode_assistant_replay_state(
        message: &ChatMessage,
        provider_name: &str,
    ) -> Result<Option<CompatibleAssistantReplayMessage>> {
        let Some(state) = message.provider_replay_state.as_ref() else {
            return Ok(None);
        };
        Self::decode_replay_state(state, provider_name).map(Some)
    }

    pub(super) fn decode_replay_state(
        state: &ProviderReplayState,
        provider_name: &str,
    ) -> Result<CompatibleAssistantReplayMessage> {
        let payload = state.payload_for(provider_name).ok_or_else(|| {
            anyhow!(
                "provider replay state `{}` cannot be rendered by `{provider_name}`",
                state.provider
            )
        })?;
        let replay = serde_json::from_value::<CompatibleAssistantReplayState>(payload.clone())
            .map_err(|error| anyhow!("invalid {provider_name} replay state: {error}"))?;
        if replay.schema_version != COMPATIBLE_REPLAY_STATE_SCHEMA_VERSION {
            return Err(anyhow!(
                "unsupported {provider_name} replay state schema version {}",
                replay.schema_version
            ));
        }
        Ok(replay.assistant_message)
    }

    pub(super) fn replay_message(
        &self,
        message: &ChatMessage,
    ) -> Result<Option<CompatibleAssistantReplayMessage>> {
        Self::decode_assistant_replay_state(message, self.name.as_str())
    }

    #[cfg(test)]
    fn replay_state_message(
        &self,
        state: &ProviderReplayState,
    ) -> Result<CompatibleAssistantReplayMessage> {
        Self::decode_replay_state(state, self.name.as_str())
    }

    fn convert_tool_choice(choice: ToolChoice) -> ApiToolChoice {
        match choice {
            ToolChoice::Auto => ApiToolChoice::Literal("auto".to_owned()),
            ToolChoice::None => ApiToolChoice::Literal("none".to_owned()),
            ToolChoice::Required => ApiToolChoice::Literal("required".to_owned()),
            ToolChoice::Tool { name } => ApiToolChoice::Tool {
                kind: "function".to_owned(),
                function: ApiToolChoiceFunction { name },
            },
        }
    }

    async fn build_chat_request_async(
        &self,
        request: ChatRequest,
        stream: bool,
    ) -> Result<ApiChatRequest> {
        let capabilities =
            <OpenAiCompatibleProvider as crate::traits::Provider>::capabilities(self);
        let prepared = prepare_messages_for_provider_async(
            self.name.as_str(),
            &capabilities,
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        self.build_chat_request_from_prepared(request, stream, prepared)
    }

    #[cfg(test)]
    fn build_chat_request(&self, request: ChatRequest, stream: bool) -> Result<ApiChatRequest> {
        let capabilities =
            <OpenAiCompatibleProvider as crate::traits::Provider>::capabilities(self);
        let prepared = prepare_messages_for_provider(
            self.name.as_str(),
            &capabilities,
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )?;
        self.build_chat_request_from_prepared(request, stream, prepared)
    }

    fn build_chat_request_from_prepared(
        &self,
        request: ChatRequest,
        stream: bool,
        prepared: PreparedProviderMessages,
    ) -> Result<ApiChatRequest> {
        ensure_no_unrendered_attachments(self.name.as_str(), &prepared)?;
        Ok(ApiChatRequest {
            model: request.model,
            messages: self.convert_messages(&prepared)?,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            parallel_tool_calls: request.parallel_tool_calls,
            stream,
        })
    }

    async fn api_error(&self, response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read error body>".into());
        anyhow!("{} API error ({status}): {body}", self.name)
    }
}

fn finish_compatible_stream(
    tool_call_accumulator: &mut StreamToolCallAccumulator,
    provider_name: &str,
    replay_reasoning_content: bool,
    content: Option<String>,
    reasoning_content: Option<String>,
    termination: ProviderTermination,
) -> Result<Vec<StreamChunk>> {
    let tool_calls = tool_call_accumulator.take_tool_calls()?;
    let mut chunks = Vec::new();
    if replay_reasoning_content && !tool_calls.is_empty() {
        chunks.push(StreamChunk::provider_replay_state(
            OpenAiCompatibleProvider::assistant_replay_state(
                provider_name,
                content,
                reasoning_content,
                tool_calls.as_slice(),
            ),
        ));
    }
    if !tool_calls.is_empty() {
        chunks.push(StreamChunk::tool_calls(tool_calls));
    }
    chunks.push(StreamChunk::final_chunk_with(termination));
    Ok(chunks)
}

// ── Provider trait implementation ───────────────────────────────────────────

#[async_trait]
impl crate::traits::Provider for OpenAiCompatibleProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: self.input_types.image.is_supported(),
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: self.input_types.clone(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let api_request = self.build_chat_request_async(request, false).await?;

        let request_builder = self
            .authorized_post(&self.chat_completions_url())
            .json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(self.api_error(response).await);
        }

        let api_response: ApiChatResponse = response.json().await?;
        let usage = api_response.usage.map(|u| TokenUsage {
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
        });

        let choice = api_response
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no response from {}", self.name))?;
        let termination = choice
            .finish_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| ProviderTermination::Unknown("missing_finish_reason".to_owned()));
        let message = choice.message;

        let raw_content = message.content.clone();
        let mut text = message.effective_content();
        let mut tool_calls =
            parse_tool_calls(message.tool_calls.as_ref(), message.function_call.as_ref())?;
        let mut reasoning_content = message.reasoning_content.or(message.reasoning);

        if tool_calls.is_empty() {
            if let Some(content) = raw_content.as_deref() {
                if let Some(parsed) = parse_embedded_tool_payload(content)? {
                    text = parsed.text;
                    if reasoning_content.is_none() {
                        reasoning_content = parsed.reasoning_content;
                    }
                    tool_calls = parsed.tool_calls;
                }
            }
        }

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(anyhow!("no response from {}", self.name));
        }

        let provider_replay_state =
            (self.replay_reasoning_content && !tool_calls.is_empty()).then(|| {
                Self::assistant_replay_state(
                    self.name.as_str(),
                    raw_content,
                    reasoning_content.clone(),
                    tool_calls.as_slice(),
                )
            });

        Ok(ChatResponse {
            text,
            usage,
            termination,
            reasoning_content,
            tool_calls,
            provider_replay_state,
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let api_request = self.build_chat_request_async(request, true).await?;

        let request_builder = self
            .authorized_post(&self.chat_completions_url())
            .json(&api_request);
        let response =
            crate::http::send_stream_request(request_builder, self.timeout_policy).await?;

        if !response.status().is_success() {
            return Err(self.api_error(response).await);
        }

        let byte_stream = response.bytes_stream();
        let provider_name = self.name.clone();
        let replay_reasoning_content = self.replay_reasoning_content;

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            let mut decoder = IncrementalLineDecoder::default();
            let mut tool_call_accumulator = StreamToolCallAccumulator::default();
            let mut response_content: Option<String> = None;
            let mut response_reasoning_content: Option<String> = None;

            tokio::pin!(byte_stream);

            while let Some(result) = byte_stream.next().await {
                let bytes = match result {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        let _ = tx.send(Err(anyhow!(e))).await;
                        return;
                    }
                };

                let lines = match decoder.push(bytes.as_ref()) {
                    Ok(lines) => lines,
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        return;
                    }
                };
                for line in lines {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    let Some(data) = sse_data(line) else {
                        continue;
                    };

                    if data.trim() == "[DONE]" {
                        let _ = tx
                            .send(Err(anyhow!(
                                "{} stream ended without a finish_reason",
                                provider_name
                            )))
                            .await;
                        return;
                    }

                    match serde_json::from_str::<StreamResponse>(data) {
                        Ok(resp) => {
                            if let Some(error) = resp.error {
                                let _ = tx
                                    .send(Err(anyhow!(
                                        "{} stream error: {}",
                                        provider_name,
                                        error.description()
                                    )))
                                    .await;
                                return;
                            }
                            for choice in resp.choices {
                                if let Some(rc) =
                                    choice.delta.reasoning_content.or(choice.delta.reasoning)
                                {
                                    response_reasoning_content
                                        .get_or_insert_with(String::new)
                                        .push_str(rc.as_str());
                                    if !rc.is_empty() {
                                        let _ = tx.send(Ok(StreamChunk::reasoning(rc))).await;
                                    }
                                }
                                if let Some(content) = choice.delta.content {
                                    response_content
                                        .get_or_insert_with(String::new)
                                        .push_str(content.as_str());
                                    if !content.is_empty() {
                                        let _ = tx.send(Ok(StreamChunk::delta(content))).await;
                                    }
                                }
                                if let Some(tool_calls) = choice.delta.tool_calls {
                                    tool_call_accumulator.ingest(tool_calls);
                                }
                                if let Some(function_call) = choice.delta.function_call {
                                    tool_call_accumulator.ingest(vec![StreamToolCallDelta {
                                        index: Some(0),
                                        id: None,
                                        function: Some(function_call),
                                        name: None,
                                        arguments: None,
                                    }]);
                                }
                                if let Some(reason) = choice.finish_reason {
                                    let chunks = match finish_compatible_stream(
                                        &mut tool_call_accumulator,
                                        provider_name.as_str(),
                                        replay_reasoning_content,
                                        response_content,
                                        response_reasoning_content,
                                        ProviderTermination::from_openai_reason(&reason),
                                    ) {
                                        Ok(chunks) => chunks,
                                        Err(error) => {
                                            let _ = tx.send(Err(error)).await;
                                            return;
                                        }
                                    };
                                    for chunk in chunks {
                                        let _ = tx.send(Ok(chunk)).await;
                                    }
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Err(anyhow!("malformed {} SSE frame: {e}", provider_name)))
                                .await;
                            return;
                        }
                    }
                }
            }

            let error = decoder.finish().err().unwrap_or_else(|| {
                anyhow!(
                    "{} stream ended before a provider terminal marker",
                    provider_name
                )
            });
            let _ = tx.send(Err(error)).await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(chunk_stream))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let request_builder = self.authorized_get(&self.models_url());
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(self.api_error(response).await);
        }

        let api_response: ModelsListResponse = response.json().await?;
        let provider_name = self.name.clone();

        Ok(api_response
            .data
            .into_iter()
            .map(|m| ProviderModelInfo {
                id: m.id.clone(),
                name: None,
                description: None,
                created: m.created,
                provider: provider_name.clone(),
                owned_by: m.owned_by,
                limits: ProviderModelLimits::default(),
                capabilities: ProviderModelCapabilities::default(),
                transcription: None,
                pricing: None,
                active: Some(true),
                family: None,
                lifecycle_status: None,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::prepare_messages_for_provider;
    use crate::traits::Provider;
    use crate::types::{
        AttachmentDataSource, InputTypeSupport, MessageAttachment, MessageContentPart,
        ProviderInputCapabilities, ProviderToolCall, ReasoningConfig, ReasoningEffort,
    };

    fn prepared_for(
        provider: &OpenAiCompatibleProvider,
        messages: &[ChatMessage],
    ) -> crate::attachments::PreparedProviderMessages {
        prepare_messages_for_provider(provider.name(), &provider.capabilities(), messages).unwrap()
    }

    fn text_content(content: &Option<ApiMessageContent>) -> Option<&str> {
        match content {
            Some(ApiMessageContent::Text(text)) => Some(text.as_str()),
            _ => None,
        }
    }

    fn test_provider() -> OpenAiCompatibleProvider {
        OpenAiCompatibleProvider::new(
            "test-provider",
            "https://api.example.com/v1",
            "sk-test-key",
            AuthStyle::Bearer,
        )
    }

    #[test]
    fn creates_with_required_fields() {
        let provider = test_provider();
        assert_eq!(provider.name, "test-provider");
        assert_eq!(provider.base_url, "https://api.example.com/v1");
        assert_eq!(provider.credential, "sk-test-key");
        assert!(!provider.input_types.image.is_supported());
        assert!(!provider.merge_system_into_user);
    }

    #[test]
    fn builder_methods() {
        let mut headers = HashMap::new();
        headers.insert("X-Custom".to_string(), "value".to_string());

        let provider = OpenAiCompatibleProvider::new(
            "custom",
            "https://api.example.com",
            "key",
            AuthStyle::XApiKey,
        )
        .with_vision(true)
        .with_merge_system_into_user(true)
        .with_timeout_secs(60)
        .with_extra_headers(headers)
        .with_user_agent("pioneer/1.0");

        assert!(provider.input_types.image.is_supported());
        assert!(provider.merge_system_into_user);
        assert_eq!(
            provider.timeout_policy.non_stream_request_timeout,
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            provider.extra_headers.get("User-Agent").unwrap(),
            "pioneer/1.0"
        );
    }

    #[test]
    fn chat_completions_url_appended() {
        let provider = test_provider();
        assert_eq!(
            provider.chat_completions_url(),
            "https://api.example.com/v1/chat/completions"
        );
    }

    #[test]
    fn chat_completions_url_already_present() {
        let provider = OpenAiCompatibleProvider::new(
            "test",
            "https://api.example.com/v1/chat/completions",
            "key",
            AuthStyle::Bearer,
        );
        assert_eq!(
            provider.chat_completions_url(),
            "https://api.example.com/v1/chat/completions"
        );
    }

    #[test]
    fn chat_completions_url_trims_trailing_slash() {
        let provider = OpenAiCompatibleProvider::new(
            "test",
            "https://api.example.com/v1/",
            "key",
            AuthStyle::Bearer,
        );
        assert_eq!(
            provider.chat_completions_url(),
            "https://api.example.com/v1/chat/completions"
        );
    }

    #[test]
    fn direct_convert_maps_roles() {
        let provider = test_provider();
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = OpenAiCompatibleProvider::direct_convert(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "system");
        assert_eq!(text_content(&api_messages[0].content), Some("Be helpful"));
        assert_eq!(api_messages[1].role, "user");
        assert_eq!(api_messages[2].role, "assistant");
    }

    #[test]
    fn reasoning_content_replay_is_enabled_by_default_for_assistant_tool_calls() {
        let provider = test_provider();
        let messages = vec![ChatMessage::assistant_tool_calls_with_reasoning(
            None::<String>,
            Some("provider reasoning token"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_skill".to_owned(),
                arguments: "{\"slug\":\"weather\"}".to_owned(),
            }],
        )];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.convert_messages(&prepared).unwrap();

        assert_eq!(
            api_messages[0].reasoning_content.as_deref(),
            Some("provider reasoning token")
        );
        assert!(api_messages[0].content.is_none());
    }

    #[test]
    fn reasoning_content_replay_can_be_disabled() {
        let provider = test_provider().with_reasoning_content_replay(false);
        let messages = vec![ChatMessage::assistant_tool_calls_with_reasoning(
            None::<String>,
            Some("do not serialize when disabled"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_skill".to_owned(),
                arguments: "{\"slug\":\"weather\"}".to_owned(),
            }],
        )];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.convert_messages(&prepared).unwrap();

        assert!(api_messages[0].reasoning_content.is_none());
    }

    #[test]
    fn compatible_provider_replay_state_preserves_present_empty_reasoning() {
        let provider = test_provider();
        let tool_calls = vec![ProviderToolCall {
            id: "call_1".to_owned(),
            name: "read_file".to_owned(),
            arguments: "{\"path\":\"README.md\"}".to_owned(),
        }];
        let replay_state = OpenAiCompatibleProvider::assistant_replay_state(
            provider.name(),
            Some(String::new()),
            Some(String::new()),
            tool_calls.as_slice(),
        );
        let message = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            None::<String>,
            tool_calls,
            Some(replay_state),
        );
        let request = ChatRequest {
            model: "compatible-model".to_owned(),
            messages: vec![message],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };

        let rendered = provider
            .build_chat_request(request, false)
            .expect("an explicitly present empty replay field must remain replayable");

        assert_eq!(rendered.messages[0].reasoning_content.as_deref(), Some(""));
        assert_eq!(text_content(&rendered.messages[0].content), Some(""));
        assert_eq!(
            rendered.messages[0]
                .tool_calls
                .as_ref()
                .expect("tool calls must be replayed")[0]
                .function
                .arguments,
            "{\"path\":\"README.md\"}"
        );
    }

    #[test]
    fn compatible_stream_completion_captures_absent_reasoning_losslessly() {
        let mut tool_calls = StreamToolCallAccumulator::default();
        tool_calls.ingest(vec![StreamToolCallDelta {
            index: Some(0),
            id: Some("call_1".to_owned()),
            function: Some(StreamToolFunctionDelta {
                name: Some("read_file".to_owned()),
                arguments: Some("{\"path\":\"README.md\"}".to_owned()),
            }),
            name: None,
            arguments: None,
        }]);

        let chunks = finish_compatible_stream(
            &mut tool_calls,
            "test-provider",
            true,
            Some(String::new()),
            None,
            ProviderTermination::ToolCalls,
        )
        .unwrap();

        let replay = OpenAiCompatibleProvider::new(
            "test-provider",
            "https://api.example.com/v1",
            "key",
            AuthStyle::Bearer,
        )
        .replay_state_message(
            chunks[0]
                .provider_replay_state
                .as_ref()
                .expect("tool-call response must carry exact compatible replay state"),
        )
        .expect("compatible replay state must decode");
        assert!(replay.reasoning_content.is_none());
        assert_eq!(chunks[1].tool_calls.len(), 1);
        assert!(chunks[2].is_final);
    }

    #[test]
    fn direct_convert_tool_attachment_splits_into_tool_and_user_messages() {
        let provider = test_provider().with_vision(true);
        let mut tool_message =
            ChatMessage::tool_result("call_1", "computer_use", "{\"status\":\"ok\"}");
        tool_message.content_parts.push(MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".to_owned(),
            name: Some("snapshot.png".to_owned()),
            size_bytes: None,
            sha256: None,
            source: AttachmentDataSource::Bytes {
                base64_data:
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+VrWQAAAAASUVORK5CYII="
                        .to_owned(),
            },
            artifact: None,
        }));

        let prepared = prepared_for(&provider, &[ChatMessage::user("hello"), tool_message]);
        let api_messages = OpenAiCompatibleProvider::direct_convert(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(api_messages[1].role, "tool");
        assert_eq!(api_messages[1].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(api_messages[1].name.as_deref(), Some("computer_use"));
        assert_eq!(api_messages[2].role, "user");
        match api_messages[2].content.as_ref() {
            Some(ApiMessageContent::Parts(parts)) => {
                assert!(
                    parts
                        .iter()
                        .any(|part| matches!(part, ApiContentPart::ImageUrl { .. }))
                );
            }
            _ => panic!("expected user parts message for tool attachment"),
        }
    }

    #[test]
    fn direct_convert_file_part_serializes_file_data_as_raw_base64() {
        let provider = test_provider().with_input_capabilities(ProviderInputCapabilities {
            text: true,
            file: InputTypeSupport::native_inline_only(),
            image: InputTypeSupport::disabled(),
            audio: InputTypeSupport::disabled(),
            video: InputTypeSupport::disabled(),
        });
        let file_bytes = [1u8, 2, 3, 4];
        let expected_base64 = BASE64.encode(file_bytes);
        let messages = [ChatMessage::user_parts(vec![MessageContentPart::file(
            MessageAttachment {
                mime_type: "application/pdf".to_owned(),
                name: Some("doc.pdf".to_owned()),
                size_bytes: Some(file_bytes.len() as u64),
                sha256: None,
                source: AttachmentDataSource::Bytes {
                    base64_data: expected_base64.clone(),
                },
                artifact: None,
            },
        )])];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = OpenAiCompatibleProvider::direct_convert(&prepared).unwrap();
        let file_data = match api_messages[0].content.as_ref() {
            Some(ApiMessageContent::Parts(parts)) => parts.iter().find_map(|part| match part {
                ApiContentPart::File { file } => file.file_data.clone(),
                _ => None,
            }),
            _ => None,
        };

        assert_eq!(file_data.as_deref(), Some(expected_base64.as_str()));
    }

    #[test]
    fn merge_system_into_first_user_message() {
        let provider = test_provider().with_merge_system_into_user(true);
        let messages = vec![
            ChatMessage::system("You are a helpful assistant."),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.merge_system_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 2);
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(
            text_content(&api_messages[0].content),
            Some("You are a helpful assistant.\n\nHello")
        );
        assert_eq!(api_messages[1].role, "assistant");
    }

    #[test]
    fn merge_multiple_system_messages() {
        let provider = test_provider().with_merge_system_into_user(true);
        let messages = vec![
            ChatMessage::system("Rule 1"),
            ChatMessage::system("Rule 2"),
            ChatMessage::user("Hello"),
        ];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.merge_system_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 1);
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(
            text_content(&api_messages[0].content),
            Some("Rule 1\nRule 2\n\nHello")
        );
    }

    #[test]
    fn merge_system_only_no_user() {
        let provider = test_provider().with_merge_system_into_user(true);
        let messages = vec![ChatMessage::system("System only")];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.merge_system_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 1);
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(text_content(&api_messages[0].content), Some("System only"));
    }

    #[test]
    fn merge_no_system_messages() {
        let provider = test_provider().with_merge_system_into_user(true);
        let messages = vec![ChatMessage::user("Hello"), ChatMessage::assistant("Hi!")];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.merge_system_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 2);
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(text_content(&api_messages[0].content), Some("Hello"));
    }

    #[test]
    fn merge_mode_uses_compiled_prompt_full_text() {
        let provider = test_provider().with_merge_system_into_user(true);
        let request = ChatRequest {
            model: "test-model".to_owned(),
            messages: vec![
                ChatMessage::system("legacy system prompt should be ignored"),
                ChatMessage::user("Hello"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: Some(crate::types::CompiledPromptPayload {
                stable_system_text: "Stable rules".to_owned(),
                dynamic_system_text: "Dynamic runtime".to_owned(),
                boundary_marker: "<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->".to_owned(),
                full_system_text:
                    "Stable rules\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic runtime"
                        .to_owned(),
            }),
        };

        let rendered = request.rendered_messages_with_compiled_prompt();
        let prepared = prepared_for(&provider, rendered.as_slice());
        let api_messages = provider.convert_messages(&prepared).unwrap();
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(
            text_content(&api_messages[0].content),
            Some("Stable rules\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic runtime\n\nHello")
        );
    }

    #[test]
    fn api_request_serializes_correctly() {
        let request = ApiChatRequest {
            model: "gpt-4".into(),
            messages: vec![
                ApiMessage {
                    role: "system".into(),
                    content: Some(ApiMessageContent::Text("You are helpful".into())),
                    reasoning_content: None,
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ApiMessage {
                    role: "user".into(),
                    content: Some(ApiMessageContent::Text("Hello".into())),
                    reasoning_content: None,
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
            ],
            temperature: Some(0.7),
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            stream: false,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("gpt-4"));
        assert!(json.contains("\"role\":\"system\""));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"stream\":false"));
        assert!(!json.contains("max_tokens"));
    }

    #[test]
    fn compatible_request_omits_reasoning_fields_even_when_selected() {
        let provider = test_provider();
        let request = ChatRequest {
            model: "custom-compatible-model".to_owned(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
            compiled_prompt: None,
        };

        let api_request = provider
            .build_chat_request(request, false)
            .expect("compatible request should render");
        let json = serde_json::to_value(&api_request).unwrap();

        assert!(json.get("reasoning_effort").is_none());
        assert!(json.get("reasoning").is_none());
        assert!(json.get("output_config").is_none());
        assert!(json.get("thinkingConfig").is_none());
    }

    #[test]
    fn api_response_deserializes() {
        let json = r#"{"choices":[{"message":{"content":"Hello from API"}}]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.choices.len(), 1);
        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Hello from API")
        );
    }

    #[test]
    fn api_response_with_usage() {
        let json = r#"{
            "choices": [{"message": {"content": "Hello"}}],
            "usage": {"prompt_tokens": 42, "completion_tokens": 15}
        }"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        let usage = response.usage.unwrap();
        assert_eq!(usage.prompt_tokens, Some(42));
        assert_eq!(usage.completion_tokens, Some(15));
    }

    #[test]
    fn api_response_empty_choices() {
        let json = r#"{"choices":[]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert!(response.choices.is_empty());
    }

    #[test]
    fn stream_response_deserializes_delta() {
        let json = r#"{"choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.choices.len(), 1);
        assert_eq!(response.choices[0].delta.content.as_deref(), Some("Hello"));
        assert!(response.choices[0].finish_reason.is_none());
    }

    #[test]
    fn stream_response_deserializes_finish() {
        let json = r#"{"choices":[{"delta":{"content":null},"finish_reason":"stop"}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.choices[0].finish_reason.as_deref(), Some("stop"));
    }

    #[test]
    fn stream_response_deserializes_error_envelope_without_delta() {
        let json = r#"{"error":{"message":"upstream failed","type":"provider_error","code":"overloaded"},"choices":[{"finish_reason":"error"}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();

        assert_eq!(
            response.error.unwrap().description(),
            "upstream failed, type=provider_error, code=overloaded"
        );
        assert_eq!(response.choices[0].finish_reason.as_deref(), Some("error"));
        assert!(response.choices[0].delta.content.is_none());
    }

    #[test]
    fn provider_name() {
        use crate::traits::Provider;
        let provider = test_provider();
        assert_eq!(provider.name(), "test-provider");
    }

    #[test]
    fn provider_capabilities_default() {
        use crate::traits::Provider;
        let provider = test_provider();
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(!caps.vision);
    }

    #[test]
    fn provider_capabilities_with_vision() {
        use crate::traits::Provider;
        let provider = test_provider().with_vision(true);
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }
}
