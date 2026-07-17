use crate::attachments::{
    PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes,
    ensure_no_unrendered_attachments, prepare_messages_for_provider,
};
use crate::reasoning_registry;
use crate::types::{
    ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
    ProviderInputCapabilities, ProviderTimeoutPolicy, ProviderToolCall, ReasoningConfig, Role,
    StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

const BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 8192;

pub struct AnthropicProvider {
    api_key: String,
    base_url: String,
    timeout_policy: ProviderTimeoutPolicy,
    client: Client,
}

// ── Anthropic API request types ─────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ApiChatRequest {
    model: String,
    messages: Vec<ApiMessage>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<AnthropicToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<AnthropicToolChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_config: Option<AnthropicOutputConfig>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    stream: bool,
}

#[derive(Debug, Serialize)]
struct AnthropicOutputConfig {
    effort: String,
}

#[derive(Debug, Serialize)]
struct ApiMessage {
    role: String,
    content: Vec<ApiMessageContentBlock>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ApiMessageContentBlock {
    Text {
        text: String,
    },
    Image {
        source: AnthropicMediaSource,
    },
    Document {
        source: AnthropicMediaSource,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AnthropicMediaSource {
    Base64 { media_type: String, data: String },
    File { file_id: String },
}

#[derive(Debug, Clone, Serialize)]
struct AnthropicToolDefinition {
    name: String,
    description: String,
    input_schema: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AnthropicToolChoice {
    Auto,
    Any,
    Tool { name: String },
}

// ── Anthropic API response types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ApiChatResponse {
    content: Vec<ContentBlock>,
    #[serde(default)]
    usage: Option<ApiUsage>,
}

#[derive(Debug, Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ApiUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

// ── SSE streaming response types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    /// Content block index for block-level events.
    #[serde(default)]
    index: Option<usize>,
    #[serde(default)]
    delta: Option<StreamDelta>,
    /// Present on `content_block_start` events — carries the block type.
    #[serde(default)]
    content_block: Option<StreamContentBlock>,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    #[serde(default)]
    text: Option<String>,
    /// Present on thinking block deltas.
    #[serde(default)]
    thinking: Option<String>,
    /// Present on tool use deltas (`input_json_delta`).
    #[serde(default)]
    partial_json: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<serde_json::Value>,
}

// ── List models response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ModelsListResponse {
    data: Vec<AnthropicModelEntry>,
}

#[derive(Debug, Deserialize)]
struct AnthropicModelEntry {
    id: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    max_tokens: Option<u64>,
    #[serde(default)]
    max_input_tokens: Option<u64>,
}

// ── Implementation ──────────────────────────────────────────────────────────

impl AnthropicProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_timeout_policy(api_key, ProviderTimeoutPolicy::default())
    }

    pub fn with_timeout_policy(
        api_key: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self::with_base_url_and_timeout_policy(api_key, BASE_URL, timeout_policy)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self::with_base_url_and_timeout_policy(api_key, base_url, ProviderTimeoutPolicy::default())
    }

    pub fn with_base_url_and_timeout_policy(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        }
    }

    fn convert_media_source(
        attachment: &crate::attachments::PreparedAttachment,
    ) -> Result<AnthropicMediaSource> {
        match &attachment.source {
            PreparedAttachmentSource::Reference { reference } => Ok(AnthropicMediaSource::File {
                file_id: reference.clone(),
            }),
            _ => Ok(AnthropicMediaSource::Base64 {
                media_type: attachment.mime_type.clone(),
                data: BASE64.encode(attachment_bytes(attachment)?),
            }),
        }
    }

    /// Extract system messages into a single system prompt and return
    /// the remaining non-system messages converted to API format.
    fn prepare_messages(
        prepared: &PreparedProviderMessages,
    ) -> Result<(Option<String>, Vec<ApiMessage>)> {
        let system_parts: Vec<&str> = prepared
            .messages
            .iter()
            .filter(|m| m.role == Role::System)
            .map(|m| m.content.as_str())
            .collect();

        let system = if system_parts.is_empty() {
            None
        } else {
            Some(system_parts.join("\n\n"))
        };

        let mut api_messages = Vec::new();
        for (message_index, m) in prepared.messages.iter().enumerate() {
            if m.role == Role::System {
                continue;
            }

            let role = match m.role {
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "user",
                Role::System => unreachable!(),
            };

            let mut content = Vec::new();

            match m.role {
                Role::Tool => {
                    let tool_use_id = m
                        .tool_call_id
                        .clone()
                        .or_else(|| m.name.clone())
                        .unwrap_or_else(|| "tool".to_owned());
                    content.push(ApiMessageContentBlock::ToolResult {
                        tool_use_id,
                        content: m.content.clone(),
                        is_error: None,
                    });
                }
                _ => {
                    if !m.content.is_empty() {
                        content.push(ApiMessageContentBlock::Text {
                            text: m.content.clone(),
                        });
                    }

                    if let Some(tool_calls) = m.tool_calls.as_ref() {
                        for call in tool_calls {
                            content.push(ApiMessageContentBlock::ToolUse {
                                id: call.id.clone(),
                                name: call.name.clone(),
                                input: parse_json_or_string(call.arguments.as_str()),
                            });
                        }
                    }

                    let attachments = prepared
                        .attachments_for_message(message_index)
                        .collect::<Vec<_>>();
                    for attachment in attachments {
                        match attachment.kind {
                            InputContentType::Image => {
                                content.push(ApiMessageContentBlock::Image {
                                    source: Self::convert_media_source(attachment)?,
                                });
                            }
                            InputContentType::File => {
                                content.push(ApiMessageContentBlock::Document {
                                    source: Self::convert_media_source(attachment)?,
                                });
                            }
                            _ => {
                                return Err(anyhow!(
                                    "provider `anthropic` does not support {:?} attachments in messages API",
                                    attachment.kind
                                ));
                            }
                        }
                    }
                }
            }

            api_messages.push(ApiMessage {
                role: role.to_owned(),
                content,
            });
        }

        Ok((system, api_messages))
    }

    fn convert_tools(tools: &[ToolDefinition]) -> Vec<AnthropicToolDefinition> {
        tools
            .iter()
            .map(|tool| AnthropicToolDefinition {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema: tool.parameters.clone(),
            })
            .collect()
    }

    fn convert_tool_choice(choice: ToolChoice) -> AnthropicToolChoice {
        match choice {
            ToolChoice::Auto => AnthropicToolChoice::Auto,
            ToolChoice::None => AnthropicToolChoice::Auto,
            ToolChoice::Required => AnthropicToolChoice::Any,
            ToolChoice::Tool { name } => AnthropicToolChoice::Tool { name },
        }
    }

    fn output_config(reasoning: Option<ReasoningConfig>) -> Option<AnthropicOutputConfig> {
        match reasoning {
            Some(ReasoningConfig::Effort(effort)) => Some(AnthropicOutputConfig {
                effort: effort.as_str().to_owned(),
            }),
            Some(ReasoningConfig::Disabled) | None => None,
        }
    }

    fn messages_url(&self) -> String {
        format!("{}/v1/messages", self.base_url)
    }

    fn models_url(&self) -> String {
        format!("{}/v1/models", self.base_url)
    }

    async fn api_error(response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read error body>".into());
        anyhow!("Anthropic API error ({status}): {body}")
    }
}

fn parse_json_or_string(raw: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(raw)
        .unwrap_or_else(|_| serde_json::Value::String(raw.to_owned()))
}

#[derive(Debug)]
struct PendingToolUse {
    id: String,
    name: String,
    arguments: String,
    has_partial_json: bool,
}

impl PendingToolUse {
    fn finalize(self) -> ProviderToolCall {
        let arguments = if self.arguments.trim().is_empty() {
            "{}".to_owned()
        } else {
            match serde_json::from_str::<serde_json::Value>(self.arguments.as_str()) {
                Ok(value) => serde_json::to_string(&value).unwrap_or(self.arguments),
                Err(_) => self.arguments,
            }
        };

        ProviderToolCall {
            id: self.id,
            name: self.name,
            arguments,
        }
    }
}

#[async_trait]
impl crate::traits::Provider for AnthropicProvider {
    fn name(&self) -> &str {
        "anthropic"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: true,
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::native_inline_only(),
                image: InputTypeSupport::native_inline_only(),
                audio: InputTypeSupport::disabled(),
                video: InputTypeSupport::disabled(),
            },
        }
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let prepared = prepare_messages_for_provider(
            self.name(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let (system, messages) = Self::prepare_messages(&prepared)?;

        let api_request = ApiChatRequest {
            model: request.model,
            messages,
            max_tokens: request.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            temperature: request.temperature,
            system,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            output_config: Self::output_config(request.reasoning),
            stream: false,
        };

        let request_builder = self
            .client
            .post(self.messages_url())
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("content-type", "application/json")
            .json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: ApiChatResponse = response.json().await?;
        let usage = api_response.usage.map(|u| TokenUsage {
            input_tokens: u.input_tokens,
            output_tokens: u.output_tokens,
        });

        let mut text_parts = Vec::new();
        let mut thinking_parts = Vec::new();
        let mut tool_calls = Vec::new();

        for block in api_response.content {
            match block.block_type.as_str() {
                "text" => {
                    if let Some(t) = block.text {
                        text_parts.push(t);
                    }
                }
                "thinking" => {
                    if let Some(t) = block.text {
                        thinking_parts.push(t);
                    }
                }
                "tool_use" => {
                    if let (Some(id), Some(name), Some(input)) = (block.id, block.name, block.input)
                    {
                        tool_calls.push(ProviderToolCall {
                            id,
                            name,
                            arguments: serde_json::to_string(&input)
                                .unwrap_or_else(|_| "{}".to_owned()),
                        });
                    }
                }
                _ => {}
            }
        }

        let text = text_parts.join("");
        let reasoning_content = if thinking_parts.is_empty() {
            None
        } else {
            Some(thinking_parts.join(""))
        };

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(anyhow!("no response from Anthropic"));
        }

        Ok(ChatResponse {
            text,
            usage,
            reasoning_content,
            tool_calls,
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let prepared = prepare_messages_for_provider(
            self.name(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let (system, messages) = Self::prepare_messages(&prepared)?;

        let api_request = ApiChatRequest {
            model: request.model,
            messages,
            max_tokens: request.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            temperature: request.temperature,
            system,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            output_config: Self::output_config(request.reasoning),
            stream: true,
        };

        let request_builder = self
            .client
            .post(self.messages_url())
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("content-type", "application/json")
            .json(&api_request);
        let response =
            crate::http::send_stream_request(request_builder, self.timeout_policy).await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let byte_stream = response.bytes_stream();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            use std::collections::{HashMap, HashSet};

            let mut buffer = String::new();
            let mut thinking_blocks = HashSet::new();
            let mut pending_tool_uses: HashMap<usize, PendingToolUse> = HashMap::new();

            tokio::pin!(byte_stream);

            while let Some(result) = byte_stream.next().await {
                let bytes = match result {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        let _ = tx.send(Err(anyhow!(e))).await;
                        return;
                    }
                };

                let text = String::from_utf8(bytes.to_vec()).unwrap_or_default();
                buffer.push_str(&text);

                while let Some(pos) = buffer.find('\n') {
                    let line = buffer[..pos].trim().to_string();
                    buffer = buffer[pos + 1..].to_string();

                    if line.is_empty() {
                        continue;
                    }

                    let Some(data) = line.strip_prefix("data: ") else {
                        continue;
                    };

                    match serde_json::from_str::<StreamEvent>(data) {
                        Ok(event) => {
                            if event.event_type == "message_stop" {
                                let remaining_calls = pending_tool_uses
                                    .drain()
                                    .map(|(_, call)| call.finalize())
                                    .collect::<Vec<_>>();
                                if !remaining_calls.is_empty() {
                                    let _ =
                                        tx.send(Ok(StreamChunk::tool_calls(remaining_calls))).await;
                                }
                                let _ = tx.send(Ok(StreamChunk::final_chunk())).await;
                                return;
                            }

                            if event.event_type == "content_block_start" {
                                let index = event.index.unwrap_or(0);
                                if let Some(block) = event.content_block.as_ref() {
                                    match block.block_type.as_str() {
                                        "thinking" => {
                                            thinking_blocks.insert(index);
                                        }
                                        "tool_use" => {
                                            thinking_blocks.remove(&index);
                                            if let (Some(id), Some(name)) =
                                                (block.id.as_ref(), block.name.as_ref())
                                            {
                                                pending_tool_uses.insert(
                                                    index,
                                                    PendingToolUse {
                                                        id: id.clone(),
                                                        name: name.clone(),
                                                        arguments: block
                                                            .input
                                                            .as_ref()
                                                            .map(|input| {
                                                                serde_json::to_string(&input)
                                                                    .unwrap_or_else(|_| {
                                                                        "{}".to_owned()
                                                                    })
                                                            })
                                                            .unwrap_or_default(),
                                                        has_partial_json: false,
                                                    },
                                                );
                                            }
                                        }
                                        _ => {
                                            thinking_blocks.remove(&index);
                                        }
                                    }
                                }
                            }

                            if event.event_type == "content_block_stop" {
                                let index = event.index.unwrap_or(0);
                                thinking_blocks.remove(&index);
                                if let Some(call) = pending_tool_uses.remove(&index) {
                                    let _ = tx
                                        .send(Ok(StreamChunk::tool_calls(vec![call.finalize()])))
                                        .await;
                                }
                            }

                            if event.event_type == "content_block_delta" {
                                if let Some(delta) = event.delta {
                                    let index = event.index.unwrap_or(0);
                                    // Thinking block deltas use the `thinking` field
                                    if thinking_blocks.contains(&index) {
                                        if let Some(thinking) = delta.thinking {
                                            if !thinking.is_empty() {
                                                let _ = tx
                                                    .send(Ok(StreamChunk::reasoning(thinking)))
                                                    .await;
                                            }
                                        }
                                    } else if let Some(partial_json) = delta.partial_json {
                                        if let Some(call) = pending_tool_uses.get_mut(&index) {
                                            if !call.has_partial_json {
                                                call.arguments.clear();
                                                call.has_partial_json = true;
                                            }
                                            call.arguments.push_str(partial_json.as_str());
                                        }
                                    } else if let Some(text) = delta.text {
                                        if !text.is_empty() {
                                            let _ = tx.send(Ok(StreamChunk::delta(text))).await;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!("failed to parse Anthropic SSE chunk: {e}");
                        }
                    }
                }
            }

            let remaining_calls = pending_tool_uses
                .drain()
                .map(|(_, call)| call.finalize())
                .collect::<Vec<_>>();
            if !remaining_calls.is_empty() {
                let _ = tx.send(Ok(StreamChunk::tool_calls(remaining_calls))).await;
            }
            let _ = tx.send(Ok(StreamChunk::final_chunk())).await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(chunk_stream))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let request_builder = self
            .client
            .get(self.models_url())
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: ModelsListResponse = response.json().await?;

        Ok(api_response
            .data
            .into_iter()
            .map(provider_model_from_anthropic_model_entry)
            .collect())
    }
}

fn provider_model_from_anthropic_model_entry(m: AnthropicModelEntry) -> ProviderModelInfo {
    let created = m.created_at.as_ref().and_then(|s| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.timestamp())
    });
    let mut capabilities = ProviderModelCapabilities::default();
    reasoning_registry::apply_reasoning_capabilities("anthropic", m.id.as_str(), &mut capabilities);

    ProviderModelInfo {
        id: m.id.clone(),
        name: m.display_name,
        description: None,
        created,
        provider: "anthropic".to_owned(),
        owned_by: Some("anthropic".to_owned()),
        limits: ProviderModelLimits {
            max_input_tokens: m.max_input_tokens,
            max_output_tokens: m.max_tokens,
            context_window: match (m.max_input_tokens, m.max_tokens) {
                (Some(i), Some(o)) => Some(i + o),
                _ => None,
            },
        },
        capabilities,
        transcription: None,
        pricing: None,
        active: Some(true),
        family: None,
        lifecycle_status: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::prepare_messages_for_provider;
    use crate::traits::Provider;
    use crate::types::{ChatMessage, CompiledPromptPayload, ReasoningConfig, ReasoningEffort};

    fn render_messages(messages: &[ChatMessage]) -> (Option<String>, Vec<ApiMessage>) {
        let provider = AnthropicProvider::new("test-key");
        let prepared =
            prepare_messages_for_provider(provider.name(), &provider.capabilities(), messages)
                .expect("prepare_messages_for_provider should succeed");
        AnthropicProvider::prepare_messages(&prepared)
            .expect("anthropic message rendering should succeed")
    }

    #[test]
    fn creates_with_api_key() {
        let provider = AnthropicProvider::new("sk-ant-test-key");
        assert_eq!(provider.api_key, "sk-ant-test-key");
        assert_eq!(provider.base_url, BASE_URL);
    }

    #[test]
    fn creates_with_custom_base_url() {
        let provider = AnthropicProvider::with_base_url("key", "http://localhost:9090");
        assert_eq!(provider.base_url, "http://localhost:9090");
    }

    #[test]
    fn messages_url_built_correctly() {
        let provider = AnthropicProvider::new("key");
        assert_eq!(
            provider.messages_url(),
            "https://api.anthropic.com/v1/messages"
        );
    }

    #[test]
    fn anthropic_reasoning_registry_exposes_supported_opus_efforts() {
        let reasoning =
            reasoning_registry::reasoning_capabilities_for_model("anthropic", "claude-opus-4-8")
                .expect("opus 4.8 reasoning metadata");

        assert_eq!(reasoning.supported, Some(true));
        assert_eq!(
            reasoning.effort_options,
            vec!["low", "medium", "high", "xhigh", "max"]
        );
        assert_eq!(reasoning.default_effort.as_deref(), Some("high"));
    }

    #[test]
    fn anthropic_reasoning_registry_exposes_supported_sonnet_efforts() {
        let reasoning =
            reasoning_registry::reasoning_capabilities_for_model("anthropic", "claude-sonnet-4-6")
                .expect("sonnet 4.6 reasoning metadata");

        assert_eq!(reasoning.supported, Some(true));
        assert_eq!(
            reasoning.effort_options,
            vec!["low", "medium", "high", "max"]
        );
    }

    #[test]
    fn anthropic_reasoning_registry_leaves_unknown_models_unset() {
        assert!(
            reasoning_registry::reasoning_capabilities_for_model("anthropic", "claude-sonnet-3-7")
                .is_none()
        );
    }

    #[test]
    fn anthropic_model_list_fixture_normalizes_reasoning_capabilities() {
        let response: ModelsListResponse = serde_json::from_str(
            r#"{
                "data": [
                    {
                        "id": "claude-opus-4-8",
                        "display_name": "Claude Opus 4.8",
                        "created_at": "2026-01-01T00:00:00Z",
                        "max_input_tokens": 200000,
                        "max_tokens": 64000
                    },
                    {
                        "id": "claude-sonnet-3-7",
                        "display_name": "Claude Sonnet 3.7"
                    }
                ]
            }"#,
        )
        .expect("fixture response");
        let models = response
            .data
            .into_iter()
            .map(provider_model_from_anthropic_model_entry)
            .collect::<Vec<_>>();

        let reasoning = models[0]
            .capabilities
            .reasoning
            .as_ref()
            .expect("supported reasoning model");
        assert_eq!(
            reasoning.effort_options,
            vec!["low", "medium", "high", "xhigh", "max"]
        );
        assert_eq!(models[0].capabilities.thinking, Some(true));
        assert_eq!(models[0].limits.context_window, Some(264000));

        assert!(models[1].capabilities.reasoning.is_none());
    }

    #[test]
    fn prepare_messages_extracts_system() {
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let (system, api_messages) = render_messages(&messages);

        assert_eq!(system.as_deref(), Some("Be helpful"));
        assert_eq!(api_messages.len(), 2);
        assert_eq!(api_messages[0].role, "user");
        assert!(matches!(
            api_messages[0].content.first(),
            Some(ApiMessageContentBlock::Text { text }) if text == "Hello"
        ));
        assert_eq!(api_messages[1].role, "assistant");
        assert!(matches!(
            api_messages[1].content.first(),
            Some(ApiMessageContentBlock::Text { text }) if text == "Hi!"
        ));
    }

    #[test]
    fn prepare_messages_joins_multiple_system() {
        let messages = vec![
            ChatMessage::system("First instruction"),
            ChatMessage::system("Second instruction"),
            ChatMessage::user("Hello"),
        ];

        let (system, api_messages) = render_messages(&messages);

        assert_eq!(
            system.as_deref(),
            Some("First instruction\n\nSecond instruction")
        );
        assert_eq!(api_messages.len(), 1);
    }

    #[test]
    fn prepare_messages_no_system() {
        let messages = vec![ChatMessage::user("Hello")];

        let (system, api_messages) = render_messages(&messages);

        assert!(system.is_none());
        assert_eq!(api_messages.len(), 1);
    }

    #[test]
    fn compiled_prompt_payload_overrides_raw_system_messages() {
        let request = ChatRequest {
            model: "claude-sonnet-4-20250514".into(),
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
            compiled_prompt: Some(CompiledPromptPayload {
                stable_system_text: "Stable rules".to_owned(),
                dynamic_system_text: "Dynamic runtime".to_owned(),
                boundary_marker: "<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->".to_owned(),
                full_system_text:
                    "Stable rules\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic runtime"
                        .to_owned(),
            }),
        };

        let rendered_messages = request.rendered_messages_with_compiled_prompt();
        let (system, api_messages) = render_messages(&rendered_messages);
        assert_eq!(
            system.as_deref(),
            Some("Stable rules\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic runtime")
        );
        assert_eq!(api_messages.len(), 1);
        assert_eq!(api_messages[0].role, "user");
    }

    #[test]
    fn api_request_serializes_correctly() {
        let request = ApiChatRequest {
            model: "claude-sonnet-4-20250514".into(),
            messages: vec![ApiMessage {
                role: "user".into(),
                content: vec![ApiMessageContentBlock::Text {
                    text: "Hello".into(),
                }],
            }],
            max_tokens: 8192,
            temperature: Some(0.7),
            system: Some("Be helpful".into()),
            tools: None,
            tool_choice: None,
            output_config: None,
            stream: false,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("claude-sonnet-4-20250514"));
        assert!(json.contains("\"role\":\"user\""));
        assert!(json.contains("\"max_tokens\":8192"));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"system\":\"Be helpful\""));
        assert!(!json.contains("\"output_config\""));
        // stream=false should be omitted via skip_serializing_if
        assert!(!json.contains("\"stream\""));
    }

    #[test]
    fn api_request_serializes_stream_true() {
        let request = ApiChatRequest {
            model: "claude-sonnet-4-20250514".into(),
            messages: vec![ApiMessage {
                role: "user".into(),
                content: vec![ApiMessageContentBlock::Text {
                    text: "Hello".into(),
                }],
            }],
            max_tokens: 8192,
            temperature: None,
            system: None,
            tools: None,
            tool_choice: None,
            output_config: None,
            stream: true,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"stream\":true"));
        assert!(!json.contains("\"temperature\""));
        assert!(!json.contains("\"system\""));
    }

    #[test]
    fn api_request_serializes_reasoning_effort_under_output_config() {
        assert!(AnthropicProvider::output_config(Some(ReasoningConfig::disabled())).is_none());
        assert_eq!(
            AnthropicProvider::output_config(Some(ReasoningConfig::effort(ReasoningEffort::None)))
                .expect("explicit none effort should serialize")
                .effort,
            "none"
        );

        let request = ApiChatRequest {
            model: "claude-sonnet-4-20250514".into(),
            messages: vec![ApiMessage {
                role: "user".into(),
                content: vec![ApiMessageContentBlock::Text {
                    text: "Hello".into(),
                }],
            }],
            max_tokens: 8192,
            temperature: None,
            system: None,
            tools: None,
            tool_choice: None,
            output_config: AnthropicProvider::output_config(Some(ReasoningConfig::effort(
                ReasoningEffort::High,
            ))),
            stream: false,
        };

        let json = serde_json::to_value(&request).unwrap();

        assert_eq!(json["output_config"]["effort"], "high");
        assert!(json.get("thinking").is_none());
    }

    #[test]
    fn api_response_deserializes() {
        let json = r#"{
            "content": [{"type": "text", "text": "Hello from Claude"}],
            "usage": {"input_tokens": 10, "output_tokens": 25}
        }"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.content.len(), 1);
        assert_eq!(
            response.content[0].text.as_deref(),
            Some("Hello from Claude")
        );
        let usage = response.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.output_tokens, Some(25));
    }

    #[test]
    fn api_response_without_usage() {
        let json = r#"{"content": [{"type": "text", "text": "Hi"}]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert!(response.usage.is_none());
        assert_eq!(response.content[0].text.as_deref(), Some("Hi"));
    }

    #[test]
    fn api_response_empty_content() {
        let json = r#"{"content": []}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert!(response.content.is_empty());
    }

    #[test]
    fn stream_event_deserializes_content_block_delta() {
        let json =
            r#"{"type": "content_block_delta", "delta": {"type": "text_delta", "text": "Hello"}}"#;
        let event: StreamEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "content_block_delta");
        assert_eq!(event.delta.unwrap().text.as_deref(), Some("Hello"));
    }

    #[test]
    fn stream_event_deserializes_message_stop() {
        let json = r#"{"type": "message_stop"}"#;
        let event: StreamEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "message_stop");
        assert!(event.delta.is_none());
    }

    #[test]
    fn provider_name() {
        let provider = AnthropicProvider::new("key");
        assert_eq!(provider.name(), "anthropic");
    }

    #[test]
    fn provider_capabilities() {
        let provider = AnthropicProvider::new("key");
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }

    #[tokio::test]
    async fn chat_fails_without_valid_key() {
        let provider = AnthropicProvider::new("sk-ant-invalid-key");
        let request = ChatRequest {
            model: "claude-sonnet-4-20250514".into(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };

        let result = provider.chat(request).await;
        assert!(result.is_err());
    }
}
