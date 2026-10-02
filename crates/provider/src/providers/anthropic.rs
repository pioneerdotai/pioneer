use crate::attachments::{
    PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes,
    ensure_no_unrendered_attachments, prepare_messages_for_provider_async,
};
use crate::reasoning_registry;
use crate::tools::stream::{IncrementalLineDecoder, sse_data};
use crate::types::{
    ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
    ProviderInputCapabilities, ProviderReplayState, ProviderTermination, ProviderTimeoutPolicy,
    ProviderToolCall, ReasoningConfig, Role, StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
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

fn append_native_string(block: &mut serde_json::Value, key: &str, delta: &str) {
    let mut text = block
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();
    text.push_str(delta);
    block[key] = serde_json::Value::String(text);
}

pub(crate) const BASE_URL: &str = "https://api.anthropic.com";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 8192;

pub struct AnthropicProvider {
    replay_authority: String,
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
    Thinking {
        thinking: String,
        signature: String,
    },
    RedactedThinking {
        data: String,
    },
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
    /// Complete provider-owned block; v2 replay preserves unknown native fields.
    #[serde(untagged)]
    Native(serde_json::Value),
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
    stop_reason: Option<String>,
    #[serde(default)]
    usage: Option<ApiUsage>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thinking: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input: Option<serde_json::Value>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ApiUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
}

impl ApiUsage {
    fn normalized(&self) -> TokenUsage {
        TokenUsage {
            input_tokens: self.input_tokens.and_then(|input| {
                input
                    .checked_add(self.cache_creation_input_tokens?)?
                    .checked_add(self.cache_read_input_tokens?)
            }),
            output_tokens: self.output_tokens,
        }
    }
}

// ── SSE streaming response types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    usage: Option<ApiUsage>,
    #[serde(default)]
    message: Option<StreamMessage>,
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
struct StreamMessage {
    #[serde(default)]
    usage: Option<ApiUsage>,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    text: Option<String>,
    /// Present on thinking block deltas.
    #[serde(default)]
    thinking: Option<String>,
    /// Anthropic sends the opaque thinking signature immediately before the
    /// corresponding thinking block stops.
    #[serde(default)]
    signature: Option<String>,
    /// Present on tool use deltas (`input_json_delta`).
    #[serde(default)]
    partial_json: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thinking: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
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
            replay_authority: pioneer_protocol::generate_id(32),
            api_key: api_key.into(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
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

    /// Keep full native blocks alongside the readable response projection.
    fn decode_response(api_response: ApiChatResponse) -> Result<ChatResponse> {
        let termination = api_response
            .stop_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| ProviderTermination::Unknown("missing_stop_reason".to_owned()));
        let usage = api_response.usage.map(|u| u.normalized());

        let mut text_parts = Vec::new();
        let mut thinking_parts = Vec::new();
        let mut tool_calls = Vec::new();
        let mut replay_blocks = Vec::new();

        for block in api_response.content {
            replay_blocks.push(serde_json::to_value(&block)?);
            match block.block_type.as_str() {
                "text" => {
                    if let Some(t) = block.text {
                        text_parts.push(t);
                    }
                }
                "thinking" => {
                    let thinking = block.thinking.or(block.text).unwrap_or_default();
                    if !thinking.is_empty() {
                        thinking_parts.push(thinking.clone());
                    }
                }
                "redacted_thinking" => {}
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
        let provider_replay_state = if replay_blocks.is_empty() {
            None
        } else {
            Some(ProviderReplayState::new(
                "anthropic",
                serde_json::json!({ "schema_version": 2, "blocks": replay_blocks }),
            ))
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
            termination,
            reasoning_content,
            tool_calls,
            provider_replay_state,
        })
    }
    /// Extract system messages and render the remaining native history.
    fn build_native_request(
        &self,
        request: &ChatRequest,
        prepared: &PreparedProviderMessages,
        stream: bool,
    ) -> Result<ApiChatRequest> {
        if self.base_url != BASE_URL
            && prepared.messages.iter().any(|message| {
                message.provider_replay_state.as_ref().is_some_and(|state| {
                    crate::continuation::retention(state)
                        != crate::continuation::Retention::Ordinary
                })
            })
        {
            anyhow::bail!(
                "native thinking replay through a custom Messages relay lacks documented prefix/account authority"
            );
        }
        let (system, messages) = Self::prepare_messages(prepared)?;
        let body = ApiChatRequest {
            model: request.model.clone(),
            messages,
            max_tokens: request.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            temperature: request.temperature,
            system,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.clone().map(Self::convert_tool_choice),
            output_config: Self::output_config(request.reasoning),
            stream,
        };
        crate::continuation::validate_prefix(
            &serde_json::to_value(&body)?,
            prepared
                .messages
                .iter()
                .filter(|m| m.role != Role::System)
                .enumerate()
                .filter_map(|(i, m)| m.provider_replay_state.clone().map(|s| (i, s))),
            &request.model,
            &self.replay_authority,
        )?;
        Ok(body)
    }

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
                    if m.role == Role::Assistant
                        && let Some(state) = m.provider_replay_state.as_ref()
                    {
                        let payload = state.payload_for("anthropic").ok_or_else(|| {
                            anyhow!(
                                "provider replay state `{}` cannot be rendered by `anthropic`",
                                state.provider
                            )
                        })?;
                        if payload
                            .get("schema_version")
                            .is_some_and(|version| version != 1 && version != 2)
                        {
                            return Err(anyhow!("unsupported anthropic replay schema version"));
                        }
                        let blocks = payload
                            .get("blocks")
                            .cloned()
                            .ok_or_else(|| anyhow!("anthropic replay state is missing `blocks`"))?;
                        if payload
                            .get("schema_version")
                            .and_then(serde_json::Value::as_u64)
                            == Some(2)
                        {
                            let blocks = blocks
                                .as_array()
                                .ok_or_else(|| anyhow!("invalid anthropic replay blocks"))?;
                            content
                                .extend(blocks.iter().cloned().map(ApiMessageContentBlock::Native));
                            api_messages.push(ApiMessage {
                                role: role.to_owned(),
                                content,
                            });
                            continue;
                        }
                        content.extend(
                            serde_json::from_value::<Vec<ApiMessageContentBlock>>(blocks).map_err(
                                |error| anyhow!("invalid anthropic replay state: {error}"),
                            )?,
                        );
                    }

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
        let body = match crate::http::read_response_text_bounded(
            response,
            16 * 1024,
            "provider_error_body",
        )
        .await
        {
            Ok(body) => body,
            Err(error) => return error,
        };
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
    fn finalize(self) -> Result<ProviderToolCall> {
        let value = serde_json::from_str::<serde_json::Value>(self.arguments.as_str())
            .map_err(|error| anyhow!("Anthropic tool call contains invalid arguments: {error}"))?;
        let arguments = serde_json::to_string(&value)?;

        Ok(ProviderToolCall {
            id: self.id,
            name: self.name,
            arguments,
        })
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
        let prepared = prepare_messages_for_provider_async(
            self.name(),
            request.model.as_str(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let api_request = self.build_native_request(&request, &prepared, false)?;
        let prefix_body = serde_json::to_value(&api_request)?;

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

        let api_response: ApiChatResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;
        let mut response = Self::decode_response(api_response)?;
        if let Some(state) = response.provider_replay_state.as_mut() {
            if self.base_url != BASE_URL
                && crate::continuation::retention(state) != crate::continuation::Retention::Ordinary
            {
                state.payload["api_profile"] = serde_json::json!("unverified-relay");
            }
            crate::continuation::bind_prefix(
                state,
                &request.model,
                &self.replay_authority,
                &prefix_body,
            )?;
        }
        Ok(response)
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let prepared = prepare_messages_for_provider_async(
            self.name(),
            request.model.as_str(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let api_request = self.build_native_request(&request, &prepared, true)?;
        let prefix_body = serde_json::to_value(&api_request)?;

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

        let byte_stream = crate::http::bounded_response_stream(
            response,
            crate::types::ProviderResponseLimits::default().max_transport_bytes,
            "provider_stream",
        );

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        let replay_authority = self.replay_authority.clone();
        let unverified_relay = self.base_url != BASE_URL;
        let replay_model = request.model.clone();
        tokio::spawn(async move {
            use std::collections::{BTreeMap, HashSet};

            let mut decoder = IncrementalLineDecoder::default();
            let mut termination = None;
            let mut thinking_blocks = HashSet::new();
            let mut replay_blocks: BTreeMap<usize, serde_json::Value> = BTreeMap::new();
            let mut pending_tool_uses: BTreeMap<usize, PendingToolUse> = BTreeMap::new();

            tokio::pin!(byte_stream);

            while let Some(result) = tokio::select! {
                biased;
                _ = tx.closed() => return,
                result = byte_stream.next() => result,
            } {
                let bytes = match result {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        if tx.send(Err(anyhow!(e))).await.is_err() {
                            return;
                        }
                        return;
                    }
                };

                let lines = match decoder.push(bytes.as_ref()) {
                    Ok(lines) => lines,
                    Err(error) => {
                        if tx.send(Err(error)).await.is_err() {
                            return;
                        }
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

                    match serde_json::from_str::<StreamEvent>(data) {
                        Ok(event) => {
                            for usage in event
                                .message
                                .as_ref()
                                .and_then(|m| m.usage.as_ref())
                                .into_iter()
                                .chain(event.usage.as_ref())
                            {
                                if tx
                                    .send(Ok(StreamChunk::usage(usage.normalized())))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            if event.event_type == "message_stop" {
                                let remaining_calls = std::mem::take(&mut pending_tool_uses)
                                    .into_iter()
                                    .map(|(index, call)| {
                                        let call = call.finalize()?;
                                        if let Some(block) = replay_blocks.get_mut(&index) {
                                            block["input"] = parse_json_or_string(&call.arguments);
                                        }
                                        Ok(call)
                                    })
                                    .collect::<Result<Vec<_>>>();
                                let remaining_calls = match remaining_calls {
                                    Ok(calls) => calls,
                                    Err(error) => {
                                        if tx.send(Err(error)).await.is_err() {
                                            return;
                                        }
                                        return;
                                    }
                                };
                                if !remaining_calls.is_empty() {
                                    if tx
                                        .send(Ok(StreamChunk::tool_calls(remaining_calls)))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                if !replay_blocks.is_empty() {
                                    let blocks = replay_blocks.into_values().collect::<Vec<_>>();
                                    let mut state = ProviderReplayState::new(
                                        "anthropic",
                                        serde_json::json!({ "schema_version": 2, "blocks": blocks }),
                                    );
                                    if unverified_relay
                                        && crate::continuation::retention(&state)
                                            != crate::continuation::Retention::Ordinary
                                    {
                                        state.payload["api_profile"] =
                                            serde_json::json!("unverified-relay");
                                    }
                                    if let Err(error) = crate::continuation::bind_prefix(
                                        &mut state,
                                        &replay_model,
                                        &replay_authority,
                                        &prefix_body,
                                    ) {
                                        let _ = tx.send(Err(error)).await;
                                        return;
                                    }
                                    if tx
                                        .send(Ok(StreamChunk::provider_replay_state(state)))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                if tx
                                    .send(Ok(StreamChunk::final_chunk_with(
                                        termination.take().unwrap_or_else(|| {
                                            ProviderTermination::Unknown(
                                                "missing_stop_reason".to_owned(),
                                            )
                                        }),
                                    )))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                return;
                            }

                            if event.event_type == "message_delta" {
                                if let Some(reason) =
                                    event.delta.and_then(|delta| delta.stop_reason)
                                {
                                    termination =
                                        Some(ProviderTermination::from_openai_reason(&reason));
                                }
                                continue;
                            }

                            if event.event_type == "content_block_start" {
                                let index = event.index.unwrap_or(0);
                                if let Some(block) = event.content_block.as_ref() {
                                    replay_blocks.insert(
                                        index,
                                        serde_json::to_value(block)
                                            .expect("native block serializes"),
                                    );
                                    match block.block_type.as_str() {
                                        "thinking" => {
                                            thinking_blocks.insert(index);
                                        }
                                        "redacted_thinking" => {
                                            thinking_blocks.remove(&index);
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
                                    match call.finalize() {
                                        Ok(call) => {
                                            if let Some(block) = replay_blocks.get_mut(&index) {
                                                block["input"] =
                                                    parse_json_or_string(&call.arguments);
                                            }
                                            if tx
                                                .send(Ok(StreamChunk::tool_calls(vec![call])))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                        }
                                        Err(error) => {
                                            if tx.send(Err(error)).await.is_err() {
                                                return;
                                            }
                                            return;
                                        }
                                    }
                                }
                            }

                            if event.event_type == "content_block_delta" {
                                if let Some(delta) = event.delta {
                                    let index = event.index.unwrap_or(0);
                                    // Thinking block deltas use the `thinking` field
                                    if thinking_blocks.contains(&index) {
                                        if let Some(thinking) = delta.thinking {
                                            if !thinking.is_empty() {
                                                if let Some(block) = replay_blocks.get_mut(&index) {
                                                    append_native_string(
                                                        block, "thinking", &thinking,
                                                    );
                                                }
                                                if tx
                                                    .send(Ok(StreamChunk::reasoning(thinking)))
                                                    .await
                                                    .is_err()
                                                {
                                                    return;
                                                }
                                            }
                                        }
                                        if let Some(signature) = delta.signature
                                            && let Some(block) = replay_blocks.get_mut(&index)
                                        {
                                            append_native_string(block, "signature", &signature);
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
                                            if let Some(block) = replay_blocks.get_mut(&index) {
                                                append_native_string(block, "text", &text);
                                            }
                                            if tx.send(Ok(StreamChunk::delta(text))).await.is_err()
                                            {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            if tx
                                .send(Err(anyhow!("malformed Anthropic SSE frame: {e}")))
                                .await
                                .is_err()
                            {
                                return;
                            }
                            return;
                        }
                    }
                }
            }

            let error = decoder
                .finish()
                .err()
                .unwrap_or_else(|| anyhow!("Anthropic stream ended before message_stop"));
            if tx.send(Err(error)).await.is_err() {
                return;
            }
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

        let api_response: ModelsListResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;

        Ok(api_response
            .data
            .into_iter()
            .map(provider_model_from_anthropic_model_entry)
            .collect())
    }

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.list_models().await?;
        Ok(crate::ProviderWarmupOutcome::Completed)
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
    // Synthetic signatures exercise our policy, never vendor cryptography.
    #[test]
    fn production_native_body_enforces_durable_prefix_and_instance_authority() {
        use super::super::history_test_support::request;
        let provider = AnthropicProvider::new("test-key");
        let mut req = request(vec![ChatMessage::user("first")]);
        req.model = "claude-sonnet-5-5".into();
        req.compiled_prompt = Some(CompiledPromptPayload {
            stable_system_text: "rules".into(),
            dynamic_system_text: "time=one".into(),
            boundary_marker: "boundary".into(),
            full_system_text: "rules\ntime=one".into(),
        });
        req.tools = Some(vec![crate::ToolDefinition {
            name: "read".into(),
            description: "read".into(),
            parameters: serde_json::json!({"type":"object"}),
        }]);
        let build = |p: &AnthropicProvider, r: &ChatRequest| {
            let prepared = prepare_messages_for_provider_model(
                p.name(),
                &r.model,
                &p.capabilities(),
                &r.rendered_messages_with_compiled_prompt(),
            )?;
            p.build_native_request(r, &prepared, false)
                .map(|body| serde_json::to_value(body).unwrap())
        };
        let sent = build(&provider, &req).unwrap();
        let response: ApiChatResponse = serde_json::from_value(serde_json::json!({
            "id":"response", "content":[{"type":"thinking","thinking":"reason","signature":"synthetic"},{"type":"redacted_thinking","data":"opaque"},{"type":"text","text":"answer"}],
            "stop_reason":"end_turn", "usage":{"input_tokens":1,"output_tokens":1}
        })).unwrap();
        let decoded = AnthropicProvider::decode_response(response).unwrap();
        let mut state = decoded.provider_replay_state.unwrap();
        crate::continuation::bind_prefix(&mut state, &req.model, &provider.replay_authority, &sent)
            .unwrap();
        let mut answer = ChatMessage::assistant("answer");
        answer.provider_replay_state = Some(state);
        complete(&mut answer);
        // Exactly the durable roundtrip used by cold history, retaining proof.
        let answer: ChatMessage =
            serde_json::from_value(serde_json::to_value(answer).unwrap()).unwrap();
        req.messages.push(answer.clone());
        req.messages
            .push(ChatMessage::user("next normal user turn"));
        assert!(build(&provider, &req).is_ok());
        for change in 0..5 {
            let mut changed = req.clone();
            match change {
                0 => changed
                    .compiled_prompt
                    .as_mut()
                    .unwrap()
                    .full_system_text
                    .push_str("timestamp refresh"),
                1 => {
                    changed.compiled_prompt.as_mut().unwrap().full_system_text =
                        "new instructions".into()
                }
                2 => {
                    changed.tools.as_mut().unwrap()[0].parameters =
                        serde_json::json!({"type":"object","required":["path"]})
                }
                3 => changed.messages[0].content = "rewritten prefix".into(),
                _ => {
                    changed.messages[1]
                        .provider_replay_state
                        .as_mut()
                        .unwrap()
                        .payload
                        .as_object_mut()
                        .unwrap()
                        .remove("prefix_proof");
                }
            }
            assert!(
                build(&provider, &changed).is_err(),
                "changed timestamp/system/tools/retry/legacy {change}"
            );
        }
        let restarted = AnthropicProvider::new("test-key");
        assert!(
            build(&restarted, &req).is_err(),
            "restart/fork has no verified account authority"
        );
        let mut foreign = req.clone();
        foreign.model = "claude-opus-4-6".into();
        assert!(
            build(&provider, &foreign).is_ok(),
            "completed foreign-model state is projected as portable history"
        );
        assert!(
            answer
                .provider_replay_state
                .unwrap()
                .payload
                .get("prefix_proof")
                .is_some()
        );
        let mut unbound = req.clone();
        unbound.model = "claude-opus-4-6".into();
        let state = unbound.messages[1].provider_replay_state.as_mut().unwrap();
        state.model = Some(unbound.model.clone());
        state
            .payload
            .as_object_mut()
            .unwrap()
            .remove("prefix_proof");
        unbound.compiled_prompt.as_mut().unwrap().full_system_text =
            "changed unbound profile".into();
        assert!(
            build(&provider, &unbound).is_ok(),
            "documented old generation does not bind prefix"
        );
    }

    #[test]
    fn full_native_response_survives_storage_and_replays_without_regrouping() {
        let response: ApiChatResponse = serde_json::from_str(include_str!(
            "../../tests/fixtures/history/anthropic-interleaved.json"
        ))
        .unwrap();
        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/history/anthropic-interleaved.json"
        ))
        .unwrap();
        let decoded = AnthropicProvider::decode_response(response).unwrap();
        assert_eq!(decoded.text, "beforebetween");
        assert_eq!(decoded.tool_calls.len(), 2);
        let mut message = ChatMessage::assistant_tool_calls_with_provider_state(
            Some(decoded.text),
            decoded.reasoning_content,
            decoded.tool_calls,
            decoded.provider_replay_state,
        );
        message.provider_replay_state.as_mut().unwrap().model = Some("fixture".into());
        let stored: ChatMessage =
            serde_json::from_str(&serde_json::to_string(&message).unwrap()).unwrap();
        let projected =
            crate::history::project_messages_for_provider("anthropic", "fixture", &[stored])
                .unwrap();
        let (_, wire) = render_messages(&projected);
        assert_eq!(
            serde_json::to_value(&wire[0]).unwrap()["content"],
            raw["content"]
        );
    }

    #[tokio::test]
    async fn streamed_native_block_indexes_preserve_signed_redacted_and_tool_parts() {
        use super::super::history_test_support::{request, serve_sse};
        let (base, server) = serve_sse(include_str!(
            "../../tests/fixtures/history/anthropic-interleaved.sse"
        ))
        .await;
        let provider =
            AnthropicProvider::with_base_url_and_timeout_policy("key", base, Default::default());
        let mut stream = provider
            .stream_chat(request(vec![ChatMessage::user("start")]))
            .await
            .unwrap();
        let mut calls = Vec::new();
        let mut state = None;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            calls.extend(chunk.tool_calls);
            if chunk.provider_replay_state.is_some() {
                state = chunk.provider_replay_state;
            }
        }
        server.await.unwrap();
        assert_eq!(
            calls
                .iter()
                .map(|call| call.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let state = state.unwrap();
        assert_eq!(
            state.payload["blocks"],
            serde_json::json!([
                {"type":"thinking","thinking":"first","signature":"signed-1"},
                {"type":"tool_use","id":"a","name":"first","input":{"x":1}},
                {"type":"text","text":"between"},
                {"type":"redacted_thinking","data":"opaque-redacted"},
                {"type":"tool_use","id":"b","name":"second","input":{}}
            ])
        );
    }

    #[test]
    fn usage_normalization_requires_complete_separate_cache_counters() {
        let complete: super::ApiUsage = serde_json::from_value(serde_json::json!({
            "input_tokens":10,"cache_read_input_tokens":100,"cache_creation_input_tokens":20,"output_tokens":8
        })).unwrap();
        assert_eq!(complete.normalized().input_tokens, Some(130));
        assert_eq!(complete.normalized().output_tokens, Some(8));
        let missing: super::ApiUsage = serde_json::from_value(serde_json::json!({
            "input_tokens":10,"output_tokens":8
        }))
        .unwrap();
        assert_eq!(missing.normalized().input_tokens, None);
        assert_eq!(missing.normalized().output_tokens, Some(8));
    }

    use super::*;
    use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
    use crate::traits::Provider;
    use crate::types::{
        ChatMessage, CompiledPromptPayload, MessageProvenance, MessageSourceRef,
        ProviderReplayState, ReasoningConfig, ReasoningEffort,
    };

    fn complete(message: &mut ChatMessage) {
        message.provenance = Some(MessageProvenance {
            logical_turn_id: Some("turn".into()),
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            context_thread: None,
            unit_id: "answer".into(),
            sources: vec![MessageSourceRef {
                scope: "event:turn".into(),
                id: "source".into(),
                version: "revision:1".into(),
            }],
            complete: true,
            protected_input: false,
            inherited: false,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
        });
    }

    #[test]
    fn foreign_reasoning_is_present_in_anthropic_wire_as_unsigned_text() {
        let provider = AnthropicProvider::new("test-key");
        let mut message = ChatMessage::assistant("answer");
        message.provider_replay_state = Some(ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"reasoning_details":[{
                "type":"reasoning.summary","summary":"meaningful rationale"
            }]}),
        ));
        complete(&mut message);
        let canonical = message.clone();
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "claude-target",
            &provider.capabilities(),
            &[message],
        )
        .unwrap();
        let (_, wire) = AnthropicProvider::prepare_messages(&prepared).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        assert!(json.contains("meaningful rationale"));
        assert!(json.contains("portable unsigned text"));
        assert!(!json.contains("reasoning_details"));
        assert!(canonical.provider_replay_state.is_some());
    }

    #[test]
    fn multiblock_thinking_is_not_duplicated_on_anthropic_wire() {
        let provider = AnthropicProvider::new("test-key");
        let mut canonical = ChatMessage::assistant("answer");
        canonical.reasoning_content = Some("first second".into());
        canonical.provider_replay_state = Some(ProviderReplayState::for_model(
            "anthropic",
            "source-model",
            serde_json::json!({"blocks":[
                {"type":"thinking","thinking":"first ","signature":"opaque-one"},
                {"type":"thinking","thinking":"second","signature":"opaque-two"}
            ]}),
        ));
        complete(&mut canonical);
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "different-model",
            &provider.capabilities(),
            &[canonical.clone()],
        )
        .unwrap();
        let (_, wire) = AnthropicProvider::prepare_messages(&prepared).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        assert_eq!(json.matches("first second").count(), 1);
        assert!(!json.contains("opaque-one"));
        let compatible = prepare_messages_for_provider_model(
            provider.name(),
            "source-model",
            &provider.capabilities(),
            &[canonical.clone()],
        )
        .unwrap();
        let (_, exact_wire) = AnthropicProvider::prepare_messages(&compatible).unwrap();
        let exact_json = serde_json::to_string(&exact_wire).unwrap();
        assert!(exact_json.contains("opaque-one"));
        assert!(exact_json.contains("opaque-two"));
        assert!(
            !exact_json.contains("first second"),
            "common reasoning must not be emitted alongside signed blocks"
        );
        assert_eq!(canonical.reasoning_content.as_deref(), Some("first second"));
        assert!(canonical.provider_replay_state.is_some());
    }

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
    fn custom_base_url_keeps_prefix_without_double_slash() {
        let provider = AnthropicProvider::with_base_url("key", "http://localhost:9090/gateway/");
        assert_eq!(
            provider.models_url(),
            "http://localhost:9090/gateway/v1/models"
        );
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
    fn prepare_messages_replays_signed_thinking_blocks_before_tool_calls() {
        let message = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            Some("summary"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_file".to_owned(),
                arguments: "{}".to_owned(),
            }],
            Some(ProviderReplayState::new(
                "anthropic",
                serde_json::json!({
                    "blocks": [{
                        "type": "thinking",
                        "thinking": "summary",
                        "signature": "opaque-signature"
                    }]
                }),
            )),
        );

        let (_, messages) = render_messages(&[message]);

        assert!(matches!(
            &messages[0].content[0],
            ApiMessageContentBlock::Thinking { thinking, signature }
                if thinking == "summary" && signature == "opaque-signature"
        ));
        assert!(matches!(
            &messages[0].content[1],
            ApiMessageContentBlock::ToolUse { id, .. } if id == "call_1"
        ));
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
