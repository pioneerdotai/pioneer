use crate::attachments::{
    PreparedProviderMessages, attachment_bytes, ensure_no_unrendered_attachments,
    prepare_messages_for_provider_async,
};
use crate::tools::stream::IncrementalLineDecoder;
use crate::types::{
    ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
    ProviderInputCapabilities, ProviderTermination, ProviderTimeoutPolicy, ProviderToolCall, Role,
    StreamChunk, TokenUsage, ToolDefinition,
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

pub(crate) const DEFAULT_BASE_URL: &str = "http://localhost:11434";

pub struct OllamaProvider {
    base_url: String,
    timeout_policy: ProviderTimeoutPolicy,
    client: Client,
}

// ── Ollama API request types ───────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct OllamaChatRequest {
    model: String,
    messages: Vec<OllamaMessage>,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OllamaToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<OllamaOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    think: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OllamaMessage {
    role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thinking: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    images: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OllamaToolCall>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaToolDefinition {
    #[serde(rename = "type")]
    kind: String,
    function: OllamaToolFunctionDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaToolFunctionDefinition {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaToolCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    function: OllamaToolFunctionCall,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OllamaToolFunctionCall {
    name: String,
    #[serde(deserialize_with = "deserialize_tool_arguments")]
    arguments: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct OllamaOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    num_predict: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct OllamaShowResponse {
    // Modern servers expose supported controls. Older versions omit this;
    // omission must never be interpreted as support for false or an effort.
    thinking: Option<OllamaThinking>,
}

#[derive(Debug, Deserialize)]
struct OllamaThinking {
    values: Vec<serde_json::Value>,
}

// ── Ollama API response types ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct OllamaChatResponse {
    message: OllamaResponseMessage,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    done_reason: Option<String>,
    #[serde(default)]
    prompt_eval_count: Option<u64>,
    #[serde(default)]
    eval_count: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct OllamaResponseMessage {
    #[serde(default)]
    content: Option<String>,
    /// Thinking/reasoning output from models like DeepSeek-R1, Qwen3.
    #[serde(default)]
    thinking: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<OllamaToolCall>>,
}

// ── List models response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct OllamaTagsResponse {
    #[serde(default)]
    models: Vec<OllamaModelEntry>,
}

#[derive(Debug, Deserialize)]
struct OllamaModelEntry {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    details: Option<OllamaModelDetails>,
}

#[derive(Debug, Deserialize)]
struct OllamaModelDetails {
    #[serde(default)]
    family: Option<String>,
    #[serde(default)]
    parameter_size: Option<String>,
    #[serde(default)]
    quantization_level: Option<String>,
}

// ── Streaming response types ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct OllamaStreamChunk {
    #[serde(default)]
    message: OllamaResponseMessage,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    done_reason: Option<String>,
    #[serde(default)]
    prompt_eval_count: Option<u64>,
    #[serde(default)]
    eval_count: Option<u64>,
}

// ── Implementation ─────────────────────────────────────────────────────────

impl OllamaProvider {
    pub fn new() -> Self {
        Self::with_timeout_policy(ProviderTimeoutPolicy::default())
    }

    pub fn with_timeout_policy(timeout_policy: ProviderTimeoutPolicy) -> Self {
        Self::with_base_url_and_timeout_policy(DEFAULT_BASE_URL, timeout_policy)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self::with_base_url_and_timeout_policy(base_url, ProviderTimeoutPolicy::default())
    }

    pub fn with_base_url_and_timeout_policy(
        base_url: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        let base_url = normalize_base_url(base_url.into());

        Self {
            base_url,
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        }
    }

    fn convert_messages(prepared: &PreparedProviderMessages) -> Result<Vec<OllamaMessage>> {
        crate::tools::policy::ordered_tool_results(&prepared.messages)
            .into_iter()
            .map(|message_index| {
                let m = &prepared.messages[message_index];
                let mut images = Vec::new();
                for attachment in prepared.attachments_for_message(message_index) {
                    match attachment.kind {
                        InputContentType::Image => {
                            images.push(BASE64.encode(attachment_bytes(attachment)?));
                        }
                        _ => {
                            return Err(anyhow!(
                                "provider `ollama` only supports image attachments on /api/chat"
                            ));
                        }
                    }
                }

                Ok(OllamaMessage {
                    role: match m.role {
                        Role::System => "system".into(),
                        Role::User => "user".into(),
                        Role::Assistant => "assistant".into(),
                        Role::Tool => "tool".into(),
                    },
                    content: if m.content.is_empty() && m.tool_calls.is_some() {
                        None
                    } else {
                        Some(m.content.clone())
                    },
                    thinking: (m.role == Role::Assistant)
                        .then(|| m.reasoning_content.clone())
                        .flatten(),
                    images: (!images.is_empty()).then_some(images),
                    tool_name: (m.role == Role::Tool).then(|| m.name.clone()).flatten(),
                    // Native Message.ToolCallID is optional; preserve an existing result ID.
                    tool_call_id: (m.role == Role::Tool)
                        .then(|| m.tool_call_id.clone())
                        .flatten(),
                    tool_calls: m
                        .tool_calls
                        .as_ref()
                        .map(|tool_calls| {
                            tool_calls
                                .iter()
                                .map(|call| {
                                    Ok(OllamaToolCall {
                                        id: Some(call.id.clone()),
                                        function: OllamaToolFunctionCall {
                                            name: call.name.clone(),
                                            arguments: parse_tool_arguments(
                                                call.arguments.as_str(),
                                            )?,
                                        },
                                    })
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn convert_tools(tools: &[ToolDefinition]) -> Vec<OllamaToolDefinition> {
        tools
            .iter()
            .map(|tool| OllamaToolDefinition {
                kind: "function".to_owned(),
                function: OllamaToolFunctionDefinition {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    parameters: tool.parameters.clone(),
                },
            })
            .collect()
    }

    fn convert_tool_calls(tool_calls: Vec<OllamaToolCall>) -> Vec<ProviderToolCall> {
        Self::convert_tool_calls_with_offset(tool_calls, 0)
    }

    fn convert_tool_calls_with_offset(
        tool_calls: Vec<OllamaToolCall>,
        offset: usize,
    ) -> Vec<ProviderToolCall> {
        tool_calls
            .into_iter()
            .enumerate()
            .map(|(index, call)| ProviderToolCall {
                id: call
                    .id
                    .unwrap_or_else(|| format!("call_{}", offset + index + 1)),
                name: call.function.name,
                arguments: call.function.arguments.to_string(),
            })
            .collect()
    }

    fn chat_url(&self) -> String {
        format!("{}/api/chat", self.base_url)
    }

    fn tags_url(&self) -> String {
        format!("{}/api/tags", self.base_url)
    }

    fn build_options(request: &ChatRequest) -> Option<OllamaOptions> {
        let options = OllamaOptions {
            temperature: request.temperature,
            num_predict: request.max_tokens,
        };
        if options.temperature.is_some() || options.num_predict.is_some() {
            Some(options)
        } else {
            None
        }
    }

    fn resolve_think(
        request: &ChatRequest,
        metadata: Option<&OllamaThinking>,
    ) -> Result<Option<serde_json::Value>> {
        let Some(reasoning) = request.reasoning else {
            return Ok(None);
        };
        let value = if matches!(
            reasoning,
            crate::types::ReasoningConfig::Effort(crate::types::ReasoningEffort::None)
        ) && metadata.is_some_and(|m| m.values.contains(&serde_json::json!("none")))
        {
            serde_json::json!("none")
        } else if crate::generation::selected_off(Some(reasoning)) {
            serde_json::json!(false)
        } else {
            let crate::types::ReasoningConfig::Effort(effort) = reasoning else {
                unreachable!()
            };
            serde_json::json!(effort.as_str())
        };
        anyhow::ensure!(
            metadata.is_some_and(|m| m.values.contains(&value)),
            "Ollama server/model does not advertise selected think control {value} in /api/show; upgrade the server or select a supported setting"
        );
        Ok(Some(value))
    }

    async fn thinking_for_request(
        &self,
        request: &ChatRequest,
    ) -> Result<Option<serde_json::Value>> {
        crate::generation::validate_cap("ollama", request)?;
        if request.reasoning.is_none() {
            return Ok(None);
        }
        let response = crate::http::non_stream_request(
            self.client
                .post(format!("{}/api/show", self.base_url))
                .json(&serde_json::json!({"model":request.model})),
            self.timeout_policy,
        )
        .send()
        .await?;
        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }
        let metadata: OllamaShowResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_model_metadata",
        )
        .await?;
        Self::resolve_think(request, metadata.thinking.as_ref())
    }

    fn build_chat_request(
        request: &ChatRequest,
        prepared: &PreparedProviderMessages,
        stream: bool,
        think: Option<serde_json::Value>,
    ) -> Result<OllamaChatRequest> {
        crate::generation::validate_cap("ollama", request)?;
        Ok(OllamaChatRequest {
            model: request.model.clone(),
            messages: Self::convert_messages(prepared)?,
            stream,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            options: Self::build_options(request),
            think,
        })
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
        anyhow!("Ollama API error ({status}): {body}")
    }
}

// Native ToolCallFunctionArguments is map-backed, unlike Chat's string arguments.
fn require_tool_arguments(arguments: serde_json::Value) -> Result<serde_json::Value> {
    if !arguments.is_object() {
        return Err(anyhow!("Ollama function arguments must be a JSON object"));
    }
    Ok(arguments)
}

fn parse_tool_arguments(raw: &str) -> Result<serde_json::Value> {
    let arguments = serde_json::from_str(raw).map_err(|_| {
        anyhow!("Ollama function arguments must be valid JSON containing an object")
    })?;
    require_tool_arguments(arguments)
}

// Shared by ordinary JSON and each native NDJSON response message. Missing args
// remain a missing required field; explicit null is not normalized into {}.
fn deserialize_tool_arguments<'de, D>(
    deserializer: D,
) -> std::result::Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let arguments = serde_json::Value::deserialize(deserializer)?;
    require_tool_arguments(arguments).map_err(serde::de::Error::custom)
}

/// Normalize the base URL by stripping trailing `/api` and trailing `/`.
fn normalize_base_url(mut url: String) -> String {
    // Strip trailing slashes first
    while url.ends_with('/') {
        url.pop();
    }
    // Strip trailing /api
    if url.ends_with("/api") {
        url.truncate(url.len() - 4);
    }
    // Strip any trailing slashes again after removing /api
    while url.ends_with('/') {
        url.pop();
    }
    url
}

// The same decoder is used by HTTP transport and in-memory regression fixtures.
impl OllamaProvider {
    fn normalize_chat_response(api_response: OllamaChatResponse) -> Result<ChatResponse> {
        if !api_response.done {
            return Err(crate::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker.into());
        }
        let usage = match (api_response.prompt_eval_count, api_response.eval_count) {
            (None, None) => None,
            (input, output) => Some(TokenUsage {
                input_tokens: input,
                output_tokens: output,
            }),
        };

        let reasoning_content = api_response.message.thinking.filter(|t| !t.is_empty());
        let tool_calls =
            Self::convert_tool_calls(api_response.message.tool_calls.unwrap_or_default());
        let mut termination = api_response
            .done_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| {
                if tool_calls.is_empty() {
                    ProviderTermination::Complete
                } else {
                    ProviderTermination::ToolCalls
                }
            });
        if !tool_calls.is_empty() && termination == ProviderTermination::Complete {
            termination = ProviderTermination::ToolCalls;
        }
        let text = api_response.message.content.unwrap_or_default();

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(anyhow!("no response from Ollama"));
        }

        Ok(ChatResponse {
            text,
            usage,
            termination,
            reasoning_content,
            tool_calls,
            provider_replay_state: None,
        })
    }

    #[cfg(test)]
    pub(super) fn decode_chat_fixture(value: serde_json::Value) -> Result<ChatResponse> {
        Self::normalize_chat_response(serde_json::from_value(value)?)
    }

    pub(super) fn decode_stream(
        byte_stream: BoxStream<'static, Result<bytes::Bytes>>,
    ) -> BoxStream<'static, Result<StreamChunk>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            use std::collections::HashSet;

            let mut decoder = IncrementalLineDecoder::default();
            let mut emitted_tool_call_keys = HashSet::new();
            let mut saw_tool_calls = false;
            let mut tool_call_count = 0;

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
                // Ollama streams newline-delimited JSON (not SSE)
                for line in lines {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<OllamaStreamChunk>(&line) {
                        Ok(chunk) => {
                            if chunk.error.is_some() {
                                let _ = tx
                                    .send(Err(crate::failure::NativeStreamStatusError::new(
                                        "Ollama", None,
                                    )
                                    .into()))
                                    .await;
                                return;
                            }

                            let OllamaResponseMessage {
                                content,
                                thinking,
                                tool_calls,
                            } = chunk.message;

                            if let Some(tool_calls) = tool_calls {
                                let count = tool_calls.len();
                                let converted = Self::convert_tool_calls_with_offset(
                                    tool_calls,
                                    tool_call_count,
                                );
                                tool_call_count += count;
                                let mut new_calls = Vec::new();
                                for call in converted {
                                    let key =
                                        format!("{}:{}:{}", call.id, call.name, call.arguments);
                                    if emitted_tool_call_keys.insert(key) {
                                        new_calls.push(call);
                                    }
                                }
                                if !new_calls.is_empty() {
                                    saw_tool_calls = true;
                                    if tx
                                        .send(Ok(StreamChunk::tool_calls(new_calls)))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                            }
                            if let Some(thinking) = thinking {
                                if !thinking.is_empty() {
                                    if tx.send(Ok(StreamChunk::reasoning(thinking))).await.is_err()
                                    {
                                        return;
                                    }
                                }
                            }
                            if let Some(content) = content {
                                if !content.is_empty() {
                                    if tx.send(Ok(StreamChunk::delta(content))).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            if chunk.done {
                                let mut termination = chunk
                                    .done_reason
                                    .as_deref()
                                    .map(ProviderTermination::from_openai_reason)
                                    .unwrap_or_else(|| {
                                        if saw_tool_calls {
                                            ProviderTermination::ToolCalls
                                        } else {
                                            ProviderTermination::Complete
                                        }
                                    });
                                if saw_tool_calls && termination == ProviderTermination::Complete {
                                    termination = ProviderTermination::ToolCalls;
                                }
                                if tx
                                    .send(Ok(StreamChunk::final_chunk_with(termination)
                                        .with_usage(Some(TokenUsage {
                                            input_tokens: chunk.prompt_eval_count,
                                            output_tokens: chunk.eval_count,
                                        }))))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                return;
                            }
                        }
                        Err(_) => {
                            if tx
                                .send(Err(anyhow!("malformed Ollama NDJSON frame")))
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

            let error = decoder.finish().err().unwrap_or_else(|| {
                crate::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker.into()
            });
            if tx.send(Err(error)).await.is_err() {
                return;
            }
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Box::pin(chunk_stream)
    }
}

#[async_trait]
impl crate::traits::Provider for OllamaProvider {
    fn name(&self) -> &str {
        "ollama"
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
                file: InputTypeSupport::disabled(),
                image: InputTypeSupport::native_inline_only(),
                audio: InputTypeSupport::disabled(),
                video: InputTypeSupport::disabled(),
            },
        }
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let request = crate::tools::policy::prepare_request(self.name(), request)?;
        let mut prepared = prepare_messages_for_provider_async(
            self.name(),
            request.model.as_str(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        crate::tools::policy::prepare_history(self.name(), &mut prepared.messages)?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let think = self.thinking_for_request(&request).await?;
        let api_request = Self::build_chat_request(&request, &prepared, false, think)?;

        let request_builder = self.client.post(self.chat_url()).json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: OllamaChatResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;
        Self::normalize_chat_response(api_response)
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let request = crate::tools::policy::prepare_request(self.name(), request)?;
        let mut prepared = prepare_messages_for_provider_async(
            self.name(),
            request.model.as_str(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        crate::tools::policy::prepare_history(self.name(), &mut prepared.messages)?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let think = self.thinking_for_request(&request).await?;
        let api_request = Self::build_chat_request(&request, &prepared, true, think)?;

        let request_builder = self.client.post(self.chat_url()).json(&api_request);
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

        Ok(Self::decode_stream(byte_stream))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let request_builder = self.client.get(self.tags_url());
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: OllamaTagsResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;

        Ok(api_response
            .models
            .into_iter()
            .map(|m| {
                let id = m
                    .model
                    .clone()
                    .or_else(|| m.name.clone())
                    .unwrap_or_default();
                let description = m.details.as_ref().and_then(|d| {
                    let parts: Vec<String> = [
                        d.parameter_size.as_deref().map(|s| format!("params: {s}")),
                        d.quantization_level
                            .as_deref()
                            .map(|s| format!("quant: {s}")),
                    ]
                    .into_iter()
                    .flatten()
                    .collect();
                    if parts.is_empty() {
                        None
                    } else {
                        Some(parts.join(", "))
                    }
                });

                ProviderModelInfo {
                    id: id.clone(),
                    name: m.name,
                    description,
                    created: None,
                    provider: "ollama".to_owned(),
                    owned_by: None,
                    limits: ProviderModelLimits::default(),
                    capabilities: ProviderModelCapabilities {
                        streaming: Some(true),
                        tool_calling: None,
                        ..ProviderModelCapabilities::default()
                    },
                    transcription: None,
                    pricing: None,
                    active: Some(true),
                    family: m.details.and_then(|d| d.family),
                    lifecycle_status: None,
                }
            })
            .collect())
    }

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.list_models().await?;
        Ok(crate::ProviderWarmupOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn native_bodies_preserve_prepared_cap_independent_of_temperature() {
        let provider = OllamaProvider::new();
        let mut request = crate::generation::test_request("qwen3:fixture");
        let prepared = crate::attachments::prepare_messages_for_provider(
            "ollama",
            &provider.capabilities(),
            &request.messages,
        )
        .unwrap();
        for stream in [false, true] {
            let body = serde_json::to_value(
                OllamaProvider::build_chat_request(&request, &prepared, stream, None).unwrap(),
            )
            .unwrap();
            assert_eq!(body["options"]["num_predict"], 1024);
            assert!(body["options"].get("temperature").is_none());
            assert!(body.get("think").is_none());
            assert_eq!(body["stream"], stream);
        }
        request.temperature = Some(0.7);
        let body = serde_json::to_value(
            OllamaProvider::build_chat_request(
                &request,
                &prepared,
                true,
                Some(serde_json::json!(false)),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(body["options"]["num_predict"], 1024);
        assert_eq!(body["think"], false);
    }

    #[test]
    fn server_metadata_controls_default_off_and_supported_efforts() {
        let mut request = crate::generation::test_request("an-installed-model-alias");
        assert_eq!(OllamaProvider::resolve_think(&request, None).unwrap(), None);
        let booleans: OllamaShowResponse =
            serde_json::from_str(r#"{"thinking":{"values":[true,false],"default":true}}"#).unwrap();
        let levels: OllamaShowResponse = serde_json::from_str(
            r#"{"thinking":{"values":["low","medium","high"],"default":"medium"}}"#,
        )
        .unwrap();
        for off in [
            crate::types::ReasoningConfig::Disabled,
            crate::types::ReasoningConfig::Effort(crate::types::ReasoningEffort::None),
        ] {
            request.reasoning = Some(off);
            assert_eq!(
                OllamaProvider::resolve_think(&request, booleans.thinking.as_ref()).unwrap(),
                Some(serde_json::json!(false))
            );
            assert!(OllamaProvider::resolve_think(&request, levels.thinking.as_ref()).is_err());
            assert!(OllamaProvider::resolve_think(&request, None).is_err());
        }
        request.reasoning = Some(crate::types::ReasoningConfig::Effort(
            crate::types::ReasoningEffort::High,
        ));
        assert_eq!(
            OllamaProvider::resolve_think(&request, levels.thinking.as_ref()).unwrap(),
            Some(serde_json::json!("high"))
        );
        assert!(OllamaProvider::resolve_think(&request, booleans.thinking.as_ref()).is_err());
        request.reasoning = Some(crate::types::ReasoningConfig::Effort(
            crate::types::ReasoningEffort::Max,
        ));
        assert!(OllamaProvider::resolve_think(&request, levels.thinking.as_ref()).is_err());
        let explicit_none: OllamaShowResponse =
            serde_json::from_str(r#"{"thinking":{"values":["none","high"]}}"#).unwrap();
        request.reasoning = Some(crate::types::ReasoningConfig::Effort(
            crate::types::ReasoningEffort::None,
        ));
        assert_eq!(
            OllamaProvider::resolve_think(&request, explicit_none.thinking.as_ref()).unwrap(),
            Some(serde_json::json!("none"))
        );
        request.reasoning = Some(crate::types::ReasoningConfig::Disabled);
        assert!(OllamaProvider::resolve_think(&request, explicit_none.thinking.as_ref()).is_err());
    }
    use super::*;
    use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
    use crate::traits::Provider;
    use crate::types::{ChatMessage, ProviderReplayState};

    #[test]
    fn legacy_name_order_projection_preserves_canonical_ids() {
        let provider = OllamaProvider::new();
        let mut assistant = crate::ChatMessage::assistant("");
        assistant.tool_calls = Some(vec![
            ProviderToolCall {
                id: "first".into(),
                name: "lookup".into(),
                arguments: "{}".into(),
            },
            ProviderToolCall {
                id: "second".into(),
                name: "lookup".into(),
                arguments: "{}".into(),
            },
        ]);
        let messages = vec![
            assistant,
            crate::ChatMessage::tool_result("second", "lookup", "second-result"),
            crate::ChatMessage::tool_result("first", "lookup", "first-result"),
        ];
        let mut prepared = crate::attachments::prepare_messages_for_provider(
            "ollama",
            &provider.capabilities(),
            &messages,
        )
        .unwrap();
        crate::tools::policy::prepare_history("ollama", &mut prepared.messages).unwrap();
        let wire = OllamaProvider::convert_messages(&prepared).unwrap();
        assert_eq!(wire[1].content.as_deref(), Some("first-result"));
        assert_eq!(wire[2].content.as_deref(), Some("second-result"));
        assert_eq!(wire[1].tool_call_id.as_deref(), Some("first"));
        assert_eq!(wire[2].tool_call_id.as_deref(), Some("second"));
        assert_eq!(
            serde_json::to_value(&wire[1]).unwrap()["tool_name"],
            "lookup"
        );
        assert_eq!(messages[1].tool_call_id.as_deref(), Some("second"));
    }

    #[test]
    fn active_foreign_replay_is_rejected_before_ollama_serializer_can_ignore_it() {
        let provider = OllamaProvider::new();
        let mut message = ChatMessage::assistant("partial");
        message.provider_replay_state = Some(ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"opaque":"state"}),
        ));
        let error = prepare_messages_for_provider_model(
            provider.name(),
            "local-target",
            &provider.capabilities(),
            &[message],
        )
        .expect_err("foreign active replay must fail before Ollama wire conversion");
        assert!(
            error
                .downcast_ref::<crate::history::IncompatibleProviderReplayContinuation>()
                .is_some()
        );
    }

    #[test]
    fn creates_with_default_base_url() {
        let provider = OllamaProvider::new();
        assert_eq!(provider.base_url, DEFAULT_BASE_URL);
    }

    #[test]
    fn creates_with_custom_base_url() {
        let provider = OllamaProvider::with_base_url("http://my-ollama:8080");
        assert_eq!(provider.base_url, "http://my-ollama:8080");
    }

    #[test]
    fn normalize_strips_trailing_slash() {
        let provider = OllamaProvider::with_base_url("http://localhost:11434/");
        assert_eq!(provider.base_url, "http://localhost:11434");
    }

    #[test]
    fn normalize_strips_trailing_api() {
        let provider = OllamaProvider::with_base_url("http://localhost:11434/api");
        assert_eq!(provider.base_url, "http://localhost:11434");
    }

    #[test]
    fn normalize_strips_trailing_api_with_slash() {
        let provider = OllamaProvider::with_base_url("http://localhost:11434/api/");
        assert_eq!(provider.base_url, "http://localhost:11434");
    }

    #[test]
    fn chat_url_built_correctly() {
        let provider = OllamaProvider::new();
        assert_eq!(provider.chat_url(), "http://localhost:11434/api/chat");
    }

    #[test]
    fn chat_url_with_custom_base() {
        let provider = OllamaProvider::with_base_url("http://remote:9999");
        assert_eq!(provider.chat_url(), "http://remote:9999/api/chat");
    }

    #[test]
    fn custom_gateway_prefix_routes_chat_and_tags() {
        let provider = OllamaProvider::with_base_url("http://localhost:11434/team/api/");
        assert_eq!(provider.chat_url(), "http://localhost:11434/team/api/chat");
        assert_eq!(provider.tags_url(), "http://localhost:11434/team/api/tags");
    }

    #[test]
    fn convert_messages_maps_roles() {
        let mut assistant = ChatMessage::assistant("Hi!");
        assistant.reasoning_content = Some("local thinking".to_owned());
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            assistant,
        ];

        let provider = OllamaProvider::new();
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = OllamaProvider::convert_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "system");
        assert_eq!(api_messages[0].content.as_deref(), Some("Be helpful"));
        assert_eq!(api_messages[1].role, "user");
        assert_eq!(api_messages[2].role, "assistant");
        assert_eq!(api_messages[2].thinking.as_deref(), Some("local thinking"));
    }

    #[test]
    fn request_serializes_without_options() {
        let request = OllamaChatRequest {
            think: None,
            model: "llama3".into(),
            messages: vec![OllamaMessage {
                role: "user".into(),
                content: Some("Hello".into()),
                thinking: None,
                images: None,
                tool_name: None,
                tool_call_id: None,
                tool_calls: None,
            }],
            stream: false,
            tools: None,
            options: None,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"model\":\"llama3\""));
        assert!(json.contains("\"stream\":false"));
        assert!(!json.contains("options"));
    }

    #[test]
    fn request_serializes_with_temperature() {
        let request = OllamaChatRequest {
            think: None,
            model: "llama3".into(),
            messages: vec![OllamaMessage {
                role: "user".into(),
                content: Some("Hello".into()),
                thinking: None,
                images: None,
                tool_name: None,
                tool_call_id: None,
                tool_calls: None,
            }],
            stream: false,
            tools: None,
            options: Some(OllamaOptions {
                num_predict: None,
                temperature: Some(0.7),
            }),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"options\""));
    }

    #[test]
    fn response_deserializes() {
        let json = r#"{
            "message": {"role": "assistant", "content": "Hello from Ollama"},
            "prompt_eval_count": 25,
            "eval_count": 40
        }"#;
        let response: OllamaChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            response.message.content.as_deref(),
            Some("Hello from Ollama")
        );
        assert_eq!(response.prompt_eval_count, Some(25));
        assert_eq!(response.eval_count, Some(40));
    }

    #[test]
    fn response_deserializes_without_token_counts() {
        let json = r#"{"message": {"content": "Hi"}}"#;
        let response: OllamaChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.message.content.as_deref(), Some("Hi"));
        assert!(response.prompt_eval_count.is_none());
        assert!(response.eval_count.is_none());
    }

    #[test]
    fn stream_chunk_deserializes_delta() {
        let json = r#"{"message": {"content": "Hello"}, "done": false}"#;
        let chunk: OllamaStreamChunk = serde_json::from_str(json).unwrap();
        assert_eq!(chunk.message.content.as_deref(), Some("Hello"));
        assert!(!chunk.done);
    }

    #[test]
    fn stream_chunk_deserializes_done() {
        let json = r#"{"message": {"content": ""}, "done": true}"#;
        let chunk: OllamaStreamChunk = serde_json::from_str(json).unwrap();
        assert!(chunk.done);
        assert_eq!(chunk.message.content.as_deref(), Some(""));
    }

    #[test]
    fn build_options_none_when_no_temperature() {
        let request = ChatRequest {
            model: "llama3".into(),
            messages: vec![ChatMessage::user("hi")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        assert!(OllamaProvider::build_options(&request).is_none());
    }

    #[test]
    fn build_options_some_when_temperature_set() {
        let request = ChatRequest {
            model: "llama3".into(),
            messages: vec![ChatMessage::user("hi")],
            temperature: Some(0.5),
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let options = OllamaProvider::build_options(&request).unwrap();
        assert_eq!(options.temperature, Some(0.5));
    }

    #[test]
    fn provider_name() {
        let provider = OllamaProvider::new();
        assert_eq!(provider.name(), "ollama");
    }

    #[test]
    fn provider_capabilities() {
        let provider = OllamaProvider::new();
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }
}

#[cfg(test)]
#[path = "wire_tests/ollama.rs"]
mod wire_contract_tests;
