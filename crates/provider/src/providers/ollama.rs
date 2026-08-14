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

const DEFAULT_BASE_URL: &str = "http://localhost:11434";

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
    arguments: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct OllamaOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
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

#[derive(Debug, Deserialize)]
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
    message: OllamaResponseMessage,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    done_reason: Option<String>,
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
        prepared
            .messages
            .iter()
            .enumerate()
            .map(|(message_index, m)| {
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
                    tool_calls: m.tool_calls.as_ref().map(|tool_calls| {
                        tool_calls
                            .iter()
                            .map(|call| OllamaToolCall {
                                id: Some(call.id.clone()),
                                function: OllamaToolFunctionCall {
                                    name: call.name.clone(),
                                    arguments: serde_json::from_str::<serde_json::Value>(
                                        call.arguments.as_str(),
                                    )
                                    .unwrap_or_else(|_| {
                                        serde_json::Value::String(call.arguments.clone())
                                    }),
                                },
                            })
                            .collect()
                    }),
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
        tool_calls
            .into_iter()
            .enumerate()
            .map(|(index, call)| ProviderToolCall {
                id: call.id.unwrap_or_else(|| format!("call_{}", index + 1)),
                name: call.function.name,
                arguments: serde_json::to_string(&call.function.arguments)
                    .unwrap_or_else(|_| "{}".to_owned()),
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
        };
        if options.temperature.is_some() {
            Some(options)
        } else {
            None
        }
    }

    async fn api_error(response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read error body>".into());
        anyhow!("Ollama API error ({status}): {body}")
    }
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
        let prepared = prepare_messages_for_provider_async(
            self.name(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let options = Self::build_options(&request);
        let api_request = OllamaChatRequest {
            model: request.model,
            messages: Self::convert_messages(&prepared)?,
            stream: false,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            options,
        };

        let request_builder = self.client.post(self.chat_url()).json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: OllamaChatResponse = response.json().await?;
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
        let termination = api_response
            .done_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| {
                if !api_response.done {
                    ProviderTermination::Unknown("missing_done_marker".to_owned())
                } else if tool_calls.is_empty() {
                    ProviderTermination::Complete
                } else {
                    ProviderTermination::ToolCalls
                }
            });
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

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let prepared = prepare_messages_for_provider_async(
            self.name(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let options = Self::build_options(&request);
        let api_request = OllamaChatRequest {
            model: request.model,
            messages: Self::convert_messages(&prepared)?,
            stream: true,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            options,
        };

        let request_builder = self.client.post(self.chat_url()).json(&api_request);
        let response =
            crate::http::send_stream_request(request_builder, self.timeout_policy).await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let byte_stream = response.bytes_stream();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            use std::collections::HashSet;

            let mut decoder = IncrementalLineDecoder::default();
            let mut emitted_tool_call_keys = HashSet::new();
            let mut saw_tool_calls = false;

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
                // Ollama streams newline-delimited JSON (not SSE)
                for line in lines {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    match serde_json::from_str::<OllamaStreamChunk>(&line) {
                        Ok(chunk) => {
                            let OllamaResponseMessage {
                                content,
                                thinking,
                                tool_calls,
                            } = chunk.message;

                            if let Some(tool_calls) = tool_calls {
                                let converted = Self::convert_tool_calls(tool_calls);
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
                                    let _ = tx.send(Ok(StreamChunk::tool_calls(new_calls))).await;
                                }
                            }
                            if chunk.done {
                                let termination = chunk
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
                                let _ = tx
                                    .send(Ok(StreamChunk::final_chunk_with(termination)))
                                    .await;
                                return;
                            }
                            if let Some(thinking) = thinking {
                                if !thinking.is_empty() {
                                    let _ = tx.send(Ok(StreamChunk::reasoning(thinking))).await;
                                }
                            }
                            if let Some(content) = content {
                                if !content.is_empty() {
                                    let _ = tx.send(Ok(StreamChunk::delta(content))).await;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Err(anyhow!("malformed Ollama NDJSON frame: {e}")))
                                .await;
                            return;
                        }
                    }
                }
            }

            let error = decoder
                .finish()
                .err()
                .unwrap_or_else(|| anyhow!("Ollama stream ended before done=true"));
            let _ = tx.send(Err(error)).await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(chunk_stream))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let request_builder = self.client.get(self.tags_url());
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: OllamaTagsResponse = response.json().await?;

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
                        tool_calling: Some(true),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::prepare_messages_for_provider;
    use crate::traits::Provider;
    use crate::types::ChatMessage;

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
            model: "llama3".into(),
            messages: vec![OllamaMessage {
                role: "user".into(),
                content: Some("Hello".into()),
                thinking: None,
                images: None,
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
            model: "llama3".into(),
            messages: vec![OllamaMessage {
                role: "user".into(),
                content: Some("Hello".into()),
                thinking: None,
                images: None,
                tool_calls: None,
            }],
            stream: false,
            tools: None,
            options: Some(OllamaOptions {
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
