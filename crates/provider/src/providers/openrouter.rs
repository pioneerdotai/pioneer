use crate::{
    attachments::{
        PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes, attachment_data_url,
        ensure_no_unrendered_attachments, prepare_messages_for_provider,
    },
    tools::call::{StreamToolCallAccumulator, StreamToolCallDelta, StreamToolFunctionDelta},
    tools::parse::{parse_embedded_tool_payload, parse_tool_calls},
    types::{
        ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
        ProviderInputCapabilities, Role, StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
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
use std::time::Duration;

use pioneer_protocol::{
    ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits, ProviderModelPricing,
};

const BASE_URL: &str = "https://openrouter.ai/api/v1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub struct OpenRouterProvider {
    api_key: String,
    base_url: String,
    client: Client,
}

// ── OpenRouter API request types ────────────────────────────────────────────

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
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ApiReasoningOptions>,
    stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiReasoningOptions {
    effort: String,
}

#[derive(Debug, Serialize)]
struct ApiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<ApiMessageContent>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: ApiToolCallFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiToolCallFunction {
    name: String,
    arguments: String,
}

// ── OpenRouter API response types ───────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ApiChatResponse {
    choices: Vec<ApiChoice>,
    #[serde(default)]
    usage: Option<ApiUsage>,
}

#[derive(Debug, Deserialize)]
struct ApiChoice {
    message: ApiResponseMessage,
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
    /// Returns assistant final text content only.
    fn effective_content(&self) -> String {
        self.content.clone().unwrap_or_default()
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
    choices: Vec<StreamChoice>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    delta: StreamDelta,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
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

// ── List models response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ModelsListResponse {
    data: Vec<OpenRouterModelEntry>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterModelEntry {
    id: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    context_length: Option<u64>,
    #[serde(default)]
    max_completion_tokens: Option<u64>,
    #[serde(default)]
    pricing: Option<OpenRouterPricing>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterPricing {
    #[serde(default)]
    prompt: Option<String>,
    #[serde(default)]
    completion: Option<String>,
    #[serde(default)]
    image: Option<String>,
    #[serde(default)]
    request: Option<String>,
}

// ── Implementation ──────────────────────────────────────────────────────────

impl OpenRouterProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_base_url(api_key, BASE_URL)
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");

        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            client,
        }
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
                file_data: Some(attachment_data_url(attachment)?),
                file_id: None,
                filename: Some(attachment.name.clone()),
            },
        };
        Ok(ApiContentPart::File { file })
    }

    fn build_parts(
        text: &str,
        attachments: &[&crate::attachments::PreparedAttachment],
        include_text: bool,
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
                _ => {
                    return Err(anyhow!(
                        "provider `openrouter` does not support {:?} attachments in chat/completions yet",
                        attachment.kind
                    ));
                }
            }
        }

        Ok(parts)
    }

    fn convert_messages(prepared: &PreparedProviderMessages) -> Result<Vec<ApiMessage>> {
        let mut rendered = Vec::new();
        for (message_index, message) in prepared.messages.iter().enumerate() {
            let attachments = prepared
                .attachments_for_message(message_index)
                .collect::<Vec<_>>();

            let tool_calls = message.tool_calls.as_ref().map(|tool_calls| {
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
                    .collect::<Vec<_>>()
            });

            if attachments.is_empty() {
                let content = if message.content.is_empty() && tool_calls.is_some() {
                    None
                } else {
                    Some(ApiMessageContent::Text(message.content.clone()))
                };
                rendered.push(ApiMessage {
                    role: match message.role {
                        Role::System => "system".into(),
                        Role::User => "user".into(),
                        Role::Assistant => "assistant".into(),
                        Role::Tool => "tool".into(),
                    },
                    content,
                    tool_calls,
                    tool_call_id: message.tool_call_id.clone(),
                    name: message.name.clone(),
                });
                continue;
            }

            if message.role == Role::Tool {
                let tool_content = if message.content.is_empty() {
                    "{}".to_owned()
                } else {
                    message.content.clone()
                };
                rendered.push(ApiMessage {
                    role: "tool".to_owned(),
                    content: Some(ApiMessageContent::Text(tool_content)),
                    tool_calls,
                    tool_call_id: message.tool_call_id.clone(),
                    name: message.name.clone(),
                });

                let mut parts = vec![ApiContentPart::Text {
                    text: "Analyze the attached tool media context and continue.".to_owned(),
                }];
                parts.extend(Self::build_parts(
                    message.content.as_str(),
                    &attachments,
                    false,
                )?);
                rendered.push(ApiMessage {
                    role: "user".to_owned(),
                    content: Some(ApiMessageContent::Parts(parts)),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                });
                continue;
            }

            let parts = Self::build_parts(message.content.as_str(), &attachments, true)?;
            rendered.push(ApiMessage {
                role: match message.role {
                    Role::System => "system".into(),
                    Role::User => "user".into(),
                    Role::Assistant => "assistant".into(),
                    Role::Tool => "tool".into(),
                },
                content: Some(ApiMessageContent::Parts(parts)),
                tool_calls,
                tool_call_id: message.tool_call_id.clone(),
                name: message.name.clone(),
            });
        }

        Ok(rendered)
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

    fn chat_completions_url(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    fn auth_key_url(&self) -> String {
        format!("{}/auth/key", self.base_url)
    }

    fn models_url(&self) -> String {
        format!("{}/models", self.base_url)
    }

    async fn api_error(response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read error body>".into());
        anyhow!("OpenRouter API error ({status}): {body}")
    }
}

#[async_trait]
impl crate::traits::Provider for OpenRouterProvider {
    fn name(&self) -> &str {
        "openrouter"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: true,
            tool_calling: true,
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport {
                    native: true,
                    file_upload: false,
                    data_url_inline: true,
                    text_fallback: false,
                },
                image: InputTypeSupport {
                    native: true,
                    file_upload: false,
                    data_url_inline: true,
                    text_fallback: false,
                },
                audio: InputTypeSupport::native_inline_only(),
                video: InputTypeSupport {
                    native: true,
                    file_upload: false,
                    data_url_inline: true,
                    text_fallback: false,
                },
            },
        }
    }

    async fn warmup(&self) -> Result<()> {
        self.client
            .get(self.auth_key_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let prepared = prepare_messages_for_provider(
            self.name(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let rendered_messages = Self::convert_messages(&prepared)?;
        let reasoning = Some(ApiReasoningOptions {
            effort: "medium".to_owned(),
        });

        let api_request = ApiChatRequest {
            model: request.model,
            messages: rendered_messages,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            parallel_tool_calls: request.parallel_tool_calls,
            reasoning,
            stream: false,
        };

        let response = self
            .client
            .post(self.chat_completions_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&api_request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: ApiChatResponse = response.json().await?;
        let usage = api_response.usage.map(|u| TokenUsage {
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
        });

        let message = api_response
            .choices
            .into_iter()
            .next()
            .map(|c| c.message)
            .ok_or_else(|| anyhow!("no response from OpenRouter"))?;

        let raw_content = message.content.clone();
        let mut text = message.effective_content();
        let mut tool_calls =
            parse_tool_calls(message.tool_calls.as_ref(), message.function_call.as_ref());
        let mut reasoning_content = message.reasoning_content.or(message.reasoning);

        if tool_calls.is_empty() {
            if let Some(content) = raw_content.as_deref() {
                if let Some(parsed) = parse_embedded_tool_payload(content) {
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
            return Err(anyhow!("no response from OpenRouter"));
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
        let rendered_messages = Self::convert_messages(&prepared)?;
        let reasoning = Some(ApiReasoningOptions {
            effort: "medium".to_owned(),
        });

        let api_request = ApiChatRequest {
            model: request.model,
            messages: rendered_messages,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            parallel_tool_calls: request.parallel_tool_calls,
            reasoning,
            stream: true,
        };

        let response = self
            .client
            .post(self.chat_completions_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&api_request)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let byte_stream = response.bytes_stream();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            let mut buffer = String::new();
            let mut tool_call_accumulator = StreamToolCallAccumulator::default();

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

                    if data.trim() == "[DONE]" {
                        let tool_calls = tool_call_accumulator.take_tool_calls();
                        if !tool_calls.is_empty() {
                            let _ = tx.send(Ok(StreamChunk::tool_calls(tool_calls))).await;
                        }
                        let _ = tx.send(Ok(StreamChunk::final_chunk())).await;
                        return;
                    }

                    match serde_json::from_str::<StreamResponse>(data) {
                        Ok(resp) => {
                            for choice in resp.choices {
                                if let Some(rc) =
                                    choice.delta.reasoning_content.or(choice.delta.reasoning)
                                {
                                    if !rc.is_empty() {
                                        let _ = tx.send(Ok(StreamChunk::reasoning(rc))).await;
                                    }
                                }
                                if let Some(content) = choice.delta.content {
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
                                if choice.finish_reason.is_some() {
                                    let tool_calls = tool_call_accumulator.take_tool_calls();
                                    if !tool_calls.is_empty() {
                                        let _ =
                                            tx.send(Ok(StreamChunk::tool_calls(tool_calls))).await;
                                    }
                                    let _ = tx.send(Ok(StreamChunk::final_chunk())).await;
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!("failed to parse SSE chunk: {e}");
                        }
                    }
                }
            }

            let tool_calls = tool_call_accumulator.take_tool_calls();
            if !tool_calls.is_empty() {
                let _ = tx.send(Ok(StreamChunk::tool_calls(tool_calls))).await;
            }
            let _ = tx.send(Ok(StreamChunk::final_chunk())).await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(chunk_stream))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let response = self
            .client
            .get(self.models_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: ModelsListResponse = response.json().await?;

        Ok(api_response
            .data
            .into_iter()
            .map(|m| {
                let pricing = m.pricing.map(|p| {
                    let parse = |s: &Option<String>| s.as_ref().and_then(|v| v.parse::<f64>().ok());
                    ProviderModelPricing {
                        input_token: parse(&p.prompt),
                        output_token: parse(&p.completion),
                        image: parse(&p.image),
                        request: parse(&p.request),
                    }
                });

                ProviderModelInfo {
                    id: m.id.clone(),
                    name: m.name,
                    description: m.description,
                    created: m.created,
                    provider: "openrouter".to_owned(),
                    owned_by: m.id.split('/').next().map(|s| s.to_owned()),
                    limits: ProviderModelLimits {
                        max_input_tokens: m.context_length,
                        max_output_tokens: m.max_completion_tokens,
                        context_window: m.context_length,
                    },
                    capabilities: ProviderModelCapabilities::default(),
                    pricing,
                    active: Some(true),
                    family: None,
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
    use crate::types::{AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart};

    #[test]
    fn creates_with_api_key() {
        let provider = OpenRouterProvider::new("sk-or-test-key");
        assert_eq!(provider.api_key, "sk-or-test-key");
        assert_eq!(provider.base_url, BASE_URL);
    }

    #[test]
    fn creates_with_custom_base_url() {
        let provider = OpenRouterProvider::with_base_url("key", "http://localhost:8080");
        assert_eq!(provider.base_url, "http://localhost:8080");
    }

    #[test]
    fn chat_completions_url_built_correctly() {
        let provider = OpenRouterProvider::new("key");
        assert_eq!(
            provider.chat_completions_url(),
            "https://openrouter.ai/api/v1/chat/completions"
        );
    }

    #[test]
    fn convert_messages_maps_roles() {
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let provider = OpenRouterProvider::new("test-key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = OpenRouterProvider::convert_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "system");
        assert!(matches!(
            api_messages[0].content,
            Some(ApiMessageContent::Text(ref text)) if text == "Be helpful"
        ));
        assert_eq!(api_messages[1].role, "user");
        assert_eq!(api_messages[2].role, "assistant");
    }

    #[test]
    fn convert_messages_tool_attachment_splits_into_tool_and_user_messages() {
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
        }));

        let messages = vec![ChatMessage::user("hello"), tool_message];
        let provider = OpenRouterProvider::new("test-key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = OpenRouterProvider::convert_messages(&prepared).unwrap();

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
    fn api_request_serializes_correctly() {
        let request = ApiChatRequest {
            model: "anthropic/claude-sonnet-4".into(),
            messages: vec![
                ApiMessage {
                    role: "system".into(),
                    content: Some(ApiMessageContent::Text("You are helpful".into())),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ApiMessage {
                    role: "user".into(),
                    content: Some(ApiMessageContent::Text("Hello".into())),
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
            reasoning: None,
            stream: false,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("anthropic/claude-sonnet-4"));
        assert!(json.contains("\"role\":\"system\""));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"stream\":false"));
        assert!(!json.contains("max_tokens"));
        assert!(!json.contains("\"reasoning\""));
    }

    #[test]
    fn api_request_serializes_reasoning_options() {
        let request = ApiChatRequest {
            model: "openai/gpt-5".into(),
            messages: vec![ApiMessage {
                role: "user".into(),
                content: Some(ApiMessageContent::Text("Hello".into())),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            }],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ApiReasoningOptions {
                effort: "medium".to_owned(),
            }),
            stream: false,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"reasoning\":{\"effort\":\"medium\"}"));
    }

    #[test]
    fn api_request_serializes_reasoning_options_for_any_model() {
        let request = ApiChatRequest {
            model: "anthropic/claude-sonnet-4".into(),
            messages: vec![ApiMessage {
                role: "user".into(),
                content: Some(ApiMessageContent::Text("Hello".into())),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            }],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ApiReasoningOptions {
                effort: "medium".to_owned(),
            }),
            stream: false,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("\"reasoning\":{\"effort\":\"medium\"}"));
    }

    #[test]
    fn api_response_deserializes() {
        let json = r#"{"choices":[{"message":{"content":"Hi from OpenRouter"}}]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.choices.len(), 1);
        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Hi from OpenRouter")
        );
    }

    #[test]
    fn api_response_reasoning_field_does_not_override_content_text() {
        let json = r#"{"choices":[{"message":{"content":"","reasoning":"thinking"}}]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.choices[0].message.effective_content(), "");
        assert_eq!(
            response.choices[0].message.reasoning.as_deref(),
            Some("thinking")
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
    fn stream_response_deserializes_reasoning_field() {
        let json = r#"{"choices":[{"delta":{"reasoning":"trace"},"finish_reason":null}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            response.choices[0].delta.reasoning.as_deref(),
            Some("trace")
        );
    }

    #[test]
    fn provider_name() {
        use crate::traits::Provider;
        let provider = OpenRouterProvider::new("key");
        assert_eq!(provider.name(), "openrouter");
    }

    #[test]
    fn provider_capabilities() {
        use crate::traits::Provider;
        let provider = OpenRouterProvider::new("key");
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }
}
