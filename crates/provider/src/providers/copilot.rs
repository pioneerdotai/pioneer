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

use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

const BASE_URL: &str = "https://api.githubcopilot.com";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub struct CopilotProvider {
    api_key: String,
    client: Client,
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
    data: Vec<ApiModelEntry>,
}

#[derive(Debug, Deserialize)]
struct ApiModelEntry {
    id: String,
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    owned_by: Option<String>,
}

// ── Implementation ──────────────────────────────────────────────────────────

impl CopilotProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");

        Self {
            api_key: api_key.into(),
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

    fn convert_messages(prepared: &PreparedProviderMessages) -> Result<Vec<ApiMessage>> {
        prepared
            .messages
            .iter()
            .enumerate()
            .map(|(message_index, m)| {
                let attachments = prepared
                    .attachments_for_message(message_index)
                    .collect::<Vec<_>>();
                let content = if attachments.is_empty() {
                    if m.content.is_empty() && m.tool_calls.is_some() {
                        None
                    } else {
                        Some(ApiMessageContent::Text(m.content.clone()))
                    }
                } else {
                    let mut parts = Vec::new();
                    if !m.content.is_empty() {
                        parts.push(ApiContentPart::Text {
                            text: m.content.clone(),
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
                                        format: Self::audio_format_from_mime(
                                            attachment.mime_type.as_str(),
                                        ),
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
                                    "provider `copilot` does not support {:?} attachments in chat/completions yet",
                                    attachment.kind
                                ));
                            }
                        }
                    }
                    Some(ApiMessageContent::Parts(parts))
                };

                Ok(ApiMessage {
                    role: match m.role {
                        Role::System => "system".into(),
                        Role::User => "user".into(),
                        Role::Assistant => "assistant".into(),
                        Role::Tool => "tool".into(),
                    },
                    content,
                    tool_calls: m.tool_calls.as_ref().map(|tool_calls| {
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
                    }),
                    tool_call_id: m.tool_call_id.clone(),
                    name: m.name.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()
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
        format!("{}/chat/completions", BASE_URL)
    }

    fn models_url(&self) -> String {
        format!("{}/models", BASE_URL)
    }

    async fn api_error(response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read error body>".into());
        anyhow!("Copilot API error ({status}): {body}")
    }
}

#[async_trait]
impl crate::traits::Provider for CopilotProvider {
    fn name(&self) -> &str {
        "copilot"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: true,
            tool_calling: true,
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::disabled(),
                image: InputTypeSupport {
                    native: true,
                    file_upload: false,
                    data_url_inline: true,
                    text_fallback: false,
                },
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
        let api_request = ApiChatRequest {
            model: request.model,
            messages: Self::convert_messages(&prepared)?,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            parallel_tool_calls: request.parallel_tool_calls,
            stream: false,
        };

        let response = self
            .client
            .post(self.chat_completions_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Editor-Version", "pioneer/0.1.0")
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
            .ok_or_else(|| anyhow!("no response from Copilot"))?;
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
            return Err(anyhow!("no response from Copilot"));
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
        let api_request = ApiChatRequest {
            model: request.model,
            messages: Self::convert_messages(&prepared)?,
            temperature: request.temperature,
            max_tokens: request.max_tokens,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            parallel_tool_calls: request.parallel_tool_calls,
            stream: true,
        };

        let response = self
            .client
            .post(self.chat_completions_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Editor-Version", "pioneer/0.1.0")
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
                                if let Some(reasoning) =
                                    choice.delta.reasoning_content.or(choice.delta.reasoning)
                                {
                                    if !reasoning.is_empty() {
                                        let _ =
                                            tx.send(Ok(StreamChunk::reasoning(reasoning))).await;
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
            .header("Editor-Version", "pioneer/0.1.0")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: ModelsListResponse = response.json().await?;

        Ok(api_response
            .data
            .into_iter()
            .map(|m| ProviderModelInfo {
                id: m.id.clone(),
                name: None,
                description: None,
                created: m.created,
                provider: "copilot".to_owned(),
                owned_by: m.owned_by.or(Some("github".to_owned())),
                limits: ProviderModelLimits::default(),
                capabilities: ProviderModelCapabilities::default(),
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
    use crate::types::ChatMessage;

    #[test]
    fn creates_with_api_key() {
        let provider = CopilotProvider::new("ghu_test_token");
        assert_eq!(provider.api_key, "ghu_test_token");
    }

    #[test]
    fn chat_completions_url_built_correctly() {
        let provider = CopilotProvider::new("key");
        assert_eq!(
            provider.chat_completions_url(),
            "https://api.githubcopilot.com/chat/completions"
        );
    }

    #[test]
    fn convert_messages_maps_roles() {
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let provider = CopilotProvider::new("key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = CopilotProvider::convert_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "system");
        assert_eq!(api_messages[1].role, "user");
        assert_eq!(api_messages[2].role, "assistant");
    }

    #[test]
    fn provider_name() {
        let provider = CopilotProvider::new("key");
        assert_eq!(provider.name(), "copilot");
    }

    #[test]
    fn provider_capabilities() {
        let provider = CopilotProvider::new("key");
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }
}
