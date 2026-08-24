use crate::{
    attachments::{
        PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes, attachment_data_url,
        ensure_no_unrendered_attachments, prepare_messages_for_provider_async,
    },
    tools::call::{StreamToolCallAccumulator, StreamToolCallDelta, StreamToolFunctionDelta},
    tools::parse::{parse_embedded_tool_payload, parse_tool_calls},
    tools::stream::{IncrementalLineDecoder, sse_data},
    types::{
        ChatRequest, ChatResponse, InputContentType, ProviderCapabilities,
        ProviderInputCapabilities, ProviderTermination, ProviderTimeoutPolicy, Role, StreamChunk,
        TokenUsage, ToolChoice, ToolDefinition,
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

use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

const DEFAULT_BASE_URL: &str = "https://open.bigmodel.cn/api/paas/v4";

pub struct GlmProvider {
    api_key: String,
    base_url: String,
    timeout_policy: ProviderTimeoutPolicy,
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

impl GlmProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_timeout_policy(api_key, ProviderTimeoutPolicy::default())
    }

    pub fn with_timeout_policy(
        api_key: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self::with_base_url_and_timeout_policy(api_key, DEFAULT_BASE_URL, timeout_policy)
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
                                    "provider `glm` does not support {:?} attachments in chat/completions yet",
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
                    reasoning_content: (m.role == Role::Assistant)
                        .then(|| m.reasoning_content.clone())
                        .flatten(),
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
        format!("{}/chat/completions", self.base_url)
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
        anyhow!("GLM API error ({status}): {body}")
    }
}

#[async_trait]
impl crate::traits::Provider for GlmProvider {
    fn name(&self) -> &str {
        "glm"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: false,
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
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

        let request_builder = self
            .client
            .post(self.chat_completions_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
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

        let choice = api_response
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no response from GLM"))?;
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
            return Err(anyhow!("no response from GLM"));
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

        let request_builder = self
            .client
            .post(self.chat_completions_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&api_request);
        let response =
            crate::http::send_stream_request(request_builder, self.timeout_policy).await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let byte_stream = response.bytes_stream();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            let mut decoder = IncrementalLineDecoder::default();
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
                            .send(Err(anyhow!("GLM stream ended without a finish_reason")))
                            .await;
                        return;
                    }

                    match serde_json::from_str::<StreamResponse>(data) {
                        Ok(resp) => {
                            if let Some(error) = resp.error {
                                let _ = tx
                                    .send(Err(anyhow!("GLM stream error: {}", error.description())))
                                    .await;
                                return;
                            }
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
                                if let Some(reason) = choice.finish_reason {
                                    let termination =
                                        ProviderTermination::from_openai_reason(&reason);
                                    let tool_calls = match tool_call_accumulator.take_tool_calls() {
                                        Ok(calls) => calls,
                                        Err(error) => {
                                            let _ = tx.send(Err(error)).await;
                                            return;
                                        }
                                    };
                                    if !tool_calls.is_empty() {
                                        let _ =
                                            tx.send(Ok(StreamChunk::tool_calls(tool_calls))).await;
                                    }
                                    let _ = tx
                                        .send(Ok(StreamChunk::final_chunk_with(termination)))
                                        .await;
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(anyhow!("malformed GLM SSE frame: {e}"))).await;
                            return;
                        }
                    }
                }
            }

            let error = decoder
                .finish()
                .err()
                .unwrap_or_else(|| anyhow!("GLM stream ended before a provider terminal marker"));
            let _ = tx.send(Err(error)).await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(chunk_stream))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let request_builder = self
            .client
            .get(self.models_url())
            .header("Authorization", format!("Bearer {}", self.api_key));
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
            .map(|m| ProviderModelInfo {
                id: m.id.clone(),
                name: None,
                description: None,
                created: m.created,
                provider: "glm".to_owned(),
                owned_by: m.owned_by.or(Some("zhipu".to_owned())),
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

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.list_models().await?;
        Ok(crate::ProviderWarmupOutcome::Completed)
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
        let provider = GlmProvider::new("glm-test-key");
        assert_eq!(provider.api_key, "glm-test-key");
        assert_eq!(provider.base_url, DEFAULT_BASE_URL);
    }

    #[test]
    fn creates_with_custom_base_url() {
        let provider = GlmProvider::with_base_url("key", "http://localhost:8080");
        assert_eq!(provider.base_url, "http://localhost:8080");
    }

    #[test]
    fn chat_completions_url_built_correctly() {
        let provider = GlmProvider::new("key");
        assert_eq!(
            provider.chat_completions_url(),
            "https://open.bigmodel.cn/api/paas/v4/chat/completions"
        );
    }

    #[test]
    fn convert_messages_maps_roles() {
        let mut assistant = ChatMessage::assistant("Hi!");
        assistant.reasoning_content = Some("signed reasoning".to_owned());
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            assistant,
        ];

        let provider = GlmProvider::new("key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = GlmProvider::convert_messages(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "system");
        assert_eq!(api_messages[1].role, "user");
        assert_eq!(api_messages[2].role, "assistant");
        assert_eq!(
            api_messages[2].reasoning_content.as_deref(),
            Some("signed reasoning")
        );
    }

    #[test]
    fn provider_name() {
        let provider = GlmProvider::new("key");
        assert_eq!(provider.name(), "glm");
    }

    #[test]
    fn provider_capabilities() {
        let provider = GlmProvider::new("key");
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(!caps.vision);
    }
}
