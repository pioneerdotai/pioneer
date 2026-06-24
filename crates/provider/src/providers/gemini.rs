use crate::attachments::{
    PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes,
    ensure_no_unrendered_attachments, prepare_messages_for_provider,
};
use crate::reasoning_registry;
use crate::types::{
    ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
    ProviderInputCapabilities, ProviderTimeoutPolicy, ProviderToolCall, ReasoningConfig,
    ReasoningEffort, Role, StreamChunk, TokenUsage, ToolChoice,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use pioneer_protocol::{
    ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits,
    ProviderModelReasoningCapabilities, ReasoningCapabilitySource,
};

const BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";

pub struct GeminiProvider {
    api_key: String,
    timeout_policy: ProviderTimeoutPolicy,
    client: Client,
}

// ── Gemini API request types ────────────────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiGenerateRequest {
    contents: Vec<ApiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<ApiSystemInstruction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<ApiGenerationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ApiTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_config: Option<ApiToolConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiContent {
    role: String,
    parts: Vec<ApiPart>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiSystemInstruction {
    parts: Vec<ApiPart>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiPart {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inline_data: Option<ApiInlineData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file_data: Option<ApiFileData>,
    /// Gemini thinking models set `thought: true` on reasoning parts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thought: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    function_call: Option<ApiFunctionCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    function_response: Option<ApiFunctionResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiInlineData {
    mime_type: String,
    data: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiFileData {
    mime_type: String,
    file_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiFunctionCall {
    name: String,
    args: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiFunctionResponse {
    name: String,
    response: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiTool {
    function_declarations: Vec<ApiFunctionDeclaration>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiFunctionDeclaration {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiToolConfig {
    function_calling_config: ApiFunctionCallingConfig,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiFunctionCallingConfig {
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    allowed_function_names: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking_config: Option<ApiThinkingConfig>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiThinkingConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking_budget: Option<i32>,
}

// ── Gemini API response types ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiGenerateResponse {
    #[serde(default)]
    candidates: Vec<ApiCandidate>,
    #[serde(default)]
    usage_metadata: Option<ApiUsageMetadata>,
}

#[derive(Debug, Deserialize)]
struct ApiCandidate {
    #[serde(default)]
    content: Option<ApiContent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiUsageMetadata {
    #[serde(default)]
    prompt_token_count: Option<u64>,
    #[serde(default)]
    candidates_token_count: Option<u64>,
}

// ── List models response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GeminiModelsListResponse {
    #[serde(default)]
    models: Vec<GeminiModelEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiModelEntry {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    input_token_limit: Option<u64>,
    #[serde(default)]
    output_token_limit: Option<u64>,
    #[serde(default)]
    supported_generation_methods: Option<Vec<String>>,
    #[serde(default, alias = "supportedThinkingLevels")]
    thinking_levels: Option<Vec<String>>,
    #[serde(default, alias = "defaultThinkingLevel")]
    default_thinking_level: Option<String>,
}

// ── Implementation ──────────────────────────────────────────────────────────

impl GeminiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_timeout_policy(api_key, ProviderTimeoutPolicy::default())
    }

    pub fn with_timeout_policy(
        api_key: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        }
    }

    fn generate_content_url(&self, model: &str) -> String {
        format!(
            "{}/models/{}:generateContent?key={}",
            BASE_URL, model, self.api_key
        )
    }

    fn stream_generate_content_url(&self, model: &str) -> String {
        format!(
            "{}/models/{}:streamGenerateContent?alt=sse&key={}",
            BASE_URL, model, self.api_key
        )
    }

    fn list_models_url(&self) -> String {
        format!("{}/models?key={}", BASE_URL, self.api_key)
    }

    #[cfg(test)]
    fn build_request(request: &ChatRequest) -> ApiGenerateRequest {
        Self::build_request_result(request).expect("gemini request rendering should succeed")
    }

    #[cfg(test)]
    fn build_request_result(request: &ChatRequest) -> Result<ApiGenerateRequest> {
        let provider = GeminiProvider::new("test-key");
        let capabilities = <GeminiProvider as crate::traits::Provider>::capabilities(&provider);
        let prepared = prepare_messages_for_provider(
            "gemini",
            &capabilities,
            request
                .rendered_messages_with_compiled_sections()
                .as_slice(),
        )
        .expect("prepare_messages_for_provider should succeed");
        Self::build_request_from_prepared(request, &prepared)
    }

    fn attachment_part(attachment: &crate::attachments::PreparedAttachment) -> Result<ApiPart> {
        let (inline_data, file_data) = match &attachment.source {
            PreparedAttachmentSource::Reference { reference } => (
                None,
                Some(ApiFileData {
                    mime_type: attachment.mime_type.clone(),
                    file_uri: reference.clone(),
                }),
            ),
            _ => (
                Some(ApiInlineData {
                    mime_type: attachment.mime_type.clone(),
                    data: BASE64.encode(attachment_bytes(attachment)?),
                }),
                None,
            ),
        };

        Ok(ApiPart {
            text: None,
            inline_data,
            file_data,
            thought: None,
            function_call: None,
            function_response: None,
        })
    }

    fn build_request_from_prepared(
        request: &ChatRequest,
        prepared: &PreparedProviderMessages,
    ) -> Result<ApiGenerateRequest> {
        let mut system_parts: Vec<ApiPart> = Vec::new();
        let mut contents: Vec<ApiContent> = Vec::new();

        for (message_index, msg) in prepared.messages.iter().enumerate() {
            match msg.role {
                Role::System => {
                    system_parts.push(ApiPart {
                        text: Some(msg.content.clone()),
                        inline_data: None,
                        file_data: None,
                        thought: None,
                        function_call: None,
                        function_response: None,
                    });
                }
                _ => {
                    let role = match msg.role {
                        Role::User => "user",
                        Role::Assistant => "model",
                        Role::Tool => "user",
                        Role::System => unreachable!(),
                    };
                    let mut parts = Vec::new();

                    if msg.role == Role::Tool {
                        let name = msg.name.clone().unwrap_or_else(|| "tool".to_owned());
                        let response_payload =
                            serde_json::from_str::<serde_json::Value>(msg.content.as_str())
                                .unwrap_or_else(
                                    |_| serde_json::json!({ "content": msg.content.clone() }),
                                );
                        parts.push(ApiPart {
                            text: None,
                            inline_data: None,
                            file_data: None,
                            thought: None,
                            function_call: None,
                            function_response: Some(ApiFunctionResponse {
                                name,
                                response: response_payload,
                            }),
                        });
                    } else if !msg.content.is_empty() {
                        parts.push(ApiPart {
                            text: Some(msg.content.clone()),
                            inline_data: None,
                            file_data: None,
                            thought: None,
                            function_call: None,
                            function_response: None,
                        });
                    }

                    if let Some(tool_calls) = msg.tool_calls.as_ref() {
                        for call in tool_calls {
                            parts.push(ApiPart {
                                text: None,
                                inline_data: None,
                                file_data: None,
                                thought: None,
                                function_call: Some(ApiFunctionCall {
                                    name: call.name.clone(),
                                    args: parse_json_or_string(call.arguments.as_str()),
                                }),
                                function_response: None,
                            });
                        }
                    }

                    for attachment in prepared.attachments_for_message(message_index) {
                        match attachment.kind {
                            InputContentType::Image
                            | InputContentType::File
                            | InputContentType::Audio
                            | InputContentType::Video => {
                                parts.push(Self::attachment_part(attachment)?);
                            }
                            _ => {
                                return Err(anyhow!(
                                    "provider `gemini` does not support {:?} attachments",
                                    attachment.kind
                                ));
                            }
                        }
                    }

                    contents.push(ApiContent {
                        role: role.into(),
                        parts,
                    });
                }
            }
        }

        let system_instruction = if system_parts.is_empty() {
            None
        } else {
            Some(ApiSystemInstruction {
                parts: system_parts,
            })
        };

        let thinking_config = Self::thinking_config(request.reasoning)?;
        let generation_config = if request.temperature.is_some()
            || request.max_tokens.is_some()
            || thinking_config.is_some()
        {
            Some(ApiGenerationConfig {
                temperature: request.temperature,
                max_output_tokens: request.max_tokens,
                thinking_config,
            })
        } else {
            None
        };

        let tools = request.tools.as_ref().map(|tools| {
            vec![ApiTool {
                function_declarations: tools
                    .iter()
                    .map(|tool| ApiFunctionDeclaration {
                        name: tool.name.clone(),
                        description: tool.description.clone(),
                        parameters: tool.parameters.clone(),
                    })
                    .collect(),
            }]
        });

        let tool_config = request.tool_choice.clone().map(|choice| match choice {
            ToolChoice::Auto => ApiToolConfig {
                function_calling_config: ApiFunctionCallingConfig {
                    mode: "AUTO".to_owned(),
                    allowed_function_names: None,
                },
            },
            ToolChoice::None => ApiToolConfig {
                function_calling_config: ApiFunctionCallingConfig {
                    mode: "NONE".to_owned(),
                    allowed_function_names: None,
                },
            },
            ToolChoice::Required => ApiToolConfig {
                function_calling_config: ApiFunctionCallingConfig {
                    mode: "ANY".to_owned(),
                    allowed_function_names: None,
                },
            },
            ToolChoice::Tool { name } => ApiToolConfig {
                function_calling_config: ApiFunctionCallingConfig {
                    mode: "ANY".to_owned(),
                    allowed_function_names: Some(vec![name]),
                },
            },
        });

        Ok(ApiGenerateRequest {
            contents,
            system_instruction,
            generation_config,
            tools,
            tool_config,
        })
    }

    fn thinking_config(reasoning: Option<ReasoningConfig>) -> Result<Option<ApiThinkingConfig>> {
        let Some(reasoning) = reasoning else {
            return Ok(None);
        };

        let level = match reasoning {
            ReasoningConfig::Effort(
                effort @ (ReasoningEffort::Minimal
                | ReasoningEffort::Low
                | ReasoningEffort::Medium
                | ReasoningEffort::High),
            ) => effort.as_str(),
            ReasoningConfig::Disabled => return Ok(None),
            ReasoningConfig::Effort(ReasoningEffort::None) => {
                return Err(anyhow!(
                    "Gemini generateContent does not support reasoning effort `none` through thinkingConfig"
                ));
            }
            ReasoningConfig::Effort(effort @ (ReasoningEffort::XHigh | ReasoningEffort::Max)) => {
                return Err(anyhow!(
                    "Gemini generateContent does not support reasoning effort `{}` through thinkingConfig",
                    effort.as_str()
                ));
            }
        };

        Ok(Some(ApiThinkingConfig {
            thinking_level: Some(level.to_owned()),
            thinking_budget: None,
        }))
    }

    fn extract_text(response: &ApiGenerateResponse) -> Option<String> {
        let parts = response
            .candidates
            .first()
            .and_then(|c| c.content.as_ref())
            .map(|content| &content.parts)?;

        let text: String = parts
            .iter()
            .filter(|p| !p.thought.unwrap_or(false) && p.text.is_some())
            .filter_map(|p| p.text.as_deref())
            .collect::<Vec<_>>()
            .join("");

        if text.is_empty() { None } else { Some(text) }
    }

    fn extract_reasoning(response: &ApiGenerateResponse) -> Option<String> {
        let parts = response
            .candidates
            .first()
            .and_then(|c| c.content.as_ref())
            .map(|content| &content.parts)?;

        let reasoning: String = parts
            .iter()
            .filter(|p| p.thought.unwrap_or(false) && p.text.is_some())
            .filter_map(|p| p.text.as_deref())
            .collect::<Vec<_>>()
            .join("");

        if reasoning.is_empty() {
            None
        } else {
            Some(reasoning)
        }
    }

    fn extract_usage(response: &ApiGenerateResponse) -> Option<TokenUsage> {
        response.usage_metadata.as_ref().map(|u| TokenUsage {
            input_tokens: u.prompt_token_count,
            output_tokens: u.candidates_token_count,
        })
    }

    fn extract_tool_calls(response: &ApiGenerateResponse) -> Vec<ProviderToolCall> {
        let parts = match response
            .candidates
            .first()
            .and_then(|c| c.content.as_ref())
            .map(|content| &content.parts)
        {
            Some(parts) => parts,
            None => return Vec::new(),
        };

        parts
            .iter()
            .filter_map(|part| part.function_call.as_ref())
            .enumerate()
            .map(|(index, call)| ProviderToolCall {
                id: format!("call_{}", index + 1),
                name: call.name.clone(),
                arguments: serde_json::to_string(&call.args).unwrap_or_else(|_| "{}".to_owned()),
            })
            .collect()
    }

    async fn api_error(response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read error body>".into());
        anyhow!("Gemini API error ({status}): {body}")
    }
}

fn parse_json_or_string(raw: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(raw)
        .unwrap_or_else(|_| serde_json::Value::String(raw.to_owned()))
}

#[async_trait]
impl crate::traits::Provider for GeminiProvider {
    fn name(&self) -> &str {
        "gemini"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: true,
            tool_calling: true,
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::native_inline_only(),
                image: InputTypeSupport::native_inline_only(),
                audio: InputTypeSupport::native_inline_only(),
                video: InputTypeSupport::native_inline_only(),
            },
        }
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let model = request.model.clone();
        let prepared = prepare_messages_for_provider(
            self.name(),
            &self.capabilities(),
            request
                .rendered_messages_with_compiled_sections()
                .as_slice(),
        )?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let api_request = Self::build_request_from_prepared(&request, &prepared)?;

        let request_builder = self
            .client
            .post(self.generate_content_url(&model))
            .json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: ApiGenerateResponse = response.json().await?;
        let usage = Self::extract_usage(&api_response);

        let text = Self::extract_text(&api_response).unwrap_or_default();
        let reasoning_content = Self::extract_reasoning(&api_response);
        let tool_calls = Self::extract_tool_calls(&api_response);

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(anyhow!("no response from Gemini"));
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
        let model = request.model.clone();
        let prepared = prepare_messages_for_provider(
            self.name(),
            &self.capabilities(),
            request
                .rendered_messages_with_compiled_sections()
                .as_slice(),
        )?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let api_request = Self::build_request_from_prepared(&request, &prepared)?;

        let request_builder = self
            .client
            .post(self.stream_generate_content_url(&model))
            .json(&api_request);
        let response =
            crate::http::send_stream_request(request_builder, self.timeout_policy).await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let byte_stream = response.bytes_stream();

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            let mut buffer = String::new();
            let mut last_tool_calls: Vec<ProviderToolCall> = Vec::new();

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

                    match serde_json::from_str::<ApiGenerateResponse>(data) {
                        Ok(resp) => {
                            if let Some(reasoning) = Self::extract_reasoning(&resp) {
                                if !reasoning.is_empty() {
                                    let _ = tx.send(Ok(StreamChunk::reasoning(reasoning))).await;
                                }
                            }
                            if let Some(text) = Self::extract_text(&resp) {
                                if !text.is_empty() {
                                    let _ = tx.send(Ok(StreamChunk::delta(text))).await;
                                }
                            }
                            let tool_calls = Self::extract_tool_calls(&resp);
                            if !tool_calls.is_empty() && tool_calls != last_tool_calls {
                                last_tool_calls = tool_calls.clone();
                                let _ = tx.send(Ok(StreamChunk::tool_calls(tool_calls))).await;
                            }
                        }
                        Err(e) => {
                            tracing::debug!("failed to parse Gemini SSE chunk: {e}");
                        }
                    }
                }
            }

            let _ = tx.send(Ok(StreamChunk::final_chunk())).await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(chunk_stream))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let request_builder = self.client.get(self.list_models_url());
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: GeminiModelsListResponse = response.json().await?;

        Ok(api_response
            .models
            .into_iter()
            .map(provider_model_from_gemini_model_entry)
            .collect())
    }
}

fn provider_model_from_gemini_model_entry(m: GeminiModelEntry) -> ProviderModelInfo {
    let id = m
        .name
        .as_deref()
        .unwrap_or("")
        .strip_prefix("models/")
        .unwrap_or(m.name.as_deref().unwrap_or(""))
        .to_owned();

    let supports_streaming = m
        .supported_generation_methods
        .as_ref()
        .is_some_and(|methods| methods.iter().any(|m| m == "streamGenerateContent"));
    let reasoning = gemini_reasoning_capabilities_for_model_entry(id.as_str(), &m);

    ProviderModelInfo {
        id,
        name: m.display_name,
        description: m.description,
        created: None,
        provider: "gemini".to_owned(),
        owned_by: Some("google".to_owned()),
        limits: ProviderModelLimits {
            max_input_tokens: m.input_token_limit,
            max_output_tokens: m.output_token_limit,
            context_window: match (m.input_token_limit, m.output_token_limit) {
                (Some(i), Some(o)) => Some(i + o),
                _ => None,
            },
        },
        capabilities: ProviderModelCapabilities {
            streaming: Some(supports_streaming),
            thinking: reasoning.as_ref().and_then(|reasoning| reasoning.supported),
            reasoning,
            ..ProviderModelCapabilities::default()
        },
        pricing: None,
        active: Some(true),
        family: None,
        lifecycle_status: None,
    }
}

fn gemini_reasoning_capabilities_for_model_entry(
    model_id: &str,
    entry: &GeminiModelEntry,
) -> Option<ProviderModelReasoningCapabilities> {
    if let Some(levels) = entry.thinking_levels.as_ref() {
        let effort_options = levels
            .iter()
            .filter_map(|level| canonical_gemini_thinking_level(level))
            .collect::<Vec<_>>();
        if !effort_options.is_empty() {
            return Some(ProviderModelReasoningCapabilities {
                supported: Some(true),
                effort_options,
                default_effort: entry
                    .default_thinking_level
                    .as_deref()
                    .and_then(canonical_gemini_thinking_level),
                mandatory: None,
                supports_token_budget: None,
                source: Some(ReasoningCapabilitySource::ProviderMetadata),
            });
        }
    }

    reasoning_registry::reasoning_capabilities_for_model("gemini", model_id)
}

fn canonical_gemini_thinking_level(level: &str) -> Option<String> {
    match level.trim().to_ascii_lowercase().as_str() {
        "minimal" => Some("minimal".to_owned()),
        "low" => Some("low".to_owned()),
        "medium" => Some("medium".to_owned()),
        "high" => Some("high".to_owned()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChatMessage, CompiledPromptPayload, ReasoningConfig, ReasoningEffort};

    #[test]
    fn creates_with_api_key() {
        let provider = GeminiProvider::new("test-gemini-key");
        assert_eq!(provider.api_key, "test-gemini-key");
    }

    #[test]
    fn generate_content_url_built_correctly() {
        let provider = GeminiProvider::new("my-key");
        let url = provider.generate_content_url("gemini-2.0-flash");
        assert_eq!(
            url,
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=my-key"
        );
    }

    #[test]
    fn stream_generate_content_url_built_correctly() {
        let provider = GeminiProvider::new("my-key");
        let url = provider.stream_generate_content_url("gemini-2.0-flash");
        assert_eq!(
            url,
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:streamGenerateContent?alt=sse&key=my-key"
        );
    }

    #[test]
    fn gemini_reasoning_registry_exposes_documented_gemini_3_flash_levels() {
        let reasoning = reasoning_registry::reasoning_capabilities_for_model(
            "gemini",
            "gemini-3-flash-preview",
        )
        .expect("gemini 3 flash thinking metadata");

        assert_eq!(
            reasoning.effort_options,
            vec!["minimal", "low", "medium", "high"]
        );
        assert_eq!(reasoning.default_effort.as_deref(), Some("high"));
    }

    #[test]
    fn gemini_reasoning_registry_exposes_documented_2_5_levels() {
        let reasoning =
            reasoning_registry::reasoning_capabilities_for_model("gemini", "gemini-2.5-flash")
                .expect("gemini 2.5 flash thinking metadata");

        assert_eq!(reasoning.effort_options, vec!["low", "medium", "high"]);
    }

    #[test]
    fn gemini_reasoning_metadata_from_model_entry_overrides_registry() {
        let entry = GeminiModelEntry {
            name: Some("models/custom-thinking".to_owned()),
            display_name: None,
            description: None,
            input_token_limit: None,
            output_token_limit: None,
            supported_generation_methods: None,
            thinking_levels: Some(vec!["low".to_owned(), "high".to_owned()]),
            default_thinking_level: Some("low".to_owned()),
        };

        let reasoning = gemini_reasoning_capabilities_for_model_entry("custom-thinking", &entry)
            .expect("provider metadata reasoning");
        assert_eq!(reasoning.effort_options, vec!["low", "high"]);
        assert_eq!(reasoning.default_effort.as_deref(), Some("low"));
        assert_eq!(
            reasoning.source,
            Some(ReasoningCapabilitySource::ProviderMetadata)
        );
    }

    #[test]
    fn gemini_reasoning_registry_leaves_unknown_models_unset() {
        let entry = GeminiModelEntry {
            name: Some("models/gemini-unknown".to_owned()),
            display_name: None,
            description: None,
            input_token_limit: None,
            output_token_limit: None,
            supported_generation_methods: None,
            thinking_levels: None,
            default_thinking_level: None,
        };

        assert!(gemini_reasoning_capabilities_for_model_entry("gemini-unknown", &entry).is_none());
    }

    #[test]
    fn gemini_model_list_fixture_normalizes_reasoning_capabilities() {
        let response: GeminiModelsListResponse = serde_json::from_str(
            r#"{
                "models": [
                    {
                        "name": "models/gemini-3-flash-preview",
                        "displayName": "Gemini 3 Flash Preview",
                        "inputTokenLimit": 1000,
                        "outputTokenLimit": 2000,
                        "supportedGenerationMethods": ["generateContent", "streamGenerateContent"]
                    },
                    {
                        "name": "models/gemini-unknown",
                        "displayName": "Gemini Unknown"
                    }
                ]
            }"#,
        )
        .expect("fixture response");
        let models = response
            .models
            .into_iter()
            .map(provider_model_from_gemini_model_entry)
            .collect::<Vec<_>>();

        let reasoning = models[0]
            .capabilities
            .reasoning
            .as_ref()
            .expect("documented thinking model");
        assert_eq!(
            reasoning.effort_options,
            vec!["minimal", "low", "medium", "high"]
        );
        assert_eq!(models[0].capabilities.streaming, Some(true));
        assert_eq!(models[0].limits.context_window, Some(3000));

        assert!(models[1].capabilities.reasoning.is_none());
    }

    #[test]
    fn build_request_separates_system_messages() {
        let request = ChatRequest {
            model: "gemini-2.0-flash".into(),
            messages: vec![
                ChatMessage::system("Be helpful"),
                ChatMessage::user("Hello"),
                ChatMessage::assistant("Hi!"),
            ],
            temperature: Some(0.7),
            max_tokens: Some(1024),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };

        let api_req = GeminiProvider::build_request(&request);

        // System message extracted into systemInstruction
        let sys = api_req.system_instruction.unwrap();
        assert_eq!(sys.parts.len(), 1);
        assert_eq!(sys.parts[0].text.as_deref(), Some("Be helpful"));

        // Only non-system messages in contents
        assert_eq!(api_req.contents.len(), 2);
        assert_eq!(api_req.contents[0].role, "user");
        assert_eq!(api_req.contents[0].parts[0].text.as_deref(), Some("Hello"));
        assert_eq!(api_req.contents[1].role, "model");
        assert_eq!(api_req.contents[1].parts[0].text.as_deref(), Some("Hi!"));

        // Generation config present
        let config = api_req.generation_config.unwrap();
        assert_eq!(config.temperature, Some(0.7));
        assert_eq!(config.max_output_tokens, Some(1024));
        assert!(config.thinking_config.is_none());
    }

    #[test]
    fn build_request_no_system_message() {
        let request = ChatRequest {
            model: "gemini-2.0-flash".into(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };

        let api_req = GeminiProvider::build_request(&request);

        assert!(api_req.system_instruction.is_none());
        assert!(api_req.generation_config.is_none());
        assert_eq!(api_req.contents.len(), 1);
        assert_eq!(api_req.contents[0].role, "user");
    }

    #[test]
    fn build_request_multiple_system_messages() {
        let request = ChatRequest {
            model: "gemini-2.0-flash".into(),
            messages: vec![
                ChatMessage::system("Rule 1"),
                ChatMessage::system("Rule 2"),
                ChatMessage::user("Hello"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };

        let api_req = GeminiProvider::build_request(&request);

        let sys = api_req.system_instruction.unwrap();
        assert_eq!(sys.parts.len(), 2);
        assert_eq!(sys.parts[0].text.as_deref(), Some("Rule 1"));
        assert_eq!(sys.parts[1].text.as_deref(), Some("Rule 2"));
    }

    #[test]
    fn build_request_uses_compiled_prompt_sections_in_order() {
        let request = ChatRequest {
            model: "gemini-2.0-flash".into(),
            messages: vec![
                ChatMessage::system("legacy prompt should be ignored"),
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

        let api_req = GeminiProvider::build_request(&request);
        let sys = api_req
            .system_instruction
            .expect("system instruction should be built from compiled prompt");
        assert_eq!(sys.parts.len(), 2);
        assert_eq!(sys.parts[0].text.as_deref(), Some("Stable rules"));
        assert_eq!(sys.parts[1].text.as_deref(), Some("Dynamic runtime"));
    }

    #[test]
    fn api_request_serializes_correctly() {
        let request = ChatRequest {
            model: "gemini-2.0-flash".into(),
            messages: vec![
                ChatMessage::system("You are helpful"),
                ChatMessage::user("Hello"),
            ],
            temperature: Some(0.7),
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };

        let api_req = GeminiProvider::build_request(&request);
        let json = serde_json::to_string(&api_req).unwrap();

        assert!(json.contains("\"systemInstruction\""));
        assert!(json.contains("\"generationConfig\""));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"role\":\"user\""));
        assert!(!json.contains("\"maxOutputTokens\""));
        assert!(!json.contains("\"thinkingConfig\""));
    }

    #[test]
    fn api_request_serializes_reasoning_effort_as_thinking_config() {
        let request = ChatRequest {
            model: "gemini-3-flash-preview".into(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::Medium)),
            compiled_prompt: None,
        };

        let api_req = GeminiProvider::build_request(&request);
        let json = serde_json::to_value(&api_req).unwrap();

        assert_eq!(
            json["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "medium"
        );
        assert!(
            json["generationConfig"]["thinkingConfig"]
                .get("thinkingBudget")
                .is_none()
        );
    }

    #[test]
    fn build_request_omits_thinking_config_for_disabled_reasoning() {
        let request = ChatRequest {
            model: "gemini-3-flash-preview".into(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::disabled()),
            compiled_prompt: None,
        };

        let api_req = GeminiProvider::build_request(&request);
        let json = serde_json::to_value(&api_req).unwrap();

        assert!(json["generationConfig"].get("thinkingConfig").is_none());
    }

    #[test]
    fn build_request_rejects_unsupported_reasoning_effort() {
        let request = ChatRequest {
            model: "gemini-3-flash-preview".into(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::XHigh)),
            compiled_prompt: None,
        };

        let err = GeminiProvider::build_request_result(&request)
            .expect_err("xhigh should not be serialized for Gemini thinkingConfig");

        assert!(err.to_string().contains("xhigh"));
    }

    #[test]
    fn api_response_deserializes() {
        let json = r#"{
            "candidates": [{"content": {"role": "model", "parts": [{"text": "Hi from Gemini"}]}}]
        }"#;
        let response: ApiGenerateResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.candidates.len(), 1);
        let text = GeminiProvider::extract_text(&response).unwrap();
        assert_eq!(text, "Hi from Gemini");
    }

    #[test]
    fn api_response_with_usage() {
        let json = r#"{
            "candidates": [{"content": {"role": "model", "parts": [{"text": "Hello"}]}}],
            "usageMetadata": {"promptTokenCount": 42, "candidatesTokenCount": 15}
        }"#;
        let response: ApiGenerateResponse = serde_json::from_str(json).unwrap();
        let usage = GeminiProvider::extract_usage(&response).unwrap();
        assert_eq!(usage.input_tokens, Some(42));
        assert_eq!(usage.output_tokens, Some(15));
    }

    #[test]
    fn api_response_empty_candidates() {
        let json = r#"{"candidates":[]}"#;
        let response: ApiGenerateResponse = serde_json::from_str(json).unwrap();
        assert!(GeminiProvider::extract_text(&response).is_none());
    }

    #[test]
    fn api_response_no_usage() {
        let json = r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"Hi"}]}}]}"#;
        let response: ApiGenerateResponse = serde_json::from_str(json).unwrap();
        assert!(GeminiProvider::extract_usage(&response).is_none());
    }

    #[test]
    fn provider_name() {
        use crate::traits::Provider;
        let provider = GeminiProvider::new("key");
        assert_eq!(provider.name(), "gemini");
    }

    #[test]
    fn provider_capabilities() {
        use crate::traits::Provider;
        let provider = GeminiProvider::new("key");
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }

    #[test]
    fn role_mapping_user() {
        let request = ChatRequest {
            model: "m".into(),
            messages: vec![ChatMessage::user("hi")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let api_req = GeminiProvider::build_request(&request);
        assert_eq!(api_req.contents[0].role, "user");
    }

    #[test]
    fn role_mapping_assistant() {
        let request = ChatRequest {
            model: "m".into(),
            messages: vec![ChatMessage::user("hi"), ChatMessage::assistant("hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let api_req = GeminiProvider::build_request(&request);
        assert_eq!(api_req.contents[1].role, "model");
    }
}
