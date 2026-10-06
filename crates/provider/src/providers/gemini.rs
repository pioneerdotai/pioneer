use crate::attachments::{
    PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes,
    ensure_no_unrendered_attachments, prepare_messages_for_provider_async,
};
#[cfg(test)]
use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
use crate::reasoning_registry;
use crate::tools::stream::{IncrementalLineDecoder, sse_data};
use crate::types::{
    ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
    ProviderInputCapabilities, ProviderReplayState, ProviderTermination, ProviderTimeoutPolicy,
    ProviderToolCall, ReasoningConfig, ReasoningEffort, Role, StreamChunk, TokenUsage, ToolChoice,
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

pub(crate) const BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";

pub struct GeminiProvider {
    api_key: String,
    base_url: String,
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
    #[serde(
        default,
        deserialize_with = "deserialize_optional_string",
        skip_serializing_if = "String::is_empty"
    )]
    role: String,
    #[serde(default)]
    parts: Vec<ApiPart>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiSystemInstruction {
    parts: Vec<ApiPart>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
    #[serde(
        default,
        rename = "thoughtSignature",
        skip_serializing_if = "Option::is_none"
    )]
    // REST bytes fields are base64 strings; retain the opaque representation.
    thought_signature: Option<String>,
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
    #[serde(
        default,
        deserialize_with = "deserialize_optional_string",
        skip_serializing_if = "String::is_empty"
    )]
    mime_type: String,
    file_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiFunctionCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    name: String,
    #[serde(
        default = "empty_json_object",
        deserialize_with = "deserialize_function_arguments"
    )]
    args: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiFunctionResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
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
    #[serde(default, rename = "finishReason")]
    finish_reason: Option<String>,
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
        Self::with_base_url_and_timeout_policy(api_key, BASE_URL, timeout_policy)
    }

    pub fn with_base_url_and_timeout_policy(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        }
    }

    fn generate_content_url(&self, model: &str) -> String {
        format!(
            "{}/models/{}:generateContent?key={}",
            self.base_url, model, self.api_key
        )
    }

    fn stream_generate_content_url(&self, model: &str) -> String {
        format!(
            "{}/models/{}:streamGenerateContent?alt=sse&key={}",
            self.base_url, model, self.api_key
        )
    }

    fn list_models_url(&self) -> String {
        format!("{}/models?key={}", self.base_url, self.api_key)
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
            thought_signature: None,
        })
    }

    fn build_request_from_prepared(
        request: &ChatRequest,
        prepared: &PreparedProviderMessages,
    ) -> Result<ApiGenerateRequest> {
        let request = crate::tools::policy::prepare_request("gemini", request.clone())?;
        let mut prepared = prepared.clone();
        crate::tools::policy::prepare_history("gemini", &mut prepared.messages)?;
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
                        thought_signature: None,
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
                        // FunctionResponse.response is a protobuf Struct, not an
                        // arbitrary JSON value. Preserve scalar/array results inside it.
                        let response_payload =
                            match serde_json::from_str::<serde_json::Value>(msg.content.as_str()) {
                                Ok(value) if value.is_object() => value,
                                Ok(value) => serde_json::json!({ "content": value }),
                                Err(_) => serde_json::json!({ "content": msg.content }),
                            };
                        parts.push(ApiPart {
                            text: None,
                            inline_data: None,
                            file_data: None,
                            thought: None,
                            function_call: None,
                            function_response: Some(ApiFunctionResponse {
                                id: msg.tool_call_id.clone(),
                                name,
                                response: response_payload,
                            }),
                            thought_signature: None,
                        });
                    } else if !msg.content.is_empty() {
                        parts.push(ApiPart {
                            text: Some(msg.content.clone()),
                            inline_data: None,
                            file_data: None,
                            thought: None,
                            function_call: None,
                            function_response: None,
                            thought_signature: None,
                        });
                    }

                    let function_call_signatures = if msg.role == Role::Assistant {
                        msg.provider_replay_state
                            .as_ref()
                            .map(|state| {
                                let payload =
                                    state.payload_for("gemini").ok_or_else(|| {
                                        anyhow!(
                                            "provider replay state `{}` cannot be rendered by `gemini`",
                                            state.provider
                                        )
                                    })?;
                                serde_json::from_value::<Vec<Option<String>>>(
                                    payload
                                        .get("function_call_signatures")
                                        .cloned()
                                        .ok_or_else(|| {
                                            anyhow!(
                                                "gemini replay state is missing `function_call_signatures`"
                                            )
                                        })?,
                                )
                                .map_err(|error| {
                                    anyhow!("invalid gemini replay state: {error}")
                                })
                            })
                            .transpose()?
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };

                    if let Some(tool_calls) = msg.tool_calls.as_ref() {
                        for (call_index, call) in tool_calls.iter().enumerate() {
                            parts.push(ApiPart {
                                text: None,
                                inline_data: None,
                                file_data: None,
                                thought: None,
                                function_call: Some(ApiFunctionCall {
                                    id: Some(call.id.clone()),
                                    name: call.name.clone(),
                                    args: parse_function_arguments(call.arguments.as_str())?,
                                }),
                                function_response: None,
                                thought_signature: function_call_signatures
                                    .get(call_index)
                                    .cloned()
                                    .flatten(),
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
                id: call
                    .id
                    .clone()
                    .unwrap_or_else(|| format!("call_{}", index + 1)),
                name: call.name.clone(),
                arguments: serde_json::to_string(&call.args).unwrap_or_else(|_| "{}".to_owned()),
            })
            .collect()
    }

    fn extract_provider_replay_state(
        response: &ApiGenerateResponse,
    ) -> Option<ProviderReplayState> {
        let parts = response
            .candidates
            .first()
            .and_then(|candidate| candidate.content.as_ref())
            .map(|content| &content.parts)?;
        let signatures = parts
            .iter()
            .filter(|part| part.function_call.is_some())
            .map(|part| part.thought_signature.clone())
            .collect::<Vec<_>>();
        signatures.iter().any(Option::is_some).then(|| {
            ProviderReplayState::new(
                "gemini",
                serde_json::json!({ "function_call_signatures": signatures }),
            )
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
        anyhow!("Gemini API error ({status}): {body}")
    }
}

fn deserialize_optional_string<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn empty_json_object() -> serde_json::Value {
    serde_json::json!({})
}

fn deserialize_function_arguments<'de, D>(
    deserializer: D,
) -> std::result::Result<serde_json::Value, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let arguments =
        Option::<serde_json::Value>::deserialize(deserializer)?.unwrap_or_else(empty_json_object);
    if !arguments.is_object() {
        return Err(serde::de::Error::custom(
            "Gemini functionCall args must be a JSON object",
        ));
    }
    Ok(arguments)
}

fn parse_function_arguments(raw: &str) -> Result<serde_json::Value> {
    let arguments: serde_json::Value = serde_json::from_str(raw)
        .map_err(|_| anyhow!("Gemini functionCall args must be a JSON object"))?;
    if !arguments.is_object() {
        return Err(anyhow!("Gemini functionCall args must be a JSON object"));
    }
    Ok(arguments)
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
            embeddings: false,
            transcription: false,
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
        let request = crate::tools::policy::prepare_request(self.name(), request)?;
        let model = request.model.clone();
        let prepared = prepare_messages_for_provider_async(
            self.name(),
            request.model.as_str(),
            &self.capabilities(),
            request
                .rendered_messages_with_compiled_sections()
                .as_slice(),
        )
        .await?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let api_request = Self::build_request_from_prepared(&request, &prepared)?;

        crate::attachments::validate_inline_payload("gemini", &api_request)?;
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

        let api_response: ApiGenerateResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;
        let usage = Self::extract_usage(&api_response);

        let text = Self::extract_text(&api_response).unwrap_or_default();
        let reasoning_content = Self::extract_reasoning(&api_response);
        let tool_calls = Self::extract_tool_calls(&api_response);
        let mut termination = api_response
            .candidates
            .first()
            .and_then(|candidate| candidate.finish_reason.as_deref())
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| ProviderTermination::Unknown("missing_finish_reason".to_owned()));
        if termination == ProviderTermination::Complete && !tool_calls.is_empty() {
            termination = ProviderTermination::ToolCalls;
        }
        let provider_replay_state = Self::extract_provider_replay_state(&api_response);

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(anyhow!("no response from Gemini"));
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

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let request = crate::tools::policy::prepare_request(self.name(), request)?;
        let model = request.model.clone();
        let prepared = prepare_messages_for_provider_async(
            self.name(),
            request.model.as_str(),
            &self.capabilities(),
            request
                .rendered_messages_with_compiled_sections()
                .as_slice(),
        )
        .await?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let api_request = Self::build_request_from_prepared(&request, &prepared)?;

        crate::attachments::validate_inline_payload("gemini", &api_request)?;
        let request_builder = self
            .client
            .post(self.stream_generate_content_url(&model))
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

        tokio::spawn(async move {
            let mut decoder = IncrementalLineDecoder::default();
            let mut last_tool_calls: Vec<ProviderToolCall> = Vec::new();
            let mut provider_replay_state = None;

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

                    match serde_json::from_str::<ApiGenerateResponse>(data) {
                        Ok(resp) => {
                            if let Some(usage) = Self::extract_usage(&resp) {
                                if tx.send(Ok(StreamChunk::usage(usage))).await.is_err() {
                                    return;
                                }
                            }
                            if let Some(state) = Self::extract_provider_replay_state(&resp) {
                                provider_replay_state = Some(state);
                            }
                            if let Some(reasoning) = Self::extract_reasoning(&resp) {
                                if !reasoning.is_empty() {
                                    if tx
                                        .send(Ok(StreamChunk::reasoning(reasoning)))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                            }
                            if let Some(text) = Self::extract_text(&resp) {
                                if !text.is_empty() {
                                    if tx.send(Ok(StreamChunk::delta(text))).await.is_err() {
                                        return;
                                    }
                                }
                            }
                            let tool_calls = Self::extract_tool_calls(&resp);
                            if !tool_calls.is_empty() && tool_calls != last_tool_calls {
                                last_tool_calls = tool_calls.clone();
                                if tx
                                    .send(Ok(StreamChunk::tool_calls(tool_calls)))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            if let Some(reason) = resp
                                .candidates
                                .first()
                                .and_then(|candidate| candidate.finish_reason.as_deref())
                            {
                                if let Some(state) = provider_replay_state.take() {
                                    if tx
                                        .send(Ok(StreamChunk::provider_replay_state(state)))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                let mut termination =
                                    ProviderTermination::from_openai_reason(reason);
                                if termination == ProviderTermination::Complete
                                    && !last_tool_calls.is_empty()
                                {
                                    termination = ProviderTermination::ToolCalls;
                                }
                                if tx
                                    .send(Ok(StreamChunk::final_chunk_with(termination)))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                return;
                            }
                        }
                        Err(e) => {
                            if tx
                                .send(Err(anyhow!("malformed Gemini SSE frame: {e}")))
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
                .unwrap_or_else(|| anyhow!("Gemini stream ended before finishReason"));
            if tx.send(Err(error)).await.is_err() {
                return;
            }
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

        let api_response: GeminiModelsListResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;

        Ok(api_response
            .models
            .into_iter()
            .map(provider_model_from_gemini_model_entry)
            .collect())
    }

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.list_models().await?;
        Ok(crate::ProviderWarmupOutcome::Completed)
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
        transcription: None,
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
    #[test]
    fn tool_modes_and_parallel_limit_use_native_config_or_error() {
        for (choice, expected) in [
            (ToolChoice::Auto, "AUTO"),
            (ToolChoice::None, "NONE"),
            (ToolChoice::Required, "ANY"),
            (
                ToolChoice::Tool {
                    name: "lookup".into(),
                },
                "ANY",
            ),
        ] {
            let mut request = crate::tools::policy::test_request();
            request.tool_choice = Some(choice);
            let wire = GeminiProvider::build_request_result(&request).unwrap();
            assert_eq!(
                wire.tool_config.unwrap().function_calling_config.mode,
                expected
            );
        }
        let mut request = crate::tools::policy::test_request();
        request.parallel_tool_calls = Some(false);
        assert!(GeminiProvider::build_request_result(&request).is_err());
        request.parallel_tool_calls = Some(true);
        assert!(GeminiProvider::build_request_result(&request).is_ok());
    }

    #[test]
    fn usage_prompt_includes_cached_content_once() {
        let response: super::ApiGenerateResponse = serde_json::from_value(serde_json::json!({
            "candidates":[], "usageMetadata": {"promptTokenCount":140,
                "cachedContentTokenCount":100,"candidatesTokenCount":9}
        }))
        .unwrap();
        let usage = super::GeminiProvider::extract_usage(&response).unwrap();
        assert_eq!(usage.input_tokens, Some(140));
        assert_eq!(usage.output_tokens, Some(9));
    }

    use super::*;
    use crate::traits::Provider;
    use crate::types::{
        ChatMessage, CompiledPromptPayload, MessageProvenance, MessageSourceRef, ReasoningConfig,
        ReasoningEffort,
    };

    #[test]
    fn foreign_reasoning_is_present_in_gemini_wire_as_unsigned_text() {
        let provider = GeminiProvider::new("key");
        let mut message = ChatMessage::assistant("answer");
        message.provider_replay_state = Some(ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"reasoning_details":[{
                "type":"reasoning.summary","summary":"meaningful rationale"
            }]}),
        ));
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
        let request = ChatRequest {
            model: "gemini-target".into(),
            messages: vec![message],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            request.model.as_str(),
            &provider.capabilities(),
            request.messages.as_slice(),
        )
        .unwrap();
        let wire = GeminiProvider::build_request_from_prepared(&request, &prepared).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        assert!(json.contains("meaningful rationale"));
        assert!(json.contains("portable unsigned text"));
        assert!(!json.contains("reasoning_details"));
    }

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
    fn custom_base_url_routes_generate_stream_and_models_without_repeating_api_prefix() {
        let provider = GeminiProvider::with_base_url_and_timeout_policy(
            "key",
            "http://localhost:8080/team/v1beta/",
            ProviderTimeoutPolicy::default(),
        );
        assert_eq!(
            provider.generate_content_url("fixture"),
            "http://localhost:8080/team/v1beta/models/fixture:generateContent?key=key"
        );
        assert_eq!(
            provider.stream_generate_content_url("fixture"),
            "http://localhost:8080/team/v1beta/models/fixture:streamGenerateContent?alt=sse&key=key"
        );
        assert_eq!(
            provider.list_models_url(),
            "http://localhost:8080/team/v1beta/models?key=key"
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
    fn build_request_replays_thought_signature_on_original_function_call() {
        let message = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            Some("summary"),
            vec![
                ProviderToolCall {
                    id: "call_1".to_owned(),
                    name: "first".to_owned(),
                    arguments: "{}".to_owned(),
                },
                ProviderToolCall {
                    id: "call_2".to_owned(),
                    name: "second".to_owned(),
                    arguments: "{}".to_owned(),
                },
            ],
            Some(ProviderReplayState::new(
                "gemini",
                serde_json::json!({
                    "function_call_signatures": ["opaque-signature", null]
                }),
            )),
        );
        let request = ChatRequest {
            model: "gemini-3-flash-preview".to_owned(),
            messages: vec![
                message,
                ChatMessage::tool_result("call_1", "first", "{}"),
                ChatMessage::tool_result("call_2", "second", "{}"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let provider = GeminiProvider::new("key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            request.messages.as_slice(),
        )
        .unwrap();
        let rendered = GeminiProvider::build_request_from_prepared(&request, &prepared).unwrap();

        assert_eq!(
            rendered.contents[0].parts[0].thought_signature.as_deref(),
            Some("opaque-signature")
        );
        assert!(rendered.contents[0].parts[1].thought_signature.is_none());
        for (content, id) in rendered.contents[1..].iter().zip(["call_1", "call_2"]) {
            let result = content.parts[0].function_response.as_ref().unwrap();
            assert_eq!(result.id.as_deref(), Some(id));
            assert!(content.parts[0].thought_signature.is_none());
        }
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

#[cfg(test)]
mod media_contract_tests {
    use super::*;
    use crate::{
        AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart, Provider,
    };
    #[test]
    fn every_declared_kind_uses_native_inline_bytes_and_mime_not_chat_content_parts() {
        let provider = GeminiProvider::new("unused");
        for (mime, kind) in [
            ("image/png", 0),
            ("application/pdf", 1),
            ("audio/wav", 2),
            ("video/mp4", 3),
        ] {
            let bytes = match kind {
                0 => crate::attachments::regression::image(image::ImageFormat::Png, 1, 1),
                1 => crate::attachments::regression::pdf(1),
                2 => crate::attachments::regression::wav(),
                _ => crate::attachments::regression::video().to_vec(),
            };
            let attachment = MessageAttachment {
                mime_type: mime.into(),
                name: None,
                size_bytes: None,
                sha256: None,
                source: AttachmentDataSource::Bytes {
                    base64_data: BASE64.encode(&bytes),
                },
                artifact: None,
            };
            let part = match kind {
                0 => MessageContentPart::image(attachment),
                1 => MessageContentPart::file(attachment),
                2 => MessageContentPart::audio(attachment),
                _ => MessageContentPart::video(attachment),
            };
            let prepared = crate::attachments::prepare_messages_for_provider(
                "gemini",
                &provider.capabilities(),
                &[ChatMessage::user_parts(vec![part])],
            )
            .unwrap();
            let native = GeminiProvider::attachment_part(&prepared.attachments[0]).unwrap();
            assert!(native.file_data.is_none());
            let inline = native.inline_data.unwrap();
            assert_eq!(inline.mime_type, mime);
            assert_eq!(inline.data, BASE64.encode(&bytes));
            // Canonical ApiPart JSON field names are the g02 dependency. This
            // fixture asserts representation without blessing snake_case wire.
        }
    }
}

#[cfg(test)]
mod async_media_admission_regressions {
    use super::*;
    use crate::Provider;
    use crate::attachments::regression as fixture;
    use std::sync::Arc;
    #[tokio::test]
    async fn four_typed_inputs_are_budgeted_pinned_and_rendered_on_generate_content() {
        let provider = GeminiProvider::new("unused");
        let state = Arc::new(fixture::state("gemini", "media", serde_json::json!({})));
        let req = fixture::request(
            "media",
            vec![
                fixture::part(
                    InputContentType::Image,
                    "image/png",
                    &fixture::image(image::ImageFormat::Png, 1, 1),
                ),
                fixture::part(InputContentType::File, "application/pdf", &fixture::pdf(1)),
                fixture::part(InputContentType::Audio, "audio/wav", &fixture::wav()),
                fixture::part(InputContentType::Video, "video/mp4", fixture::video()),
            ],
        );
        let budget = fixture::scoped(state.clone(), provider.prepare_input_budget(req))
            .await
            .unwrap();
        assert_eq!(budget.media.len(), 4);
        let prepared = fixture::scoped(
            state,
            crate::attachments::prepare_messages_for_provider_async(
                "gemini",
                "media",
                &provider.capabilities(),
                &budget.request.messages,
            ),
        )
        .await
        .unwrap();
        let body = GeminiProvider::build_request_from_prepared(&budget.request, &prepared).unwrap();
        let parts = &body.contents[0].parts;
        for (index, mime) in ["image/png", "application/pdf", "audio/wav", "video/mp4"]
            .into_iter()
            .enumerate()
        {
            let native = parts[index + 1].inline_data.as_ref().unwrap();
            assert_eq!(native.mime_type, mime);
            let crate::AttachmentDataSource::Bytes { base64_data } =
                &fixture::attachment(&budget.request.messages[0].content_parts[index]).source
            else {
                panic!("must pin")
            };
            assert_eq!(&native.data, base64_data);
        }
        // Canonical ApiPart JSON field casing remains the explicit G02
        // dependency; this fixture verifies typed representation, not acceptance.
    }
}

#[cfg(test)]
mod webm_wire_regressions {
    use super::*;
    use crate::{
        Provider,
        attachments::{media_fixtures::webm, regression as fixture},
    };
    use std::sync::Arc;
    #[tokio::test]
    async fn identified_webm_video_budget_and_generate_content_keep_mime_and_bytes() {
        let provider = GeminiProvider::new("unused");
        for audio in [false, true] {
            let bytes = webm(audio, true, "webm");
            let state = Arc::new(fixture::state("gemini", "media", serde_json::json!({})));
            let budget = fixture::scoped(
                state.clone(),
                provider.prepare_input_budget(fixture::request(
                    "media",
                    vec![fixture::part(InputContentType::Video, "video/webm", &bytes)],
                )),
            )
            .await
            .unwrap();
            let prepared = fixture::scoped(
                state,
                crate::attachments::prepare_messages_for_provider_async(
                    "gemini",
                    "media",
                    &provider.capabilities(),
                    &budget.request.messages,
                ),
            )
            .await
            .unwrap();
            let wire =
                GeminiProvider::build_request_from_prepared(&budget.request, &prepared).unwrap();
            let native = wire.contents[0]
                .parts
                .iter()
                .find_map(|p| p.inline_data.as_ref())
                .unwrap();
            assert_eq!(native.mime_type, "video/webm");
            assert_eq!(native.data, BASE64.encode(&bytes));
        }
    }
}

#[cfg(test)]
mod container_timeline_wire_regressions {
    use super::*;
    use crate::{
        Provider,
        attachments::{
            media_fixtures::{TimingFixture, webm_timeline},
            regression as fixture,
        },
    };
    use std::sync::Arc;
    #[tokio::test]
    async fn eleven_second_container_budget_both_modes_and_replay_keep_native_bytes() {
        let provider = GeminiProvider::new("unused");
        let bytes = webm_timeline(
            true,
            true,
            "webm",
            TimingFixture {
                video_start: 10000,
                declared_duration: Some(11000.0),
                ..Default::default()
            },
        );
        let state = Arc::new(fixture::state(
            "gemini",
            "media",
            serde_json::json!({"video":{"maxDurationMillis":11000}}),
        ));
        let budget = fixture::scoped(
            state.clone(),
            provider.prepare_input_budget(fixture::request(
                "media",
                vec![fixture::part(InputContentType::Video, "video/webm", &bytes)],
            )),
        )
        .await
        .unwrap();
        assert_eq!(budget.media[0].input_tokens, 3850);
        for _stream in [false, true] {
            // both GenerateContent modes share this typed builder
            let replay = fixture::scoped(
                state.clone(),
                provider.prepare_input_budget(budget.request.clone()),
            )
            .await
            .unwrap();
            let prepared = fixture::scoped(
                state.clone(),
                crate::attachments::prepare_messages_for_provider_async(
                    "gemini",
                    "media",
                    &provider.capabilities(),
                    &replay.request.messages,
                ),
            )
            .await
            .unwrap();
            let wire =
                GeminiProvider::build_request_from_prepared(&replay.request, &prepared).unwrap();
            let native = wire.contents[0]
                .parts
                .iter()
                .find_map(|p| p.inline_data.as_ref())
                .unwrap();
            assert_eq!(native.mime_type, "video/webm");
            assert_eq!(native.data, BASE64.encode(&bytes));
        }
    }
}

#[cfg(test)]
mod confirmed_duration_wire_regressions {
    use super::*;
    use crate::{Provider, attachments::regression as fixture};
    use std::sync::Arc;
    #[tokio::test]
    async fn confirmed_elementary_audio_and_no_edit_mp4_keep_native_bytes_in_both_modes() {
        let provider = GeminiProvider::new("unused");
        for (kind, mime, bytes) in fixture::confirmed_wire_inputs("gemini") {
            let state = Arc::new(fixture::state("gemini", "media", serde_json::json!({})));
            let budget = fixture::scoped(
                state.clone(),
                provider.prepare_input_budget(fixture::request(
                    "media",
                    vec![fixture::part(kind, mime, &bytes)],
                )),
            )
            .await
            .unwrap();
            for _stream in [false, true] {
                let replay = fixture::scoped(
                    state.clone(),
                    provider.prepare_input_budget(budget.request.clone()),
                )
                .await
                .unwrap();
                let prepared = fixture::scoped(
                    state.clone(),
                    crate::attachments::prepare_messages_for_provider_async(
                        "gemini",
                        "media",
                        &provider.capabilities(),
                        &replay.request.messages,
                    ),
                )
                .await
                .unwrap();
                assert_eq!(prepared.attachments[0].kind, kind);
                assert_eq!(prepared.attachments[0].mime_type, mime);
                let wire = GeminiProvider::build_request_from_prepared(&replay.request, &prepared)
                    .unwrap();
                let native = wire.contents[0]
                    .parts
                    .iter()
                    .find_map(|p| p.inline_data.as_ref())
                    .unwrap();
                assert_eq!(native.mime_type, mime);
                assert_eq!(native.data, BASE64.encode(&bytes));
            }
        }
    }
}

#[cfg(test)]
#[path = "wire_tests/gemini.rs"]
mod wire_contract_tests;
