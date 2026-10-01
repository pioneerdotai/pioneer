use super::embedding::{ApiEmbeddingData, ApiEmbeddingUsage, ordered_vectors, validate_input};
use crate::failure::ProviderStreamIncomplete;
use crate::{
    attachments::{
        PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes, attachment_data_url,
        ensure_no_unrendered_attachments, prepare_messages_for_provider_async,
    },
    tools::call::{StreamToolCallAccumulator, StreamToolCallDelta, StreamToolFunctionDelta},
    tools::parse::parse_tool_calls,
    tools::stream::{IncrementalLineDecoder, sse_data},
    types::{
        ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, InputContentType,
        InputTypeSupport, ProviderCapabilities, ProviderInputCapabilities, ProviderReplayState,
        ProviderTermination, ProviderTimeoutPolicy, ReasoningConfig, ReasoningEffort, Role,
        StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
    },
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};

use pioneer_protocol::{
    ProviderErrorReason, ProviderFailureStage, ProviderModelCapabilities, ProviderModelInfo,
    ProviderModelLimits, ProviderModelPricing, ProviderModelReasoningCapabilities,
    ProviderRequestId, ReasoningCapabilitySource,
};

pub(crate) const BASE_URL: &str = "https://openrouter.ai/api/v1";
const APP_REFERER: &str = "https://getpioneer.dev";
const APP_TITLE: &str = "Pioneer";
const APP_CATEGORIES: &str = "personal-agent,general-chat";

pub struct OpenRouterProvider {
    api_key: String,
    base_url: String,
    timeout_policy: ProviderTimeoutPolicy,
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
    reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_details: Option<Vec<serde_json::Value>>,
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
    reasoning_details: Option<Vec<serde_json::Value>>,
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

#[derive(Debug, Serialize)]
struct ApiEmbeddingRequest {
    model: String,
    input: Vec<String>,
    encoding_format: &'static str,
}

#[derive(Debug, Deserialize)]
struct ApiEmbeddingResponse {
    data: Vec<ApiEmbeddingData>,
    #[serde(default)]
    usage: Option<ApiEmbeddingUsage>,
}

// ── SSE streaming response types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct StreamResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    usage: Option<ApiUsage>,
    #[serde(default)]
    choices: Vec<StreamChoice>,
    #[serde(default)]
    error: Option<StreamError>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    #[serde(default)]
    error: Option<StreamError>,
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
    reasoning_details: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    tool_calls: Option<Vec<StreamToolCallDelta>>,
    #[serde(default)]
    function_call: Option<StreamToolFunctionDelta>,
}

impl StreamDelta {
    fn has_payload(&self) -> bool {
        self.content
            .as_deref()
            .is_some_and(|value| !value.is_empty())
            || self
                .reasoning_content
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            || self
                .reasoning
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            || self
                .reasoning_details
                .as_ref()
                .is_some_and(|details| !details.is_empty())
            || self
                .tool_calls
                .as_ref()
                .is_some_and(|calls| calls.iter().any(StreamToolCallDelta::has_payload))
            || self
                .function_call
                .as_ref()
                .is_some_and(StreamToolFunctionDelta::has_payload)
    }
}

#[derive(Debug, Deserialize)]
struct StreamError {
    #[serde(default, deserialize_with = "deserialize_error_metadata")]
    metadata: Option<ErrorMetadata>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    code: Option<serde_json::Value>,
    #[serde(default, rename = "type")]
    error_type: Option<String>,
}

#[derive(Debug)]
struct ErrorMetadata {
    error_type: Option<ProviderErrorReason>,
}

// Optional diagnostics must not invalidate the error envelope. Retain only the
// allowlisted fact; neither malformed metadata nor raw details enter an error.
fn deserialize_error_metadata<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<ErrorMetadata>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(value.as_object().map(|metadata| ErrorMetadata {
        error_type: metadata
            .get("error_type")
            .and_then(serde_json::Value::as_str)
            .and_then(ProviderErrorReason::from_openrouter_code),
    }))
}

/// Safe facts from this response only. A successful HTTP status is not a
/// generation error code. Valid body IDs supersede the validated header fallback.
#[derive(Default)]
struct ResponseFailureFacts {
    request_id: Option<String>,
    http_status: Option<u16>,
    retry_after_ms: Option<u64>,
}

impl ResponseFailureFacts {
    fn capture(response: &reqwest::Response) -> Self {
        Self {
            request_id: OpenRouterProvider::response_request_id(response),
            http_status: (!response.status().is_success()).then(|| response.status().as_u16()),
            retry_after_ms: response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(|seconds| seconds.saturating_mul(1000)),
        }
    }

    fn observe_body_id(&mut self, body: &serde_json::Value) {
        if let Some(id) = body
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| ProviderRequestId::try_from((*id).to_owned()).is_ok())
        {
            self.request_id = Some(id.to_owned());
        }
    }

    fn failure(&self, error: anyhow::Error, stage: ProviderFailureStage) -> anyhow::Error {
        // Classify the original failure, not the additional response status:
        // response facts must not change read/decode/limit recovery policy.
        let description = if error.is::<serde_json::Error>() {
            "malformed OpenRouter response".to_owned()
        } else if error.is::<std::string::FromUtf8Error>() {
            "provider response body was not valid UTF-8".to_owned()
        } else {
            error.to_string()
        };
        let source_status = error
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
            .map(|status| status.as_u16());
        let mut failure = OpenRouterFailure::from_envelope(
            None,
            self.request_id.clone(),
            source_status,
            self.retry_after_ms,
            &description,
            stage,
        );
        if let Some(oversized) = error.downcast_ref::<crate::types::ProviderHttpErrorBodyTooLarge>()
        {
            failure.classification.class =
                crate::failure::classify_http_error_body_too_large(oversized.status).class;
            failure.classification.http_status = Some(oversized.status);
        } else if error.is::<crate::types::ProviderResponseTooLarge>() {
            failure.classification.class = pioneer_protocol::ProviderFailureClass::ProviderRejected;
        }
        failure.classification.http_status =
            self.http_status.or(failure.classification.http_status);
        failure.classification.is_network_error =
            error.chain().any(|cause| cause.is::<reqwest::Error>());
        // No source chain: FromUtf8Error owns raw bytes, serde errors can own
        // provider-controlled values, and reqwest errors can carry secret URLs.
        failure.into()
    }
}

/// The raw envelope is used only for the established classification, then discarded.
#[derive(Debug)]
struct OpenRouterFailure {
    classification: crate::types::ProviderFailureClassification,
    // Only the established, data-free completion enum may survive as a source.
    incomplete: Option<ProviderStreamIncomplete>,
}

impl std::fmt::Display for OpenRouterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OpenRouter provider request failed")
    }
}

impl std::error::Error for OpenRouterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.incomplete
            .as_ref()
            .map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

impl OpenRouterFailure {
    fn from_envelope(
        error: Option<StreamError>,
        id: Option<String>,
        status: Option<u16>,
        retry_after_ms: Option<u64>,
        legacy_description: &str,
        stage: ProviderFailureStage,
    ) -> Self {
        let provider_code = crate::failure::extract_provider_code(legacy_description);
        let lower = legacy_description.to_ascii_lowercase();
        let class = crate::failure::classify_provider_failure_class(
            &lower,
            stage,
            status,
            provider_code.as_deref(),
        );
        let mut classification = crate::types::ProviderFailureClassification::new(class);
        classification.http_status = status;
        classification.retry_after_ms =
            retry_after_ms.or_else(|| crate::failure::extract_retry_after_ms(&lower));
        classification.error_reason = error
            .and_then(|error| error.metadata)
            .and_then(|metadata| metadata.error_type);
        classification.request_id = id.and_then(|id| ProviderRequestId::try_from(id).ok());
        Self {
            classification,
            incomplete: None,
        }
    }

    fn stream(error: StreamError, id: Option<String>) -> Self {
        Self::generation(error, id, ProviderFailureStage::MidStream, None)
    }

    fn generation(
        error: StreamError,
        id: Option<String>,
        stage: ProviderFailureStage,
        retry_after_ms: Option<u64>,
    ) -> Self {
        let status = error
            .code
            .as_ref()
            .and_then(|code| {
                code.as_u64()
                    .or_else(|| code.as_str().and_then(|code| code.parse().ok()))
            })
            .and_then(|code| u16::try_from(code).ok())
            .filter(|code| (100..=599).contains(code));
        let description = error.description();
        Self::from_envelope(Some(error), id, status, retry_after_ms, &description, stage)
    }

    // Classify the original transport failure before discarding its text and
    // source chain. Adding an ID must not turn a transport failure into a 5xx.
    fn stream_transport(error: anyhow::Error, id: Option<String>) -> anyhow::Error {
        let incomplete = crate::failure::provider_stream_incomplete(&error);
        let description = error.to_string();
        let status = error
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
            .map(|status| status.as_u16());
        let mut failure = Self::from_envelope(
            None,
            id,
            status,
            None,
            &description,
            ProviderFailureStage::MidStream,
        );
        if error.is::<crate::types::ProviderResponseTooLarge>() {
            failure.classification.class = pioneer_protocol::ProviderFailureClass::ProviderRejected;
        }
        failure.incomplete = incomplete;
        failure.into()
    }

    fn http(
        body: &str,
        status: u16,
        retry_after_ms: Option<u64>,
        request_id: Option<String>,
    ) -> Self {
        let value = serde_json::from_str::<serde_json::Value>(body).ok();
        let mut facts = ResponseFailureFacts {
            request_id,
            http_status: Some(status),
            retry_after_ms,
        };
        if let Some(value) = &value {
            facts.observe_body_id(value);
        }
        let error = value
            .as_ref()
            .and_then(|value| value.get("error"))
            .and_then(|error| serde_json::from_value::<StreamError>(error.clone()).ok());
        Self::from_envelope(
            error,
            facts.request_id,
            Some(status),
            retry_after_ms,
            body,
            ProviderFailureStage::Connect,
        )
    }
}

impl StreamError {
    fn description(&self) -> String {
        let mut parts = Vec::new();
        if let Some(message) = self.message.as_ref().filter(|message| !message.is_empty()) {
            parts.push(message.clone());
        }
        if let Some(error_type) = self
            .error_type
            .as_ref()
            .filter(|error_type| !error_type.is_empty())
        {
            parts.push(format!("type={error_type}"));
        }
        if let Some(code) = &self.code {
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
    data: Vec<OpenRouterModelEntry>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterModelEntry {
    id: String,
    // Retain malformed marker lists as unknown rather than rejecting discovery
    // or interpreting partial lists as a trustworthy negative.
    #[serde(default)]
    supported_parameters: serde_json::Value,
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
    #[serde(default)]
    reasoning: Option<OpenRouterReasoningMetadata>,
}

#[derive(Debug, Deserialize)]
struct OpenRouterReasoningMetadata {
    #[serde(default)]
    supported_efforts: Option<Vec<String>>,
    #[serde(default)]
    default_effort: Option<String>,
    #[serde(default)]
    default_enabled: Option<bool>,
    #[serde(default)]
    mandatory: Option<bool>,
    #[serde(default)]
    supports_max_tokens: Option<bool>,
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
            base_url: base_url.into().trim_end_matches('/').to_owned(),
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

    fn reasoning_options(
        request_reasoning: Option<ReasoningConfig>,
    ) -> Option<ApiReasoningOptions> {
        let effort = match request_reasoning {
            Some(ReasoningConfig::Effort(effort)) => effort,
            Some(ReasoningConfig::Disabled) | None => return None,
        };

        Some(ApiReasoningOptions {
            effort: effort.as_str().to_owned(),
        })
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
                    reasoning_content: (message.role == Role::Assistant)
                        .then(|| message.reasoning_content.clone())
                        .flatten(),
                    reasoning_details: Self::replay_reasoning_details(message)?,
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
                    reasoning_content: None,
                    reasoning_details: None,
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
                    reasoning_content: None,
                    reasoning_details: None,
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
                reasoning_content: (message.role == Role::Assistant)
                    .then(|| message.reasoning_content.clone())
                    .flatten(),
                reasoning_details: Self::replay_reasoning_details(message)?,
                tool_calls,
                tool_call_id: message.tool_call_id.clone(),
                name: message.name.clone(),
            });
        }

        Ok(rendered)
    }

    fn replay_reasoning_details(
        message: &crate::types::ChatMessage,
    ) -> Result<Option<Vec<serde_json::Value>>> {
        if message.role != Role::Assistant {
            return Ok(None);
        }
        let Some(state) = message.provider_replay_state.as_ref() else {
            return Ok(None);
        };
        let payload = state.payload_for("openrouter").ok_or_else(|| {
            anyhow!(
                "provider replay state `{}` cannot be rendered by `openrouter`",
                state.provider
            )
        })?;
        serde_json::from_value(
            payload
                .get("reasoning_details")
                .cloned()
                .ok_or_else(|| anyhow!("openrouter replay state is missing `reasoning_details`"))?,
        )
        .map(Some)
        .map_err(|error| anyhow!("invalid openrouter replay state: {error}"))
    }

    fn reasoning_details_state(
        reasoning_details: Vec<serde_json::Value>,
    ) -> Option<ProviderReplayState> {
        (!reasoning_details.is_empty()).then(|| {
            ProviderReplayState::new(
                "openrouter",
                serde_json::json!({ "reasoning_details": reasoning_details }),
            )
        })
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

    fn models_request(&self) -> RequestBuilder {
        Self::with_app_attribution(
            self.client
                .get(self.models_url())
                .header("Authorization", format!("Bearer {}", self.api_key)),
        )
    }

    fn embeddings_url(&self) -> String {
        format!("{}/embeddings", self.base_url)
    }

    fn with_app_attribution(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header("HTTP-Referer", APP_REFERER)
            .header("X-OpenRouter-Title", APP_TITLE)
            .header("X-OpenRouter-Categories", APP_CATEGORIES)
    }

    fn response_request_id(response: &reqwest::Response) -> Option<String> {
        // A validated body ID takes precedence later. Among headers, prefer
        // OpenRouter's generation ID, then the existing request-ID fallback.
        ["x-generation-id", "x-request-id"]
            .into_iter()
            .find_map(|name| {
                response
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| ProviderRequestId::try_from((*value).to_owned()).is_ok())
                    .map(str::to_owned)
            })
    }

    async fn api_error(response: reqwest::Response) -> anyhow::Error {
        let facts = ResponseFailureFacts::capture(&response);
        let body = match crate::http::read_response_text_bounded(
            response,
            16 * 1024,
            "provider_error_body",
        )
        .await
        {
            Ok(body) => body,
            Err(error) => return facts.failure(error, ProviderFailureStage::Connect),
        };
        OpenRouterFailure::http(
            &body,
            facts.http_status.expect("non-success response"),
            facts.retry_after_ms,
            facts.request_id,
        )
        .into()
    }

    async fn list_model_entries(
        &self,
        request_builder: RequestBuilder,
    ) -> Result<Vec<OpenRouterModelEntry>> {
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
        Ok(api_response.data)
    }
}

#[async_trait]
impl crate::traits::Provider for OpenRouterProvider {
    fn classify_failure(
        &self,
        error: &anyhow::Error,
    ) -> Option<crate::types::ProviderFailureClassification> {
        error
            .downcast_ref::<OpenRouterFailure>()
            .map(|failure| failure.classification.clone())
    }

    fn name(&self) -> &str {
        "openrouter"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: true,
            tool_calling: true,
            embeddings: true,
            transcription: false,
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

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        let request_builder = Self::with_app_attribution(
            self.client
                .get(self.auth_key_url())
                .header("Authorization", format!("Bearer {}", self.api_key)),
        );
        crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?
            .error_for_status()?;
        Ok(crate::ProviderWarmupOutcome::Completed)
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
        let rendered_messages = Self::convert_messages(&prepared)?;
        let reasoning = Self::reasoning_options(request.reasoning);

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

        let request_builder = Self::with_app_attribution(
            self.client
                .post(self.chat_completions_url())
                .header("Authorization", format!("Bearer {}", self.api_key)),
        )
        .json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let mut facts = ResponseFailureFacts::capture(&response);
        let body: serde_json::Value = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await
        .map_err(|error| facts.failure(error, ProviderFailureStage::Connect))?;
        facts.observe_body_id(&body);
        // Chat Completions can report generation failure inside HTTP 200,
        // either at the top level or in a choice. The envelope code, not 200,
        // is the failure status in this case.
        if let Some(error) = body
            .get("error")
            .filter(|error| !error.is_null())
            .or_else(|| {
                body.get("choices")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|choices| {
                        choices
                            .iter()
                            .find_map(|choice| choice.get("error").filter(|error| !error.is_null()))
                    })
            })
        {
            let error: StreamError = serde_json::from_value(error.clone())
                .map_err(|error| facts.failure(error.into(), ProviderFailureStage::Connect))?;
            return Err(OpenRouterFailure::generation(
                error,
                facts.request_id,
                ProviderFailureStage::Connect,
                facts.retry_after_ms,
            )
            .into());
        }
        let api_response: ApiChatResponse = serde_json::from_value(body)
            .map_err(|error| facts.failure(error.into(), ProviderFailureStage::Connect))?;
        let usage = api_response.usage.map(|u| TokenUsage {
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
        });

        let choice = api_response.choices.into_iter().next().ok_or_else(|| {
            facts.failure(
                anyhow!("no response from OpenRouter"),
                ProviderFailureStage::Connect,
            )
        })?;
        let termination = choice
            .finish_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| ProviderTermination::Unknown("missing_finish_reason".to_owned()));
        let message = choice.message;

        let provider_replay_state =
            Self::reasoning_details_state(message.reasoning_details.clone().unwrap_or_default());
        let text = message.effective_content();
        let tool_calls =
            parse_tool_calls(message.tool_calls.as_ref(), message.function_call.as_ref())
                .map_err(|error| facts.failure(error, ProviderFailureStage::Connect))?;
        let reasoning_content = message.reasoning_content.or(message.reasoning);

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(facts.failure(
                anyhow!("no response from OpenRouter"),
                ProviderFailureStage::Connect,
            ));
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
        Ok(self.stream_chat_with_diagnostics(request).await?.stream)
    }

    async fn stream_chat_with_diagnostics(
        &self,
        request: ChatRequest,
    ) -> Result<crate::ProviderStream> {
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
        let rendered_messages = Self::convert_messages(&prepared)?;
        let reasoning = Self::reasoning_options(request.reasoning);

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

        let request_builder = Self::with_app_attribution(
            self.client
                .post(self.chat_completions_url())
                .header("Authorization", format!("Bearer {}", self.api_key)),
        )
        .json(&api_request);
        let response =
            crate::http::send_stream_request(request_builder, self.timeout_policy).await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let request_id = Self::response_request_id(&response);
        let diagnostics = crate::ProviderStreamDiagnostics::default();
        if let Some(id) = request_id
            .clone()
            .and_then(|id| ProviderRequestId::try_from(id).ok())
        {
            diagnostics.set_request_id(id);
        }
        let stream_diagnostics = diagnostics.clone();
        let byte_stream = crate::http::bounded_response_stream(
            response,
            crate::types::ProviderResponseLimits::default().max_transport_bytes,
            "provider_stream",
        );

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            let mut decoder = IncrementalLineDecoder::default();
            let mut tool_call_accumulator = StreamToolCallAccumulator::default();
            let mut terminal_reason = None;
            let mut generation_id = request_id;
            let mut reasoning_details = Vec::new();

            tokio::pin!(byte_stream);

            while let Some(result) = tokio::select! {
                biased;
                _ = tx.closed() => return,
                result = byte_stream.next() => result,
            } {
                let bytes = match result {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        if tx
                            .send(Err(OpenRouterFailure::stream_transport(
                                e,
                                generation_id.clone(),
                            )))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        return;
                    }
                };

                let lines = match decoder.push(bytes.as_ref()) {
                    Ok(lines) => lines,
                    Err(error) => {
                        if tx
                            .send(Err(OpenRouterFailure::stream_transport(
                                error,
                                generation_id.clone(),
                            )))
                            .await
                            .is_err()
                        {
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

                    if data.trim() == "[DONE]" {
                        let terminal = terminal_reason
                            .take()
                            .map(StreamChunk::final_chunk_with)
                            .ok_or_else(|| {
                                anyhow::Error::from(
                                    ProviderStreamIncomplete::DoneWithoutFinishReason,
                                )
                            });
                        let _ = tx
                            .send(terminal.map_err(|error| {
                                OpenRouterFailure::stream_transport(error, generation_id.clone())
                            }))
                            .await;
                        return;
                    }

                    match serde_json::from_str::<StreamResponse>(data) {
                        Ok(mut resp) => {
                            if let Some(id) = resp
                                .id
                                .clone()
                                .filter(|id| ProviderRequestId::try_from(id.clone()).is_ok())
                            {
                                stream_diagnostics.set_request_id(
                                    ProviderRequestId::try_from(id.clone()).expect("validated ID"),
                                );
                                generation_id = Some(id);
                            }
                            if terminal_reason.is_some()
                                && resp.choices.iter().any(|choice| choice.delta.has_payload())
                            {
                                let _ = tx
                                    .send(Err(OpenRouterFailure::stream_transport(
                                        anyhow!("provider sent payload after finish_reason"),
                                        generation_id.clone(),
                                    )))
                                    .await;
                                return;
                            }
                            if let Some(usage) = resp.usage {
                                if tx
                                    .send(Ok(StreamChunk::usage(TokenUsage {
                                        input_tokens: usage.prompt_tokens,
                                        output_tokens: usage.completion_tokens,
                                    })))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            if let Some(error) = resp.error.take().or_else(|| {
                                resp.choices
                                    .iter_mut()
                                    .find_map(|choice| choice.error.take())
                            }) {
                                if tx
                                    .send(Err(OpenRouterFailure::stream(
                                        error,
                                        generation_id.clone(),
                                    )
                                    .into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                return;
                            }
                            for choice in resp.choices {
                                if terminal_reason.is_some() {
                                    if choice.delta.has_payload() {
                                        let _ = tx
                                            .send(Err(OpenRouterFailure::stream_transport(
                                                anyhow!(
                                                    "provider sent payload after finish_reason"
                                                ),
                                                generation_id.clone(),
                                            )))
                                            .await;
                                        return;
                                    }
                                    continue;
                                }
                                if let Some(details) = choice.delta.reasoning_details {
                                    reasoning_details.extend(details);
                                }
                                if let Some(rc) =
                                    choice.delta.reasoning_content.or(choice.delta.reasoning)
                                {
                                    if !rc.is_empty() {
                                        if tx.send(Ok(StreamChunk::reasoning(rc))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                if let Some(content) = choice.delta.content {
                                    if !content.is_empty() {
                                        if tx.send(Ok(StreamChunk::delta(content))).await.is_err() {
                                            return;
                                        }
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
                                    if let Some(state) = OpenRouterProvider::reasoning_details_state(
                                        std::mem::take(&mut reasoning_details),
                                    ) {
                                        if tx
                                            .send(Ok(StreamChunk::provider_replay_state(state)))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                    let tool_calls = match tool_call_accumulator.take_tool_calls() {
                                        Ok(calls) => calls,
                                        Err(error) => {
                                            if tx
                                                .send(Err(OpenRouterFailure::stream_transport(
                                                    error,
                                                    generation_id.clone(),
                                                )))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                            return;
                                        }
                                    };
                                    if !tool_calls.is_empty() {
                                        if tx
                                            .send(Ok(StreamChunk::tool_calls(tool_calls)))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                    terminal_reason = Some(termination);
                                }
                            }
                        }
                        Err(_) => {
                            if tx
                                .send(Err(OpenRouterFailure::stream_transport(
                                    anyhow!("malformed OpenRouter SSE frame"),
                                    generation_id.clone(),
                                )))
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

            let terminal = match decoder.finish() {
                Err(error) => Err(error),
                Ok(_) => terminal_reason
                    .map(StreamChunk::final_chunk_with)
                    .ok_or_else(|| {
                        anyhow::Error::from(ProviderStreamIncomplete::EofWithoutTerminalMarker)
                    }),
            };
            let _ = tx
                .send(terminal.map_err(|error| {
                    OpenRouterFailure::stream_transport(error, generation_id.clone())
                }))
                .await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(crate::ProviderStream {
            stream: Box::pin(chunk_stream),
            diagnostics,
        })
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        Ok(self
            .list_model_entries(self.models_request())
            .await?
            .into_iter()
            .map(provider_model_from_openrouter_model_entry)
            .collect())
    }

    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        Ok(self
            .list_model_entries(
                self.models_request()
                    .query(&[("output_modalities", "embeddings")]),
            )
            .await?
            .into_iter()
            .map(openrouter_embedding_model_from_openrouter_model_entry)
            .collect())
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        validate_input(&request.model, &request.input)?;
        let expected_count = request.input.len();
        let api_request = ApiEmbeddingRequest {
            model: request.model,
            input: request.input,
            encoding_format: "float",
        };

        let request_builder = Self::with_app_attribution(
            self.client
                .post(self.embeddings_url())
                .header("Authorization", format!("Bearer {}", self.api_key))
                .json(&api_request),
        );
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let response = crate::http::read_response_json_bounded::<ApiEmbeddingResponse>(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;
        Ok(EmbeddingResponse {
            embeddings: ordered_vectors(response.data, expected_count)?,
            usage: response.usage.map(Into::into),
        })
    }
}

const OPENROUTER_GATEWAY_REASONING_EFFORTS: &[&str] =
    &["max", "xhigh", "high", "medium", "low", "minimal", "none"];

fn provider_model_from_openrouter_model_entry(m: OpenRouterModelEntry) -> ProviderModelInfo {
    let pricing = m.pricing.map(|p| {
        let parse = |s: &Option<String>| s.as_ref().and_then(|v| v.parse::<f64>().ok());
        ProviderModelPricing {
            input_token: parse(&p.prompt),
            output_token: parse(&p.completion),
            image: parse(&p.image),
            request: parse(&p.request),
        }
    });
    let reasoning = m.reasoning.and_then(openrouter_reasoning_capabilities);
    let mut capabilities = ProviderModelCapabilities {
        tool_calling: crate::catalog::tool_support_from_marker_list(
            &m.supported_parameters,
            "tools",
        ),
        ..Default::default()
    };
    if let Some(reasoning) = reasoning {
        capabilities.thinking = reasoning.supported;
        capabilities.reasoning = Some(reasoning);
    }

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
        capabilities,
        transcription: None,
        pricing,
        active: Some(true),
        family: None,
        lifecycle_status: None,
    }
}

// Test fixtures cross the same native response and normalizer boundary as
// list_model_entries -> list_models; no precomputed capability is injected.
#[cfg(test)]
pub(crate) fn models_from_native_discovery_fixture(json: &str) -> Vec<ProviderModelInfo> {
    let response: ModelsListResponse = serde_json::from_str(json).expect("native models response");
    response
        .data
        .into_iter()
        .map(provider_model_from_openrouter_model_entry)
        .collect()
}

fn openrouter_embedding_model_from_openrouter_model_entry(
    m: OpenRouterModelEntry,
) -> ProviderModelInfo {
    let mut model = provider_model_from_openrouter_model_entry(m);
    model.capabilities.embeddings = Some(true);
    model.family = Some("embedding".to_owned());
    model
}

fn openrouter_reasoning_capabilities(
    metadata: OpenRouterReasoningMetadata,
) -> Option<ProviderModelReasoningCapabilities> {
    let effort_options = metadata
        .supported_efforts
        .map(|efforts| {
            efforts
                .into_iter()
                .filter_map(|effort| {
                    ReasoningEffort::canonical_value(effort.as_str()).map(str::to_owned)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| {
            OPENROUTER_GATEWAY_REASONING_EFFORTS
                .iter()
                .map(|effort| (*effort).to_owned())
                .collect()
        });
    let default_effort = metadata
        .default_effort
        .as_deref()
        .and_then(ReasoningEffort::canonical_value)
        .map(str::to_owned);
    if effort_options.is_empty()
        && default_effort.is_none()
        && metadata.default_enabled != Some(true)
        && metadata.mandatory != Some(true)
        && metadata.supports_max_tokens != Some(true)
    {
        return None;
    }

    Some(ProviderModelReasoningCapabilities {
        supported: Some(true),
        effort_options,
        default_effort,
        mandatory: metadata.mandatory,
        supports_token_budget: metadata.supports_max_tokens,
        source: Some(ReasoningCapabilitySource::ProviderMetadata),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_wire_contract_has_actual_endpoint_model_input_encoding_and_usage() {
        let provider = OpenRouterProvider::new("fixture-key");
        assert_eq!(
            provider.embeddings_url(),
            "https://openrouter.ai/api/v1/embeddings"
        );
        let input = vec!["first".to_owned(), "second".to_owned()];
        let body = ApiEmbeddingRequest {
            model: "openai/text-embedding-3-small".to_owned(),
            input: input.clone(),
            encoding_format: "float",
        };
        assert_eq!(
            serde_json::to_value(body).unwrap(),
            serde_json::json!({
                "model": "openai/text-embedding-3-small", "input": input, "encoding_format": "float"
            })
        );
        let response: ApiEmbeddingResponse = serde_json::from_value(serde_json::json!({
            "data": [{"index":1,"embedding":[2.0]},{"index":0,"embedding":[1.0]}],
            "usage": {"prompt_tokens":7,"total_tokens":7}
        }))
        .unwrap();
        assert_eq!(
            ordered_vectors(response.data, 2).unwrap(),
            vec![vec![1.0], vec![2.0]]
        );
        let usage: crate::types::TokenUsage = response.usage.unwrap().into();
        assert_eq!(usage.input_tokens, Some(7));
        assert_eq!(usage.output_tokens, Some(0));
        for bad in [serde_json::json!(-1), serde_json::json!(0.5)] {
            assert!(
                serde_json::from_value::<ApiEmbeddingResponse>(serde_json::json!({
                    "data":[{"index":bad,"embedding":[1.0]}]
                }))
                .is_err()
            );
        }
    }
    use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
    use crate::providers::OpenAiCompatibleProvider;
    use crate::traits::Provider;
    use crate::types::{
        AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart,
        MessageProvenance, MessageSourceRef, ProviderToolCall, ReasoningConfig, ReasoningEffort,
    };

    fn complete_round(messages: &mut [ChatMessage]) {
        for (index, message) in messages.iter_mut().enumerate() {
            message.provenance = Some(MessageProvenance {
                logical_turn_id: Some("turn".into()),
                workspace_id: "workspace".into(),
                thread_id: "thread".into(),
                context_thread: None,
                unit_id: "round".into(),
                sources: vec![MessageSourceRef {
                    scope: "event:turn".into(),
                    id: format!("source-{index}"),
                    version: "revision:1".into(),
                }],
                complete: true,
                protected_input: false,
                inherited: false,
                source_aliases: vec![],
                ambiguous_input_aliases: vec![],
            });
        }
    }

    fn model_from_json(json: &str) -> ProviderModelInfo {
        let response: ModelsListResponse = serde_json::from_str(json).expect("models response");
        provider_model_from_openrouter_model_entry(
            response.data.into_iter().next().expect("fixture model"),
        )
    }

    fn embedding_model_from_json(json: &str) -> ProviderModelInfo {
        let response: ModelsListResponse = serde_json::from_str(json).expect("models response");
        openrouter_embedding_model_from_openrouter_model_entry(
            response.data.into_iter().next().expect("fixture model"),
        )
    }

    #[test]
    fn native_supported_parameters_preserves_tri_state() {
        for (field, expected) in [
            (r#", "supported_parameters": ["tools"]"#, Some(true)),
            (
                r#", "supported_parameters": ["temperature", "tools"]"#,
                Some(true),
            ),
            (r#", "supported_parameters": ["tool_choice"]"#, Some(false)),
            (r#", "supported_parameters": []"#, Some(false)),
            ("", None),
            (r#", "supported_parameters": null"#, None),
            (r#", "supported_parameters": "tools""#, None),
            (r#", "supported_parameters": {}"#, None),
            (r#", "supported_parameters": [42]"#, None),
            (r#", "supported_parameters": ["tools", null]"#, None),
        ] {
            let json = format!(r#"{{"data":[{{"id":"custom-model"{field}}}]}}"#);
            let model = models_from_native_discovery_fixture(&json).remove(0);
            assert_eq!(model.capabilities.tool_calling, expected, "{field}");
        }
    }

    #[test]
    fn model_reasoning_metadata_maps_supported_efforts_array() {
        let model = model_from_json(
            r#"{
                "data": [{
                    "id": "google/gemini-3.5-flash",
                    "reasoning": {
                        "supported_efforts": ["maximum", "extra-high", "medium", "low", "minimal"],
                        "default_effort": "Extra High",
                        "default_enabled": true,
                        "mandatory": true,
                        "supports_max_tokens": false
                    }
                }]
            }"#,
        );

        let reasoning = model.capabilities.reasoning.expect("reasoning metadata");
        assert_eq!(
            reasoning.effort_options,
            vec!["max", "xhigh", "medium", "low", "minimal"]
        );
        assert_eq!(reasoning.default_effort.as_deref(), Some("xhigh"));
        assert_eq!(reasoning.mandatory, Some(true));
        assert_eq!(reasoning.supports_token_budget, Some(false));
        assert_eq!(
            reasoning.source,
            Some(ReasoningCapabilitySource::ProviderMetadata)
        );
    }

    #[test]
    fn model_reasoning_metadata_null_efforts_uses_gateway_values() {
        let model = model_from_json(
            r#"{
                "data": [{
                    "id": "anthropic/claude",
                    "reasoning": {
                        "supported_efforts": null,
                        "supports_max_tokens": true
                    }
                }]
            }"#,
        );

        let reasoning = model.capabilities.reasoning.expect("reasoning metadata");
        assert_eq!(
            reasoning.effort_options,
            vec!["max", "xhigh", "high", "medium", "low", "minimal", "none"]
        );
        assert_eq!(reasoning.supports_token_budget, Some(true));
    }

    #[test]
    fn openrouter_embedding_model_mapping_marks_embedding_capability() {
        let model = embedding_model_from_json(
            r#"{
                "data": [{
                    "id": "openai/text-embedding-3-large",
                    "name": "OpenAI Text Embedding 3 Large",
                    "description": "Embedding model returned by OpenRouter",
                    "created": 1700000000,
                    "pricing": { "prompt": "0.00000013" }
                }]
            }"#,
        );

        assert_eq!(model.id, "openai/text-embedding-3-large");
        assert_eq!(model.name.as_deref(), Some("OpenAI Text Embedding 3 Large"));
        assert_eq!(
            model.description.as_deref(),
            Some("Embedding model returned by OpenRouter")
        );
        assert_eq!(model.capabilities.embeddings, Some(true));
        assert_eq!(model.family.as_deref(), Some("embedding"));
        assert_eq!(
            model
                .pricing
                .as_ref()
                .and_then(|pricing| pricing.input_token),
            Some(0.00000013)
        );
    }

    #[test]
    fn openrouter_embedding_models_request_filters_by_output_modality() {
        let provider = OpenRouterProvider::with_base_url("key", "http://localhost:8080");
        let request = provider
            .models_request()
            .query(&[("output_modalities", "embeddings")])
            .build()
            .expect("models request should build");

        assert_eq!(
            request.url().as_str(),
            "http://localhost:8080/models?output_modalities=embeddings"
        );
        assert_eq!(
            request
                .headers()
                .get("Authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer key")
        );
    }

    #[test]
    fn model_without_reasoning_metadata_leaves_capability_unset() {
        let model = model_from_json(
            r#"{
                "data": [{
                    "id": "openai/gpt-4o",
                    "pricing": { "prompt": "0.1", "completion": "0.2" }
                }]
            }"#,
        );

        assert!(model.capabilities.reasoning.is_none());
        assert!(model.capabilities.thinking.is_none());
        assert_eq!(
            model
                .pricing
                .as_ref()
                .and_then(|pricing| pricing.input_token),
            Some(0.1)
        );
    }

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
    fn custom_base_url_keeps_prefix_without_double_slash() {
        let provider = OpenRouterProvider::with_base_url("key", "http://localhost:8080/gateway/");
        assert_eq!(
            provider.models_url(),
            "http://localhost:8080/gateway/models"
        );
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
    fn app_attribution_headers_are_added_to_requests() {
        let provider = OpenRouterProvider::new("key");
        let request = OpenRouterProvider::with_app_attribution(
            provider
                .client
                .get("https://openrouter.ai/api/v1/models")
                .header("Authorization", "Bearer key"),
        )
        .build()
        .unwrap();

        let headers = request.headers();
        assert_eq!(
            headers
                .get("HTTP-Referer")
                .and_then(|value| value.to_str().ok()),
            Some(APP_REFERER)
        );
        assert_eq!(
            headers
                .get("X-OpenRouter-Title")
                .and_then(|value| value.to_str().ok()),
            Some(APP_TITLE)
        );
        assert_eq!(
            headers
                .get("X-OpenRouter-Categories")
                .and_then(|value| value.to_str().ok()),
            Some(APP_CATEGORIES)
        );
        assert_eq!(
            headers
                .get("Authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer key")
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
        assert_eq!(
            api_messages[2].reasoning_content.as_deref(),
            Some("signed reasoning")
        );
    }

    #[test]
    fn convert_messages_replays_openrouter_reasoning_details_unchanged() {
        let reasoning_details = serde_json::json!([
            {
                "type": "reasoning.encrypted",
                "data": "opaque-encrypted-state",
                "id": "reasoning-1",
                "format": "unknown"
            },
            {
                "type": "reasoning.summary",
                "summary": "provider-generated summary",
                "id": "reasoning-2",
                "format": "unknown"
            }
        ]);
        let assistant = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            Some("visible reasoning"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "inspect".to_owned(),
                arguments: "{}".to_owned(),
            }],
            Some(ProviderReplayState::for_model(
                "openrouter",
                "same-model",
                serde_json::json!({ "reasoning_details": reasoning_details.clone() }),
            )),
        );

        let provider = OpenRouterProvider::new("test-key");
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "same-model",
            &provider.capabilities(),
            &[assistant],
        )
        .unwrap();
        let api_messages = OpenRouterProvider::convert_messages(&prepared).unwrap();

        assert_eq!(
            api_messages[0].reasoning_details,
            serde_json::from_value::<Vec<serde_json::Value>>(reasoning_details).ok()
        );
    }

    #[test]
    fn deepseek_completed_tool_round_renders_for_openrouter_without_foreign_replay() {
        let calls = vec![ProviderToolCall {
            id: "call_1".to_owned(),
            name: "inspect".to_owned(),
            arguments: "{}".to_owned(),
        }];
        let replay = OpenAiCompatibleProvider::assistant_replay_state(
            "deepseek",
            None,
            Some("portable deepseek reasoning".to_owned()),
            calls.as_slice(),
        );
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                Some("portable deepseek reasoning"),
                calls,
                Some(replay.clone()),
            ),
            ChatMessage::tool_result("call_1", "inspect", "tool outcome"),
        ];
        complete_round(&mut messages);
        let canonical = messages.clone();

        let provider = OpenRouterProvider::new("test-key");
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "openrouter-target-model",
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let wire = OpenRouterProvider::convert_messages(&prepared).unwrap();

        assert_eq!(wire.len(), 2);
        assert_eq!(
            wire[0].reasoning_content.as_deref(),
            Some("portable deepseek reasoning")
        );
        assert!(wire[0].reasoning_details.is_none());
        assert_eq!(wire[0].tool_calls.as_ref().unwrap()[0].id, "call_1");
        assert_eq!(wire[1].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(canonical[0].provider_replay_state.as_ref(), Some(&replay));
        assert_eq!(messages, canonical);
    }

    #[test]
    fn openrouter_model_change_uses_portable_reasoning_not_source_model_replay() {
        let replay = ProviderReplayState::for_model(
            "openrouter",
            "source-model",
            serde_json::json!({"reasoning_details":[
                {"type":"reasoning.encrypted","data":"opaque-source-state"},
                {"type":"reasoning.summary","summary":"portable model-change rationale"}
            ]}),
        );
        let mut messages = vec![ChatMessage::assistant("answer")];
        messages[0].provider_replay_state = Some(replay.clone());
        complete_round(&mut messages);
        let provider = OpenRouterProvider::new("test-key");

        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "target-model",
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let wire = OpenRouterProvider::convert_messages(&prepared).unwrap();

        assert!(wire[0].reasoning_details.is_none());
        assert_eq!(
            wire[0].reasoning_content.as_deref(),
            Some("portable model-change rationale")
        );
        assert_eq!(messages[0].provider_replay_state.as_ref(), Some(&replay));
    }

    #[test]
    fn foreign_replay_adds_distinct_readable_reasoning_to_actual_wire() {
        let replay = ProviderReplayState::for_model(
            "deepseek",
            "source-model",
            serde_json::json!({
                "schema_version":1,
                "assistant_message":{
                    "content":"answer",
                    "reasoning_content":"additional replay rationale",
                    "tool_calls":[]
                },
                "opaque":"opaque-signature"
            }),
        );
        let mut message = ChatMessage::assistant("answer");
        message.reasoning_content = Some("common rationale".into());
        message.provider_replay_state = Some(replay.clone());
        complete_round(std::slice::from_mut(&mut message));
        let canonical = message.clone();
        let provider = OpenRouterProvider::new("test-key");
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "target-model",
            &provider.capabilities(),
            &[message],
        )
        .unwrap();
        let wire = OpenRouterProvider::convert_messages(&prepared).unwrap();
        assert_eq!(
            wire[0].reasoning_content.as_deref(),
            Some("common rationale\n\nadditional replay rationale")
        );
        assert!(wire[0].reasoning_details.is_none());
        assert!(
            !serde_json::to_string(&wire)
                .unwrap()
                .contains("opaque-signature")
        );
        assert_eq!(canonical.provider_replay_state.as_ref(), Some(&replay));
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
            artifact: None,
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
                    reasoning_content: None,
                    reasoning_details: None,
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ApiMessage {
                    role: "user".into(),
                    content: Some(ApiMessageContent::Text("Hello".into())),
                    reasoning_content: None,
                    reasoning_details: None,
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
                reasoning_content: None,
                reasoning_details: None,
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
    fn reasoning_options_omit_absent_or_disabled_effort_and_serialize_explicit_none() {
        assert!(OpenRouterProvider::reasoning_options(None).is_none());
        assert!(OpenRouterProvider::reasoning_options(Some(ReasoningConfig::disabled())).is_none());

        let reasoning = OpenRouterProvider::reasoning_options(Some(ReasoningConfig::effort(
            ReasoningEffort::None,
        )))
        .expect("explicit none reasoning effort should serialize");

        assert_eq!(reasoning.effort, "none");
    }

    #[test]
    fn api_request_serializes_reasoning_options_for_any_model() {
        let request = ApiChatRequest {
            model: "anthropic/claude-sonnet-4".into(),
            messages: vec![ApiMessage {
                role: "user".into(),
                content: Some(ApiMessageContent::Text("Hello".into())),
                reasoning_content: None,
                reasoning_details: None,
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
    fn api_response_preserves_structured_reasoning_details() {
        let json = r#"{
            "choices": [{
                "message": {
                    "content": "",
                    "reasoning_details": [
                        {"type":"reasoning.encrypted","data":"opaque","id":"r1","format":"unknown"},
                        {"type":"reasoning.summary","summary":"summary","id":"r2","format":"unknown"}
                    ],
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "inspect", "arguments": "{}"}
                    }]
                }
            }]
        }"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        let details = response.choices[0]
            .message
            .reasoning_details
            .clone()
            .unwrap();
        let state = OpenRouterProvider::reasoning_details_state(details.clone()).unwrap();

        assert_eq!(
            state.payload_for("openrouter"),
            Some(&serde_json::json!({ "reasoning_details": details }))
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
        assert!(!response.choices[0].delta.has_payload());
    }

    #[test]
    fn stream_response_distinguishes_empty_terminal_repeats_from_real_payload() {
        for json in [
            r#"{"choices":[{"delta":{},"finish_reason":"stop"}]}"#,
            r#"{"choices":[{"delta":{"content":"","reasoning":"","tool_calls":[],"function_call":{}},"finish_reason":"stop"}]}"#,
        ] {
            let response: StreamResponse = serde_json::from_str(json).unwrap();
            assert!(!response.choices[0].delta.has_payload(), "{json}");
        }

        for json in [
            r#"{"choices":[{"delta":{"content":"late"},"finish_reason":null}]}"#,
            r#"{"choices":[{"delta":{"reasoning":"late"},"finish_reason":null}]}"#,
            r#"{"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.summary","summary":"late"}]},"finish_reason":null}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"late"}}]},"finish_reason":null}]}"#,
        ] {
            let response: StreamResponse = serde_json::from_str(json).unwrap();
            assert!(response.choices[0].delta.has_payload(), "{json}");
        }
    }

    #[test]
    fn stream_response_allows_usage_with_empty_post_terminal_choices() {
        let json = r#"{"usage":{"prompt_tokens":42,"completion_tokens":15},"choices":[{"delta":{},"finish_reason":"stop"}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();

        assert!(response.usage.is_some());
        assert!(!response.choices[0].delta.has_payload());
    }

    #[tokio::test]
    async fn chat_still_decodes_successful_completion_and_usage() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 8192];
            socket.read(&mut request).await.unwrap();
            let body = r#"{"id":"gen-success","choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}"#;
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let provider = crate::ProviderRegistry::with_provider(
            "openrouter",
            std::sync::Arc::new(OpenRouterProvider::with_base_url("key", url)),
        )
        .get_or_create("openrouter")
        .unwrap();
        let response = provider
            .chat(ChatRequest {
                model: "fixture".into(),
                messages: vec![ChatMessage::user("hello")],
                temperature: None,
                max_tokens: None,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            })
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(response.text, "hello");
        assert_eq!(response.termination, ProviderTermination::Complete);
        let usage = response.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(2));
        assert_eq!(usage.output_tokens, Some(1));
    }

    #[test]
    fn stream_transport_ids_do_not_change_failure_classes_or_retain_sources() {
        use pioneer_protocol::ProviderFailureClass;
        for (error, expected) in [
            (
                anyhow!("connection reset private_fixture"),
                ProviderFailureClass::NetworkTransient,
            ),
            (
                anyhow!("provider stream ended before a terminal marker"),
                ProviderFailureClass::StreamStall,
            ),
            (
                ProviderStreamIncomplete::EofWithoutTerminalMarker.into(),
                ProviderFailureClass::StreamStall,
            ),
            (
                ProviderStreamIncomplete::DoneWithoutFinishReason.into(),
                ProviderFailureClass::StreamStall,
            ),
            (
                IncrementalLineDecoder::default()
                    .push(&[0xff, b'\n'])
                    .unwrap_err(),
                ProviderFailureClass::StreamStall,
            ),
            (
                crate::types::ProviderResponseTooLarge::new("provider_stream", 10, 11).into(),
                ProviderFailureClass::ProviderRejected,
            ),
        ] {
            let incomplete = crate::failure::provider_stream_incomplete(&error);
            let error = OpenRouterFailure::stream_transport(error, Some("gen-fixture".to_owned()));
            assert_eq!(
                crate::failure::provider_stream_incomplete(&error),
                incomplete
            );
            let failure = error.downcast_ref::<OpenRouterFailure>().unwrap();
            assert_eq!(failure.classification.class, expected);
            assert_eq!(failure.classification.http_status, None);
            assert_eq!(
                failure.classification.request_id,
                Some(ProviderRequestId::try_from("gen-fixture".to_owned()).unwrap())
            );
            assert!(!format!("{error:#?}").contains("private_fixture"));
            assert_eq!(error.chain().count(), 1 + usize::from(incomplete.is_some()));
        }
    }

    #[test]
    fn structured_failures_discard_raw_details_and_keep_only_allowlisted_facts() {
        for reason in [
            serde_json::json!("provider_unavailable"),
            serde_json::json!("secret-reason"),
            serde_json::json!({"raw": "secret"}),
            serde_json::Value::Null,
        ] {
            let body = serde_json::json!({
                "id": "https://secret.example?key=credential",
                "error": {"code": 502, "message": "private credential provider_overloaded",
                    "metadata": {"error_type": reason, "raw": "private request history", "provider_code": "private credential"}}
            }).to_string();
            let failure =
                OpenRouterFailure::http(&body, 502, Some(5000), Some("req-fixture_123".to_owned()));
            assert_eq!(
                failure.classification.class,
                pioneer_protocol::ProviderFailureClass::Provider5xx
            );
            assert_eq!(failure.classification.retry_after_ms, Some(5000));
            assert_eq!(
                failure.classification.error_reason,
                (reason == "provider_unavailable")
                    .then_some(ProviderErrorReason::ProviderUnavailable)
            );
            assert!(failure.classification.provider_code.is_none());
            assert_eq!(
                failure.classification.request_id,
                Some(ProviderRequestId::try_from("req-fixture_123".to_owned()).unwrap())
            );
            let safe = format!("{failure:?} {failure}");
            for secret in [
                "credential",
                "secret.example",
                "secret-reason",
                "private",
                "history",
            ] {
                assert!(!safe.contains(secret));
            }
        }
    }

    #[test]
    fn stream_response_deserializes_error_envelope_without_delta() {
        let json = r#"{"error":{"message":"upstream failed","type":"provider_error","code":524},"choices":[{"finish_reason":"error"}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();

        assert_eq!(
            response.error.unwrap().description(),
            "upstream failed, type=provider_error, code=524"
        );
        assert_eq!(response.choices[0].finish_reason.as_deref(), Some("error"));
        assert!(response.choices[0].delta.content.is_none());
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
    fn stream_response_preserves_reasoning_detail_chunks_in_order() {
        let first: StreamResponse = serde_json::from_str(
            r#"{"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.encrypted","data":"first"}]},"finish_reason":null}]}"#,
        )
        .unwrap();
        let second: StreamResponse = serde_json::from_str(
            r#"{"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.summary","summary":"second"}]},"finish_reason":"tool_calls"}]}"#,
        )
        .unwrap();
        let mut details = first.choices[0].delta.reasoning_details.clone().unwrap();
        details.extend(second.choices[0].delta.reasoning_details.clone().unwrap());

        assert_eq!(
            details,
            vec![
                serde_json::json!({"type":"reasoning.encrypted","data":"first"}),
                serde_json::json!({"type":"reasoning.summary","summary":"second"}),
            ]
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

#[cfg(test)]
mod wire_contract_tests {
    use super::*;
    type WireProvider = OpenRouterProvider;
    fn wire_provider() -> WireProvider {
        WireProvider::new("fixture")
    }
    include!("wire_tests/chat.rs");
}
