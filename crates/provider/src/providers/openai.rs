use crate::{
    attachments::{
        AttachmentOperationError, AttachmentPipelineConfig, AttachmentTransportKind,
        PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes, attachment_data_url,
        default_attachment_pipeline_config, ensure_no_unrendered_attachments,
        lookup_uploaded_reference_with_artifact_for_authority, model_family_for_model,
        prepare_messages_for_provider_async, runtime, store_uploaded_reference_for_authority,
    },
    reasoning_registry,
    tools::call::{StreamToolCallAccumulator, StreamToolCallDelta, StreamToolFunctionDelta},
    tools::parse::parse_tool_calls,
    tools::stream::{IncrementalLineDecoder, sse_data},
    types::{
        ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, InputContentType,
        InputTypeSupport, ProviderCapabilities, ProviderInputCapabilities, ProviderTermination,
        ProviderTimeoutPolicy, ReasoningConfig, Role, StreamChunk, TokenUsage, ToolChoice,
        ToolDefinition,
    },
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use reqwest::Client;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use sha2::Digest;

use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

const BASE_URL: &str = "https://api.openai.com/v1";

#[derive(Clone, Copy)]
struct OpenAiEmbeddingModelDefinition {
    id: &'static str,
    name: &'static str,
    description: &'static str,
}

const OPENAI_EMBEDDING_MODELS: &[OpenAiEmbeddingModelDefinition] = &[
    OpenAiEmbeddingModelDefinition {
        id: "text-embedding-3-small",
        name: "Text Embedding 3 Small",
        description: "1536-dimensional embedding model optimized for cost and latency.",
    },
    OpenAiEmbeddingModelDefinition {
        id: "text-embedding-3-large",
        name: "Text Embedding 3 Large",
        description: "3072-dimensional embedding model optimized for higher retrieval quality.",
    },
    OpenAiEmbeddingModelDefinition {
        id: "text-embedding-ada-002",
        name: "Text Embedding Ada 002",
        description: "Legacy 1536-dimensional embedding model.",
    },
];

pub struct OpenAiProvider {
    api_key: String,
    base_url: String,
    authority_fingerprint: String,
    timeout_policy: ProviderTimeoutPolicy,
    client: Client,
}

// ── OpenAI API request types ────────────────────────────────────────────────

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
    reasoning_effort: Option<String>,
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

// ── OpenAI API response types ───────────────────────────────────────────────

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
    /// Reasoning/thinking models may return output in `reasoning_content`.
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

#[derive(Debug, Serialize)]
struct ApiEmbeddingRequest {
    model: String,
    input: Vec<String>,
    encoding_format: &'static str,
}

#[derive(Debug, Deserialize)]
struct ApiEmbeddingResponse {
    data: Vec<ApiEmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct ApiEmbeddingData {
    embedding: Vec<f32>,
    index: usize,
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

#[derive(Debug, Deserialize)]
struct OpenAiFileUploadResponse {
    id: String,
}

// ── Implementation ──────────────────────────────────────────────────────────

impl OpenAiProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self::with_timeout_policy(api_key, ProviderTimeoutPolicy::default())
    }

    pub fn with_timeout_policy(
        api_key: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self::with_base_url_and_timeout_policy(api_key, BASE_URL, timeout_policy)
    }

    pub(crate) fn with_timeout_policy_and_authority(
        api_key: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
        authority_fingerprint: impl Into<String>,
    ) -> Self {
        Self::with_base_url_timeout_policy_and_authority(
            api_key,
            BASE_URL,
            timeout_policy,
            authority_fingerprint,
        )
    }

    pub fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self::with_base_url_and_timeout_policy(api_key, base_url, ProviderTimeoutPolicy::default())
    }

    pub fn with_base_url_and_timeout_policy(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        let api_key = api_key.into();
        let base_url = base_url.into();
        let mut digest = sha2::Sha256::new();
        digest.update(b"pioneer-provider-authority-v1");
        digest.update([0]);
        digest.update(b"<direct-openai>");
        digest.update([0]);
        digest.update(api_key.as_bytes());
        digest.update([0]);
        digest.update(base_url.as_bytes());
        let authority_fingerprint = hex::encode(digest.finalize());
        Self::with_base_url_timeout_policy_and_authority(
            api_key,
            base_url,
            timeout_policy,
            authority_fingerprint,
        )
    }

    fn with_base_url_timeout_policy_and_authority(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
        authority_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
            authority_fingerprint: authority_fingerprint.into(),
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        }
    }

    async fn upload_file(
        &self,
        attachment: &crate::attachments::PreparedAttachment,
        config: &AttachmentPipelineConfig,
    ) -> Result<String> {
        let bytes = attachment_bytes(attachment)?.to_vec();
        let endpoint = format!("{}/files", self.base_url);
        let operation_authority = runtime::AttachmentOperationAuthority::new(
            self.authority_fingerprint.as_str(),
            "upload_file",
            format!("{}#{}", endpoint, attachment.sha256),
        );
        let idempotency_key = Self::upload_idempotency_key(attachment);
        let file_name = attachment.name.clone();
        let mime_type = attachment.mime_type.clone();
        let client = self.client.clone();
        let auth_header = format!("Bearer {}", self.api_key);
        let timeout_policy = self.timeout_policy;

        runtime::execute_with_retry_async(
            "openai",
            "upload_file",
            &operation_authority,
            &config.runtime,
            move |_| {
                let payload = bytes.clone();
                let endpoint = endpoint.clone();
                let idempotency_key = idempotency_key.clone();
                let file_name = file_name.clone();
                let mime_type = mime_type.clone();
                let client = client.clone();
                let auth_header = auth_header.clone();
                let timeout_policy = timeout_policy;
                async move {
                    let file_part = Part::bytes(payload)
                        .file_name(file_name)
                        .mime_str(mime_type.as_str())
                        .map_err(AttachmentOperationError::non_retryable)?;
                    let form = Form::new()
                        .text("purpose", "user_data")
                        .part("file", file_part);

                    let request_builder = client
                        .post(endpoint)
                        .header("Authorization", auth_header)
                        .header("Idempotency-Key", idempotency_key)
                        .multipart(form);
                    let response = crate::http::non_stream_request(request_builder, timeout_policy)
                        .send()
                        .await
                        .map_err(Self::classify_upload_reqwest_error)?;

                    let status = response.status();
                    if !status.is_success() {
                        let body = match crate::http::read_response_text_bounded(
                            response,
                            16 * 1024,
                            "provider_error_body",
                        )
                        .await
                        {
                            Ok(body) => body,
                            Err(error) => {
                                return Err(AttachmentOperationError::non_retryable(error));
                            }
                        };
                        let error = anyhow!("OpenAI file upload error ({status}): {body}");
                        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                            || status.is_server_error()
                        {
                            return Err(AttachmentOperationError::retryable(error));
                        }
                        return Err(AttachmentOperationError::non_retryable(error));
                    }

                    let uploaded: OpenAiFileUploadResponse =
                        crate::http::read_response_json_bounded(
                            response,
                            Default::default(),
                            "provider_response",
                        )
                        .await
                        .map_err(AttachmentOperationError::non_retryable)?;
                    Ok(uploaded.id)
                }
            },
        )
        .await
    }

    async fn file_id_for_upload(
        &self,
        model: &str,
        attachment: &crate::attachments::PreparedAttachment,
        config: &AttachmentPipelineConfig,
    ) -> Result<String> {
        let model_family = model_family_for_model(model);
        if let Some(file_id) = lookup_uploaded_reference_with_artifact_for_authority(
            config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            attachment.sha256.as_str(),
            self.authority_fingerprint.as_str(),
            attachment.artifact.as_ref(),
        )
        .await?
        {
            return Ok(file_id);
        }

        let file_id = self.upload_file(attachment, config).await?;
        store_uploaded_reference_for_authority(
            config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            attachment,
            file_id.as_str(),
            self.authority_fingerprint.as_str(),
        )
        .await?;
        Ok(file_id)
    }

    async fn materialize_upload_references(
        &self,
        model: &str,
        prepared: PreparedProviderMessages,
    ) -> Result<PreparedProviderMessages> {
        let config = default_attachment_pipeline_config();
        let mut prepared = prepared;
        for attachment in &mut prepared.attachments {
            if attachment.kind != InputContentType::File
                || attachment.transport_plan.kind != AttachmentTransportKind::Upload
            {
                continue;
            }

            if matches!(
                attachment.source,
                PreparedAttachmentSource::Reference { .. }
            ) {
                continue;
            }

            let file_id = self.file_id_for_upload(model, attachment, &config).await?;
            attachment.source = PreparedAttachmentSource::Reference { reference: file_id };
            attachment.bytes = None;
        }
        Ok(prepared)
    }

    fn upload_idempotency_key(attachment: &crate::attachments::PreparedAttachment) -> String {
        format!(
            "openai-upload:{}:{}:{}",
            attachment.sha256, attachment.mime_type, attachment.size_bytes
        )
    }

    fn classify_upload_reqwest_error(error: reqwest::Error) -> AttachmentOperationError {
        if error.is_timeout() {
            return AttachmentOperationError::retryable(error);
        }
        if error.is_connect() || error.is_request() || error.is_body() || error.is_decode() {
            return AttachmentOperationError::retryable(error);
        }
        AttachmentOperationError::non_retryable(error)
    }

    fn audio_format_from_mime(mime: &str) -> Result<&'static str> {
        match mime {
            "audio/wav" | "audio/x-wav" => Ok("wav"),
            "audio/mpeg" | "audio/mp3" => Ok("mp3"),
            _ => Err(anyhow!(
                "provider `openai` only supports `audio/wav` and `audio/mpeg` for input_audio, got `{mime}`"
            )),
        }
    }

    fn build_file_part(
        attachment: &crate::attachments::PreparedAttachment,
    ) -> Result<ApiContentPart> {
        let file = match &attachment.source {
            PreparedAttachmentSource::Reference { reference } => ApiFilePart {
                file_data: None,
                file_id: Some(reference.clone()),
                filename: None,
            },
            _ => ApiFilePart {
                file_data: Some(BASE64.encode(attachment_bytes(attachment)?)),
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
                            url: attachment_data_url(attachment)?,
                        },
                    });
                }
                InputContentType::File => {
                    if attachment.transport_plan.kind == AttachmentTransportKind::Upload
                        && !matches!(
                            attachment.source,
                            PreparedAttachmentSource::Reference { .. }
                        )
                    {
                        return Err(anyhow!(
                            "provider `openai` expected file upload to produce file_id for `{}`",
                            attachment.name
                        ));
                    }
                    parts.push(Self::build_file_part(attachment)?);
                }
                InputContentType::Audio => {
                    let format = Self::audio_format_from_mime(attachment.mime_type.as_str())?;
                    parts.push(ApiContentPart::InputAudio {
                        input_audio: ApiInputAudioPart {
                            data: BASE64.encode(attachment_bytes(attachment)?),
                            format: format.to_owned(),
                        },
                    });
                }
                _ => {
                    return Err(anyhow!(
                        "provider `openai` does not support {:?} attachments in chat/completions yet",
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
                    reasoning_content: Self::replay_reasoning_content(message),
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
                    reasoning_content: Self::replay_reasoning_content(message),
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
                reasoning_content: Self::replay_reasoning_content(message),
                tool_calls,
                tool_call_id: message.tool_call_id.clone(),
                name: message.name.clone(),
            });
        }

        Ok(rendered)
    }

    fn replay_reasoning_content(message: &crate::types::ChatMessage) -> Option<String> {
        (message.role == Role::Assistant)
            .then(|| message.reasoning_content.clone())
            .flatten()
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

    fn embeddings_url(&self) -> String {
        format!("{}/embeddings", self.base_url)
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
        anyhow!("OpenAI API error ({status}): {body}")
    }
}

fn reasoning_effort_for_openai_request(reasoning: Option<ReasoningConfig>) -> Option<String> {
    match reasoning {
        Some(ReasoningConfig::Effort(effort)) => Some(effort.as_str().to_owned()),
        Some(ReasoningConfig::Disabled) | None => None,
    }
}

#[async_trait]
impl crate::traits::Provider for OpenAiProvider {
    fn name(&self) -> &str {
        "openai"
    }

    fn authority_fingerprint(&self) -> Option<&str> {
        Some(self.authority_fingerprint.as_str())
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
                    file_upload: true,
                    data_url_inline: false,
                    text_fallback: false,
                },
                image: InputTypeSupport::data_url_inline_only(),
                audio: InputTypeSupport::native_inline_only(),
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
        let prepared = self
            .materialize_upload_references(request.model.as_str(), prepared)
            .await?;
        let rendered_messages = Self::convert_messages(&prepared)?;
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
            reasoning_effort: reasoning_effort_for_openai_request(request.reasoning),
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

        let api_response: ApiChatResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;
        let usage = api_response.usage.map(|u| TokenUsage {
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
        });

        let choice = api_response
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("no response from OpenAI"))?;
        let termination = choice
            .finish_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| ProviderTermination::Unknown("missing_finish_reason".to_owned()));
        let message = choice.message;

        let text = message.effective_content();
        let tool_calls =
            parse_tool_calls(message.tool_calls.as_ref(), message.function_call.as_ref())?;
        let reasoning_content = message.reasoning_content.or(message.reasoning);

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(anyhow!("no response from OpenAI"));
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
        let prepared = self
            .materialize_upload_references(request.model.as_str(), prepared)
            .await?;
        let rendered_messages = Self::convert_messages(&prepared)?;
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
            reasoning_effort: reasoning_effort_for_openai_request(request.reasoning),
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

        let byte_stream = crate::http::bounded_response_stream(
            response,
            crate::types::ProviderResponseLimits::default().max_transport_bytes,
            "provider_stream",
        );

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            let mut decoder = IncrementalLineDecoder::default();
            let mut tool_call_accumulator = StreamToolCallAccumulator::default();

            tokio::pin!(byte_stream);

            while let Some(result) = byte_stream.next().await {
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

                    if data.trim() == "[DONE]" {
                        if tx
                            .send(Err(anyhow!("OpenAI stream ended without a finish_reason")))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        return;
                    }

                    match serde_json::from_str::<StreamResponse>(data) {
                        Ok(resp) => {
                            if let Some(error) = resp.error {
                                if tx
                                    .send(Err(anyhow!(
                                        "OpenAI stream error: {}",
                                        error.description()
                                    )))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                return;
                            }
                            for choice in resp.choices {
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
                                        ProviderTermination::from_openai_reason(reason.as_str());
                                    let tool_calls = match tool_call_accumulator.take_tool_calls() {
                                        Ok(calls) => calls,
                                        Err(error) => {
                                            if tx.send(Err(error)).await.is_err() {
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
                        }
                        Err(e) => {
                            if tx
                                .send(Err(anyhow!("malformed OpenAI SSE frame: {e}")))
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
                anyhow!("OpenAI stream ended before a provider terminal marker")
            });
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
            .header("Authorization", format!("Bearer {}", self.api_key));
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
            .map(provider_model_from_openai_model_entry)
            .collect())
    }

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.list_models().await?;
        Ok(crate::ProviderWarmupOutcome::Completed)
    }

    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        Ok(OPENAI_EMBEDDING_MODELS
            .iter()
            .map(openai_embedding_model_info)
            .collect())
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        let expected_count = request.input.len();
        let api_request = ApiEmbeddingRequest {
            model: request.model,
            input: request.input,
            encoding_format: "float",
        };

        let request_builder = self
            .client
            .post(self.embeddings_url())
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let mut data = crate::http::read_response_json_bounded::<ApiEmbeddingResponse>(
            response,
            Default::default(),
            "provider_response",
        )
        .await?
        .data;
        data.sort_by_key(|item| item.index);
        if data.len() != expected_count {
            return Err(anyhow!(
                "OpenAI embedding response returned {} embeddings for {} inputs",
                data.len(),
                expected_count
            ));
        }

        Ok(EmbeddingResponse {
            embeddings: data.into_iter().map(|item| item.embedding).collect(),
        })
    }
}

fn provider_model_from_openai_model_entry(m: ApiModelEntry) -> ProviderModelInfo {
    let mut capabilities = ProviderModelCapabilities::default();
    reasoning_registry::apply_reasoning_capabilities("openai", m.id.as_str(), &mut capabilities);

    ProviderModelInfo {
        id: m.id.clone(),
        name: None,
        description: None,
        created: m.created,
        provider: "openai".to_owned(),
        owned_by: m.owned_by,
        limits: ProviderModelLimits::default(),
        capabilities,
        transcription: None,
        pricing: None,
        active: Some(true),
        family: None,
        lifecycle_status: None,
    }
}

fn openai_embedding_model_info(model: &OpenAiEmbeddingModelDefinition) -> ProviderModelInfo {
    ProviderModelInfo {
        id: model.id.to_owned(),
        name: Some(model.name.to_owned()),
        description: Some(model.description.to_owned()),
        created: None,
        provider: "openai".to_owned(),
        owned_by: Some("openai".to_owned()),
        limits: ProviderModelLimits::default(),
        capabilities: ProviderModelCapabilities {
            embeddings: Some(true),
            ..ProviderModelCapabilities::default()
        },
        transcription: None,
        pricing: None,
        active: Some(true),
        family: Some("embedding".to_owned()),
        lifecycle_status: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::prepare_messages_for_provider;
    use crate::traits::Provider;
    use crate::types::{
        AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart, ProviderToolCall,
        ReasoningConfig, ReasoningEffort,
    };

    #[test]
    fn creates_with_api_key() {
        let provider = OpenAiProvider::new("sk-test-key");
        assert_eq!(provider.api_key, "sk-test-key");
        assert_eq!(provider.base_url, BASE_URL);
    }

    #[test]
    fn creates_with_custom_base_url() {
        let provider = OpenAiProvider::with_base_url("key", "http://localhost:8080");
        assert_eq!(provider.base_url, "http://localhost:8080");
    }

    #[test]
    fn default_base_url() {
        let provider = OpenAiProvider::new("key");
        assert_eq!(provider.base_url, "https://api.openai.com/v1");
    }

    #[test]
    fn chat_completions_url_built_correctly() {
        let provider = OpenAiProvider::new("key");
        assert_eq!(
            provider.chat_completions_url(),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[test]
    fn chat_completions_url_with_custom_base() {
        let provider = OpenAiProvider::with_base_url("key", "http://localhost:3000");
        assert_eq!(
            provider.chat_completions_url(),
            "http://localhost:3000/chat/completions"
        );
    }

    #[test]
    fn openai_reasoning_registry_exposes_documented_gpt_5_5_efforts() {
        let reasoning = reasoning_registry::reasoning_capabilities_for_model("openai", "gpt-5.5")
            .expect("gpt-5.5 reasoning metadata");

        assert_eq!(reasoning.supported, Some(true));
        assert_eq!(
            reasoning.effort_options,
            vec!["none", "low", "medium", "high", "xhigh"]
        );
        assert_eq!(reasoning.default_effort.as_deref(), Some("medium"));
    }

    #[test]
    fn openai_reasoning_registry_matches_documented_gpt_5_4_family() {
        let reasoning =
            reasoning_registry::reasoning_capabilities_for_model("openai", "gpt-5.4-mini")
                .expect("gpt-5.4 mini reasoning metadata");

        assert_eq!(reasoning.supported, Some(true));
        assert_eq!(
            reasoning.effort_options,
            vec!["none", "low", "medium", "high", "xhigh"]
        );
    }

    #[test]
    fn openai_reasoning_registry_leaves_unknown_models_unset() {
        assert!(
            reasoning_registry::reasoning_capabilities_for_model("openai", "custom-model")
                .is_none()
        );
    }

    #[test]
    fn openai_model_list_fixture_normalizes_reasoning_capabilities() {
        let response: ModelsListResponse = serde_json::from_str(
            r#"{
                "data": [
                    { "id": "gpt-5.5", "created": 1, "owned_by": "openai" },
                    { "id": "custom-model", "created": 2, "owned_by": "user" }
                ]
            }"#,
        )
        .expect("fixture response");
        let models = response
            .data
            .into_iter()
            .map(provider_model_from_openai_model_entry)
            .collect::<Vec<_>>();

        let known = &models[0];
        let reasoning = known
            .capabilities
            .reasoning
            .as_ref()
            .expect("known reasoning model");
        assert_eq!(reasoning.supported, Some(true));
        assert_eq!(
            reasoning.effort_options,
            vec!["none", "low", "medium", "high", "xhigh"]
        );
        assert_eq!(known.capabilities.thinking, Some(true));

        assert!(models[1].capabilities.reasoning.is_none());
        assert!(models[1].capabilities.thinking.is_none());
        assert_eq!(models[1].capabilities.embeddings, None);
    }

    #[test]
    fn openai_embedding_model_list_exposes_only_embedding_models() {
        let models = OPENAI_EMBEDDING_MODELS
            .iter()
            .map(openai_embedding_model_info)
            .collect::<Vec<_>>();

        assert_eq!(
            models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "text-embedding-3-small",
                "text-embedding-3-large",
                "text-embedding-ada-002"
            ]
        );
        assert_eq!(models[0].name.as_deref(), Some("Text Embedding 3 Small"));
        assert_eq!(
            models[1].description.as_deref(),
            Some("3072-dimensional embedding model optimized for higher retrieval quality.")
        );
        assert!(
            models
                .iter()
                .all(|model| model.capabilities.embeddings == Some(true))
        );
    }

    #[test]
    fn convert_messages_maps_roles() {
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let provider = OpenAiProvider::new("test-key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = OpenAiProvider::convert_messages(&prepared).unwrap();

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
    fn convert_messages_replays_assistant_reasoning_content() {
        let messages = vec![ChatMessage::assistant_tool_calls_with_reasoning(
            None::<String>,
            Some("provider reasoning token"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_skill".to_owned(),
                arguments: "{\"slug\":\"weather\"}".to_owned(),
            }],
        )];

        let provider = OpenAiProvider::new("test-key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = OpenAiProvider::convert_messages(&prepared).unwrap();

        assert_eq!(
            api_messages[0].reasoning_content.as_deref(),
            Some("provider reasoning token")
        );
        assert!(api_messages[0].content.is_none());
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
        let provider = OpenAiProvider::new("test-key");
        let prepared = prepare_messages_for_provider(
            provider.name(),
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let api_messages = OpenAiProvider::convert_messages(&prepared).unwrap();

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
            model: "gpt-4o".into(),
            messages: vec![
                ApiMessage {
                    role: "system".into(),
                    content: Some(ApiMessageContent::Text("You are helpful".into())),
                    reasoning_content: None,
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
                ApiMessage {
                    role: "user".into(),
                    content: Some(ApiMessageContent::Text("Hello".into())),
                    reasoning_content: None,
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
            reasoning_effort: None,
            stream: false,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("gpt-4o"));
        assert!(json.contains("\"role\":\"system\""));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"stream\":false"));
        assert!(!json.contains("max_tokens"));
    }

    #[test]
    fn api_request_serializes_with_max_tokens() {
        let request = ApiChatRequest {
            model: "gpt-4o".into(),
            messages: vec![],
            temperature: None,
            max_tokens: Some(1024),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning_effort: None,
            stream: true,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"max_tokens\":1024"));
        assert!(json.contains("\"stream\":true"));
        assert!(!json.contains("temperature"));
    }

    #[test]
    fn api_request_serializes_reasoning_effort_only_when_selected() {
        let request = ApiChatRequest {
            model: "gpt-5.4".into(),
            messages: vec![],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning_effort: Some("high".to_owned()),
            stream: false,
        };

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["reasoning_effort"], "high");

        let request_without_reasoning = ApiChatRequest {
            reasoning_effort: None,
            ..request
        };
        let json = serde_json::to_value(&request_without_reasoning).unwrap();
        assert!(json.get("reasoning_effort").is_none());
    }

    #[test]
    fn reasoning_effort_mapping_omits_disabled_and_serializes_explicit_none() {
        assert_eq!(
            reasoning_effort_for_openai_request(Some(ReasoningConfig::disabled())),
            None
        );
        assert_eq!(
            reasoning_effort_for_openai_request(Some(ReasoningConfig::effort(
                ReasoningEffort::None
            ))),
            Some("none".to_owned())
        );
    }

    #[test]
    fn api_response_deserializes() {
        let json = r#"{"choices":[{"message":{"content":"Hi from OpenAI"}}]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.choices.len(), 1);
        assert_eq!(
            response.choices[0].message.content.as_deref(),
            Some("Hi from OpenAI")
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
    fn api_response_without_usage() {
        let json = r#"{"choices":[{"message":{"content":"Hello"}}]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert!(response.usage.is_none());
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
    fn stream_response_empty_delta() {
        let json = r#"{"choices":[{"delta":{},"finish_reason":null}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();
        assert!(response.choices[0].delta.content.is_none());
    }

    #[test]
    fn provider_name() {
        use crate::traits::Provider;
        let provider = OpenAiProvider::new("key");
        assert_eq!(provider.name(), "openai");
    }

    #[test]
    fn provider_capabilities() {
        use crate::traits::Provider;
        let provider = OpenAiProvider::new("key");
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }
}
