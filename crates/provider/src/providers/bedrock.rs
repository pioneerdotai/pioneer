use crate::attachments::{
    PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes,
    ensure_no_unrendered_attachments, prepare_messages_for_provider_async,
};
use crate::reasoning_registry;
use crate::types::{
    ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
    ProviderInputCapabilities, ProviderReplayState, ProviderTermination, ProviderTimeoutPolicy,
    ProviderToolCall, ReasoningConfig, Role, StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::stream::BoxStream;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

const SERVICE: &str = "bedrock";

type HmacSha256 = Hmac<Sha256>;

// ── Bedrock Converse API request types ─────────────────────────────────────

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockRequest {
    messages: Vec<BedrockMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    system: Vec<BedrockSystemBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inference_config: Option<BedrockInferenceConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_config: Option<BedrockToolConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_model_request_fields: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct BedrockMessage {
    role: String,
    content: Vec<BedrockContentBlock>,
}

#[derive(Debug, Serialize)]
struct BedrockContentBlock {
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<BedrockImageBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    document: Option<BedrockDocumentBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audio: Option<BedrockAudioBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    video: Option<BedrockVideoBlock>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_use: Option<BedrockToolUse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_result: Option<BedrockToolResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_content: Option<BedrockReasoningContent>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockImageBlock {
    format: String,
    source: BedrockBinarySource,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockDocumentBlock {
    format: String,
    name: String,
    source: BedrockBinarySource,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockAudioBlock {
    format: String,
    source: BedrockBinarySource,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockVideoBlock {
    format: String,
    source: BedrockBinarySource,
}

#[derive(Debug, Serialize)]
struct BedrockBinarySource {
    bytes: String,
}

#[derive(Debug, Serialize)]
struct BedrockSystemBlock {
    text: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockInferenceConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolConfig {
    tools: Vec<BedrockToolEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolEntry {
    tool_spec: BedrockToolSpec,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolSpec {
    name: String,
    description: String,
    input_schema: BedrockInputSchema,
}

#[derive(Debug, Serialize)]
struct BedrockInputSchema {
    json: serde_json::Value,
}

// ── Bedrock Converse API response types ────────────────────────────────────

#[derive(Debug, Deserialize)]
struct BedrockResponse {
    output: BedrockOutput,
    #[serde(default, rename = "stopReason")]
    stop_reason: Option<String>,
    #[serde(default)]
    usage: Option<BedrockUsage>,
}

#[derive(Debug, Deserialize)]
struct BedrockOutput {
    message: BedrockResponseMessage,
}

#[derive(Debug, Deserialize)]
struct BedrockResponseMessage {
    content: Vec<BedrockResponseContent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockResponseContent {
    #[serde(default)]
    text: Option<String>,
    /// Reasoning/thinking content from models that support extended thinking.
    #[serde(default)]
    reasoning_content: Option<BedrockReasoningContent>,
    #[serde(default)]
    tool_use: Option<BedrockToolUse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockReasoningContent {
    #[serde(default)]
    reasoning_text: Option<BedrockReasoningText>,
    #[serde(default)]
    redacted_content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BedrockReasoningText {
    text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolUse {
    tool_use_id: String,
    name: String,
    input: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockToolResult {
    tool_use_id: String,
    content: Vec<BedrockToolResultContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BedrockToolResultContent {
    text: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

// ── List models response types ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockModelsResponse {
    #[serde(default)]
    model_summaries: Vec<BedrockModelSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockModelSummary {
    #[serde(default)]
    model_id: Option<String>,
    #[serde(default)]
    model_name: Option<String>,
    #[serde(default)]
    provider_name: Option<String>,
    #[serde(default)]
    input_modalities: Option<Vec<String>>,
    #[serde(default)]
    output_modalities: Option<Vec<String>>,
    #[serde(default)]
    model_lifecycle: Option<BedrockModelLifecycle>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BedrockModelLifecycle {
    #[serde(default)]
    status: Option<String>,
}

// ── Provider struct ────────────────────────────────────────────────────────

pub struct BedrockProvider {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    region: String,
    timeout_policy: ProviderTimeoutPolicy,
    client: Client,
}

// ── SigV4 signing utilities ────────────────────────────────────────────────

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Derive the SigV4 signing key from the secret access key.
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Build an AWS SigV4 `Authorization` header value.
///
/// Returns `(authorization_header_value, amz_date)`.
fn sign_request(
    method: &str,
    url: &Url,
    body: &[u8],
    access_key_id: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
    region: &str,
    service: &str,
    datetime: &str, // e.g. "20260319T120000Z"
) -> String {
    let date = &datetime[..8]; // "20260319"
    let host = url.host_str().unwrap_or_default();
    let path = url.path();

    // Canonical query string (empty for POST)
    let canonical_query = url.query().unwrap_or("");

    let payload_hash = sha256_hex(body);

    // Build signed headers and canonical headers.
    // Headers must be sorted by lowercase name.
    let mut headers: Vec<(&str, String)> = vec![
        ("content-type", "application/json".to_string()),
        ("host", host.to_string()),
        ("x-amz-date", datetime.to_string()),
    ];
    if let Some(token) = session_token {
        headers.push(("x-amz-security-token", token.to_string()));
    }
    headers.sort_by_key(|(k, _)| *k);

    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();

    let signed_headers: String = headers
        .iter()
        .map(|(k, _)| *k)
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    let scope = format!("{date}/{region}/{service}/aws4_request");
    let canonical_request_hash = sha256_hex(canonical_request.as_bytes());

    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_request_hash}");

    let key = signing_key(secret_access_key, date, region, service);
    let signature = hex::encode(hmac_sha256(&key, string_to_sign.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    )
}

// ── Implementation ─────────────────────────────────────────────────────────

impl BedrockProvider {
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self::with_timeout_policy(
            access_key_id,
            secret_access_key,
            region,
            ProviderTimeoutPolicy::default(),
        )
    }

    pub fn with_timeout_policy(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
            region: region.into(),
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        }
    }

    pub fn with_session_token(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
        session_token: impl Into<String>,
    ) -> Self {
        Self::with_session_token_and_timeout_policy(
            access_key_id,
            secret_access_key,
            region,
            session_token,
            ProviderTimeoutPolicy::default(),
        )
    }

    pub fn with_session_token_and_timeout_policy(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
        session_token: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: Some(session_token.into()),
            region: region.into(),
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        }
    }

    /// Create a `BedrockProvider` from standard AWS environment variables.
    ///
    /// Reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
    /// (optional), and `AWS_REGION` (defaults to `us-east-1`).
    pub fn from_env() -> Result<Self> {
        Self::from_env_with_timeout_policy(ProviderTimeoutPolicy::default())
    }

    pub fn from_env_with_timeout_policy(timeout_policy: ProviderTimeoutPolicy) -> Result<Self> {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| anyhow!("AWS_ACCESS_KEY_ID environment variable not set"))?;
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| anyhow!("AWS_SECRET_ACCESS_KEY environment variable not set"))?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        Ok(Self {
            access_key_id,
            secret_access_key,
            session_token,
            region,
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        })
    }

    fn list_foundation_models_url(&self) -> String {
        format!(
            "https://bedrock.{}.amazonaws.com/foundation-models",
            self.region
        )
    }

    /// Build the Converse API endpoint URL for the given model ID.
    fn converse_url(&self, model_id: &str) -> String {
        let encoded_model = model_id.replace('/', "%2F");
        format!(
            "https://bedrock-runtime.{}.amazonaws.com/model/{}/converse",
            self.region, encoded_model
        )
    }

    fn mime_subtype(mime: &str) -> Option<&str> {
        mime.split_once('/').map(|(_, subtype)| subtype)
    }

    fn binary_source(
        attachment: &crate::attachments::PreparedAttachment,
    ) -> Result<BedrockBinarySource> {
        if matches!(
            &attachment.source,
            PreparedAttachmentSource::Reference { .. }
        ) {
            return Err(anyhow!(
                "provider `bedrock` requires materialized bytes for {:?} attachments",
                attachment.kind
            ));
        }
        Ok(BedrockBinarySource {
            bytes: BASE64.encode(attachment_bytes(attachment)?),
        })
    }

    fn attachment_block(
        attachment: &crate::attachments::PreparedAttachment,
    ) -> Result<BedrockContentBlock> {
        let subtype = Self::mime_subtype(attachment.mime_type.as_str()).ok_or_else(|| {
            anyhow!(
                "provider `bedrock` could not derive format from mime `{}`",
                attachment.mime_type
            )
        })?;
        let normalize_format = |value: &str| value.split('+').next().unwrap_or(value).to_owned();
        let source = Self::binary_source(attachment)?;

        match attachment.kind {
            InputContentType::Image => Ok(BedrockContentBlock {
                text: None,
                image: Some(BedrockImageBlock {
                    format: normalize_format(subtype),
                    source,
                }),
                document: None,
                audio: None,
                video: None,
                tool_use: None,
                tool_result: None,
                reasoning_content: None,
            }),
            InputContentType::File => Ok(BedrockContentBlock {
                text: None,
                image: None,
                document: Some(BedrockDocumentBlock {
                    format: normalize_format(subtype),
                    name: attachment.name.clone(),
                    source,
                }),
                audio: None,
                video: None,
                tool_use: None,
                tool_result: None,
                reasoning_content: None,
            }),
            InputContentType::Audio => Ok(BedrockContentBlock {
                text: None,
                image: None,
                document: None,
                audio: Some(BedrockAudioBlock {
                    format: normalize_format(subtype),
                    source,
                }),
                video: None,
                tool_use: None,
                tool_result: None,
                reasoning_content: None,
            }),
            InputContentType::Video => Ok(BedrockContentBlock {
                text: None,
                image: None,
                document: None,
                audio: None,
                video: Some(BedrockVideoBlock {
                    format: normalize_format(subtype),
                    source,
                }),
                tool_use: None,
                tool_result: None,
                reasoning_content: None,
            }),
            _ => Err(anyhow!(
                "provider `bedrock` does not support {:?} attachments",
                attachment.kind
            )),
        }
    }

    /// Convert messages into Bedrock format, extracting system messages.
    fn convert_messages(
        prepared: &PreparedProviderMessages,
    ) -> Result<(Vec<BedrockMessage>, Vec<BedrockSystemBlock>)> {
        let mut bedrock_messages = Vec::new();
        let mut system_blocks = Vec::new();

        for (message_index, msg) in prepared.messages.iter().enumerate() {
            match msg.role {
                Role::System => {
                    system_blocks.push(BedrockSystemBlock {
                        text: msg.content.clone(),
                    });
                }
                _ => {
                    let role = match msg.role {
                        Role::User => "user",
                        Role::Assistant => "assistant",
                        Role::Tool => "user",
                        Role::System => unreachable!(),
                    };
                    let mut content = Vec::new();

                    match msg.role {
                        Role::Tool => {
                            let tool_use_id = msg
                                .tool_call_id
                                .clone()
                                .or_else(|| msg.name.clone())
                                .unwrap_or_else(|| "tool".to_owned());
                            content.push(BedrockContentBlock {
                                text: None,
                                image: None,
                                document: None,
                                audio: None,
                                video: None,
                                tool_use: None,
                                tool_result: Some(BedrockToolResult {
                                    tool_use_id,
                                    content: vec![BedrockToolResultContent {
                                        text: msg.content.clone(),
                                    }],
                                    status: None,
                                }),
                                reasoning_content: None,
                            });
                        }
                        _ => {
                            if msg.role == Role::Assistant
                                && let Some(state) = msg.provider_replay_state.as_ref()
                            {
                                let payload = state.payload_for("bedrock").ok_or_else(|| {
                                    anyhow!(
                                        "provider replay state `{}` cannot be rendered by `bedrock`",
                                        state.provider
                                    )
                                })?;
                                let blocks = payload.get("blocks").cloned().ok_or_else(|| {
                                    anyhow!("bedrock replay state is missing `blocks`")
                                })?;
                                for reasoning_content in serde_json::from_value::<
                                    Vec<BedrockReasoningContent>,
                                >(blocks)
                                .map_err(|error| anyhow!("invalid bedrock replay state: {error}"))?
                                {
                                    content.push(BedrockContentBlock {
                                        text: None,
                                        image: None,
                                        document: None,
                                        audio: None,
                                        video: None,
                                        tool_use: None,
                                        tool_result: None,
                                        reasoning_content: Some(reasoning_content),
                                    });
                                }
                            }

                            if !msg.content.is_empty() {
                                content.push(BedrockContentBlock {
                                    text: Some(msg.content.clone()),
                                    image: None,
                                    document: None,
                                    audio: None,
                                    video: None,
                                    tool_use: None,
                                    tool_result: None,
                                    reasoning_content: None,
                                });
                            }

                            if let Some(tool_calls) = msg.tool_calls.as_ref() {
                                for call in tool_calls {
                                    content.push(BedrockContentBlock {
                                        text: None,
                                        image: None,
                                        document: None,
                                        audio: None,
                                        video: None,
                                        tool_use: Some(BedrockToolUse {
                                            tool_use_id: call.id.clone(),
                                            name: call.name.clone(),
                                            input: parse_json_or_string(call.arguments.as_str()),
                                        }),
                                        tool_result: None,
                                        reasoning_content: None,
                                    });
                                }
                            }
                        }
                    }

                    for attachment in prepared.attachments_for_message(message_index) {
                        content.push(Self::attachment_block(attachment)?);
                    }

                    bedrock_messages.push(BedrockMessage {
                        role: role.to_string(),
                        content,
                    });
                }
            }
        }

        Ok((bedrock_messages, system_blocks))
    }

    fn convert_tool_config(
        tools: &[ToolDefinition],
        choice: Option<ToolChoice>,
    ) -> BedrockToolConfig {
        let tools = tools
            .iter()
            .map(|tool| BedrockToolEntry {
                tool_spec: BedrockToolSpec {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    input_schema: BedrockInputSchema {
                        json: tool.parameters.clone(),
                    },
                },
            })
            .collect::<Vec<_>>();

        let tool_choice = choice.map(|choice| match choice {
            ToolChoice::Auto => serde_json::json!({ "auto": {} }),
            ToolChoice::None => serde_json::json!({ "auto": {} }),
            ToolChoice::Required => serde_json::json!({ "any": {} }),
            ToolChoice::Tool { name } => serde_json::json!({ "tool": { "name": name } }),
        });

        BedrockToolConfig { tools, tool_choice }
    }

    fn build_request(
        request: &ChatRequest,
        prepared: &PreparedProviderMessages,
    ) -> Result<BedrockRequest> {
        let (messages, system) = Self::convert_messages(prepared)?;

        let inference_config = if request.temperature.is_some() || request.max_tokens.is_some() {
            Some(BedrockInferenceConfig {
                temperature: request.temperature,
                max_tokens: request.max_tokens,
            })
        } else {
            None
        };

        Ok(BedrockRequest {
            messages,
            system,
            inference_config,
            tool_config: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tool_config(tools, request.tool_choice.clone())),
            additional_model_request_fields: Self::additional_model_request_fields(
                request.model.as_str(),
                request.reasoning,
            ),
        })
    }

    fn additional_model_request_fields(
        model_id: &str,
        reasoning: Option<ReasoningConfig>,
    ) -> Option<serde_json::Value> {
        if !Self::is_anthropic_claude_model(model_id) {
            return None;
        }

        match reasoning {
            Some(ReasoningConfig::Effort(effort)) => Some(serde_json::json!({
                "output_config": {
                    "effort": effort.as_str(),
                },
            })),
            Some(ReasoningConfig::Disabled) | None => None,
        }
    }

    fn is_anthropic_claude_model(model_id: &str) -> bool {
        model_id.contains("anthropic.claude")
    }

    /// Get the current UTC datetime in the format required by SigV4.
    fn amz_datetime() -> String {
        // Use a simple approach: read system time and format manually.
        use std::time::SystemTime;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time before epoch");

        let secs = now.as_secs();

        // Convert unix timestamp to date components using a simple algorithm.
        let (year, month, day, hour, minute, second) = unix_to_datetime(secs);

        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
    }

    async fn api_error(response: reqwest::Response) -> anyhow::Error {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read error body>".into());
        anyhow!("Bedrock API error ({status}): {body}")
    }
}

fn parse_json_or_string(raw: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(raw)
        .unwrap_or_else(|_| serde_json::Value::String(raw.to_owned()))
}

/// Convert a Unix timestamp (seconds since epoch) to (year, month, day, hour, minute, second).
fn unix_to_datetime(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let second = secs % 60;
    let minute = (secs / 60) % 60;
    let hour = (secs / 3600) % 24;

    // Days since epoch
    let mut days = secs / 86400;

    // Calculate year
    let mut year = 1970u64;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }

    // Calculate month and day
    let leap = is_leap_year(year);
    let month_days: [u64; 12] = if leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1u64;
    for &md in &month_days {
        if days < md {
            break;
        }
        days -= md;
        month += 1;
    }
    let day = days + 1;

    (year, month, day, hour, minute, second)
}

fn is_leap_year(y: u64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

#[async_trait]
impl crate::traits::Provider for BedrockProvider {
    fn name(&self) -> &str {
        "bedrock"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
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
        let prepared = prepare_messages_for_provider_async(
            self.name(),
            &self.capabilities(),
            request
                .rendered_messages_with_compiled_sections()
                .as_slice(),
        )
        .await?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let bedrock_request = Self::build_request(&request, &prepared)?;

        let body = serde_json::to_vec(&bedrock_request)?;
        let url_str = self.converse_url(&request.model);
        let url: Url = url_str.parse()?;

        let datetime = Self::amz_datetime();
        let authorization = sign_request(
            "POST",
            &url,
            &body,
            &self.access_key_id,
            &self.secret_access_key,
            self.session_token.as_deref(),
            &self.region,
            SERVICE,
            &datetime,
        );

        let mut req = self
            .client
            .post(url_str)
            .header("Content-Type", "application/json")
            .header("X-Amz-Date", &datetime)
            .header("Authorization", &authorization);

        if let Some(ref token) = self.session_token {
            req = req.header("X-Amz-Security-Token", token);
        }

        let response = crate::http::non_stream_request(req.body(body), self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: BedrockResponse = response.json().await?;
        let termination = api_response
            .stop_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| ProviderTermination::Unknown("missing_stop_reason".to_owned()));

        let usage = api_response.usage.map(|u| TokenUsage {
            input_tokens: u.input_tokens,
            output_tokens: u.output_tokens,
        });

        let mut text_parts = Vec::new();
        let mut reasoning_parts = Vec::new();
        let mut tool_calls = Vec::new();
        let mut replay_blocks = Vec::new();

        for block in api_response.output.message.content {
            if let Some(t) = block.text {
                text_parts.push(t);
            }
            if let Some(rc) = block.reasoning_content {
                if let Some(rt) = rc.reasoning_text.as_ref() {
                    if !rt.text.is_empty() {
                        reasoning_parts.push(rt.text.clone());
                    }
                }
                replay_blocks.push(rc);
            }
            if let Some(tool_use) = block.tool_use {
                tool_calls.push(ProviderToolCall {
                    id: tool_use.tool_use_id,
                    name: tool_use.name,
                    arguments: serde_json::to_string(&tool_use.input)
                        .unwrap_or_else(|_| "{}".to_owned()),
                });
            }
        }

        let text = text_parts.join("");
        let reasoning_content = if reasoning_parts.is_empty() {
            None
        } else {
            Some(reasoning_parts.join(""))
        };
        let provider_replay_state = if replay_blocks.is_empty() {
            None
        } else {
            Some(ProviderReplayState::new(
                "bedrock",
                serde_json::json!({ "blocks": replay_blocks }),
            ))
        };

        if text.is_empty()
            && tool_calls.is_empty()
            && reasoning_content.as_deref().unwrap_or_default().is_empty()
        {
            return Err(anyhow!("no response from Bedrock"));
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
        // Bedrock Converse streaming uses a different binary event-stream protocol.
        // Fall back to a single non-streaming call returned as one chunk.
        let response = self.chat(request).await?;
        let termination = response.termination.clone();
        let mut chunks = Vec::new();
        if let Some(reasoning) = response.reasoning_content {
            if !reasoning.is_empty() {
                chunks.push(Ok(StreamChunk::reasoning(reasoning)));
            }
        }
        if !response.tool_calls.is_empty() {
            chunks.push(Ok(StreamChunk::tool_calls(response.tool_calls)));
        }
        if !response.text.is_empty() {
            chunks.push(Ok(StreamChunk::delta(response.text)));
        }
        if let Some(state) = response.provider_replay_state {
            chunks.push(Ok(StreamChunk::provider_replay_state(state)));
        }
        chunks.push(Ok(StreamChunk::final_chunk_with(termination)));
        Ok(Box::pin(futures_util::stream::iter(chunks)))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let url_str = self.list_foundation_models_url();
        let url: Url = url_str.parse()?;

        let datetime = Self::amz_datetime();
        let authorization = sign_request(
            "GET",
            &url,
            b"",
            &self.access_key_id,
            &self.secret_access_key,
            self.session_token.as_deref(),
            &self.region,
            SERVICE,
            &datetime,
        );

        let mut req = self
            .client
            .get(&url_str)
            .header("Content-Type", "application/json")
            .header("X-Amz-Date", &datetime)
            .header("Authorization", &authorization);

        if let Some(ref token) = self.session_token {
            req = req.header("X-Amz-Security-Token", token);
        }

        let response = crate::http::non_stream_request(req, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::api_error(response).await);
        }

        let api_response: BedrockModelsResponse = response.json().await?;

        Ok(api_response
            .model_summaries
            .into_iter()
            .map(provider_model_from_bedrock_model_summary)
            .collect())
    }

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.list_models().await?;
        Ok(crate::ProviderWarmupOutcome::Completed)
    }
}

fn provider_model_from_bedrock_model_summary(m: BedrockModelSummary) -> ProviderModelInfo {
    let lifecycle_status = m.model_lifecycle.and_then(|lc| lc.status);

    let has_vision = m
        .input_modalities
        .as_ref()
        .is_some_and(|mods| mods.iter().any(|m| m == "IMAGE"));
    let model_id = m.model_id.clone().unwrap_or_default();
    let mut capabilities = ProviderModelCapabilities {
        vision: Some(has_vision),
        input_modalities: m.input_modalities,
        output_modalities: m.output_modalities,
        ..ProviderModelCapabilities::default()
    };
    reasoning_registry::apply_reasoning_capabilities(
        "bedrock",
        model_id.as_str(),
        &mut capabilities,
    );

    ProviderModelInfo {
        id: model_id,
        name: m.model_name,
        description: None,
        created: None,
        provider: "bedrock".to_owned(),
        owned_by: m.provider_name,
        limits: ProviderModelLimits::default(),
        capabilities,
        transcription: None,
        pricing: None,
        active: lifecycle_status.as_deref().map(|s| s == "ACTIVE"),
        family: None,
        lifecycle_status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::prepare_messages_for_provider;
    use crate::traits::Provider;
    use crate::types::{
        ChatMessage, ChatRequest, CompiledPromptPayload, ReasoningConfig, ReasoningEffort,
    };
    use std::sync::{Mutex, OnceLock};

    fn bedrock_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn prepared_for(messages: &[ChatMessage]) -> crate::attachments::PreparedProviderMessages {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        prepare_messages_for_provider(provider.name(), &provider.capabilities(), messages).unwrap()
    }

    #[test]
    fn creates_with_credentials() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-west-2");
        assert_eq!(provider.access_key_id, "AKID");
        assert_eq!(provider.secret_access_key, "SECRET");
        assert_eq!(provider.region, "us-west-2");
        assert!(provider.session_token.is_none());
    }

    #[test]
    fn creates_with_session_token() {
        let provider = BedrockProvider::with_session_token("AKID", "SECRET", "eu-west-1", "TOKEN");
        assert_eq!(provider.access_key_id, "AKID");
        assert_eq!(provider.secret_access_key, "SECRET");
        assert_eq!(provider.region, "eu-west-1");
        assert_eq!(provider.session_token.as_deref(), Some("TOKEN"));
    }

    #[test]
    fn bedrock_reasoning_registry_exposes_aws_documented_claude_opus_4_5() {
        let reasoning = reasoning_registry::reasoning_capabilities_for_model(
            "bedrock",
            "anthropic.claude-opus-4-5",
        )
        .expect("bedrock opus 4.5 effort metadata");

        assert_eq!(reasoning.supported, Some(true));
        assert_eq!(reasoning.effort_options, vec!["low", "medium", "high"]);
        assert_eq!(reasoning.default_effort.as_deref(), Some("high"));
    }

    #[test]
    fn bedrock_reasoning_registry_leaves_older_claude_unset() {
        assert!(
            reasoning_registry::reasoning_capabilities_for_model(
                "bedrock",
                "anthropic.claude-3-7-sonnet"
            )
            .is_none()
        );
    }

    #[test]
    fn bedrock_reasoning_registry_leaves_non_anthropic_models_unset() {
        assert!(
            reasoning_registry::reasoning_capabilities_for_model("bedrock", "amazon.nova-pro-v1:0")
                .is_none()
        );
    }

    #[test]
    fn bedrock_model_list_fixture_normalizes_reasoning_capabilities() {
        let response: BedrockModelsResponse = serde_json::from_str(
            r#"{
                "modelSummaries": [
                    {
                        "modelId": "anthropic.claude-opus-4-5",
                        "modelName": "Claude Opus 4.5",
                        "providerName": "Anthropic",
                        "inputModalities": ["TEXT", "IMAGE"],
                        "outputModalities": ["TEXT"],
                        "modelLifecycle": { "status": "ACTIVE" }
                    },
                    {
                        "modelId": "amazon.nova-pro-v1:0",
                        "modelName": "Nova Pro",
                        "providerName": "Amazon"
                    }
                ]
            }"#,
        )
        .expect("fixture response");
        let models = response
            .model_summaries
            .into_iter()
            .map(provider_model_from_bedrock_model_summary)
            .collect::<Vec<_>>();

        let reasoning = models[0]
            .capabilities
            .reasoning
            .as_ref()
            .expect("bedrock claude reasoning model");
        assert_eq!(reasoning.effort_options, vec!["low", "medium", "high"]);
        assert_eq!(models[0].capabilities.vision, Some(true));
        assert_eq!(models[0].active, Some(true));

        assert!(models[1].capabilities.reasoning.is_none());
    }

    #[test]
    fn from_env_reads_variables() {
        let _env_guard = bedrock_env_lock()
            .lock()
            .expect("bedrock env lock poisoned");
        // Temporarily set env vars for test.
        // SAFETY: test-only; these env vars are not used by other threads in tests.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "env-akid");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "env-secret");
            std::env::set_var("AWS_SESSION_TOKEN", "env-token");
            std::env::set_var("AWS_REGION", "ap-southeast-1");
        }

        let provider = BedrockProvider::from_env().unwrap();
        assert_eq!(provider.access_key_id, "env-akid");
        assert_eq!(provider.secret_access_key, "env-secret");
        assert_eq!(provider.session_token.as_deref(), Some("env-token"));
        assert_eq!(provider.region, "ap-southeast-1");

        // Clean up
        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
        }
    }

    #[test]
    fn from_env_defaults_region() {
        let _env_guard = bedrock_env_lock()
            .lock()
            .expect("bedrock env lock poisoned");
        // SAFETY: test-only; these env vars are not used by other threads in tests.
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "akid");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "secret");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_REGION");
        }

        let provider = BedrockProvider::from_env().unwrap();
        assert_eq!(provider.region, "us-east-1");
        assert!(provider.session_token.is_none());

        unsafe {
            std::env::remove_var("AWS_ACCESS_KEY_ID");
            std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        }
    }

    #[test]
    fn provider_name() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        assert_eq!(provider.name(), "bedrock");
    }

    #[test]
    fn provider_capabilities() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        let caps = provider.capabilities();
        assert!(!caps.streaming);
        assert!(caps.vision);
    }

    #[test]
    fn converse_url_simple_model() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        let url = provider.converse_url("anthropic.claude-3-sonnet-20240229-v1:0");
        assert_eq!(
            url,
            "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-3-sonnet-20240229-v1:0/converse"
        );
    }

    #[test]
    fn converse_url_encodes_slashes() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-west-2");
        let url = provider
            .converse_url("arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-v2");
        assert!(url.contains("%2F"));
        assert!(!url.contains("foundation-model/anthropic"));
    }

    #[test]
    fn convert_messages_extracts_system() {
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let prepared = prepared_for(messages.as_slice());
        let (bedrock_msgs, system_blocks) = BedrockProvider::convert_messages(&prepared).unwrap();

        assert_eq!(system_blocks.len(), 1);
        assert_eq!(system_blocks[0].text, "Be helpful");

        assert_eq!(bedrock_msgs.len(), 2);
        assert_eq!(bedrock_msgs[0].role, "user");
        assert_eq!(bedrock_msgs[0].content[0].text.as_deref(), Some("Hello"));
        assert_eq!(bedrock_msgs[1].role, "assistant");
        assert_eq!(bedrock_msgs[1].content[0].text.as_deref(), Some("Hi!"));
    }

    #[test]
    fn convert_messages_replays_signed_reasoning_blocks_before_tool_calls() {
        let message = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            Some("summary"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_file".to_owned(),
                arguments: "{}".to_owned(),
            }],
            Some(ProviderReplayState::new(
                "bedrock",
                serde_json::json!({
                    "blocks": [{
                        "reasoningText": {
                            "text": "summary",
                            "signature": "opaque-signature"
                        }
                    }]
                }),
            )),
        );

        let prepared = prepared_for(&[message]);
        let (messages, _) = BedrockProvider::convert_messages(&prepared).unwrap();

        let replay = messages[0].content[0]
            .reasoning_content
            .as_ref()
            .and_then(|content| content.reasoning_text.as_ref())
            .expect("signed reasoning block");
        assert_eq!(replay.text, "summary");
        assert_eq!(replay.signature.as_deref(), Some("opaque-signature"));
        assert_eq!(
            messages[0].content[1]
                .tool_use
                .as_ref()
                .map(|tool| tool.tool_use_id.as_str()),
            Some("call_1")
        );
    }

    #[test]
    fn convert_messages_no_system() {
        let messages = vec![ChatMessage::user("Hello")];

        let prepared = prepared_for(messages.as_slice());
        let (bedrock_msgs, system_blocks) = BedrockProvider::convert_messages(&prepared).unwrap();

        assert!(system_blocks.is_empty());
        assert_eq!(bedrock_msgs.len(), 1);
    }

    #[test]
    fn convert_messages_uses_compiled_prompt_sections_in_order() {
        let request = ChatRequest {
            model: "anthropic.claude-3-sonnet-20240229-v1:0".to_owned(),
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

        let rendered_messages = request.rendered_messages_with_compiled_sections();
        let prepared = prepared_for(rendered_messages.as_slice());
        let (_bedrock_msgs, system_blocks) = BedrockProvider::convert_messages(&prepared).unwrap();
        assert_eq!(system_blocks.len(), 2);
        assert_eq!(system_blocks[0].text, "Stable rules");
        assert_eq!(system_blocks[1].text, "Dynamic runtime");
    }

    #[test]
    fn bedrock_request_serializes_correctly() {
        let request = BedrockRequest {
            messages: vec![BedrockMessage {
                role: "user".into(),
                content: vec![BedrockContentBlock {
                    text: Some("Hello".into()),
                    image: None,
                    document: None,
                    audio: None,
                    video: None,
                    tool_use: None,
                    tool_result: None,
                    reasoning_content: None,
                }],
            }],
            system: vec![BedrockSystemBlock {
                text: "Be helpful".into(),
            }],
            inference_config: Some(BedrockInferenceConfig {
                temperature: Some(0.7),
                max_tokens: Some(8192),
            }),
            tool_config: None,
            additional_model_request_fields: None,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"inferenceConfig\""));
        assert!(json.contains("\"maxTokens\":8192"));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"system\""));
        assert!(json.contains("Be helpful"));
    }

    #[test]
    fn bedrock_request_omits_empty_system() {
        let request = BedrockRequest {
            messages: vec![BedrockMessage {
                role: "user".into(),
                content: vec![BedrockContentBlock {
                    text: Some("Hello".into()),
                    image: None,
                    document: None,
                    audio: None,
                    video: None,
                    tool_use: None,
                    tool_result: None,
                    reasoning_content: None,
                }],
            }],
            system: vec![],
            inference_config: None,
            tool_config: None,
            additional_model_request_fields: None,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(!json.contains("\"system\""));
        assert!(!json.contains("\"inferenceConfig\""));
    }

    #[test]
    fn bedrock_claude_request_serializes_reasoning_effort_in_additional_fields() {
        let request = ChatRequest {
            model: "anthropic.claude-opus-4-5".to_owned(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
            compiled_prompt: None,
        };
        let rendered = request.rendered_messages_with_compiled_sections();
        let prepared = prepared_for(rendered.as_slice());

        let bedrock_request = BedrockProvider::build_request(&request, &prepared).unwrap();
        let json = serde_json::to_value(&bedrock_request).unwrap();

        assert_eq!(
            json["additionalModelRequestFields"]["output_config"]["effort"],
            "high"
        );
        assert!(
            json["additionalModelRequestFields"]
                .get("thinking")
                .is_none()
        );
    }

    #[test]
    fn bedrock_claude_request_omits_reasoning_extension_for_disabled_reasoning() {
        let request = ChatRequest {
            model: "anthropic.claude-opus-4-5".to_owned(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::disabled()),
            compiled_prompt: None,
        };
        let rendered = request.rendered_messages_with_compiled_sections();
        let prepared = prepared_for(rendered.as_slice());

        let bedrock_request = BedrockProvider::build_request(&request, &prepared).unwrap();
        let json = serde_json::to_value(&bedrock_request).unwrap();

        assert!(json.get("additionalModelRequestFields").is_none());
    }

    #[test]
    fn bedrock_non_claude_request_omits_reasoning_extension() {
        let request = ChatRequest {
            model: "amazon.nova-pro-v1:0".to_owned(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
            compiled_prompt: None,
        };
        let rendered = request.rendered_messages_with_compiled_sections();
        let prepared = prepared_for(rendered.as_slice());

        let bedrock_request = BedrockProvider::build_request(&request, &prepared).unwrap();
        let json = serde_json::to_value(&bedrock_request).unwrap();

        assert!(json.get("additionalModelRequestFields").is_none());
    }

    #[test]
    fn bedrock_response_deserializes() {
        let json = r#"{
            "output": {
                "message": {
                    "content": [{"text": "Hello from Bedrock"}]
                }
            },
            "usage": {"inputTokens": 42, "outputTokens": 15}
        }"#;

        let response: BedrockResponse = serde_json::from_str(json).unwrap();
        assert_eq!(
            response.output.message.content[0].text.as_deref(),
            Some("Hello from Bedrock")
        );
        let usage = response.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(42));
        assert_eq!(usage.output_tokens, Some(15));
    }

    #[test]
    fn bedrock_response_without_usage() {
        let json = r#"{
            "output": {
                "message": {
                    "content": [{"text": "Hello"}]
                }
            }
        }"#;

        let response: BedrockResponse = serde_json::from_str(json).unwrap();
        assert!(response.usage.is_none());
    }

    #[test]
    fn sigv4_signing_produces_valid_format() {
        let url: Url = "https://bedrock-runtime.us-east-1.amazonaws.com/model/test/converse"
            .parse()
            .unwrap();
        let body = b"{}";
        let datetime = "20260319T120000Z";

        let auth = sign_request(
            "POST",
            &url,
            body,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            "us-east-1",
            "bedrock",
            datetime,
        );

        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260319/us-east-1/bedrock/aws4_request"));
        assert!(auth.contains("SignedHeaders=content-type;host;x-amz-date"));
        assert!(auth.contains("Signature="));
    }

    #[test]
    fn sigv4_signing_includes_security_token_header() {
        let url: Url = "https://bedrock-runtime.us-east-1.amazonaws.com/model/test/converse"
            .parse()
            .unwrap();
        let body = b"{}";
        let datetime = "20260319T120000Z";

        let auth = sign_request(
            "POST",
            &url,
            body,
            "AKID",
            "SECRET",
            Some("TOKEN"),
            "us-east-1",
            "bedrock",
            datetime,
        );

        assert!(auth.contains("x-amz-security-token"));
    }

    #[test]
    fn unix_to_datetime_epoch() {
        let (y, m, d, h, min, s) = unix_to_datetime(0);
        assert_eq!((y, m, d, h, min, s), (1970, 1, 1, 0, 0, 0));
    }

    #[test]
    fn unix_to_datetime_known_date() {
        // 2026-03-20 12:00:00 UTC = 1774008000
        let (y, m, d, h, min, s) = unix_to_datetime(1774008000);
        assert_eq!((y, m, d, h, min, s), (2026, 3, 20, 12, 0, 0));
    }

    #[test]
    fn amz_datetime_format() {
        let dt = BedrockProvider::amz_datetime();
        // Should be 16 chars: YYYYMMDDTHHmmSSZ
        assert_eq!(dt.len(), 16);
        assert!(dt.contains('T'));
        assert!(dt.ends_with('Z'));
    }
}
