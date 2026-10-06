use crate::attachments::{
    PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes,
    ensure_no_unrendered_attachments, prepare_messages_for_provider_async,
};
use crate::reasoning_registry;
use crate::types::{
    ChatRequest, ChatResponse, InputContentType, InputTypeSupport, ProviderCapabilities,
    ProviderInputCapabilities, ProviderReplayState, ProviderTermination, ProviderTimeoutPolicy,
    ProviderToolCall, Role, StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
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
#[serde(rename_all = "camelCase")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reasoning_text: Option<BedrockReasoningText>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
    #[serde(default)]
    cache_write_input_tokens: Option<u64>,
}

impl BedrockUsage {
    fn normalized(&self) -> TokenUsage {
        TokenUsage {
            input_tokens: self.input_tokens.and_then(|input| {
                input
                    .checked_add(self.cache_read_input_tokens?)?
                    .checked_add(self.cache_write_input_tokens?)
            }),
            output_tokens: self.output_tokens,
        }
    }
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

/// The exact request used by the signer, separately inspectable from the
/// transmitted URL. Keep this single construction path for all operations.
fn canonical_request(
    method: &str,
    url: &Url,
    body: &[u8],
    session_token: Option<&str>,
    datetime: &str,
) -> (String, String) {
    let host = url.host_str().unwrap_or_default();
    // AWS's non-S3 default signs a second URI encoding of the escaped path.
    // Preserve separators while encoding the percent bytes in model IDs/ARNs.
    // https://docs.rs/aws-sigv4/latest/aws_sigv4/http_request/enum.PercentEncodingMode.html
    let path = url
        .path()
        .split('/')
        .map(crate::definition::encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");

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
    (canonical_request, signed_headers)
}

/// Build an AWS SigV4 `Authorization` header value.
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
    let date = &datetime[..8];
    let (canonical_request, signed_headers) =
        canonical_request(method, url, body, session_token, datetime);
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
    pub(crate) fn environment_is_configured() -> bool {
        let access = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
        let secret = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
        let session = std::env::var("AWS_SESSION_TOKEN").ok();
        Self::validate_connection_values(
            &access,
            &secret,
            session.as_deref(),
            &Self::environment_region(),
        )
        .is_ok()
    }

    fn validate_connection(&self) -> Result<()> {
        Self::validate_connection_values(
            &self.access_key_id,
            &self.secret_access_key,
            self.session_token.as_deref(),
            &self.region,
        )
    }

    fn validate_connection_values(
        access: &str,
        secret: &str,
        session: Option<&str>,
        region: &str,
    ) -> Result<()> {
        if access.trim().is_empty() || secret.trim().is_empty() {
            anyhow::bail!(
                "Bedrock SigV4 requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY; a single provider API key is insufficient"
            );
        }
        if session.is_some_and(|token| token.trim().is_empty()) {
            anyhow::bail!("AWS_SESSION_TOKEN must be nonempty when supplied");
        }
        // AWS Bedrock bindRegion uses Smithy's host-label validation. Retain
        // this adapter's lowercase region contract; require one DNS label,
        // 1..=63 ASCII bytes, with alphanumeric boundaries, not a region list.
        // https://github.com/aws/smithy-go/blob/9b28af0b8afffb9debb149a07df6fc40edc6e529/endpoints/private/rulesfn/uri.go
        // https://github.com/aws/smithy-go/blob/9b28af0b8afffb9debb149a07df6fc40edc6e529/transport/http/host.go
        if !(1..=63).contains(&region.len())
            || region.starts_with('-')
            || region.ends_with('-')
            || !region
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        {
            anyhow::bail!(
                "Bedrock requires an AWS region that is a lowercase DNS label (1-63 bytes, alphanumeric start/end)"
            );
        }
        Ok(())
    }

    fn environment_region() -> String {
        std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string())
    }

    fn dns_suffix(&self) -> &'static str {
        if self.region.starts_with("cn-") {
            "amazonaws.com.cn"
        } else {
            "amazonaws.com"
        }
    }
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
        let region = Self::environment_region();

        let provider = Self {
            access_key_id,
            secret_access_key,
            session_token,
            region,
            timeout_policy,
            client: crate::http::build_client(timeout_policy),
        };
        provider.validate_connection()?;
        Ok(provider)
    }

    fn list_foundation_models_url(&self) -> String {
        format!(
            "https://bedrock.{}.{}/foundation-models",
            self.region,
            self.dns_suffix()
        )
    }

    /// Build the Converse API endpoint URL for the given model ID.
    fn converse_url(&self, model_id: &str) -> String {
        let encoded_model = crate::definition::encode_path_segment(model_id);
        format!(
            "https://bedrock-runtime.{}.{}/model/{}/converse",
            self.region,
            self.dns_suffix(),
            encoded_model
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
    ) -> Result<BedrockToolConfig> {
        if matches!(choice, Some(ToolChoice::None)) {
            anyhow::bail!("Bedrock None must omit toolConfig; it cannot become Auto");
        }
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
            ToolChoice::None => serde_json::Value::Null, // rejected above
            ToolChoice::Required => serde_json::json!({ "any": {} }),
            ToolChoice::Tool { name } => serde_json::json!({ "tool": { "name": name } }),
        });

        Ok(BedrockToolConfig { tools, tool_choice })
    }

    fn build_request(
        request: &ChatRequest,
        prepared: &PreparedProviderMessages,
    ) -> Result<BedrockRequest> {
        Self::build_request_with_catalog(
            request,
            prepared,
            crate::catalog::model_catalog().ok().as_deref(),
        )
    }

    fn build_request_with_catalog(
        request: &ChatRequest,
        prepared: &PreparedProviderMessages,
        catalog: Option<&crate::catalog::ModelCatalog>,
    ) -> Result<BedrockRequest> {
        let request = crate::tools::policy::prepare_request("bedrock", request.clone())?;
        let mut prepared = prepared.clone();
        crate::tools::policy::prepare_history("bedrock", &mut prepared.messages)?;
        let (messages, system) = Self::convert_messages(&prepared)?;
        let generation =
            crate::generation::anthropic_fields_with_catalog(catalog, "bedrock", &request)?;

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
                .map(|tools| Self::convert_tool_config(tools, request.tool_choice.clone()))
                .transpose()?,
            additional_model_request_fields: (!generation.is_empty())
                .then(|| serde_json::Value::Object(generation)),
        })
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

    fn parse_response(api_response: BedrockResponse) -> Result<ChatResponse> {
        let termination = api_response
            .stop_reason
            .as_deref()
            .map(ProviderTermination::from_openai_reason)
            .unwrap_or_else(|| ProviderTermination::Unknown("missing_stop_reason".to_owned()));

        let usage = api_response.usage.map(|u| u.normalized());

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
        let request = crate::tools::policy::prepare_request(self.name(), request)?;
        self.validate_connection()?;
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

        let api_response: BedrockResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;
        Self::parse_response(api_response)
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let request = crate::tools::policy::prepare_request(self.name(), request)?;
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
        chunks.push(Ok(
            StreamChunk::final_chunk_with(termination).with_usage(response.usage)
        ));
        Ok(Box::pin(futures_util::stream::iter(chunks)))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.validate_connection()?;
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

        let api_response: BedrockModelsResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;

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
#[path = "bedrock_signing_tests.rs"]
mod signing_tests;

#[cfg(test)]
mod tests {
    #[test]
    fn dated_and_regional_discovery_controls_match_actual_converse_platform_subset() {
        for (id, expected) in [
            (
                "anthropic.claude-opus-4-5-20251101-v1:0",
                vec!["none", "low", "medium", "high"],
            ),
            (
                "eu.anthropic.claude-opus-4-5-20251101-v1:0",
                vec!["none", "low", "medium", "high"],
            ),
            (
                "anthropic.claude-opus-4-6-v1",
                vec!["none", "low", "medium", "high", "xhigh", "max"],
            ),
            (
                "us.anthropic.claude-opus-4-6-v1",
                vec!["none", "low", "medium", "high", "xhigh", "max"],
            ),
            (
                "anthropic.claude-sonnet-4-6",
                vec!["none", "low", "medium", "high", "max"],
            ),
            (
                "anthropic.claude-opus-5",
                vec!["none", "low", "medium", "high", "xhigh", "max"],
            ),
        ] {
            let parsed = provider_model_from_bedrock_model_summary(
                serde_json::from_value(serde_json::json!({"modelId":id})).unwrap(),
            );
            let partial = crate::generation::test_catalog_model(
                "bedrock",
                id,
                "anthropic.claude-opus-4-6-v1",
                serde_json::json!({"thinkingLevelMap":{"max":"max"}}),
            );
            for catalog in [
                crate::generation::test_catalog(false),
                crate::generation::test_catalog(true),
                partial,
            ] {
                let mut models = vec![parsed.clone()];
                catalog.enrich("bedrock", &mut models);
                assert_eq!(models[0].id, id);
                let r = models[0].capabilities.reasoning.as_ref().unwrap();
                assert_eq!(r.effort_options, expected);
                let mut request = crate::generation::test_request(id);
                let prepared = prepared_for(&request.messages);
                for effort in &r.effort_options {
                    request.reasoning = Some(ReasoningConfig::Effort(
                        ReasoningEffort::from_str(effort).unwrap(),
                    ));
                    // Existing stream uses chat fallback and this same constructor.
                    let body = serde_json::to_value(
                        BedrockProvider::build_request_with_catalog(
                            &request,
                            &prepared,
                            Some(&catalog),
                        )
                        .unwrap(),
                    )
                    .unwrap();
                    if effort == "none" {
                        assert_eq!(
                            body["additionalModelRequestFields"]["thinking"]["type"],
                            "disabled"
                        );
                        assert!(
                            body["additionalModelRequestFields"]
                                .get("output_config")
                                .is_none()
                        );
                    } else {
                        assert_eq!(
                            body["additionalModelRequestFields"]["output_config"]["effort"],
                            effort.as_str()
                        );
                    }
                    assert_eq!(body["inferenceConfig"]["maxTokens"], 1024);
                    if effort == "none" {
                        continue;
                    }
                    if id.contains("4-5") {
                        assert!(
                            body["additionalModelRequestFields"]
                                .get("thinking")
                                .is_none()
                        );
                    } else {
                        assert_eq!(
                            body["additionalModelRequestFields"]["thinking"]["type"],
                            "adaptive"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn aws_optional_off_and_catalog_veto_match_converse_discovery() {
        for id in [
            "anthropic.claude-opus-4-5-20251101-v1:0",
            "us.anthropic.claude-opus-4-6-v1",
            "anthropic.claude-sonnet-5",
            "anthropic.claude-opus-5",
            "anthropic.claude-fable-5",
        ] {
            for veto in [false, true] {
                let catalog = crate::generation::test_catalog_model(
                    "bedrock",
                    id,
                    "anthropic.claude-opus-4-6-v1",
                    if veto {
                        serde_json::json!({"thinkingLevelMap":{"off":null}})
                    } else {
                        serde_json::json!({"thinkingLevelMap":{"max":"max"}})
                    },
                );
                let mut models = vec![provider_model_from_bedrock_model_summary(
                    serde_json::from_value(serde_json::json!({"modelId":id})).unwrap(),
                )];
                catalog.enrich("bedrock", &mut models);
                let allowed = !veto && !id.contains("fable");
                assert_eq!(
                    models[0]
                        .capabilities
                        .reasoning
                        .as_ref()
                        .unwrap()
                        .effort_options
                        .iter()
                        .any(|e| e == "none"),
                    allowed
                );
                for selected in [
                    None,
                    Some(ReasoningConfig::Disabled),
                    Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                ] {
                    let mut request = crate::generation::test_request(id);
                    request.reasoning = selected;
                    // Production stream remains chat fallback; both use this constructor.
                    let result = BedrockProvider::build_request_with_catalog(
                        &request,
                        &prepared_for(&request.messages),
                        Some(&catalog),
                    );
                    if selected.is_none() || allowed {
                        let body = serde_json::to_value(result.unwrap()).unwrap();
                        assert!(
                            body["additionalModelRequestFields"]
                                .get("output_config")
                                .is_none()
                        );
                        if selected.is_none() {
                            assert!(
                                body["additionalModelRequestFields"]
                                    .get("thinking")
                                    .is_none()
                            );
                        } else {
                            assert_eq!(
                                body["additionalModelRequestFields"]["thinking"]["type"],
                                "disabled"
                            );
                        }
                        assert_eq!(body["inferenceConfig"]["maxTokens"], 1024);
                    } else {
                        assert!(result.is_err());
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn aws_explicit_off_cannot_be_omitted_by_negative_catalog() {
        for id in [
            "anthropic.claude-sonnet-5",
            "anthropic.claude-opus-5",
            "us.anthropic.claude-opus-4-6-v1",
        ] {
            for reasoning in [false, true] {
                for off_map in [
                    serde_json::json!({}),
                    serde_json::json!({"off":"none"}),
                    serde_json::json!({"off":null}),
                ] {
                    for native_case in ["absent", "unknown", "veto"] {
                        let mut parsed = provider_model_from_bedrock_model_summary(
                            serde_json::from_value(serde_json::json!({"modelId":id})).unwrap(),
                        );
                        // AWS summary has no disabled-mode field. These two
                        // synthetic internal facts exercise the shared native
                        // boundary, not a claimed AWS discovery schema.
                        if native_case != "absent" {
                            parsed
                                .capabilities
                                .reasoning
                                .as_mut()
                                .unwrap()
                                .native
                                .insert(
                                    "thinking.types.disabled".into(),
                                    if native_case == "veto" {
                                        Some(false)
                                    } else {
                                        None
                                    },
                                );
                        }
                        let native = parsed
                            .capabilities
                            .reasoning
                            .as_ref()
                            .unwrap()
                            .native
                            .clone();
                        let catalog = crate::generation::test_catalog_model(
                            "bedrock",
                            id,
                            "anthropic.claude-opus-4-6-v1",
                            serde_json::json!({"reasoning":reasoning,"thinkingLevelMap":off_map}),
                        );
                        let mut models = vec![parsed];
                        catalog.enrich("bedrock", &mut models);
                        let allowed = reasoning
                            && off_map.get("off") != Some(&serde_json::Value::Null)
                            && native_case != "veto";
                        assert_eq!(
                            models[0]
                                .capabilities
                                .reasoning
                                .as_ref()
                                .unwrap()
                                .effort_options
                                .iter()
                                .any(|e| e == "none"),
                            allowed
                        );
                        for setting in [
                            None,
                            Some(ReasoningConfig::Disabled),
                            Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                        ] {
                            let mut request = crate::generation::test_request(id);
                            request.reasoning = setting;
                            // Bedrock stream still calls chat and uses this constructor.
                            let result = crate::generation::with_native_reasoning(
                                "bedrock",
                                true,
                                [(id.into(), native.clone())].into_iter().collect(),
                                async {
                                    BedrockProvider::build_request_with_catalog(
                                        &request,
                                        &prepared_for(&request.messages),
                                        Some(&catalog),
                                    )
                                },
                            )
                            .await;
                            if setting.is_none() || allowed {
                                let body = serde_json::to_value(result.unwrap()).unwrap();
                                assert!(
                                    body["additionalModelRequestFields"]
                                        .get("output_config")
                                        .is_none()
                                );
                                if setting.is_none() {
                                    assert!(
                                        body["additionalModelRequestFields"]
                                            .get("thinking")
                                            .is_none()
                                    );
                                } else {
                                    assert_eq!(
                                        body["additionalModelRequestFields"]["thinking"]["type"],
                                        "disabled"
                                    );
                                }
                                assert_eq!(body["inferenceConfig"]["maxTokens"], 1024);
                            } else {
                                let error = result.unwrap_err().to_string();
                                assert!(
                                    error.contains("explicit Claude off")
                                        || error.contains(
                                            "unsupported by the model's catalog thinking map"
                                        )
                                        || error.contains("denies disabled thinking"),
                                    "{error}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// A documented AWS platform vocabulary update, represented both as a
    /// saved catalog profile and as a fresh models.dev source update. Limits
    /// remain those of the pinned source; no second limits catalog is created.
    fn aws_46_catalog(fresh: bool) -> crate::catalog::ModelCatalog {
        use crate::catalog::generator::{SOURCE_URLS, SourceSnapshot, generate};
        let ids = [
            "anthropic.claude-opus-4-6-v1",
            "us.anthropic.claude-opus-4-6-v1",
            "eu.anthropic.claude-opus-4-6-v1",
            "au.anthropic.claude-opus-4-6-v1",
            "global.anthropic.claude-opus-4-6-v1",
        ];
        if fresh {
            let mut source: SourceSnapshot =
                serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                    .unwrap();
            for id in ids {
                source.sources.get_mut(SOURCE_URLS[0]).unwrap().body["amazon-bedrock"]["models"]
                    [id]["reasoning_options"][0]["values"] =
                    serde_json::json!(["low", "medium", "high", "xhigh", "max"]);
            }
            let generated = generate(&source, true).unwrap();
            crate::catalog::ModelCatalog::parse(
                &serde_json::to_string(&generated.models).unwrap(),
                &serde_json::to_string(&generated.provenance).unwrap(),
            )
            .unwrap()
        } else {
            let mut models: serde_json::Value =
                serde_json::from_str(include_str!("../../tests/fixtures/catalog/models.json"))
                    .unwrap();
            for id in ids {
                models["amazon-bedrock"][id]["thinkingLevelMap"]["xhigh"] =
                    serde_json::json!("xhigh");
                models["amazon-bedrock"][id]["sourceGeneration"]["reasoningOptions"] = serde_json::json!([{"type":"effort","values":["low","medium","high","xhigh","max"]}]);
            }
            crate::catalog::ModelCatalog::parse(
                &models.to_string(),
                include_str!("../../tests/fixtures/catalog/provenance.json"),
            )
            .unwrap()
        }
    }

    #[test]
    fn aws_opus_46_xhigh_survives_discovery_and_native_converse_materialization() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        for fresh in [false, true] {
            let updated = aws_46_catalog(fresh);
            let original = crate::generation::test_catalog(fresh);
            for id in [
                "anthropic.claude-opus-4-6-v1",
                "us.anthropic.claude-opus-4-6-v1",
                "eu.anthropic.claude-opus-4-6-v1",
                "au.anthropic.claude-opus-4-6-v1",
                "global.anthropic.claude-opus-4-6-v1",
            ] {
                let discovered = crate::generation::test_discovery(&updated, "bedrock", id);
                assert!(
                    discovered
                        .capabilities
                        .reasoning
                        .as_ref()
                        .unwrap()
                        .effort_options
                        .contains(&"xhigh".into())
                );
                let mut request = crate::generation::test_request(id);
                request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::XHigh));
                let prepared = prepare_messages_for_provider_model(
                    "bedrock",
                    id,
                    &provider.capabilities(),
                    &request.messages,
                )
                .unwrap();
                for catalog in [None, Some(&original), Some(&updated)] {
                    // Existing stream_chat delegates to chat and uses this same constructor.
                    let body = serde_json::to_value(
                        BedrockProvider::build_request_with_catalog(&request, &prepared, catalog)
                            .unwrap(),
                    )
                    .unwrap();
                    assert_eq!(
                        body["additionalModelRequestFields"]["output_config"]["effort"],
                        "xhigh"
                    );
                    assert_eq!(
                        body["additionalModelRequestFields"]["thinking"]["type"],
                        "adaptive"
                    );
                    assert_eq!(body["inferenceConfig"]["maxTokens"], 1024);
                }
            }
        }
    }

    #[test]
    fn aws_46_platform_exception_cannot_widen_other_profiles_or_override_denials() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        let id = "anthropic.claude-opus-4-6-v1";
        for metadata in [
            serde_json::json!({"compat":{"supportsReasoningEffort":false},"thinkingLevelMap":{"xhigh":"xhigh"}}),
            serde_json::json!({"thinkingLevelMap":{"xhigh":"invented"}}),
        ] {
            let catalog = crate::generation::test_catalog_model("amazon-bedrock", id, id, metadata);
            let mut request = crate::generation::test_request(id);
            request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::XHigh));
            let prepared = prepare_messages_for_provider_model(
                "bedrock",
                id,
                &provider.capabilities(),
                &request.messages,
            )
            .unwrap();
            assert!(
                BedrockProvider::build_request_with_catalog(&request, &prepared, Some(&catalog))
                    .is_err()
            );
            let discovered = crate::generation::test_discovery(&catalog, "bedrock", id);
            assert!(
                discovered
                    .capabilities
                    .reasoning
                    .as_ref()
                    .is_none_or(|r| !r.effort_options.contains(&"xhigh".into()))
            );
        }
        for id in [
            "anthropic.claude-opus-4-5-20251101-v1:0",
            "anthropic.claude-sonnet-4-6",
            "anthropic.claude-opus-4-8",
            "anthropic.claude-opus-4-6-opaque",
            "arn:aws:bedrock:region:account:application-inference-profile/opaque",
        ] {
            let mut request = crate::generation::test_request(id);
            request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::XHigh));
            let prepared = prepare_messages_for_provider_model(
                "bedrock",
                id,
                &provider.capabilities(),
                &request.messages,
            )
            .unwrap();
            assert!(
                BedrockProvider::build_request_with_catalog(&request, &prepared, None).is_err()
            );
        }
    }

    #[test]
    fn converse_mandatory_and_source_denials_override_stale_metadata() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        let stale = crate::generation::test_catalog_model(
            "amazon-bedrock",
            "anthropic.claude-fable-5",
            "anthropic.claude-opus-4-7",
            serde_json::json!({"reasoning":false,"thinkingLevelMap":{"off":"low"}}),
        );
        let mut request = crate::generation::test_request("anthropic.claude-fable-5");
        let prepared = prepare_messages_for_provider_model(
            "bedrock",
            &request.model,
            &provider.capabilities(),
            &request.messages,
        )
        .unwrap();
        for off in [
            ReasoningConfig::Disabled,
            ReasoningConfig::Effort(ReasoningEffort::None),
        ] {
            request.reasoning = Some(off);
            assert!(
                BedrockProvider::build_request_with_catalog(&request, &prepared, Some(&stale))
                    .is_err()
            );
        }
        let fresh = crate::generation::test_source_temperature(
            "amazon-bedrock",
            "anthropic.claude-sonnet-5",
        );
        request.model = "anthropic.claude-sonnet-5".into();
        request.temperature = Some(0.7);
        for reasoning in [
            None,
            Some(ReasoningConfig::Disabled),
            Some(ReasoningConfig::Effort(ReasoningEffort::High)),
        ] {
            request.reasoning = reasoning;
            assert!(
                BedrockProvider::build_request_with_catalog(&request, &prepared, Some(&fresh))
                    .is_err()
            );
        }
        request.model = "anthropic.claude-opus-4-7-opaque-alias".into();
        request.temperature = None;
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        assert!(BedrockProvider::build_request_with_catalog(&request, &prepared, None).is_err());
    }

    #[test]
    fn saved_fresh_and_fallback_converse_profiles_enable_adaptive_not_manual_thinking() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        for fresh in [false, true] {
            let catalog = crate::generation::test_catalog(fresh);
            for id in [
                "anthropic.claude-opus-4-7",
                "us.anthropic.claude-opus-4-7",
                "eu.anthropic.claude-opus-4-7",
                "anthropic.claude-opus-4-6-v1",
            ] {
                let mut request = crate::generation::test_request(id);
                request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
                let prepared = prepare_messages_for_provider_model(
                    "bedrock",
                    id,
                    &provider.capabilities(),
                    &request.messages,
                )
                .unwrap();
                for snapshot in [None, Some(&catalog)] {
                    // stream_chat delegates to chat; both use this Converse constructor.
                    let body = serde_json::to_value(
                        BedrockProvider::build_request_with_catalog(&request, &prepared, snapshot)
                            .unwrap(),
                    )
                    .unwrap();
                    assert_eq!(
                        body["additionalModelRequestFields"]["thinking"]["type"],
                        "adaptive"
                    );
                    assert_eq!(
                        body["additionalModelRequestFields"]["output_config"]["effort"],
                        "high"
                    );
                    assert_eq!(body["inferenceConfig"]["maxTokens"], 1024);
                    assert!(
                        body["additionalModelRequestFields"]
                            .get("anthropic_version")
                            .is_none()
                    );
                }
            }
            for id in [
                "anthropic.claude-fable-5",
                "us.anthropic.claude-mythos-5",
                "anthropic.claude-mythos-preview",
            ] {
                let mut request = crate::generation::test_request(id);
                let prepared = prepare_messages_for_provider_model(
                    "bedrock",
                    id,
                    &provider.capabilities(),
                    &request.messages,
                )
                .unwrap();
                for off in [
                    ReasoningConfig::Disabled,
                    ReasoningConfig::Effort(ReasoningEffort::None),
                ] {
                    request.reasoning = Some(off);
                    for snapshot in [None, Some(&catalog)] {
                        assert!(
                            BedrockProvider::build_request_with_catalog(
                                &request, &prepared, snapshot
                            )
                            .is_err()
                        );
                    }
                }
            }
        }
        let mut request =
            crate::generation::test_request("anthropic.claude-opus-4-5-20251101-v1:0");
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        let prepared = prepare_messages_for_provider_model(
            "bedrock",
            &request.model,
            &provider.capabilities(),
            &request.messages,
        )
        .unwrap();
        let body = serde_json::to_value(
            BedrockProvider::build_request_with_catalog(&request, &prepared, None).unwrap(),
        )
        .unwrap();
        assert_eq!(
            body["additionalModelRequestFields"]["output_config"]["effort"],
            "high"
        );
        assert_eq!(
            body["additionalModelRequestFields"]["anthropic_beta"][0],
            "effort-2025-11-24"
        );
        assert!(
            body["additionalModelRequestFields"]
                .get("thinking")
                .is_none()
        );
        request.model =
            "arn:aws:bedrock:region:account:application-inference-profile/opaque".into();
        assert!(BedrockProvider::build_request_with_catalog(&request, &prepared, None).is_err());
    }

    #[test]
    fn converse_honors_source_temperature_denial_without_applying_claude_policy_to_nova() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        let id = "amazon.nova-pro-v1:0";
        let negative = crate::generation::test_source_temperature("amazon-bedrock", id);
        let mut request = crate::generation::test_request(id);
        request.temperature = Some(0.7);
        let prepared = prepare_messages_for_provider_model(
            "bedrock",
            id,
            &provider.capabilities(),
            &request.messages,
        )
        .unwrap();
        assert!(
            BedrockProvider::build_request_with_catalog(&request, &prepared, Some(&negative))
                .is_err()
        );
        let body = serde_json::to_value(
            BedrockProvider::build_request_with_catalog(&request, &prepared, None).unwrap(),
        )
        .unwrap();
        assert!(body["inferenceConfig"].get("temperature").is_some());
        request.model = "anthropic.claude-opus-4-8".into();
        for reasoning in [
            None,
            Some(ReasoningConfig::Disabled),
            Some(ReasoningConfig::Effort(ReasoningEffort::High)),
        ] {
            request.reasoning = reasoning;
            assert!(
                BedrockProvider::build_request_with_catalog(&request, &prepared, None).is_err()
            );
        }
    }

    #[test]
    fn tool_modes_none_and_unsupported_limit_are_validated_before_converse() {
        let provider = BedrockProvider::new("unused", "unused", "us-east-1");
        for (choice, expected) in [
            (ToolChoice::Auto, "auto"),
            (ToolChoice::Required, "any"),
            (
                ToolChoice::Tool {
                    name: "lookup".into(),
                },
                "tool",
            ),
        ] {
            let mut request = crate::tools::policy::test_request();
            request.model = "anthropic.claude-3-5-sonnet-20240620-v1:0".into();
            request.tool_choice = Some(choice);
            let prepared = prepare_messages_for_provider(
                "bedrock",
                &provider.capabilities(),
                &request.messages,
            )
            .unwrap();
            let wire = BedrockProvider::build_request(&request, &prepared).unwrap();
            assert!(
                wire.tool_config
                    .unwrap()
                    .tool_choice
                    .unwrap()
                    .get(expected)
                    .is_some()
            );
        }
        let mut request = crate::tools::policy::test_request();
        request.tool_choice = Some(ToolChoice::None);
        let prepared =
            prepare_messages_for_provider("bedrock", &provider.capabilities(), &request.messages)
                .unwrap();
        assert!(
            BedrockProvider::build_request(&request, &prepared)
                .unwrap()
                .tool_config
                .is_none()
        );
        request.tool_choice = Some(ToolChoice::Auto);
        request.parallel_tool_calls = Some(false);
        assert!(BedrockProvider::build_request(&request, &prepared).is_err());
    }

    #[test]
    fn usage_normalization_requires_complete_separate_cache_counters() {
        let complete: super::BedrockUsage = serde_json::from_value(serde_json::json!({
            "inputTokens":10,"cacheReadInputTokens":100,"cacheWriteInputTokens":20,"outputTokens":8
        }))
        .unwrap();
        assert_eq!(complete.normalized().input_tokens, Some(130));
        assert_eq!(complete.normalized().output_tokens, Some(8));
        let missing: super::BedrockUsage = serde_json::from_value(serde_json::json!({
            "inputTokens":10,"outputTokens":8
        }))
        .unwrap();
        assert_eq!(missing.normalized().input_tokens, None);
        assert_eq!(missing.normalized().output_tokens, Some(8));
    }

    use super::*;
    use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
    use crate::traits::Provider;
    use crate::types::{
        ChatMessage, ChatRequest, CompiledPromptPayload, MessageProvenance, MessageSourceRef,
        ProviderReplayState, ReasoningConfig, ReasoningEffort,
    };
    use std::sync::{Mutex, OnceLock};

    fn bedrock_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    // Restore every AWS connection input even when an assertion panics, while
    // holding the same lock as the pre-existing environment tests.
    struct BedrockTestEnvironment {
        saved: [(&'static str, Option<std::ffi::OsString>); 5],
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl BedrockTestEnvironment {
        fn new() -> Self {
            let lock = bedrock_env_lock()
                .lock()
                .expect("bedrock env lock poisoned");
            let saved = [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
            ]
            .map(|name| (name, std::env::var_os(name)));
            Self { saved, _lock: lock }
        }

        fn set(&self, name: &str, value: Option<&str>) {
            assert!(self.saved.iter().any(|(saved, _)| *saved == name));
            // SAFETY: test-only AWS mutations are serialized by bedrock_env_lock.
            unsafe {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    impl Drop for BedrockTestEnvironment {
        fn drop(&mut self) {
            // SAFETY: the environment lock remains held until restoration ends.
            unsafe {
                for (name, value) in &self.saved {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    #[test]
    fn bedrock_region_requires_one_bounded_dns_label() {
        for region in [
            "",
            "-",
            "-us-east-1",
            "us-east-1-",
            "region.invalid",
            "us_east_1",
            &"a".repeat(64),
        ] {
            let error = BedrockProvider::validate_connection_values(
                "dummy-access",
                "dummy-secret",
                Some("dummy-session"),
                region,
            )
            .unwrap_err();
            assert!(error.to_string().contains("DNS label"));
            assert!(!error.to_string().contains("dummy"));
        }
        // A label bound, not an allowlist of regions currently offered by AWS.
        for region in [
            "us-east-1",
            "us-gov-west-1",
            "cn-north-1",
            "a",
            &"a".repeat(63),
        ] {
            assert!(
                BedrockProvider::validate_connection_values(
                    "dummy-access",
                    "dummy-secret",
                    Some("dummy-session"),
                    region,
                )
                .is_ok(),
                "{region}"
            );
        }
    }

    #[tokio::test]
    async fn invalid_bedrock_regions_fail_lifecycle_before_network() {
        for region in [
            "-",
            "-us-east-1",
            "us-east-1-",
            "region.invalid",
            &"a".repeat(64),
        ] {
            let provider = BedrockProvider::new("dummy-access", "dummy-secret", region);
            let request = ChatRequest {
                model: "model:0".into(),
                messages: vec![ChatMessage::user("dummy")],
                temperature: None,
                max_tokens: None,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            };
            for error in [
                provider.list_models().await.unwrap_err(),
                provider.warmup().await.unwrap_err(),
                provider.chat(request.clone()).await.unwrap_err(),
            ] {
                assert!(error.to_string().contains("DNS label"));
            }
            let error = match provider.stream_chat(request).await {
                Err(error) => error,
                Ok(_) => panic!("invalid region must fail before stream setup"),
            };
            assert!(error.to_string().contains("DNS label"));
        }
    }

    #[test]
    fn bedrock_region_environment_availability_and_validation_agree() {
        let env = BedrockTestEnvironment::new();
        env.set("AWS_ACCESS_KEY_ID", Some("dummy-access"));
        env.set("AWS_SECRET_ACCESS_KEY", Some("dummy-secret"));
        env.set("AWS_SESSION_TOKEN", Some("dummy-session"));
        env.set("AWS_DEFAULT_REGION", Some("cn-north-1"));
        let definition = crate::provider_definition("bedrock").unwrap();
        for (region, valid) in [
            ("-", false),
            ("-us-east-1", false),
            ("us-east-1-", false),
            ("region.invalid", false),
            (&"a".repeat(64), false),
            ("us-east-1", true),
            ("us-gov-west-1", true),
            ("cn-north-1", true),
        ] {
            env.set("AWS_REGION", Some(region));
            assert_eq!(
                BedrockProvider::environment_is_configured(),
                valid,
                "{region}"
            );
            assert_eq!(
                crate::provider_is_available(true, true, true, definition),
                valid,
                "{region}"
            );
            let provider = BedrockProvider::from_env();
            assert_eq!(provider.is_ok(), valid, "{region}");
            assert_eq!(
                BedrockProvider::new("dummy-access", "dummy-secret", region)
                    .validate_connection()
                    .is_ok(),
                valid
            );
            if let Ok(provider) = provider {
                assert_eq!(
                    provider.region, region,
                    "AWS_REGION must outrank the fallback"
                );
                assert_eq!(provider.session_token.as_deref(), Some("dummy-session"));
            }
        }
        env.set("AWS_REGION", None);
        assert_eq!(BedrockProvider::from_env().unwrap().region, "cn-north-1");
        env.set("AWS_DEFAULT_REGION", Some("-"));
        assert!(!BedrockProvider::environment_is_configured());
        assert!(!crate::provider_is_available(
            false, false, false, definition
        ));
        assert!(BedrockProvider::from_env().is_err());
        env.set("AWS_DEFAULT_REGION", None);
        assert_eq!(BedrockProvider::from_env().unwrap().region, "us-east-1");
        assert!(BedrockProvider::environment_is_configured());
        env.set("AWS_SESSION_TOKEN", Some(""));
        assert!(!BedrockProvider::environment_is_configured());
        env.set("AWS_SESSION_TOKEN", None);
        assert!(BedrockProvider::environment_is_configured());
        env.set("AWS_SECRET_ACCESS_KEY", None);
        assert!(!BedrockProvider::environment_is_configured());
        assert!(!crate::provider_is_available(true, true, true, definition));
    }

    fn prepared_for(messages: &[ChatMessage]) -> crate::attachments::PreparedProviderMessages {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        prepare_messages_for_provider(provider.name(), &provider.capabilities(), messages).unwrap()
    }

    #[test]
    fn foreign_reasoning_is_present_in_bedrock_wire_as_unsigned_text() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
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
            model: "anthropic.claude-target".into(),
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
        let wire = BedrockProvider::build_request(&request, &prepared).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        assert!(json.contains("meaningful rationale"));
        assert!(json.contains("portable unsigned text"));
        assert!(!json.contains("reasoning_details"));
    }

    #[test]
    fn multiblock_reasoning_is_not_duplicated_on_bedrock_wire() {
        let provider = BedrockProvider::new("AKID", "SECRET", "us-east-1");
        let mut canonical = ChatMessage::assistant("answer");
        canonical.reasoning_content = Some("first second".into());
        canonical.provider_replay_state = Some(ProviderReplayState::for_model(
            "bedrock",
            "source-model",
            serde_json::json!({"blocks":[
                {"reasoningText":{"text":"first ","signature":"opaque-one"}},
                {"reasoningText":{"text":"second","signature":"opaque-two"}}
            ]}),
        ));
        canonical.provenance = Some(MessageProvenance {
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
            model: "different-model".into(),
            messages: vec![canonical.clone()],
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
            &request.model,
            &provider.capabilities(),
            &request.messages,
        )
        .unwrap();
        let wire = BedrockProvider::build_request(&request, &prepared).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        assert_eq!(json.matches("first second").count(), 1);
        assert!(!json.contains("opaque-one"));
        let same_request = ChatRequest {
            model: "source-model".into(),
            ..request.clone()
        };
        let compatible = prepare_messages_for_provider_model(
            provider.name(),
            &same_request.model,
            &provider.capabilities(),
            &same_request.messages,
        )
        .unwrap();
        let exact_wire = BedrockProvider::build_request(&same_request, &compatible).unwrap();
        let exact_json = serde_json::to_string(&exact_wire).unwrap();
        assert!(exact_json.contains("opaque-one"));
        assert!(exact_json.contains("opaque-two"));
        assert!(!exact_json.contains("first second"));
        assert!(canonical.provider_replay_state.is_some());
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
    fn sigv4_connection_requires_pair_and_nonempty_session_token() {
        for provider in [
            BedrockProvider::new("dummy-access", "", "us-east-1"),
            BedrockProvider::new("", "dummy-secret", "us-east-1"),
            BedrockProvider::new("dummy-access", "dummy-secret", "region.invalid"),
            BedrockProvider::with_session_token("dummy-access", "dummy-secret", "us-east-1", ""),
        ] {
            assert!(provider.validate_connection().is_err());
        }
        let provider = BedrockProvider::with_session_token(
            "dummy-access",
            "dummy-secret",
            "cn-north-1",
            "dummy-session",
        );
        assert!(provider.validate_connection().is_ok());
        assert_eq!(
            provider.converse_url("arn:aws-cn:bedrock:cn-north-1::foundation-model/example~1"),
            "https://bedrock-runtime.cn-north-1.amazonaws.com.cn/model/arn%3Aaws-cn%3Abedrock%3Acn-north-1%3A%3Afoundation-model%2Fexample~1/converse"
        );
        assert_eq!(
            provider.list_foundation_models_url(),
            "https://bedrock.cn-north-1.amazonaws.com.cn/foundation-models"
        );
        let url: Url = provider.converse_url("model:0").parse().unwrap();
        let auth = sign_request(
            "POST",
            &url,
            b"{}",
            "dummy-access",
            "dummy-secret",
            Some("dummy-session"),
            "cn-north-1",
            SERVICE,
            "20261001T120000Z",
        );
        assert!(auth.contains("Credential=dummy-access/20261001/cn-north-1/bedrock/aws4_request"));
        assert!(auth.contains("SignedHeaders=content-type;host;x-amz-date;x-amz-security-token"));
        let without_session = sign_request(
            "POST",
            &url,
            b"{}",
            "dummy-access",
            "dummy-secret",
            None,
            "cn-north-1",
            SERVICE,
            "20261001T120000Z",
        );
        assert_ne!(auth, without_session);
        assert!(!auth.contains("dummy-secret"));
        assert!(!auth.contains("dummy-session"));
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
        assert_eq!(
            reasoning.effort_options,
            vec!["none", "low", "medium", "high"]
        );
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
        assert_eq!(
            reasoning.effort_options,
            vec!["none", "low", "medium", "high"]
        );
        assert_eq!(models[0].capabilities.vision, Some(true));
        assert_eq!(models[0].active, Some(true));

        assert!(models[1].capabilities.reasoning.is_none());
    }

    #[test]
    fn from_env_reads_variables() {
        let env = BedrockTestEnvironment::new();
        env.set("AWS_ACCESS_KEY_ID", Some("env-akid"));
        env.set("AWS_SECRET_ACCESS_KEY", Some("env-secret"));
        env.set("AWS_SESSION_TOKEN", Some("env-token"));
        env.set("AWS_REGION", Some("ap-southeast-1"));
        let provider = BedrockProvider::from_env().unwrap();
        assert_eq!(provider.access_key_id, "env-akid");
        assert_eq!(provider.secret_access_key, "env-secret");
        assert_eq!(provider.session_token.as_deref(), Some("env-token"));
        assert_eq!(provider.region, "ap-southeast-1");
    }

    #[test]
    fn from_env_defaults_region() {
        let env = BedrockTestEnvironment::new();
        env.set("AWS_ACCESS_KEY_ID", Some("akid"));
        env.set("AWS_SECRET_ACCESS_KEY", Some("secret"));
        env.set("AWS_SESSION_TOKEN", None);
        env.set("AWS_REGION", None);
        env.set("AWS_DEFAULT_REGION", None);
        let provider = BedrockProvider::from_env().unwrap();
        assert_eq!(provider.region, "us-east-1");
        assert!(provider.session_token.is_none());
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
            "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-3-sonnet-20240229-v1%3A0/converse"
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
    fn bedrock_claude_request_sends_explicit_disabled_thinking() {
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

        assert_eq!(
            json["additionalModelRequestFields"]["thinking"]["type"],
            "disabled"
        );
    }

    #[test]
    fn bedrock_non_claude_rejects_unimplemented_reasoning_setting() {
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

        assert!(BedrockProvider::build_request(&request, &prepared).is_err());
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

#[cfg(test)]
#[path = "wire_tests/bedrock.rs"]
mod wire_contract_tests;
