#[cfg(test)]
use crate::attachments::prepare_messages_for_request;
use crate::{
    attachments::{
        PreparedAttachmentSource, PreparedProviderMessages, attachment_bytes, attachment_data_url,
        ensure_no_unrendered_attachments, prepare_messages_for_request_async,
    },
    tools::call::{StreamToolCallAccumulator, StreamToolCallDelta, StreamToolFunctionDelta},
    tools::parse::parse_tool_calls,
    tools::stream::IncrementalSseDecoder,
    types::{
        ChatMessage, ChatRequest, ChatResponse, InputContentType, InputTypeSupport,
        ProviderCapabilities, ProviderInputCapabilities, ProviderReplayState, ProviderTermination,
        ProviderTimeoutPolicy, Role, StreamChunk, ToolChoice, ToolDefinition,
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
use std::collections::HashMap;

use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

/// How to pass the credential in HTTP requests.
#[derive(Debug, Clone)]
pub enum AuthStyle {
    /// `Authorization: Bearer <credential>`
    Bearer,
    /// `x-api-key: <credential>`
    XApiKey,
    /// Custom header name: `<header>: <credential>`
    Custom(String),
}

/// A generic provider for any API that follows the OpenAI chat completions format.
///
/// Configurable with name, base URL, credential, auth style, and various
/// capability flags. Used as the backbone for 40+ providers that share the
/// same wire format but differ in endpoint and authentication.
pub struct ChatCompletionsApiCompatibleProvider {
    name: String,
    base_url: String,
    credential: String,
    auth_style: AuthStyle,
    input_types: ProviderInputCapabilities,
    merge_system_into_user: bool,
    replay_reasoning_content: bool,
    timeout_policy: ProviderTimeoutPolicy,
    extra_headers: HashMap<String, String>,
    client: Client,
}

const COMPATIBLE_REPLAY_STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CompatibleAssistantReplayState {
    schema_version: u32,
    assistant_message: CompatibleAssistantReplayMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(super) struct CompatibleAssistantReplayMessage {
    content: Option<String>,
    pub(super) reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reasoning_details: Option<serde_json::Value>,
    tool_calls: Vec<ApiToolCall>,
}

// ── OpenAI-compatible API request types ─────────────────────────────────────

#[derive(Debug, Serialize)]
struct ApiChatRequest {
    #[serde(skip)]
    cache_affinity: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_options: Option<serde_json::Value>,
    #[serde(flatten)]
    generation: crate::generation::Fields,
}

#[derive(Debug, Serialize)]
struct ApiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<ApiMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_details: Option<serde_json::Value>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    function: ApiToolCallFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ApiToolCallFunction {
    name: String,
    arguments: String,
}

// ── OpenAI-compatible API response types ────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ApiChatResponse {
    #[serde(default)]
    x_groq: Option<serde_json::Value>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    id: Option<String>,
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
    reasoning_details: Option<serde_json::Value>,
    #[serde(default)]
    tool_calls: Option<serde_json::Value>,
    #[serde(default)]
    function_call: Option<serde_json::Value>,
}

impl ApiResponseMessage {
    fn effective_content(&self) -> String {
        self.content
            .as_ref()
            .map(|c| strip_think_tags(c))
            .unwrap_or_default()
    }
}

type ApiUsage = crate::usage::ChatUsage;

// ── SSE streaming response types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct StreamResponse {
    #[serde(default)]
    model: Option<String>,
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
    reasoning_details: Option<serde_json::Value>,
    #[serde(default)]
    tool_calls: Option<Vec<StreamToolCallDelta>>,
    #[serde(default)]
    function_call: Option<StreamToolFunctionDelta>,
}

impl StreamDelta {
    fn has_payload(&self, native_details: bool) -> bool {
        (native_details && self.reasoning_details.is_some())
            || self
                .content
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
#[serde(untagged)]
enum ModelsListResponse {
    Wrapped { data: Vec<CompatModelEntry> },
    // Together's existing /models operation returns a top-level array.
    Flat(Vec<CompatModelEntry>),
}

#[derive(Debug, Deserialize)]
struct CompatModelEntry {
    id: String,
    #[serde(default)]
    created: Option<i64>,
    #[serde(default)]
    owned_by: Option<String>,
    #[serde(default)]
    capabilities: serde_json::Value,
    #[serde(default, rename = "type")]
    model_type: serde_json::Value,
}

/// Strip `<think>...</think>` tags from content returned by some thinking models.
fn strip_think_tags(text: &str) -> String {
    let mut result = text.to_owned();
    while let Some(start) = result.find("<think>") {
        if let Some(end) = result[start..].find("</think>") {
            result = format!("{}{}", &result[..start], &result[start + end + 8..]);
        } else {
            // Unclosed <think> tag — strip from tag to end
            result = result[..start].to_owned();
            break;
        }
    }
    result.trim().to_owned()
}

// ── Builder-style construction ──────────────────────────────────────────────

impl ChatCompletionsApiCompatibleProvider {
    /// Create a new provider with the required fields.
    pub fn new(
        name: impl Into<String>,
        base_url: impl Into<String>,
        credential: impl Into<String>,
        auth_style: AuthStyle,
    ) -> Self {
        let base_url = base_url.into();
        let timeout_policy = ProviderTimeoutPolicy::default();
        Self {
            name: name.into(),
            base_url,
            credential: credential.into(),
            auth_style,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
            merge_system_into_user: false,
            replay_reasoning_content: true,
            timeout_policy,
            extra_headers: HashMap::new(),
            client: crate::http::build_client(timeout_policy),
        }
    }

    /// Enable or disable vision support.
    pub fn with_vision(mut self, supports_vision: bool) -> Self {
        self.input_types = if supports_vision {
            ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::disabled(),
                image: InputTypeSupport::data_url_inline_only(),
                audio: InputTypeSupport::disabled(),
                video: InputTypeSupport::disabled(),
            }
        } else {
            ProviderInputCapabilities::disabled_for_all_file_types()
        };
        self
    }

    pub fn with_input_capabilities(mut self, input_types: ProviderInputCapabilities) -> Self {
        self.input_types = input_types;
        self
    }

    /// When enabled, prepend system message content into the first user message.
    /// Useful for providers that do not support the `system` role.
    pub fn with_merge_system_into_user(mut self, merge: bool) -> Self {
        self.merge_system_into_user = merge;
        self
    }

    /// Include prior assistant `reasoning_content` in replayed chat history.
    pub fn with_reasoning_content_replay(mut self, replay: bool) -> Self {
        self.replay_reasoning_content = replay;
        self
    }

    /// Override the default request timeout.
    pub fn with_timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_policy.non_stream_request_timeout =
            std::time::Duration::from_secs(secs.max(1));
        self.client = crate::http::build_client(self.timeout_policy);
        self
    }

    pub fn with_timeout_policy(mut self, timeout_policy: ProviderTimeoutPolicy) -> Self {
        self.timeout_policy = timeout_policy;
        self.client = crate::http::build_client(timeout_policy);
        self
    }

    /// Add extra headers sent with every request.
    pub fn with_extra_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.extra_headers = headers;
        self
    }

    /// Set a custom User-Agent header.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.extra_headers
            .insert("User-Agent".to_string(), user_agent.into());
        self
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Resolve the chat completions endpoint URL.
    /// If the base_url already ends with `/chat/completions`, use it as-is.
    fn chat_completions_url(&self) -> String {
        let base = self.base_url.trim_end_matches('/');
        if base.ends_with("/chat/completions") {
            base.to_owned()
        } else {
            format!("{base}/chat/completions")
        }
    }

    /// Resolve the models endpoint URL.
    fn models_url(&self) -> String {
        Self::models_url_for_base(&self.base_url)
    }

    fn models_url_for_base(base: &str) -> String {
        let base = base.trim_end_matches('/');
        // Strip /chat/completions if the base_url points directly there
        let base = base.strip_suffix("/chat/completions").unwrap_or(base);
        let base = base.trim_end_matches('/');
        format!("{base}/models")
    }

    fn normalize_models(&self, api_response: ModelsListResponse) -> Vec<ProviderModelInfo> {
        let provider_name = self.name.clone();
        // These stock endpoints document a chat/text-only default list. A
        // custom URL using the same adapter name cannot inherit that fact.
        let chat_only_list = matches!(provider_name.as_str(), "huggingface" | "venice")
            && crate::provider_definition(&provider_name)
                .and_then(|definition| definition.default_base_url)
                .is_some_and(|stock| Self::models_url_for_base(stock) == self.models_url());
        let entries = match api_response {
            ModelsListResponse::Wrapped { data } => data,
            ModelsListResponse::Flat(data) => data,
        };

        entries
            .into_iter()
            .map(|m| ProviderModelInfo {
                id: m.id.clone(),
                name: None,
                description: None,
                created: m.created,
                provider: provider_name.clone(),
                owned_by: m.owned_by,
                limits: ProviderModelLimits::default(),
                capabilities: ProviderModelCapabilities {
                    // These are documented operation facts, not shared defaults
                    // for all APIs that happen to use the OpenAI list envelope.
                    chat: match provider_name.as_str() {
                        "mistral" => m.capabilities["completion_chat"].as_bool(),
                        "together" => match m.model_type.as_str() {
                            Some("chat") => Some(true),
                            Some(
                                "language" | "code" | "image" | "embedding" | "moderation"
                                | "rerank" | "audio" | "transcribe" | "video",
                            ) => Some(false),
                            _ => None,
                        },
                        "venice" => match m.model_type.as_str() {
                            Some("text") => Some(true),
                            Some(
                                "asr" | "decision" | "embedding" | "image" | "music" | "tts"
                                | "upscale" | "inpaint" | "video",
                            ) => Some(false),
                            None if m.model_type.is_null() => chat_only_list.then_some(true),
                            _ => None,
                        },
                        "huggingface" => chat_only_list.then_some(true),
                        _ => None,
                    },
                    ..Default::default()
                },
                transcription: None,
                pricing: None,
                active: Some(true),
                family: None,
                lifecycle_status: None,
            })
            .collect()
    }
    /// Build a GET request builder with the correct auth and extra headers applied.
    fn authorized_get(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.client.get(url);

        match &self.auth_style {
            AuthStyle::Bearer if self.credential.is_empty() => {}
            AuthStyle::Bearer => {
                builder = builder.header("Authorization", format!("Bearer {}", self.credential));
            }
            AuthStyle::XApiKey => {
                builder = builder.header("x-api-key", &self.credential);
            }
            AuthStyle::Custom(header_name) => {
                builder = builder.header(header_name, &self.credential);
            }
        }

        for (key, value) in &self.extra_headers {
            builder = builder.header(key, value);
        }

        builder
    }

    /// Build a request builder with the correct auth and extra headers applied.
    fn authorized_post(&self, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.client.post(url);

        match &self.auth_style {
            AuthStyle::Bearer if self.credential.is_empty() => {}
            AuthStyle::Bearer => {
                builder = builder.header("Authorization", format!("Bearer {}", self.credential));
            }
            AuthStyle::XApiKey => {
                builder = builder.header("x-api-key", &self.credential);
            }
            AuthStyle::Custom(header_name) => {
                builder = builder.header(header_name, &self.credential);
            }
        }

        for (key, value) in &self.extra_headers {
            builder = builder.header(key, value);
        }

        builder
    }

    // These controls belong to the documented native Chat endpoints. A
    // compatible wire format alone does not authorize vendor extensions.
    fn native_cache_affinity(&self, request: &ChatRequest) -> Option<String> {
        if matches!(
            self.name.as_str(),
            "together" | "venice" | "cerebras" | "deepinfra" | "baseten" | "litellm"
        ) {
            if self.name != "litellm" && !self.documented_chat_route() {
                return None;
            }
            let authority = self.cache_authority();
            return if self.name == "baseten" {
                crate::prompt_cache::Comparison::task_affinity_key(&authority)
            } else {
                crate::prompt_cache::Comparison::begin(&authority)?.affinity_key()
            };
        }
        if !matches!(self.name.as_str(), "mistral" | "xai" | "fireworks")
            || crate::definition::provider_definition(&self.name)?.default_base_url?
                != self.base_url.trim_end_matches('/')
        {
            return None;
        }
        let origin = request.messages.iter().rev().find_map(|message| {
            (message.role == Role::User)
                .then_some(message.provenance.as_ref())
                .flatten()
        })?;
        let owner = origin
            .context_thread
            .as_deref()
            .unwrap_or(&origin.thread_id);
        if origin.workspace_id.is_empty() || owner.is_empty() {
            return None;
        }
        use sha2::Digest;
        let identity = serde_json::to_vec(&(
            "pioneer-native-chat-affinity-v1",
            &self.name,
            &origin.workspace_id,
            owner,
        ))
        .ok()?;
        Some(hex::encode(sha2::Sha256::digest(identity)))
    }

    fn documented_chat_route(&self) -> bool {
        crate::provider_definition(&self.name)
            .and_then(|d| d.default_base_url)
            .is_some_and(|base| Self::models_url_for_base(base) == self.models_url())
    }

    // The native Chat route supplies the unknown/default contract, but cannot
    // override a per-model prohibition on this exact body field. Native facts
    // are consumed only if already available in the existing authority scope.
    fn prompt_cache_key_admitted(
        &self,
        model: &str,
        catalog: Option<&crate::catalog::ModelCatalog>,
    ) -> bool {
        let native = crate::generation::native_model(&self.name, model);
        let source = crate::generation::uses_public_catalog(&self.name)
            .then(|| catalog.and_then(|catalog| catalog.model(&self.name, model)))
            .flatten();
        let supported = native
            .as_ref()
            .and_then(|entry| entry.metadata.get("compat"))
            .and_then(|compat| compat.get("supportsPromptCacheKey"))
            .and_then(serde_json::Value::as_bool)
            .or_else(|| {
                source
                    .and_then(|entry| entry.metadata.get("compat"))
                    .and_then(|compat| compat.get("supportsPromptCacheKey"))
                    .and_then(serde_json::Value::as_bool)
            });
        supported != Some(false)
    }

    fn cache_authority(&self) -> String {
        use sha2::Digest;
        // The registry authority includes the connection and credential generation.
        // The direct adapter path also separates endpoints/auth without emitting secrets.
        hex::encode(sha2::Sha256::digest(
            serde_json::to_vec(&(
                "pioneer-chat-routing-authority-v1",
                &self.name,
                &self.base_url,
                format!("{:?}", self.auth_style),
                &self.credential,
                crate::attachments::runtime::current_authority_fingerprint().ok(),
                self.extra_headers
                    .iter()
                    .collect::<std::collections::BTreeMap<_, _>>(),
            ))
            .expect("routing identity"),
        ))
    }

    fn cache_header_usage(
        &self,
        headers: &reqwest::header::HeaderMap,
    ) -> Option<crate::TokenUsage> {
        if self.name != "fireworks" || !self.documented_chat_route() {
            return None;
        }
        let count = |name| headers.get(name)?.to_str().ok()?.parse::<u64>().ok();
        let input = count("fireworks-prompt-tokens");
        let read = count("fireworks-cached-prompt-tokens");
        if input.is_none() && read.is_none() {
            return None;
        }
        let value = serde_json::json!({"prompt_tokens": input,
            "prompt_tokens_details":{"cached_tokens":read}});
        let mut usage = serde_json::from_value::<ApiUsage>(value).ok()?.normalized();
        usage.prompt_cache_diagnostics = Some(serde_json::json!({"fireworks_cache_headers": {
            "prompt_tokens":input, "cached_tokens":read}}));
        Some(usage)
    }

    fn native_details_authority(&self) -> Option<String> {
        (self.name == "aihubmix" && self.documented_chat_route())
            .then(|| crate::continuation::replay_authority(&self.name, &[&self.models_url()]))
    }

    fn chat_post(&self, request: &ApiChatRequest) -> reqwest::RequestBuilder {
        let mut builder = self.authorized_post(&self.chat_completions_url());
        let affinity_header = match self.name.as_str() {
            "xai" => Some("x-grok-conv-id"),
            "fireworks" | "baseten" => Some("x-session-affinity"),
            "litellm" => Some("x-litellm-session-id"),
            _ => None,
        };
        if let Some(header) = affinity_header
            && !self
                .extra_headers
                .keys()
                .any(|key| key.eq_ignore_ascii_case(header))
            && let Some(key) = &request.cache_affinity
        {
            builder = builder.header(header, key);
        }
        builder.json(request)
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
                file_data: Some(BASE64.encode(attachment_bytes(attachment)?)),
                file_id: None,
                filename: Some(attachment.name.clone()),
            },
        };
        Ok(ApiContentPart::File { file })
    }

    /// Convert our domain messages to the API wire format, optionally merging
    /// the system message into the first user message.
    fn convert_messages(&self, prepared: &PreparedProviderMessages) -> Result<Vec<ApiMessage>> {
        if self.merge_system_into_user {
            self.merge_system_messages(prepared)
        } else {
            self.direct_convert_for_provider(prepared)
        }
    }

    fn direct_convert(prepared: &PreparedProviderMessages) -> Result<Vec<ApiMessage>> {
        Self::direct_convert_with_options(prepared, false, "openai_compatible")
    }

    fn direct_convert_for_provider(
        &self,
        prepared: &PreparedProviderMessages,
    ) -> Result<Vec<ApiMessage>> {
        if self.replay_reasoning_content || self.name == "aihubmix" {
            Self::direct_convert_with_options(
                prepared,
                self.replay_reasoning_content,
                self.name.as_str(),
            )
        } else {
            Self::direct_convert(prepared)
        }
    }

    fn direct_convert_with_options(
        prepared: &PreparedProviderMessages,
        replay_reasoning_content: bool,
        provider_name: &str,
    ) -> Result<Vec<ApiMessage>> {
        let mut rendered = Vec::new();
        for (message_index, message) in prepared.messages.iter().enumerate() {
            let attachments = prepared
                .attachments_for_message(message_index)
                .collect::<Vec<_>>();
            if message.role == Role::Tool && !attachments.is_empty() {
                let tool_text = if message.content.is_empty() {
                    "{}".to_owned()
                } else {
                    message.content.clone()
                };
                rendered.push(Self::build_api_message(
                    "tool",
                    message,
                    &[],
                    Some(tool_text),
                    provider_name,
                    replay_reasoning_content,
                )?);
                rendered.push(Self::build_tool_context_message(
                    attachments.as_slice(),
                    provider_name,
                )?);
                continue;
            }

            rendered.push(Self::build_api_message(
                match message.role {
                    Role::System => "system",
                    Role::User => "user",
                    Role::Assistant => "assistant",
                    Role::Tool => "tool",
                },
                message,
                attachments.as_slice(),
                None,
                provider_name,
                replay_reasoning_content,
            )?);
        }
        Ok(rendered)
    }

    fn build_parts(
        text: &str,
        attachments: &[&crate::attachments::PreparedAttachment],
        include_text: bool,
        provider_name: &str,
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
                InputContentType::Text => {
                    return Err(anyhow!(
                        "provider `{provider_name}` received unexpected text attachment part"
                    ));
                }
            }
        }

        Ok(parts)
    }

    fn build_tool_context_message(
        attachments: &[&crate::attachments::PreparedAttachment],
        provider_name: &str,
    ) -> Result<ApiMessage> {
        let parts = Self::build_parts(
            "Analyze the attached tool media context and continue.",
            attachments,
            true,
            provider_name,
        )?;
        Ok(ApiMessage {
            role: "user".to_owned(),
            content: Some(ApiMessageContent::Parts(parts)),
            reasoning_content: None,
            reasoning_details: None,
            tool_calls: None,
            tool_call_id: None,
            name: None,
        })
    }

    fn build_content(
        message: &ChatMessage,
        attachments: &[&crate::attachments::PreparedAttachment],
        override_text: Option<String>,
        provider_name: &str,
    ) -> Result<Option<ApiMessageContent>> {
        if attachments.is_empty() {
            let text = override_text.unwrap_or_else(|| message.content.clone());
            if text.is_empty() && message.tool_calls.is_some() {
                return Ok(None);
            }
            return Ok(Some(ApiMessageContent::Text(text)));
        }

        let text = override_text.unwrap_or_else(|| message.content.clone());
        let parts = Self::build_parts(text.as_str(), attachments, true, provider_name)?;
        Ok(Some(ApiMessageContent::Parts(parts)))
    }

    fn build_api_message(
        role: &str,
        message: &ChatMessage,
        attachments: &[&crate::attachments::PreparedAttachment],
        override_text: Option<String>,
        provider_name: &str,
        replay_reasoning_content: bool,
    ) -> Result<ApiMessage> {
        if message.role == Role::Assistant
            && let Some(replay) = Self::decode_assistant_replay_state(message, provider_name)?
        {
            return Ok(ApiMessage {
                role: role.to_owned(),
                content: replay.content.map(ApiMessageContent::Text),
                reasoning_content: replay.reasoning_content,
                reasoning_details: replay.reasoning_details,
                tool_calls: (!replay.tool_calls.is_empty()).then_some(replay.tool_calls),
                tool_call_id: None,
                name: None,
            });
        }

        Ok(ApiMessage {
            role: role.to_owned(),
            content: Self::build_content(message, attachments, override_text, provider_name)?,
            reasoning_content: if replay_reasoning_content && message.role == Role::Assistant {
                message.reasoning_content.clone()
            } else {
                None
            },
            reasoning_details: None,
            tool_calls: message.tool_calls.as_deref().map(Self::convert_tool_calls),
            tool_call_id: message.tool_call_id.clone(),
            name: message.name.clone(),
        })
    }

    /// Merge system messages: collect all leading system messages and prepend
    /// their content to the first user message.
    fn merge_system_messages(
        &self,
        prepared: &PreparedProviderMessages,
    ) -> Result<Vec<ApiMessage>> {
        let messages = &prepared.messages;
        let mut system_parts: Vec<&str> = Vec::new();
        let mut rest_start = 0;

        for (i, msg) in messages.iter().enumerate() {
            if msg.role == Role::System {
                system_parts.push(&msg.content);
                rest_start = i + 1;
            } else {
                break;
            }
        }

        let system_prefix = system_parts.join("\n");
        let mut result = Vec::with_capacity(messages.len());

        let mut system_prepended = false;
        for (offset, msg) in messages[rest_start..].iter().enumerate() {
            let message_index = rest_start + offset;
            let attachments = prepared
                .attachments_for_message(message_index)
                .collect::<Vec<_>>();
            if msg.role == Role::Tool && !attachments.is_empty() {
                let tool_text = if msg.content.is_empty() {
                    "{}".to_owned()
                } else {
                    msg.content.clone()
                };
                result.push(Self::build_api_message(
                    "tool",
                    msg,
                    &[],
                    Some(tool_text),
                    self.name.as_str(),
                    self.replay_reasoning_content,
                )?);
                result.push(Self::build_tool_context_message(
                    attachments.as_slice(),
                    self.name.as_str(),
                )?);
                continue;
            }

            if !system_prepended && msg.role == Role::User && !system_prefix.is_empty() {
                let merged_text = if msg.content.is_empty() {
                    system_prefix.clone()
                } else {
                    format!("{system_prefix}\n\n{}", msg.content)
                };
                result.push(Self::build_api_message(
                    "user",
                    msg,
                    attachments.as_slice(),
                    Some(merged_text),
                    self.name.as_str(),
                    self.replay_reasoning_content,
                )?);
                system_prepended = true;
            } else {
                result.push(Self::build_api_message(
                    match msg.role {
                        Role::System => "user", // demote any later system messages too
                        Role::User => "user",
                        Role::Assistant => "assistant",
                        Role::Tool => "tool",
                    },
                    msg,
                    attachments.as_slice(),
                    None,
                    self.name.as_str(),
                    self.replay_reasoning_content,
                )?);
            }
        }

        // Edge case: only system messages, no user message to merge into.
        if !system_prefix.is_empty() && !system_prepended {
            result.insert(
                0,
                ApiMessage {
                    role: "user".into(),
                    content: Some(ApiMessageContent::Text(system_prefix)),
                    reasoning_content: None,
                    reasoning_details: None,
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                },
            );
        }

        Ok(result)
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

    fn convert_tool_calls(tool_calls: &[crate::types::ProviderToolCall]) -> Vec<ApiToolCall> {
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
    }

    pub(super) fn assistant_replay_state(
        provider_name: &str,
        content: Option<String>,
        reasoning_content: Option<String>,
        tool_calls: &[crate::types::ProviderToolCall],
    ) -> ProviderReplayState {
        ProviderReplayState::new(
            provider_name,
            serde_json::to_value(CompatibleAssistantReplayState {
                schema_version: COMPATIBLE_REPLAY_STATE_SCHEMA_VERSION,
                assistant_message: CompatibleAssistantReplayMessage {
                    content,
                    reasoning_content,
                    reasoning_details: None,
                    tool_calls: Self::convert_tool_calls(tool_calls),
                },
            })
            .expect("compatible assistant replay state must serialize"),
        )
    }

    fn decode_assistant_replay_state(
        message: &ChatMessage,
        provider_name: &str,
    ) -> Result<Option<CompatibleAssistantReplayMessage>> {
        let Some(state) = message.provider_replay_state.as_ref() else {
            return Ok(None);
        };
        Self::decode_replay_state(state, provider_name).map(Some)
    }

    pub(super) fn decode_replay_state(
        state: &ProviderReplayState,
        provider_name: &str,
    ) -> Result<CompatibleAssistantReplayMessage> {
        let payload = state.payload_for(provider_name).ok_or_else(|| {
            anyhow!(
                "provider replay state `{}` cannot be rendered by `{provider_name}`",
                state.provider
            )
        })?;
        anyhow::ensure!(
            crate::continuation::retention(state) != crate::continuation::Retention::Unsupported,
            "unsupported {provider_name} continuation: unrecognized native state must not be discarded by the readable replay schema"
        );
        let replay = serde_json::from_value::<CompatibleAssistantReplayState>(payload.clone())
            .map_err(|error| anyhow!("invalid {provider_name} replay state: {error}"))?;
        if replay.schema_version != COMPATIBLE_REPLAY_STATE_SCHEMA_VERSION {
            return Err(anyhow!(
                "unsupported {provider_name} replay state schema version {}",
                replay.schema_version
            ));
        }
        Ok(replay.assistant_message)
    }

    pub(super) fn replay_message(
        &self,
        message: &ChatMessage,
    ) -> Result<Option<CompatibleAssistantReplayMessage>> {
        Self::decode_assistant_replay_state(message, self.name.as_str())
    }

    #[cfg(test)]
    fn replay_state_message(
        &self,
        state: &ProviderReplayState,
    ) -> Result<CompatibleAssistantReplayMessage> {
        Self::decode_replay_state(state, self.name.as_str())
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

    async fn build_chat_request_async(
        &self,
        request: ChatRequest,
        stream: bool,
    ) -> Result<ApiChatRequest> {
        let request = crate::tools::policy::prepare_request(self.name.as_str(), request)?;
        let capabilities =
            <ChatCompletionsApiCompatibleProvider as crate::traits::Provider>::capabilities(self);
        let prepared = prepare_messages_for_request_async(
            self.name.as_str(),
            &capabilities,
            &request,
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )
        .await?;
        let has_image = prepared
            .attachments
            .iter()
            .any(|a| a.kind == InputContentType::Image);
        let body = self.build_chat_request_from_prepared(request, stream, prepared)?;
        if self.name == "groq" && has_image {
            crate::attachments::validate_inline_payload("groq", &body)?;
        }
        Ok(body)
    }

    #[cfg(test)]
    fn build_chat_request(&self, request: ChatRequest, stream: bool) -> Result<ApiChatRequest> {
        let request = crate::tools::policy::prepare_request(self.name.as_str(), request)?;
        let capabilities =
            <ChatCompletionsApiCompatibleProvider as crate::traits::Provider>::capabilities(self);
        let prepared = prepare_messages_for_request(
            self.name.as_str(),
            &capabilities,
            &request,
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )?;
        self.build_chat_request_from_prepared(request, stream, prepared)
    }

    #[cfg(test)]
    pub(super) fn render_chat_request_for_test(
        &self,
        request: ChatRequest,
    ) -> Result<serde_json::Value> {
        self.render_chat_request_mode_for_test(request, false)
    }

    #[cfg(test)]
    pub(super) fn render_chat_request_mode_for_test(
        &self,
        request: ChatRequest,
        stream: bool,
    ) -> Result<serde_json::Value> {
        serde_json::to_value(self.build_chat_request(request, stream)?).map_err(Into::into)
    }

    /// Uses production's asynchronous model/request-aware materialization and
    /// shared wire constructor. No HTTP/provider endpoint is involved.
    #[cfg(test)]
    pub(crate) async fn render_chat_request_async_for_test(
        &self,
        request: ChatRequest,
        stream: bool,
    ) -> Result<serde_json::Value> {
        serde_json::to_value(self.build_chat_request_async(request, stream).await?)
            .map_err(Into::into)
    }

    fn build_chat_request_from_prepared(
        &self,
        request: ChatRequest,
        stream: bool,
        prepared: PreparedProviderMessages,
    ) -> Result<ApiChatRequest> {
        let catalog = crate::catalog::model_catalog().ok();
        self.build_chat_request_with_catalog(request, stream, prepared, catalog.as_deref())
    }

    fn build_chat_request_with_catalog(
        &self,
        request: ChatRequest,
        stream: bool,
        mut prepared: PreparedProviderMessages,
        catalog: Option<&crate::catalog::ModelCatalog>,
    ) -> Result<ApiChatRequest> {
        let request = crate::tools::policy::prepare_request(self.name.as_str(), request)?;
        for state in prepared
            .messages
            .iter()
            .filter_map(|m| m.provider_replay_state.as_ref())
        {
            if state.provider == "aihubmix"
                && state.payload["assistant_message"]
                    .get("reasoning_details")
                    .is_some()
            {
                anyhow::ensure!(
                    self.native_details_authority().as_deref()
                        == state.payload["authority_scope"].as_str(),
                    "AIHubMix native replay endpoint authority changed"
                );
            }
        }
        crate::tools::policy::prepare_history(self.name.as_str(), &mut prepared.messages)?;
        ensure_no_unrendered_attachments(self.name.as_str(), &prepared)?;
        let mut generation =
            crate::generation::chat_fields_from_catalog(catalog, &self.name, &request)?;
        let cache_affinity = self.native_cache_affinity(&request);
        if matches!(
            self.name.as_str(),
            "mistral" | "together" | "venice" | "cerebras" | "deepinfra"
        ) && (self.name == "mistral" || self.prompt_cache_key_admitted(&request.model, catalog))
            && let Some(key) = &cache_affinity
        {
            generation.insert("prompt_cache_key".into(), serde_json::json!(key));
        }
        Ok(ApiChatRequest {
            cache_affinity,
            stream_options: stream
                .then(|| crate::usage::stream_options(&self.name, &request.model, &self.base_url))
                .flatten(),
            generation,
            model: request.model,
            messages: self.convert_messages(&prepared)?,
            temperature: None,
            max_tokens: None,
            tools: request
                .tools
                .as_ref()
                .map(|tools| Self::convert_tools(tools)),
            tool_choice: request.tool_choice.map(Self::convert_tool_choice),
            parallel_tool_calls: request.parallel_tool_calls,
            stream,
        })
    }

    fn response_from_api(
        &self,
        api_response: ApiChatResponse,
        header_usage: Option<crate::TokenUsage>,
    ) -> Result<ChatResponse> {
        let mut combined_usage = header_usage.unwrap_or_default();
        combined_usage.update(
            &api_response
                .usage
                .map(|u| u.normalized())
                .unwrap_or_default()
                .with_native_id(api_response.id.as_deref())
                .with_reported_model(api_response.model.as_deref())
                .with_request_id(if self.name == "groq" {
                    api_response
                        .x_groq
                        .as_ref()
                        .and_then(|m| m.get("id"))
                        .and_then(serde_json::Value::as_str)
                } else {
                    None
                }),
        );
        let usage = Some(combined_usage.clone());
        (|| {
            let choice = api_response
                .choices
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("no response from {}", self.name))?;
            let termination = choice
                .finish_reason
                .as_deref()
                .map(ProviderTermination::from_openai_reason)
                .unwrap_or_else(|| {
                    ProviderTermination::Unknown("missing_finish_reason".to_owned())
                });
            let message = choice.message;

            let raw_content = message.content.clone();
            let text = message.effective_content();
            let tool_calls =
                parse_tool_calls(message.tool_calls.as_ref(), message.function_call.as_ref())?;
            let reasoning_content = message.reasoning_content.or(message.reasoning);

            let details_authority = self.native_details_authority();
            let native_details = details_authority.as_ref().and(message.reasoning_details);
            if text.is_empty()
                && tool_calls.is_empty()
                && reasoning_content.as_deref().unwrap_or_default().is_empty()
                && native_details.is_none()
            {
                return Err(anyhow!("no response from {}", self.name));
            }

            let provider_replay_state = (native_details.is_some()
                || self.replay_reasoning_content
                    && (self.name == "deepseek" || !tool_calls.is_empty()))
            .then(|| {
                let mut state = Self::assistant_replay_state(
                    self.name.as_str(),
                    raw_content,
                    reasoning_content.clone(),
                    tool_calls.as_slice(),
                );
                if let Some(details) = native_details {
                    state.payload["assistant_message"]["reasoning_details"] = details;
                    state.payload["authority_scope"] = serde_json::json!(details_authority);
                }
                state
            });

            if let Some(state) = &provider_replay_state {
                crate::ProviderResponseLimits::default().validate_replay_state(state)?;
            }
            Ok(ChatResponse {
                messages: Vec::new(),
                text,
                usage,
                termination,
                reasoning_content,
                tool_calls,
                provider_replay_state,
            })
        })()
        .map_err(|error| crate::usage::with_error_usage(error, &combined_usage))
    }

    async fn api_error(&self, response: reqwest::Response) -> anyhow::Error {
        crate::http::provider_api_error(response, &self.name).await
    }
}

#[cfg(test)]
fn finish_compatible_stream(
    tool_call_accumulator: &mut StreamToolCallAccumulator,
    provider_name: &str,
    replay_reasoning_content: bool,
    content: Option<String>,
    reasoning_content: Option<String>,
    termination: ProviderTermination,
) -> Result<Vec<StreamChunk>> {
    finish_compatible_stream_with_details(
        tool_call_accumulator,
        provider_name,
        replay_reasoning_content,
        content,
        reasoning_content,
        None,
        None,
        termination,
    )
}

fn finish_compatible_stream_with_details(
    tool_call_accumulator: &mut StreamToolCallAccumulator,
    provider_name: &str,
    replay_reasoning_content: bool,
    content: Option<String>,
    reasoning_content: Option<String>,
    reasoning_details: Option<serde_json::Value>,
    authority: Option<&str>,
    termination: ProviderTermination,
) -> Result<Vec<StreamChunk>> {
    let tool_calls = tool_call_accumulator.take_tool_calls()?;
    let mut chunks = Vec::new();
    if reasoning_details.is_some()
        || replay_reasoning_content && (provider_name == "deepseek" || !tool_calls.is_empty())
    {
        let mut state = ChatCompletionsApiCompatibleProvider::assistant_replay_state(
            provider_name,
            content,
            reasoning_content,
            tool_calls.as_slice(),
        );
        if let Some(details) = reasoning_details {
            state.payload["assistant_message"]["reasoning_details"] = details;
            state.payload["authority_scope"] = serde_json::json!(authority);
        }
        crate::ProviderResponseLimits::default().validate_replay_state(&state)?;
        chunks.push(StreamChunk::provider_replay_state(state));
    }
    if !tool_calls.is_empty() {
        chunks.push(StreamChunk::tool_calls(tool_calls));
    }
    chunks.push(StreamChunk::final_chunk_with(termination));
    Ok(chunks)
}

// AIHubMix's Claude Chat example concatenates string members (including
// signatures), except `type`; other non-null members are complete values.
fn merge_aihubmix_reasoning_details(
    accumulated: &mut Option<serde_json::Value>,
    delta: serde_json::Value,
) -> Result<()> {
    let fields = delta
        .as_object()
        .ok_or_else(|| anyhow!("invalid AIHubMix reasoning_details delta"))?;
    let target = accumulated
        .get_or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .unwrap();
    for (key, value) in fields {
        if key == "type" {
            target.insert(key.clone(), value.clone());
        } else if let Some(text) = value.as_str() {
            let entry = target
                .entry(key.clone())
                .or_insert_with(|| serde_json::json!(""));
            let serde_json::Value::String(previous) = entry else {
                return Err(anyhow!("conflicting AIHubMix reasoning_details field"));
            };
            previous.push_str(text);
        } else if !value.is_null() {
            target.insert(key.clone(), value.clone());
        }
    }
    anyhow::ensure!(
        serde_json::to_vec(accumulated)?.len()
            <= crate::ProviderResponseLimits::default().max_replay_bytes,
        "AIHubMix reasoning_details limit"
    );
    Ok(())
}

// ── Provider trait implementation ───────────────────────────────────────────

// The same decoder is used by HTTP transport and in-memory regression fixtures.
impl ChatCompletionsApiCompatibleProvider {
    #[cfg(test)]
    pub(super) fn decode_stream(
        byte_stream: BoxStream<'static, Result<bytes::Bytes>>,
        provider_name: String,
        replay_reasoning_content: bool,
    ) -> BoxStream<'static, Result<StreamChunk>> {
        Self::decode_stream_with_context(
            byte_stream,
            provider_name,
            replay_reasoning_content,
            None,
            None,
        )
    }

    fn decode_stream_with_context(
        byte_stream: BoxStream<'static, Result<bytes::Bytes>>,
        provider_name: String,
        replay_reasoning_content: bool,
        details_authority: Option<String>,
        header_usage: Option<crate::TokenUsage>,
    ) -> BoxStream<'static, Result<StreamChunk>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);

        tokio::spawn(async move {
            let mut decoder = IncrementalSseDecoder::default();
            let mut tool_call_accumulator = StreamToolCallAccumulator::default();
            let mut terminal_reason = None;
            let mut response_content: Option<String> = None;
            let mut response_reasoning_content: Option<String> = None;
            let mut response_reasoning_details: Option<serde_json::Value> = None;
            let mut accumulated_usage = header_usage.clone().unwrap_or_default();
            if let Some(usage) = header_usage {
                if tx.send(Ok(StreamChunk::usage(usage))).await.is_err() {
                    return;
                }
            }

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
                for frame in lines {
                    let data = frame.data.as_str();

                    if data.trim() == "[DONE]" {
                        let terminal = terminal_reason
                            .take()
                            .map(StreamChunk::final_chunk_with)
                            .ok_or_else(|| {
                                crate::failure::ProviderStreamIncomplete::DoneWithoutFinishReason
                                    .into()
                            });
                        let _ = tx.send(terminal).await;
                        return;
                    }

                    match serde_json::from_str::<StreamResponse>(data) {
                        Ok(resp) => {
                            if terminal_reason.is_some()
                                && resp.choices.iter().any(|choice| {
                                    choice.delta.has_payload(details_authority.is_some())
                                })
                            {
                                let _ = tx
                                    .send(Err(anyhow!("provider sent payload after finish_reason")))
                                    .await;
                                return;
                            }
                            if resp.usage.is_some() || resp.id.is_some() {
                                let usage = resp
                                    .usage
                                    .map(|u| u.normalized())
                                    .unwrap_or_default()
                                    .with_native_id(resp.id.as_deref())
                                    .with_reported_model(resp.model.as_deref());
                                accumulated_usage.update(&usage);
                                if tx
                                    .send(Ok(StreamChunk::usage(accumulated_usage.clone())))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            if let Some(error) = resp.error {
                                let status = error
                                    .code
                                    .as_ref()
                                    .and_then(serde_json::Value::as_u64)
                                    .and_then(|code| u16::try_from(code).ok());
                                let outcome = crate::failure::native_chat_stream_error(
                                    &error.description(),
                                    status,
                                );
                                let _ = tx.send(Err(outcome)).await;
                                return;
                            }
                            for choice in resp.choices {
                                if terminal_reason.is_some() {
                                    if choice.finish_reason.as_deref().is_some_and(|reason| {
                                        Some(ProviderTermination::from_openai_reason(reason))
                                            != terminal_reason
                                    }) {
                                        let _ = tx
                                            .send(Err(anyhow!(
                                                "provider changed finish_reason after completion"
                                            )))
                                            .await;
                                        return;
                                    }
                                    if choice.delta.has_payload(details_authority.is_some()) {
                                        let _ = tx
                                            .send(Err(anyhow!(
                                                "provider sent payload after finish_reason"
                                            )))
                                            .await;
                                        return;
                                    }
                                    continue;
                                }
                                if details_authority.is_some()
                                    && let Some(delta) = choice.delta.reasoning_details
                                {
                                    if let Err(error) = merge_aihubmix_reasoning_details(
                                        &mut response_reasoning_details,
                                        delta,
                                    ) {
                                        let _ = tx.send(Err(error)).await;
                                        return;
                                    }
                                }
                                if let Some(rc) =
                                    choice.delta.reasoning_content.or(choice.delta.reasoning)
                                {
                                    response_reasoning_content
                                        .get_or_insert_with(String::new)
                                        .push_str(rc.as_str());
                                    if !rc.is_empty() {
                                        if tx.send(Ok(StreamChunk::reasoning(rc))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                if let Some(content) = choice.delta.content {
                                    response_content
                                        .get_or_insert_with(String::new)
                                        .push_str(content.as_str());
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
                                    let chunks = match finish_compatible_stream_with_details(
                                        &mut tool_call_accumulator,
                                        provider_name.as_str(),
                                        replay_reasoning_content,
                                        response_content.take(),
                                        response_reasoning_content.take(),
                                        response_reasoning_details.take(),
                                        details_authority.as_deref(),
                                        ProviderTermination::from_openai_reason(&reason),
                                    ) {
                                        Ok(chunks) => chunks,
                                        Err(error) => {
                                            if tx.send(Err(error)).await.is_err() {
                                                return;
                                            }
                                            return;
                                        }
                                    };
                                    for chunk in chunks {
                                        if chunk.is_final {
                                            terminal_reason = chunk.termination;
                                        } else if tx.send(Ok(chunk)).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            if tx
                                .send(Err(anyhow!("malformed {} SSE frame", provider_name)))
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
                Ok(_) => {
                    Err(crate::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker.into())
                }
            };
            let _ = tx.send(terminal).await;
        });

        let chunk_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Box::pin(chunk_stream)
    }
}

#[async_trait]
impl crate::traits::Provider for ChatCompletionsApiCompatibleProvider {
    fn usage_api(&self) -> &'static str {
        "chat_completions"
    }
    fn usage_route(&self) -> Option<String> {
        let root = self.models_url();
        crate::usage::route(
            root.strip_suffix("/models").unwrap_or(&root),
            "/chat/completions",
        )
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: self.input_types.image.is_supported(),
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: self.input_types.clone(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let api_request = self.build_chat_request_async(request, false).await?;

        let request_builder = self.chat_post(&api_request);
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        let header_usage = self.cache_header_usage(response.headers());
        if !response.status().is_success() {
            let error = self.api_error(response).await;
            return Err(if let Some(usage) = &header_usage {
                crate::usage::with_error_usage(error, usage)
            } else {
                error
            });
        }

        let api_response: ApiChatResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await
        .map_err(|error| {
            if let Some(usage) = &header_usage {
                crate::usage::with_error_usage(error, usage)
            } else {
                error
            }
        })?;
        self.response_from_api(api_response, header_usage)
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let api_request = self.build_chat_request_async(request, true).await?;

        let request_builder = self.chat_post(&api_request);
        let response =
            crate::http::send_stream_request(request_builder, self.timeout_policy).await?;

        let header_usage = self.cache_header_usage(response.headers());
        if !response.status().is_success() {
            let error = self.api_error(response).await;
            return Err(if let Some(usage) = &header_usage {
                crate::usage::with_error_usage(error, usage)
            } else {
                error
            });
        }

        let byte_stream = crate::http::bounded_response_stream(
            response,
            crate::types::ProviderResponseLimits::default().max_transport_bytes,
            "provider_stream",
        );
        let provider_name = self.name.clone();
        let replay_reasoning_content = self.replay_reasoning_content;

        Ok(Self::decode_stream_with_context(
            byte_stream,
            provider_name,
            replay_reasoning_content,
            self.native_details_authority(),
            header_usage,
        ))
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        let request_builder = self.authorized_get(&self.models_url());
        let response = crate::http::non_stream_request(request_builder, self.timeout_policy)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(self.api_error(response).await);
        }

        let api_response: ModelsListResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await?;
        Ok(self.normalize_models(api_response))
    }

    async fn warmup(&self) -> Result<crate::ProviderWarmupOutcome> {
        self.list_models().await?;
        Ok(crate::ProviderWarmupOutcome::Completed)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn stock_chat_discovery_accepts_equivalent_completion_urls_only() {
        use super::*;
        use crate::traits::Provider;
        for name in ["huggingface", "venice"] {
            let base = crate::provider_definition(name)
                .unwrap()
                .default_base_url
                .unwrap();
            for (url, expected) in [
                (base.to_owned(), true),
                (format!("{base}/chat/completions"), true),
                (format!("{base}/chat/completions/"), true),
                (format!("{base}/foreign"), false),
                ("https://foreign.example/v1".into(), false),
            ] {
                let provider =
                    ChatCompletionsApiCompatibleProvider::new(name, url, "", AuthStyle::Bearer);
                let response =
                    serde_json::from_str(r#"{"data":[{"id":"private-model"}]}"#).unwrap();
                let rows = provider.normalize_models(response);
                assert_eq!(rows[0].capabilities.chat, expected.then_some(true));
                let selected = crate::selection::filter_models(
                    name,
                    "chat_completions",
                    &provider.capabilities(),
                    crate::selection::ModelPurpose::Chat,
                    rows,
                    None,
                );
                assert_eq!(
                    selected
                        .iter()
                        .map(|row| row.id.as_str())
                        .collect::<Vec<_>>(),
                    if expected {
                        vec!["private-model"]
                    } else {
                        vec![]
                    }
                );
            }
        }
    }

    #[test]
    fn vendor_formats_cannot_bypass_explicit_negative_effort_capability() {
        for format in [
            "deepseek",
            "zai",
            "together",
            "qwen",
            "openai",
            "string-thinking",
            "openrouter",
        ] {
            let catalog = crate::generation::test_catalog_model(
                "nvidia",
                "negative-vendor-profile",
                "deepseek-ai/deepseek-v4-flash-0731",
                serde_json::json!({"reasoning":true,"api":"openai-completions","compat":{"thinkingFormat":format,"supportsReasoningEffort":false},"thinkingLevelMap":{"high":"high","max":"max"}}),
            );
            let provider = ChatCompletionsApiCompatibleProvider::new(
                "nvidia",
                "https://example.test/v1",
                "",
                AuthStyle::Bearer,
            );
            let mut request = crate::generation::test_request("negative-vendor-profile");
            request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
            for stream in [false, true] {
                let prepared = prepared_for(&provider, &request.messages);
                assert!(
                    provider
                        .build_chat_request_with_catalog(
                            request.clone(),
                            stream,
                            prepared,
                            Some(&catalog)
                        )
                        .unwrap_err()
                        .to_string()
                        .contains("explicitly does not support")
                );
            }
        }
    }

    #[test]
    fn nvidia_negative_effort_capability_matches_discovery_and_wire_even_with_maps() {
        for fresh in [false, true] {
            let catalog = crate::generation::test_catalog(fresh);
            let id = "deepseek-ai/deepseek-v4-flash-0731";
            let model = catalog.model("nvidia", id).unwrap();
            assert_eq!(model.metadata["compat"]["supportsReasoningEffort"], false);
            let discovered = crate::generation::test_discovery(&catalog, "nvidia", id);
            assert_eq!(
                discovered
                    .capabilities
                    .reasoning
                    .as_ref()
                    .unwrap()
                    .effort_options
                    .contains(&"none".into()),
                fresh
            );
            for effort in ["high", "max"] {
                assert!(
                    !discovered
                        .capabilities
                        .reasoning
                        .as_ref()
                        .unwrap()
                        .effort_options
                        .contains(&effort.into())
                );
            }
            let provider = ChatCompletionsApiCompatibleProvider::new(
                "nvidia",
                "https://example.test/v1",
                "",
                AuthStyle::Bearer,
            );
            for reasoning in [
                None,
                Some(ReasoningConfig::Disabled),
                Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                Some(ReasoningConfig::Effort(ReasoningEffort::High)),
                Some(ReasoningConfig::Effort(ReasoningEffort::Max)),
            ] {
                let mut request = crate::generation::test_request(id);
                request.reasoning = reasoning;
                for stream in [false, true] {
                    let prepared = prepared_for(&provider, &request.messages);
                    let result = provider.build_chat_request_with_catalog(
                        request.clone(),
                        stream,
                        prepared,
                        Some(&catalog),
                    );
                    // Fresh source metadata explicitly lists none. The saved
                    // snapshot has no verified off control; high/max stay denied
                    // by supportsReasoningEffort=false in both snapshots.
                    let off = crate::generation::selected_off(reasoning);
                    let supported = reasoning.is_none() || fresh && off;
                    assert_eq!(result.is_ok(), supported);
                    if supported {
                        let body = serde_json::to_value(result.unwrap()).unwrap();
                        assert!(body.get("reasoning_effort").is_none());
                        if off {
                            assert_eq!(body["thinking"]["type"], "disabled");
                        } else {
                            assert!(body.get("thinking").is_none());
                        }
                        assert_eq!(body["max_tokens"], 1024);
                        assert_eq!(body["stream"], stream);
                    } else {
                        let error = result.unwrap_err().to_string();
                        assert!(
                            error.contains(if crate::generation::selected_off(reasoning) {
                                "no confirmed mapping for selected reasoning"
                            } else {
                                "explicitly does not support"
                            })
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn native_deepseek_source_profiles_preserve_caps_and_selected_controls() {
        let provider = ChatCompletionsApiCompatibleProvider::new(
            "deepseek",
            "https://example.test/v1",
            "",
            AuthStyle::Bearer,
        );
        for fresh in [false, true] {
            let catalog = crate::generation::test_catalog(fresh);
            let ids: &[&str] = if fresh {
                &["deepseek-v4-flash", "deepseek-v4-pro", "deepseek-flash"]
            } else {
                &["deepseek-v4-flash", "deepseek-v4-pro"]
            };
            for &id in ids {
                let model = catalog.model("deepseek", id).unwrap();
                assert_ne!(model.metadata["compat"]["supportsReasoningEffort"], false);
                let discovered = crate::generation::test_discovery(&catalog, "deepseek", id);
                assert!(
                    discovered
                        .capabilities
                        .reasoning
                        .as_ref()
                        .unwrap()
                        .effort_options
                        .contains(&"high".into())
                );
                for reasoning in [
                    None,
                    Some(ReasoningConfig::Disabled),
                    Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                    Some(ReasoningConfig::Effort(ReasoningEffort::High)),
                ] {
                    let mut request = crate::generation::test_request(id);
                    request.reasoning = reasoning;
                    for stream in [false, true] {
                        let result = provider.build_chat_request_with_catalog(
                            request.clone(),
                            stream,
                            prepared_for(&provider, &request.messages),
                            Some(&catalog),
                        );
                        // The saved snapshot has neither an off mapping nor the
                        // fresh source's toggle metadata. A model name proves neither.
                        if !fresh && crate::generation::selected_off(reasoning) {
                            assert!(
                                result
                                    .unwrap_err()
                                    .to_string()
                                    .contains("no confirmed mapping for selected reasoning")
                            );
                            continue;
                        }
                        let body = serde_json::to_value(result.unwrap()).unwrap();
                        assert_eq!(body["max_tokens"], 1024);
                        assert_eq!(body["stream"], stream);
                        if reasoning.is_none() {
                            assert!(body.get("thinking").is_none());
                        } else if crate::generation::selected_off(reasoning) {
                            assert_eq!(body["thinking"]["type"], "disabled");
                            assert!(body.get("reasoning_effort").is_none());
                        } else {
                            assert_eq!(body["thinking"]["type"], "enabled");
                            assert_eq!(body["reasoning_effort"], "high");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn every_compatible_profile_preserves_prepared_cap_and_default_in_both_bodies() {
        let profiles: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/generation-profiles.json"
        ))
        .unwrap();
        for profile in profiles.as_array().unwrap() {
            let provider = ChatCompletionsApiCompatibleProvider::new(
                profile["provider"].as_str().unwrap(),
                "https://example.test/v1",
                "",
                AuthStyle::Bearer,
            );
            for stream in [false, true] {
                let request = crate::generation::test_request(profile["model"].as_str().unwrap());
                let prepared = prepared_for(&provider, &request.messages);
                let result =
                    provider.build_chat_request_with_catalog(request, stream, prepared, None);
                if profile["default_supported"] == false {
                    assert!(result.is_err(), "{}", profile["provider"]);
                    continue;
                }
                let body = serde_json::to_value(result.unwrap()).unwrap();
                assert_eq!(body[profile["cap_field"].as_str().unwrap()], profile["cap"]);
                assert_eq!(body["stream"], stream);
                assert_eq!(body["messages"][0]["content"], "Hello");
                assert!(body.get("reasoning_effort").is_none());
                assert!(body.get("thinking").is_none());
                assert!(body.get("thinking_budget").is_none());
            }
            for reasoning in [
                ReasoningConfig::Disabled,
                ReasoningConfig::Effort(ReasoningEffort::None),
                ReasoningConfig::Effort(ReasoningEffort::High),
            ] {
                let mut request =
                    crate::generation::test_request(profile["model"].as_str().unwrap());
                request.reasoning = Some(reasoning);
                let prepared = prepared_for(&provider, &request.messages);
                assert!(
                    provider
                        .build_chat_request_with_catalog(request, false, prepared, None)
                        .is_err(),
                    "unknown explicit control must not disappear: {}",
                    profile["provider"]
                );
            }
        }
    }

    #[test]
    fn siliconflow_splits_the_prepared_total_reserve_for_default_thinking() {
        use crate::catalog::{
            ModelCatalog,
            generator::{SOURCE_URLS, SourceSnapshot, generate},
        };
        let mut source: SourceSnapshot =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                .unwrap();
        let data = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
        data["siliconflow"]["models"] = serde_json::json!({
            "toggle": {"tool_call":true,"reasoning":true,
                "reasoning_options":[{"type":"toggle","values":[true,false]}],
                "limit":{"context":24000,"output":2048}},
            "mandatory": {"tool_call":true,"reasoning":true,
                "limit":{"context":24000,"output":2048}},
            "instruct": {"tool_call":true,"reasoning":false,
                "limit":{"context":24000,"output":2048}}
        });
        let generated = generate(&source, true).unwrap();
        let catalog = ModelCatalog::parse(
            &serde_json::to_string(&generated.models).unwrap(),
            &serde_json::to_string(&generated.provenance).unwrap(),
        )
        .unwrap();
        let provider = ChatCompletionsApiCompatibleProvider::new(
            "siliconflow",
            "https://example.test/v1",
            "",
            AuthStyle::Bearer,
        );
        for stream in [false, true] {
            for reasoning in [
                None,
                Some(ReasoningConfig::Disabled),
                Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                Some(ReasoningConfig::Effort(ReasoningEffort::High)),
            ] {
                for id in ["toggle", "mandatory", "instruct"] {
                    let mut request = crate::generation::test_request(id);
                    request.reasoning = reasoning;
                    let prepared = prepared_for(&provider, &request.messages);
                    let result = provider.build_chat_request_with_catalog(
                        request,
                        stream,
                        prepared,
                        Some(&catalog),
                    );
                    let off = crate::generation::selected_off(reasoning);
                    let supported = reasoning.is_none() || (id == "toggle" && off);
                    if supported {
                        let body = serde_json::to_value(result.unwrap()).unwrap();
                        if id != "instruct" && reasoning.is_none() {
                            assert_eq!(body["thinking_budget"], 512);
                            assert_eq!(body["max_tokens"], 512);
                        } else {
                            assert_eq!(body["max_tokens"], 1024);
                            assert!(body.get("thinking_budget").is_none());
                        }
                        if id == "toggle" && off {
                            assert_eq!(body["enable_thinking"], false);
                        } else {
                            assert!(body.get("enable_thinking").is_none());
                        }
                    } else {
                        assert!(result.is_err(), "{id}: {reasoning:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn updated_source_limits_maps_and_compat_reach_discovery_and_outgoing_json() {
        use crate::catalog::{
            ModelCatalog,
            generator::{SOURCE_URLS, SourceSnapshot, generate},
        };
        let mut source: SourceSnapshot =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                .unwrap();
        let data = &mut source.sources.get_mut(SOURCE_URLS[0]).unwrap().body;
        data["deepinfra"]["models"]["fixture-generation-update"] = serde_json::json!({
            "tool_call":true,"reasoning":true,"temperature":false,
            "reasoning_options":[{"type":"effort","values":["none","high"]}],
            "limit":{"context":24000,"output":2048}});
        let generated = generate(&source, true).unwrap();
        let mut models = serde_json::to_value(&generated.models).unwrap();
        // Simulate a validated published metadata update without mutating the
        // process-global store shared with other tests.
        models["deepinfra"]["fixture-generation-update"]["compat"]["maxTokensField"] =
            serde_json::json!("max_completion_tokens");
        models["deepinfra"]["fixture-generation-update"]["thinkingLevelMap"]["high"] =
            serde_json::json!("medium");
        let catalog = ModelCatalog::parse(
            &models.to_string(),
            &serde_json::to_string(&generated.provenance).unwrap(),
        )
        .unwrap();
        let provider = ChatCompletionsApiCompatibleProvider::new(
            "deepinfra",
            "https://example.test/v1",
            "",
            AuthStyle::Bearer,
        );
        let mut request = crate::generation::test_request("fixture-generation-update");
        request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
        for stream in [false, true] {
            let prepared = prepared_for(&provider, &request.messages);
            let body = serde_json::to_value(
                provider
                    .build_chat_request_with_catalog(
                        request.clone(),
                        stream,
                        prepared,
                        Some(&catalog),
                    )
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(body["max_completion_tokens"], 1024);
            assert!(body.get("max_tokens").is_none());
            assert_eq!(body["reasoning_effort"], "medium");
        }
        let mut discovered = vec![ProviderModelInfo {
            id: request.model.clone(),
            name: None,
            description: None,
            created: None,
            provider: "deepinfra".into(),
            owned_by: None,
            limits: Default::default(),
            capabilities: Default::default(),
            transcription: None,
            pricing: None,
            active: None,
            family: None,
            lifecycle_status: None,
        }];
        catalog.enrich("deepinfra", &mut discovered);
        assert_eq!(discovered[0].limits.max_output_tokens, Some(2048));
        assert!(
            discovered[0]
                .capabilities
                .reasoning
                .as_ref()
                .unwrap()
                .effort_options
                .contains(&"high".into())
        );
        request.max_tokens = Some(2049);
        let prepared = prepared_for(&provider, &request.messages);
        assert!(
            provider
                .build_chat_request_with_catalog(request.clone(), false, prepared, Some(&catalog))
                .is_err()
        );
        request.max_tokens = Some(1024);
        request.temperature = Some(0.3);
        let prepared = prepared_for(&provider, &request.messages);
        assert!(
            provider
                .build_chat_request_with_catalog(request.clone(), false, prepared, Some(&catalog))
                .is_err()
        );
        request.temperature = None;
        request.reasoning = Some(ReasoningConfig::Disabled);
        let prepared = prepared_for(&provider, &request.messages);
        let body = serde_json::to_value(
            provider
                .build_chat_request_with_catalog(request, true, prepared, Some(&catalog))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["reasoning_effort"], "none");
    }

    #[test]
    fn documented_toggle_and_effort_profiles_render_their_actual_chat_controls() {
        use crate::catalog::{
            ModelCatalog,
            generator::{SOURCE_URLS, SourceSnapshot, generate},
        };
        let mut source: SourceSnapshot =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                .unwrap();
        source.sources.get_mut(SOURCE_URLS[0]).unwrap().body["novita"]["models"]["zai-org/glm-4.5"] = serde_json::json!({"tool_call":true,"reasoning":true,
                "reasoning_options":[{"type":"toggle"}],
                "limit":{"context":24000,"output":2048}});
        let generated = generate(&source, true).unwrap();
        let catalog = ModelCatalog::parse(
            &serde_json::to_string(&generated.models).unwrap(),
            &serde_json::to_string(&generated.provenance).unwrap(),
        )
        .unwrap();
        for (name, id, mode, expected) in [
            (
                "novita",
                "zai-org/glm-4.5",
                ReasoningConfig::Disabled,
                serde_json::json!({"enable_thinking":false}),
            ),
            (
                "cohere",
                "command-a-reasoning-08-2025",
                ReasoningConfig::Disabled,
                serde_json::json!({"reasoning_effort":"none"}),
            ),
            (
                "cohere",
                "command-a-reasoning-08-2025",
                ReasoningConfig::Effort(ReasoningEffort::High),
                serde_json::json!({"reasoning_effort":"high"}),
            ),
            (
                "mistral",
                "mistral-medium-latest",
                ReasoningConfig::Effort(ReasoningEffort::High),
                serde_json::json!({"reasoning_effort":"high"}),
            ),
            (
                "together",
                "Qwen/Qwen3.5-9B",
                ReasoningConfig::Disabled,
                serde_json::json!({"reasoning":{"enabled":false}}),
            ),
            (
                "together",
                "deepseek-ai/DeepSeek-V4-Pro",
                ReasoningConfig::Effort(ReasoningEffort::High),
                serde_json::json!({"reasoning":{"enabled":true},"reasoning_effort":"high"}),
            ),
        ] {
            let provider = ChatCompletionsApiCompatibleProvider::new(
                name,
                "https://example.test/v1",
                "",
                AuthStyle::Bearer,
            );
            let mut request = crate::generation::test_request(id);
            request.reasoning = Some(mode);
            for stream in [false, true] {
                let prepared = prepared_for(&provider, &request.messages);
                let body = serde_json::to_value(
                    provider
                        .build_chat_request_with_catalog(
                            request.clone(),
                            stream,
                            prepared,
                            Some(&catalog),
                        )
                        .unwrap(),
                )
                .unwrap();
                for (key, value) in expected.as_object().unwrap() {
                    assert_eq!(&body[key], value, "{name}/{id}");
                }
                assert_eq!(body["max_tokens"], 1024);
                assert!(body.get("chat_template_kwargs").is_none());
            }
            request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::Medium));
            let prepared = prepared_for(&provider, &request.messages);
            assert!(
                provider
                    .build_chat_request_with_catalog(request, false, prepared, Some(&catalog))
                    .is_err()
            );
        }
        let provider = ChatCompletionsApiCompatibleProvider::new(
            "together",
            "https://example.test/v1",
            "",
            AuthStyle::Bearer,
        );
        let mut request = crate::generation::test_request("zai-org/GLM-5.3");
        request.reasoning = Some(ReasoningConfig::Disabled);
        let prepared = prepared_for(&provider, &request.messages);
        assert!(
            provider
                .build_chat_request_with_catalog(request, false, prepared, Some(&catalog))
                .is_err()
        );
    }

    #[test]
    fn documented_profile_formats_use_vendor_controls_not_a_universal_effort_field() {
        let catalog = crate::catalog::ModelCatalog::parse(
            include_str!("../../tests/fixtures/catalog/models.json"),
            include_str!("../../tests/fixtures/catalog/provenance.json"),
        )
        .unwrap();
        for (name, model, expected) in [
            (
                "groq",
                "openai/gpt-oss-120b",
                serde_json::json!({"reasoning_effort":"high"}),
            ),
            (
                "cerebras",
                "gpt-oss-120b",
                serde_json::json!({"reasoning_effort":"high"}),
            ),
            (
                "deepseek",
                "deepseek-v4-flash",
                serde_json::json!({"thinking":{"type":"enabled"},"reasoning_effort":"high"}),
            ),
            (
                "zai-coding",
                "glm-5.2",
                serde_json::json!({"thinking":{"type":"enabled"},"reasoning_effort":"high"}),
            ),
            (
                "fireworks",
                "accounts/fireworks/models/glm-5p2",
                serde_json::json!({"reasoning_effort":"high"}),
            ),
        ] {
            let provider = ChatCompletionsApiCompatibleProvider::new(
                name,
                "https://example.test/v1",
                "",
                AuthStyle::Bearer,
            );
            let mut request = crate::generation::test_request(model);
            request.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
            for stream in [false, true] {
                let prepared = prepared_for(&provider, &request.messages);
                let body = serde_json::to_value(
                    provider
                        .build_chat_request_with_catalog(
                            request.clone(),
                            stream,
                            prepared,
                            Some(&catalog),
                        )
                        .unwrap(),
                )
                .unwrap();
                for (key, value) in expected.as_object().unwrap() {
                    assert_eq!(&body[key], value, "{name}/{model}");
                }
                assert_eq!(body["max_tokens"], 1024);
            }
        }
    }
    #[test]
    fn unknown_signed_extension_is_refused_by_production_compatible_decoder() {
        let state = ProviderReplayState::for_model(
            "deepseek",
            "deepseek-reasoner",
            serde_json::json!({"schema_version":1,"assistant_message":{"content":"answer","reasoning_content":"readable","tool_calls":[],"signature":"unknown opaque"}}),
        );
        assert!(
            ChatCompletionsApiCompatibleProvider::decode_replay_state(&state, "deepseek").is_err()
        );
        assert_eq!(
            state.payload["assistant_message"]["signature"],
            "unknown opaque"
        );
    }

    #[test]
    fn deepseek_non_tool_stream_keeps_native_empty_reasoning_field() {
        let chunks = finish_compatible_stream(
            &mut StreamToolCallAccumulator::default(),
            "deepseek",
            true,
            Some("answer".into()),
            Some(String::new()),
            ProviderTermination::Complete,
        )
        .unwrap();
        let state = chunks
            .iter()
            .find_map(|chunk| chunk.provider_replay_state.as_ref())
            .unwrap();
        assert_eq!(state.payload["assistant_message"]["reasoning_content"], "");
        assert!(chunks.last().unwrap().is_final);
        let ordinary = finish_compatible_stream(
            &mut StreamToolCallAccumulator::default(),
            "groq",
            true,
            Some("answer".into()),
            None,
            ProviderTermination::Complete,
        )
        .unwrap();
        assert!(
            ordinary
                .iter()
                .all(|chunk| chunk.provider_replay_state.is_none())
        );
    }

    use super::*;
    use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
    use crate::traits::Provider;
    use crate::types::{
        AttachmentArtifactContext, AttachmentDataSource, InputTypeSupport, MessageAttachment,
        MessageContentPart, MessageProvenance, MessageSourceRef, ProviderInputCapabilities,
        ProviderToolCall, ReasoningConfig, ReasoningEffort,
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

    fn prepared_for(
        provider: &ChatCompletionsApiCompatibleProvider,
        messages: &[ChatMessage],
    ) -> crate::attachments::PreparedProviderMessages {
        prepare_messages_for_provider(provider.name(), &provider.capabilities(), messages).unwrap()
    }

    fn text_content(content: &Option<ApiMessageContent>) -> Option<&str> {
        match content {
            Some(ApiMessageContent::Text(text)) => Some(text.as_str()),
            _ => None,
        }
    }

    fn test_provider() -> ChatCompletionsApiCompatibleProvider {
        ChatCompletionsApiCompatibleProvider::new(
            "test-provider",
            "https://api.example.com/v1",
            "sk-test-key",
            AuthStyle::Bearer,
        )
    }

    #[tokio::test]
    async fn completed_transcript_keeps_typed_media_through_preflight_budget_and_wire() {
        let provider = ChatCompletionsApiCompatibleProvider::new(
            "fixture-media-renderer",
            "https://api.example.com/v1",
            "test-key",
            AuthStyle::Bearer,
        )
        .with_input_capabilities(ProviderInputCapabilities {
            text: true,
            file: InputTypeSupport::native_inline_only(),
            image: InputTypeSupport::data_url_inline_only(),
            audio: InputTypeSupport::disabled(),
            video: InputTypeSupport::disabled(),
        });
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![ProviderToolCall {
                    id: "call".into(),
                    name: "read".into(),
                    arguments: "{}".into(),
                }],
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "source",
                    serde_json::json!({"reasoning_details":[]}),
                )),
            ),
            ChatMessage::tool_result("call", "read", "tool result"),
        ];
        messages[1].content_parts = vec![
            MessageContentPart::image(MessageAttachment {
                mime_type: "image/png".into(),
                name: Some("snapshot.png".into()),
                size_bytes: None,
                sha256: None,
                source: AttachmentDataSource::Bytes {
                    base64_data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+VrWQAAAAASUVORK5CYII=".into(),
                },
                artifact: Some(AttachmentArtifactContext {
                    workspace_id: "workspace".into(),
                    artifact_id: "artifact-image".into(),
                    artifact_version_id: Some("v1".into()),
                }),
            }),
            MessageContentPart::file(MessageAttachment {
                mime_type: "text/plain".into(),
                name: Some("document.txt".into()),
                size_bytes: Some(20),
                sha256: None,
                source: AttachmentDataSource::Bytes {
                    base64_data: "dG9vbCByZXN1bHQgZG9jdW1lbnQ=".into(),
                },
                artifact: Some(AttachmentArtifactContext {
                    workspace_id: "workspace".into(),
                    artifact_id: "artifact-file".into(),
                    artifact_version_id: Some("v2".into()),
                }),
            }),
        ];
        complete_round(&mut messages);
        let canonical = messages.clone();
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "fixture-media",
            &provider.capabilities(),
            &messages,
        )
        .unwrap();
        assert_eq!(prepared.budget_report.attachment_count, 2);
        assert!(prepared.budget_report.total_bytes > 20);
        assert_eq!(
            prepared.attachments[0]
                .artifact
                .as_ref()
                .map(|artifact| artifact.artifact_id.as_str()),
            Some("artifact-image")
        );
        assert_eq!(
            prepared.attachments[1]
                .artifact
                .as_ref()
                .map(|artifact| artifact.artifact_id.as_str()),
            Some("artifact-file")
        );
        let request = ChatRequest {
            model: "fixture-media".into(),
            messages: messages.clone(),
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let budgeted = crate::attachments::runtime::with_async_authority_scope(
            "completed-transcript-media-fixture".to_owned(),
            provider.prepare_input_budget(request),
        )
        .await
        .unwrap();
        assert_eq!(budgeted.media.len(), 2);
        assert!(
            budgeted
                .media
                .iter()
                .all(|estimate| estimate.input_tokens > 0)
        );
        assert_eq!(budgeted.request.messages[2].content_parts.len(), 2);
        assert!(matches!(
            &budgeted.request.messages[2].content_parts[0],
            MessageContentPart::Image { image }
                if image.sha256.is_some()
                    && image.artifact.as_ref().is_some_and(|artifact| artifact.artifact_id == "artifact-image")
        ));
        assert!(matches!(
            &budgeted.request.messages[2].content_parts[1],
            MessageContentPart::File { file }
                if file.sha256.is_some()
                    && file.artifact.as_ref().is_some_and(|artifact| artifact.artifact_id == "artifact-file")
        ));
        let prepared_wire = prepare_messages_for_provider_model(
            provider.name(),
            "fixture-media",
            &provider.capabilities(),
            &budgeted.request.messages,
        )
        .unwrap();
        assert_eq!(
            prepared_wire.budget_report.total_bytes,
            prepared.budget_report.total_bytes
        );
        let wire = provider
            .build_chat_request_from_prepared(budgeted.request, false, prepared_wire)
            .unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        assert_eq!(json["messages"].as_array().unwrap().len(), 3);
        assert_eq!(json["messages"][0]["role"], "assistant");
        assert_eq!(json["messages"][0]["tool_calls"][0]["id"], "call");
        assert_eq!(json["messages"][1]["role"], "tool");
        assert_eq!(json["messages"][1]["tool_call_id"], "call");
        assert_eq!(json["messages"][1]["content"], "tool result");
        assert_eq!(json["messages"][2]["role"], "user");
        assert!(json["messages"][2].get("tool_call_id").is_none());
        assert!(
            json["messages"][2]["content"]
                .to_string()
                .contains("image_url")
        );
        assert!(
            json["messages"][2]["content"]
                .to_string()
                .contains("file_data")
        );
        assert_eq!(messages, canonical);
    }

    #[test]
    fn mistral_wire_keeps_two_parallel_rounds_and_canonical_ids_separate() {
        let provider = ChatCompletionsApiCompatibleProvider::new(
            "mistral",
            "https://example.invalid/v1",
            "unused",
            AuthStyle::Bearer,
        );
        let mut request = crate::tools::policy::test_request();
        let mut messages = vec![ChatMessage::user("use tools")];
        for ids in [["foreign/a", "foreign?b"], ["foreign/a", "round-two"]] {
            let mut assistant = ChatMessage::assistant("");
            assistant.tool_calls = Some(
                ids.iter()
                    .map(|id| ProviderToolCall {
                        id: (*id).into(),
                        name: "lookup".into(),
                        arguments: "{}".into(),
                    })
                    .collect(),
            );
            messages.push(assistant);
            messages.extend(
                ids.iter()
                    .rev()
                    .map(|id| ChatMessage::tool_result(*id, "lookup", *id)),
            );
        }
        request.messages = messages.clone();
        let wire =
            serde_json::to_value(provider.build_chat_request(request, false).unwrap()).unwrap();
        for (assistant, result_one, result_two) in [(1, 3, 2), (4, 6, 5)] {
            assert_eq!(
                wire["messages"][assistant]["tool_calls"][0]["id"],
                wire["messages"][result_one]["tool_call_id"]
            );
            assert_eq!(
                wire["messages"][assistant]["tool_calls"][1]["id"],
                wire["messages"][result_two]["tool_call_id"]
            );
            assert_ne!(
                wire["messages"][assistant]["tool_calls"][0]["id"],
                wire["messages"][assistant]["tool_calls"][1]["id"]
            );
        }
        assert_eq!(
            wire["messages"][1]["tool_calls"][0]["id"],
            wire["messages"][4]["tool_calls"][0]["id"]
        );
        assert_eq!(messages[1].tool_calls.as_ref().unwrap()[0].id, "foreign/a");
    }

    #[test]
    fn each_compatible_profile_serializes_its_own_tool_controls() {
        let providers = [
            "groq",
            "mistral",
            "xai",
            "together",
            "fireworks",
            "novita",
            "perplexity",
            "cohere",
            "venice",
            "cerebras",
            "sambanova",
            "hyperbolic",
            "deepinfra",
            "huggingface",
            "ai21",
            "reka",
            "baseten",
            "nscale",
            "anyscale",
            "nebius",
            "friendli",
            "lepton",
            "siliconflow",
            "aihubmix",
            "astrai",
            "stepfun",
            "baichuan",
            "yi",
            "hunyuan",
            "ovhcloud",
            "nvidia",
            "synthetic",
            "doubao",
            "qianfan",
            "lmstudio",
            "llamacpp",
            "sglang",
            "vllm",
            "osaurus",
            "litellm",
            "custom",
            "deepseek", // delegates to this builder; replay validation is provider-owned
        ];
        for name in providers {
            let provider = ChatCompletionsApiCompatibleProvider::new(
                name,
                "https://example.invalid/v1",
                "unused",
                AuthStyle::Bearer,
            );
            for parallel in [None, Some(true), Some(false)] {
                for choice in [
                    ToolChoice::Auto,
                    ToolChoice::None,
                    ToolChoice::Required,
                    ToolChoice::Tool {
                        name: "lookup".into(),
                    },
                ] {
                    let mut request = crate::tools::policy::test_request();
                    request.tool_choice = Some(choice.clone());
                    request.parallel_tool_calls = parallel;
                    let supports_parallel = matches!(
                        name,
                        "groq"
                            | "mistral"
                            | "xai"
                            | "fireworks"
                            | "venice"
                            | "cerebras"
                            | "friendli"
                            | "synthetic"
                    );
                    let disabled = matches!(choice, ToolChoice::None);
                    let forced = matches!(choice, ToolChoice::Required | ToolChoice::Tool { .. });
                    let unsupported = matches!(name, "cohere" | "novita") && forced
                        || name == "synthetic" && matches!(choice, ToolChoice::Required)
                        || parallel == Some(false) && !disabled && !supports_parallel;
                    let wire = provider.build_chat_request(request, false);
                    assert_eq!(wire.is_err(), unsupported, "{name} {choice:?} {parallel:?}");
                    if let Ok(wire) = wire {
                        let json = serde_json::to_value(wire).unwrap();
                        assert_eq!(
                            json.get("parallel_tool_calls")
                                .and_then(serde_json::Value::as_bool),
                            if disabled || !supports_parallel {
                                None
                            } else {
                                parallel
                            },
                            "{name}"
                        );
                        if matches!(name, "cohere" | "novita") {
                            assert!(json.get("tool_choice").is_none());
                        } else {
                            match choice {
                                ToolChoice::Auto => {
                                    assert_eq!(json["tool_choice"], "auto", "{name}")
                                }
                                ToolChoice::None => {
                                    assert_eq!(json["tool_choice"], "none", "{name}")
                                }
                                ToolChoice::Required => {
                                    assert_eq!(json["tool_choice"], "required", "{name}")
                                }
                                ToolChoice::Tool { .. } => assert_eq!(
                                    json["tool_choice"]["function"]["name"], "lookup",
                                    "{name}"
                                ),
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn creates_with_required_fields() {
        let provider = test_provider();
        assert_eq!(provider.name, "test-provider");
        assert_eq!(provider.base_url, "https://api.example.com/v1");
        assert_eq!(provider.credential, "sk-test-key");
        assert!(!provider.input_types.image.is_supported());
        assert!(!provider.merge_system_into_user);
    }

    #[test]
    fn builder_methods() {
        let mut headers = HashMap::new();
        headers.insert("X-Custom".to_string(), "value".to_string());

        let provider = ChatCompletionsApiCompatibleProvider::new(
            "custom",
            "https://api.example.com",
            "key",
            AuthStyle::XApiKey,
        )
        .with_vision(true)
        .with_merge_system_into_user(true)
        .with_timeout_secs(60)
        .with_extra_headers(headers)
        .with_user_agent("pioneer/1.0");

        assert!(provider.input_types.image.is_supported());
        assert!(provider.merge_system_into_user);
        assert_eq!(
            provider.timeout_policy.non_stream_request_timeout,
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            provider.extra_headers.get("User-Agent").unwrap(),
            "pioneer/1.0"
        );
    }

    #[test]
    fn registry_endpoint_profiles_construct_exact_chat_and_discovery_routes() {
        let profiles: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../../tests/fixtures/endpoint_profiles.json"))
                .unwrap();
        assert_eq!(profiles.len(), 41);
        for fixture in profiles {
            let name = fixture["provider"].as_str().unwrap();
            let definition = crate::definition::provider_definition(name).unwrap();
            let base = definition.default_base_url.unwrap();
            assert_eq!(base, fixture["base"].as_str().unwrap(), "{name}");
            let provider = ChatCompletionsApiCompatibleProvider::new(
                name,
                format!("{base}/"),
                "dummy-key",
                AuthStyle::Bearer,
            );
            assert_eq!(
                provider.chat_completions_url(),
                fixture["chat"].as_str().unwrap(),
                "{name}"
            );
            assert_eq!(
                provider.models_url(),
                fixture["models"].as_str().unwrap(),
                "{name}"
            );
            for request in [
                provider.authorized_get(&provider.models_url()),
                provider.authorized_post(&provider.chat_completions_url()),
            ] {
                let request = request.build().unwrap();
                assert_eq!(
                    request.headers()["authorization"],
                    "Bearer dummy-key",
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn explicit_chat_route_with_trailing_slash_and_keyless_local_override() {
        for base in [
            "https://example.test/team/v9",
            "https://example.test/team/v9/",
            "https://example.test/team/v9/chat/completions/",
        ] {
            let provider =
                ChatCompletionsApiCompatibleProvider::new("custom", base, "", AuthStyle::Bearer);
            assert_eq!(
                provider.chat_completions_url(),
                "https://example.test/team/v9/chat/completions"
            );
            assert_eq!(provider.models_url(), "https://example.test/team/v9/models");
            for request in [
                provider.authorized_get(&provider.models_url()),
                provider.authorized_post(&provider.chat_completions_url()),
            ] {
                assert!(
                    !request
                        .build()
                        .unwrap()
                        .headers()
                        .contains_key("authorization")
                );
            }
        }
    }

    #[test]
    fn direct_convert_maps_roles() {
        let provider = test_provider();
        let messages = vec![
            ChatMessage::system("Be helpful"),
            ChatMessage::user("Hello"),
            ChatMessage::assistant("Hi!"),
        ];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = ChatCompletionsApiCompatibleProvider::direct_convert(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "system");
        assert_eq!(text_content(&api_messages[0].content), Some("Be helpful"));
        assert_eq!(api_messages[1].role, "user");
        assert_eq!(api_messages[2].role, "assistant");
    }

    #[test]
    fn reasoning_content_replay_is_enabled_by_default_for_assistant_tool_calls() {
        let provider = test_provider();
        let messages = vec![ChatMessage::assistant_tool_calls_with_reasoning(
            None::<String>,
            Some("provider reasoning token"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_skill".to_owned(),
                arguments: "{\"slug\":\"weather\"}".to_owned(),
            }],
        )];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.convert_messages(&prepared).unwrap();

        assert_eq!(
            api_messages[0].reasoning_content.as_deref(),
            Some("provider reasoning token")
        );
        assert!(api_messages[0].content.is_none());
    }

    #[test]
    fn reasoning_content_replay_can_be_disabled() {
        let provider = test_provider().with_reasoning_content_replay(false);
        let messages = vec![ChatMessage::assistant_tool_calls_with_reasoning(
            None::<String>,
            Some("do not serialize when disabled"),
            vec![ProviderToolCall {
                id: "call_1".to_owned(),
                name: "read_skill".to_owned(),
                arguments: "{\"slug\":\"weather\"}".to_owned(),
            }],
        )];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = provider.convert_messages(&prepared).unwrap();

        assert!(api_messages[0].reasoning_content.is_none());
    }

    #[test]
    fn compatible_provider_replay_state_preserves_present_empty_reasoning() {
        let provider = test_provider();
        let tool_calls = vec![ProviderToolCall {
            id: "call_1".to_owned(),
            name: "read_file".to_owned(),
            arguments: "{\"path\":\"README.md\"}".to_owned(),
        }];
        let mut replay_state = ChatCompletionsApiCompatibleProvider::assistant_replay_state(
            provider.name(),
            Some(String::new()),
            Some(String::new()),
            tool_calls.as_slice(),
        );
        replay_state.model = Some("compatible-model".to_owned());
        let message = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            None::<String>,
            tool_calls,
            Some(replay_state),
        );
        let request = ChatRequest {
            model: "compatible-model".to_owned(),
            messages: vec![
                message,
                ChatMessage::tool_result("call_1", "read_file", "file contents"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };

        assert!(request.messages[0].provenance.is_none());
        for stream in [false, true] {
            let rendered = provider
                .build_chat_request(request.clone(), stream)
                .expect("an explicitly present empty replay field must remain replayable");

            assert_eq!(rendered.messages[0].reasoning_content.as_deref(), Some(""));
            assert_eq!(text_content(&rendered.messages[0].content), Some(""));
            assert_eq!(rendered.messages[1].tool_call_id.as_deref(), Some("call_1"));
            assert_eq!(
                rendered.messages[0]
                    .tool_calls
                    .as_ref()
                    .expect("tool calls must be replayed")[0]
                    .function
                    .arguments,
                "{\"path\":\"README.md\"}"
            );
            let wire = serde_json::to_value(rendered).unwrap();
            assert_eq!(wire["stream"], stream);
            assert_eq!(
                wire["messages"][0].get("reasoning_content"),
                Some(&serde_json::json!(""))
            );
            assert_eq!(wire["messages"][0]["content"], "");
            assert_eq!(
                wire["messages"][0]["tool_calls"][0]["function"]["arguments"],
                "{\"path\":\"README.md\"}"
            );
        }
    }

    #[test]
    fn compatible_model_unbound_active_empty_replay_is_rejected_by_request_projection() {
        let provider = test_provider();
        let calls = vec![ProviderToolCall {
            id: "call_1".to_owned(),
            name: "read_file".to_owned(),
            arguments: "{\"path\":\"README.md\"}".to_owned(),
        }];
        let replay = ChatCompletionsApiCompatibleProvider::assistant_replay_state(
            provider.name(),
            Some(String::new()),
            Some(String::new()),
            &calls,
        );
        assert!(replay.model.is_none());
        let request = ChatRequest {
            model: "compatible-model".to_owned(),
            messages: vec![ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                calls,
                Some(replay),
            )],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        assert!(request.messages[0].provenance.is_none());
        for stream in [false, true] {
            let error = provider
                .build_chat_request(request.clone(), stream)
                .expect_err("model-unbound active replay must fail at the request-aware projector");
            assert!(
                error
                    .downcast_ref::<crate::history::IncompatibleProviderReplayContinuation>()
                    .is_some()
            );
            assert!(error.to_string().contains("test-provider/unknown-model"));
            assert!(error.to_string().contains("test-provider/compatible-model"));
        }
    }

    #[test]
    fn openrouter_completed_tool_round_renders_for_deepseek_compatible_wire() {
        let calls = vec![ProviderToolCall {
            id: "call_1".to_owned(),
            name: "inspect".to_owned(),
            arguments: "{}".to_owned(),
        }];
        let replay = ProviderReplayState::new(
            "openrouter",
            serde_json::json!({
                "reasoning_details": [
                    {"type":"reasoning.encrypted","data":"opaque"},
                    {"type":"reasoning.summary","summary":"portable openrouter reasoning"}
                ]
            }),
        );
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                Some("portable openrouter reasoning"),
                calls,
                Some(replay.clone()),
            ),
            ChatMessage::tool_result("call_1", "inspect", "tool outcome"),
        ];
        complete_round(&mut messages);
        let canonical = messages.clone();
        let provider = ChatCompletionsApiCompatibleProvider::new(
            "deepseek",
            "https://api.deepseek.com",
            "key",
            AuthStyle::Bearer,
        );

        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "deepseek-chat",
            &provider.capabilities(),
            messages.as_slice(),
        )
        .unwrap();
        let wire = provider.convert_messages(&prepared).unwrap();

        assert_eq!(wire.len(), 2);
        assert_eq!(
            wire[0].reasoning_content.as_deref(),
            Some("portable openrouter reasoning")
        );
        assert_eq!(wire[0].tool_calls.as_ref().unwrap()[0].id, "call_1");
        assert_eq!(wire[1].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(canonical[0].provider_replay_state.as_ref(), Some(&replay));
        assert_eq!(messages, canonical);
    }

    #[test]
    fn compatible_stream_completion_captures_absent_reasoning_losslessly() {
        let mut tool_calls = StreamToolCallAccumulator::default();
        tool_calls.ingest(vec![StreamToolCallDelta {
            index: Some(0),
            id: Some("call_1".to_owned()),
            function: Some(StreamToolFunctionDelta {
                name: Some("read_file".to_owned()),
                arguments: Some("{\"path\":\"README.md\"}".to_owned()),
            }),
            name: None,
            arguments: None,
        }]);

        let chunks = finish_compatible_stream(
            &mut tool_calls,
            "test-provider",
            true,
            Some(String::new()),
            None,
            ProviderTermination::ToolCalls,
        )
        .unwrap();

        let replay = ChatCompletionsApiCompatibleProvider::new(
            "test-provider",
            "https://api.example.com/v1",
            "key",
            AuthStyle::Bearer,
        )
        .replay_state_message(
            chunks[0]
                .provider_replay_state
                .as_ref()
                .expect("tool-call response must carry exact compatible replay state"),
        )
        .expect("compatible replay state must decode");
        assert!(replay.reasoning_content.is_none());
        assert_eq!(chunks[1].tool_calls.len(), 1);
        assert!(chunks[2].is_final);
    }

    #[test]
    fn direct_convert_tool_attachment_splits_into_tool_and_user_messages() {
        let provider = test_provider().with_vision(true);
        let mut tool_message =
            ChatMessage::tool_result("call_1", "cua_get_window_state", "{\"status\":\"ok\"}");
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

        let prepared = prepared_for(&provider, &[ChatMessage::user("hello"), tool_message]);
        let api_messages = ChatCompletionsApiCompatibleProvider::direct_convert(&prepared).unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(api_messages[1].role, "tool");
        assert_eq!(api_messages[1].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(
            api_messages[1].name.as_deref(),
            Some("cua_get_window_state")
        );
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
    fn direct_convert_file_part_serializes_file_data_as_raw_base64() {
        let provider = test_provider().with_input_capabilities(ProviderInputCapabilities {
            text: true,
            file: InputTypeSupport::native_inline_only(),
            image: InputTypeSupport::disabled(),
            audio: InputTypeSupport::disabled(),
            video: InputTypeSupport::disabled(),
        });
        let file_bytes = [1u8, 2, 3, 4];
        let expected_base64 = BASE64.encode(file_bytes);
        let messages = [ChatMessage::user_parts(vec![MessageContentPart::file(
            MessageAttachment {
                mime_type: "application/pdf".to_owned(),
                name: Some("doc.pdf".to_owned()),
                size_bytes: Some(file_bytes.len() as u64),
                sha256: None,
                source: AttachmentDataSource::Bytes {
                    base64_data: expected_base64.clone(),
                },
                artifact: None,
            },
        )])];

        let prepared = prepared_for(&provider, messages.as_slice());
        let api_messages = ChatCompletionsApiCompatibleProvider::direct_convert(&prepared).unwrap();
        let file_data = match api_messages[0].content.as_ref() {
            Some(ApiMessageContent::Parts(parts)) => parts.iter().find_map(|part| match part {
                ApiContentPart::File { file } => file.file_data.clone(),
                _ => None,
            }),
            _ => None,
        };

        assert_eq!(file_data.as_deref(), Some(expected_base64.as_str()));
    }

    #[test]
    fn merge_mode_uses_compiled_prompt_full_text() {
        let provider = test_provider().with_merge_system_into_user(true);
        let request = ChatRequest {
            model: "test-model".to_owned(),
            messages: vec![
                ChatMessage::system("legacy system prompt should be ignored"),
                ChatMessage::user("Hello"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: Some(crate::types::CompiledPromptPayload {
                stable_system_text: "Stable rules".to_owned(),
                dynamic_system_text: "Dynamic runtime".to_owned(),
                boundary_marker: "<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->".to_owned(),
                full_system_text:
                    "Stable rules\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic runtime"
                        .to_owned(),
            }),
        };

        let rendered = request.rendered_messages_with_compiled_prompt();
        let prepared = prepared_for(&provider, rendered.as_slice());
        let api_messages = provider.convert_messages(&prepared).unwrap();
        assert_eq!(api_messages[0].role, "user");
        assert_eq!(
            text_content(&api_messages[0].content),
            Some("Stable rules\n<!-- PIONEER_PROMPT_CACHE_BOUNDARY -->\nDynamic runtime\n\nHello")
        );
    }

    #[test]
    fn api_request_serializes_correctly() {
        let request = ApiChatRequest {
            cache_affinity: None,
            stream_options: None,
            generation: Default::default(),
            model: "gpt-4".into(),
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
            stream: false,
        };

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("gpt-4"));
        assert!(json.contains("\"role\":\"system\""));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(json.contains("\"stream\":false"));
        assert!(!json.contains("max_tokens"));
    }

    #[test]
    fn unknown_compatible_model_rejects_explicit_unverified_reasoning() {
        let provider = test_provider();
        let request = ChatRequest {
            model: "custom-compatible-model".to_owned(),
            messages: vec![ChatMessage::user("Hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
            compiled_prompt: None,
        };

        let error = provider.build_chat_request(request, false).unwrap_err();
        assert!(error.to_string().contains("no confirmed mapping"));
    }

    #[test]
    fn api_response_with_usage() {
        let json = r#"{
            "choices": [{"message": {"content": "Hello"}}],
            "usage": {"prompt_tokens": 42, "completion_tokens": 15}
        }"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        let usage = response.usage.unwrap().normalized();
        assert_eq!(usage.input_tokens, Some(42));
        assert_eq!(usage.output_tokens, Some(15));
    }

    #[test]
    fn api_response_empty_choices() {
        let json = r#"{"choices":[]}"#;
        let response: ApiChatResponse = serde_json::from_str(json).unwrap();
        assert!(response.choices.is_empty());
    }

    #[test]
    fn stream_response_deserializes_error_envelope_without_delta() {
        let json = r#"{"error":{"message":"upstream failed","type":"provider_error","code":"overloaded"},"choices":[{"finish_reason":"error"}]}"#;
        let response: StreamResponse = serde_json::from_str(json).unwrap();

        assert_eq!(
            response.error.unwrap().description(),
            "upstream failed, type=provider_error, code=overloaded"
        );
        assert_eq!(response.choices[0].finish_reason.as_deref(), Some("error"));
        assert!(response.choices[0].delta.content.is_none());
    }

    #[test]
    fn provider_capabilities_with_vision() {
        use crate::traits::Provider;
        let provider = test_provider().with_vision(true);
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(caps.vision);
    }

    #[test]
    fn provider_metadata_preserves_default_identity_and_capabilities() {
        use crate::traits::Provider;
        let provider = test_provider();
        assert_eq!(provider.name(), "test-provider");
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(!caps.vision);
    }

    #[test]
    fn system_message_merge_matrix() {
        let cases = [
            (
                "merge_system_into_first_user_message",
                vec![
                    ChatMessage::system("You are a helpful assistant."),
                    ChatMessage::user("Hello"),
                    ChatMessage::assistant("Hi!"),
                ],
                vec!["user", "assistant"],
                "You are a helpful assistant.\n\nHello",
            ),
            (
                "merge_multiple_system_messages",
                vec![
                    ChatMessage::system("Rule 1"),
                    ChatMessage::system("Rule 2"),
                    ChatMessage::user("Hello"),
                ],
                vec!["user"],
                "Rule 1\nRule 2\n\nHello",
            ),
            (
                "merge_system_only_no_user",
                vec![ChatMessage::system("System only")],
                vec!["user"],
                "System only",
            ),
            (
                "merge_no_system_messages",
                vec![ChatMessage::user("Hello"), ChatMessage::assistant("Hi!")],
                vec!["user", "assistant"],
                "Hello",
            ),
        ];
        for (case, messages, expected_roles, expected_first_text) in cases {
            let provider = test_provider().with_merge_system_into_user(true);
            let prepared = prepared_for(&provider, messages.as_slice());
            let api_messages = provider.merge_system_messages(&prepared).unwrap();
            assert_eq!(api_messages.len(), expected_roles.len(), "{case}");
            let roles: Vec<_> = api_messages
                .iter()
                .map(|message| message.role.as_str())
                .collect();
            assert_eq!(roles, expected_roles, "{case}");
            assert_eq!(
                text_content(&api_messages[0].content),
                Some(expected_first_text),
                "{case}"
            );
        }
    }
}

#[cfg(test)]
#[path = "wire_tests/compatible.rs"]
mod wire_contract_tests;

#[cfg(test)]
mod prompt_cache_tests {
    use super::*;
    use crate::{MessageProvenance, MessageSourceRef};

    fn request(owner: &str) -> ChatRequest {
        let mut input = ChatMessage::user("current input");
        input.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "workspace".into(),
            thread_id: "source-thread".into(),
            context_thread: Some(owner.into()),
            unit_id: "turn:input".into(),
            sources: vec![MessageSourceRef {
                scope: "input:turn".into(),
                id: "input".into(),
                version: "input-revision:1".into(),
            }],
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            inherited: false,
            complete: true,
            protected_input: true,
        });
        crate::providers::history_test_support::request(vec![input])
    }

    #[tokio::test]
    async fn native_chat_cache_affinity_is_stable_and_endpoint_scoped_at_wire_boundary() {
        crate::attachments::runtime::with_async_authority_scope(
            "chat-cache-affinity-fixture".into(),
            native_chat_cache_affinity_fixture(),
        )
        .await;
    }

    async fn native_chat_cache_affinity_fixture() {
        for (name, root) in [
            ("mistral", "https://api.mistral.ai/v1"),
            ("xai", "https://api.x.ai/v1"),
            ("fireworks", "https://api.fireworks.ai/inference/v1"),
        ] {
            let provider =
                ChatCompletionsApiCompatibleProvider::new(name, root, "fixture", AuthStyle::Bearer);
            let first = provider
                .build_chat_request_async(request("execution-owner"), false)
                .await
                .unwrap();
            let key = first.cache_affinity.clone().unwrap();
            let mut next = request("execution-owner");
            let mut latest = next.messages[0].clone();
            latest.content = "a new question".into();
            let origin = latest.provenance.as_mut().unwrap();
            origin.thread_id = "other-source".into();
            origin.unit_id = "another-turn:input".into();
            origin.sources[0].version = "input-revision:3".into();
            next.messages.push(latest);
            let next = provider.build_chat_request_async(next, true).await.unwrap();
            assert_eq!(next.cache_affinity.as_ref(), Some(&key));
            assert_ne!(
                provider
                    .native_cache_affinity(&request("other-owner"))
                    .as_ref(),
                Some(&key)
            );
            let first_wire = serde_json::to_value(&first).unwrap();
            let http = provider.chat_post(&first).build().unwrap();
            if name == "mistral" {
                assert_eq!(first_wire["prompt_cache_key"], key);
                assert!(!http.headers().contains_key("x-grok-conv-id"));
            } else {
                assert!(first_wire.get("prompt_cache_key").is_none());
                let header = if name == "xai" {
                    "x-grok-conv-id"
                } else {
                    "x-session-affinity"
                };
                assert_eq!(http.headers()[header].to_str().unwrap(), key);
            }
            for field in ["cache_affinity", "prompt_cache_retention", "cache_control"] {
                assert!(first_wire.get(field).is_none());
            }
            for endpoint in [
                "https://proxy.example/v1",
                "https://api.mistral.ai/v1/private",
                "https://api.x.ai/v1/private",
            ] {
                let custom = ChatCompletionsApiCompatibleProvider::new(
                    name,
                    endpoint,
                    "fixture",
                    AuthStyle::Bearer,
                );
                assert!(
                    custom
                        .native_cache_affinity(&request("execution-owner"))
                        .is_none()
                );
            }
            let unknown = crate::providers::history_test_support::request(vec![ChatMessage::user(
                "no trusted origin",
            )]);
            assert!(provider.native_cache_affinity(&unknown).is_none());
        }
        let custom = ChatCompletionsApiCompatibleProvider::new(
            "custom",
            "https://api.x.ai/v1",
            "fixture",
            AuthStyle::Bearer,
        );
        assert!(
            custom
                .native_cache_affinity(&request("execution-owner"))
                .is_none()
        );
        let explicit = ChatCompletionsApiCompatibleProvider::new(
            "xai",
            "https://api.x.ai/v1",
            "fixture",
            AuthStyle::Bearer,
        )
        .with_extra_headers(HashMap::from([(
            "X-Grok-Conv-Id".into(),
            "explicit-owner".into(),
        )]));
        let body = explicit
            .build_chat_request_async(request("execution-owner"), false)
            .await
            .unwrap();
        assert_eq!(
            explicit.chat_post(&body).build().unwrap().headers()["x-grok-conv-id"],
            "explicit-owner"
        );
    }
}

#[cfg(test)]
mod cache_extension_regressions {
    use super::*;
    use crate::traits::Provider;
    use serde_json::{Value, json};

    fn provider(name: &str, credential: &str) -> ChatCompletionsApiCompatibleProvider {
        ChatCompletionsApiCompatibleProvider::new(
            name,
            crate::provider_definition(name)
                .unwrap()
                .default_base_url
                .unwrap(),
            credential,
            AuthStyle::Bearer,
        )
    }
    fn request() -> ChatRequest {
        crate::providers::history_test_support::request(vec![
            ChatMessage::system("instructions"),
            ChatMessage::user("question"),
        ])
    }
    async fn render(
        provider: &ChatCompletionsApiCompatibleProvider,
        workspace: &str,
        owner: &str,
        task: Option<&str>,
        purpose: &'static str,
        stream: bool,
    ) -> reqwest::Request {
        crate::prompt_cache::conversation_task_scope(
            workspace.into(),
            owner.into(),
            task.map(str::to_owned),
            crate::prompt_cache::scope(workspace, owner, purpose, async {
                let body = provider
                    .build_chat_request_async(request(), stream)
                    .await
                    .unwrap();
                provider.chat_post(&body).build().unwrap()
            }),
        )
        .await
    }
    fn key(request: &reqwest::Request, name: &str) -> String {
        if matches!(name, "baseten" | "litellm") {
            request.headers()[if name == "baseten" {
                "x-session-affinity"
            } else {
                "x-litellm-session-id"
            }]
            .to_str()
            .unwrap()
            .to_owned()
        } else {
            serde_json::from_slice::<Value>(request.body().unwrap().as_bytes().unwrap()).unwrap()["prompt_cache_key"].as_str().unwrap().to_owned()
        }
    }

    #[tokio::test]
    async fn documented_routing_is_scoped_stable_and_shared_by_json_and_sse() {
        crate::attachments::runtime::with_async_authority_scope(
            "cache-routing-fixture".into(),
            documented_routing_fixture(),
        )
        .await;
    }

    async fn documented_routing_fixture() {
        for name in [
            "together",
            "venice",
            "cerebras",
            "deepinfra",
            "baseten",
            "litellm",
        ] {
            let p = provider(name, "credential-a");
            let a = key(
                &render(&p, "ws", "conversation", Some("task"), "main", false).await,
                name,
            );
            assert_eq!(a.len(), 64);
            let recreated = provider(name, "credential-a");
            let b = key(
                &render(&recreated, "ws", "conversation", Some("task"), "main", true).await,
                name,
            );
            assert_eq!(a, b, "{name}");
            let (parallel_left, parallel_right) = tokio::join!(
                render(
                    &p,
                    "ws",
                    "parallel-left",
                    Some("shared-task"),
                    "main",
                    false
                ),
                render(
                    &p,
                    "ws",
                    "parallel-right",
                    Some("shared-task"),
                    "main",
                    true
                )
            );
            assert_eq!(
                key(&parallel_left, name) == key(&parallel_right, name),
                name == "baseten"
            );
            let changed = crate::prompt_cache::conversation_task_scope(
                "ws".into(),
                "conversation".into(),
                Some("task".into()),
                crate::prompt_cache::scope("ws", "execution-next-turn", "main", async {
                    let mut next = request();
                    next.model = "another-fixture".into();
                    next.messages.push(ChatMessage::user("next question"));
                    let body = p.build_chat_request_async(next, true).await.unwrap();
                    p.chat_post(&body).build().unwrap()
                }),
            )
            .await;
            assert_eq!(a, key(&changed, name));
            assert_ne!(
                a,
                key(
                    &render(&p, "other-ws", "conversation", Some("task"), "main", true).await,
                    name
                )
            );
            assert_ne!(
                a,
                key(
                    &render(&p, "ws", "conversation", Some("task"), "preflight", true).await,
                    name
                )
            );
            assert_ne!(
                a,
                key(
                    &render(
                        &provider(name, "credential-b"),
                        "ws",
                        "conversation",
                        Some("task"),
                        "main",
                        true
                    )
                    .await,
                    name
                )
            );
            let child = key(
                &render(&p, "ws", "child", Some("task"), "main", true).await,
                name,
            );
            assert_eq!(a == child, name == "baseten");
            if name == "baseten" {
                let main = key(
                    &render(&p, "ws", "conversation", None, "main", false).await,
                    name,
                );
                let first_child = key(
                    &render(
                        &p,
                        "ws",
                        "child",
                        Some("conversation:conversation"),
                        "main",
                        true,
                    )
                    .await,
                    name,
                );
                assert_eq!(main, first_child);
                assert_ne!(
                    a,
                    key(
                        &render(&p, "ws", "child", Some("unrelated-task"), "main", true).await,
                        name
                    )
                );
            }
            let unscoped = p.build_chat_request_async(request(), false).await.unwrap();
            assert!(unscoped.cache_affinity.is_none());
            let proxy = ChatCompletionsApiCompatibleProvider::new(
                name,
                "https://unverified.invalid/v1",
                "credential-a",
                AuthStyle::Bearer,
            );
            let wire = render(&proxy, "ws", "conversation", Some("task"), "main", true).await;
            assert!(
                serde_json::from_slice::<Value>(wire.body().unwrap().as_bytes().unwrap())
                    .unwrap()
                    .get("prompt_cache_key")
                    .is_none()
            );
            assert!(!wire.headers().contains_key("x-session-affinity"));
            assert_eq!(
                wire.headers().contains_key("x-litellm-session-id"),
                name == "litellm"
            );
        }
        // Synthetic source facts exercise admission, not a new discovery
        // pipeline or a claim that every native model endpoint emits the fact.
        for name in ["together", "venice", "cerebras", "deepinfra"] {
            let p = provider(name, "credential-a");
            for (native, source, public, expected) in [
                (None, Some(false), true, false),
                (None, None, true, true),
                (None, Some(true), true, true),
                (Some(json!(false)), Some(true), true, false),
                (Some(json!(true)), Some(false), true, true),
                (Some(Value::Null), Some(false), true, false),
                (None, Some(false), false, true),
            ] {
                let model = |support: Value| {
                    json!({
                        "id":"fixture", "name":"Fixture", "provider":name,
                        "api":"openai-completions", "baseUrl":p.base_url,
                        "contextWindow":4096, "maxTokens":2048, "reasoning":false,
                        "input":["text"], "cost":{},
                        "compat":{"supportsPromptCacheKey":support}
                    })
                };
                let catalog = crate::catalog::ModelCatalog::parse(
                    &json!({name:{"fixture":model(json!(source))}}).to_string(),
                    &json!({name:{"fixture":{
                        "contextWindow":{"kind":"source","expression":"fixture"},
                        "maxTokens":{"kind":"source","expression":"fixture"}
                    }}})
                    .to_string(),
                )
                .unwrap();
                let models = native
                    .map(|fact| {
                        (
                            "fixture".into(),
                            serde_json::from_value(model(fact)).unwrap(),
                        )
                    })
                    .into_iter()
                    .collect();
                crate::generation::with_native_models(
                    name,
                    public,
                    models,
                    crate::prompt_cache::scope("ws", "source-owner", "main", async {
                        for stream in [false, true] {
                            let mut request = request();
                            request.model = "fixture".into();
                            let prepared = crate::attachments::prepare_messages_for_provider(
                                name,
                                &p.capabilities(),
                                &request.messages,
                            )
                            .unwrap();
                            let body = p
                                .build_chat_request_with_catalog(
                                    request,
                                    stream,
                                    prepared,
                                    Some(&catalog),
                                )
                                .unwrap();
                            let wire = p.chat_post(&body).build().unwrap();
                            let body: Value =
                                serde_json::from_slice(wire.body().unwrap().as_bytes().unwrap())
                                    .unwrap();
                            assert_eq!(
                                body.get("prompt_cache_key").is_some(),
                                expected,
                                "{name}: stream={stream}, source={source:?}, public={public}"
                            );
                            assert_eq!(body["stream"], stream);
                        }
                    }),
                )
                .await;
            }
        }
        for (name, header) in [
            ("baseten", "X-Session-Affinity"),
            ("litellm", "X-LiteLLM-Session-ID"),
        ] {
            let p = provider(name, "a")
                .with_extra_headers(HashMap::from([(header.into(), "operator-session".into())]));
            let wire = render(&p, "ws", "owner", None, "main", true).await;
            assert_eq!(wire.headers()[header], "operator-session");
        }
    }

    #[tokio::test]
    async fn aihubmix_signed_json_and_sse_survive_canonical_disk_history_and_next_tool_request() {
        crate::attachments::runtime::with_async_authority_scope(
            "aihubmix-replay-fixture".into(),
            aihubmix_signed_round_trip_fixture(),
        )
        .await;
    }

    async fn aihubmix_signed_round_trip_fixture() {
        let p = provider("aihubmix", "a");
        let details = json!({"type":"thinking","thinking":"look up","signature":"sig-part-two","vendor":{"opaque":true}});
        let call =
            json!({"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}});
        let response = p.response_from_api(serde_json::from_value(json!({"choices":[{"message":{"content":null,"reasoning_details":details,"tool_calls":[call]},"finish_reason":"tool_calls"}]})).unwrap(), None).unwrap();

        let unverified = ChatCompletionsApiCompatibleProvider::new(
            "custom",
            "https://example.invalid/v1",
            "a",
            AuthStyle::Bearer,
        );
        assert!(unverified.response_from_api(serde_json::from_value(json!({"choices":[{"message":{"reasoning_details":details},"finish_reason":"stop"}]})).unwrap(), None).is_err());
        let incomplete = "data: {\"choices\":[{\"delta\":{\"reasoning_details\":{\"type\":\"thinking\",\"signature\":\"part\"}}}]}\n\n";
        let failed = ChatCompletionsApiCompatibleProvider::decode_stream_with_context(
            Box::pin(futures_util::stream::iter(vec![Ok(bytes::Bytes::from(
                incomplete,
            ))])),
            "aihubmix".into(),
            true,
            p.native_details_authority(),
            None,
        )
        .collect::<Vec<_>>()
        .await;
        assert!(failed.iter().any(Result::is_err));
        assert!(
            !failed
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .any(|c| c.is_final || c.provider_replay_state.is_some())
        );
        let wire = concat!(
            "data: {\"choices\":[{\"delta\":{\"reasoning_details\":{\"type\":\"thinking\",\"thinking\":\"look \",\"signature\":\"sig-part-\"}}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"reasoning_details\":{\"thinking\":\"up\",\"signature\":\"two\",\"vendor\":{\"opaque\":true}},\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"prompt_tokens_details\":{\"cached_tokens\":80}}}\n\n",
            "data: [DONE]\n\n"
        );
        let chunks = ChatCompletionsApiCompatibleProvider::decode_stream_with_context(
            Box::pin(futures_util::stream::iter(vec![Ok(
                bytes::Bytes::from_static(wire.as_bytes()),
            )])),
            "aihubmix".into(),
            true,
            p.native_details_authority(),
            None,
        )
        .collect::<Vec<_>>()
        .await;
        let chunks = chunks.into_iter().collect::<Result<Vec<_>>>().unwrap();
        let streamed = chunks
            .iter()
            .find_map(|c| c.provider_replay_state.clone())
            .unwrap();
        assert_eq!(streamed, response.provider_replay_state.clone().unwrap());
        assert_eq!(
            chunks
                .iter()
                .filter_map(|c| c.usage.as_ref())
                .last()
                .unwrap()
                .cache_read_input_tokens,
            Some(80)
        );
        for mut state in [streamed, response.provider_replay_state.unwrap()] {
            state.model = Some("fixture".into());
            let mut message =
                ChatMessage::assistant_tool_calls(None::<String>, response.tool_calls.clone());
            message.provider_replay_state = Some(state);
            let envelope = crate::CanonicalProviderRoundEnvelope {
                ui_copies: None,
                version: 1,
                round_id: "round".into(),
                termination: ProviderTermination::ToolCalls,
                message,
                calls: vec![crate::ProviderCallIdentity {
                    provider_call_id: "call_1".into(),
                    turn_item_id: "item".into(),
                    ordinal: 0,
                }],
            };
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("canonical-round.json");
            std::fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();
            drop(envelope);
            let restored: crate::CanonicalProviderRoundEnvelope =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            let mut next = request();
            next.messages.push(restored.message);
            next.messages
                .push(ChatMessage::tool_result("call_1", "lookup", "found"));
            next.tools = Some(vec![ToolDefinition {
                name: "lookup".into(),
                description: "lookup".into(),
                parameters: json!({"type":"object","properties":{}}),
            }]);
            let recreated =
                provider("aihubmix", "rotated-credential").with_reasoning_content_replay(false);
            let rendered = recreated
                .build_chat_request_async(next.clone(), false)
                .await
                .unwrap();
            let body = serde_json::to_value(rendered).unwrap();
            assert_eq!(body["messages"][2]["reasoning_details"], details);
            assert_eq!(body["messages"][2]["tool_calls"][0]["id"], "call_1");
            assert_eq!(body["messages"][3]["role"], "tool");
            assert_eq!(body["messages"][3]["tool_call_id"], "call_1");
            let mut unknown = next.clone();
            unknown.messages[2]
                .provider_replay_state
                .as_mut()
                .unwrap()
                .payload["assistant_message"]["required_native_extension"] = json!("opaque");
            assert!(p.build_chat_request_async(unknown, false).await.is_err());
            let foreign = ChatCompletionsApiCompatibleProvider::new(
                "aihubmix",
                "https://unverified.invalid/v1",
                "a",
                AuthStyle::Bearer,
            );
            assert!(foreign.build_chat_request_async(next, false).await.is_err());
        }
    }

    #[tokio::test]
    async fn fireworks_headers_are_fallback_observations_and_final_sse_body_is_authoritative() {
        let p = provider("fireworks", "a");
        let headers = reqwest::header::HeaderMap::from_iter([
            (
                reqwest::header::HeaderName::from_static("fireworks-prompt-tokens"),
                "100".parse().unwrap(),
            ),
            (
                reqwest::header::HeaderName::from_static("fireworks-cached-prompt-tokens"),
                "80".parse().unwrap(),
            ),
        ]);
        for (wire, expected) in [
            (None, None),
            (Some("0"), Some(0)),
            (Some("-1"), None),
            (Some("invalid"), None),
            (Some("18446744073709551616"), None),
        ] {
            let mut partial = reqwest::header::HeaderMap::new();
            partial.insert("fireworks-prompt-tokens", "100".parse().unwrap());
            if let Some(wire) = wire {
                partial.insert("fireworks-cached-prompt-tokens", wire.parse().unwrap());
            }
            let parsed = p.cache_header_usage(&partial).unwrap();
            assert_eq!(parsed.input_tokens, Some(100));
            assert_eq!(parsed.cache_read_input_tokens, expected);
        }
        let observed = p.cache_header_usage(&headers).unwrap();
        assert_eq!(observed.input_tokens, Some(100));
        assert_eq!(observed.cache_read_input_tokens, Some(80));
        let partial: ApiChatResponse = serde_json::from_value(json!({"choices":[{"message":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":120,"prompt_tokens_details":{"cache_write_tokens":10}}})).unwrap();
        let merged = p
            .response_from_api(partial, Some(observed.clone()))
            .unwrap()
            .usage
            .unwrap();
        assert_eq!(merged.uncached_input_tokens, Some(30));
        let invalid: ApiChatResponse =
            serde_json::from_value(json!({"choices":[],"usage":{"completion_tokens":1}})).unwrap();
        let error = p
            .response_from_api(invalid, Some(observed.clone()))
            .unwrap_err();
        assert_eq!(
            crate::usage::error_usage(&error)
                .unwrap()
                .cache_read_input_tokens,
            Some(80)
        );
        let api: ApiChatResponse = serde_json::from_value(json!({"choices":[{"message":{"content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":120,"prompt_tokens_details":{"cached_tokens":0}}})).unwrap();
        let usage = p
            .response_from_api(api, Some(observed.clone()))
            .unwrap()
            .usage
            .unwrap();
        assert_eq!(
            (usage.input_tokens, usage.cache_read_input_tokens),
            (Some(120), Some(0))
        );
        let failure = "data: {\"error\":{\"message\":\"fixture failure\"}}\n\n";
        let failed = ChatCompletionsApiCompatibleProvider::decode_stream_with_context(
            Box::pin(futures_util::stream::iter(vec![Ok(bytes::Bytes::from(
                failure,
            ))])),
            "fireworks".into(),
            true,
            None,
            Some(observed.clone()),
        )
        .collect::<Vec<_>>()
        .await;
        assert_eq!(
            failed[0]
                .as_ref()
                .unwrap()
                .usage
                .as_ref()
                .unwrap()
                .cache_read_input_tokens,
            Some(80)
        );
        assert!(failed.iter().any(Result::is_err));
        assert!(
            !failed
                .iter()
                .filter_map(|r| r.as_ref().ok())
                .any(|c| c.is_final)
        );
        let wire = "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":120,\"prompt_tokens_details\":{\"cached_tokens\":0}}}\n\ndata: [DONE]\n\n";
        let chunks = ChatCompletionsApiCompatibleProvider::decode_stream_with_context(
            Box::pin(futures_util::stream::iter(vec![Ok(bytes::Bytes::from(
                wire,
            ))])),
            "fireworks".into(),
            true,
            None,
            Some(observed),
        )
        .collect::<Vec<_>>()
        .await;
        let chunks = chunks.into_iter().collect::<Result<Vec<_>>>().unwrap();
        let mut usage = crate::TokenUsage::default();
        for chunk in chunks {
            if let Some(snapshot) = chunk.usage {
                usage.update(&snapshot);
            }
        }
        assert_eq!(
            (usage.input_tokens, usage.cache_read_input_tokens),
            (Some(120), Some(0))
        );
        assert!(
            usage
                .prompt_cache_diagnostics
                .unwrap()
                .get("fireworks_cache_headers")
                .is_some()
        );
        assert!(
            provider("custom", "a")
                .cache_header_usage(&headers)
                .is_none()
        );
    }
}
