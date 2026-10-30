//! Vercel owns discovery/account admission; generation stays on Responses.
use super::{AuthStyle, ResponsesApiCompatibleProvider};
use crate::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, InputTypeSupport, Provider,
    ProviderCapabilities, ProviderFailureClassification, ProviderInputCapabilities, ProviderStream,
    ProviderTimeoutPolicy, ProviderWarmupOutcome, StreamChunk,
};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use pioneer_protocol::{
    ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits, ProviderModelPricing,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::sync::RwLock;

pub(crate) const BASE_URL: &str = "https://ai-gateway.vercel.sh/v1";
const NAME: &str = "vercel-ai-gateway";

pub struct VercelAiGatewayProvider {
    adapter: ResponsesApiCompatibleProvider,
    client: reqwest::Client,
    root: String,
    key: String,
    policy: ProviderTimeoutPolicy,
    models: std::sync::RwLock<ModelSnapshot>,
    account: RwLock<Option<String>>,
}

#[derive(Default)]
struct ModelSnapshot {
    entries: BTreeMap<String, crate::catalog::CatalogModel>,
    complete: bool,
}

#[derive(Debug)]
struct SafeFailure(ProviderFailureClassification);
#[derive(serde::Deserialize)]
struct EmbeddingWireResponse {
    #[serde(flatten)]
    indexed: super::embedding::IndexedEmbeddingResponse,
    model: Option<String>,
    #[serde(default, rename = "providerMetadata")]
    provider_metadata: Value,
}
impl std::fmt::Display for SafeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Vercel AI Gateway request failed")
    }
}
impl std::error::Error for SafeFailure {}
fn safe_decode(error: anyhow::Error) -> anyhow::Error {
    if error
        .downcast_ref::<crate::ProviderResponseTooLarge>()
        .is_some()
    {
        return error;
    }
    // Bounded reader preserves reqwest's typed transport error; JSON decoding
    // happens afterwards and must not be classified as a retryable transport.
    if let Some(transport) = error.downcast_ref::<reqwest::Error>() {
        let mut facts = ProviderFailureClassification::new(
            pioneer_protocol::ProviderFailureClass::NetworkTransient,
        );
        facts.is_network_error = true;
        facts.http_status = transport.status().map(|status| status.as_u16());
        return SafeFailure(facts).into();
    }
    SafeFailure(ProviderFailureClassification::new(
        pioneer_protocol::ProviderFailureClass::ProviderRejected,
    ))
    .into()
}

impl VercelAiGatewayProvider {
    pub fn new(key: impl Into<String>) -> Self {
        Self::with_base_url(key, BASE_URL)
    }
    pub fn with_base_url(key: impl Into<String>, root: impl Into<String>) -> Self {
        let key = key.into();
        let root = root.into();
        let adapter = ResponsesApiCompatibleProvider::new(NAME, &root, &key, AuthStyle::Bearer);
        let authority = adapter
            .authority_fingerprint()
            .expect("Responses authority")
            .to_owned();
        Self::from_context(&key, &root, ProviderTimeoutPolicy::default(), &authority)
    }
    pub(crate) fn from_context(
        key: &str,
        root: &str,
        policy: ProviderTimeoutPolicy,
        authority: &str,
    ) -> Self {
        let client = crate::http::build_client(policy);
        let root = root.trim_end_matches('/').to_owned();
        let inputs = ProviderInputCapabilities {
            text: true,
            image: InputTypeSupport::data_url_inline_only(),
            file: InputTypeSupport {
                native: true,
                file_upload: false,
                data_url_inline: true,
                text_fallback: false,
            },
            ..ProviderInputCapabilities::disabled_for_all_file_types()
        };
        let adapter = ResponsesApiCompatibleProvider::from_context(
            NAME.into(),
            root.clone(),
            key.into(),
            AuthStyle::Bearer,
            authority.into(),
            policy,
            client.clone(),
            inputs,
        )
        .with_provisional_messages(true);
        Self {
            adapter,
            client,
            root,
            key: key.into(),
            policy,
            models: std::sync::RwLock::new(ModelSnapshot::default()),
            account: RwLock::new(None),
        }
    }
    async fn ensure_model(&self, id: &str) -> Result<()> {
        let cached = self
            .models
            .read()
            .expect("Vercel model snapshot")
            .entries
            .contains_key(id);
        if !cached {
            self.discover_models(false).await?;
        }
        let (cached, complete) = {
            let snapshot = self.models.read().expect("Vercel model snapshot");
            (snapshot.entries.contains_key(id), snapshot.complete)
        };
        if !cached {
            return Err(SafeFailure(ProviderFailureClassification::new(if complete {
                pioneer_protocol::ProviderFailureClass::ModelNotFound
            } else {
                // A partial/unspecified catalog cannot establish absence.
                pioneer_protocol::ProviderFailureClass::Unknown
            }))
            .into());
        }
        self.validate_admission(id).await
    }
    async fn ensure_operation(&self, id: &str, embeddings: bool) -> Result<()> {
        self.ensure_model(id).await?;
        let supported = self
            .models
            .read()
            .expect("Vercel model snapshot")
            .entries
            .get(id)
            .and_then(|model| model.metadata.get("sourceMetadata"))
            .is_some_and(|raw| {
                if embeddings {
                    crate::catalog::generator::vercel_embedding_eligible(raw)
                } else {
                    crate::catalog::generator::vercel_chat_eligible(raw)
                }
            });
        if !supported {
            return Err(SafeFailure(ProviderFailureClassification::new(
                pioneer_protocol::ProviderFailureClass::UnsupportedCapability,
            ))
            .into());
        }
        Ok(())
    }
    async fn validate_admission(&self, id: &str) -> Result<()> {
        let blocked_account = self.account.read().await.as_deref() == Some("unavailable");
        let blocked_model = self
            .models
            .read()
            .expect("Vercel model snapshot")
            .entries
            .get(id)
            .is_some_and(|model| {
                model
                    .metadata
                    .get("sourceMetadata")
                    .is_some_and(|raw| http_ineligible(raw))
            });
        if blocked_account || blocked_model {
            return Err(SafeFailure(ProviderFailureClassification::new(
                pioneer_protocol::ProviderFailureClass::ProviderRejected,
            ))
            .into());
        }
        Ok(())
    }
    async fn scoped<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let models = self
            .models
            .read()
            .expect("Vercel model snapshot")
            .entries
            .clone();
        let tools = models
            .iter()
            .filter_map(|(id, model)| {
                model
                    .metadata
                    .get("toolCalling")
                    .and_then(Value::as_bool)
                    .map(|v| (id.clone(), v))
            })
            .collect();
        crate::tools::policy::with_discovery_tools(
            NAME,
            false,
            tools,
            crate::generation::with_native_models(NAME, self.root == BASE_URL, models, future),
        )
        .await
    }
    async fn send(&self, builder: reqwest::RequestBuilder) -> Result<reqwest::Response> {
        let response = crate::http::non_stream_request(builder.bearer_auth(&self.key), self.policy)
            .send()
            .await
            .map_err(|error| {
                let mut facts = ProviderFailureClassification::new(
                    pioneer_protocol::ProviderFailureClass::NetworkTransient,
                );
                facts.is_network_error = true;
                facts.http_status = error.status().map(|s| s.as_u16());
                SafeFailure(facts)
            })?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let retry = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse::<u64>().ok())
            .map(|v| v.saturating_mul(1000));
        let body =
            crate::http::read_response_text_bounded(response, 16 * 1024, "provider_error_body")
                .await
                .unwrap_or_default();
        let class = if matches!(status, 408 | 425) {
            pioneer_protocol::ProviderFailureClass::NetworkTransient
        } else {
            crate::failure::classify_provider_failure_class(
                &body.to_ascii_lowercase(),
                pioneer_protocol::ProviderFailureStage::Connect,
                Some(status),
                None,
            )
        };
        let mut facts = ProviderFailureClassification::new(class);
        facts.http_status = Some(status);
        facts.retry_after_ms = retry;
        Err(SafeFailure(facts).into())
    }
    async fn discovery(&self) -> Result<Value> {
        let response = self
            .send(
                self.client
                    .get(format!("{}/models?include_availability", self.root)),
            )
            .await?;
        crate::http::read_response_json_bounded(response, Default::default(), "provider_models")
            .await
            .map_err(safe_decode)
    }
    async fn discover_models(&self, embeddings: bool) -> Result<Vec<ProviderModelInfo>> {
        let value = self.discovery().await?;
        self.apply_snapshot(&value, embeddings).await
    }
    async fn apply_snapshot(
        &self,
        value: &Value,
        embeddings: bool,
    ) -> Result<Vec<ProviderModelInfo>> {
        let rows = value["data"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Vercel model catalog is malformed"))?;
        *self.account.write().await = account_status(&value).map(str::to_owned);
        let mut native = BTreeMap::new();
        let mut result = Vec::new();
        for raw in rows {
            let Some(mut model) = crate::catalog::generator::vercel_model(raw) else {
                continue;
            };
            model.base_url = self.root.clone();
            if let Some(source) = model
                .metadata
                .get_mut("pricingSource")
                .and_then(Value::as_object_mut)
            {
                source.remove("url");
                source.insert("source".into(), json!("authority_native_models"));
            }
            if raw["type"] == "embedding" {
                model.api = "openai-embeddings".into();
            }
            model
                .metadata
                .insert("pricingEvidenceVersion".into(), json!(1));
            model
                .metadata
                .insert("pricingUnits".into(), json!("USD_per_million_tokens"));
            model.metadata.insert(
                "pricingCapturedAt".into(),
                json!(chrono::Utc::now().to_rfc3339()),
            );
            let eligible = if embeddings {
                crate::catalog::generator::vercel_embedding_eligible(raw)
            } else {
                crate::catalog::generator::vercel_chat_eligible(raw)
            };
            if eligible {
                let mut info = model_info(raw, &model);
                // Eligibility was established by the existing operation-specific
                // native discovery above, including the actual Responses route.
                info.capabilities.chat = Some(!embeddings);
                if account_status(&value) == Some("unavailable") {
                    info.active = Some(false);
                }
                result.push(info);
            }
            native.insert(model.id.clone(), model);
        }
        // Only an explicit complete catalog establishes deletions. Missing or
        // future status values use the same conservative merge as partial.
        let mut snapshot = self.models.write().expect("Vercel model snapshot");
        snapshot.complete = value["catalog_status"] == "complete";
        if snapshot.complete {
            snapshot.entries = native;
        } else {
            // Replace whole present rows, including negative/unknown facts.
            snapshot.entries.extend(native);
        }
        Ok(result)
    }
}

fn account_status(value: &Value) -> Option<&str> {
    value["request_context_availability"]["http"]["status"]
        .as_str()
        .or_else(|| value["request_context_availability"]["http"].as_str())
}
fn http_ineligible(raw: &Value) -> bool {
    raw["model_eligibility"]["evaluated_runtime"] == "http"
        && raw["model_eligibility"]["status"] == "ineligible"
}

fn model_info(raw: &Value, model: &crate::catalog::CatalogModel) -> ProviderModelInfo {
    let rate = |key: &str| {
        raw["pricing"][key]
            .as_str()
            .and_then(|v| v.parse::<f64>().ok())
            .or_else(|| raw["pricing"][key].as_f64())
            .filter(|v| v.is_finite() && *v >= 0.)
    };
    let tiered = raw["pricing"]["varies_by_provider"] == true
        || raw["pricing"].as_object().is_some_and(|p| {
            p.keys()
                .any(|key| key.ends_with("_tiers") || key == "tiers")
        });
    let mut capabilities = ProviderModelCapabilities {
        tool_calling: crate::catalog::tool_support_from_marker_list(&raw["tags"], "tool-use"),
        embeddings: Some(raw["type"] == "embedding"),
        input_modalities: model.input_is_known().then(|| {
            model
                .input
                .iter()
                .filter(|kind| {
                    matches!(
                        kind.to_ascii_lowercase().as_str(),
                        "text" | "image" | "pdf" | "file" | "document"
                    )
                })
                .cloned()
                .collect()
        }),
        output_modalities: model
            .metadata
            .get("output")
            .filter(|_| {
                model
                    .metadata
                    .get("outputOrigin")
                    .is_some_and(|v| v["kind"] != "fallback")
            })
            .and_then(|v| serde_json::from_value(v.clone()).ok()),
        ..Default::default()
    };
    if raw["reasoning_options"].is_array() {
        let efforts = [
            crate::ReasoningEffort::None,
            crate::ReasoningEffort::Minimal,
            crate::ReasoningEffort::Low,
            crate::ReasoningEffort::Medium,
            crate::ReasoningEffort::High,
            crate::ReasoningEffort::XHigh,
            crate::ReasoningEffort::Max,
        ]
        .into_iter()
        .filter(|effort| {
            crate::generation::effort_supported_for_profile(NAME, &model.id, Some(model), *effort)
        })
        .map(|effort| effort.as_str().to_owned())
        .collect();
        capabilities.reasoning = Some(pioneer_protocol::ProviderModelReasoningCapabilities {
            supported: Some(model.reasoning),
            effort_options: efforts,
            supports_token_budget: Some(false),
            source: Some(pioneer_protocol::ReasoningCapabilitySource::ProviderMetadata),
            ..Default::default()
        });
    }
    capabilities.thinking = model.reasoning.then_some(true);
    capabilities.vision = match model.input_capability(crate::InputContentType::Image) {
        crate::catalog::InputCapabilityState::Supported => Some(true),
        crate::catalog::InputCapabilityState::Unsupported => Some(false),
        crate::catalog::InputCapabilityState::Unknown => None,
    };
    ProviderModelInfo {
        id: model.id.clone(),
        name: Some(model.name.clone()),
        description: raw["description"].as_str().map(str::to_owned),
        created: raw["created"].as_i64(),
        provider: NAME.into(),
        owned_by: raw["owned_by"].as_str().map(str::to_owned),
        limits: ProviderModelLimits {
            context_window: raw["context_window"].as_u64().filter(|v| *v > 0),
            max_output_tokens: raw["max_tokens"].as_u64().filter(|v| *v > 0),
            max_input_tokens: (raw["type"] == "embedding")
                .then(|| raw["context_window"].as_u64().filter(|v| *v > 0))
                .flatten(),
        },
        capabilities,
        transcription: None,
        pricing: (!tiered && (rate("input").is_some() || rate("output").is_some())).then(|| {
            ProviderModelPricing {
                input_token: rate("input"),
                output_token: rate("output"),
                ..Default::default()
            }
        }),
        active: http_ineligible(raw).then_some(false),
        family: None,
        lifecycle_status: None,
    }
}

#[async_trait]
impl Provider for VercelAiGatewayProvider {
    fn name(&self) -> &str {
        NAME
    }
    fn usage_api(&self) -> &'static str {
        self.adapter.usage_api()
    }
    fn usage_route(&self) -> Option<String> {
        self.adapter.usage_route()
    }
    fn authority_fingerprint(&self) -> Option<&str> {
        self.adapter.authority_fingerprint()
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            embeddings: true,
            ..self.adapter.capabilities()
        }
    }
    async fn prepare_model(&self, id: &str) -> Result<()> {
        self.ensure_model(id).await
    }
    async fn model_limits(&self, id: &str) -> Result<crate::catalog::CatalogLimits> {
        self.ensure_model(id).await?;
        let model = self.model_catalog_entry(id).ok_or_else(|| {
            SafeFailure(ProviderFailureClassification::new(
                pioneer_protocol::ProviderFailureClass::ModelNotFound,
            ))
        })?;
        let source = |field: &str| {
            model
                .metadata
                .get("vercelLimitOrigins")
                .is_some_and(|v| v[field]["kind"] == "source")
        };
        Ok(crate::catalog::CatalogLimits {
            context_window: model.context_window,
            context_origin: if source("contextWindow") {
                crate::catalog::OriginKind::Source
            } else {
                crate::catalog::OriginKind::Fallback
            },
            max_output: source("maxTokens").then_some(model.max_tokens),
            max_input: None,
        })
    }
    fn model_catalog_entry(&self, id: &str) -> Option<crate::catalog::CatalogModel> {
        self.models
            .read()
            .expect("Vercel model snapshot")
            .entries
            .get(id)
            .cloned()
    }
    fn model_tool_calling(&self, model: &str) -> bool {
        if let Ok(models) = self.models.try_read() {
            if let Some(model) = models.entries.get(model) {
                return model
                    .metadata
                    .get("sourceMetadata")
                    .is_some_and(crate::catalog::generator::vercel_chat_eligible)
                    && model.metadata.get("toolCalling").and_then(Value::as_bool) == Some(true);
            }
        }
        self.root == BASE_URL && self.adapter.model_tool_calling(model)
    }
    fn classify_failure(&self, error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        error
            .downcast_ref::<SafeFailure>()
            .map(|e| e.0.clone())
            .or_else(|| self.adapter.classify_failure(error))
    }
    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> Result<crate::attachments::PreparedInputBudget> {
        self.ensure_operation(&request.model, false).await?;
        self.scoped(self.adapter.prepare_input_budget(request))
            .await
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        self.ensure_operation(&request.model, false).await?;
        self.scoped(self.adapter.chat(request)).await
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        Ok(self.stream_chat_with_diagnostics(request).await?.stream)
    }
    async fn stream_chat_with_diagnostics(&self, request: ChatRequest) -> Result<ProviderStream> {
        self.ensure_operation(&request.model, false).await?;
        self.scoped(self.adapter.stream_chat_with_diagnostics(request))
            .await
    }
    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.discover_models(false).await
    }
    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.discover_models(true).await
    }
    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        let value = match self.discovery().await {
            Ok(value) => value,
            Err(error)
                if self.root != BASE_URL
                    && self.classify_failure(&error).is_some_and(|facts| {
                        matches!(facts.http_status, Some(404 | 405 | 501))
                    }) =>
            {
                return Ok(ProviderWarmupOutcome::NotSupported);
            }
            Err(error) => return Err(error),
        };
        self.apply_snapshot(&value, false).await?;
        // No annotation is not authentication evidence, even with HTTP 200.
        match account_status(&value) {
            Some("available") => Ok(ProviderWarmupOutcome::Completed),
            Some("unavailable") => {
                let facts = ProviderFailureClassification::new(
                    pioneer_protocol::ProviderFailureClass::ProviderRejected,
                );
                Err(SafeFailure(facts).into())
            }
            _ => Ok(ProviderWarmupOutcome::NotSupported),
        }
    }
    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        super::embedding::validate_input(&request.model, &request.input)?;
        super::embedding::EmbeddingBatchLimits::for_model(NAME, &request.model, None, None, None)?
            .validate_request(&request.input)?;
        self.ensure_operation(&request.model, true).await?;
        let entry = self.model_catalog_entry(&request.model);
        let model_id = request.model.clone();
        let count = request.input.len();
        let body = json!({"model":request.model,"input":request.input,"encoding_format":"float"});
        let response = self
            .send(
                self.client
                    .post(format!("{}/embeddings", self.root))
                    .json(&body),
            )
            .await?;
        let request_id = crate::usage::native_id(
            response
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
        );
        let parsed: EmbeddingWireResponse = crate::http::read_response_json_bounded(
            response,
            Default::default(),
            "provider_response",
        )
        .await
        .map_err(safe_decode)?;
        let reported_cost = parsed.provider_metadata["gateway"]["cost"]
            .as_str()
            .and_then(|value| value.parse::<f64>().ok())
            .or_else(|| parsed.provider_metadata["gateway"]["cost"].as_f64())
            .filter(|value| value.is_finite() && *value >= 0.0);
        let mut result = parsed.indexed.into_response(count).map_err(safe_decode)?;
        if reported_cost.is_some() && result.usage.is_none() {
            result.usage = Some(crate::TokenUsage {
                output_tokens: Some(0),
                ..Default::default()
            });
        }
        if let Some(usage) = &mut result.usage {
            *usage = usage.clone().with_reported_model(parsed.model.as_deref());
            usage.request_id = request_id;
            usage.api = Some("embeddings".into());
            usage.route = crate::usage::route(&self.root, "/embeddings");
            let mut accounting =
                crate::usage::accounting_with_entry(usage, NAME, &model_id, None, entry.as_ref());
            if let Some(amount) = reported_cost {
                accounting["reported_cost"] = json!({"amount":amount,"currency":"USD","provenance":"provider_metadata.gateway.cost"});
            }
            usage.accounting = Some(accounting);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Synthetic protocol fixtures, not recorded network responses.
    async fn server(
        replies: Vec<(u16, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        server_frames(replies.into_iter().map(|(status, body)| {
            format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
        }).collect()).await
    }

    async fn server_frames(replies: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let size = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + size {
                            break;
                        }
                    }
                }
                requests.push(String::from_utf8(bytes).unwrap());
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
            requests
        });
        (root, task)
    }

    #[tokio::test]
    async fn body_transport_decode_and_size_failures_are_distinct_and_redacted() {
        use pioneer_protocol::ProviderFailureClass as C;
        // A declared length larger than the sent payload makes HTTP 200 fail
        // during body reading, rather than during JSON decoding.
        let truncated = "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{\"secret-body\":\"fixture-key".to_owned();
        let malformed =
            "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\nsecret-body"
                .to_owned();
        let oversized =
            "HTTP/1.1 200 OK\r\nContent-Length: 1000000000\r\nConnection: close\r\n\r\n".to_owned();
        for embeddings in [false, true] {
            let (root, task) = server_frames(vec![
                truncated.clone(),
                malformed.clone(),
                oversized.clone(),
            ])
            .await;
            let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root.clone());
            // Cache authority-owned embedding facts without an extra HTTP
            // response, so every fixture below exercises its intended route.
            if embeddings {
                provider
                    .apply_snapshot(
                        &json!({"catalog_status":"complete","data":[{
                            "id":"private/embed","type":"embedding",
                            "modalities":{"input":["text"],"output":["embedding"]}
                        }]}),
                        true,
                    )
                    .await
                    .unwrap();
            }
            for expected in [Some(C::NetworkTransient), Some(C::ProviderRejected), None] {
                let error = if embeddings {
                    provider
                        .embed(EmbeddingRequest::new("private/embed", vec!["one".into()]))
                        .await
                        .err()
                        .unwrap()
                } else {
                    provider.list_models().await.err().unwrap()
                };
                if let Some(expected) = expected {
                    let facts = provider.classify_failure(&error).unwrap();
                    assert_eq!(facts.class, expected);
                    assert_eq!(facts.is_network_error, expected == C::NetworkTransient);
                } else {
                    assert!(
                        error
                            .downcast_ref::<crate::ProviderResponseTooLarge>()
                            .is_some()
                    );
                }
                let diagnostic = format!("{error:?}");
                for secret in ["fixture-key", "secret-body", root.as_str()] {
                    assert!(!diagnostic.contains(secret));
                }
            }
            let requests = task.await.unwrap();
            let route = if embeddings {
                "POST /v1/embeddings "
            } else {
                "GET /v1/models?include_availability "
            };
            assert!(requests.iter().all(|request| request.starts_with(route)));
        }
    }

    #[tokio::test]
    async fn annotated_snapshots_merge_partial_replace_complete_and_refresh_admission() {
        use pioneer_protocol::ProviderFailureClass as C;
        let (root, task) = server(vec![
            (200, r#"{"catalog_status":"complete","data":[{"id":"private/agent","type":"language","tags":["tool-use"],"context_window":32000,"modalities":{"input":["text"],"output":["text"]},"model_eligibility":{"evaluated_runtime":"http","status":"ineligible","category":"transient"}}]}"#),
            (200, r#"{"catalog_status":"partial","data":[],"request_context_availability":{"http":{"status":"available"}}}"#),
            (200, r#"{"catalog_status":"complete","data":[{"id":"private/agent","type":"language","tags":["tool-use"],"context_window":64000,"modalities":{"input":["text"],"output":["text"]},"model_eligibility":{"evaluated_runtime":"http","status":"eligible"}}],"request_context_availability":{"http":{"status":"available"}}}"#),
            (200, r#"{"catalog_status":"partial","data":[],"request_context_availability":{"http":{"status":"available"}}}"#),
            (200, r#"{"catalog_status":"partial","data":[{"id":"private/agent","type":"language","tags":[],"context_window":16000,"modalities":{"input":["text"],"output":["text"]},"model_eligibility":{"evaluated_runtime":"http","status":"ineligible"}}],"request_context_availability":{"http":{"status":"available"}}}"#),
            (200, r#"{"catalog_status":"complete","data":[]}"#),
            (200, r#"{"catalog_status":"complete","data":[]}"#),
        ]).await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        provider.list_models().await.unwrap();
        assert!(
            provider
                .ensure_operation("private/agent", false)
                .await
                .is_err()
        );
        provider.warmup().await.unwrap();
        // Partial omission preserves a real negative too, not only positives.
        assert!(
            provider
                .ensure_operation("private/agent", false)
                .await
                .is_err()
        );
        assert_eq!(
            provider.warmup().await.unwrap(),
            ProviderWarmupOutcome::Completed
        );
        assert!(
            provider
                .ensure_operation("private/agent", false)
                .await
                .is_ok()
        );
        assert_eq!(
            provider
                .model_limits("private/agent")
                .await
                .unwrap()
                .context_window,
            64000
        );
        provider.warmup().await.unwrap();
        assert!(
            provider
                .ensure_operation("private/agent", false)
                .await
                .is_ok()
        );
        provider.warmup().await.unwrap();
        assert!(
            provider
                .ensure_operation("private/agent", false)
                .await
                .is_err()
        );
        assert!(!provider.model_tool_calling("private/agent"));
        assert_eq!(
            provider
                .model_catalog_entry("private/agent")
                .unwrap()
                .context_window,
            16000
        );
        provider.warmup().await.unwrap();
        assert!(provider.model_catalog_entry("private/agent").is_none());
        let error = provider.model_limits("private/agent").await.err().unwrap();
        assert_eq!(
            provider.classify_failure(&error).unwrap().class,
            C::ModelNotFound
        );
        assert_eq!(task.await.unwrap().len(), 7);
    }

    #[tokio::test]
    async fn absent_model_in_partial_or_unspecified_catalog_remains_unknown() {
        for body in [
            r#"{"catalog_status":"partial","data":[]}"#,
            r#"{"data":[]}"#,
        ] {
            let (root, task) = server(vec![(200, body)]).await;
            let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
            let error = provider
                .model_limits("private/unresolved")
                .await
                .err()
                .unwrap();
            assert_eq!(
                provider.classify_failure(&error).unwrap().class,
                pioneer_protocol::ProviderFailureClass::Unknown
            );
            assert!(provider.model_catalog_entry("private/unresolved").is_none());
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn discovery_preserves_partial_positive_unknown_and_negative_vision() {
        let (root, task) = server(vec![(200, r#"{"data":[
            {"id":"private/vision","type":"language","tags":["tool-use","vision"],"modalities":{"output":["text"]}},
            {"id":"private/unknown","type":"language","tags":["tool-use"],"modalities":{"output":["text"]}},
            {"id":"private/text","type":"language","tags":["tool-use"],"modalities":{"input":["text"],"output":["text"]}}
        ]}"#)]).await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        let models = provider.list_models().await.unwrap();
        assert_eq!(models[0].capabilities.vision, Some(true));
        assert_eq!(models[0].capabilities.input_modalities, None);
        assert_eq!(models[1].capabilities.vision, None);
        assert_eq!(models[1].capabilities.input_modalities, None);
        assert_eq!(models[2].capabilities.vision, Some(false));
        assert_eq!(
            models[2].capabilities.input_modalities,
            Some(vec!["text".into()])
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn embeddings_preserve_full_id_float_wire_and_indexed_usage() {
        let (root, task) = server(vec![
            (200, r#"{"data":[{"id":"owner/private-model","type":"embedding","context_window":8192,"modalities":{"input":["text"],"output":["embedding"]},"pricing":{"input":"0.000002"}}]}"#),
            (200, r#"{"data":[{"index":0,"embedding":[1.0]}],"model":"owner/private-model","usage":{"prompt_tokens":7,"total_tokens":7},"providerMetadata":{"gateway":{"cost":"0.000014","routing":{"private_host":"must not persist"}}}}"#)
        ]).await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        let response = provider
            .embed(EmbeddingRequest::new(
                "owner/private-model",
                vec!["one".into()],
            ))
            .await
            .unwrap();
        assert_eq!(response.embeddings, vec![vec![1.]]);
        let usage = response.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(7));
        assert_eq!(usage.reported_total_tokens, Some(7));
        let accounting = usage.accounting.unwrap();
        assert_eq!(accounting["pricing"]["cost"]["input"], 2.0);
        assert_eq!(accounting["pricing_eligibility"]["eligible"], true);
        assert_eq!(accounting["reported_cost"]["amount"], 0.000014);
        assert_eq!(
            accounting["reported_cost"]["provenance"],
            "provider_metadata.gateway.cost"
        );
        assert!(!accounting.to_string().contains("private_host"));
        let requests = task.await.unwrap();
        assert!(requests[0].starts_with("GET /v1/models?include_availability HTTP/1.1"));
        assert!(requests[1].starts_with("POST /v1/embeddings HTTP/1.1"));
        assert!(
            requests[0]
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key\r\n")
        );
        let body: Value =
            serde_json::from_str(requests[1].split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(
            body,
            json!({"model":"owner/private-model","input":["one"],"encoding_format":"float"})
        );
    }

    #[tokio::test]
    async fn readiness_requires_account_annotation_and_sanitizes_denial() {
        let (root, task) = server(vec![
            (200, r#"{"data":[]}"#),
            (
                200,
                r#"{"data":[],"request_context_availability":{"http":{"status":"unknown"}}}"#,
            ),
            (
                200,
                r#"{"data":[],"request_context_availability":{"http":{"status":"available"}}}"#,
            ),
            (405, r#"{}"#),
            (
                401,
                r#"{"error":{"message":"secret-body fixture-key private-host"}}"#,
            ),
        ])
        .await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        assert_eq!(
            provider.warmup().await.unwrap(),
            ProviderWarmupOutcome::NotSupported
        );
        assert_eq!(
            provider.warmup().await.unwrap(),
            ProviderWarmupOutcome::NotSupported
        );
        assert_eq!(
            provider.warmup().await.unwrap(),
            ProviderWarmupOutcome::Completed
        );
        assert_eq!(
            provider.warmup().await.unwrap(),
            ProviderWarmupOutcome::NotSupported
        );
        let error = provider.warmup().await.unwrap_err();
        assert_eq!(
            provider.classify_failure(&error).unwrap().http_status,
            Some(401)
        );
        assert!(!format!("{error:?}").contains("secret-body"));
        assert!(!format!("{error:?}").contains("fixture-key"));
        for request in task.await.unwrap() {
            assert!(request.starts_with("GET /v1/models?include_availability HTTP/1.1"));
        }
    }

    #[tokio::test]
    async fn private_discovery_facts_drive_actual_responses_request() {
        let (root, task) = server(vec![
            (200, r#"{"data":[{"id":"private/agent","name":"Private agent","type":"language","tags":["tool-use"],"modalities":{"input":["text"],"output":["text"]},"context_window":16000,"max_tokens":1000,"temperature":true,"pricing":{"input":"0.000002"},"model_eligibility":{"status":"eligible","evaluated_runtime":"http"}}]}"#),
            (200, r#"{"id":"response-fixture","status":"completed","output":[{"id":"message-fixture","type":"message","role":"assistant","content":[{"type":"output_text","text":"private reply"}]}]}"#),
        ]).await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        // Persisted private ID after restart: no preceding model listing.
        provider.prepare_model("private/agent").await.unwrap();
        assert!(provider.model_tool_calling("private/agent"));
        let limits = provider.model_limits("private/agent").await.unwrap();
        assert_eq!(limits.context_window, 16000);
        assert_eq!(limits.context_origin, crate::catalog::OriginKind::Source);
        assert_eq!(limits.max_output, Some(1000));
        let entry = provider.model_catalog_entry("private/agent").unwrap();
        assert_eq!(entry.cost["input"], 2.0);
        assert_eq!(entry.base_url, provider.root);
        assert_eq!(entry.metadata["pricingUnits"], "USD_per_million_tokens");
        let response = crate::attachments::runtime::with_async_authority_scope(
            provider.authority_fingerprint().unwrap().to_owned(),
            provider.chat(ChatRequest {
                model: "private/agent".into(),
                messages: vec![crate::ChatMessage::user("hello")],
                temperature: Some(0.5),
                max_tokens: Some(200),
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.text, "private reply");
        let requests = task.await.unwrap();
        assert!(requests[1].starts_with("POST /v1/responses HTTP/1.1"));
        let body: Value =
            serde_json::from_str(requests[1].split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body["model"], "private/agent");
        assert_eq!(body["max_output_tokens"], 200);
        assert_eq!(body["temperature"], 0.5);
        assert_eq!(body["store"], false);
        assert!(body.get("reasoning").is_none());
    }

    #[tokio::test]
    async fn http_failures_and_embedding_bounds_stay_sanitized() {
        use pioneer_protocol::ProviderFailureClass as C;
        let (root, task) = server(vec![
            (403, "sensitive-body"),
            (429, "sensitive-body"),
            (503, "sensitive-body"),
            (408, "sensitive-body"),
        ])
        .await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        // Existing generic embedding consumer splits unknown model profiles
        // into singleton batches; direct calls enforce that same boundary.
        let error = provider
            .embed(EmbeddingRequest::new(
                "owner/model",
                vec!["a".into(), "b".into()],
            ))
            .await
            .unwrap_err();
        assert!(provider.classify_failure(&error).is_none());
        for (status, class) in [
            (403, C::AuthOrPermission),
            (429, C::RateLimit),
            (503, C::Provider5xx),
            (408, C::NetworkTransient),
        ] {
            let error = provider.warmup().await.unwrap_err();
            let facts = provider.classify_failure(&error).unwrap();
            assert_eq!(facts.http_status, Some(status));
            assert_eq!(facts.class, class);
            assert!(!format!("{error:?}").contains("sensitive-body"));
        }
        assert_eq!(task.await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn annotations_separate_http_eligibility_account_and_unknown() {
        let (root, task) = server(vec![(200, r#"{"data":[{"id":"private/blocked","type":"language","tags":["tool-use"],"modalities":{"input":["text"],"output":["text"]},"model_eligibility":{"status":"ineligible","evaluated_runtime":"http"}},{"id":"private/unknown","type":"language","tags":["tool-use"],"modalities":{"input":["text"],"output":["text"]},"model_eligibility":{"status":"unknown","evaluated_runtime":"http"}}],"request_context_availability":{"http":{"status":"unknown"}}}"#)]).await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        let models = provider.list_models().await.unwrap();
        assert_eq!(models[0].active, Some(false));
        assert_eq!(models[1].active, None);
        assert!(
            provider
                .validate_admission("private/blocked")
                .await
                .is_err()
        );
        assert!(provider.validate_admission("private/unknown").await.is_ok());
        assert!(models[1].pricing.is_none());
        let limits = provider.model_limits("private/unknown").await.unwrap();
        assert_eq!(limits.context_window, 4096);
        assert_eq!(limits.context_origin, crate::catalog::OriginKind::Fallback);
        assert_eq!(limits.max_output, None);
        assert!(!http_ineligible(
            &json!({"model_eligibility":{"status":"ineligible","evaluated_runtime":"realtime_websocket"}})
        ));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn operation_admission_rejects_cross_route_ids_and_preserves_embedding_input_limit() {
        let (root, task) = server(vec![(200, r#"{"data":[{"id":"private/embed","type":"embedding","context_window":512,"modalities":{"input":["text"],"output":["embedding"]}},{"id":"private/language-batch","type":"language","tags":["tool-use"],"modalities":{"input":["text"],"output":["text"]}},{"id":"private/language","type":"language","tags":["tool-use"],"modalities":{"input":["text"],"output":["text"]}}]}"#)]).await;
        let provider = VercelAiGatewayProvider::with_base_url("fixture-key", root);
        let embeddings = provider.list_embedding_models().await.unwrap();
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].limits.max_input_tokens, Some(512));
        assert!(!provider.model_tool_calling("private/language-batch"));
        for id in ["private/embed", "private/language-batch"] {
            let error = provider
                .prepare_input_budget(ChatRequest {
                    model: id.into(),
                    messages: vec![crate::ChatMessage::user("hello")],
                    temperature: None,
                    max_tokens: None,
                    tools: None,
                    tool_choice: None,
                    parallel_tool_calls: None,
                    reasoning: None,
                    compiled_prompt: None,
                })
                .await
                .err()
                .unwrap();
            assert_eq!(
                provider.classify_failure(&error).unwrap().class,
                pioneer_protocol::ProviderFailureClass::UnsupportedCapability
            );
        }
        let error = provider
            .embed(EmbeddingRequest::new(
                "private/language",
                vec!["hello".into()],
            ))
            .await
            .unwrap_err();
        assert_eq!(
            provider.classify_failure(&error).unwrap().class,
            pioneer_protocol::ProviderFailureClass::UnsupportedCapability
        );
        assert_eq!(task.await.unwrap().len(), 1);
    }

    #[test]
    fn registration_selects_responses_and_connection_authorities_are_isolated() {
        for name in [
            NAME,
            "vercel",
            "vercel_ai_gateway",
            "vercel-gateway",
            "Vercel AI Gateway",
            "vercelaigateway",
        ] {
            let provider = crate::factory::create_provider(name, "fixture-key").unwrap();
            assert_eq!(provider.name(), NAME);
            assert_eq!(provider.usage_api(), "responses");
            assert!(provider.capabilities().embeddings);
            assert!(!provider.capabilities().transcription);
        }
        let a = VercelAiGatewayProvider::new("a");
        let b = VercelAiGatewayProvider::new("b");
        let c = VercelAiGatewayProvider::with_base_url("a", "https://example.invalid/v1");
        assert_ne!(a.authority_fingerprint(), b.authority_fingerprint());
        assert_ne!(a.authority_fingerprint(), c.authority_fingerprint());
        assert_eq!(
            crate::definition::provider_definition(NAME)
                .unwrap()
                .default_base_url,
            Some(BASE_URL)
        );
    }
}
