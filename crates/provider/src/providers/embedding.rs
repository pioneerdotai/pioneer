//! Validation shared by the implemented indexed embeddings APIs.
use anyhow::{Result, bail};
use serde::Deserialize;

use crate::types::TokenUsage;

#[derive(Debug, Deserialize)]
pub(super) struct ApiEmbeddingData {
    pub embedding: Vec<f32>,
    pub index: usize,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub(super) struct ApiEmbeddingUsage(crate::usage::ChatUsage);

/// Actual indexed float response schema shared by both implemented endpoints.
#[derive(Debug, Deserialize)]
pub struct IndexedEmbeddingResponse {
    pub(super) data: Vec<ApiEmbeddingData>,
    #[serde(default)]
    pub(super) usage: Option<ApiEmbeddingUsage>,
}
impl IndexedEmbeddingResponse {
    pub fn into_response(self, expected_count: usize) -> Result<crate::types::EmbeddingResponse> {
        Ok(crate::types::EmbeddingResponse {
            embeddings: ordered_vectors(self.data, expected_count)?,
            usage: self.usage.map(Into::into),
        })
    }
}

impl From<ApiEmbeddingUsage> for TokenUsage {
    fn from(usage: ApiEmbeddingUsage) -> Self {
        let mut usage = usage.0.normalized();
        // Embeddings have no generated output or reasoning tokens. Preserve
        // reported totals/cache evidence and the shared bounded metric record.
        usage.output_tokens = Some(0);
        usage.reasoning_tokens = None;
        usage
    }
}

pub(super) fn validate_input(model: &str, input: &[String]) -> Result<()> {
    if model.trim().is_empty() || input.is_empty() || input.iter().any(String::is_empty) {
        bail!("embedding request requires a model and nonempty inputs");
    }
    Ok(())
}

/// Embedding-only HTTP classification. Status and structured non-size codes
/// outrank text; a negative size decision never returns to the legacy classifier
/// with the same misleading body. Callers discard the bounded body afterwards.
pub(super) fn classify_input_failure(
    status: u16,
    body: &str,
) -> pioneer_protocol::ProviderFailureClass {
    use pioneer_protocol::ProviderFailureClass as C;
    match status {
        401 => return C::AuthExpired,
        403 => return C::AuthOrPermission,
        429 => return C::RateLimit,
        500..=599 => return C::Provider5xx,
        408 | 425 => return C::NetworkTransient,
        413 => return C::ContextTooLarge,
        400 => {}
        _ => {
            return crate::failure::classify_provider_failure_class(
                "",
                pioneer_protocol::ProviderFailureStage::Connect,
                Some(status),
                None,
            );
        }
    }
    let value = serde_json::from_str::<serde_json::Value>(body).ok();
    let mut structured_overflow = false;
    for code in ["/error/code", "/error/type"]
        .into_iter()
        .filter_map(|path| {
            value
                .as_ref()
                .and_then(|value| value.pointer(path))
                .filter(|code| !code.is_null())
        })
    {
        let code = code
            .as_str()
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| code.to_string());
        match code.as_str() {
            "context_length_exceeded" | "max_tokens_per_request" | "max_tokens_per_input" => {
                // Check both fields before admitting size recovery: a specific
                // non-size code/type vetoes even conflicting size metadata.
                structured_overflow = true;
            }
            "invalid_api_key"
            | "authentication_error"
            | "unauthorized"
            | "token_expired"
            | "401" => return C::AuthExpired,
            "permission_denied" | "forbidden" | "insufficient_permissions" | "403" => {
                return C::AuthOrPermission;
            }
            "rate_limit_exceeded" | "rate_limit_error" | "insufficient_quota" | "429" => {
                return C::RateLimit;
            }
            // These are generic bad-request envelopes, not specific validation
            // evidence. An explicit size message may refine only this category.
            "" | "400" | "invalid_request_error" | "invalid_request" | "bad_request" => {}
            _ => return C::ProviderRejected,
        }
    }
    if structured_overflow {
        return C::ContextTooLarge;
    }
    let lower = body.to_ascii_lowercase();
    let class = crate::failure::classify_provider_failure_class(
        &lower,
        pioneer_protocol::ProviderFailureStage::Connect,
        Some(status),
        None,
    );
    let context_overflow = lower.contains("prompt too long")
        || ((lower.contains("context length") || lower.contains("context window"))
            && (lower.contains("exceed")
                || lower.contains("too long")
                || lower.contains("is full")
                || (lower.contains("maximum context")
                    && lower.contains("requested")
                    && lower.contains("tokens"))));
    if context_overflow
        || (matches!(class, C::ProviderRejected | C::Unknown)
            && lower.contains("tokens per request")
            && (lower.contains("max") || lower.contains("limit") || lower.contains("exceed")))
    {
        C::ContextTooLarge
    } else if class == C::ContextTooLarge {
        // Mentioning a context parameter/capability alone is not size evidence.
        C::ProviderRejected
    } else {
        class
    }
}

pub(super) fn ordered_vectors(
    mut data: Vec<ApiEmbeddingData>,
    expected_count: usize,
) -> Result<Vec<Vec<f32>>> {
    if data.len() != expected_count {
        bail!(
            "embedding response returned {} vectors for {expected_count} inputs",
            data.len()
        );
    }
    data.sort_by_key(|item| item.index);
    let dimension = data.first().map_or(0, |item| item.embedding.len());
    for (expected_index, item) in data.iter().enumerate() {
        // With an exact count, this also proves uniqueness, range and no gaps.
        if item.index != expected_index {
            bail!("embedding response indices must be a permutation of 0..{expected_count}");
        }
        if dimension == 0 || item.embedding.len() != dimension {
            bail!("embedding response has empty or inconsistent vector dimensions");
        }
        if item.embedding.iter().any(|value| !value.is_finite()) {
            bail!("embedding response contains non-finite values");
        }
    }
    Ok(data.into_iter().map(|item| item.embedding).collect())
}

// One protocol matrix shared by real adapters, including each adapter's error
// mapping and safe public classifier. No production-only injection seam.
#[cfg(test)]
pub(super) const HTTP_FAILURE_CASES: &[(u16, &str, pioneer_protocol::ProviderFailureClass)] = &[
    (
        401,
        r#"{"error":{"message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::AuthExpired,
    ),
    (
        429,
        r#"{"error":{"message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::RateLimit,
    ),
    (
        503,
        r#"{"error":{"message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::Provider5xx,
    ),
    (
        400,
        r#"{"error":{"code":"invalid_api_key","message":"max tokens per request limit"}}"#,
        pioneer_protocol::ProviderFailureClass::AuthExpired,
    ),
    (
        400,
        r#"{"error":{"code":"invalid_parameter","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::ProviderRejected,
    ),
    (
        400,
        r#"{"error":{"code":400,"type":"authentication_error","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::AuthExpired,
    ),
    (
        400,
        r#"{"error":{"code":"","type":"rate_limit_error","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::RateLimit,
    ),
    (
        400,
        r#"{"error":{"code":"invalid_request_error","type":"invalid_parameter","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::ProviderRejected,
    ),
    (
        400,
        r#"{"error":{"code":"context_length_exceeded","type":"authentication_error","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::AuthExpired,
    ),
    (
        400,
        r#"{"error":{"code":"context_length_exceeded","type":"invalid_parameter","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::ProviderRejected,
    ),
    (
        400,
        r#"{"error":{"code":"rate_limit_error","type":"context_length_exceeded","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::RateLimit,
    ),
    (
        400,
        r#"{"error":{"code":400,"type":"context_length_exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::ContextTooLarge,
    ),
    (
        400,
        r#"{"error":{"code":"invalid_request_error","type":"max_tokens_per_request"}}"#,
        pioneer_protocol::ProviderFailureClass::ContextTooLarge,
    ),
    (
        400,
        r#"{"error":{"code":"context_length_exceeded","type":"invalid_request_error"}}"#,
        pioneer_protocol::ProviderFailureClass::ContextTooLarge,
    ),
    (
        400,
        r#"{"error":{"code":"context_length_exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::ContextTooLarge,
    ),
    (
        400,
        r#"{"error":{"code":"invalid_request_error","message":"maximum context length exceeded"}}"#,
        pioneer_protocol::ProviderFailureClass::ContextTooLarge,
    ),
    (
        400,
        r#"{"error":{"message":"Requested 300001 tokens, max 300000 tokens per request"}}"#,
        pioneer_protocol::ProviderFailureClass::ContextTooLarge,
    ),
    (
        400,
        r#"{"error":{"message":"Unknown parameter: context window; unsupported request"}}"#,
        pioneer_protocol::ProviderFailureClass::ProviderRejected,
    ),
    (
        413,
        r#"{"error":{"message":"payload too large"}}"#,
        pioneer_protocol::ProviderFailureClass::ContextTooLarge,
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    fn item(index: usize, embedding: Vec<f32>) -> ApiEmbeddingData {
        ApiEmbeddingData { index, embedding }
    }

    #[test]
    fn indexed_embeddings_reorder_and_reject_corrupt_permutations() {
        assert_eq!(
            ordered_vectors(vec![item(1, vec![2.0]), item(0, vec![1.0])], 2).unwrap(),
            vec![vec![1.0], vec![2.0]]
        );
        for indices in [[0, 0], [1, 1], [0, 2], [1, 2], [0, usize::MAX]] {
            assert!(
                ordered_vectors(indices.into_iter().map(|i| item(i, vec![1.0])).collect(), 2)
                    .is_err()
            );
        }
        assert!(ordered_vectors(vec![item(0, vec![1.0])], 2).is_err());
        assert!(ordered_vectors(vec![item(0, vec![1.0]), item(1, vec![2.0])], 1).is_err());
    }

    #[test]
    fn indexed_embeddings_reject_dimension_and_nonfinite_corruption() {
        assert!(ordered_vectors(vec![item(0, vec![])], 1).is_err());
        assert!(ordered_vectors(vec![item(0, vec![1.0]), item(1, vec![1.0, 2.0])], 2).is_err());
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(ordered_vectors(vec![item(0, vec![bad])], 1).is_err());
        }
    }

    #[test]
    fn embedding_input_requires_model_and_nonempty_batch_items() {
        assert!(validate_input(" ", &["text".to_owned()]).is_err());
        assert!(validate_input("model", &[]).is_err());
        assert!(validate_input("model", &[String::new()]).is_err());
        assert!(validate_input("model", &["text".to_owned()]).is_ok());
    }

    #[tokio::test]
    async fn indexed_embedding_adapters_preserve_authoritative_failure_evidence() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for endpoint in ["openai", "openrouter"] {
            for &(status, body, expected) in HTTP_FAILURE_CASES {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let root = format!("http://{}/v1", listener.local_addr().unwrap());
                let server = tokio::spawn(async move {
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
                    socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                    String::from_utf8(bytes).unwrap()
                });
                let provider: Box<dyn crate::Provider> = if endpoint == "openai" {
                    Box::new(super::super::openai::OpenAiProvider::with_base_url(
                        "fixture", root,
                    ))
                } else {
                    Box::new(super::super::openrouter::OpenRouterProvider::with_base_url(
                        "fixture", root,
                    ))
                };
                let model = if endpoint == "openai" {
                    "text-embedding-3-small"
                } else {
                    "openai/text-embedding-3-small"
                };
                let error = provider
                    .embed(crate::EmbeddingRequest::new(model, vec!["one".into()]))
                    .await
                    .unwrap_err();
                let facts = provider.classify_failure(&error).unwrap();
                assert_eq!(facts.class, expected, "{endpoint} {status} {body}");
                assert_eq!(facts.http_status, Some(status));
                assert!(server.await.unwrap().starts_with("POST /v1/embeddings "));
            }
        }
    }

    #[test]
    fn embedding_usage_preserves_missing_input_as_unknown() {
        let usage: TokenUsage = serde_json::from_value::<ApiEmbeddingUsage>(serde_json::json!({}))
            .unwrap()
            .into();
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, Some(0));
        let reported: TokenUsage = serde_json::from_value::<ApiEmbeddingUsage>(serde_json::json!({
            "prompt_tokens":7,"total_tokens":7,"private_payload":"must not survive"
        }))
        .unwrap()
        .into();
        assert_eq!(reported.input_tokens, Some(7));
        assert_eq!(reported.reported_total_tokens, Some(7));
        assert!(reported.raw_usage.unwrap().get("private_payload").is_none());
    }
}

/// Endpoint policy, separate from the existing model catalog. Known routed
/// OpenAI identities inherit the upstream model contract; arbitrary Router
/// models do not. Source (2026-10-02):
/// https://developers.openai.com/api/reference/resources/embeddings/methods/create
/// https://openrouter.ai/docs/api/api-reference/embeddings/submit-an-embedding-request
#[derive(Debug, Clone, Copy)]
pub struct EmbeddingBatchLimits {
    pub dimension: Option<usize>,
    pub max_items: usize,
    pub max_input_tokens: Option<usize>,
    pub max_total_tokens: Option<usize>,
    pub max_response_bytes: usize,
}
/// Identity/dimension from the existing OpenAI catalog, without a second table.
pub fn known_embedding_model(provider: &str, model: &str) -> Option<(&'static str, usize)> {
    // Preserve the existing Router known-profile boundary (small/large). The
    // legacy ada route was custom and is not promoted based on a branded name.
    let id = match provider {
        "openai" => Some(model),
        "openrouter" => model
            .strip_prefix("openai/")
            .filter(|id| *id != "text-embedding-ada-002"),
        _ => None,
    }?;
    super::openai::embedding_model_definition(id).map(|model| (model.id, model.dimension))
}
/// Apply the same 15% uncertainty allowance to known remote input/aggregate
/// limits. Unknown models keep their existing singleton/unknown-limit policy.
/// Integer arithmetic rounds the allowed estimate down without overflow.
pub fn estimated_token_budget(limit: usize) -> usize {
    ((limit as u128 * 100) / 115) as usize
}

impl EmbeddingBatchLimits {
    pub fn for_model(
        provider: &str,
        model: &str,
        configured_dimension: Option<usize>,
        configured_input: Option<usize>,
        configured_items: Option<usize>,
    ) -> Result<Self> {
        let upstream = known_embedding_model(provider, model)
            .and_then(|(id, _)| super::openai::embedding_model_definition(id));
        let dimension = upstream.map(|m| m.dimension).or(configured_dimension);
        if configured_dimension == Some(0) {
            bail!("embedding dimension must be positive");
        }
        if let Some(model) = upstream
            && configured_dimension.is_some_and(|dim| dim != model.dimension)
        {
            bail!("embedding configured dimension differs from known model output");
        }
        let known = upstream.is_some();
        let result = Self {
            dimension,
            max_items: if known {
                configured_items.unwrap_or(2048).min(2048)
            } else {
                1
            },
            max_input_tokens: known.then(|| configured_input.unwrap_or(8192).min(8192)),
            max_total_tokens: known.then_some(300_000),
            max_response_bytes: crate::types::ProviderResponseLimits::default().max_transport_bytes,
        };
        if result.max_items == 0 || result.max_input_tokens == Some(0) {
            bail!("embedding input budget is unusable");
        }
        Ok(result)
    }

    // Conservative local planning allowance, NOT a new provider model limit.
    // 64 bytes/value covers expanded finite f32 decimals and separators; 128
    // bytes/item covers index/object/array syntax. Reserve 64 KiB for envelope,
    // model/usage/routing metadata. Noncanonical/unbounded metadata or decimals
    // can still exceed this estimate: the unchanged bounded reader is authoritative.
    pub fn response_size_budget(&self, count: usize) -> Option<usize> {
        let dimension = self.dimension?;
        dimension
            .checked_mul(64)?
            .checked_add(128)?
            .checked_mul(count)?
            .checked_add(64 * 1024)
    }
    fn fits(&self, count: usize, tokens: usize) -> bool {
        count <= self.max_items
            && self
                .max_total_tokens
                .is_none_or(|cap| tokens <= estimated_token_budget(cap))
            && self.dimension.is_none_or(|_| {
                self.response_size_budget(count)
                    .is_some_and(|bytes| bytes <= self.max_response_bytes)
            })
    }
    fn count_input(&self, text: &str) -> Result<usize> {
        if text.is_empty() {
            bail!("embedding input must not be empty");
        }
        if let Some(cap) = self.max_input_tokens {
            // Estimate the actual decorated string. This is admission policy,
            // not a guarantee about the remote model's tokenizer.
            let tokens = usize::try_from(pioneer_compaction::text_tokens(text))
                .map_err(|_| anyhow::anyhow!("embedding estimate exceeds addressable budget"))?;
            if tokens > estimated_token_budget(cap) {
                bail!("embedding input exceeds model token budget");
            }
            Ok(tokens)
        } else {
            // Unknown custom tokenizer/aggregate contract: no invented accounting.
            // Singleton requests and known/configured dimension still bound output.
            Ok(0)
        }
    }
    pub fn plan(&self, inputs: &[String]) -> Result<Vec<std::ops::Range<usize>>> {
        let mut batches = Vec::new();
        let mut start = 0;
        let mut tokens = 0_usize;
        for (index, text) in inputs.iter().enumerate() {
            let cost = self.count_input(text)?;
            if !self.fits(1, cost) {
                bail!("one embedding cannot fit the bounded response/input budget");
            }
            let next = tokens
                .checked_add(cost)
                .ok_or_else(|| anyhow::anyhow!("embedding token budget overflow"))?;
            if index > start && !self.fits(index - start + 1, next) {
                batches.push(start..index);
                start = index;
                tokens = cost;
            } else {
                tokens = next;
            }
        }
        if start < inputs.len() {
            batches.push(start..inputs.len());
        }
        Ok(batches)
    }
    /// Direct adapters reject locally oversized estimates before network I/O.
    /// The gateway owns confirmed-overflow recovery; adapters never retry.
    pub fn validate_request(&self, inputs: &[String]) -> Result<()> {
        if inputs.is_empty() || self.plan(inputs)?.len() != 1 {
            bail!(
                "embedding batch exceeds endpoint or bounded response budget; split before sending"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    fn input(tokens: usize) -> String {
        "abcd".repeat(tokens)
    }
    #[test]
    fn embedding_batch_known_profiles_enforce_input_count_and_response_boundaries() {
        for (provider, model, dims) in [
            ("openai", "text-embedding-3-small", 1536),
            ("openai", "text-embedding-3-large", 3072),
            ("openai", "text-embedding-ada-002", 1536),
            ("openrouter", "openai/text-embedding-3-small", 1536),
            ("openrouter", "openai/text-embedding-3-large", 3072),
        ] {
            let limits =
                EmbeddingBatchLimits::for_model(provider, model, None, None, None).unwrap();
            assert_eq!(limits.dimension, Some(dims));
            assert!(limits.validate_request(&[input(7123)]).is_ok());
            assert!(limits.validate_request(&[input(7124)]).is_err());
            let max_size_count = (limits.max_response_bytes - 64 * 1024) / (dims * 64 + 128);
            let short = vec!["a".to_owned(); max_size_count];
            assert!(limits.validate_request(&short).is_ok());
            let mut excess = short;
            excess.push("a".into());
            assert!(limits.validate_request(&excess).is_err());
            assert_eq!(
                limits.plan(&excess).unwrap(),
                vec![0..max_size_count, max_size_count..max_size_count + 1]
            );
            // Isolate the endpoint's count boundary independently of the lower
            // production transport budget; both predicates are the same planner.
            let mut count_only = limits;
            count_only.max_response_bytes = usize::MAX;
            assert!(count_only.validate_request(&vec!["a".into(); 2048]).is_ok());
            assert!(
                count_only
                    .validate_request(&vec!["a".into(); 2049])
                    .is_err()
            );
        }
    }
    #[test]
    fn embedding_batch_aggregate_boundary_mixed_lengths_and_decorated_input() {
        let limits =
            EmbeddingBatchLimits::for_model("openai", "text-embedding-3-small", None, None, None)
                .unwrap();
        let capacity = 260_869; // floor(300000 / 1.15), independent policy boundary
        let full = capacity / 6000;
        let mut inputs = vec![input(6000); full];
        inputs.push(input(capacity % 6000));
        assert!(limits.validate_request(&inputs).is_ok());
        inputs.push(input(1));
        assert!(limits.validate_request(&inputs).is_err());
        assert_eq!(
            limits.plan(&inputs).unwrap(),
            vec![0..full + 1, full + 1..full + 2]
        );
        let inputs = vec![input(6000); 64];
        assert_eq!(limits.plan(&inputs).unwrap(), vec![0..full, full..64]);
        let raw = input(10);
        let decorated = format!("Instruct: retrieve\nQuery: {raw}");
        let mut local = limits;
        local.max_input_tokens = Some(12);
        assert!(local.validate_request(&[raw]).is_ok());
        assert!(local.validate_request(&[decorated]).is_err());
    }
    #[test]
    fn embedding_batch_unknown_router_is_singleton_without_invented_tokenizer() {
        let unknown = EmbeddingBatchLimits::for_model(
            "openrouter",
            "vendor/custom",
            Some(4096),
            Some(1024),
            Some(512),
        )
        .unwrap();
        assert_eq!(unknown.max_input_tokens, None);
        assert_eq!(unknown.max_total_tokens, None);
        assert_eq!(
            unknown.plan(&["a".into(), "b".into()]).unwrap(),
            vec![0..1, 1..2]
        );
        assert!(unknown.validate_request(&["a".into(), "b".into()]).is_err());
        assert!(unknown.validate_request(&["a".into()]).is_ok());
        let too_large = EmbeddingBatchLimits::for_model(
            "openrouter",
            "vendor/custom",
            Some(usize::MAX),
            None,
            None,
        )
        .unwrap();
        assert!(too_large.validate_request(&["a".into()]).is_err());
        assert!(
            EmbeddingBatchLimits::for_model(
                "openai",
                "text-embedding-3-large",
                Some(1536),
                None,
                None
            )
            .is_err()
        );
        assert_eq!(
            known_embedding_model("openrouter", "openai/text-embedding-ada-002"),
            None
        );
        let unknown_dimension =
            EmbeddingBatchLimits::for_model("openrouter", "vendor/custom", None, None, None)
                .unwrap();
        assert_eq!(unknown_dimension.dimension, None); // direct call: bounded reader, no invented dims
    }
}
