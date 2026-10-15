//! Validation shared only by the two implemented indexed embeddings APIs.
use anyhow::{Result, bail};
use serde::Deserialize;

use crate::types::TokenUsage;

#[derive(Debug, Deserialize)]
pub(super) struct ApiEmbeddingData {
    pub embedding: Vec<f32>,
    pub index: usize,
}

#[derive(Debug, Deserialize)]
pub(super) struct ApiEmbeddingUsage {
    pub prompt_tokens: Option<u64>,
}

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
        Self {
            input_tokens: usage.prompt_tokens,
            // Embeddings have no generated output tokens.
            output_tokens: Some(0),
            ..Default::default()
        }
    }
}

pub(super) fn validate_input(model: &str, input: &[String]) -> Result<()> {
    if model.trim().is_empty() || input.is_empty() || input.iter().any(String::is_empty) {
        bail!("embedding request requires a model and nonempty inputs");
    }
    Ok(())
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

    #[test]
    fn embedding_usage_preserves_missing_input_as_unknown() {
        let usage: TokenUsage = ApiEmbeddingUsage {
            prompt_tokens: None,
        }
        .into();
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, Some(0));
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
            && self.max_total_tokens.is_none_or(|cap| tokens <= cap)
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
            // All three known OpenAI embedding models use cl100k_base. Measure
            // the string actually sent (prefixes/instructions included), not raw text.
            let tokens = tiktoken_rs::cl100k_base_singleton()
                .encode_ordinary(text)
                .len();
            if tokens > cap {
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
    /// Direct adapters reject a request requiring splitting BEFORE network I/O.
    /// The gateway uses the same planner; there are no hidden retries/splits.
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
        let text = " a".repeat(tokens);
        assert_eq!(
            tiktoken_rs::cl100k_base_singleton()
                .encode_ordinary(&text)
                .len(),
            tokens
        );
        text
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
            assert!(limits.validate_request(&[input(8192)]).is_ok());
            assert!(limits.validate_request(&[input(8193)]).is_err());
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
        let mut inputs = vec![input(6000); 49];
        inputs.push(input(5999));
        inputs.push(input(1));
        assert!(limits.validate_request(&inputs).is_ok()); // exactly300000
        inputs.push(input(1));
        assert!(limits.validate_request(&inputs).is_err());
        assert_eq!(limits.plan(&inputs).unwrap(), vec![0..51, 51..52]);
        let inputs = vec![input(6000); 64];
        assert_eq!(limits.plan(&inputs).unwrap(), vec![0..50, 50..64]);
        let raw = input(10);
        let decorated = format!("Instruct: retrieve\nQuery: {raw}");
        let mut local = limits;
        local.max_input_tokens = Some(10);
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
