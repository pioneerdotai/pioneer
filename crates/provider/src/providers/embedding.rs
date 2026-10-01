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

impl From<ApiEmbeddingUsage> for TokenUsage {
    fn from(usage: ApiEmbeddingUsage) -> Self {
        Self {
            input_tokens: usage.prompt_tokens,
            // Embeddings have no generated output tokens.
            output_tokens: Some(0),
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
