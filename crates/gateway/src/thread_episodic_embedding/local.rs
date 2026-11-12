//! Local ONNX inference for the existing embedding preparation pipeline.
//!
//! Preparation (including document/query prefixes and long-input pooling) stays
//! in the parent provider. Keep the installed models' previous inference contract:
//! right-pad/truncate to 512 tokens, first output's CLS vector, then L2 normalize.
use super::{LOCAL_PROVIDER_ID, LocalEmbeddingRuntime, LocalEmbeddingRuntimeFactory};
use anyhow::{Context, Result, bail};
use ort::{
    session::{Session, builder::GraphOptimizationLevel},
    value::Tensor,
};
use pioneer_memory::ThreadEpisodicEmbeddingError;
use pioneer_provider::providers::local_embedding_model_info;
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokenizers::{
    PaddingDirection, PaddingParams, PaddingStrategy, Tokenizer, TruncationDirection,
    TruncationParams, TruncationStrategy,
};

pub(super) struct LocalRuntimeConfig {
    pub model_name: String,
    pub models_dir: PathBuf,
}

pub(super) struct OnnxLocalEmbeddingRuntimeFactory;

impl LocalEmbeddingRuntimeFactory for OnnxLocalEmbeddingRuntimeFactory {
    fn create(
        &self,
        config: LocalRuntimeConfig,
    ) -> Result<Arc<dyn LocalEmbeddingRuntime>, ThreadEpisodicEmbeddingError> {
        let model = config.model_name.clone();
        OnnxLocalEmbeddingRuntime::load(config)
            .map(|runtime| Arc::new(runtime) as Arc<dyn LocalEmbeddingRuntime>)
            .map_err(|error| provider_error(&model, error))
    }
}

struct OnnxLocalEmbeddingRuntime {
    model: String,
    dimension: usize,
    tokenizer: Tokenizer,
    state: Mutex<InferenceState>,
}

struct InferenceState {
    session: Session,
    // Store exact prepared text, avoiding hash-collision reuse. Existing provider
    // token budgets bound each key; entry count is bounded independently.
    cache: VecDeque<(String, Vec<f32>)>,
}

fn provider_error(model: &str, error: impl std::fmt::Display) -> ThreadEpisodicEmbeddingError {
    ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
        LOCAL_PROVIDER_ID,
        model,
        format!("local embedding failed: {error}"),
    )
}

impl OnnxLocalEmbeddingRuntime {
    fn load(config: LocalRuntimeConfig) -> Result<Self> {
        let info = local_embedding_model_info(&config.model_name)
            .context("unsupported local embedding model")?;
        let mut tokenizer = Tokenizer::from_file(
            config
                .models_dir
                .join(format!("{}_tokenizer.json", info.id)),
        )
        .map_err(|error| anyhow::anyhow!("failed to load embedding tokenizer: {error}"))?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::Fixed(info.max_tokens),
            direction: PaddingDirection::Right,
            pad_to_multiple_of: None,
            pad_id: 0,
            pad_type_id: 0,
            pad_token: "[PAD]".to_owned(),
        }));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: info.max_tokens,
                strategy: TruncationStrategy::LongestFirst,
                stride: 0,
                direction: TruncationDirection::Right,
            }))
            .map_err(|error| anyhow::anyhow!("failed to configure embedding tokenizer: {error}"))?;
        let session = Session::builder()?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(ort::Error::<()>::from)?
            .with_intra_threads(4)
            .map_err(ort::Error::<()>::from)?
            .commit_from_file(config.models_dir.join(format!("{}.onnx", info.id)))?;
        Ok(Self {
            model: config.model_name,
            dimension: info.dimension,
            tokenizer,
            state: Mutex::new(InferenceState {
                session,
                cache: VecDeque::new(),
            }),
        })
    }

    fn infer(&self, text: &str) -> Result<Vec<f32>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("embedding session lock poisoned"))?;
        if let Some(position) = state.cache.iter().position(|(key, _)| key == text) {
            let entry = state
                .cache
                .remove(position)
                .context("missing cache entry")?;
            let vector = entry.1.clone();
            state.cache.push_front(entry);
            return Ok(vector);
        }
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(|error| anyhow::anyhow!("embedding tokenization failed: {error}"))?;
        let shape = [1usize, encoding.len()];
        let ids = Tensor::from_array((
            shape,
            encoding
                .get_ids()
                .iter()
                .map(|v| i64::from(*v))
                .collect::<Vec<_>>(),
        ))?;
        let mask = Tensor::from_array((
            shape,
            encoding
                .get_attention_mask()
                .iter()
                .map(|v| i64::from(*v))
                .collect::<Vec<_>>(),
        ))?;
        let types = Tensor::from_array((
            shape,
            encoding
                .get_type_ids()
                .iter()
                .map(|v| i64::from(*v))
                .collect::<Vec<_>>(),
        ))?;
        let session = &mut state.session;
        let names = session
            .inputs()
            .iter()
            .map(|input| input.name().to_owned())
            .collect::<Vec<_>>();
        let output_name = session
            .outputs()
            .first()
            .context("embedding model has no outputs")?
            .name()
            .to_owned();
        let outputs = match names.as_slice() {
            [ids_name] => session.run(ort::inputs![ids_name.clone() => ids])?,
            [ids_name, mask_name] => session.run(ort::inputs![ids_name.clone() => ids, mask_name.clone() => mask])?,
            [ids_name, mask_name, types_name] => session.run(ort::inputs![ids_name.clone() => ids, mask_name.clone() => mask, types_name.clone() => types])?,
            _ => bail!("unsupported embedding model inputs"),
        };
        let (shape, data) = outputs
            .get(&output_name)
            .context("missing embedding output")?
            .try_extract_tensor::<f32>()?;
        let vector = extract_cls(&shape, data, self.dimension)?;
        drop(outputs);
        if state.cache.len() == 1000 {
            state.cache.pop_back();
        }
        state.cache.push_front((text.to_owned(), vector.clone()));
        Ok(vector)
    }
}

// Both token-level and pre-pooled model exports are supported. A wrong hidden
// dimension must fail instead of truncating another model's output.
fn extract_cls(shape: &[i64], data: &[f32], dimension: usize) -> Result<Vec<f32>> {
    if !matches!(shape, [1, hidden] if *hidden == dimension as i64)
        && !matches!(shape, [1, tokens, hidden] if *tokens > 0 && *hidden == dimension as i64)
    {
        bail!("unexpected embedding output shape");
    }
    normalize_cls(data.get(..dimension).context("short embedding output")?)
}

fn normalize_cls(values: &[f32]) -> Result<Vec<f32>> {
    if values.iter().any(|value| !value.is_finite()) {
        bail!("embedding contains non-finite values");
    }
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    Ok(if norm.is_finite() && norm > 1e-10 {
        values.iter().map(|value| value / norm).collect()
    } else {
        vec![0.0; values.len()]
    })
}

impl LocalEmbeddingRuntime for OnnxLocalEmbeddingRuntime {
    fn embed_text(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        self.infer(text)
            .map_err(|error| provider_error(&self.model, error))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ThreadEpisodicEmbeddingError> {
        texts.iter().map(|text| self.embed_text(text)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::extract_cls;

    #[test]
    fn local_model_output_preserves_cls_pooling_and_unit_normalization() {
        // The second token is deliberately different: averaging tokens changes
        // retrieval identity for existing model files and must not slip in here.
        let vector = extract_cls(&[1, 2, 2], &[3.0, 4.0, 100.0, 0.0], 2).unwrap();
        assert!((vector[0] - 0.6).abs() < 1e-6);
        assert!((vector[1] - 0.8).abs() < 1e-6);
        let pooled_export = extract_cls(&[1, 2], &[3.0, 4.0], 2).unwrap();
        assert_eq!(vector, pooled_export);
        assert_eq!(
            extract_cls(&[1, 2], &[0.0, 0.0], 2).unwrap(),
            vec![0.0, 0.0]
        );
    }

    #[test]
    fn invalid_local_model_outputs_fail_before_entering_the_search_projection() {
        for (shape, data, dimension) in [
            (vec![1, 3], vec![3.0, 4.0, 5.0], 2),
            (vec![2, 2], vec![3.0, 4.0, 5.0, 6.0], 2),
            (vec![1, 0, 2], vec![], 2),
            (vec![1, 2], vec![3.0], 2),
            (vec![1, 2], vec![f32::NAN, 4.0], 2),
            (vec![1, 2], vec![3.0, f32::INFINITY], 2),
        ] {
            assert!(extract_cls(&shape, &data, dimension).is_err());
        }
    }
}
