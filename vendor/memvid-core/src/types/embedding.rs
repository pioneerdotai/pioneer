//! Embedding provider trait and implementations.
//!
//! The `EmbeddingProvider` trait defines a unified interface for generating
//! embeddings from text, supporting both local models (fastembed, candle) and
//! cloud APIs (`OpenAI`, Anthropic).

use crate::error::Result;

/// Configuration for an embedding provider.
#[derive(Debug, Clone)]
pub struct EmbeddingConfig {
    /// Model identifier (e.g., "text-embedding-3-large", "nomic-embed-text-v1.5")
    pub model: String,
    /// Embedding dimension (used for validation)
    pub dimension: usize,
    /// Optional batch size for bulk embedding operations
    pub batch_size: Option<usize>,
    /// Whether to normalize embeddings to unit length
    pub normalize: bool,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            model: "text-embedding-3-large".to_string(),
            dimension: 3072,
            batch_size: Some(100),
            normalize: true,
        }
    }
}

impl EmbeddingConfig {
    /// Create config for `OpenAI` text-embedding-3-large
    #[must_use]
    pub fn openai_large() -> Self {
        Self {
            model: "text-embedding-3-large".to_string(),
            dimension: 3072,
            batch_size: Some(100),
            normalize: true,
        }
    }

    /// Create config for `OpenAI` text-embedding-3-small
    #[must_use]
    pub fn openai_small() -> Self {
        Self {
            model: "text-embedding-3-small".to_string(),
            dimension: 1536,
            batch_size: Some(100),
            normalize: true,
        }
    }

    /// Create config for `OpenAI` text-embedding-ada-002
    #[must_use]
    pub fn openai_ada() -> Self {
        Self {
            model: "text-embedding-ada-002".to_string(),
            dimension: 1536,
            batch_size: Some(100),
            normalize: true,
        }
    }

    /// Create config for local Nomic model
    #[must_use]
    pub fn nomic() -> Self {
        Self {
            model: "nomic-embed-text-v1.5".to_string(),
            dimension: 768,
            batch_size: Some(32),
            normalize: true,
        }
    }

    /// Create config for local BGE-small model
    #[must_use]
    pub fn bge_small() -> Self {
        Self {
            model: "BAAI/bge-small-en-v1.5".to_string(),
            dimension: 384,
            batch_size: Some(32),
            normalize: true,
        }
    }

    /// Create config for local BGE-base model
    #[must_use]
    pub fn bge_base() -> Self {
        Self {
            model: "BAAI/bge-base-en-v1.5".to_string(),
            dimension: 768,
            batch_size: Some(32),
            normalize: true,
        }
    }

    /// Create config for local GTE-large model
    #[must_use]
    pub fn gte_large() -> Self {
        Self {
            model: "thenlper/gte-large".to_string(),
            dimension: 1024,
            batch_size: Some(16),
            normalize: true,
        }
    }
}

/// Trait for embedding providers that generate vector embeddings from text.
///
/// This is a superset of the existing `VecEmbedder` trait, adding:
/// - Provider identification (kind, model, dimension)
/// - Async-friendly batch operations
/// - Configuration management
///
/// # Example
///
/// ```ignore
/// use memvid_core::types::embedding::{EmbeddingProvider, EmbeddingConfig};
///
/// struct OpenAIProvider {
///     api_key: String,
///     config: EmbeddingConfig,
/// }
///
/// impl EmbeddingProvider for OpenAIProvider {
///     fn kind(&self) -> &str { "openai" }
///     fn model(&self) -> &str { &self.config.model }
///     fn dimension(&self) -> usize { self.config.dimension }
///
///     fn embed_text(&self, text: &str) -> Result<Vec<f32>> {
///         // Call OpenAI API
///         todo!()
///     }
/// }
/// ```
pub trait EmbeddingProvider: Send + Sync {
    /// Return the provider kind (e.g., "openai", "local", "anthropic").
    fn kind(&self) -> &str;

    /// Return the model identifier.
    fn model(&self) -> &str;

    /// Return the embedding dimension.
    fn dimension(&self) -> usize;

    /// Generate an embedding for a single text string.
    fn embed_text(&self, text: &str) -> Result<Vec<f32>>;

    /// Generate embeddings for multiple text strings.
    ///
    /// Default implementation calls `embed_text` in a loop.
    /// Providers should override this for efficient batch processing.
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut embeddings = Vec::with_capacity(texts.len());
        for text in texts {
            embeddings.push(self.embed_text(text)?);
        }
        Ok(embeddings)
    }

    /// Check if the provider is ready to generate embeddings.
    fn is_ready(&self) -> bool {
        true
    }

    /// Initialize the provider (e.g., load models, verify API key).
    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Enum wrapper for different embedding provider implementations.
#[derive(Debug, Clone)]
pub enum EmbeddingProviderKind {
    /// Local fastembed/ONNX model
    Local(String),
    /// `OpenAI` API
    OpenAI { model: String, api_key_env: String },
    /// Anthropic API (future)
    Anthropic { model: String, api_key_env: String },
    /// Custom provider
    Custom(String),
}

impl Default for EmbeddingProviderKind {
    fn default() -> Self {
        Self::Local("nomic-embed-text-v1.5".to_string())
    }
}

/// Result type for embedding operations
pub type EmbeddingResult = Result<Vec<f32>>;
pub type BatchEmbeddingResult = Result<Vec<Vec<f32>>>;

#[cfg(test)]
mod tests {
    use super::*;

    struct MockProvider {
        dimension: usize,
    }

    impl EmbeddingProvider for MockProvider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn kind(&self) -> &str {
            "mock"
        }

        #[allow(clippy::unnecessary_literal_bound)]
        fn model(&self) -> &str {
            "mock-model"
        }

        fn dimension(&self) -> usize {
            self.dimension
        }

        fn embed_text(&self, _text: &str) -> Result<Vec<f32>> {
            Ok(vec![0.0; self.dimension])
        }
    }

    #[test]
    fn test_mock_provider() {
        let provider = MockProvider { dimension: 768 };
        assert_eq!(provider.kind(), "mock");
        assert_eq!(provider.dimension(), 768);

        let embedding = provider.embed_text("test").unwrap();
        assert_eq!(embedding.len(), 768);
    }

    #[test]
    fn test_batch_default_impl() {
        let provider = MockProvider { dimension: 384 };
        let texts = vec!["hello", "world"];
        let embeddings = provider.embed_batch(&texts).unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), 384);
    }

    #[test]
    fn test_configs() {
        let openai = EmbeddingConfig::openai_large();
        assert_eq!(openai.dimension, 3072);

        let nomic = EmbeddingConfig::nomic();
        assert_eq!(nomic.dimension, 768);

        let bge = EmbeddingConfig::bge_small();
        assert_eq!(bge.dimension, 384);
    }

    /// Integration test for LocalTextEmbedder
    /// This test requires the BGE-small model to be downloaded to ~/.cache/memvid/text-models/
    /// If the model is not present, the test will print a skip message and pass.
    #[cfg(feature = "vec")]
    #[test]
    fn test_local_text_embedder_integration() {
        use crate::text_embed::{LocalTextEmbedder, TextEmbedConfig};
        use crate::types::embedding::EmbeddingProvider;

        let config = TextEmbedConfig::default();
        let embedder = match LocalTextEmbedder::new(config) {
            Ok(e) => e,
            Err(_) => {
                println!("Skipping test_local_text_embedder_integration: model setup required");
                return;
            }
        };

        // Verify trait implementation
        assert_eq!(embedder.kind(), "local");
        assert_eq!(embedder.model(), "bge-small-en-v1.5");
        assert_eq!(embedder.dimension(), 384);
        assert!(embedder.is_ready());

        // Test single embedding (skip if model not downloaded)
        match embedder.embed_text("hello world") {
            Ok(embedding) => {
                assert_eq!(embedding.len(), 384);
                // Verify normalization (L2 norm should be ~1.0)
                let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
                assert!((norm - 1.0).abs() < 0.01, "Embedding should be normalized");
            }
            Err(e) => {
                println!("Skipping embedding test: {}", e);
                println!("To run this test, download the model:");
                println!("  mkdir -p ~/.cache/memvid/text-models");
                println!(
                    "  curl -L 'https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/onnx/model.onnx' -o ~/.cache/memvid/text-models/bge-small-en-v1.5.onnx"
                );
                println!(
                    "  curl -L 'https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/tokenizer.json' -o ~/.cache/memvid/text-models/bge-small-en-v1.5_tokenizer.json"
                );
                return;
            }
        }

        // Test batch processing
        let texts = vec!["hello", "world", "embedding"];
        match embedder.embed_batch(&texts) {
            Ok(batch) => {
                assert_eq!(batch.len(), 3);
                assert_eq!(batch[0].len(), 384);
                assert_eq!(batch[1].len(), 384);
                assert_eq!(batch[2].len(), 384);
            }
            Err(e) => {
                println!("Batch embedding failed: {}", e);
            }
        }
    }
}
