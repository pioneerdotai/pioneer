use memvid_core::{MemvidError, VecEmbedder};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicEmbeddingIdentity {
    pub provider_id: String,
    pub model: String,
    pub dimension: usize,
    pub normalized: bool,
}

impl ThreadEpisodicEmbeddingIdentity {
    pub fn new(
        provider_id: impl Into<String>,
        model: impl Into<String>,
        dimension: usize,
        normalized: bool,
    ) -> Self {
        Self {
            provider_id: provider_id.into(),
            model: model.into(),
            dimension,
            normalized,
        }
    }

    pub fn validate_embedding(
        &self,
        actual_dimension: usize,
    ) -> Result<(), ThreadEpisodicEmbeddingError> {
        if actual_dimension == self.dimension {
            return Ok(());
        }

        Err(ThreadEpisodicEmbeddingError::dimension_mismatch(
            self.provider_id.clone(),
            self.model.clone(),
            self.dimension,
            actual_dimension,
        ))
    }

    pub fn memvid_model_id(&self) -> String {
        format!(
            "{}:{}:{}:normalized={}",
            self.provider_id, self.model, self.dimension, self.normalized
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadEpisodicEmbeddingErrorKind {
    MissingKey,
    MissingModel,
    DimensionMismatch { expected: usize, actual: usize },
    RetryableProviderFailure,
    NonRetryableProviderFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadEpisodicEmbeddingFailureClass {
    Configuration,
    RetryableProviderFailure,
    PermanentProviderFailure,
    DimensionMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicEmbeddingDiagnostic {
    pub provider_id: String,
    pub model: String,
    pub failure_class: ThreadEpisodicEmbeddingFailureClass,
    pub retryable: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadEpisodicEmbeddingSafeLogFields {
    pub provider_id: String,
    pub model: String,
    pub failure_class: ThreadEpisodicEmbeddingFailureClass,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEpisodicEmbeddingError {
    pub kind: ThreadEpisodicEmbeddingErrorKind,
    pub provider_id: String,
    pub model: String,
    pub message: String,
}

impl ThreadEpisodicEmbeddingError {
    pub fn missing_key(provider_id: impl Into<String>, model: impl Into<String>) -> Self {
        let provider_id = provider_id.into();
        let model = model.into();
        Self {
            kind: ThreadEpisodicEmbeddingErrorKind::MissingKey,
            message: format!("embedding provider `{provider_id}` is missing an API key"),
            provider_id,
            model,
        }
    }

    pub fn missing_model(provider_id: impl Into<String>, model: impl Into<String>) -> Self {
        let provider_id = provider_id.into();
        let model = model.into();
        Self {
            kind: ThreadEpisodicEmbeddingErrorKind::MissingModel,
            message: format!("embedding model `{model}` is not installed or available"),
            provider_id,
            model,
        }
    }

    pub fn dimension_mismatch(
        provider_id: impl Into<String>,
        model: impl Into<String>,
        expected: usize,
        actual: usize,
    ) -> Self {
        let provider_id = provider_id.into();
        let model = model.into();
        Self {
            kind: ThreadEpisodicEmbeddingErrorKind::DimensionMismatch { expected, actual },
            message: format!(
                "embedding dimension mismatch for `{provider_id}`/`{model}`: expected {expected}, got {actual}"
            ),
            provider_id,
            model,
        }
    }

    pub fn retryable_provider_failure(
        provider_id: impl Into<String>,
        model: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: ThreadEpisodicEmbeddingErrorKind::RetryableProviderFailure,
            provider_id: provider_id.into(),
            model: model.into(),
            message: message.into(),
        }
    }

    pub fn non_retryable_provider_failure(
        provider_id: impl Into<String>,
        model: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: ThreadEpisodicEmbeddingErrorKind::NonRetryableProviderFailure,
            provider_id: provider_id.into(),
            model: model.into(),
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            ThreadEpisodicEmbeddingErrorKind::RetryableProviderFailure
        )
    }

    pub fn failure_class(&self) -> ThreadEpisodicEmbeddingFailureClass {
        match self.kind {
            ThreadEpisodicEmbeddingErrorKind::MissingKey
            | ThreadEpisodicEmbeddingErrorKind::MissingModel => {
                ThreadEpisodicEmbeddingFailureClass::Configuration
            }
            ThreadEpisodicEmbeddingErrorKind::DimensionMismatch { .. } => {
                ThreadEpisodicEmbeddingFailureClass::DimensionMismatch
            }
            ThreadEpisodicEmbeddingErrorKind::RetryableProviderFailure => {
                ThreadEpisodicEmbeddingFailureClass::RetryableProviderFailure
            }
            ThreadEpisodicEmbeddingErrorKind::NonRetryableProviderFailure => {
                ThreadEpisodicEmbeddingFailureClass::PermanentProviderFailure
            }
        }
    }

    pub fn is_configuration_failure(&self) -> bool {
        matches!(
            self.failure_class(),
            ThreadEpisodicEmbeddingFailureClass::Configuration
        )
    }

    pub fn diagnostic(&self) -> ThreadEpisodicEmbeddingDiagnostic {
        ThreadEpisodicEmbeddingDiagnostic {
            provider_id: self.provider_id.clone(),
            model: self.model.clone(),
            failure_class: self.failure_class(),
            retryable: self.is_retryable(),
            message: self.message.clone(),
        }
    }

    pub fn safe_log_fields(&self) -> ThreadEpisodicEmbeddingSafeLogFields {
        ThreadEpisodicEmbeddingSafeLogFields {
            provider_id: self.provider_id.clone(),
            model: self.model.clone(),
            failure_class: self.failure_class(),
            retryable: self.is_retryable(),
        }
    }

    fn into_memvid_error(self) -> MemvidError {
        if let ThreadEpisodicEmbeddingErrorKind::DimensionMismatch { expected, actual } = &self.kind
        {
            if let Ok(expected) = u32::try_from(*expected) {
                return MemvidError::VecDimensionMismatch {
                    expected,
                    actual: *actual,
                };
            }
        }

        MemvidError::EmbeddingFailed {
            reason: self.to_string().into_boxed_str(),
        }
    }
}

impl Display for ThreadEpisodicEmbeddingError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} (provider={}, model={})",
            self.message, self.provider_id, self.model
        )
    }
}

impl Error for ThreadEpisodicEmbeddingError {}

pub trait ThreadEpisodicEmbeddingProvider: Send + Sync {
    fn provider_id(&self) -> &str;

    fn model(&self) -> &str;

    fn dimension(&self) -> usize;

    fn normalized(&self) -> bool;

    fn embed_text(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError>;

    fn embed_query(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        self.embed_text(text)
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, ThreadEpisodicEmbeddingError> {
        let mut embeddings = Vec::with_capacity(texts.len());
        for text in texts {
            embeddings.push(self.embed_text_checked(text)?);
        }
        Ok(embeddings)
    }

    fn identity(&self) -> ThreadEpisodicEmbeddingIdentity {
        ThreadEpisodicEmbeddingIdentity::new(
            self.provider_id(),
            self.model(),
            self.dimension(),
            self.normalized(),
        )
    }

    fn embed_text_checked(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        let embedding = self.embed_text(text)?;
        self.identity().validate_embedding(embedding.len())?;
        Ok(embedding)
    }

    fn embed_query_checked(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        let embedding = self.embed_query(text)?;
        self.identity().validate_embedding(embedding.len())?;
        Ok(embedding)
    }
}

#[derive(Clone)]
pub struct ThreadEpisodicMemvidEmbedder {
    provider: Arc<dyn ThreadEpisodicEmbeddingProvider>,
}

impl ThreadEpisodicMemvidEmbedder {
    pub fn new(provider: Arc<dyn ThreadEpisodicEmbeddingProvider>) -> Self {
        Self { provider }
    }

    pub fn provider(&self) -> &dyn ThreadEpisodicEmbeddingProvider {
        self.provider.as_ref()
    }

    pub fn identity(&self) -> ThreadEpisodicEmbeddingIdentity {
        self.provider.identity()
    }
}

impl VecEmbedder for ThreadEpisodicMemvidEmbedder {
    fn embed_query(&self, text: &str) -> memvid_core::Result<Vec<f32>> {
        self.provider
            .embed_query_checked(text)
            .map_err(ThreadEpisodicEmbeddingError::into_memvid_error)
    }

    fn embed_chunks(&self, texts: &[&str]) -> memvid_core::Result<Vec<Vec<f32>>> {
        self.provider
            .embed_batch(texts)
            .map_err(ThreadEpisodicEmbeddingError::into_memvid_error)
    }

    fn embedding_dimension(&self) -> usize {
        self.provider.dimension()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockEmbeddingProvider {
        provider_id: &'static str,
        model: &'static str,
        dimension: usize,
        returned_dimension: usize,
    }

    impl ThreadEpisodicEmbeddingProvider for MockEmbeddingProvider {
        fn provider_id(&self) -> &str {
            self.provider_id
        }

        fn model(&self) -> &str {
            self.model
        }

        fn dimension(&self) -> usize {
            self.dimension
        }

        fn normalized(&self) -> bool {
            true
        }

        fn embed_text(&self, _text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
            Ok(vec![0.5; self.returned_dimension])
        }
    }

    #[test]
    fn embedding_identity_tracks_provider_model_dimension_and_normalization() {
        let provider = MockEmbeddingProvider {
            provider_id: "openrouter",
            model: "openai/text-embedding-3-small",
            dimension: 1536,
            returned_dimension: 1536,
        };

        assert_eq!(
            provider.identity(),
            ThreadEpisodicEmbeddingIdentity::new(
                "openrouter",
                "openai/text-embedding-3-small",
                1536,
                true
            )
        );
    }

    #[test]
    fn embedding_provider_checked_methods_validate_dimension() {
        let provider = MockEmbeddingProvider {
            provider_id: "openai",
            model: "text-embedding-3-small",
            dimension: 1536,
            returned_dimension: 3,
        };

        let error = provider
            .embed_text_checked("hello")
            .expect_err("wrong dimension must fail");
        assert_eq!(
            error.kind,
            ThreadEpisodicEmbeddingErrorKind::DimensionMismatch {
                expected: 1536,
                actual: 3
            }
        );
    }

    #[test]
    fn memvid_embedder_delegates_query_and_dimension() {
        let provider = Arc::new(MockEmbeddingProvider {
            provider_id: "local",
            model: "bge-small-en-v1.5",
            dimension: 384,
            returned_dimension: 384,
        });
        let embedder = ThreadEpisodicMemvidEmbedder::new(provider);

        assert_eq!(embedder.embedding_dimension(), 384);
        assert_eq!(embedder.embed_query("query").unwrap().len(), 384);
    }

    #[test]
    fn memvid_embedder_maps_dimension_mismatch_to_memvid_error() {
        let provider = Arc::new(MockEmbeddingProvider {
            provider_id: "local",
            model: "bge-small-en-v1.5",
            dimension: 384,
            returned_dimension: 768,
        });
        let embedder = ThreadEpisodicMemvidEmbedder::new(provider);

        let error = embedder
            .embed_query("query")
            .expect_err("wrong dimension must fail");
        assert!(matches!(
            error,
            MemvidError::VecDimensionMismatch {
                expected: 384,
                actual: 768
            }
        ));
    }

    #[test]
    fn embedding_errors_classify_retryable_provider_failure() {
        let error = ThreadEpisodicEmbeddingError::retryable_provider_failure(
            "openai",
            "text-embedding-3-small",
            "rate limited",
        );

        assert!(error.is_retryable());
        assert!(!error.to_string().contains("sk-"));
    }

    #[test]
    fn embedding_errors_expose_stable_failure_classes_and_diagnostics() {
        let missing = ThreadEpisodicEmbeddingError::missing_key("openai", "text-embedding-3-small");
        assert_eq!(
            missing.failure_class(),
            ThreadEpisodicEmbeddingFailureClass::Configuration
        );
        assert!(missing.is_configuration_failure());
        assert!(!missing.is_retryable());

        let retryable = ThreadEpisodicEmbeddingError::retryable_provider_failure(
            "openrouter",
            "openai/text-embedding-3-small",
            "rate limited",
        );
        assert_eq!(
            retryable.failure_class(),
            ThreadEpisodicEmbeddingFailureClass::RetryableProviderFailure
        );
        assert!(retryable.is_retryable());

        let permanent = ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
            "local",
            "bge-small-en-v1.5",
            "bad model file",
        );
        assert_eq!(
            permanent.failure_class(),
            ThreadEpisodicEmbeddingFailureClass::PermanentProviderFailure
        );

        let dimension = ThreadEpisodicEmbeddingError::dimension_mismatch(
            "openai",
            "text-embedding-3-large",
            3072,
            1536,
        );
        assert_eq!(
            dimension.failure_class(),
            ThreadEpisodicEmbeddingFailureClass::DimensionMismatch
        );

        let diagnostic = retryable.diagnostic();
        assert_eq!(diagnostic.provider_id, "openrouter");
        assert_eq!(
            diagnostic.failure_class,
            ThreadEpisodicEmbeddingFailureClass::RetryableProviderFailure
        );
        assert!(diagnostic.retryable);
    }

    #[test]
    fn embedding_safe_log_fields_exclude_error_message_and_secrets() {
        let error = ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
            "openai",
            "text-embedding-3-small",
            "provider rejected key sk-secret and text payload hello",
        );

        let fields = error.safe_log_fields();
        assert_eq!(fields.provider_id, "openai");
        assert_eq!(fields.model, "text-embedding-3-small");
        assert_eq!(
            fields.failure_class,
            ThreadEpisodicEmbeddingFailureClass::PermanentProviderFailure
        );
        let serialized = serde_json::to_string(&fields).expect("serialize fields");
        assert!(!serialized.contains("sk-secret"));
        assert!(!serialized.contains("text payload"));
    }
}
