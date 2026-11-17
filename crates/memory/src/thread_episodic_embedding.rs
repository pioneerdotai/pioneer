use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};

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
    /// Provider-supplied cooldown in milliseconds; never inferred from the message.
    pub retry_after_ms: Option<u64>,
}

impl ThreadEpisodicEmbeddingError {
    pub fn missing_key(provider_id: impl Into<String>, model: impl Into<String>) -> Self {
        let provider_id = provider_id.into();
        let model = model.into();
        Self {
            retry_after_ms: None,
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
            retry_after_ms: None,
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
            retry_after_ms: None,
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
            retry_after_ms: None,
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
            retry_after_ms: None,
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

    /// Stable identity for document preprocessing and embedding semantics.
    ///
    /// Implementations that chunk, pool, prefix, or otherwise transform document
    /// input must include those settings here so persisted artifacts are never
    /// reused across incompatible pipelines.
    fn document_embedding_pipeline_identity(&self) -> String {
        format!(
            "thread_episodic_document_embedding_v1\nprovider={}\nmodel={}\ndimension={}\nnormalized={}\n",
            self.provider_id(),
            self.model(),
            self.dimension(),
            self.normalized()
        )
    }

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
        if embedding.iter().any(|value| !value.is_finite())
            || !embedding.iter().any(|value| *value != 0.0)
        {
            return Err(
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    self.provider_id(),
                    self.model(),
                    "embedding must be finite and nonzero",
                ),
            );
        }
        Ok(embedding)
    }

    fn embed_query_checked(&self, text: &str) -> Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
        let embedding = self.embed_query(text)?;
        self.identity().validate_embedding(embedding.len())?;
        if embedding.iter().any(|value| !value.is_finite())
            || !embedding.iter().any(|value| *value != 0.0)
        {
            return Err(
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    self.provider_id(),
                    self.model(),
                    "embedding must be finite and nonzero",
                ),
            );
        }
        Ok(embedding)
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
