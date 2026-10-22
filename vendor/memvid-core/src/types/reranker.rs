//! Reranker trait and implementations for improving retrieval accuracy.
//!
//! The `Reranker` trait defines a unified interface for reranking search results
//! using various techniques like cross-encoders, LLMs, or BM25 rescoring.
//!
//! ## Architecture
//!
//! A reranker takes a query and a list of candidate documents, and returns
//! a reordered list with relevance scores. This is typically used as a
//! second-stage ranking step after initial retrieval.
//!
//! ## Usage
//!
//! ```ignore
//! let candidates = vec![
//!     RerankerDocument { id: 0, text: "Document 1 text" },
//!     RerankerDocument { id: 1, text: "Document 2 text" },
//! ];
//!
//! let reranked = reranker.rerank("What is the capital?", &candidates, 5)?;
//! for result in reranked {
//!     println!("ID: {}, Score: {}", result.id, result.score);
//! }
//! ```

use crate::error::Result;

/// A document candidate for reranking.
#[derive(Debug, Clone)]
pub struct RerankerDocument {
    /// Unique identifier for this document (usually `frame_id`).
    pub id: u64,
    /// The text content to be evaluated for relevance.
    pub text: String,
    /// Optional metadata (e.g., title, URI).
    pub metadata: Option<String>,
}

impl RerankerDocument {
    /// Create a new reranker document.
    #[must_use]
    pub fn new(id: u64, text: impl Into<String>) -> Self {
        Self {
            id,
            text: text.into(),
            metadata: None,
        }
    }

    /// Create with metadata.
    #[must_use]
    pub fn with_metadata(id: u64, text: impl Into<String>, metadata: impl Into<String>) -> Self {
        Self {
            id,
            text: text.into(),
            metadata: Some(metadata.into()),
        }
    }
}

/// Result of reranking a document.
#[derive(Debug, Clone)]
pub struct RerankerResult {
    /// Document ID.
    pub id: u64,
    /// Relevance score (higher is more relevant, typically 0.0-1.0).
    pub score: f32,
    /// Original rank before reranking.
    pub original_rank: usize,
    /// New rank after reranking.
    pub new_rank: usize,
}

/// Configuration for reranking.
#[derive(Debug, Clone)]
pub struct RerankerConfig {
    /// Maximum number of candidates to consider.
    pub max_candidates: usize,
    /// Maximum number of results to return.
    pub top_k: usize,
    /// Minimum score threshold (0.0-1.0).
    pub min_score: f32,
    /// Whether to use document metadata in ranking.
    pub use_metadata: bool,
}

impl Default for RerankerConfig {
    fn default() -> Self {
        Self {
            max_candidates: 50,
            top_k: 10,
            min_score: 0.0,
            use_metadata: false,
        }
    }
}

impl RerankerConfig {
    /// Create config for high recall (more candidates, lower threshold).
    #[must_use]
    pub fn high_recall() -> Self {
        Self {
            max_candidates: 100,
            top_k: 20,
            min_score: 0.0,
            use_metadata: true,
        }
    }

    /// Create config for high precision (fewer candidates, higher threshold).
    #[must_use]
    pub fn high_precision() -> Self {
        Self {
            max_candidates: 20,
            top_k: 5,
            min_score: 0.3,
            use_metadata: true,
        }
    }
}

/// Trait for reranking search results.
///
/// Rerankers improve retrieval quality by evaluating query-document pairs
/// more thoroughly than initial retrieval methods allow.
///
/// # Implementations
///
/// - `CrossEncoderReranker`: Uses a cross-encoder model (BERT-based)
/// - `LLMReranker`: Uses an LLM to score relevance
/// - `BM25Reranker`: Uses BM25 scoring as a secondary signal
///
/// # Example
///
/// ```ignore
/// struct MyReranker;
///
/// impl Reranker for MyReranker {
///     fn kind(&self) -> &str { "my-reranker" }
///
///     fn rerank(
///         &self,
///         query: &str,
///         documents: &[RerankerDocument],
///         top_k: usize,
///     ) -> Result<Vec<RerankerResult>> {
///         // Score and rerank documents
///         todo!()
///     }
/// }
/// ```
pub trait Reranker: Send + Sync {
    /// Return the reranker kind identifier.
    fn kind(&self) -> &'static str;

    /// Rerank documents by relevance to the query.
    ///
    /// # Arguments
    /// * `query` - The search query
    /// * `documents` - Candidate documents to rerank
    /// * `top_k` - Maximum number of results to return
    ///
    /// # Returns
    /// Reranked results sorted by relevance score (highest first).
    fn rerank(
        &self,
        query: &str,
        documents: &[RerankerDocument],
        top_k: usize,
    ) -> Result<Vec<RerankerResult>>;

    /// Check if the reranker is ready.
    fn is_ready(&self) -> bool {
        true
    }

    /// Initialize the reranker (e.g., load models).
    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Enum wrapper for reranker kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerankerKind {
    /// No reranking.
    None,
    /// BM25-based reranking.
    Bm25,
    /// Cross-encoder model reranking.
    CrossEncoder,
    /// LLM-based reranking.
    Llm,
    /// OpenAI-based reranking.
    OpenAI,
}

impl Default for RerankerKind {
    fn default() -> Self {
        Self::None
    }
}

impl std::fmt::Display for RerankerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Bm25 => write!(f, "bm25"),
            Self::CrossEncoder => write!(f, "cross-encoder"),
            Self::Llm => write!(f, "llm"),
            Self::OpenAI => write!(f, "openai"),
        }
    }
}

impl std::str::FromStr for RerankerKind {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" | "off" | "disabled" => Ok(Self::None),
            "bm25" => Ok(Self::Bm25),
            "cross-encoder" | "crossencoder" | "cross_encoder" => Ok(Self::CrossEncoder),
            "llm" | "local" => Ok(Self::Llm),
            "openai" => Ok(Self::OpenAI),
            _ => Err(format!("Unknown reranker kind: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockReranker;

    impl Reranker for MockReranker {
        fn kind(&self) -> &'static str {
            "mock"
        }

        fn rerank(
            &self,
            _query: &str,
            documents: &[RerankerDocument],
            top_k: usize,
        ) -> Result<Vec<RerankerResult>> {
            // Simple mock: reverse order and assign scores
            let mut results: Vec<RerankerResult> = documents
                .iter()
                .enumerate()
                .map(|(idx, doc)| RerankerResult {
                    id: doc.id,
                    score: 1.0 - (idx as f32 / documents.len() as f32),
                    original_rank: idx + 1,
                    new_rank: documents.len() - idx,
                })
                .collect();

            results.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            for (idx, result) in results.iter_mut().enumerate() {
                result.new_rank = idx + 1;
            }

            Ok(results.into_iter().take(top_k).collect())
        }
    }

    #[test]
    fn test_mock_reranker() {
        let reranker = MockReranker;
        assert_eq!(reranker.kind(), "mock");
        assert!(reranker.is_ready());

        let docs = vec![
            RerankerDocument::new(0, "First document"),
            RerankerDocument::new(1, "Second document"),
            RerankerDocument::new(2, "Third document"),
        ];

        let results = reranker.rerank("query", &docs, 2).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].score >= results[1].score);
    }

    #[test]
    fn test_reranker_kind_parsing() {
        assert_eq!("none".parse::<RerankerKind>().unwrap(), RerankerKind::None);
        assert_eq!("bm25".parse::<RerankerKind>().unwrap(), RerankerKind::Bm25);
        assert_eq!(
            "openai".parse::<RerankerKind>().unwrap(),
            RerankerKind::OpenAI
        );
        assert_eq!("llm".parse::<RerankerKind>().unwrap(), RerankerKind::Llm);
    }

    #[test]
    fn test_config_defaults() {
        let default = RerankerConfig::default();
        assert_eq!(default.max_candidates, 50);
        assert_eq!(default.top_k, 10);

        let high_recall = RerankerConfig::high_recall();
        assert_eq!(high_recall.max_candidates, 100);

        let high_precision = RerankerConfig::high_precision();
        assert!(high_precision.min_score > 0.0);
    }
}
