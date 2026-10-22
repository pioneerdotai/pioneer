//! Enrichment engine trait and context types.
//!
//! The `EnrichmentEngine` trait defines the interface that all enrichment
//! engines must implement. Each engine processes frames and produces
//! structured memory cards.

use crate::error::Result;
use crate::types::{FrameId, MemoryCard};

/// Context provided to enrichment engines during processing.
#[derive(Debug, Clone)]
pub struct EnrichmentContext {
    /// The frame ID being processed.
    pub frame_id: FrameId,
    /// The frame's URI (e.g., "<mv2://session-1/msg-5>").
    pub uri: String,
    /// The frame's text content.
    pub text: String,
    /// The frame's title (if any).
    pub title: Option<String>,
    /// The frame's timestamp (Unix seconds).
    pub timestamp: i64,
    /// Optional metadata from the frame.
    pub metadata: Option<String>,
}

impl EnrichmentContext {
    /// Create a new enrichment context.
    #[must_use]
    pub fn new(
        frame_id: FrameId,
        uri: String,
        text: String,
        title: Option<String>,
        timestamp: i64,
        metadata: Option<String>,
    ) -> Self {
        Self {
            frame_id,
            uri,
            text,
            title,
            timestamp,
            metadata,
        }
    }
}

/// Result of running an enrichment engine on a frame.
#[derive(Debug, Clone, Default)]
pub struct EnrichmentResult {
    /// Memory cards extracted from the frame.
    pub cards: Vec<MemoryCard>,
    /// Whether the engine successfully processed the frame.
    /// Even if no cards were extracted, this can be true.
    pub success: bool,
    /// Optional error message if processing failed.
    pub error: Option<String>,
}

impl EnrichmentResult {
    /// Create a successful result with cards.
    #[must_use]
    pub fn success(cards: Vec<MemoryCard>) -> Self {
        Self {
            cards,
            success: true,
            error: None,
        }
    }

    /// Create an empty successful result (no cards extracted).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            cards: Vec::new(),
            success: true,
            error: None,
        }
    }

    /// Create a failed result.
    #[must_use]
    pub fn failed(error: impl Into<String>) -> Self {
        Self {
            cards: Vec::new(),
            success: false,
            error: Some(error.into()),
        }
    }
}

/// Trait for enrichment engines that process frames and extract memory cards.
///
/// Engines are identified by a kind (e.g., "rules", "llm:phi-3.5-mini") and
/// a version string. The combination allows tracking which frames have been
/// processed by which engine versions.
///
/// # Example
///
/// ```ignore
/// use memvid_core::enrich::{EnrichmentEngine, EnrichmentContext, EnrichmentResult};
///
/// struct MyEngine;
///
/// impl EnrichmentEngine for MyEngine {
///     fn kind(&self) -> &str { "my-engine" }
///     fn version(&self) -> &str { "1.0.0" }
///
///     fn enrich(&self, ctx: &EnrichmentContext) -> EnrichmentResult {
///         // Extract memory cards from ctx.text
///         EnrichmentResult::empty()
///     }
/// }
/// ```
pub trait EnrichmentEngine: Send + Sync {
    /// Return the engine kind identifier (e.g., "rules", "llm:phi-3.5-mini").
    fn kind(&self) -> &str;

    /// Return the engine version string (e.g., "1.0.0").
    fn version(&self) -> &str;

    /// Process a frame and extract memory cards.
    ///
    /// The engine receives the frame's text content and metadata via the
    /// `EnrichmentContext` and should return any extracted memory cards.
    fn enrich(&self, ctx: &EnrichmentContext) -> EnrichmentResult;

    /// Initialize the engine (e.g., load models).
    ///
    /// This is called once before processing begins. Engines that need
    /// to load models or other resources should do so here.
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    /// Check if the engine is ready for processing.
    ///
    /// Returns true if `init()` has been called and the engine is ready.
    fn is_ready(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestEngine;

    impl EnrichmentEngine for TestEngine {
        fn kind(&self) -> &'static str {
            "test"
        }
        fn version(&self) -> &'static str {
            "1.0.0"
        }
        fn enrich(&self, _ctx: &EnrichmentContext) -> EnrichmentResult {
            EnrichmentResult::empty()
        }
    }

    #[test]
    fn test_enrichment_context() {
        let ctx = EnrichmentContext::new(
            42,
            "mv2://test/msg-1".to_string(),
            "Hello, I work at Anthropic.".to_string(),
            Some("Test".to_string()),
            1700000000,
            None,
        );
        assert_eq!(ctx.frame_id, 42);
        assert_eq!(ctx.uri, "mv2://test/msg-1");
    }

    #[test]
    fn test_enrichment_result() {
        let success = EnrichmentResult::success(vec![]);
        assert!(success.success);
        assert!(success.error.is_none());

        let empty = EnrichmentResult::empty();
        assert!(empty.success);
        assert!(empty.cards.is_empty());

        let failed = EnrichmentResult::failed("test error");
        assert!(!failed.success);
        assert_eq!(failed.error, Some("test error".to_string()));
    }

    #[test]
    fn test_engine_trait() {
        let engine = TestEngine;
        assert_eq!(engine.kind(), "test");
        assert_eq!(engine.version(), "1.0.0");
        assert!(engine.is_ready());
    }
}
