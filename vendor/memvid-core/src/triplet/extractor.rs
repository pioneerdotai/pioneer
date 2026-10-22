//! Triplet extractor that wraps enrichment engines for automatic extraction.
//!
//! The extractor uses the configured `ExtractionMode` to determine which
//! engines to run. By default, it uses the `RulesEngine` for fast, offline
//! pattern-based extraction.

use std::time::Instant;

use crate::enrich::{EnrichmentContext, EnrichmentEngine, RulesEngine};
use crate::types::{FrameId, MemoryCard};

use super::types::{ExtractionMode, ExtractionStats};

/// Triplet extractor that runs enrichment engines on text.
///
/// The extractor is stateless and can be reused across multiple extractions.
/// It wraps the existing `RulesEngine` and adds support for LLM extraction
/// when configured.
#[derive(Debug)]
pub struct TripletExtractor {
    mode: ExtractionMode,
    rules_engine: RulesEngine,
}

impl Default for TripletExtractor {
    fn default() -> Self {
        Self::new(ExtractionMode::default())
    }
}

impl TripletExtractor {
    /// Create a new triplet extractor with the given mode.
    #[must_use]
    pub fn new(mode: ExtractionMode) -> Self {
        Self {
            mode,
            rules_engine: RulesEngine::new(),
        }
    }

    /// Create an extractor with rules-only mode (default).
    #[must_use]
    pub fn rules_only() -> Self {
        Self::new(ExtractionMode::Rules)
    }

    /// Create an extractor with hybrid mode.
    #[must_use]
    pub fn hybrid() -> Self {
        Self::new(ExtractionMode::Hybrid)
    }

    /// Create a disabled extractor (no extraction).
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(ExtractionMode::Disabled)
    }

    /// Get the current extraction mode.
    #[must_use]
    pub fn mode(&self) -> &ExtractionMode {
        &self.mode
    }

    /// Set the extraction mode.
    pub fn set_mode(&mut self, mode: ExtractionMode) {
        self.mode = mode;
    }

    /// Check if extraction is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.mode.is_enabled()
    }

    /// Extract triplets from text and convert to memory cards.
    ///
    /// # Arguments
    /// * `frame_id` - The frame ID for provenance tracking
    /// * `text` - The text to extract triplets from
    /// * `uri` - Optional URI for provenance
    /// * `title` - Optional title for context
    /// * `timestamp` - Unix timestamp when the content was created
    ///
    /// # Returns
    /// A tuple of (extracted cards, extraction stats)
    pub fn extract(
        &self,
        frame_id: FrameId,
        text: &str,
        uri: Option<&str>,
        title: Option<&str>,
        timestamp: i64,
    ) -> (Vec<MemoryCard>, ExtractionStats) {
        if !self.mode.is_enabled() {
            return (Vec::new(), ExtractionStats::default());
        }

        let start = Instant::now();
        let mut all_cards = Vec::new();

        // Run rules-based extraction
        if self.mode.should_run_rules() {
            let ctx = EnrichmentContext::new(
                frame_id,
                uri.unwrap_or(&format!("mv2://frames/{frame_id}"))
                    .to_string(),
                text.to_string(),
                title.map(String::from),
                timestamp,
                None,
            );

            let result = self.rules_engine.enrich(&ctx);
            if result.success {
                all_cards.extend(result.cards);
            }
        }

        // Run LLM-based extraction
        let llm_count = if self.mode.should_run_llm() {
            tracing::debug!(
                target: "memvid::triplet",
                "LLM extraction requested"
            );
            0
        } else {
            0
        };

        let elapsed_ms = start.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        let rules_count = all_cards.len();

        // Deduplicate cards with same entity:slot
        let (unique_cards, dedup_count) = deduplicate_cards(all_cards);

        let mut stats = ExtractionStats::from_rules(rules_count, elapsed_ms);
        stats.add_llm(llm_count);
        stats.record_dedup(dedup_count);

        (unique_cards, stats)
    }

    /// Extract triplets from an existing `EnrichmentContext`.
    ///
    /// This is useful when you already have a context from the enrichment pipeline.
    #[must_use]
    pub fn extract_from_context(
        &self,
        ctx: &EnrichmentContext,
    ) -> (Vec<MemoryCard>, ExtractionStats) {
        self.extract(
            ctx.frame_id,
            &ctx.text,
            Some(&ctx.uri),
            ctx.title.as_deref(),
            ctx.timestamp,
        )
    }
}

/// Deduplicate cards by entity:slot, keeping the highest confidence one.
fn deduplicate_cards(mut cards: Vec<MemoryCard>) -> (Vec<MemoryCard>, usize) {
    use std::collections::HashMap;

    if cards.is_empty() {
        return (cards, 0);
    }

    let original_count = cards.len();

    // Group by entity:slot, keep highest confidence
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut keep = vec![true; cards.len()];

    for (i, card) in cards.iter().enumerate() {
        let key = card.default_version_key();
        if let Some(&existing_idx) = seen.get(&key) {
            // Compare confidence, keep higher one
            let existing_conf = cards[existing_idx].confidence.unwrap_or(0.0);
            let current_conf = card.confidence.unwrap_or(0.0);
            if current_conf > existing_conf {
                keep[existing_idx] = false;
                seen.insert(key, i);
            } else {
                keep[i] = false;
            }
        } else {
            seen.insert(key, i);
        }
    }

    // Remove duplicates in reverse order to maintain indices
    for i in (0..cards.len()).rev() {
        if !keep[i] {
            cards.remove(i);
        }
    }

    let removed = original_count - cards.len();
    (cards, removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extractor_default() {
        let extractor = TripletExtractor::default();
        assert_eq!(*extractor.mode(), ExtractionMode::Rules);
        assert!(extractor.is_enabled());
    }

    #[test]
    fn test_extractor_disabled() {
        let extractor = TripletExtractor::disabled();
        assert!(!extractor.is_enabled());

        let (cards, stats) = extractor.extract(1, "I work at Anthropic", None, None, 0);
        assert!(cards.is_empty());
        assert_eq!(stats.total_stored, 0);
    }

    #[test]
    fn test_extractor_rules() {
        let extractor = TripletExtractor::rules_only();

        let (cards, stats) = extractor.extract(
            1,
            "I work at Anthropic. I live in San Francisco.",
            Some("mv2://test/1"),
            Some("Test"),
            1700000000,
        );

        assert!(!cards.is_empty());
        assert!(stats.rules_extracted > 0);
        assert_eq!(stats.llm_extracted, 0);

        // Verify card structure
        let employer_card = cards.iter().find(|c| c.slot == "employer");
        assert!(employer_card.is_some());
        let card = employer_card.unwrap();
        assert_eq!(card.entity, "user");
        assert_eq!(card.value, "Anthropic");
        assert_eq!(card.source_frame_id, 1);
    }

    #[test]
    fn test_extractor_no_matches() {
        let extractor = TripletExtractor::rules_only();

        let (cards, stats) = extractor.extract(1, "The weather is nice today.", None, None, 0);

        assert!(cards.is_empty());
        assert_eq!(stats.rules_extracted, 0);
    }

    #[test]
    fn test_deduplicate_cards() {
        use crate::types::MemoryCardBuilder;

        let cards = vec![
            MemoryCardBuilder::new()
                .fact()
                .entity("user")
                .slot("employer")
                .value("Company A")
                .source(1, None)
                .engine("rules", "1.0.0")
                .confidence(0.8)
                .build(0)
                .unwrap(),
            MemoryCardBuilder::new()
                .fact()
                .entity("user")
                .slot("employer")
                .value("Company B")
                .source(1, None)
                .engine("rules", "1.0.0")
                .confidence(0.9) // Higher confidence
                .build(0)
                .unwrap(),
        ];

        let (unique, removed) = deduplicate_cards(cards);
        assert_eq!(unique.len(), 1);
        assert_eq!(removed, 1);
        assert_eq!(unique[0].value, "Company B"); // Higher confidence kept
    }
}
