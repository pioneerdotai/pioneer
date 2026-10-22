//! Triplet extraction types for structured knowledge extraction.
//!
//! Triplets are Subject-Predicate-Object relationships extracted from text.
//! They map directly to `MemoryCards`: entity=Subject, slot=Predicate, value=Object.

use serde::{Deserialize, Serialize};

/// Extraction mode for triplet extraction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionMode {
    /// Fast, offline pattern-based extraction (default).
    /// Uses regex patterns to identify common relationship patterns.
    Rules,
    /// LLM-based extraction for complex sentences.
    /// Requires an LLM model to be configured.
    Llm(String),
    /// Hybrid mode: run both rules and LLM, deduplicate results.
    /// Automatically enabled when LLM is configured.
    Hybrid,
    /// Extraction disabled.
    Disabled,
}

impl Default for ExtractionMode {
    fn default() -> Self {
        Self::Rules
    }
}

impl ExtractionMode {
    /// Check if extraction is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// Check if rules extraction should run.
    #[must_use]
    pub fn should_run_rules(&self) -> bool {
        matches!(self, Self::Rules | Self::Hybrid)
    }

    /// Check if LLM extraction should run.
    #[must_use]
    pub fn should_run_llm(&self) -> bool {
        matches!(self, Self::Llm(_) | Self::Hybrid)
    }

    /// Get the LLM model name if configured.
    #[must_use]
    pub fn llm_model(&self) -> Option<&str> {
        match self {
            Self::Llm(model) => Some(model),
            _ => None,
        }
    }
}

/// Statistics from a triplet extraction run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtractionStats {
    /// Number of triplets extracted via rules.
    pub rules_extracted: usize,
    /// Number of triplets extracted via LLM.
    pub llm_extracted: usize,
    /// Number of duplicate triplets removed.
    pub duplicates_removed: usize,
    /// Total triplets stored.
    pub total_stored: usize,
    /// Extraction time in milliseconds.
    pub extraction_time_ms: u64,
}

impl ExtractionStats {
    /// Create stats for rules-only extraction.
    #[must_use]
    pub fn from_rules(count: usize, time_ms: u64) -> Self {
        Self {
            rules_extracted: count,
            llm_extracted: 0,
            duplicates_removed: 0,
            total_stored: count,
            extraction_time_ms: time_ms,
        }
    }

    /// Merge stats from LLM extraction.
    pub fn add_llm(&mut self, count: usize) {
        self.llm_extracted = count;
        self.total_stored += count;
    }

    /// Record duplicate removal.
    pub fn record_dedup(&mut self, removed: usize) {
        self.duplicates_removed = removed;
        self.total_stored = self.total_stored.saturating_sub(removed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extraction_mode_default() {
        let mode = ExtractionMode::default();
        assert_eq!(mode, ExtractionMode::Rules);
        assert!(mode.is_enabled());
        assert!(mode.should_run_rules());
        assert!(!mode.should_run_llm());
    }

    #[test]
    fn test_extraction_mode_llm() {
        let mode = ExtractionMode::Llm("gpt-4".to_string());
        assert!(mode.is_enabled());
        assert!(!mode.should_run_rules());
        assert!(mode.should_run_llm());
        assert_eq!(mode.llm_model(), Some("gpt-4"));
    }

    #[test]
    fn test_extraction_mode_hybrid() {
        let mode = ExtractionMode::Hybrid;
        assert!(mode.is_enabled());
        assert!(mode.should_run_rules());
        assert!(mode.should_run_llm());
    }

    #[test]
    fn test_extraction_mode_disabled() {
        let mode = ExtractionMode::Disabled;
        assert!(!mode.is_enabled());
        assert!(!mode.should_run_rules());
        assert!(!mode.should_run_llm());
    }

    #[test]
    fn test_extraction_stats() {
        let mut stats = ExtractionStats::from_rules(5, 100);
        assert_eq!(stats.rules_extracted, 5);
        assert_eq!(stats.total_stored, 5);

        stats.add_llm(3);
        assert_eq!(stats.llm_extracted, 3);
        assert_eq!(stats.total_stored, 8);

        stats.record_dedup(2);
        assert_eq!(stats.duplicates_removed, 2);
        assert_eq!(stats.total_stored, 6);
    }
}
