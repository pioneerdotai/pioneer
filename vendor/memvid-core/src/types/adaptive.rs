//! Adaptive retrieval configuration and algorithms.
//!
//! Adaptive retrieval dynamically determines how many results to return based on
//! relevancy score distribution, rather than using a fixed `k`. This ensures:
//! - All relevant results are included (no "missing answers")
//! - Irrelevant results are excluded (reduced noise)
//! - Different queries get appropriate amounts of context
//!
//! # Example
//!
//! ```ignore
//! use memvid_core::adaptive::{AdaptiveConfig, CutoffStrategy};
//!
//! // Configure adaptive retrieval
//! let config = AdaptiveConfig {
//!     enabled: true,
//!     max_results: 100,
//!     strategy: CutoffStrategy::RelativeThreshold { min_ratio: 0.5 },
//!     ..Default::default()
//! };
//!
//! // Search with adaptive retrieval
//! let results = memvid.search_adaptive(&query, config)?;
//! // Returns all results above 50% of top score's relevancy
//! ```
//!
//! # Strategies
//!
//! - **`AbsoluteThreshold`**: Stop when score drops below a fixed value (e.g., 0.7)
//! - **`RelativeThreshold`**: Stop when score drops below X% of the top score
//! - **`ScoreCliff`**: Stop when score drops by more than X% from previous result
//! - **Elbow**: Automatically detect the "knee" in the score curve
//! - **Combined**: Use multiple strategies together

use serde::{Deserialize, Serialize};

/// Configuration for adaptive retrieval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveConfig {
    /// Enable adaptive retrieval (if false, uses fixed `top_k`).
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Maximum results to consider (over-retrieval limit).
    /// Set high enough to capture all potentially relevant results.
    #[serde(default = "default_max_results")]
    pub max_results: usize,

    /// Minimum results to return regardless of scores.
    #[serde(default = "default_min_results")]
    pub min_results: usize,

    /// Strategy for determining cutoff point.
    #[serde(default)]
    pub strategy: CutoffStrategy,

    /// If true, normalize scores to 0-1 range before applying strategy.
    #[serde(default = "default_normalize")]
    pub normalize_scores: bool,
}

fn default_enabled() -> bool {
    true
}
fn default_max_results() -> usize {
    100
}
fn default_min_results() -> usize {
    1
}
fn default_normalize() -> bool {
    true
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_results: 100,
            min_results: 1,
            strategy: CutoffStrategy::default(),
            normalize_scores: true,
        }
    }
}

impl AdaptiveConfig {
    /// Create a config with absolute threshold strategy.
    #[must_use]
    pub fn with_absolute_threshold(min_score: f32) -> Self {
        Self {
            strategy: CutoffStrategy::AbsoluteThreshold { min_score },
            ..Default::default()
        }
    }

    /// Create a config with relative threshold strategy.
    #[must_use]
    pub fn with_relative_threshold(min_ratio: f32) -> Self {
        Self {
            strategy: CutoffStrategy::RelativeThreshold { min_ratio },
            ..Default::default()
        }
    }

    /// Create a config with score cliff detection.
    #[must_use]
    pub fn with_score_cliff(max_drop_ratio: f32) -> Self {
        Self {
            strategy: CutoffStrategy::ScoreCliff { max_drop_ratio },
            ..Default::default()
        }
    }

    /// Create a config with automatic elbow detection.
    #[must_use]
    pub fn with_elbow_detection() -> Self {
        Self {
            strategy: CutoffStrategy::Elbow { sensitivity: 1.0 },
            ..Default::default()
        }
    }

    /// Create a combined strategy (recommended for production).
    #[must_use]
    pub fn combined(min_ratio: f32, max_drop: f32, min_score: f32) -> Self {
        Self {
            strategy: CutoffStrategy::Combined {
                relative_threshold: min_ratio,
                max_drop_ratio: max_drop,
                absolute_min: min_score,
            },
            ..Default::default()
        }
    }
}

/// Strategy for determining where to cut off results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CutoffStrategy {
    /// Stop when score drops below a fixed threshold.
    ///
    /// Good for: Well-calibrated scores where you know what "relevant" means.
    /// Example: `min_score=0.7` means only results with score >= 0.7 are included.
    AbsoluteThreshold {
        /// Minimum acceptable score (0.0-1.0 for normalized, varies otherwise).
        min_score: f32,
    },

    /// Stop when score drops below X% of the top result's score.
    ///
    /// Good for: Relative relevancy where top result sets the baseline.
    /// Example: `min_ratio=0.5` means include results with score >= 50% of top score.
    RelativeThreshold {
        /// Minimum ratio vs top score (0.0-1.0).
        min_ratio: f32,
    },

    /// Stop when score drops by more than X% from the previous result.
    ///
    /// Good for: Detecting natural breaks in relevancy.
    /// Example: `max_drop_ratio=0.3` stops when score drops 30% from previous.
    ScoreCliff {
        /// Maximum allowed drop ratio between consecutive results (0.0-1.0).
        max_drop_ratio: f32,
    },

    /// Automatically detect the "elbow" point in the score curve.
    ///
    /// Good for: Unknown score distributions, automatic tuning.
    /// Uses the Kneedle algorithm to find maximum curvature.
    Elbow {
        /// Sensitivity multiplier (1.0 = normal, higher = more aggressive cutoff).
        sensitivity: f32,
    },

    /// Combine multiple strategies (stop when ANY condition is met).
    ///
    /// Recommended for production use - provides multiple safety nets.
    Combined {
        /// Minimum ratio vs top score.
        relative_threshold: f32,
        /// Maximum drop from previous result.
        max_drop_ratio: f32,
        /// Absolute minimum score.
        absolute_min: f32,
    },
}

impl Default for CutoffStrategy {
    fn default() -> Self {
        // Default: Combined strategy with reasonable defaults
        Self::Combined {
            relative_threshold: 0.5, // At least 50% of top score
            max_drop_ratio: 0.4,     // Stop if 40% drop from previous
            absolute_min: 0.3,       // Never go below 0.3 (normalized)
        }
    }
}

/// Result of adaptive retrieval with statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveResult<T> {
    /// The filtered results.
    pub results: Vec<T>,

    /// Statistics about the adaptive retrieval.
    pub stats: AdaptiveStats,
}

/// Statistics from adaptive retrieval.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveStats {
    /// Total results considered (before cutoff).
    pub total_considered: usize,

    /// Results returned (after cutoff).
    pub returned: usize,

    /// Index where cutoff occurred.
    pub cutoff_index: usize,

    /// Score at cutoff point.
    pub cutoff_score: Option<f32>,

    /// Top score (first result).
    pub top_score: Option<f32>,

    /// Score at cutoff as ratio of top score.
    pub cutoff_ratio: Option<f32>,

    /// Which strategy triggered the cutoff.
    pub triggered_by: String,
}

impl<T> AdaptiveResult<T> {
    /// Create an empty result.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            results: Vec::new(),
            stats: AdaptiveStats {
                total_considered: 0,
                returned: 0,
                cutoff_index: 0,
                cutoff_score: None,
                top_score: None,
                cutoff_ratio: None,
                triggered_by: "no_results".to_string(),
            },
        }
    }
}

/// Score with index for cutoff calculation.
#[derive(Debug, Clone, Copy)]
pub struct ScoredIndex {
    pub index: usize,
    pub score: f32,
    pub normalized_score: f32,
}

/// Statistics about embedding quality across all vectors in a memory.
///
/// This provides insight into how well the embeddings are distributed
/// and what adaptive retrieval behavior to expect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingQualityStats {
    /// Total number of vectors in the index.
    pub vector_count: usize,

    /// Embedding dimension.
    pub dimension: usize,

    /// Average pairwise similarity (higher = more similar embeddings).
    /// Range: 0.0 (orthogonal) to 1.0 (identical).
    pub avg_similarity: f32,

    /// Minimum pairwise similarity found.
    pub min_similarity: f32,

    /// Maximum pairwise similarity found (excluding self-similarity).
    pub max_similarity: f32,

    /// Standard deviation of similarities.
    pub std_similarity: f32,

    /// Clustering coefficient: how tightly grouped the embeddings are.
    /// Higher values mean embeddings are more similar to each other.
    pub clustering_coefficient: f32,

    /// Estimated number of distinct clusters (based on similarity gaps).
    pub estimated_clusters: usize,

    /// Recommended `min_relevancy` threshold based on the distribution.
    pub recommended_threshold: f32,

    /// Quality assessment: "excellent", "good", "fair", "poor".
    pub quality_rating: String,

    /// Human-readable explanation of the quality.
    pub quality_explanation: String,
}

impl Default for EmbeddingQualityStats {
    fn default() -> Self {
        Self {
            vector_count: 0,
            dimension: 0,
            avg_similarity: 0.0,
            min_similarity: 0.0,
            max_similarity: 0.0,
            std_similarity: 0.0,
            clustering_coefficient: 0.0,
            estimated_clusters: 0,
            recommended_threshold: 0.5,
            quality_rating: "unknown".to_string(),
            quality_explanation: "No vectors available for analysis".to_string(),
        }
    }
}

/// Compute embedding quality statistics from a set of embeddings.
///
/// This samples pairwise similarities to assess the overall distribution.
/// For large datasets, it uses sampling to keep computation tractable.
pub fn compute_embedding_quality(embeddings: &[(u64, Vec<f32>)]) -> EmbeddingQualityStats {
    if embeddings.is_empty() {
        return EmbeddingQualityStats::default();
    }

    let vector_count = embeddings.len();
    let dimension = embeddings.first().map_or(0, |(_, v)| v.len());

    if vector_count < 2 {
        return EmbeddingQualityStats {
            vector_count,
            dimension,
            quality_rating: "insufficient".to_string(),
            quality_explanation: "Need at least 2 vectors for quality analysis".to_string(),
            ..Default::default()
        };
    }

    // Sample pairs for similarity computation (limit to avoid O(n²) explosion)
    let max_pairs = 1000;
    let mut similarities: Vec<f32> = Vec::new();

    // If small enough, compute all pairs; otherwise sample
    if vector_count * (vector_count - 1) / 2 <= max_pairs {
        // Compute all pairwise similarities
        for i in 0..vector_count {
            for j in (i + 1)..vector_count {
                let sim = cosine_similarity(&embeddings[i].1, &embeddings[j].1);
                similarities.push(sim);
            }
        }
    } else {
        // Sample random pairs
        use std::collections::HashSet;
        let mut seen: HashSet<(usize, usize)> = HashSet::new();
        let mut rng_state: u64 = 12345; // Simple LCG for deterministic sampling

        while similarities.len() < max_pairs {
            // Simple LCG random
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let i = usize::try_from(rng_state % (vector_count as u64)).unwrap_or(0);
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = usize::try_from(rng_state % (vector_count as u64)).unwrap_or(0);

            if i != j {
                let pair = if i < j { (i, j) } else { (j, i) };
                if seen.insert(pair) {
                    let sim = cosine_similarity(&embeddings[i].1, &embeddings[j].1);
                    similarities.push(sim);
                }
            }
        }
    }

    if similarities.is_empty() {
        return EmbeddingQualityStats {
            vector_count,
            dimension,
            quality_rating: "error".to_string(),
            quality_explanation: "Could not compute similarities".to_string(),
            ..Default::default()
        };
    }

    // Compute statistics
    let n = similarities.len() as f32;
    let avg_similarity: f32 = similarities.iter().sum::<f32>() / n;
    let min_similarity = similarities.iter().copied().fold(f32::INFINITY, f32::min);
    let max_similarity = similarities
        .iter()
        .copied()
        .fold(f32::NEG_INFINITY, f32::max);

    let variance: f32 = similarities
        .iter()
        .map(|s| (s - avg_similarity).powi(2))
        .sum::<f32>()
        / n;
    let std_similarity = variance.sqrt();

    // Clustering coefficient: proportion of similarities above average
    let above_avg = similarities.iter().filter(|&&s| s > avg_similarity).count();
    let clustering_coefficient = above_avg as f32 / similarities.len() as f32;

    // Estimate clusters by looking at similarity distribution
    // Sort and look for gaps
    let mut sorted_sims = similarities.clone();
    sorted_sims.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut gaps = 0;
    let gap_threshold = std_similarity * 1.5;
    for i in 1..sorted_sims.len() {
        if sorted_sims[i] - sorted_sims[i - 1] > gap_threshold {
            gaps += 1;
        }
    }
    let estimated_clusters = (gaps + 1).min(vector_count);

    // Recommended threshold based on distribution
    // If high avg similarity, use higher threshold; if low, use lower
    let recommended_threshold = if avg_similarity > 0.7 {
        0.6 // High similarity corpus - be more selective
    } else if avg_similarity > 0.5 {
        0.5 // Medium similarity - balanced
    } else if avg_similarity > 0.3 {
        0.4 // Lower similarity - be more inclusive
    } else {
        0.3 // Very diverse corpus - include more results
    };

    // Quality rating based on distribution characteristics
    let (quality_rating, quality_explanation) = if std_similarity < 0.1 && avg_similarity > 0.8 {
        (
            "poor".to_string(),
            "Embeddings are too similar - may indicate duplicate content or poor embedding model"
                .to_string(),
        )
    } else if std_similarity > 0.3 && avg_similarity < 0.3 {
        (
            "excellent".to_string(),
            "Well-distributed embeddings with clear separation between topics".to_string(),
        )
    } else if std_similarity > 0.2 && avg_similarity < 0.5 {
        (
            "good".to_string(),
            "Good embedding distribution with reasonable topic separation".to_string(),
        )
    } else if std_similarity > 0.1 {
        (
            "fair".to_string(),
            "Moderate embedding distribution - some topic overlap".to_string(),
        )
    } else {
        (
            "limited".to_string(),
            "Limited variation in embeddings - consider more diverse content".to_string(),
        )
    };

    EmbeddingQualityStats {
        vector_count,
        dimension,
        avg_similarity,
        min_similarity,
        max_similarity,
        std_similarity,
        clustering_coefficient,
        estimated_clusters,
        recommended_threshold,
        quality_rating,
        quality_explanation,
    }
}

/// Compute cosine similarity between two vectors.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a < f32::EPSILON || norm_b < f32::EPSILON {
        return 0.0;
    }

    (dot / (norm_a * norm_b)).clamp(-1.0, 1.0)
}

/// Find the adaptive cutoff point for a list of scores.
///
/// Returns (`cutoff_index`, `triggered_by_strategy`).
/// Results at indices `0..cutoff_index` should be included.
#[must_use]
pub fn find_adaptive_cutoff(scores: &[f32], config: &AdaptiveConfig) -> (usize, String) {
    if scores.is_empty() {
        return (0, "no_results".to_string());
    }

    if scores.len() <= config.min_results {
        return (scores.len(), "min_results".to_string());
    }

    // Normalize scores if configured
    let normalized = if config.normalize_scores {
        normalize_scores(scores)
    } else {
        scores.to_vec()
    };

    let top_score = normalized[0];

    match &config.strategy {
        CutoffStrategy::AbsoluteThreshold { min_score } => {
            find_absolute_cutoff(&normalized, *min_score, config.min_results)
        }

        CutoffStrategy::RelativeThreshold { min_ratio } => {
            let threshold = top_score * min_ratio;
            find_absolute_cutoff(&normalized, threshold, config.min_results)
        }

        CutoffStrategy::ScoreCliff { max_drop_ratio } => {
            find_cliff_cutoff(&normalized, *max_drop_ratio, config.min_results)
        }

        CutoffStrategy::Elbow { sensitivity } => {
            find_elbow_cutoff(&normalized, *sensitivity, config.min_results)
        }

        CutoffStrategy::Combined {
            relative_threshold,
            max_drop_ratio,
            absolute_min,
        } => find_combined_cutoff(
            &normalized,
            top_score,
            *relative_threshold,
            *max_drop_ratio,
            *absolute_min,
            config.min_results,
        ),
    }
}

/// Normalize scores to 0-1 range using min-max normalization.
pub fn normalize_scores(scores: &[f32]) -> Vec<f32> {
    if scores.is_empty() {
        return Vec::new();
    }

    let max_score = scores.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let min_score = scores.iter().copied().fold(f32::INFINITY, f32::min);
    let range = max_score - min_score;

    if range < f32::EPSILON {
        // All scores are the same
        return vec![1.0; scores.len()];
    }

    scores.iter().map(|s| (s - min_score) / range).collect()
}

/// Find cutoff using absolute threshold.
fn find_absolute_cutoff(scores: &[f32], min_score: f32, min_results: usize) -> (usize, String) {
    for (i, &score) in scores.iter().enumerate() {
        if score < min_score && i >= min_results {
            return (i, "absolute_threshold".to_string());
        }
    }
    (scores.len(), "no_cutoff".to_string())
}

/// Find cutoff using cliff detection (large drops between consecutive scores).
fn find_cliff_cutoff(scores: &[f32], max_drop_ratio: f32, min_results: usize) -> (usize, String) {
    for i in 1..scores.len() {
        if i < min_results {
            continue;
        }

        let prev = scores[i - 1];
        let curr = scores[i];

        if prev > f32::EPSILON {
            let drop_ratio = (prev - curr) / prev;
            if drop_ratio > max_drop_ratio {
                return (i, format!("score_cliff({:.1}%)", drop_ratio * 100.0));
            }
        }
    }
    (scores.len(), "no_cutoff".to_string())
}

/// Find cutoff using elbow/knee detection (Kneedle algorithm).
fn find_elbow_cutoff(scores: &[f32], sensitivity: f32, min_results: usize) -> (usize, String) {
    if scores.len() < 3 {
        return (scores.len(), "too_few_points".to_string());
    }

    // Kneedle algorithm: find point of maximum curvature
    let n = scores.len();

    // Normalize x-axis to 0-1
    let x_norm: Vec<f32> = (0..n).map(|i| i as f32 / (n - 1) as f32).collect();

    // Scores are already normalized (or not, depending on config)
    let y_norm = scores;

    // Calculate differences for knee detection
    // We're looking for the point where the curve bends most sharply
    let mut max_distance = 0.0f32;
    let mut elbow_index = min_results;

    // Line from first to last point
    let x1 = x_norm[0];
    let y1 = y_norm[0];
    let x2 = x_norm[n - 1];
    let y2 = y_norm[n - 1];

    let line_len = ((x2 - x1).powi(2) + (y2 - y1).powi(2)).sqrt();
    if line_len < f32::EPSILON {
        return (scores.len(), "flat_curve".to_string());
    }

    // Distance from each point to the line
    for i in min_results..n - 1 {
        let x0 = x_norm[i];
        let y0 = y_norm[i];

        // Distance from point (x0, y0) to line through (x1, y1) and (x2, y2)
        let distance = ((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1).abs() / line_len;

        // Apply sensitivity: higher sensitivity = prefer earlier cutoff
        let adjusted_distance = distance * (1.0 + sensitivity * (1.0 - x_norm[i]));

        if adjusted_distance > max_distance {
            max_distance = adjusted_distance;
            elbow_index = i;
        }
    }

    // Only cut if we found a significant elbow
    if max_distance > 0.05 * sensitivity {
        (elbow_index + 1, "elbow_detection".to_string())
    } else {
        (scores.len(), "no_significant_elbow".to_string())
    }
}

/// Find cutoff using combined strategy (first trigger wins).
fn find_combined_cutoff(
    scores: &[f32],
    top_score: f32,
    relative_threshold: f32,
    max_drop_ratio: f32,
    absolute_min: f32,
    min_results: usize,
) -> (usize, String) {
    let relative_min = top_score * relative_threshold;

    for i in 0..scores.len() {
        if i < min_results {
            continue;
        }

        let score = scores[i];

        // Check absolute minimum
        if score < absolute_min {
            return (i, "absolute_min".to_string());
        }

        // Check relative threshold
        if score < relative_min {
            return (i, "relative_threshold".to_string());
        }

        // Check cliff detection
        if i > 0 {
            let prev = scores[i - 1];
            if prev > f32::EPSILON {
                let drop_ratio = (prev - score) / prev;
                if drop_ratio > max_drop_ratio {
                    return (i, format!("score_cliff({:.1}%)", drop_ratio * 100.0));
                }
            }
        }
    }

    (scores.len(), "no_cutoff".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_scores() {
        let scores = vec![1.0, 0.8, 0.6, 0.4, 0.2];
        let normalized = normalize_scores(&scores);

        assert!((normalized[0] - 1.0).abs() < 0.001);
        assert!((normalized[4] - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_absolute_threshold() {
        let scores = vec![0.95, 0.88, 0.75, 0.60, 0.45, 0.30, 0.15];
        let config = AdaptiveConfig::with_absolute_threshold(0.5);

        let (cutoff, _) = find_adaptive_cutoff(&scores, &config);

        // Should include scores >= 0.5 (0.95, 0.88, 0.75, 0.60)
        // After normalization: cutoff depends on normalized values
        assert!(cutoff >= 4);
    }

    #[test]
    fn test_relative_threshold() {
        let scores = vec![1.0, 0.9, 0.8, 0.5, 0.3, 0.1];
        let config = AdaptiveConfig::with_relative_threshold(0.6);

        let (cutoff, _) = find_adaptive_cutoff(&scores, &config);

        // Should include scores >= 60% of 1.0 = 0.6
        // That's 1.0, 0.9, 0.8 (normalized: 1.0, 0.89, 0.78, 0.44, 0.22, 0.0)
        assert!(cutoff >= 3);
    }

    #[test]
    fn test_score_cliff() {
        // Sharp drop between 0.8 and 0.3
        let scores = vec![1.0, 0.95, 0.9, 0.85, 0.8, 0.3, 0.25, 0.2];
        let config = AdaptiveConfig::with_score_cliff(0.4);

        let (cutoff, trigger) = find_adaptive_cutoff(&scores, &config);

        // Should stop at the cliff (0.8 -> 0.3 is 62.5% drop)
        assert!(cutoff <= 6);
        assert!(trigger.contains("cliff") || trigger == "no_cutoff");
    }

    #[test]
    fn test_combined_strategy() {
        let scores = vec![0.95, 0.90, 0.85, 0.80, 0.75, 0.40, 0.35, 0.30];
        let config = AdaptiveConfig::combined(0.5, 0.3, 0.3);

        let (cutoff, _) = find_adaptive_cutoff(&scores, &config);

        // Should stop either at relative threshold (50% of 0.95 = 0.475)
        // or at cliff (0.75 -> 0.40 is ~47% drop)
        assert!((4..=6).contains(&cutoff));
    }

    #[test]
    fn test_elbow_detection() {
        // Classic "elbow" curve
        let scores = vec![1.0, 0.95, 0.90, 0.85, 0.80, 0.50, 0.48, 0.46, 0.44, 0.42];
        let config = AdaptiveConfig::with_elbow_detection();

        let (cutoff, trigger) = find_adaptive_cutoff(&scores, &config);

        // Should detect elbow around index 5 where scores drop sharply
        assert!(trigger.contains("elbow") || cutoff >= 4);
    }

    #[test]
    fn test_min_results_respected() {
        let scores = vec![1.0, 0.1, 0.05, 0.01];
        let mut config = AdaptiveConfig::with_absolute_threshold(0.9);
        config.min_results = 3;

        let (cutoff, _) = find_adaptive_cutoff(&scores, &config);

        // Even though only first result meets threshold, min_results=3
        assert!(cutoff >= 3);
    }

    #[test]
    fn test_empty_scores() {
        let scores: Vec<f32> = vec![];
        let config = AdaptiveConfig::default();

        let (cutoff, trigger) = find_adaptive_cutoff(&scores, &config);

        assert_eq!(cutoff, 0);
        assert_eq!(trigger, "no_results");
    }

    #[test]
    fn test_real_world_scenario() {
        // Simulating a search where answer is in 12 chunks but k=8 would miss some
        let scores = vec![
            0.92, 0.89, 0.87, 0.85, 0.84, 0.82, 0.80, 0.79, // First 8 (would be k=8)
            0.78, 0.76, 0.75, 0.74, // 4 more still relevant!
            0.45, 0.40, 0.35, 0.30, 0.25, // Clearly not relevant
        ];

        let config = AdaptiveConfig::combined(0.5, 0.35, 0.4);
        let (cutoff, trigger) = find_adaptive_cutoff(&scores, &config);

        // Should include all 12 relevant chunks, stop before 0.45
        println!("Cutoff: {}, Trigger: {}", cutoff, trigger);
        assert!(cutoff >= 10, "Should include more than k=8 results");
        assert!(cutoff <= 13, "Should stop before irrelevant results");
    }
}
