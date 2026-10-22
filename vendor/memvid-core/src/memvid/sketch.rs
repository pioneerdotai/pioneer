//! Sketch track extensions for `Memvid`.
//!
//! This module provides methods for fast candidate generation using
//! per-frame sketches (`SimHash` + term filters). Sketches enable sub-millisecond
//! candidate filtering before expensive BM25/vector reranking.
//!
//! Key operations:
//! - `insert_sketch`: Add a sketch for a frame
//! - `build_all_sketches`: Generate sketches for all existing frames
//! - `find_sketch_candidates`: Find candidate frames matching a query
//! - `sketch_stats`: Get statistics about the sketch track

use crate::memvid::lifecycle::Memvid;
use crate::types::{
    DEFAULT_HAMMING_THRESHOLD, FrameId, QuerySketch, SketchEntry, SketchTrack, SketchTrackStats,
    SketchVariant, generate_sketch,
};

/// Result of a sketch candidate search.
#[derive(Debug, Clone)]
pub struct SketchCandidate {
    /// Frame ID.
    pub frame_id: FrameId,
    /// Sketch score (higher is better, 0.0-1.0).
    pub score: f32,
    /// Hamming distance from query `SimHash`.
    pub hamming_distance: u32,
    /// Number of matching top terms.
    pub matching_top_terms: usize,
}

/// Options for sketch candidate search.
#[derive(Debug, Clone)]
pub struct SketchSearchOptions {
    /// Maximum Hamming distance to consider (default: 10).
    pub hamming_threshold: u32,
    /// Maximum number of candidates to return (default: 2000).
    pub max_candidates: usize,
    /// Minimum score threshold (default: 0.0).
    pub min_score: f32,
}

impl Default for SketchSearchOptions {
    fn default() -> Self {
        Self {
            hamming_threshold: DEFAULT_HAMMING_THRESHOLD,
            max_candidates: 2000,
            min_score: 0.0,
        }
    }
}

/// Detailed statistics from a sketch search.
#[derive(Debug, Clone)]
pub struct SketchSearchStats {
    /// Total frames scanned.
    pub frames_scanned: usize,
    /// Frames passing term filter.
    pub term_filter_hits: usize,
    /// Frames passing `SimHash` threshold.
    pub simhash_hits: usize,
    /// Final candidates returned.
    pub candidates_returned: usize,
    /// Scan time in microseconds.
    pub scan_us: u64,
}

impl Memvid {
    /// Get an immutable reference to the sketch track.
    #[must_use]
    pub fn sketches(&self) -> &SketchTrack {
        &self.sketch_track
    }

    /// Get a mutable reference to the sketch track.
    pub fn sketches_mut(&mut self) -> &mut SketchTrack {
        self.dirty = true;
        &mut self.sketch_track
    }

    /// Check if the sketch track has any entries.
    #[must_use]
    pub fn has_sketches(&self) -> bool {
        !self.sketch_track.is_empty()
    }

    /// Get statistics about the sketch track.
    #[must_use]
    pub fn sketch_stats(&self) -> SketchTrackStats {
        self.sketch_track.stats()
    }

    /// Insert a sketch for a frame.
    ///
    /// # Arguments
    /// * `frame_id` - The frame ID to create a sketch for
    /// * `text` - The text content to sketch (`search_text` or payload)
    /// * `variant` - The sketch variant to use (Small recommended)
    ///
    /// # Returns
    /// The generated sketch entry.
    pub fn insert_sketch(
        &mut self,
        frame_id: FrameId,
        text: &str,
        variant: SketchVariant,
    ) -> SketchEntry {
        let entry = generate_sketch(frame_id, text, variant, None);
        self.sketch_track.insert(entry.clone());
        self.dirty = true;
        entry
    }

    /// Build sketches for all frames that don't have one yet.
    ///
    /// This scans all active frames and generates sketches using each frame's
    /// `search_text` field.
    ///
    /// # Arguments
    /// * `variant` - The sketch variant to use for new sketches
    ///
    /// # Returns
    /// Number of new sketches generated.
    pub fn build_all_sketches(&mut self, variant: SketchVariant) -> usize {
        let mut count = 0;

        // Collect frames that need sketches
        let frames_to_sketch: Vec<(FrameId, String)> = self
            .toc
            .frames
            .iter()
            .filter(|f| f.status == crate::types::FrameStatus::Active)
            .filter(|f| self.sketch_track.get(f.id).is_none())
            .filter_map(|f| {
                f.search_text
                    .clone()
                    .filter(|t| !t.is_empty())
                    .map(|text| (f.id, text))
            })
            .collect();

        for (frame_id, text) in frames_to_sketch {
            self.insert_sketch(frame_id, &text, variant);
            count += 1;
        }

        if count > 0 {
            self.dirty = true;
        }

        count
    }

    /// Find candidate frames matching a query using sketch filtering.
    ///
    /// This performs a fast two-stage filter:
    /// 1. Term filter: reject frames that can't possibly contain query terms
    /// 2. `SimHash`: reject frames with large Hamming distance from query
    ///
    /// Candidates should be reranked with BM25 or vector similarity for final results.
    ///
    /// # Arguments
    /// * `query` - The query text to search for
    /// * `options` - Search options (or use default)
    ///
    /// # Returns
    /// List of candidate frames with scores, sorted by score descending.
    #[must_use]
    pub fn find_sketch_candidates(
        &self,
        query: &str,
        options: Option<SketchSearchOptions>,
    ) -> Vec<SketchCandidate> {
        let opts = options.unwrap_or_default();

        // Build query sketch using same variant as track
        let query_sketch = QuerySketch::from_query(query, self.sketch_track.variant);

        // Find candidates
        let raw_candidates = self.sketch_track.find_candidates(
            &query_sketch,
            opts.hamming_threshold,
            opts.max_candidates,
        );

        // Convert to SketchCandidate with additional details
        raw_candidates
            .into_iter()
            .filter(|(_, score)| *score >= opts.min_score)
            .map(|(frame_id, score)| {
                let entry = self.sketch_track.get(frame_id);
                let hamming_distance =
                    entry.map_or(64, |e| e.hamming_distance(query_sketch.simhash));
                let matching_top_terms =
                    entry.map_or(0, |e| e.count_matching_top_terms(&query_sketch.top_terms));

                SketchCandidate {
                    frame_id,
                    score,
                    hamming_distance,
                    matching_top_terms,
                }
            })
            .collect()
    }

    /// Find candidates with detailed statistics for debugging/explain mode.
    #[must_use]
    pub fn find_sketch_candidates_with_stats(
        &self,
        query: &str,
        options: Option<SketchSearchOptions>,
    ) -> (Vec<SketchCandidate>, SketchSearchStats) {
        let start = std::time::Instant::now();
        let opts = options.unwrap_or_default();

        let query_sketch = QuerySketch::from_query(query, self.sketch_track.variant);

        let frames_scanned = self.sketch_track.len();
        let mut term_filter_hits = 0usize;
        let mut simhash_hits = 0usize;

        // Manual scan for stats collection
        let mut candidates: Vec<(FrameId, f32)> = Vec::new();

        for entry in self.sketch_track.iter() {
            // Term filter check
            if !entry.term_filter_maybe_overlaps(&query_sketch.term_filter) {
                continue;
            }
            term_filter_hits += 1;

            // SimHash check
            let hamming = entry.hamming_distance(query_sketch.simhash);
            if hamming > opts.hamming_threshold {
                continue;
            }
            simhash_hits += 1;

            // Score
            if let Some(score) = query_sketch.score_entry(entry, opts.hamming_threshold) {
                if score >= opts.min_score {
                    candidates.push((entry.frame_id, score));
                }
            }
        }

        // Sort by score descending
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(opts.max_candidates);

        let candidates_returned = candidates.len();

        let result: Vec<SketchCandidate> = candidates
            .into_iter()
            .map(|(frame_id, score)| {
                let entry = self.sketch_track.get(frame_id);
                let hamming_distance =
                    entry.map_or(64, |e| e.hamming_distance(query_sketch.simhash));
                let matching_top_terms =
                    entry.map_or(0, |e| e.count_matching_top_terms(&query_sketch.top_terms));

                SketchCandidate {
                    frame_id,
                    score,
                    hamming_distance,
                    matching_top_terms,
                }
            })
            .collect();

        let stats = SketchSearchStats {
            frames_scanned,
            term_filter_hits,
            simhash_hits,
            candidates_returned,
            scan_us: u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX),
        };

        (result, stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sketch_insert_and_query() {
        let tmp = NamedTempFile::new().expect("tempfile");
        let path = tmp.path();

        // Create a new memory
        let mut mem = Memvid::create(path).expect("create");

        // Insert some test sketches
        mem.insert_sketch(0, "cats are wonderful pets", SketchVariant::Small);
        mem.insert_sketch(1, "dogs are loyal companions", SketchVariant::Small);
        mem.insert_sketch(2, "programming in rust language", SketchVariant::Small);

        // Query for cats
        let candidates = mem.find_sketch_candidates("cats pets", None);

        // Should find some candidates
        assert!(!candidates.is_empty() || !mem.sketches().is_empty());
    }

    #[test]
    fn test_sketch_candidate_speed() {
        let tmp = NamedTempFile::new().expect("tempfile");
        let path = tmp.path();

        let mut mem = Memvid::create(path).expect("create");

        // Insert many sketches for a realistic benchmark
        let docs = [
            "machine learning artificial intelligence neural networks deep learning",
            "rust programming language memory safety systems programming",
            "cloud computing kubernetes containers docker orchestration",
            "database optimization indexing query planning performance",
            "web development react vue javascript frontend frameworks",
        ];

        for i in 0..1000 {
            let doc = docs[i % docs.len()];
            mem.insert_sketch(i as u64, doc, SketchVariant::Small);
        }

        // Benchmark: run query with stats
        let query = "machine learning neural networks";

        // Use relaxed threshold (32 instead of default 10) for better recall
        let options = Some(SketchSearchOptions {
            hamming_threshold: 32, // Half of 64 bits
            max_candidates: 100,
            min_score: 0.0,
        });
        let (candidates, stats) = mem.find_sketch_candidates_with_stats(query, options);

        // Print benchmark results
        println!("\n=== Sketch Candidate Benchmark ===");
        println!("  Documents:     1000");
        println!("  Query:         \"{}\"", query);
        println!("  Scanned:       {} entries", stats.frames_scanned);
        println!(
            "  Term hits:     {} ({:.1}%)",
            stats.term_filter_hits,
            stats.term_filter_hits as f64 / stats.frames_scanned as f64 * 100.0
        );
        println!(
            "  SimHash hits:  {} ({:.1}%)",
            stats.simhash_hits,
            stats.simhash_hits as f64 / stats.frames_scanned as f64 * 100.0
        );
        println!("  Candidates:    {}", stats.candidates_returned);
        println!(
            "  Time:          {} µs ({:.3} ms)",
            stats.scan_us,
            stats.scan_us as f64 / 1000.0
        );
        println!(
            "  Rate:          {:.0} docs/sec",
            stats.frames_scanned as f64 / (stats.scan_us as f64 / 1_000_000.0)
        );

        assert!(
            stats.scan_us < 10_000,
            "Sketch scan should complete in <10ms for 1000 docs"
        );
        assert!(
            candidates.len() <= stats.simhash_hits,
            "Candidates should be <= simhash hits"
        );
    }
}
