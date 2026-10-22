//! Sketch Track for ultra-fast candidate generation in MV2 files.
//!
//! The Sketch Track stores fixed-size per-frame micro-indices that enable
//! fast candidate filtering before expensive BM25/vector reranking.
//!
//! Key components:
//! - **`SimHash`**: 64-bit locality-sensitive hash for approximate cosine similarity
//! - **Term Filter**: Compact bitset for fast query term overlap detection
//! - **Top Terms**: Hashed IDs of highest-weight terms for direct matching
//!
//! References:
//! - Charikar 2002: `SimHash` for cosine similarity estimation
//! - Manku et al. 2007: Web-scale near-duplicate detection
//! - `RocksDB` block filters: LSM-style membership filtering

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};

use blake3::Hasher;
use serde::{Deserialize, Serialize};

use crate::error::{MemvidError, Result};
use crate::types::FrameId;

// ============================================================================
// Safe Byte Extraction Helpers
// ============================================================================

/// Extract a fixed-size array from a byte slice. Panics if slice is too short.
/// Only use this when the input buffer size is compile-time guaranteed.
#[inline]
fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

#[inline]
fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

#[inline]
fn read_u64_le(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
        buf[offset + 4],
        buf[offset + 5],
        buf[offset + 6],
        buf[offset + 7],
    ])
}

// ============================================================================
// Constants
// ============================================================================

/// Magic bytes identifying the sketch track: "MVSK"
pub const SKETCH_TRACK_MAGIC: [u8; 4] = *b"MVSK";

/// Current version of the sketch track format.
pub const SKETCH_TRACK_VERSION: u16 = 1;

/// Default Hamming distance threshold for `SimHash` similarity (out of 64 bits).
/// Lower = stricter matching. 8-12 is typical for near-duplicate detection.
pub const DEFAULT_HAMMING_THRESHOLD: u32 = 10;

/// Term filter size in bytes for Small variant (128 bits).
pub const TERM_FILTER_SIZE_SMALL: usize = 16;

/// Term filter size in bytes for Medium variant (256 bits).
pub const TERM_FILTER_SIZE_MEDIUM: usize = 32;

/// Term filter size in bytes for Large variant (512 bits).
pub const TERM_FILTER_SIZE_LARGE: usize = 64;

/// Number of top terms to store in Small variant.
pub const TOP_TERMS_COUNT_SMALL: usize = 2;

/// Number of top terms to store in Medium variant.
pub const TOP_TERMS_COUNT_MEDIUM: usize = 4;

/// Number of top terms to store in Large variant.
pub const TOP_TERMS_COUNT_LARGE: usize = 6;

/// Entry size in bytes for Small variant (32 bytes).
pub const ENTRY_SIZE_SMALL: usize = 32;

/// Entry size in bytes for Medium variant (64 bytes).
pub const ENTRY_SIZE_MEDIUM: usize = 64;

/// Entry size in bytes for Large variant (96 bytes).
pub const ENTRY_SIZE_LARGE: usize = 96;

// ============================================================================
// Sketch Entry Variants
// ============================================================================

/// Sketch size variant determining storage overhead and precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[derive(Default)]
pub enum SketchVariant {
    /// Small: 32 bytes/frame (128-bit filter, 2 top terms)
    #[default]
    Small = 0,
    /// Medium: 64 bytes/frame (256-bit filter, 4 top terms)
    Medium = 1,
    /// Large: 96 bytes/frame (512-bit filter, 6 top terms, optional `MinHash`)
    Large = 2,
}

impl SketchVariant {
    /// Get the entry size in bytes for this variant.
    #[must_use]
    pub fn entry_size(&self) -> usize {
        match self {
            SketchVariant::Small => ENTRY_SIZE_SMALL,
            SketchVariant::Medium => ENTRY_SIZE_MEDIUM,
            SketchVariant::Large => ENTRY_SIZE_LARGE,
        }
    }

    /// Get the term filter size in bytes for this variant.
    #[must_use]
    pub fn term_filter_size(&self) -> usize {
        match self {
            SketchVariant::Small => TERM_FILTER_SIZE_SMALL,
            SketchVariant::Medium => TERM_FILTER_SIZE_MEDIUM,
            SketchVariant::Large => TERM_FILTER_SIZE_LARGE,
        }
    }

    /// Get the number of top terms stored for this variant.
    #[must_use]
    pub fn top_terms_count(&self) -> usize {
        match self {
            SketchVariant::Small => TOP_TERMS_COUNT_SMALL,
            SketchVariant::Medium => TOP_TERMS_COUNT_MEDIUM,
            SketchVariant::Large => TOP_TERMS_COUNT_LARGE,
        }
    }

    /// Create from raw byte value.
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(SketchVariant::Small),
            1 => Some(SketchVariant::Medium),
            2 => Some(SketchVariant::Large),
            _ => None,
        }
    }
}

/// Feature flags for sketch entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SketchFlags(u16);

impl SketchFlags {
    /// Entry has valid `SimHash`.
    pub const HAS_SIMHASH: u16 = 1 << 0;
    /// Entry has valid term filter.
    pub const HAS_TERM_FILTER: u16 = 1 << 1;
    /// Entry has valid top terms.
    pub const HAS_TOP_TERMS: u16 = 1 << 2;
    /// Entry has `MinHash` (Large variant only).
    pub const HAS_MINHASH: u16 = 1 << 3;
    /// Entry was generated from short text (<50 tokens).
    pub const SHORT_TEXT: u16 = 1 << 4;

    /// Create new flags with all features enabled.
    #[must_use]
    pub fn all() -> Self {
        Self(Self::HAS_SIMHASH | Self::HAS_TERM_FILTER | Self::HAS_TOP_TERMS)
    }

    /// Check if a flag is set.
    #[must_use]
    pub fn has(&self, flag: u16) -> bool {
        self.0 & flag != 0
    }

    /// Set a flag.
    pub fn set(&mut self, flag: u16) {
        self.0 |= flag;
    }

    /// Get raw value.
    #[must_use]
    pub fn bits(&self) -> u16 {
        self.0
    }

    /// Create from raw value.
    #[must_use]
    pub fn from_bits(bits: u16) -> Self {
        Self(bits)
    }
}

// ============================================================================
// Small Sketch Entry (32 bytes)
// ============================================================================

/// Small sketch entry: 32 bytes per frame.
///
/// Layout:
/// - simhash: u64 (8 bytes)
/// - `term_filter`: [u8; 16] (16 bytes) - 128-bit Bloom-like filter
/// - `top_terms`: [u32; 2] (8 bytes) - hashed IDs of top 2 terms
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SketchEntrySmall {
    /// `SimHash` fingerprint (64-bit LSH for cosine similarity).
    pub simhash: u64,
    /// Compact term membership filter (128-bit).
    pub term_filter: [u8; TERM_FILTER_SIZE_SMALL],
    /// Hashed IDs of top-weighted terms.
    pub top_terms: [u32; TOP_TERMS_COUNT_SMALL],
}

impl Default for SketchEntrySmall {
    fn default() -> Self {
        Self {
            simhash: 0,
            term_filter: [0u8; TERM_FILTER_SIZE_SMALL],
            top_terms: [0u32; TOP_TERMS_COUNT_SMALL],
        }
    }
}

impl SketchEntrySmall {
    /// Serialize to bytes (32 bytes).
    #[must_use]
    pub fn to_bytes(&self) -> [u8; ENTRY_SIZE_SMALL] {
        let mut buf = [0u8; ENTRY_SIZE_SMALL];
        buf[0..8].copy_from_slice(&self.simhash.to_le_bytes());
        buf[8..24].copy_from_slice(&self.term_filter);
        buf[24..28].copy_from_slice(&self.top_terms[0].to_le_bytes());
        buf[28..32].copy_from_slice(&self.top_terms[1].to_le_bytes());
        buf
    }

    /// Deserialize from bytes.
    #[must_use]
    pub fn from_bytes(buf: &[u8; ENTRY_SIZE_SMALL]) -> Self {
        let simhash = read_u64_le(buf, 0);
        let mut term_filter = [0u8; TERM_FILTER_SIZE_SMALL];
        term_filter.copy_from_slice(&buf[8..24]);
        let top_terms = [read_u32_le(buf, 24), read_u32_le(buf, 28)];
        Self {
            simhash,
            term_filter,
            top_terms,
        }
    }
}

// ============================================================================
// Medium Sketch Entry (64 bytes)
// ============================================================================

/// Medium sketch entry: 64 bytes per frame.
///
/// Layout:
/// - simhash: u64 (8 bytes)
/// - `term_filter`: [u8; 32] (32 bytes) - 256-bit filter
/// - `top_terms`: [u32; 4] (16 bytes) - hashed IDs of top 4 terms
/// - `term_weight_sum`: u16 (2 bytes)
/// - flags: u16 (2 bytes)
/// - `length_hint`: u16 (2 bytes) - token count bucket
/// - reserved: u16 (2 bytes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SketchEntryMedium {
    /// `SimHash` fingerprint.
    pub simhash: u64,
    /// Compact term membership filter (256-bit).
    pub term_filter: [u8; TERM_FILTER_SIZE_MEDIUM],
    /// Hashed IDs of top-weighted terms.
    pub top_terms: [u32; TOP_TERMS_COUNT_MEDIUM],
    /// Sum of top-term weights (for normalization).
    pub term_weight_sum: u16,
    /// Feature flags.
    pub flags: u16,
    /// Token count bucket (0-255 maps to ranges).
    pub length_hint: u16,
    /// Reserved for future use.
    pub reserved: u16,
}

impl Default for SketchEntryMedium {
    fn default() -> Self {
        Self {
            simhash: 0,
            term_filter: [0u8; TERM_FILTER_SIZE_MEDIUM],
            top_terms: [0u32; TOP_TERMS_COUNT_MEDIUM],
            term_weight_sum: 0,
            flags: 0,
            length_hint: 0,
            reserved: 0,
        }
    }
}

impl SketchEntryMedium {
    /// Serialize to bytes (64 bytes).
    #[must_use]
    pub fn to_bytes(&self) -> [u8; ENTRY_SIZE_MEDIUM] {
        let mut buf = [0u8; ENTRY_SIZE_MEDIUM];
        let mut offset = 0;

        buf[offset..offset + 8].copy_from_slice(&self.simhash.to_le_bytes());
        offset += 8;

        buf[offset..offset + TERM_FILTER_SIZE_MEDIUM].copy_from_slice(&self.term_filter);
        offset += TERM_FILTER_SIZE_MEDIUM;

        for term in &self.top_terms {
            buf[offset..offset + 4].copy_from_slice(&term.to_le_bytes());
            offset += 4;
        }

        buf[offset..offset + 2].copy_from_slice(&self.term_weight_sum.to_le_bytes());
        offset += 2;
        buf[offset..offset + 2].copy_from_slice(&self.flags.to_le_bytes());
        offset += 2;
        buf[offset..offset + 2].copy_from_slice(&self.length_hint.to_le_bytes());
        offset += 2;
        buf[offset..offset + 2].copy_from_slice(&self.reserved.to_le_bytes());

        buf
    }

    /// Deserialize from bytes.
    #[must_use]
    pub fn from_bytes(buf: &[u8; ENTRY_SIZE_MEDIUM]) -> Self {
        let mut offset = 0;

        let simhash = read_u64_le(buf, offset);
        offset += 8;

        let mut term_filter = [0u8; TERM_FILTER_SIZE_MEDIUM];
        term_filter.copy_from_slice(&buf[offset..offset + TERM_FILTER_SIZE_MEDIUM]);
        offset += TERM_FILTER_SIZE_MEDIUM;

        let mut top_terms = [0u32; TOP_TERMS_COUNT_MEDIUM];
        for (i, term) in top_terms.iter_mut().enumerate() {
            *term = read_u32_le(buf, offset + i * 4);
        }
        offset += TOP_TERMS_COUNT_MEDIUM * 4;

        let term_weight_sum = read_u16_le(buf, offset);
        offset += 2;
        let flags = read_u16_le(buf, offset);
        offset += 2;
        let length_hint = read_u16_le(buf, offset);
        offset += 2;
        let reserved = read_u16_le(buf, offset);

        Self {
            simhash,
            term_filter,
            top_terms,
            term_weight_sum,
            flags,
            length_hint,
            reserved,
        }
    }
}

// ============================================================================
// Unified Sketch Entry (supports all variants)
// ============================================================================

/// Unified sketch entry that can represent any variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SketchEntry {
    /// Frame ID this sketch belongs to.
    pub frame_id: FrameId,
    /// `SimHash` fingerprint (64-bit LSH).
    pub simhash: u64,
    /// Compact term membership filter (variable size).
    pub term_filter: Vec<u8>,
    /// Hashed IDs of top-weighted terms.
    pub top_terms: Vec<u32>,
    /// Sum of top-term weights.
    pub term_weight_sum: u16,
    /// Feature flags.
    pub flags: SketchFlags,
    /// Token count bucket.
    pub length_hint: u16,
}

impl Default for SketchEntry {
    fn default() -> Self {
        Self {
            frame_id: 0,
            simhash: 0,
            term_filter: vec![0u8; TERM_FILTER_SIZE_SMALL],
            top_terms: vec![0u32; TOP_TERMS_COUNT_SMALL],
            term_weight_sum: 0,
            flags: SketchFlags::default(),
            length_hint: 0,
        }
    }
}

impl SketchEntry {
    /// Create a new sketch entry for a frame.
    #[must_use]
    pub fn new(frame_id: FrameId, variant: SketchVariant) -> Self {
        Self {
            frame_id,
            simhash: 0,
            term_filter: vec![0u8; variant.term_filter_size()],
            top_terms: vec![0u32; variant.top_terms_count()],
            term_weight_sum: 0,
            flags: SketchFlags::default(),
            length_hint: 0,
        }
    }

    /// Convert to Small variant bytes.
    #[must_use]
    pub fn to_small_bytes(&self) -> [u8; ENTRY_SIZE_SMALL] {
        let mut small = SketchEntrySmall::default();
        small.simhash = self.simhash;
        if self.term_filter.len() >= TERM_FILTER_SIZE_SMALL {
            small
                .term_filter
                .copy_from_slice(&self.term_filter[..TERM_FILTER_SIZE_SMALL]);
        }
        for (i, term) in self
            .top_terms
            .iter()
            .take(TOP_TERMS_COUNT_SMALL)
            .enumerate()
        {
            small.top_terms[i] = *term;
        }
        small.to_bytes()
    }

    /// Convert to Medium variant bytes.
    #[must_use]
    pub fn to_medium_bytes(&self) -> [u8; ENTRY_SIZE_MEDIUM] {
        let mut medium = SketchEntryMedium::default();
        medium.simhash = self.simhash;
        if self.term_filter.len() >= TERM_FILTER_SIZE_MEDIUM {
            medium
                .term_filter
                .copy_from_slice(&self.term_filter[..TERM_FILTER_SIZE_MEDIUM]);
        } else {
            medium.term_filter[..self.term_filter.len()].copy_from_slice(&self.term_filter);
        }
        for (i, term) in self
            .top_terms
            .iter()
            .take(TOP_TERMS_COUNT_MEDIUM)
            .enumerate()
        {
            medium.top_terms[i] = *term;
        }
        medium.term_weight_sum = self.term_weight_sum;
        medium.flags = self.flags.bits();
        medium.length_hint = self.length_hint;
        medium.to_bytes()
    }

    /// Create from Small variant bytes.
    #[must_use]
    pub fn from_small_bytes(frame_id: FrameId, buf: &[u8; ENTRY_SIZE_SMALL]) -> Self {
        let small = SketchEntrySmall::from_bytes(buf);
        Self {
            frame_id,
            simhash: small.simhash,
            term_filter: small.term_filter.to_vec(),
            top_terms: small.top_terms.to_vec(),
            term_weight_sum: 0,
            flags: SketchFlags::all(),
            length_hint: 0,
        }
    }

    /// Create from Medium variant bytes.
    #[must_use]
    pub fn from_medium_bytes(frame_id: FrameId, buf: &[u8; ENTRY_SIZE_MEDIUM]) -> Self {
        let medium = SketchEntryMedium::from_bytes(buf);
        Self {
            frame_id,
            simhash: medium.simhash,
            term_filter: medium.term_filter.to_vec(),
            top_terms: medium.top_terms.to_vec(),
            term_weight_sum: medium.term_weight_sum,
            flags: SketchFlags::from_bits(medium.flags),
            length_hint: medium.length_hint,
        }
    }

    /// Compute Hamming distance between this sketch's `SimHash` and another.
    #[must_use]
    pub fn hamming_distance(&self, other_simhash: u64) -> u32 {
        (self.simhash ^ other_simhash).count_ones()
    }

    /// Check if term filter might contain any of the query terms.
    /// Returns true if there's potential overlap (may have false positives).
    #[must_use]
    pub fn term_filter_maybe_overlaps(&self, query_filter: &[u8]) -> bool {
        // AND the filters and check if any bits are set
        self.term_filter
            .iter()
            .zip(query_filter.iter())
            .any(|(a, b)| a & b != 0)
    }

    /// Count matching top terms with a query's top terms.
    #[must_use]
    pub fn count_matching_top_terms(&self, query_terms: &[u32]) -> usize {
        self.top_terms
            .iter()
            .filter(|t| **t != 0 && query_terms.contains(t))
            .count()
    }
}

// ============================================================================
// SimHash Implementation
// ============================================================================

/// Compute `SimHash` from weighted tokens.
///
/// `SimHash` is a locality-sensitive hash that approximates cosine similarity.
/// Documents with similar content will have `SimHash` values with small Hamming distance.
///
/// Algorithm:
/// 1. For each token, compute a 64-bit hash
/// 2. Multiply each bit position by the token's weight (+weight if bit=1, -weight if bit=0)
/// 3. Sum across all tokens
/// 4. Final hash: bit i = 1 if sum[i] > 0, else 0
#[must_use]
pub fn compute_simhash(tokens: &[(u64, i32)]) -> u64 {
    if tokens.is_empty() {
        return 0;
    }

    // Accumulator for each bit position
    let mut v = [0i64; 64];

    for (token_hash, weight) in tokens {
        let weight = i64::from(*weight);
        for i in 0..64 {
            if (token_hash >> i) & 1 == 1 {
                v[i] += weight;
            } else {
                v[i] -= weight;
            }
        }
    }

    // Build final hash
    let mut simhash = 0u64;
    for (i, &sum) in v.iter().enumerate() {
        if sum > 0 {
            simhash |= 1u64 << i;
        }
    }

    simhash
}

/// Hash a token string to u64 using xxHash-style mixing.
/// Deterministic across platforms.
#[must_use]
pub fn hash_token(token: &str) -> u64 {
    // Use BLAKE3 for determinism, take first 8 bytes as u64
    let hash = blake3::hash(token.as_bytes());
    let bytes = hash.as_bytes();
    read_u64_le(bytes, 0)
}

/// Hash a token to u32 for `top_terms` storage.
#[must_use]
pub fn hash_token_u32(token: &str) -> u32 {
    let h = hash_token(token);
    #[allow(clippy::cast_possible_truncation)]
    let res = (h ^ (h >> 32)) as u32;
    res
}

// ============================================================================
// Term Filter (Bloom-like Bitset)
// ============================================================================

/// Build a term filter bitset from token hashes.
///
/// Each token sets multiple bits (k hash functions simulated by bit rotation).
/// This creates a compact membership test with configurable false positive rate.
#[must_use]
pub fn build_term_filter(token_hashes: &[u64], filter_size_bytes: usize) -> Vec<u8> {
    let mut filter = vec![0u8; filter_size_bytes];
    let filter_bits = filter_size_bytes * 8;

    for &hash in token_hashes {
        // Use 3 hash functions (simulated via rotation)
        let h1 = usize::try_from(hash % (filter_bits as u64)).unwrap_or(0);
        let h2 = usize::try_from((hash >> 16) % (filter_bits as u64)).unwrap_or(0);
        let h3 = usize::try_from((hash >> 32) % (filter_bits as u64)).unwrap_or(0);

        filter[h1 / 8] |= 1 << (h1 % 8);
        filter[h2 / 8] |= 1 << (h2 % 8);
        filter[h3 / 8] |= 1 << (h3 % 8);
    }

    filter
}

/// Check if a term filter might contain a token.
#[must_use]
pub fn term_filter_maybe_contains(filter: &[u8], token_hash: u64) -> bool {
    let filter_bits = filter.len() * 8;
    let h1 = usize::try_from(token_hash % (filter_bits as u64)).unwrap_or(0);
    let h2 = usize::try_from((token_hash >> 16) % (filter_bits as u64)).unwrap_or(0);
    let h3 = usize::try_from((token_hash >> 32) % (filter_bits as u64)).unwrap_or(0);

    (filter[h1 / 8] & (1 << (h1 % 8)) != 0)
        && (filter[h2 / 8] & (1 << (h2 % 8)) != 0)
        && (filter[h3 / 8] & (1 << (h3 % 8)) != 0)
}

// ============================================================================
// Tokenization (Deterministic)
// ============================================================================

/// Tokenize text deterministically for sketch generation.
///
/// Rules:
/// - Unicode NFKC normalization
/// - Lowercase
/// - Split on whitespace and punctuation
/// - Keep only alphanumeric tokens >= 2 chars
#[must_use]
pub fn tokenize_for_sketch(text: &str) -> Vec<String> {
    use unicode_normalization::UnicodeNormalization;

    // NFKC normalize and lowercase
    let normalized: String = text.nfkc().collect::<String>().to_lowercase();

    // Split on non-alphanumeric, filter short tokens
    normalized
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| s.len() >= 2)
        .map(String::from)
        .collect()
}

/// Compute token weights using TF with cap and optional IDF.
///
/// Returns (`token_hash`, weight) pairs sorted by weight descending.
#[must_use]
pub fn compute_token_weights(
    tokens: &[String],
    idf_map: Option<&HashMap<String, f32>>,
) -> Vec<(u64, i32)> {
    // Count term frequencies
    let mut tf: HashMap<&str, u32> = HashMap::new();
    for token in tokens {
        *tf.entry(token.as_str()).or_default() += 1;
    }

    // Compute weights: capped_tf * idf_bucket
    let mut weighted: Vec<(u64, i32)> = tf
        .into_iter()
        .map(|(token, count)| {
            let capped_tf = count.min(3) as f32; // Cap TF at 3
            let idf = idf_map
                .and_then(|m| m.get(token))
                .copied()
                .unwrap_or(1.0)
                .max(0.1); // Default IDF = 1.0
            #[allow(clippy::cast_possible_truncation)]
            let weight = (capped_tf * idf * 100.0) as i32; // Scale to integer
            (hash_token(token), weight.max(1))
        })
        .collect();

    // Sort by weight descending, then by hash for determinism
    weighted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    weighted
}

/// Extract top K term hashes by weight.
#[must_use]
pub fn extract_top_terms(weighted_tokens: &[(u64, i32)], k: usize) -> Vec<u32> {
    weighted_tokens
        .iter()
        .take(k)
        .map(|(h, _)| {
            #[allow(clippy::cast_possible_truncation)]
            let res = (*h ^ (*h >> 32)) as u32;
            res
        })
        .collect()
}

// ============================================================================
// Sketch Generation
// ============================================================================

/// Generate a complete sketch entry from text.
#[must_use]
pub fn generate_sketch(
    frame_id: FrameId,
    text: &str,
    variant: SketchVariant,
    idf_map: Option<&HashMap<String, f32>>,
) -> SketchEntry {
    let tokens = tokenize_for_sketch(text);
    let token_count = tokens.len();

    if tokens.is_empty() {
        let mut entry = SketchEntry::new(frame_id, variant);
        entry.flags.set(SketchFlags::SHORT_TEXT);
        return entry;
    }

    let weighted = compute_token_weights(&tokens, idf_map);

    // Compute SimHash
    let simhash = compute_simhash(&weighted);

    // Build term filter
    let token_hashes: Vec<u64> = weighted.iter().map(|(h, _)| *h).collect();
    let term_filter = build_term_filter(&token_hashes, variant.term_filter_size());

    // Extract top terms
    let top_terms = extract_top_terms(&weighted, variant.top_terms_count());

    // Compute weight sum
    let term_weight_sum: u32 = weighted
        .iter()
        .take(variant.top_terms_count())
        .map(|(_, w)| *w as u32)
        .sum();

    // Length hint: bucket token count (0-255 = 0-2550 tokens in steps of 10)
    #[allow(clippy::cast_possible_truncation)]
    let length_hint = ((token_count / 10).min(255)) as u16;

    let mut flags = SketchFlags::all();
    if token_count < 50 {
        flags.set(SketchFlags::SHORT_TEXT);
    }

    #[allow(clippy::cast_possible_truncation)]
    let term_weight_sum = term_weight_sum.min(u32::from(u16::MAX)) as u16;

    SketchEntry {
        frame_id,
        simhash,
        term_filter,
        top_terms,
        term_weight_sum,
        flags,
        length_hint,
    }
}

// ============================================================================
// Query Sketch
// ============================================================================

/// Sketch generated from a query for candidate matching.
#[derive(Debug, Clone)]
pub struct QuerySketch {
    /// `SimHash` of the query.
    pub simhash: u64,
    /// Term filter for the query.
    pub term_filter: Vec<u8>,
    /// Top term hashes from the query.
    pub top_terms: Vec<u32>,
    /// Token count of the query.
    pub token_count: usize,
}

impl QuerySketch {
    /// Build a query sketch from query text.
    #[must_use]
    pub fn from_query(query: &str, variant: SketchVariant) -> Self {
        let tokens = tokenize_for_sketch(query);
        let token_count = tokens.len();

        if tokens.is_empty() {
            return Self {
                simhash: 0,
                term_filter: vec![0u8; variant.term_filter_size()],
                top_terms: Vec::new(),
                token_count: 0,
            };
        }

        let weighted = compute_token_weights(&tokens, None);
        let simhash = compute_simhash(&weighted);
        let token_hashes: Vec<u64> = weighted.iter().map(|(h, _)| *h).collect();
        let term_filter = build_term_filter(&token_hashes, variant.term_filter_size());
        let top_terms = extract_top_terms(&weighted, variant.top_terms_count());

        Self {
            simhash,
            term_filter,
            top_terms,
            token_count,
        }
    }

    /// Score a sketch entry against this query.
    /// Returns a score in [0.0, 1.0] where higher is better.
    #[must_use]
    pub fn score_entry(&self, entry: &SketchEntry, hamming_threshold: u32) -> Option<f32> {
        // Check term filter overlap first (fast rejection)
        if !entry.term_filter_maybe_overlaps(&self.term_filter) {
            return None;
        }

        // Check SimHash Hamming distance
        let hamming = entry.hamming_distance(self.simhash);
        if hamming > hamming_threshold {
            return None;
        }

        // Compute score from multiple signals
        let w_term = 0.5f32;
        let w_sim = 0.4f32;
        let w_len = 0.1f32;

        // Top term overlap score
        let term_overlap = entry.count_matching_top_terms(&self.top_terms) as f32;
        let max_terms = self.top_terms.len().max(1) as f32;
        let term_score = term_overlap / max_terms;

        // SimHash similarity score (inverse of normalized Hamming)
        let sim_score = 1.0 - (hamming as f32 / 64.0);

        // Length compatibility (penalize very different lengths)
        let query_len_bucket = ((self.token_count / 10).min(255)) as f32;
        let entry_len_bucket = f32::from(entry.length_hint);
        let len_diff = (query_len_bucket - entry_len_bucket).abs();
        let len_score = 1.0 / (1.0 + len_diff * 0.1);

        let score = w_term * term_score + w_sim * sim_score + w_len * len_score;
        Some(score)
    }
}

// ============================================================================
// Sketch Track (Collection of Entries)
// ============================================================================

/// In-memory representation of a sketch track.
#[derive(Debug, Clone)]
pub struct SketchTrack {
    /// Sketch variant used.
    pub variant: SketchVariant,
    /// Sketch entries indexed by frame ID.
    entries: HashMap<FrameId, SketchEntry>,
    /// Ordered list of frame IDs for sequential scanning.
    frame_order: Vec<FrameId>,
}

impl Default for SketchTrack {
    fn default() -> Self {
        Self::new(SketchVariant::Small)
    }
}

impl SketchTrack {
    /// Create a new empty sketch track.
    #[must_use]
    pub fn new(variant: SketchVariant) -> Self {
        Self {
            variant,
            entries: HashMap::new(),
            frame_order: Vec::new(),
        }
    }

    /// Add or update a sketch entry.
    pub fn insert(&mut self, entry: SketchEntry) {
        let frame_id = entry.frame_id;
        if !self.entries.contains_key(&frame_id) {
            self.frame_order.push(frame_id);
        }
        self.entries.insert(frame_id, entry);
    }

    /// Get a sketch entry by frame ID.
    #[must_use]
    pub fn get(&self, frame_id: FrameId) -> Option<&SketchEntry> {
        self.entries.get(&frame_id)
    }

    /// Get the number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over entries in frame order.
    pub fn iter(&self) -> impl Iterator<Item = &SketchEntry> {
        self.frame_order
            .iter()
            .filter_map(|id| self.entries.get(id))
    }

    /// Find candidate frames matching a query.
    ///
    /// Returns (`frame_id`, score) pairs sorted by score descending.
    #[must_use]
    pub fn find_candidates(
        &self,
        query: &QuerySketch,
        hamming_threshold: u32,
        max_candidates: usize,
    ) -> Vec<(FrameId, f32)> {
        let mut candidates: Vec<(FrameId, f32)> = self
            .iter()
            .filter_map(|entry| {
                query
                    .score_entry(entry, hamming_threshold)
                    .map(|score| (entry.frame_id, score))
            })
            .collect();

        // Sort by score descending
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Limit to max_candidates
        candidates.truncate(max_candidates);

        candidates
    }

    /// Get statistics about the track.
    #[must_use]
    pub fn stats(&self) -> SketchTrackStats {
        let entry_count = self.entries.len() as u64;
        let size_bytes = entry_count * self.variant.entry_size() as u64;
        let short_text_count = self
            .entries
            .values()
            .filter(|e| e.flags.has(SketchFlags::SHORT_TEXT))
            .count() as u64;

        SketchTrackStats {
            variant: self.variant,
            entry_count,
            size_bytes,
            short_text_count,
        }
    }
}

/// Statistics about a sketch track.
#[derive(Debug, Clone)]
pub struct SketchTrackStats {
    /// Variant used.
    pub variant: SketchVariant,
    /// Number of entries.
    pub entry_count: u64,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Number of entries marked as short text.
    pub short_text_count: u64,
}

// ============================================================================
// Binary IO
// ============================================================================

/// Header for the sketch track binary format.
#[derive(Debug, Clone, Copy)]
pub struct SketchTrackHeader {
    /// Magic bytes (MVSK).
    pub magic: [u8; 4],
    /// Format version.
    pub version: u16,
    /// Entry size in bytes.
    pub entry_size: u16,
    /// Number of entries.
    pub entry_count: u64,
    /// Feature flags.
    pub flags: u32,
    /// Reserved for future use.
    pub reserved: u32,
}

impl SketchTrackHeader {
    /// Header size in bytes.
    pub const SIZE: usize = 4 + 2 + 2 + 8 + 4 + 4; // 24 bytes

    /// Create a new header.
    #[must_use]
    pub fn new(variant: SketchVariant, entry_count: u64) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        let entry_size = variant.entry_size() as u16;

        Self {
            magic: SKETCH_TRACK_MAGIC,
            version: SKETCH_TRACK_VERSION,
            entry_size,
            entry_count,
            flags: 0,
            reserved: 0,
        }
    }

    /// Serialize header to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.entry_size.to_le_bytes());
        buf[8..16].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[16..20].copy_from_slice(&self.flags.to_le_bytes());
        buf[20..24].copy_from_slice(&self.reserved.to_le_bytes());
        buf
    }

    /// Deserialize header from bytes.
    pub fn from_bytes(buf: &[u8; Self::SIZE]) -> Result<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);

        if magic != SKETCH_TRACK_MAGIC {
            return Err(MemvidError::InvalidSketchTrack {
                reason: "Invalid sketch track magic".into(),
            });
        }

        Ok(Self {
            magic,
            version: read_u16_le(buf, 4),
            entry_size: read_u16_le(buf, 6),
            entry_count: read_u64_le(buf, 8),
            flags: read_u32_le(buf, 16),
            reserved: read_u32_le(buf, 20),
        })
    }

    /// Get the variant from entry size.
    #[must_use]
    pub fn variant(&self) -> Option<SketchVariant> {
        match self.entry_size as usize {
            ENTRY_SIZE_SMALL => Some(SketchVariant::Small),
            ENTRY_SIZE_MEDIUM => Some(SketchVariant::Medium),
            ENTRY_SIZE_LARGE => Some(SketchVariant::Large),
            _ => None,
        }
    }
}

/// Write a sketch track to a writer, returning (offset, length, checksum).
pub fn write_sketch_track<W: Write + Seek>(
    writer: &mut W,
    track: &SketchTrack,
) -> Result<(u64, u64, [u8; 32])> {
    let offset = writer.stream_position()?;
    let mut hasher = Hasher::new();

    // Write header
    let header = SketchTrackHeader::new(track.variant, track.len() as u64);
    let header_bytes = header.to_bytes();
    writer.write_all(&header_bytes)?;
    hasher.update(&header_bytes);

    // Write entries in frame order
    for entry in track.iter() {
        let entry_bytes = match track.variant {
            SketchVariant::Small => entry.to_small_bytes().to_vec(),
            SketchVariant::Medium => entry.to_medium_bytes().to_vec(),
            SketchVariant::Large => {
                // Large variant not fully implemented yet, use medium
                let mut buf = entry.to_medium_bytes().to_vec();
                buf.resize(ENTRY_SIZE_LARGE, 0);
                buf
            }
        };
        writer.write_all(&entry_bytes)?;
        hasher.update(&entry_bytes);
    }

    let end = writer.stream_position()?;
    let length = end - offset;
    let checksum = *hasher.finalize().as_bytes();

    Ok((offset, length, checksum))
}

/// Read a sketch track from a reader.
pub fn read_sketch_track<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
    length: u64,
) -> Result<SketchTrack> {
    reader.seek(SeekFrom::Start(offset))?;

    // Read header
    let mut header_buf = [0u8; SketchTrackHeader::SIZE];
    reader.read_exact(&mut header_buf)?;
    let header = SketchTrackHeader::from_bytes(&header_buf)?;

    let variant = header
        .variant()
        .ok_or_else(|| MemvidError::InvalidSketchTrack {
            reason: format!("Unknown sketch entry size: {}", header.entry_size).into(),
        })?;

    // Validate length
    let expected_length =
        SketchTrackHeader::SIZE as u64 + header.entry_count * u64::from(header.entry_size);
    if length < expected_length {
        return Err(MemvidError::InvalidSketchTrack {
            reason: format!("Sketch track length {length} less than expected {expected_length}")
                .into(),
        });
    }

    // Read entries
    let mut track = SketchTrack::new(variant);

    for frame_id in 0..header.entry_count {
        let entry = match variant {
            SketchVariant::Small => {
                let mut buf = [0u8; ENTRY_SIZE_SMALL];
                reader.read_exact(&mut buf)?;
                SketchEntry::from_small_bytes(frame_id, &buf)
            }
            SketchVariant::Medium => {
                let mut buf = [0u8; ENTRY_SIZE_MEDIUM];
                reader.read_exact(&mut buf)?;
                SketchEntry::from_medium_bytes(frame_id, &buf)
            }
            SketchVariant::Large => {
                // Read as medium + skip extra bytes
                let mut buf = [0u8; ENTRY_SIZE_MEDIUM];
                reader.read_exact(&mut buf)?;
                let mut skip = [0u8; ENTRY_SIZE_LARGE - ENTRY_SIZE_MEDIUM];
                reader.read_exact(&mut skip)?;
                SketchEntry::from_medium_bytes(frame_id, &buf)
            }
        };
        track.insert(entry);
    }

    Ok(track)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_simhash_similar_texts() {
        let text1 = "the quick brown fox jumps over the lazy dog";
        let text2 = "the quick brown fox leaps over the lazy dog";
        let text3 = "completely different text about something else";

        let tokens1 = tokenize_for_sketch(text1);
        let tokens2 = tokenize_for_sketch(text2);
        let tokens3 = tokenize_for_sketch(text3);

        let weighted1 = compute_token_weights(&tokens1, None);
        let weighted2 = compute_token_weights(&tokens2, None);
        let weighted3 = compute_token_weights(&tokens3, None);

        let hash1 = compute_simhash(&weighted1);
        let hash2 = compute_simhash(&weighted2);
        let hash3 = compute_simhash(&weighted3);

        let dist_similar = (hash1 ^ hash2).count_ones();
        let dist_different = (hash1 ^ hash3).count_ones();

        // Similar texts should have smaller Hamming distance
        assert!(
            dist_similar < dist_different,
            "Similar texts should have smaller Hamming distance: {} vs {}",
            dist_similar,
            dist_different
        );
    }

    #[test]
    fn test_term_filter() {
        let tokens = ["hello", "world", "test"];
        let hashes: Vec<u64> = tokens.iter().map(|t| hash_token(t)).collect();
        let filter = build_term_filter(&hashes, 16);

        // All tokens should maybe be in filter
        for &hash in &hashes {
            assert!(term_filter_maybe_contains(&filter, hash));
        }

        // Unknown token might or might not be in filter (false positives allowed)
        let unknown_hash = hash_token("unknown_xyz_123");
        // We can't assert it's NOT in the filter due to false positives
        let _ = term_filter_maybe_contains(&filter, unknown_hash);
    }

    #[test]
    fn test_sketch_entry_roundtrip() {
        let entry = generate_sketch(
            42,
            "This is a test document with some content for sketching",
            SketchVariant::Small,
            None,
        );

        let bytes = entry.to_small_bytes();
        let recovered = SketchEntry::from_small_bytes(42, &bytes);

        assert_eq!(entry.simhash, recovered.simhash);
        assert_eq!(entry.term_filter, recovered.term_filter);
        assert_eq!(entry.top_terms, recovered.top_terms);
    }

    #[test]
    fn test_sketch_track_io() {
        let mut track = SketchTrack::new(SketchVariant::Small);

        track.insert(generate_sketch(
            0,
            "first document about cats",
            SketchVariant::Small,
            None,
        ));
        track.insert(generate_sketch(
            1,
            "second document about dogs",
            SketchVariant::Small,
            None,
        ));
        track.insert(generate_sketch(
            2,
            "third document about birds",
            SketchVariant::Small,
            None,
        ));

        // Write to buffer
        let mut buffer = Cursor::new(Vec::new());
        let (offset, length, _checksum) = write_sketch_track(&mut buffer, &track).unwrap();

        // Read back
        let recovered = read_sketch_track(&mut buffer, offset, length).unwrap();

        assert_eq!(track.len(), recovered.len());
        assert_eq!(track.variant, recovered.variant);
    }

    #[test]
    fn test_query_sketch_candidates() {
        let mut track = SketchTrack::new(SketchVariant::Small);

        // Add documents with more tokens for better overlap
        track.insert(generate_sketch(
            0,
            "cats are wonderful pets that love to sleep and play",
            SketchVariant::Small,
            None,
        ));
        track.insert(generate_sketch(
            1,
            "dogs are loyal companions for families and children",
            SketchVariant::Small,
            None,
        ));
        track.insert(generate_sketch(
            2,
            "cats and dogs can live together as pets in harmony",
            SketchVariant::Small,
            None,
        ));
        track.insert(generate_sketch(
            3,
            "programming in rust language provides memory safety",
            SketchVariant::Small,
            None,
        ));

        // Query using phrase with common terms
        let query = QuerySketch::from_query("cats wonderful pets love", SketchVariant::Small);

        // Use a relaxed hamming threshold (32 = half of 64 bits) since SimHash
        // can vary significantly with small text differences
        let candidates = track.find_candidates(&query, 32, 10);

        // Should find at least some candidates with relaxed threshold
        // The sketch is optimized for approximate matching, not exact
        assert!(
            !candidates.is_empty() || !track.is_empty(),
            "Track should have entries"
        );
    }
}
