//! Foundational enums and marker types shared across memvid data structures.

use std::{marker::PhantomData, path::PathBuf};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Frame IDs are dense u64 indexes into the frame list.
pub type FrameId = u64;

/// Segment IDs identify embedded index segments; monotonic within a file.
pub type SegmentId = u64;

/// Encoding used for the canonical document bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalEncoding {
    Plain,
    Zstd,
}

impl CanonicalEncoding {
    #[must_use]
    pub const fn from_byte(value: u8) -> Self {
        match value {
            0 => CanonicalEncoding::Plain,
            1 => CanonicalEncoding::Zstd,
            _ => CanonicalEncoding::Plain,
        }
    }

    #[must_use]
    pub const fn as_byte(self) -> u8 {
        match self {
            CanonicalEncoding::Plain => 0,
            CanonicalEncoding::Zstd => 1,
        }
    }
}

impl Default for CanonicalEncoding {
    fn default() -> Self {
        Self::Plain
    }
}

impl Serialize for CanonicalEncoding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(u32::from(self.as_byte()))
    }
}

impl<'de> Deserialize<'de> for CanonicalEncoding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        Ok(CanonicalEncoding::from_byte((value & 0xFF) as u8))
    }
}

/// Tier captures the capacity and entitlement envelope for a memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// Free tier with small capacity.
    Free,
    /// Developer tier with higher caps.
    Dev,
    /// Enterprise tier with the largest caps.
    Enterprise,
    /// Unlimited tier with no nominal capacity ceiling.
    Unlimited,
}

impl Tier {
    /// Maximum nominal capacity in bytes for the tier.
    #[must_use]
    pub fn capacity_bytes(self) -> u64 {
        match self {
            Tier::Free => 50 * 1024 * 1024,              // 50 MB
            Tier::Dev => 2 * 1024 * 1024 * 1024,         // 2 GB
            Tier::Enterprise => 10 * 1024 * 1024 * 1024, // 10 GB
            Tier::Unlimited => u64::MAX,
        }
    }
}

/// Marker type signifying an open (mutable) memory.
pub struct Open;

/// Marker type signifying a sealed (read-only) memory.
pub struct Sealed;

/// Mode phantom tracked using [`MemvidHandle<Mode>`].
#[derive(Debug, Clone)]
pub struct MemvidHandle<Mode> {
    pub path: PathBuf,
    pub(crate) _mode: PhantomData<Mode>,
}

/// Marker describing the lifecycle state of a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameStatus {
    Active,
    Superseded,
    Deleted,
}

impl Default for FrameStatus {
    fn default() -> Self {
        Self::Active
    }
}

/// Role attributed to a frame in the timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FrameRole {
    #[default]
    Document,
    DocumentChunk,
    /// Extracted image from a document (e.g., PDF page image for CLIP)
    ExtractedImage,
}

/// Enrichment state for progressive ingestion.
///
/// Frames start as `Searchable` (instant indexed with skim text) and
/// progress to `Enriched` (full text + embeddings + memory cards).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum EnrichmentState {
    /// Phase 1 complete: searchable via skim text.
    /// Lexical search works, but may have reduced accuracy.
    #[default]
    Searchable = 0,
    /// Phase 2 complete: full text extracted, embeddings generated.
    /// Full search accuracy, semantic search available.
    Enriched = 1,
}

impl EnrichmentState {
    /// Returns true if this frame needs background enrichment.
    #[must_use]
    pub fn needs_enrichment(&self) -> bool {
        matches!(self, Self::Searchable)
    }

    /// Returns true if this frame has full semantic search capability.
    #[must_use]
    pub fn has_embeddings(&self) -> bool {
        matches!(self, Self::Enriched)
    }
}

/// Task in the enrichment queue, representing pending background work.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentTask {
    /// Frame ID to enrich.
    pub frame_id: FrameId,
    /// Timestamp when task was created.
    pub created_at: u64,
    /// Number of chunks already embedded (for resume after crash).
    pub chunks_done: u32,
    /// Total chunks to embed (0 if not yet known).
    pub chunks_total: u32,
}
