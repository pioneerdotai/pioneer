//! Frame representations and timeline summarisation types.

use std::{collections::BTreeMap, fmt, marker::PhantomData, num::NonZeroU64};

use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};

#[cfg(feature = "temporal_track")]
use super::search::SearchHitTemporal;
#[cfg(feature = "temporal_track")]
use super::temporal::TemporalFilter;
use super::{
    common::{CanonicalEncoding, FrameId, FrameRole, FrameStatus, Tier},
    metadata::{DocMetadata, TextChunkManifest},
};

// Note: AnchorSource is always defined (not feature-gated) to maintain binary compatibility

/// Timeline query parameters for scanning frames chronologically or in reverse.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimelineQuery {
    pub limit: Option<NonZeroU64>,
    pub since: Option<i64>,
    pub until: Option<i64>,
    pub reverse: bool,
    #[cfg(feature = "temporal_track")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal: Option<TemporalFilter>,
}

impl TimelineQuery {
    /// Start a fluent builder for timeline queries.
    #[must_use]
    pub fn builder() -> TimelineQueryBuilder {
        TimelineQueryBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct TimelineQueryBuilder {
    inner: TimelineQuery,
}

impl TimelineQueryBuilder {
    #[must_use]
    pub fn limit(mut self, limit: NonZeroU64) -> Self {
        self.inner.limit = Some(limit);
        self
    }

    #[must_use]
    pub fn since(mut self, ts: i64) -> Self {
        self.inner.since = Some(ts);
        self
    }

    #[must_use]
    pub fn until(mut self, ts: i64) -> Self {
        self.inner.until = Some(ts);
        self
    }

    #[must_use]
    pub fn reverse(mut self, reverse: bool) -> Self {
        self.inner.reverse = reverse;
        self
    }

    #[cfg(feature = "temporal_track")]
    pub fn temporal(mut self, filter: TemporalFilter) -> Self {
        self.inner.temporal = Some(filter);
        self
    }

    #[must_use]
    pub fn no_limit(mut self) -> Self {
        self.inner.limit = None;
        self
    }

    #[must_use]
    pub fn build(mut self) -> TimelineQuery {
        if self.inner.limit.is_none() {
            self.inner.limit = NonZeroU64::new(100);
        }
        self.inner
    }
}

/// Public-facing statistics summarising a memory.
/// Aggregates counts, sizes, capacity, and index presence for quick health checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub frame_count: u64,
    pub size_bytes: u64,
    pub tier: Tier,
    pub has_lex_index: bool,
    pub has_vec_index: bool,
    pub has_clip_index: bool,
    pub has_time_index: bool,
    pub seq_no: Option<i64>,
    pub capacity_bytes: u64,
    #[serde(default)]
    pub active_frame_count: u64,
    #[serde(default)]
    pub payload_bytes: u64,
    #[serde(default)]
    pub logical_bytes: u64,
    #[serde(default)]
    pub saved_bytes: u64,
    #[serde(default)]
    pub compression_ratio_percent: f64,
    #[serde(default)]
    pub savings_percent: f64,
    #[serde(default)]
    pub storage_utilisation_percent: f64,
    #[serde(default)]
    pub remaining_capacity_bytes: u64,
    #[serde(default)]
    pub average_frame_payload_bytes: u64,
    #[serde(default)]
    pub average_frame_logical_bytes: u64,
    // PHASE 2: Detailed overhead breakdown for observability
    #[serde(default)]
    pub wal_bytes: u64,
    #[serde(default)]
    pub lex_index_bytes: u64,
    #[serde(default)]
    pub vec_index_bytes: u64,
    #[serde(default)]
    pub time_index_bytes: u64,
    #[serde(default)]
    pub vector_count: u64,
    /// Number of CLIP visual embeddings (images/PDF pages)
    #[serde(default)]
    pub clip_image_count: u64,
    /// Whether the lex (full-text) search engine is enabled at runtime.
    #[serde(default)]
    pub lex_enabled: bool,
    /// Whether the vec (vector/semantic) search engine is enabled at runtime.
    #[serde(default)]
    pub vec_enabled: bool,
}

/// Entry returned by `timeline` queries, carrying a lightweight preview.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub frame_id: FrameId,
    pub timestamp: i64,
    pub preview: String,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "deserialize_child_frames"
    )]
    pub child_frames: Vec<FrameId>,
    #[cfg(feature = "temporal_track")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal: Option<SearchHitTemporal>,
}

/// Frame - core content unit serialized to TOC.
/// binary format compatibility. Feature flags control functionality, NOT structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub id: FrameId,
    pub timestamp: i64,
    /// Temporal anchor timestamp. ALWAYS present - feature only controls if code uses it.
    #[serde(default)]
    pub anchor_ts: Option<i64>,
    /// Temporal anchor source. ALWAYS present - feature only controls if code uses it.
    #[serde(default)]
    pub anchor_source: Option<AnchorSource>,
    pub kind: Option<String>,
    pub track: Option<String>,
    pub payload_offset: u64,
    pub payload_length: u64,
    pub checksum: [u8; 32],
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub canonical_encoding: CanonicalEncoding,
    #[serde(default)]
    pub canonical_length: Option<u64>,
    #[serde(default)]
    pub metadata: Option<DocMetadata>,
    #[serde(default)]
    pub search_text: Option<String>,
    #[serde(default, deserialize_with = "deserialize_tags")]
    pub tags: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_labels")]
    pub labels: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_extra_metadata")]
    pub extra_metadata: BTreeMap<String, String>,
    #[serde(default, deserialize_with = "deserialize_content_dates")]
    pub content_dates: Vec<String>,
    #[serde(default)]
    pub chunk_manifest: Option<TextChunkManifest>,
    #[serde(default)]
    pub role: FrameRole,
    #[serde(default)]
    pub parent_id: Option<FrameId>,
    #[serde(default)]
    pub chunk_index: Option<u32>,
    #[serde(default)]
    pub chunk_count: Option<u32>,
    #[serde(default)]
    pub status: FrameStatus,
    #[serde(default)]
    pub supersedes: Option<FrameId>,
    #[serde(default)]
    pub superseded_by: Option<FrameId>,
    /// SHA-256 hash of original source file (set when --no-raw is used).
    /// Allows verification of source without storing the raw binary.
    #[serde(default)]
    pub source_sha256: Option<[u8; 32]>,
    /// Original source file path (set when --no-raw is used).
    /// Stored for reference; the actual binary is not in the memory file.
    #[serde(default)]
    pub source_path: Option<String>,
    /// Enrichment state for progressive ingestion.
    /// Frames start as Searchable and progress to Enriched in background.
    #[serde(default)]
    pub enrichment_state: super::common::EnrichmentState,
}

const MAX_CHILD_FRAMES: usize = 10_000;
const MAX_TAGS: usize = 1_024;
const MAX_LABELS: usize = 1_024;
const MAX_CONTENT_DATES: usize = 1_024;
const MAX_EXTRA_METADATA_ENTRIES: usize = 4_096;

fn deserialize_vec_bounded<'de, D, T, const LIMIT: usize>(
    deserializer: D,
) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVisitor<T, const LIMIT: usize>(PhantomData<T>);

    impl<'de, T, const LIMIT: usize> Visitor<'de> for BoundedVisitor<T, LIMIT>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a sequence with a bounded length")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut values = if let Some(size) = seq.size_hint() {
                if size > LIMIT {
                    return Err(de::Error::custom(format!(
                        "sequence length {size} exceeds bound {LIMIT}"
                    )));
                }
                Vec::with_capacity(size.min(LIMIT))
            } else {
                Vec::new()
            };
            while let Some(value) = seq.next_element()? {
                if values.len() == LIMIT {
                    return Err(de::Error::custom(format!(
                        "sequence length exceeds bound {LIMIT}"
                    )));
                }
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVisitor::<T, LIMIT>(PhantomData))
}

fn deserialize_child_frames<'de, D>(deserializer: D) -> Result<Vec<FrameId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_vec_bounded::<D, FrameId, MAX_CHILD_FRAMES>(deserializer)
}

fn deserialize_tags<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_vec_bounded::<D, String, MAX_TAGS>(deserializer)
}

fn deserialize_labels<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_vec_bounded::<D, String, MAX_LABELS>(deserializer)
}

fn deserialize_content_dates<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_vec_bounded::<D, String, MAX_CONTENT_DATES>(deserializer)
}

fn deserialize_extra_metadata<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct MapVisitor<const LIMIT: usize>;

    impl<'de, const LIMIT: usize> Visitor<'de> for MapVisitor<LIMIT> {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map with a bounded number of entries")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut values = BTreeMap::new();
            while let Some((key, value)) = map.next_entry()? {
                if values.len() == LIMIT {
                    return Err(de::Error::custom(format!(
                        "map entries exceed bound {LIMIT}"
                    )));
                }
                values.insert(key, value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_map(MapVisitor::<MAX_EXTRA_METADATA_ENTRIES>)
}

/// Source of temporal anchor for a frame.
/// ALWAYS defined - feature only controls if code uses it.
#[repr(u8)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnchorSource {
    Explicit,
    FrameTimestamp,
    Metadata,
    IngestionClock,
}
