//! Builder-style options used when writing frames into a memory.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::common::{FrameId, FrameRole};
use super::metadata::DocMetadata;

fn default_true() -> bool {
    true
}

/// Tunable options for writing frames into a memory.
/// Attach metadata, control embeddings, auto-tagging, and URI/title hints. Builders make it
/// easy to set only what you need.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutOptions {
    pub timestamp: Option<i64>,
    pub track: Option<String>,
    pub kind: Option<String>,
    pub uri: Option<String>,
    pub title: Option<String>,
    #[serde(default)]
    pub metadata: Option<DocMetadata>,
    #[serde(default)]
    pub search_text: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub extra_metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub enable_embedding: bool,
    #[serde(default = "default_true")]
    pub auto_tag: bool,
    #[serde(default = "default_true")]
    pub extract_dates: bool,
    /// Extract triplets (Subject-Predicate-Object) from text and store as `MemoryCards`.
    /// Enabled by default. Triplets enable O(1) entity lookups and graph queries.
    #[serde(default = "default_true")]
    pub extract_triplets: bool,
    /// Parent frame ID for child frames (e.g., extracted images from a PDF)
    #[serde(default)]
    pub parent_id: Option<FrameId>,
    /// Role of the frame (defaults to Document)
    #[serde(default)]
    pub role: FrameRole,
    /// Don't store raw binary content, only extracted text + SHA256 hash.
    /// Saves storage for documents where only searchable text is needed.
    #[serde(default)]
    pub no_raw: bool,
    /// Original source file path (for --no-raw reference tracking).
    #[serde(default)]
    pub source_path: Option<String>,
    /// Skip ingestion if a frame with matching BLAKE3 hash already exists.
    /// When enabled, returns the existing frame's sequence number instead of creating a duplicate.
    #[serde(default)]
    pub dedup: bool,
    /// Enable instant indexing for immediate searchability (<1s).
    /// When enabled, performs a soft Tantivy commit after WAL append.
    /// Frame becomes searchable immediately but full enrichment happens in background.
    /// Default: true for single-doc `put()`, false for `put_many()` batch.
    #[serde(default = "default_true")]
    pub instant_index: bool,
    /// Time budget for text extraction in milliseconds.
    /// When `instant_index` is enabled, extraction stops after this time.
    /// 0 means no budget (extract everything).
    /// Default: 350ms (optimized for sub-second total ingestion).
    #[serde(default = "default_extraction_budget_ms")]
    pub extraction_budget_ms: u64,
}

fn default_extraction_budget_ms() -> u64 {
    crate::extract_budgeted::DEFAULT_EXTRACTION_BUDGET_MS
}

impl Default for PutOptions {
    fn default() -> Self {
        Self {
            timestamp: None,
            track: None,
            kind: None,
            uri: None,
            title: None,
            metadata: None,
            search_text: None,
            tags: Vec::new(),
            labels: Vec::new(),
            extra_metadata: BTreeMap::new(),
            enable_embedding: false,
            auto_tag: true,
            extract_dates: true,
            extract_triplets: true,
            parent_id: None,
            role: FrameRole::default(),
            no_raw: false,
            source_path: None,
            dedup: false,
            instant_index: true, // Instant searchability by default
            extraction_budget_ms: default_extraction_budget_ms(),
        }
    }
}

impl PutOptions {
    /// Start a fluent builder for `PutOptions`.
    #[must_use]
    pub fn builder() -> PutOptionsBuilder {
        PutOptionsBuilder::default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutOptionsBuilder {
    inner: PutOptions,
}

impl PutOptionsBuilder {
    #[must_use]
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.inner.timestamp = Some(timestamp);
        self
    }

    pub fn tag<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        let key = key.into();
        let value = value.into();
        self.inner.extra_metadata.insert(key.clone(), value);
        self.inner.tags.push(key);
        self
    }

    pub fn push_tag<S: Into<String>>(mut self, tag: S) -> Self {
        self.inner.tags.push(tag.into());
        self
    }

    pub fn label<S: Into<String>>(mut self, label: S) -> Self {
        self.inner.labels.push(label.into());
        self
    }

    pub fn metadata_entry<S: Into<String>>(mut self, key: S, value: Value) -> Self {
        self.inner
            .extra_metadata
            .insert(key.into(), value.to_string());
        self
    }

    #[must_use]
    pub fn metadata(mut self, metadata: DocMetadata) -> Self {
        self.inner.metadata = Some(metadata);
        self
    }

    pub fn track<S: Into<String>>(mut self, track: S) -> Self {
        self.inner.track = Some(track.into());
        self
    }

    pub fn kind<S: Into<String>>(mut self, kind: S) -> Self {
        self.inner.kind = Some(kind.into());
        self
    }

    pub fn uri<S: Into<String>>(mut self, uri: S) -> Self {
        self.inner.uri = Some(uri.into());
        self
    }

    pub fn title<S: Into<String>>(mut self, title: S) -> Self {
        self.inner.title = Some(title.into());
        self
    }

    pub fn search_text<S: Into<String>>(mut self, text: S) -> Self {
        self.inner.search_text = Some(text.into());
        self
    }

    #[must_use]
    pub fn enable_embedding(mut self, enable: bool) -> Self {
        self.inner.enable_embedding = enable;
        self
    }

    #[must_use]
    pub fn auto_tag(mut self, enabled: bool) -> Self {
        self.inner.auto_tag = enabled;
        self
    }

    #[must_use]
    pub fn extract_dates(mut self, enabled: bool) -> Self {
        self.inner.extract_dates = enabled;
        self
    }

    #[must_use]
    pub fn extract_triplets(mut self, enabled: bool) -> Self {
        self.inner.extract_triplets = enabled;
        self
    }

    #[must_use]
    pub fn parent_id(mut self, parent_id: FrameId) -> Self {
        self.inner.parent_id = Some(parent_id);
        self
    }

    #[must_use]
    pub fn role(mut self, role: FrameRole) -> Self {
        self.inner.role = role;
        self
    }

    /// Don't store raw binary content, only extracted text + SHA256 hash.
    #[must_use]
    pub fn no_raw(mut self, enabled: bool) -> Self {
        self.inner.no_raw = enabled;
        self
    }

    /// Set the original source file path (for --no-raw reference).
    pub fn source_path<S: Into<String>>(mut self, path: S) -> Self {
        self.inner.source_path = Some(path.into());
        self
    }

    /// Skip ingestion if a frame with matching BLAKE3 hash already exists.
    #[must_use]
    pub fn dedup(mut self, enabled: bool) -> Self {
        self.inner.dedup = enabled;
        self
    }

    /// Enable instant indexing for immediate searchability.
    /// When disabled, full commit is deferred (faster for batches).
    #[must_use]
    pub fn instant_index(mut self, enabled: bool) -> Self {
        self.inner.instant_index = enabled;
        self
    }

    /// Set extraction time budget in milliseconds.
    /// 0 means no budget (extract everything, slower but complete).
    #[must_use]
    pub fn extraction_budget_ms(mut self, ms: u64) -> Self {
        self.inner.extraction_budget_ms = ms;
        self
    }

    #[must_use]
    pub fn build(self) -> PutOptions {
        self.inner
    }
}

// ============================================================================
// Phase 1: Batch API Types
// ============================================================================

/// Request for batch put operation (`put_many`)
///
/// This struct represents a single document in a batch ingestion operation.
/// Use with `Memvid::put_many()` for 100-200x faster ingestion than individual `put()` calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutRequest {
    /// Document title (required)
    pub title: String,

    /// Primary label/category (required)
    pub label: String,

    /// Document text content (required)
    pub text: String,

    /// Document URI (optional, auto-generated if not provided)
    pub uri: Option<String>,

    /// Custom metadata (optional)
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,

    /// Document tags (optional)
    #[serde(default)]
    pub tags: Vec<String>,

    /// Additional labels (optional)
    #[serde(default)]
    pub labels: Vec<String>,

    /// Pre-computed embedding vector (optional, for batch operations with embeddings)
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
}

/// Options for `put_many` batch operation
///
/// Controls compression, checkpointing, and other performance/safety trade-offs
/// for bulk document ingestion.
///
/// # Performance vs Safety
///
/// **Fast Mode** (for bulk import): Use `compression_level=1`, `disable_auto_checkpoint=true`, `skip_sync=false`
///
/// **Ultra-Fast Mode** (NOT crash-safe!): Use `compression_level=1`, `disable_auto_checkpoint=true`, `skip_sync=true`
#[derive(Debug, Clone)]
pub struct PutManyOpts {
    /// Compression level for text content
    /// - 0: No compression (fastest ingestion, largest files)
    /// - 1: Fast compression (recommended for bulk import)
    /// - 3: Default compression (good balance)
    /// - 11: Maximum compression (slowest, smallest files)
    pub compression_level: i32,

    /// Disable auto-checkpoint during batch
    /// - true (default): Batch mode - caller must commit explicitly
    /// - false: May trigger checkpoints mid-batch (slower)
    pub disable_auto_checkpoint: bool,

    /// Skip fsync for maximum speed (NOT CRASH-SAFE!)
    /// - false (default): Safe mode - fsync after batch
    /// - true: Fast mode - trades crash-safety for speed
    pub skip_sync: bool,

    /// Enable embedding generation (slower, not yet implemented)
    pub enable_embedding: bool,

    /// Enable auto-tagging (slower, not yet implemented)
    pub auto_tag: bool,

    /// Extract dates from text (slower, not yet implemented)
    pub extract_dates: bool,

    /// Don't store raw binary content (default: true).
    /// Only extracted text + BLAKE3 hash is stored for space efficiency.
    /// Set to false if you need to retrieve the original files later.
    pub no_raw: bool,

    /// Run rules-based memory extraction after ingestion (default: true).
    /// Extracts facts, preferences, events, relationships from ingested content.
    pub enable_enrichment: bool,

    /// Pre-allocate the embedded WAL to this many bytes before the batch starts.
    ///
    /// When `disable_auto_checkpoint` is true, WAL entries accumulate for the
    /// entire batch. If the WAL is too small it must grow repeatedly, shifting
    /// all payload data each time — an O(file_size) operation per growth.
    ///
    /// Setting this to `num_entries * avg_entry_bytes` (e.g. 10 KB per entry
    /// with 1536-dim embeddings, 2 KB without) eliminates mid-batch WAL growth
    /// and can provide a **3-5× speedup** for large batches.
    ///
    /// 0 (default): no pre-sizing — WAL grows on demand.
    pub wal_pre_size_bytes: u64,
}

impl Default for PutManyOpts {
    fn default() -> Self {
        Self {
            compression_level: 3,          // Good balance
            disable_auto_checkpoint: true, // Batch mode by default
            skip_sync: false,              // Safe by default
            enable_embedding: false,       // Fast by default
            auto_tag: false,               // Fast by default
            extract_dates: false,          // Fast by default
            no_raw: true,                  // Text-only mode by default for space efficiency
            enable_enrichment: true,       // Enrichment enabled by default
            wal_pre_size_bytes: 0,         // No pre-sizing by default
        }
    }
}
