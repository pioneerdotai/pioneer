//! Lifecycle management for creating and opening `.mv2` memories.
//!
//! Responsibilities:
//! - Enforce single-file invariant (no sidecars) and take OS locks.
//! - Bootstrap headers, internal WAL, and TOC on create, and recover them on open.
//! - Validate TOC/footer layout, recover the latest valid footer when needed.
//! - Wire up index state (lex/vector/time) without mutating payload bytes.

use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::panic;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::constants::{MAGIC, SPEC_VERSION, WAL_OFFSET, WAL_SIZE_TINY};
use crate::error::{MemvidError, Result};
use crate::footer::{FooterSlice, find_last_valid_footer, find_last_valid_footer_with_charge};
use crate::io::header::HeaderCodec;
#[cfg(feature = "parallel_segments")]
use crate::io::manifest_wal::ManifestWal;
use crate::io::wal::EmbeddedWal;
use crate::lock::{FileLock, LockMode};
#[cfg(feature = "lex")]
use crate::search::{EmbeddedLexStorage, TantivyEngine};
#[cfg(feature = "temporal_track")]
use crate::types::FrameId;
#[cfg(feature = "parallel_segments")]
use crate::types::IndexSegmentRef;
use crate::types::{
    FrameStatus, Header, IndexManifests, LogicMesh, MemoriesTrack, PutManyOpts, SchemaRegistry,
    SegmentCatalog, SketchTrack, TicketRef, Tier, Toc, VectorCompression,
};
#[cfg(feature = "temporal_track")]
use crate::{TemporalTrack, temporal_track_read};
use crate::{lex::LexIndex, vec::VecIndex};
use blake3::Hasher;
use memmap2::Mmap;

const DEFAULT_LOCK_TIMEOUT_MS: u64 = 250;
const DEFAULT_HEARTBEAT_MS: u64 = 2_000;
const DEFAULT_STALE_GRACE_MS: u64 = 10_000;
const MAX_TOC_SCAN_BYTES: usize = 64 * 1024 * 1024;
const TOC_CHECKSUM_BYTES: usize = 32;
const MAX_TOC_DECODE_PASSES: usize = 3;
const HINT_NEIGHBORHOOD_BYTES: usize = 64;
// A valid candidate can require two full hashes (commit footer plus internal checksum) and three
// decode passes (current, legacy V2, legacy V1). Keep those format-required allowances separate
// from eight searchable-window hash passes so false candidates cannot consume decode capacity.
// At MAX_INDEX_BYTES this bounds recovery to 3 GiB of charged work:
//   hashes: 2 * 512 MiB + 8 * 64 MiB = 1.5 GiB
//   decode: 3 * 512 MiB = 1.5 GiB
const MAX_TOC_HASH_PASSES: usize = 2;
const TOC_SEARCH_WORK_MULTIPLIER: usize = 8;

#[derive(Debug)]
struct TocScanBudget {
    limit: usize,
    hash_limit: usize,
    decode_limit: usize,
    hashed_bytes: usize,
    decoded_bytes: usize,
    candidates_checked: usize,
    prefix_matches: usize,
}

impl TocScanBudget {
    fn for_file_len(file_len: usize) -> Self {
        let searchable_bytes = file_len.min(MAX_TOC_SCAN_BYTES);
        let supported_toc_bytes =
            file_len.min(usize::try_from(crate::MAX_INDEX_BYTES).unwrap_or(usize::MAX));
        let hash_limit = supported_toc_bytes
            .saturating_mul(MAX_TOC_HASH_PASSES)
            .saturating_add(searchable_bytes.saturating_mul(TOC_SEARCH_WORK_MULTIPLIER));
        let decode_limit = supported_toc_bytes.saturating_mul(MAX_TOC_DECODE_PASSES);
        Self::with_limits(hash_limit, decode_limit)
    }

    #[cfg(test)]
    fn new(limit: usize) -> Self {
        Self {
            limit,
            hash_limit: limit,
            decode_limit: limit,
            hashed_bytes: 0,
            decoded_bytes: 0,
            candidates_checked: 0,
            prefix_matches: 0,
        }
    }

    fn with_limits(hash_limit: usize, decode_limit: usize) -> Self {
        Self {
            limit: hash_limit.saturating_add(decode_limit),
            hash_limit,
            decode_limit,
            hashed_bytes: 0,
            decoded_bytes: 0,
            candidates_checked: 0,
            prefix_matches: 0,
        }
    }

    fn charge_hash(&mut self, bytes: usize) -> Result<()> {
        self.charge(bytes, self.hashed_bytes, self.hash_limit, "hashing")?;
        self.hashed_bytes = self.hashed_bytes.saturating_add(bytes);
        Ok(())
    }

    fn charge_decode(&mut self, bytes: usize) -> Result<()> {
        self.charge(bytes, self.decoded_bytes, self.decode_limit, "decoding")?;
        self.decoded_bytes = self.decoded_bytes.saturating_add(bytes);
        Ok(())
    }

    fn charge(
        &self,
        bytes: usize,
        operation_used: usize,
        operation_limit: usize,
        operation: &str,
    ) -> Result<()> {
        let used = self.hashed_bytes.saturating_add(self.decoded_bytes);
        if bytes > operation_limit.saturating_sub(operation_used)
            || bytes > self.limit.saturating_sub(used)
        {
            return Err(MemvidError::InvalidToc {
                reason: format!(
                    "TOC recovery work limit exceeded while {operation} candidates \
                     (total limit: {} bytes, total used: {} bytes, {operation} limit: {} bytes, \
                      {operation} used: {} bytes)",
                    self.limit, used, operation_limit, operation_used
                )
                .into(),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    fn used(&self) -> usize {
        self.hashed_bytes.saturating_add(self.decoded_bytes)
    }
}

/// Primary handle for interacting with a `.mv2` memory file.
///
/// Holds the file descriptor, lock, header, TOC, and in-memory index state. Mutations
/// append to the embedded WAL and are materialized at commit time to keep the layout deterministic.
pub struct Memvid {
    pub(crate) file: File,
    pub(crate) path: PathBuf,
    pub(crate) lock: FileLock,
    pub(crate) read_only: bool,
    /// Once set, this handle is terminal and can never publish another mutation. Cached reads may
    /// still work, but callers must reopen before relying on a coherent snapshot because this
    /// handle may no longer hold any OS lock.
    pub(crate) write_disabled: Option<String>,
    /// A second guard retained only when an I/O error prevents determining which inode is
    /// currently published. Holding both possible destination inodes is safer than releasing one.
    pub(crate) publication_fallback_lock: Option<FileLock>,
    pub(crate) header: Header,
    pub(crate) toc: Toc,
    pub(crate) wal: EmbeddedWal,
    /// Number of frame inserts appended to WAL but not yet materialized into `toc.frames`.
    ///
    /// This lets frontends predict stable frame IDs before an explicit commit.
    pub(crate) pending_frame_inserts: u64,
    pub(crate) data_end: u64,
    /// Cached end of the payload region (max of payload_offset + payload_length across all frames).
    /// Updated incrementally on frame insert to avoid O(n) scans.
    pub(crate) cached_payload_end: u64,
    pub(crate) generation: u64,
    pub(crate) lock_settings: LockSettings,
    pub(crate) lex_enabled: bool,
    pub(crate) lex_index: Option<LexIndex>,
    #[cfg(feature = "lex")]
    #[allow(dead_code)]
    pub(crate) lex_storage: Arc<RwLock<EmbeddedLexStorage>>,
    pub(crate) vec_enabled: bool,
    pub(crate) vec_compression: VectorCompression,
    pub(crate) vec_model: Option<String>,
    pub(crate) vec_index: Option<VecIndex>,
    /// CLIP visual embeddings index (separate from vec due to different dimensions)
    pub(crate) clip_enabled: bool,
    pub(crate) clip_index: Option<crate::clip::ClipIndex>,
    pub(crate) dirty: bool,
    #[cfg(feature = "lex")]
    pub(crate) tantivy: Option<TantivyEngine>,
    #[cfg(feature = "lex")]
    pub(crate) tantivy_dirty: bool,
    #[cfg(feature = "temporal_track")]
    pub(crate) temporal_track: Option<TemporalTrack>,
    #[cfg(feature = "parallel_segments")]
    pub(crate) manifest_wal: Option<ManifestWal>,
    /// In-memory track for structured memory cards.
    pub(crate) memories_track: MemoriesTrack,
    /// In-memory Logic-Mesh graph for entity-relationship traversal.
    pub(crate) logic_mesh: LogicMesh,
    /// In-memory sketch track for fast candidate generation.
    pub(crate) sketch_track: SketchTrack,
    /// Schema registry for predicate validation.
    pub(crate) schema_registry: SchemaRegistry,
    /// Whether to enforce strict schema validation on card insert.
    pub(crate) schema_strict: bool,
    /// Active batch mode options (set by `begin_batch`, cleared by `end_batch`).
    pub(crate) batch_opts: Option<PutManyOpts>,
    /// Active replay session being recorded (if any).
    #[cfg(feature = "replay")]
    pub(crate) active_session: Option<crate::replay::ActiveSession>,
    /// Completed sessions stored in memory (until persisted to file).
    #[cfg(feature = "replay")]
    pub(crate) completed_sessions: Vec<crate::replay::ReplaySession>,
}

/// Controls read-only open behaviour for `.mv2` memories.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenReadOptions {
    pub allow_repair: bool,
}

/// Controls creation of a new `.mv2` memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateOptions {
    pub tier: Tier,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self { tier: Tier::Free }
    }
}

impl CreateOptions {
    #[must_use]
    pub const fn new(tier: Tier) -> Self {
        Self { tier }
    }

    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            tier: Tier::Unlimited,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LockSettings {
    pub timeout_ms: u64,
    pub heartbeat_ms: u64,
    pub stale_grace_ms: u64,
    pub force_stale: bool,
    pub command: Option<String>,
}

impl Default for LockSettings {
    fn default() -> Self {
        Self {
            timeout_ms: DEFAULT_LOCK_TIMEOUT_MS,
            heartbeat_ms: DEFAULT_HEARTBEAT_MS,
            stale_grace_ms: DEFAULT_STALE_GRACE_MS,
            force_stale: false,
            command: None,
        }
    }
}

impl Memvid {
    /// Create a new, empty `.mv2` file with an embedded WAL and empty TOC.
    /// The file is locked exclusively for the lifetime of the handle.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::create_with_options(path, CreateOptions::default())
    }

    /// Create a new, empty `.mv2` file using the requested capacity tier.
    /// The file is locked exclusively for the lifetime of the handle.
    pub fn create_with_tier<P: AsRef<Path>>(path: P, tier: Tier) -> Result<Self> {
        Self::create_with_options(path, CreateOptions::new(tier))
    }

    /// Create a new, empty `.mv2` file using explicit creation options.
    /// The file is locked exclusively for the lifetime of the handle.
    pub fn create_with_options<P: AsRef<Path>>(path: P, options: CreateOptions) -> Result<Self> {
        let path_ref = path.as_ref();
        ensure_single_file(path_ref)?;

        let (mut file, lock) = FileLock::open_or_create_and_lock(path_ref)?;
        file.set_len(0)?;

        let header = Header {
            magic: MAGIC,
            version: SPEC_VERSION,
            footer_offset: WAL_OFFSET + WAL_SIZE_TINY,
            wal_offset: WAL_OFFSET,
            wal_size: WAL_SIZE_TINY,
            wal_checkpoint_pos: 0,
            wal_sequence: 0,
            toc_checksum: [0u8; 32],
        };

        let mut toc = empty_toc_with_options(options);
        // If lex feature is enabled, set the catalog flag immediately
        #[cfg(feature = "lex")]
        {
            toc.segment_catalog.lex_enabled = true;
        }
        file.set_len(header.footer_offset)?;
        HeaderCodec::write(&mut file, &header)?;

        let wal = EmbeddedWal::open(&file, &header)?;
        let data_end = header.footer_offset;
        #[cfg(feature = "lex")]
        let lex_storage = Arc::new(RwLock::new(EmbeddedLexStorage::new()));
        #[cfg(feature = "parallel_segments")]
        let manifest_wal = ManifestWal::open(manifest_wal_path(path_ref))?;
        #[cfg(feature = "parallel_segments")]
        let manifest_wal_entries = manifest_wal.replay()?;

        // No frames yet, so payload region ends at WAL boundary
        let cached_payload_end = header.wal_offset + header.wal_size;

        let mut memvid = Self {
            file,
            path: path_ref.to_path_buf(),
            lock,
            read_only: false,
            write_disabled: None,
            publication_fallback_lock: None,
            header,
            toc,
            wal,
            pending_frame_inserts: 0,
            data_end,
            cached_payload_end,
            generation: 0,
            lock_settings: LockSettings::default(),
            lex_enabled: cfg!(feature = "lex"), // Enable by default if feature is enabled
            lex_index: None,
            #[cfg(feature = "lex")]
            lex_storage,
            vec_enabled: cfg!(feature = "vec"), // Enable by default if feature is enabled
            vec_compression: VectorCompression::None,
            vec_model: None,
            vec_index: None,
            clip_enabled: cfg!(feature = "clip"), // Enable by default if feature is enabled
            clip_index: None,
            dirty: false,
            #[cfg(feature = "lex")]
            tantivy: None,
            #[cfg(feature = "lex")]
            tantivy_dirty: false,
            #[cfg(feature = "temporal_track")]
            temporal_track: None,
            #[cfg(feature = "parallel_segments")]
            manifest_wal: Some(manifest_wal),
            memories_track: MemoriesTrack::new(),
            logic_mesh: LogicMesh::new(),
            sketch_track: SketchTrack::default(),
            schema_registry: SchemaRegistry::new(),
            schema_strict: false,
            batch_opts: None,
            #[cfg(feature = "replay")]
            active_session: None,
            #[cfg(feature = "replay")]
            completed_sessions: Vec::new(),
        };

        #[cfg(feature = "lex")]
        memvid.init_tantivy()?;

        #[cfg(feature = "parallel_segments")]
        memvid.load_manifest_segments(manifest_wal_entries);

        memvid.bootstrap_segment_catalog();

        // Create empty manifests for enabled indexes so they persist across open/close
        let empty_offset = memvid.data_end;
        let empty_checksum = *b"\xe3\xb0\xc4\x42\x98\xfc\x1c\x14\x9a\xfb\xf4\xc8\x99\x6f\xb9\x24\
                                \x27\xae\x41\xe4\x64\x9b\x93\x4c\xa4\x95\x99\x1b\x78\x52\xb8\x55";

        #[cfg(feature = "lex")]
        if memvid.lex_enabled && memvid.toc.indexes.lex.is_none() {
            memvid.toc.indexes.lex = Some(crate::types::LexIndexManifest {
                doc_count: 0,
                generation: 0,
                bytes_offset: empty_offset,
                bytes_length: 0,
                checksum: empty_checksum,
            });
        }

        #[cfg(feature = "vec")]
        if memvid.vec_enabled && memvid.toc.indexes.vec.is_none() {
            memvid.toc.indexes.vec = Some(crate::types::VecIndexManifest {
                vector_count: 0,
                dimension: 0,
                bytes_offset: empty_offset,
                bytes_length: 0,
                checksum: empty_checksum,
                compression_mode: memvid.vec_compression.clone(),
                model: memvid.vec_model.clone(),
            });
        }

        memvid.rewrite_toc_footer()?;
        memvid.header.toc_checksum = memvid.toc.toc_checksum;
        crate::persist_header(&mut memvid.file, &memvid.header)?;
        memvid.file.sync_all()?;
        Ok(memvid)
    }

    #[must_use]
    pub fn lock_settings(&self) -> &LockSettings {
        &self.lock_settings
    }

    pub fn lock_settings_mut(&mut self) -> &mut LockSettings {
        &mut self.lock_settings
    }

    /// Set the vector compression mode for this memory
    /// Must be called before ingesting documents with embeddings
    pub fn set_vector_compression(&mut self, compression: VectorCompression) {
        self.vec_compression = compression;
    }

    /// Get the current vector compression mode
    #[must_use]
    pub fn vector_compression(&self) -> &VectorCompression {
        &self.vec_compression
    }

    /// Predict the next frame ID that would be assigned to a new insert.
    ///
    /// Frame IDs are dense indices into `toc.frames`. When a memory is mutable, inserts are first
    /// appended to the embedded WAL and only materialized into `toc.frames` on commit. This helper
    /// lets frontends allocate stable frame IDs before an explicit commit.
    #[must_use]
    pub fn next_frame_id(&self) -> u64 {
        (self.toc.frames.len() as u64).saturating_add(self.pending_frame_inserts)
    }

    /// Returns the total number of frames in the memory.
    ///
    /// This includes all frames regardless of status (active, deleted, etc.).
    #[must_use]
    pub fn frame_count(&self) -> usize {
        self.toc.frames.len()
    }

    fn open_locked(mut file: File, lock: FileLock, path_ref: &Path) -> Result<Self> {
        // Fast-path detection for encrypted capsules (.mv2e).
        // This avoids confusing "invalid header" errors and provides an actionable hint.
        let mut magic = [0u8; 4];
        let is_mv2e = file.read_exact(&mut magic).is_ok() && magic == *b"MV2E";
        file.seek(SeekFrom::Start(0))?;
        if is_mv2e {
            return Err(MemvidError::EncryptedFile {
                path: path_ref.to_path_buf(),
                hint: format!("Run: memvid unlock {}", path_ref.display()),
            });
        }

        let mut header = HeaderCodec::read(&mut file)?;
        let (toc, recovery_verified_checksum) = match read_toc(&mut file, &header) {
            Ok(toc) => (toc, false),
            Err(err @ (MemvidError::Decode(_) | MemvidError::InvalidToc { .. })) => {
                tracing::info!("toc decode failed ({}); attempting recovery", err);
                let (toc, recovered_offset) = recover_toc(&mut file, Some(header.footer_offset))?;
                if recovered_offset != header.footer_offset
                    || header.toc_checksum != toc.toc_checksum
                {
                    header.footer_offset = recovered_offset;
                    header.toc_checksum = toc.toc_checksum;
                    crate::persist_header(&mut file, &header)?;
                }
                (toc, true)
            }
            Err(err) => return Err(err),
        };
        // recover_toc verifies the checksum against the original serialized bytes under its
        // shared work budget. Avoid an unbudgeted decode/serialize/hash verification pass here.
        let checksum_result = if recovery_verified_checksum {
            Ok(())
        } else {
            toc.verify_checksum()
        };

        // Validate segment integrity early to catch corruption before loading indexes
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        if let Err(e) = validate_segment_integrity(&toc, &header, file_len) {
            tracing::warn!("Segment integrity validation failed: {}", e);
            // Don't fail file open - let doctor handle it
            // This is just an early warning system
        }
        ensure_non_overlapping_frames(&toc, file_len)?;

        let wal = EmbeddedWal::open(&file, &header)?;
        #[cfg(feature = "lex")]
        let lex_storage = Arc::new(RwLock::new(EmbeddedLexStorage::from_manifest(
            toc.indexes.lex.as_ref(),
            &toc.indexes.lex_segments,
        )));
        #[cfg(feature = "parallel_segments")]
        let manifest_wal = ManifestWal::open(manifest_wal_path(path_ref))?;
        #[cfg(feature = "parallel_segments")]
        let manifest_wal_entries = manifest_wal.replay()?;

        let generation = detect_generation(&file)?.unwrap_or(0);
        let read_only = lock.mode() == LockMode::Shared;

        let mut memvid = Self {
            file,
            path: path_ref.to_path_buf(),
            lock,
            read_only,
            write_disabled: None,
            publication_fallback_lock: None,
            header,
            toc,
            wal,
            pending_frame_inserts: 0,
            data_end: 0,
            cached_payload_end: 0,
            generation,
            lock_settings: LockSettings::default(),
            lex_enabled: false,
            lex_index: None,
            #[cfg(feature = "lex")]
            lex_storage,
            vec_enabled: false,
            vec_compression: VectorCompression::None,
            vec_model: None,
            vec_index: None,
            clip_enabled: false,
            clip_index: None,
            dirty: false,
            #[cfg(feature = "lex")]
            tantivy: None,
            #[cfg(feature = "lex")]
            tantivy_dirty: false,
            #[cfg(feature = "temporal_track")]
            temporal_track: None,
            #[cfg(feature = "parallel_segments")]
            manifest_wal: Some(manifest_wal),
            memories_track: MemoriesTrack::new(),
            logic_mesh: LogicMesh::new(),
            sketch_track: SketchTrack::default(),
            schema_registry: SchemaRegistry::new(),
            schema_strict: false,
            batch_opts: None,
            #[cfg(feature = "replay")]
            active_session: None,
            #[cfg(feature = "replay")]
            completed_sessions: Vec::new(),
        };
        memvid.data_end = compute_data_end(&memvid.toc, &memvid.header);
        // One-time O(n) scan to initialize cached_payload_end from existing frames
        memvid.cached_payload_end = compute_payload_region_end(&memvid.toc, &memvid.header);
        // Use consolidated helper for lex_enabled check
        memvid.lex_enabled = has_lex_index(&memvid.toc);
        if memvid.lex_enabled {
            memvid.load_lex_index_from_manifest()?;
        }
        #[cfg(feature = "lex")]
        {
            memvid.init_tantivy()?;
        }
        memvid.vec_enabled =
            memvid.toc.indexes.vec.is_some() || !memvid.toc.segment_catalog.vec_segments.is_empty();
        if memvid.vec_enabled {
            memvid.load_vec_index_from_manifest()?;
        }
        memvid.clip_enabled = memvid.toc.indexes.clip.is_some();
        if memvid.clip_enabled {
            memvid.load_clip_index_from_manifest()?;
        }
        // Recovery may compact payloads over the previous derived tail. Load
        // persistent tracks while their committed ranges are still intact so
        // recover_wal can republish them with the rebuilt indexes.
        memvid.load_memories_track()?;
        memvid.load_logic_mesh()?;
        memvid.load_sketch_track()?;
        memvid.recover_wal()?;
        #[cfg(feature = "parallel_segments")]
        memvid.load_manifest_segments(manifest_wal_entries);
        memvid.bootstrap_segment_catalog();
        #[cfg(feature = "temporal_track")]
        memvid.ensure_temporal_track_loaded()?;
        if checksum_result.is_err() {
            memvid.toc.verify_checksum()?;
            if memvid.toc.toc_checksum != memvid.header.toc_checksum {
                memvid.header.toc_checksum = memvid.toc.toc_checksum;
                crate::persist_header(&mut memvid.file, &memvid.header)?;
                memvid.file.sync_all()?;
            }
        }
        Ok(memvid)
    }

    /// Open an existing `.mv2` with exclusive access, performing recovery if needed.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        ensure_single_file(path_ref)?;

        let (file, lock) = FileLock::open_and_lock(path_ref)?;
        Self::open_locked(file, lock, path_ref)
    }

    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_read_only_with_options(path, OpenReadOptions::default())
    }

    pub fn open_read_only_with_options<P: AsRef<Path>>(
        path: P,
        options: OpenReadOptions,
    ) -> Result<Self> {
        let path_ref = path.as_ref();
        ensure_single_file(path_ref)?;

        if options.allow_repair {
            return Self::open(path_ref);
        }

        Self::open_read_only_snapshot(path_ref)
    }

    fn open_read_only_snapshot(path_ref: &Path) -> Result<Self> {
        let (mut file, lock) = FileLock::open_read_only(path_ref)?;
        let TailSnapshot {
            toc,
            footer_offset,
            data_end,
            generation,
        } = load_tail_snapshot(&file)?;

        let mut header = HeaderCodec::read(&mut file)?;
        header.footer_offset = footer_offset;
        header.toc_checksum = toc.toc_checksum;

        let wal = EmbeddedWal::open_read_only(&file, &header)?;

        #[cfg(feature = "lex")]
        let lex_storage = Arc::new(RwLock::new(EmbeddedLexStorage::from_manifest(
            toc.indexes.lex.as_ref(),
            &toc.indexes.lex_segments,
        )));

        let cached_payload_end = compute_payload_region_end(&toc, &header);

        let mut memvid = Self {
            file,
            path: path_ref.to_path_buf(),
            lock,
            read_only: true,
            write_disabled: None,
            publication_fallback_lock: None,
            header,
            toc,
            wal,
            pending_frame_inserts: 0,
            data_end,
            cached_payload_end,
            generation,
            lock_settings: LockSettings::default(),
            lex_enabled: false,
            lex_index: None,
            #[cfg(feature = "lex")]
            lex_storage,
            vec_enabled: false,
            vec_compression: VectorCompression::None,
            vec_model: None,
            vec_index: None,
            clip_enabled: false,
            clip_index: None,
            dirty: false,
            #[cfg(feature = "lex")]
            tantivy: None,
            #[cfg(feature = "lex")]
            tantivy_dirty: false,
            #[cfg(feature = "temporal_track")]
            temporal_track: None,
            #[cfg(feature = "parallel_segments")]
            manifest_wal: None,
            memories_track: MemoriesTrack::new(),
            logic_mesh: LogicMesh::new(),
            sketch_track: SketchTrack::default(),
            schema_registry: SchemaRegistry::new(),
            schema_strict: false,
            batch_opts: None,
            #[cfg(feature = "replay")]
            active_session: None,
            #[cfg(feature = "replay")]
            completed_sessions: Vec::new(),
        };

        // Use consolidated helper for lex_enabled check
        memvid.lex_enabled = has_lex_index(&memvid.toc);
        if memvid.lex_enabled {
            memvid.load_lex_index_from_manifest()?;
        }
        #[cfg(feature = "lex")]
        memvid.init_tantivy()?;

        memvid.vec_enabled =
            memvid.toc.indexes.vec.is_some() || !memvid.toc.segment_catalog.vec_segments.is_empty();
        if memvid.vec_enabled {
            memvid.load_vec_index_from_manifest()?;
        }
        memvid.clip_enabled = memvid.toc.indexes.clip.is_some();
        if memvid.clip_enabled {
            memvid.load_clip_index_from_manifest()?;
        }
        // Load memories track, Logic-Mesh, and sketch track if present
        memvid.load_memories_track()?;
        memvid.load_logic_mesh()?;
        memvid.load_sketch_track()?;

        memvid.bootstrap_segment_catalog();
        #[cfg(feature = "temporal_track")]
        memvid.ensure_temporal_track_loaded()?;

        Ok(memvid)
    }

    pub(crate) fn try_open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_ref = path.as_ref();
        ensure_single_file(path_ref)?;

        let candidate = OpenOptions::new().read(true).write(true).open(path_ref)?;
        let lock = match FileLock::try_acquire(&candidate, path_ref)? {
            Some(lock) => lock,
            None => {
                return Err(MemvidError::Lock(
                    "exclusive access unavailable for doctor".to_string(),
                ));
            }
        };
        let file = lock.clone_handle()?;
        Self::open_locked(file, lock, path_ref)
    }

    fn bootstrap_segment_catalog(&mut self) {
        let catalog = &mut self.toc.segment_catalog;
        if catalog.version == 0 {
            catalog.version = 1;
        }
        if catalog.next_segment_id == 0 {
            let mut max_id = 0u64;
            for descriptor in &catalog.lex_segments {
                max_id = max_id.max(descriptor.common.segment_id);
            }
            for descriptor in &catalog.vec_segments {
                max_id = max_id.max(descriptor.common.segment_id);
            }
            for descriptor in &catalog.time_segments {
                max_id = max_id.max(descriptor.common.segment_id);
            }
            #[cfg(feature = "temporal_track")]
            for descriptor in &catalog.temporal_segments {
                max_id = max_id.max(descriptor.common.segment_id);
            }
            #[cfg(feature = "parallel_segments")]
            for descriptor in &catalog.index_segments {
                max_id = max_id.max(descriptor.common.segment_id);
            }
            if max_id > 0 {
                catalog.next_segment_id = max_id.saturating_add(1);
            }
        }
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn load_manifest_segments(&mut self, entries: Vec<IndexSegmentRef>) {
        if entries.is_empty() {
            return;
        }
        for entry in entries {
            let duplicate = self
                .toc
                .segment_catalog
                .index_segments
                .iter()
                .any(|existing| existing.common.segment_id == entry.common.segment_id);
            if !duplicate {
                self.toc.segment_catalog.index_segments.push(entry);
            }
        }
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn release_manifest_wal_before_shared_lock(&mut self) -> Result<()> {
        let Some(manifest_wal) = self.manifest_wal.as_mut() else {
            return Ok(());
        };
        manifest_wal.flush()?;
        if !manifest_wal.is_empty() {
            return Err(MemvidError::Lock(
                "cannot downgrade while the manifest WAL contains recovery entries".to_string(),
            ));
        }

        // Close our fd before unlinking the name, while the capsule is still exclusively locked.
        // A later upgrade must open a fresh named journal rather than reuse an unlinked fd.
        drop(self.manifest_wal.take());
        let wal_path = manifest_wal_path(&self.path);
        match std::fs::remove_file(wal_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn reopen_manifest_wal_after_exclusive_lock(&mut self) -> Result<()> {
        if self.manifest_wal.is_some() {
            return Ok(());
        }
        let manifest_wal = ManifestWal::open(manifest_wal_path(&self.path))?;
        let entries = manifest_wal.replay()?;
        self.load_manifest_segments(entries);
        self.manifest_wal = Some(manifest_wal);
        Ok(())
    }

    /// Load the memories track from the manifest if present.
    fn load_memories_track(&mut self) -> Result<()> {
        let manifest = match &self.toc.memories_track {
            Some(m) => m,
            None => return Ok(()),
        };

        // Read the compressed data from the file
        if manifest.bytes_length > crate::MAX_INDEX_BYTES {
            return Err(MemvidError::InvalidToc {
                reason: "memories track exceeds safety limit".into(),
            });
        }
        // Safe: guarded by MAX_INDEX_BYTES check above
        #[allow(clippy::cast_possible_truncation)]
        let mut buf = vec![0u8; manifest.bytes_length as usize];
        self.file
            .seek(std::io::SeekFrom::Start(manifest.bytes_offset))?;
        self.file.read_exact(&mut buf)?;

        // Verify checksum
        let actual_checksum: [u8; 32] = blake3::hash(&buf).into();
        if actual_checksum != manifest.checksum {
            return Err(MemvidError::InvalidToc {
                reason: "memories track checksum mismatch".into(),
            });
        }

        // Deserialize the memories track
        self.memories_track = MemoriesTrack::deserialize(&buf)?;

        Ok(())
    }

    /// Load the Logic-Mesh from the manifest if present.
    fn load_logic_mesh(&mut self) -> Result<()> {
        let manifest = match &self.toc.logic_mesh {
            Some(m) => m,
            None => return Ok(()),
        };

        // Read the serialized data from the file
        if manifest.bytes_length > crate::MAX_INDEX_BYTES {
            return Err(MemvidError::InvalidToc {
                reason: "logic mesh exceeds safety limit".into(),
            });
        }
        // Safe: guarded by MAX_INDEX_BYTES check above
        #[allow(clippy::cast_possible_truncation)]
        let mut buf = vec![0u8; manifest.bytes_length as usize];
        self.file
            .seek(std::io::SeekFrom::Start(manifest.bytes_offset))?;
        self.file.read_exact(&mut buf)?;

        // Verify checksum
        let actual_checksum: [u8; 32] = blake3::hash(&buf).into();
        if actual_checksum != manifest.checksum {
            return Err(MemvidError::InvalidToc {
                reason: "logic mesh checksum mismatch".into(),
            });
        }

        // Deserialize the logic mesh
        self.logic_mesh = LogicMesh::deserialize(&buf)?;

        Ok(())
    }

    /// Load the sketch track from the manifest if present.
    fn load_sketch_track(&mut self) -> Result<()> {
        let manifest = match &self.toc.sketch_track {
            Some(m) => m.clone(),
            None => return Ok(()),
        };

        // Read and deserialize the sketch track (read_sketch_track handles seeking and checksum)
        self.sketch_track = crate::types::read_sketch_track(
            &mut self.file,
            manifest.bytes_offset,
            manifest.bytes_length,
        )?;

        Ok(())
    }

    #[cfg(feature = "temporal_track")]
    pub(crate) fn ensure_temporal_track_loaded(&mut self) -> Result<()> {
        if self.temporal_track.is_some() {
            return Ok(());
        }
        let manifest = match &self.toc.temporal_track {
            Some(manifest) => manifest.clone(),
            None => return Ok(()),
        };
        if manifest.bytes_length == 0 {
            return Ok(());
        }
        let file_len = self.file.metadata()?.len();
        let Some(end) = manifest.bytes_offset.checked_add(manifest.bytes_length) else {
            return Ok(());
        };
        if end > file_len {
            return Ok(());
        }
        match temporal_track_read(&mut self.file, manifest.bytes_offset, manifest.bytes_length) {
            Ok(track) => self.temporal_track = Some(track),
            Err(MemvidError::InvalidTemporalTrack { .. }) => {
                return Ok(());
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    #[cfg(feature = "temporal_track")]
    pub(crate) fn temporal_track_ref(&mut self) -> Result<Option<&TemporalTrack>> {
        self.ensure_temporal_track_loaded()?;
        Ok(self.temporal_track.as_ref())
    }

    #[cfg(feature = "temporal_track")]
    pub(crate) fn temporal_anchor_timestamp(&mut self, frame_id: FrameId) -> Result<Option<i64>> {
        self.ensure_temporal_track_loaded()?;
        let Some(track) = self.temporal_track.as_ref() else {
            return Ok(None);
        };
        if !track.capabilities().has_anchors {
            return Ok(None);
        }
        Ok(track
            .anchor_for_frame(frame_id)
            .map(|anchor| anchor.anchor_ts))
    }

    #[cfg(feature = "temporal_track")]
    pub(crate) fn clear_temporal_track_cache(&mut self) {
        self.temporal_track = None;
    }

    #[cfg(feature = "temporal_track")]
    pub(crate) fn effective_temporal_timestamp(
        &mut self,
        frame_id: FrameId,
        fallback: i64,
    ) -> Result<i64> {
        Ok(self
            .temporal_anchor_timestamp(frame_id)?
            .unwrap_or(fallback))
    }

    #[cfg(not(feature = "temporal_track"))]
    pub(crate) fn effective_temporal_timestamp(
        &mut self,
        _frame_id: crate::types::FrameId,
        fallback: i64,
    ) -> Result<i64> {
        Ok(fallback)
    }

    /// Get current memory binding information.
    ///
    /// Returns the binding if this file is bound to a dashboard memory,
    /// or None if unbound.
    #[must_use]
    pub fn get_memory_binding(&self) -> Option<&crate::types::MemoryBinding> {
        self.toc.memory_binding.as_ref()
    }

    /// Bind this file to a dashboard memory.
    ///
    /// This stores the binding in the TOC and applies a temporary ticket for initial binding.
    /// The caller should follow up with `apply_signed_ticket` for cryptographic verification.
    ///
    /// # Errors
    ///
    /// Returns `MemoryAlreadyBound` if this file is already bound to a different memory.
    #[allow(deprecated)]
    pub fn bind_memory(
        &mut self,
        binding: crate::types::MemoryBinding,
        ticket: crate::types::Ticket,
    ) -> Result<()> {
        // Check existing binding
        if let Some(existing) = self.get_memory_binding() {
            if existing.memory_id != binding.memory_id {
                return Err(MemvidError::MemoryAlreadyBound {
                    existing_memory_id: existing.memory_id,
                    existing_memory_name: existing.memory_name.clone(),
                    bound_at: existing.bound_at.to_rfc3339(),
                });
            }
        }

        // Apply ticket for capacity
        self.apply_ticket(ticket)?;

        // Store binding in TOC
        self.toc.memory_binding = Some(binding);
        self.dirty = true;

        Ok(())
    }

    /// Set only the memory binding without applying a ticket.
    ///
    /// This is used when the caller will immediately follow up with `apply_signed_ticket`
    /// to apply the cryptographically verified ticket. This avoids the sequence number
    /// conflict that occurs when using `bind_memory` with a temporary ticket.
    ///
    /// # Errors
    ///
    /// Returns `MemoryAlreadyBound` if this file is already bound to a different memory.
    pub fn set_memory_binding_only(&mut self, binding: crate::types::MemoryBinding) -> Result<()> {
        self.ensure_writable()?;

        // Check existing binding
        if let Some(existing) = self.get_memory_binding() {
            if existing.memory_id != binding.memory_id {
                return Err(MemvidError::MemoryAlreadyBound {
                    existing_memory_id: existing.memory_id,
                    existing_memory_name: existing.memory_name.clone(),
                    bound_at: existing.bound_at.to_rfc3339(),
                });
            }
        }

        // Store binding in TOC (without applying a ticket)
        self.toc.memory_binding = Some(binding);
        self.dirty = true;

        Ok(())
    }

    /// Unbind this file from its dashboard memory.
    ///
    /// This clears the binding and reverts to the default free tier capacity.
    pub fn unbind_memory(&mut self) -> Result<()> {
        self.toc.memory_binding = None;
        self.toc.ticket_ref = ticket_ref_for_tier(crate::types::Tier::Free);
        self.dirty = true;
        Ok(())
    }
}

pub(crate) fn read_toc(file: &mut File, header: &Header) -> Result<Toc> {
    use crate::footer::{CommitFooter, FOOTER_SIZE};

    let len = file.metadata()?.len();
    if len < header.footer_offset {
        return Err(MemvidError::InvalidToc {
            reason: "footer offset beyond file length".into(),
        });
    }

    // Read the entire region from footer_offset to EOF (includes TOC + footer)
    file.seek(SeekFrom::Start(header.footer_offset))?;
    // Safe: total_size bounded by file length, and we check MAX_INDEX_BYTES before reading
    #[allow(clippy::cast_possible_truncation)]
    let total_size = (len - header.footer_offset) as usize;
    if total_size as u64 > crate::MAX_INDEX_BYTES {
        return Err(MemvidError::InvalidToc {
            reason: "toc region exceeds safety limit".into(),
        });
    }

    if total_size < FOOTER_SIZE {
        return Err(MemvidError::InvalidToc {
            reason: "region too small to contain footer".into(),
        });
    }

    let mut buf = Vec::with_capacity(total_size);
    file.read_to_end(&mut buf)?;

    // Parse the footer (last FOOTER_SIZE bytes)
    let footer_start = buf.len() - FOOTER_SIZE;
    let footer_bytes = &buf[footer_start..];
    let footer = CommitFooter::decode(footer_bytes).ok_or(MemvidError::InvalidToc {
        reason: "failed to decode commit footer".into(),
    })?;

    // Extract only the TOC bytes (excluding the footer)
    let toc_bytes = &buf[..footer_start];
    #[allow(clippy::cast_possible_truncation)]
    if toc_bytes.len() != footer.toc_len as usize {
        return Err(MemvidError::InvalidToc {
            reason: "toc length mismatch".into(),
        });
    }
    if !footer.hash_matches(toc_bytes) {
        return Err(MemvidError::InvalidToc {
            reason: "commit footer toc hash mismatch".into(),
        });
    }

    verify_toc_prefix(toc_bytes)?;
    let toc = Toc::decode(toc_bytes)?;
    Ok(toc)
}

fn verify_toc_prefix(bytes: &[u8]) -> Result<u64> {
    const MAX_SEGMENTS: u64 = 1_000_000;
    const MAX_FRAMES: u64 = 10_000_000;
    // canonical_config uses fixed-width integers. SegmentMeta consists of three u64 values,
    // two FrameId (u64) values, a 32-byte checksum, and a u32 enum discriminant.
    const SEGMENT_META_BYTES: u64 = 76;
    const MIN_FRAME_BYTES: u64 = 64;
    // TOC prefix layout (fixed-int little-endian bincode):
    // [toc_version:u64][segments_len:u64][SegmentMeta; segments_len][frames_len:u64]...
    let read_u64 = |offset: usize, context: &str| -> Result<u64> {
        let end = offset
            .checked_add(8)
            .ok_or_else(|| MemvidError::InvalidToc {
                reason: context.to_string().into(),
            })?;
        let slice = bytes
            .get(offset..end)
            .ok_or_else(|| MemvidError::InvalidToc {
                reason: context.to_string().into(),
            })?;
        let array: [u8; 8] = slice.try_into().map_err(|_| MemvidError::InvalidToc {
            reason: context.to_string().into(),
        })?;
        Ok(u64::from_le_bytes(array))
    };

    if bytes.len() < 16 {
        return Err(MemvidError::InvalidToc {
            reason: "toc trailer too small".into(),
        });
    }
    let toc_version = read_u64(0, "toc version missing or truncated")?;
    if toc_version > 32 {
        return Err(MemvidError::InvalidToc {
            reason: "toc version unreasonable".into(),
        });
    }
    let segments_len = read_u64(8, "segment count missing or truncated")?;
    if segments_len > MAX_SEGMENTS {
        return Err(MemvidError::InvalidToc {
            reason: "segment count unreasonable".into(),
        });
    }
    let segment_bytes = segments_len
        .checked_mul(SEGMENT_META_BYTES)
        .ok_or_else(|| MemvidError::InvalidToc {
            reason: "segment byte length overflow".into(),
        })?;
    let frames_len_offset =
        16u64
            .checked_add(segment_bytes)
            .ok_or_else(|| MemvidError::InvalidToc {
                reason: "frame count offset overflow".into(),
            })?;
    let frames_len_offset =
        usize::try_from(frames_len_offset).map_err(|_| MemvidError::InvalidToc {
            reason: "frame count offset exceeds addressable memory".into(),
        })?;
    let frames_len = read_u64(
        frames_len_offset,
        "segment metadata or frame count truncated",
    )?;
    if frames_len > MAX_FRAMES {
        return Err(MemvidError::InvalidToc {
            reason: "frame count unreasonable".into(),
        });
    }
    let required = (frames_len_offset as u64)
        .saturating_add(8)
        .saturating_add(frames_len.saturating_mul(MIN_FRAME_BYTES))
        .saturating_add(TOC_CHECKSUM_BYTES as u64);
    if required > bytes.len() as u64 {
        return Err(MemvidError::InvalidToc {
            reason: "toc payload inconsistent with counts".into(),
        });
    }
    Ok(frames_len)
}

/// Ensure frame payloads do not overlap each other or exceed file boundary.
///
/// Frames in the TOC are ordered by `frame_id`, not by `payload_offset`, so we must
/// sort by `payload_offset` before checking for overlaps.
///
/// Note: Frames with `payload_length` == 0 are "virtual" frames (e.g., document
/// frames that reference chunks) and are skipped from this check.
fn ensure_non_overlapping_frames(toc: &Toc, file_len: u64) -> Result<()> {
    // Collect active frames with actual payloads and sort by payload_offset
    let mut frames_by_offset: Vec<_> = toc
        .frames
        .iter()
        .filter(|f| f.status == FrameStatus::Active && f.payload_length > 0)
        .collect();
    frames_by_offset.sort_by_key(|f| f.payload_offset);

    let mut previous_end = 0u64;
    for frame in frames_by_offset {
        let end = frame
            .payload_offset
            .checked_add(frame.payload_length)
            .ok_or_else(|| MemvidError::InvalidToc {
                reason: "frame payload offsets overflow".into(),
            })?;
        if end > file_len {
            return Err(MemvidError::InvalidToc {
                reason: "frame payload exceeds file length".into(),
            });
        }
        if frame.payload_offset < previous_end {
            return Err(MemvidError::InvalidToc {
                reason: format!(
                    "frame {} payload overlaps with previous frame (offset {} < previous end {})",
                    frame.id, frame.payload_offset, previous_end
                )
                .into(),
            });
        }
        previous_end = end;
    }
    Ok(())
}

pub(crate) fn recover_toc(file: &mut File, hint: Option<u64>) -> Result<(Toc, u64)> {
    let len = file.metadata()?.len();
    let file_len = usize::try_from(len).unwrap_or(usize::MAX);
    let mut budget = TocScanBudget::for_file_len(file_len);
    recover_toc_with_budget(file, hint, &mut budget)
}

fn recover_toc_with_budget(
    file: &mut File,
    hint: Option<u64>,
    budget: &mut TocScanBudget,
) -> Result<(Toc, u64)> {
    let len = file.metadata()?.len();
    // Safety: we only create a read-only mapping over stable file bytes.
    let mmap = unsafe { Mmap::map(&*file)? };
    tracing::debug!(file_len = len, "attempting toc recovery");

    // First, try to find a valid footer. Footer hashes, internal TOC checksum verification, and
    // decoding all consume the same budget later used by hint and fallback scanning.
    if let Some(footer_slice) =
        find_last_valid_footer_with_charge(&mmap, |bytes| budget.charge_hash(bytes))?
    {
        tracing::debug!(
            footer_offset = footer_slice.footer_offset,
            toc_offset = footer_slice.toc_offset,
            toc_len = footer_slice.toc_bytes.len(),
            "found valid footer during recovery"
        );
        if let Some(toc) = decode_checksummed_toc(footer_slice.toc_bytes, budget, true)? {
            return Ok((toc, footer_slice.toc_offset as u64));
        }
        tracing::warn!("footer-validated TOC failed internal validation, falling back to scan");
    }

    // A committed TOC can end either immediately at EOF (footer wholly absent) or immediately
    // before the fixed-size footer (footer present but corrupt). Try both layouts at and close to
    // the header hint before blind scanning. Nearby offsets preserve recovery from small hint
    // damage without paying for the many naturally plausible prefixes inside a serialized TOC.
    if let Some(hint_offset) = hint {
        // Safe: file successfully mmapped so length fits in usize
        #[allow(clippy::cast_possible_truncation)]
        let start = (hint_offset.min(len)) as usize;
        if let Some((toc, recovered_offset)) = recover_toc_near_hint(&mmap, start, budget)? {
            tracing::debug!(
                recovered_offset,
                recovered_frames = toc.frames.len(),
                "recovered checksummed toc near header hint"
            );
            return Ok((toc, recovered_offset as u64));
        }
    }

    // Fallback to manual scan if footer-based recovery failed. The budget is shared by both
    // ranges, and is linear in the searchable window. This prevents plausible prefixes from
    // causing the same large suffix to be hashed once per byte offset.
    let mut ranges = Vec::new();
    if let Some(hint_offset) = hint {
        // Safe: file successfully mmapped so length fits in usize
        #[allow(clippy::cast_possible_truncation)]
        let hint_idx = hint_offset.min(len) as usize;
        ranges.push((hint_idx, mmap.len()));
        if hint_idx > 0 {
            ranges.push((0, hint_idx));
        }
    } else {
        ranges.push((0, mmap.len()));
    }

    for (start, end) in ranges {
        if let Some(found) = scan_range_for_toc(&mmap, start, end, budget)? {
            return Ok(found);
        }
    }

    Err(MemvidError::InvalidToc {
        reason: "unable to recover table of contents from file trailer".into(),
    })
}

fn recover_toc_near_hint(
    data: &[u8],
    hint: usize,
    budget: &mut TocScanBudget,
) -> Result<Option<(Toc, usize)>> {
    if let Some(toc) = try_toc_layouts_at(data, hint, budget)? {
        return Ok(Some((toc, hint)));
    }

    for distance in 1..=HINT_NEIGHBORHOOD_BYTES {
        if let Some(offset) = hint.checked_sub(distance)
            && let Some(toc) = try_toc_layouts_at(data, offset, budget)?
        {
            return Ok(Some((toc, offset)));
        }
        if let Some(offset) = hint
            .checked_add(distance)
            .filter(|offset| *offset < data.len())
            && let Some(toc) = try_toc_layouts_at(data, offset, budget)?
        {
            return Ok(Some((toc, offset)));
        }
    }
    Ok(None)
}

fn try_toc_layouts_at(
    data: &[u8],
    offset: usize,
    budget: &mut TocScanBudget,
) -> Result<Option<Toc>> {
    if offset >= data.len() {
        return Ok(None);
    }

    // Whole TOC at EOF: the commit footer was never written or was completely truncated.
    if let Some(toc) = decode_checksummed_toc(&data[offset..], budget, false)? {
        return Ok(Some(toc));
    }

    // Whole TOC followed by a corrupt fixed-size commit footer.
    let toc_end = data.len().saturating_sub(crate::footer::FOOTER_SIZE);
    if toc_end > offset
        && let Some(toc) = decode_checksummed_toc(&data[offset..toc_end], budget, false)?
    {
        return Ok(Some(toc));
    }
    Ok(None)
}

fn decode_checksummed_toc(
    bytes: &[u8],
    budget: &mut TocScanBudget,
    allow_empty: bool,
) -> Result<Option<Toc>> {
    budget.candidates_checked = budget.candidates_checked.saturating_add(1);
    if bytes.len() < TOC_CHECKSUM_BYTES {
        return Ok(None);
    }
    let frames_len = match verify_toc_prefix(bytes) {
        Ok(frames_len) => frames_len,
        Err(_) => return Ok(None),
    };
    // An empty TOC is valid only when a commit footer has authenticated its exact extent.
    // Hint and blind-scan recovery must never accept an empty candidate as a fallback.
    if frames_len == 0 && !allow_empty {
        return Ok(None);
    }
    budget.prefix_matches = budget.prefix_matches.saturating_add(1);

    budget.charge_hash(bytes.len())?;
    let (body, stored_checksum) = bytes.split_at(bytes.len() - TOC_CHECKSUM_BYTES);
    let mut hasher = Hasher::new();
    hasher.update(body);
    hasher.update(&[0u8; TOC_CHECKSUM_BYTES]);
    if hasher.finalize().as_bytes() != stored_checksum {
        return Ok(None);
    }

    // Toc::decode may inspect current, V2, and V1 encodings. Reserving all three full input
    // passes is a conservative upper bound; no checksum verification re-serialization follows.
    budget.charge_decode(bytes.len().saturating_mul(MAX_TOC_DECODE_PASSES))?;
    let attempt = panic::catch_unwind(|| Toc::decode(bytes));
    match attempt {
        Ok(Ok(toc)) => Ok(Some(toc)),
        _ => Ok(None),
    }
}

fn scan_range_for_toc(
    data: &[u8],
    start: usize,
    end: usize,
    budget: &mut TocScanBudget,
) -> Result<Option<(Toc, u64)>> {
    if start >= end || end > data.len() {
        return Ok(None);
    }
    // We only ever consider offsets where the candidate TOC slice would be <= MAX_TOC_SCAN_BYTES,
    // otherwise the loop devolves into iterating over the entire file for large memories.
    let min_offset = data.len().saturating_sub(MAX_TOC_SCAN_BYTES);
    let scan_start = start.max(min_offset);

    for offset in (scan_start..end).rev() {
        let slice = &data[offset..];
        if slice.len() < TOC_CHECKSUM_BYTES {
            continue;
        }
        debug_assert!(slice.len() <= MAX_TOC_SCAN_BYTES);
        // Both supported endpoint layouts share the same structural prefix. Reject an invalid or
        // empty fallback once per offset before trying either checksum extent.
        if !matches!(verify_toc_prefix(slice), Ok(frames_len) if frames_len > 0) {
            continue;
        }
        if let Some(toc) = try_toc_layouts_at(data, offset, budget)? {
            let recovered_offset = offset as u64;
            tracing::debug!(
                recovered_offset,
                recovered_frames = toc.frames.len(),
                "recovered toc via scan"
            );
            return Ok(Some((toc, recovered_offset)));
        }
    }
    Ok(None)
}

pub(crate) fn prepare_toc_bytes(toc: &mut Toc) -> Result<Vec<u8>> {
    toc.toc_checksum = [0u8; 32];
    let bytes = toc.encode()?;
    let checksum = Toc::calculate_checksum(&bytes);
    toc.toc_checksum = checksum;
    toc.encode()
}

fn empty_toc_with_options(options: CreateOptions) -> Toc {
    Toc {
        toc_version: 0,
        segments: Vec::new(),
        frames: Vec::new(),
        indexes: IndexManifests::default(),
        time_index: None,
        temporal_track: None,
        memories_track: None,
        logic_mesh: None,
        sketch_track: None,
        segment_catalog: SegmentCatalog::default(),
        ticket_ref: ticket_ref_for_tier(options.tier),
        memory_binding: None,
        replay_manifest: None,
        enrichment_queue: crate::types::EnrichmentQueueManifest::default(),
        merkle_root: [0u8; 32],
        toc_checksum: [0u8; 32],
    }
}

fn ticket_ref_for_tier(tier: Tier) -> TicketRef {
    TicketRef {
        issuer: tier_issuer(tier).into(),
        seq_no: 1,
        expires_in_secs: 0,
        capacity_bytes: tier.capacity_bytes(),
        verified: false,
    }
}

fn tier_issuer(tier: Tier) -> &'static str {
    match tier {
        Tier::Free => "free-tier",
        Tier::Dev => "dev-tier",
        Tier::Enterprise => "enterprise-tier",
        Tier::Unlimited => "unlimited-tier",
    }
}

/// Compute the end of the payload region from frame payloads only.
/// Used once at open time to seed `cached_payload_end`.
pub(crate) fn compute_payload_region_end(toc: &Toc, header: &Header) -> u64 {
    let wal_region_end = header.wal_offset.saturating_add(header.wal_size);
    let mut max_end = wal_region_end;
    for frame in &toc.frames {
        if frame.payload_length != 0 {
            if let Some(end) = frame.payload_offset.checked_add(frame.payload_length) {
                max_end = max_end.max(end);
            }
        }
    }
    max_end
}

pub(crate) fn compute_data_end(toc: &Toc, header: &Header) -> u64 {
    // `data_end` tracks the end of all data bytes that should not be overwritten by appends:
    // - frame payloads
    // - embedded indexes / metadata segments referenced by the TOC
    // - the current footer boundary (TOC offset), since callers may safely overwrite old TOCs
    //
    // Keeping this conservative prevents WAL replay / appends from corrupting embedded segments.
    let wal_region_end = header.wal_offset.saturating_add(header.wal_size);
    let mut max_end = wal_region_end.max(header.footer_offset);

    // Frame payloads (active only).
    for frame in toc
        .frames
        .iter()
        .filter(|f| f.status == FrameStatus::Active && f.payload_length > 0)
    {
        if let Some(end) = frame.payload_offset.checked_add(frame.payload_length) {
            max_end = max_end.max(end);
        }
    }

    // Segment catalog entries.
    let catalog = &toc.segment_catalog;
    for seg in &catalog.lex_segments {
        if let Some(end) = seg.common.bytes_offset.checked_add(seg.common.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    for seg in &catalog.vec_segments {
        if let Some(end) = seg.common.bytes_offset.checked_add(seg.common.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    for seg in &catalog.time_segments {
        if let Some(end) = seg.common.bytes_offset.checked_add(seg.common.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    #[cfg(feature = "temporal_track")]
    for seg in &catalog.temporal_segments {
        if let Some(end) = seg.common.bytes_offset.checked_add(seg.common.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    #[cfg(feature = "lex")]
    for seg in &catalog.tantivy_segments {
        if let Some(end) = seg.common.bytes_offset.checked_add(seg.common.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    #[cfg(feature = "parallel_segments")]
    for seg in &catalog.index_segments {
        if let Some(end) = seg.common.bytes_offset.checked_add(seg.common.bytes_length) {
            max_end = max_end.max(end);
        }
    }

    // Global manifests (non-segment storage paths).
    if let Some(manifest) = toc.indexes.lex.as_ref() {
        if let Some(end) = manifest.bytes_offset.checked_add(manifest.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    if let Some(manifest) = toc.indexes.vec.as_ref() {
        if let Some(end) = manifest.bytes_offset.checked_add(manifest.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    if let Some(manifest) = toc.indexes.clip.as_ref() {
        if let Some(end) = manifest.bytes_offset.checked_add(manifest.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    if let Some(manifest) = toc.time_index.as_ref() {
        if let Some(end) = manifest.bytes_offset.checked_add(manifest.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    #[cfg(feature = "temporal_track")]
    if let Some(track) = toc.temporal_track.as_ref() {
        if let Some(end) = track.bytes_offset.checked_add(track.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    if let Some(track) = toc.memories_track.as_ref() {
        if let Some(end) = track.bytes_offset.checked_add(track.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    if let Some(mesh) = toc.logic_mesh.as_ref() {
        if let Some(end) = mesh.bytes_offset.checked_add(mesh.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    if let Some(track) = toc.sketch_track.as_ref() {
        if let Some(end) = track.bytes_offset.checked_add(track.bytes_length) {
            max_end = max_end.max(end);
        }
    }
    if let Some(manifest) = toc.replay_manifest.as_ref() {
        if let Some(end) = manifest.segment_offset.checked_add(manifest.segment_size) {
            max_end = max_end.max(end);
        }
    }

    tracing::debug!(
        wal_region_end,
        footer_offset = header.footer_offset,
        computed_data_end = max_end,
        "compute_data_end"
    );

    max_end
}

struct TailSnapshot {
    toc: Toc,
    footer_offset: u64,
    data_end: u64,
    generation: u64,
}

fn locate_footer_window(mmap: &[u8]) -> Option<(FooterSlice<'_>, usize)> {
    const MAX_SEARCH_SIZE: usize = 16 * 1024 * 1024;
    if mmap.is_empty() {
        return None;
    }
    let mut window = MAX_SEARCH_SIZE.min(mmap.len());
    loop {
        let start = mmap.len() - window;
        if let Some(slice) = find_last_valid_footer(&mmap[start..]) {
            return Some((slice, start));
        }
        if window == mmap.len() {
            break;
        }
        window = (window * 2).min(mmap.len());
    }
    None
}

fn load_tail_snapshot(file: &File) -> Result<TailSnapshot> {
    // Safety: we only create a read-only mapping over the stable file bytes.
    let mmap = unsafe { Mmap::map(file)? };

    let (slice, offset_adjustment) =
        locate_footer_window(&mmap).ok_or_else(|| MemvidError::InvalidToc {
            reason: "no valid commit footer found".into(),
        })?;
    let toc = Toc::decode(slice.toc_bytes)?;
    toc.verify_checksum()?;

    Ok(TailSnapshot {
        toc,
        footer_offset: slice.footer_offset as u64 + offset_adjustment as u64,
        // Using toc_offset causes stale data_end that moves footer backwards on next commit
        data_end: slice.footer_offset as u64 + offset_adjustment as u64,
        generation: slice.footer.generation,
    })
}

fn detect_generation(file: &File) -> Result<Option<u64>> {
    // Safety: read-only mapping for footer inspection.
    let mmap = unsafe { Mmap::map(file)? };

    Ok(locate_footer_window(&mmap).map(|(slice, _)| slice.footer.generation))
}

pub(crate) fn ensure_single_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let forbidden = ["-wal", "-shm", "-lock", "-journal"];
        for suffix in forbidden {
            let candidate = parent.join(format!("{name}{suffix}"));
            if candidate.exists() {
                return Err(MemvidError::AuxiliaryFileDetected { path: candidate });
            }
        }
        let hidden_forbidden = [".wal", ".shm", ".lock", ".journal"];
        for suffix in hidden_forbidden {
            let candidate = parent.join(format!(".{name}{suffix}"));
            if candidate.exists() {
                return Err(MemvidError::AuxiliaryFileDetected { path: candidate });
            }
        }
    }
    Ok(())
}

#[cfg(feature = "parallel_segments")]
fn manifest_wal_path(path: &Path) -> PathBuf {
    let mut wal_path = path.to_path_buf();
    wal_path.set_extension("manifest.wal");
    wal_path
}

#[cfg(feature = "parallel_segments")]
pub(crate) fn cleanup_manifest_wal_public(path: &Path) {
    let wal_path = manifest_wal_path(path);
    if wal_path.exists() {
        let _ = std::fs::remove_file(&wal_path);
    }
}

/// Single source of truth: does this TOC have a lexical index?
/// Checks all possible locations: old manifest, `lex_segments`, and `tantivy_segments`.
pub(crate) fn has_lex_index(toc: &Toc) -> bool {
    toc.segment_catalog.lex_enabled
        || toc.indexes.lex.is_some()
        || !toc.indexes.lex_segments.is_empty()
        || !toc.segment_catalog.tantivy_segments.is_empty()
}

/// Single source of truth: expected document count for lex index.
/// Returns None if we can't determine (e.g., Tantivy segments without manifest).
#[cfg(feature = "lex")]
pub(crate) fn lex_doc_count(
    toc: &Toc,
    lex_storage: &crate::search::EmbeddedLexStorage,
) -> Option<u64> {
    // First try old manifest
    if let Some(manifest) = &toc.indexes.lex {
        if manifest.doc_count > 0 {
            return Some(manifest.doc_count);
        }
    }

    // Then try lex_storage (contains info from lex_segments)
    let storage_count = lex_storage.doc_count();
    if storage_count > 0 {
        return Some(storage_count);
    }

    // For Tantivy files with segments but no manifest/storage doc_count,
    // we can't know doc count without loading the index.
    // Return None and let caller decide (init_tantivy should trust segments exist)
    None
}

/// Validates segment integrity on file open to catch corruption early.
/// This helps doctor by detecting issues before they cause problems.
#[allow(dead_code)]
fn validate_segment_integrity(toc: &Toc, header: &Header, file_len: u64) -> Result<()> {
    let data_limit = header.footer_offset;

    // Validate replay segment (if present). Replay is stored AT the footer boundary,
    // and footer_offset is moved forward after writing. So we only check against file_len,
    // not against footer_offset (which would be after the replay segment).
    if let Some(manifest) = toc.replay_manifest.as_ref() {
        if manifest.segment_size != 0 {
            let end = manifest
                .segment_offset
                .checked_add(manifest.segment_size)
                .ok_or_else(|| MemvidError::Doctor {
                    reason: format!(
                        "Replay segment offset overflow: {} + {}",
                        manifest.segment_offset, manifest.segment_size
                    ),
                })?;

            // Only check against file_len - replay segments sit at the footer boundary
            // and footer_offset is updated to point after them
            if end > file_len {
                return Err(MemvidError::Doctor {
                    reason: format!(
                        "Replay segment out of bounds: offset={}, length={}, end={}, file_len={}",
                        manifest.segment_offset, manifest.segment_size, end, file_len
                    ),
                });
            }
        }
    }

    // Validate Tantivy segments
    for (idx, seg) in toc.segment_catalog.tantivy_segments.iter().enumerate() {
        let offset = seg.common.bytes_offset;
        let length = seg.common.bytes_length;

        if length == 0 {
            continue; // Empty segments are okay
        }

        let end = offset
            .checked_add(length)
            .ok_or_else(|| MemvidError::Doctor {
                reason: format!("Tantivy segment {idx} offset overflow: {offset} + {length}"),
            })?;

        if end > file_len || end > data_limit {
            return Err(MemvidError::Doctor {
                reason: format!(
                    "Tantivy segment {idx} out of bounds: offset={offset}, length={length}, end={end}, file_len={file_len}, data_limit={data_limit}"
                ),
            });
        }
    }

    // Validate time index segments
    for (idx, seg) in toc.segment_catalog.time_segments.iter().enumerate() {
        let offset = seg.common.bytes_offset;
        let length = seg.common.bytes_length;

        if length == 0 {
            continue;
        }

        let end = offset
            .checked_add(length)
            .ok_or_else(|| MemvidError::Doctor {
                reason: format!("Time segment {idx} offset overflow: {offset} + {length}"),
            })?;

        if end > file_len || end > data_limit {
            return Err(MemvidError::Doctor {
                reason: format!(
                    "Time segment {idx} out of bounds: offset={offset}, length={length}, end={end}, file_len={file_len}, data_limit={data_limit}"
                ),
            });
        }
    }

    // Validate vec segments
    for (idx, seg) in toc.segment_catalog.vec_segments.iter().enumerate() {
        let offset = seg.common.bytes_offset;
        let length = seg.common.bytes_length;

        if length == 0 {
            continue;
        }

        let end = offset
            .checked_add(length)
            .ok_or_else(|| MemvidError::Doctor {
                reason: format!("Vec segment {idx} offset overflow: {offset} + {length}"),
            })?;

        if end > file_len || end > data_limit {
            return Err(MemvidError::Doctor {
                reason: format!(
                    "Vec segment {idx} out of bounds: offset={offset}, length={length}, end={end}, file_len={file_len}, data_limit={data_limit}"
                ),
            });
        }
    }

    log::debug!("✓ Segment integrity validation passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PutOptions;
    use crate::constants::HEADER_SIZE;
    use crate::footer::{CommitFooter, FOOTER_SIZE};
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    const RECOVERY_HINT_DELTAS: [i64; 10] = [0, -9, 9, -64, 64, -65, 65, -128, 128, 512];

    fn checksummed_nonempty_toc() -> (Toc, Vec<u8>) {
        let dir = tempdir().expect("TOC source tmp");
        let path = dir.path().join("source.mv2");
        let toc = {
            let mut mem = Memvid::create(&path).expect("create TOC source");
            mem.put_bytes(b"recovery fixture payload").expect("put");
            mem.commit().expect("commit");
            mem.toc.clone()
        };
        let bytes = toc.encode().expect("encode TOC");
        (toc, bytes)
    }

    fn open_fixture(bytes: &[u8]) -> (tempfile::TempDir, PathBuf, File) {
        let dir = tempdir().expect("tmp");
        let path = dir.path().join("recovery.mv2");
        std::fs::write(&path, bytes).expect("write fixture");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open fixture");
        (dir, path, file)
    }

    fn recovery_put_options(index: usize) -> PutOptions {
        PutOptions::builder()
            .uri(format!("mv2://doc/{index}"))
            .auto_tag(false)
            .extract_dates(false)
            .extract_triplets(false)
            .instant_index(false)
            .extraction_budget_ms(0)
            .build()
    }

    fn create_document_capsule(path: &Path, documents: usize) -> (Header, Toc) {
        let toc = {
            let mut mem = Memvid::create(path).expect("create document capsule");
            for index in 0..documents {
                let payload = format!("recovery document {index}");
                mem.put_bytes_with_options(payload.as_bytes(), recovery_put_options(index))
                    .expect("put recovery document");
            }
            mem.commit().expect("commit document capsule");
            mem.toc.clone()
        };
        let mut file = File::open(path).expect("open document capsule header");
        let header = HeaderCodec::read(&mut file).expect("read document capsule header");
        (header, toc)
    }

    fn hint_with_delta(offset: u64, delta: i64) -> u64 {
        offset.checked_add_signed(delta).expect("valid test hint")
    }

    fn remove_commit_footer(path: &Path, hint_delta: i64) -> Header {
        let mut bytes = std::fs::read(path).expect("read committed capsule");
        let header_bytes: [u8; HEADER_SIZE] = bytes[..HEADER_SIZE]
            .try_into()
            .expect("header-sized prefix");
        let header = HeaderCodec::decode(&header_bytes).expect("decode capsule header");
        bytes.truncate(bytes.len() - FOOTER_SIZE);
        let hint = hint_with_delta(header.footer_offset, hint_delta);
        bytes[8..16].copy_from_slice(&hint.to_le_bytes());
        std::fs::write(path, bytes).expect("write footerless capsule");
        header
    }

    fn assert_recovered_documents(path: &Path, documents: usize) {
        let mut mem = Memvid::open(path).expect("recover footerless capsule");
        assert_eq!(mem.frame_count(), documents);
        for index in 0..documents {
            let uri = format!("mv2://doc/{index}");
            let expected = format!("recovery document {index}");
            let frame = mem.frame_by_uri(&uri).expect("recovered URI");
            assert_eq!(frame.uri.as_deref(), Some(uri.as_str()));
            assert_eq!(
                mem.frame_canonical_payload(frame.id)
                    .expect("recovered payload"),
                expected.as_bytes()
            );
        }
    }

    fn rewrite_header_hint(path: &Path, hint: u64) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open capsule for header rewrite");
        let mut header = HeaderCodec::read(&mut file).expect("read capsule header for rewrite");
        header.footer_offset = hint;
        crate::persist_header(&mut file, &header).expect("rewrite capsule header hint");
    }

    fn add_test_segment(toc: &mut Toc, segment_id: u64) {
        toc.segments.push(crate::types::SegmentMeta {
            id: segment_id,
            frame_range: (0, toc.frames.len() as u64),
            primary_checksum: [0x5A; 32],
            compression: crate::types::SegmentCompression::None,
            bytes_offset: 0,
            bytes_length: 0,
        });
    }

    fn install_serialized_toc(path: &Path, toc_bytes: &[u8]) -> Header {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open fixture for TOC rewrite");
        let mut header = HeaderCodec::read(&mut file).expect("read fixture header");
        let footer = CommitFooter {
            toc_len: toc_bytes.len() as u64,
            toc_hash: Toc::calculate_checksum(toc_bytes),
            generation: 2,
        };
        file.set_len(header.footer_offset)
            .expect("truncate fixture at TOC offset");
        file.seek(SeekFrom::Start(header.footer_offset))
            .expect("seek fixture TOC offset");
        file.write_all(toc_bytes).expect("write replacement TOC");
        file.write_all(&footer.encode())
            .expect("write replacement footer");
        file.flush().expect("flush replacement TOC");
        header.toc_checksum.copy_from_slice(
            toc_bytes
                .get(toc_bytes.len() - TOC_CHECKSUM_BYTES..)
                .expect("serialized TOC checksum"),
        );
        crate::persist_header(&mut file, &header).expect("persist segmented TOC checksum");
        header
    }

    #[test]
    fn toc_prefix_underflow_surfaces_reason() {
        let err = verify_toc_prefix(&[0u8; 8]).expect_err("should reject short toc prefix");
        match err {
            MemvidError::InvalidToc { reason } => {
                assert!(
                    reason.contains("trailer too small"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn toc_prefix_accounts_for_serialized_segment_metadata() {
        let dir = tempdir().expect("prefix fixture tmp");
        let path = dir.path().join("prefix.mv2");
        let (_, mut toc) = create_document_capsule(&path, 8);
        add_test_segment(&mut toc, 1000);
        let bytes = prepare_toc_bytes(&mut toc).expect("encode segmented TOC");
        assert_eq!(verify_toc_prefix(&bytes).expect("valid prefix"), 8);

        let truncated = &bytes[..16 + 75];
        assert!(verify_toc_prefix(truncated).is_err());

        let mut excessive_segments = bytes.clone();
        excessive_segments[8..16].copy_from_slice(&1_000_001u64.to_le_bytes());
        assert!(verify_toc_prefix(&excessive_segments).is_err());

        let mut excessive_frames = bytes;
        excessive_frames[16 + 76..16 + 84].copy_from_slice(&10_000_001u64.to_le_bytes());
        assert!(verify_toc_prefix(&excessive_frames).is_err());
    }

    #[test]
    fn recovery_budget_covers_supported_toc_size_and_separates_hash_from_decode() {
        let max_toc = usize::try_from(crate::MAX_INDEX_BYTES).expect("MAX_INDEX_BYTES fits usize");
        let max_search = MAX_TOC_SCAN_BYTES * TOC_SEARCH_WORK_MULTIPLIER;
        let mut budget = TocScanBudget::for_file_len(max_toc);

        assert_eq!(budget.hash_limit, 2 * max_toc + max_search);
        assert_eq!(budget.decode_limit, 3 * max_toc);
        assert_eq!(budget.limit, 3 * 1024 * 1024 * 1024);
        budget
            .charge_hash(2 * max_toc)
            .expect("two maximum-size hashes must fit");
        budget
            .charge_decode(3 * max_toc)
            .expect("three maximum-size decode passes must fit");
        assert_eq!(budget.used(), 5 * max_toc);

        let mut false_candidate_budget = TocScanBudget::for_file_len(max_toc);
        false_candidate_budget
            .charge_hash(false_candidate_budget.hash_limit)
            .expect("hash allowance must be usable");
        let err = false_candidate_budget
            .charge_hash(1)
            .expect_err("hashing must not borrow the reserved decode allowance");
        assert!(err.to_string().contains("work limit exceeded"));
        false_candidate_budget
            .charge_decode(false_candidate_budget.decode_limit)
            .expect("decode allowance remains independently available");

        const REPRO_TOC_BYTES: usize = 135_266_972;
        let old_limit = MAX_TOC_SCAN_BYTES * 10;
        assert!(REPRO_TOC_BYTES * 5 > old_limit);
        let mut repro_budget = TocScanBudget::for_file_len(REPRO_TOC_BYTES + 69_688 + FOOTER_SIZE);
        repro_budget
            .charge_hash(REPRO_TOC_BYTES * 2)
            .expect("reproduction hashes fit the revised budget");
        repro_budget
            .charge_decode(REPRO_TOC_BYTES * 3)
            .expect("reproduction decode reserve fits the revised budget");
    }

    #[test]
    fn recovers_segmented_current_and_legacy_tocs_from_valid_footer() {
        let mut measurements = Vec::new();
        for segment_id in [7, 1000, 1_000_001] {
            let source_dir = tempdir().expect("segmented source tmp");
            let source = source_dir.path().join("source.mv2");
            let (_, mut toc) = create_document_capsule(&source, 32);
            add_test_segment(&mut toc, segment_id);
            let mut current = toc.clone();
            let variants = [
                prepare_toc_bytes(&mut current).expect("encode current segmented TOC"),
                crate::toc::encode_legacy_v1_for_test(&toc),
                crate::toc::encode_legacy_v2_for_test(&toc),
            ];

            for (variant, toc_bytes) in variants.into_iter().enumerate() {
                let case_dir = tempdir().expect("segmented case tmp");
                let path = case_dir
                    .path()
                    .join(format!("segmented-{variant}-{segment_id}.mv2"));
                std::fs::copy(&source, &path).expect("copy segmented fixture");
                let header = install_serialized_toc(&path, &toc_bytes);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("open segmented fixture for recovery");
                let file_len = file.metadata().expect("stat segmented fixture").len() as usize;
                let mut budget = TocScanBudget::for_file_len(file_len);
                let (recovered, offset) =
                    recover_toc_with_budget(&mut file, Some(header.footer_offset), &mut budget)
                        .expect("valid segmented TOC footer must recover");
                assert_eq!(offset, header.footer_offset);
                assert_eq!(recovered.frames.len(), 32);
                assert_eq!(recovered.segments[0].id, segment_id);
                assert!(budget.used() <= budget.limit);
                measurements.push((segment_id, variant, file_len, budget.used()));
                drop(file);
                assert_recovered_documents(&path, 32);
            }
        }
        eprintln!("segmented TOC footer recovery measurements: {measurements:?}");
    }

    #[test]
    fn recovers_multidocument_current_toc_without_footer_for_hint_matrix() {
        let mut measurements = Vec::new();
        for documents in [1, 8, 32, 64] {
            let source_dir = tempdir().expect("source tmp");
            let source = source_dir.path().join(format!("source-{documents}.mv2"));
            create_document_capsule(&source, documents);

            for hint_delta in RECOVERY_HINT_DELTAS {
                let case_dir = tempdir().expect("case tmp");
                let path = case_dir.path().join("footerless.mv2");
                std::fs::copy(&source, &path).expect("copy independent fixture");
                let header = remove_commit_footer(&path, hint_delta);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("open recovery measurement fixture");
                let file_len = file.metadata().expect("stat fixture").len() as usize;
                let mut budget = TocScanBudget::for_file_len(file_len);
                let hint = hint_with_delta(header.footer_offset, hint_delta);
                let (toc, recovered_offset) =
                    recover_toc_with_budget(&mut file, Some(hint), &mut budget)
                        .expect("measure footerless recovery");
                assert_eq!(toc.frames.len(), documents);
                assert_eq!(recovered_offset, header.footer_offset);
                measurements.push((
                    documents,
                    hint_delta,
                    file_len,
                    budget.hashed_bytes,
                    budget.decoded_bytes,
                    budget.used(),
                ));
                drop(file);
                assert_recovered_documents(&path, documents);
            }
        }
        eprintln!("footerless current TOC recovery measurements: {measurements:?}");
    }

    #[test]
    fn recovers_multidocument_legacy_tocs_without_footer_for_hint_matrix() {
        let mut measurements = Vec::new();
        for documents in [1, 8, 32, 64] {
            let source_dir = tempdir().expect("source tmp");
            let source = source_dir.path().join(format!("source-{documents}.mv2"));
            let (header, toc) = create_document_capsule(&source, documents);
            let source_bytes = std::fs::read(&source).expect("read source capsule");
            let variants = [
                crate::toc::encode_legacy_v1_for_test(&toc),
                crate::toc::encode_legacy_v2_for_test(&toc),
            ];

            for (variant, legacy_toc) in variants.into_iter().enumerate() {
                for hint_delta in RECOVERY_HINT_DELTAS {
                    let case_dir = tempdir().expect("legacy case tmp");
                    let path = case_dir
                        .path()
                        .join(format!("legacy-{variant}-{hint_delta}.mv2"));
                    let mut bytes = source_bytes[..header.footer_offset as usize].to_vec();
                    bytes.extend_from_slice(&legacy_toc);
                    let hint = hint_with_delta(header.footer_offset, hint_delta);
                    bytes[8..16].copy_from_slice(&hint.to_le_bytes());
                    std::fs::write(&path, bytes).expect("write legacy fixture");

                    let mut file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)
                        .expect("open legacy recovery fixture");
                    let file_len = file.metadata().expect("stat legacy fixture").len() as usize;
                    let mut budget = TocScanBudget::for_file_len(file_len);
                    let (recovered, offset) =
                        recover_toc_with_budget(&mut file, Some(hint), &mut budget)
                            .expect("measure legacy footerless recovery");
                    assert_eq!(offset, header.footer_offset);
                    assert_eq!(recovered.frames.len(), documents);
                    assert!(budget.used() <= budget.limit);
                    measurements.push((variant, documents, hint_delta, file_len, budget.used()));
                    drop(file);
                    assert_recovered_documents(&path, documents);
                }
            }
        }
        eprintln!("footerless legacy TOC recovery measurements: {measurements:?}");
    }

    #[test]
    fn recovers_current_and_legacy_tocs_without_hint() {
        let source_dir = tempdir().expect("no-hint source tmp");
        let source = source_dir.path().join("source.mv2");
        let (header, toc) = create_document_capsule(&source, 32);
        let source_bytes = std::fs::read(&source).expect("read no-hint source");
        let mut current = toc.clone();
        let variants = [
            prepare_toc_bytes(&mut current).expect("encode current no-hint TOC"),
            crate::toc::encode_legacy_v1_for_test(&toc),
            crate::toc::encode_legacy_v2_for_test(&toc),
        ];

        for (variant, toc_bytes) in variants.into_iter().enumerate() {
            let mut bytes = source_bytes[..header.footer_offset as usize].to_vec();
            bytes.extend_from_slice(&toc_bytes);
            let (_dir, _path, mut file) = open_fixture(&bytes);
            let mut budget = TocScanBudget::for_file_len(bytes.len());
            let (recovered, offset) = recover_toc_with_budget(&mut file, None, &mut budget)
                .expect("recover ordinary TOC without hint");
            assert_eq!(offset, header.footer_offset, "variant {variant}");
            assert_eq!(recovered.frames.len(), 32, "variant {variant}");
            assert!(budget.used() <= budget.limit);
        }
    }

    #[test]
    fn recovers_large_supported_toc_across_footer_and_hint_paths() {
        const LARGE_TITLE_BYTES: usize = 129 * 1024 * 1024;

        let source_dir = tempdir().expect("large TOC source tmp");
        let source = source_dir.path().join("large-source.mv2");
        let (header, mut toc) = create_document_capsule(&source, 1);
        toc.frames[0].title = Some("t".repeat(LARGE_TITLE_BYTES));
        let toc_bytes = prepare_toc_bytes(&mut toc).expect("encode large supported TOC");
        assert!(toc_bytes.len() > 128 * 1024 * 1024);
        assert!(toc_bytes.len() as u64 <= crate::MAX_INDEX_BYTES);
        install_serialized_toc(&source, &toc_bytes);
        let toc_len = toc_bytes.len();
        drop(toc_bytes);
        drop(toc);

        for case in [
            "correct-footer",
            "wrong-hint",
            "missing-footer",
            "corrupt-footer",
        ] {
            let case_dir = tempdir().expect("large TOC case tmp");
            let path = case_dir.path().join(format!("{case}.mv2"));
            std::fs::copy(&source, &path).expect("copy large TOC fixture");

            match case {
                "correct-footer" => {}
                "wrong-hint" => rewrite_header_hint(&path, header.footer_offset + 9),
                "missing-footer" => {
                    let file = OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .expect("open large fixture for footer truncation");
                    let len = file.metadata().expect("stat large fixture").len();
                    file.set_len(len - FOOTER_SIZE as u64)
                        .expect("remove large fixture footer");
                }
                "corrupt-footer" => {
                    let mut file = OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .expect("open large fixture for footer corruption");
                    let len = file.metadata().expect("stat large fixture").len();
                    file.seek(SeekFrom::Start(len - FOOTER_SIZE as u64))
                        .expect("seek large fixture footer");
                    file.write_all(b"X").expect("corrupt large fixture footer");
                    file.flush().expect("flush corrupt large fixture footer");
                }
                _ => unreachable!(),
            }

            assert_recovered_documents(&path, 1);
        }

        eprintln!(
            "large TOC recovery fixture: toc_bytes={toc_len}, toc_offset={}",
            header.footer_offset
        );
    }

    #[test]
    fn full_recovery_bounds_overlapping_invalid_footer_hashes() {
        let mut measurements = Vec::new();
        for marker_count in [64usize, 128, 256] {
            let mut bytes = vec![0xA5; FOOTER_SIZE];
            for generation in 0..marker_count {
                let footer = CommitFooter {
                    toc_len: bytes.len() as u64,
                    toc_hash: [0xFF; 32],
                    generation: generation as u64,
                };
                bytes.extend_from_slice(&footer.encode());
            }
            let (_dir, _path, mut file) = open_fixture(&bytes);
            let mut budget = TocScanBudget::for_file_len(bytes.len());
            let err = recover_toc_with_budget(&mut file, None, &mut budget)
                .expect_err("invalid overlapping footers must not recover");

            assert!(err.to_string().contains("recovery work limit exceeded"));
            assert!(budget.used() <= budget.limit);
            measurements.push((
                marker_count,
                bytes.len(),
                budget.hashed_bytes,
                budget.used(),
            ));
        }
        eprintln!("full recovery invalid-footer measurements: {measurements:?}");
    }

    #[test]
    fn second_fallback_range_uses_first_ranges_remaining_budget() {
        let prefix = [
            1, 0, 0, 0, 0, 0, 0, 0, // version
            0, 0, 0, 0, 0, 0, 0, 0, // segments
            1, 0, 0, 0, 0, 0, 0, 0, // frames
        ];
        let mut data = Vec::with_capacity(64 * 1024);
        while data.len() < 32 * 1024 {
            data.extend_from_slice(&prefix);
        }
        data.truncate(32 * 1024);
        data.extend(std::iter::repeat_n(0xFF, 32 * 1024));
        let split = data.len() / 2;
        data[split..split + prefix.len()].copy_from_slice(&prefix);
        let mut budget = TocScanBudget::new(data.len() * 2);

        let first = scan_range_for_toc(&data, split, data.len(), &mut budget)
            .expect("first range must complete within budget");
        assert!(first.is_none());
        let after_first = budget.used();
        assert!(
            after_first > 0,
            "first range must consume part of the budget"
        );
        assert!(after_first < budget.limit);

        let err = scan_range_for_toc(&data, 0, split, &mut budget)
            .expect_err("second range must consume the shared remainder");
        assert!(err.to_string().contains("recovery work limit exceeded"));
        assert!(budget.used() > after_first);
        assert!(budget.used() <= budget.limit);
    }

    #[test]
    fn recovery_scan_work_is_linear_for_zero_filled_inputs() {
        let mut measurements = Vec::new();
        for size in [4 * 1024, 16 * 1024, 64 * 1024] {
            let data = vec![0u8; size];
            let limit = size * (MAX_TOC_HASH_PASSES + TOC_SEARCH_WORK_MULTIPLIER);
            let mut budget = TocScanBudget::new(limit);
            let found = scan_range_for_toc(&data, 0, data.len(), &mut budget)
                .expect("zero-filled input must finish within budget");
            assert!(found.is_none());
            assert!(budget.hashed_bytes <= limit);
            assert_eq!(budget.decoded_bytes, 0);
            assert!(budget.candidates_checked <= size);
            measurements.push((size, budget.hashed_bytes, budget.candidates_checked));
        }

        eprintln!("zero-filled recovery measurements: {measurements:?}");
    }

    #[test]
    fn recovery_scan_bounds_repeated_plausible_prefixes_across_ranges() {
        let prefix = [
            1, 0, 0, 0, 0, 0, 0, 0, // version
            0, 0, 0, 0, 0, 0, 0, 0, // segments
            1, 0, 0, 0, 0, 0, 0, 0, // frames
        ];
        let mut data = Vec::with_capacity(96 * 1024);
        while data.len() < 96 * 1024 {
            data.extend_from_slice(&prefix);
        }
        data.truncate(96 * 1024);

        let limit = data.len() * (MAX_TOC_HASH_PASSES + TOC_SEARCH_WORK_MULTIPLIER);
        let mut budget = TocScanBudget::new(limit);
        let split = data.len() / 2;
        let result = match scan_range_for_toc(&data, split, data.len(), &mut budget) {
            Ok(None) => scan_range_for_toc(&data, 0, split, &mut budget),
            other => other,
        };

        let err = result.expect_err("repeated plausible prefixes must exhaust the shared budget");
        assert!(err.to_string().contains("recovery work limit exceeded"));
        assert!(
            budget.prefix_matches > 1,
            "fixture must exercise false candidates"
        );
        assert!(budget.hashed_bytes <= limit);
        assert!(budget.hashed_bytes.saturating_add(budget.decoded_bytes) <= limit);
    }

    #[test]
    fn recovery_finds_checksum_verified_toc_from_commit_footer() {
        let (expected, toc_bytes) = checksummed_nonempty_toc();
        let footer = CommitFooter {
            toc_len: toc_bytes.len() as u64,
            toc_hash: Toc::calculate_checksum(&toc_bytes),
            generation: 7,
        };
        let mut bytes = vec![0xA5; 257];
        let expected_offset = bytes.len() as u64;
        bytes.extend_from_slice(&toc_bytes);
        bytes.extend_from_slice(&footer.encode());
        let (_dir, _path, mut file) = open_fixture(&bytes);

        let (recovered, offset) = recover_toc(&mut file, None).expect("recover by footer");
        assert_eq!(offset, expected_offset);
        assert_eq!(recovered.toc_checksum, expected.toc_checksum);
    }

    #[test]
    fn recovery_uses_intact_toc_when_commit_footer_is_corrupt() {
        let source_dir = tempdir().expect("corrupt-footer source tmp");
        let source = source_dir.path().join("source.mv2");
        let (header, expected) = create_document_capsule(&source, 32);

        for hint_delta in [0, -128, 128] {
            let case_dir = tempdir().expect("corrupt-footer case tmp");
            let path = case_dir
                .path()
                .join(format!("corrupt-footer-{hint_delta}.mv2"));
            std::fs::copy(&source, &path).expect("copy corrupt-footer fixture");
            let mut bytes = std::fs::read(&path).expect("read corrupt-footer fixture");
            let footer_offset = bytes.len() - FOOTER_SIZE;
            bytes[footer_offset] ^= 0xFF;
            std::fs::write(&path, bytes).expect("corrupt commit footer");
            let hint = hint_with_delta(header.footer_offset, hint_delta);
            rewrite_header_hint(&path, hint);

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .expect("open corrupt-footer fixture");
            let mut budget = TocScanBudget::for_file_len(
                file.metadata().expect("stat corrupt-footer fixture").len() as usize,
            );
            let (recovered, offset) = recover_toc_with_budget(&mut file, Some(hint), &mut budget)
                .expect("recover intact TOC before corrupt footer");
            assert_eq!(offset, header.footer_offset);
            assert_eq!(recovered.toc_checksum, expected.toc_checksum);
            assert!(budget.used() <= budget.limit);
            drop(file);
            assert_recovered_documents(&path, 32);
        }
    }

    #[test]
    fn recovery_scan_supports_legacy_toc_and_incorrect_hint() {
        let source_dir = tempdir().expect("source tmp");
        let source_path = source_dir.path().join("source.mv2");
        let expected = {
            let mut mem = Memvid::create(&source_path).expect("create source");
            mem.put_bytes(b"legacy recovery payload").expect("put");
            mem.commit().expect("commit");
            mem.toc.clone()
        };
        let variants = [
            crate::toc::encode_legacy_v1_for_test(&expected),
            crate::toc::encode_legacy_v2_for_test(&expected),
        ];
        for legacy_bytes in variants {
            let (legacy_body, legacy_checksum) = legacy_bytes.split_at(legacy_bytes.len() - 32);
            let mut hasher = Hasher::new();
            hasher.update(legacy_body);
            hasher.update(&[0u8; 32]);
            assert_eq!(hasher.finalize().as_bytes(), legacy_checksum);
            let mut bytes = vec![0xA5; 211];
            let expected_offset = bytes.len() as u64;
            bytes.extend_from_slice(&legacy_bytes);
            let (_dir, _path, mut file) = open_fixture(&bytes);

            let wrong_hint = expected_offset + 9;
            let (recovered, offset) =
                recover_toc(&mut file, Some(wrong_hint)).expect("scan for legacy TOC");
            assert_eq!(offset, expected_offset);
            assert_eq!(recovered.frames.len(), expected.frames.len());
            recovered.verify_checksum().expect("legacy checksum");
        }
    }

    #[test]
    fn recovery_rejects_corrupt_toc_checksum() {
        let (_toc, mut toc_bytes) = checksummed_nonempty_toc();
        let expected_offset = 149;
        toc_bytes.last_mut().map(|byte| *byte ^= 0xFF);
        let mut bytes = vec![0xA5; expected_offset];
        bytes.extend_from_slice(&toc_bytes);
        let (_dir, path, mut file) = open_fixture(&bytes);
        let before = std::fs::read(&path).expect("read before recovery");

        let err = recover_toc(&mut file, Some(expected_offset as u64))
            .expect_err("corrupt checksum must not recover");
        assert!(matches!(err, MemvidError::InvalidToc { .. }));
        assert_eq!(std::fs::read(path).expect("read after recovery"), before);
    }

    #[test]
    fn truncated_at_header_footer_offset_fails_without_modifying_file() {
        let dir = tempdir().expect("tmp");
        let path = dir.path().join("truncated.mv2");
        {
            let mut mem = Memvid::create(&path).expect("create");
            mem.put_bytes(b"payload retained before missing TOC")
                .expect("put");
            mem.commit().expect("commit");
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open fixture");
        let mut header_bytes = [0u8; HEADER_SIZE];
        file.read_exact(&mut header_bytes).expect("read header");
        let header = HeaderCodec::decode(&header_bytes).expect("decode header");
        file.set_len(header.footer_offset)
            .expect("truncate before TOC");
        file.flush().expect("flush truncation");
        drop(file);
        let before = std::fs::read(&path).expect("read truncated fixture");

        let err = Memvid::open(&path).err().expect("missing TOC must fail");
        assert!(matches!(err, MemvidError::InvalidToc { .. }));
        assert_eq!(
            std::fs::read(&path).expect("read after failed open"),
            before
        );
        assert_eq!(before.len() as u64, header.footer_offset);
        assert!(before.len() >= HEADER_SIZE + FOOTER_SIZE);
    }

    #[test]
    fn ensure_single_file_blocks_sidecars() {
        let dir = tempdir().expect("tmp");
        let path = dir.path().join("mem.mv2");
        std::fs::write(dir.path().join("mem.mv2-wal"), b"junk").expect("sidecar");
        let result = Memvid::create(&path);
        assert!(matches!(
            result,
            Err(MemvidError::AuxiliaryFileDetected { .. })
        ));
    }
}
