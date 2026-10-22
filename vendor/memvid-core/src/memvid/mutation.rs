//! Frame mutation and ingestion routines for `Memvid`.
//!
//! Owns the ingest pipeline: bytes/documents → extraction → chunking → metadata/temporal tags
//! → WAL entries → manifest/index updates. This module keeps mutations crash-safe and
//! deterministic; no bytes touch the data region until the embedded WAL is flushed during commit.
//!
//! The long-term structure will split into ingestion/chunking/WAL staging modules. For now
//! everything lives here, grouped by section so the pipeline is easy to scan.

use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
#[cfg(test)]
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bincode::serde::{decode_from_slice, encode_to_vec};
use blake3::hash;
use log::info;
#[cfg(feature = "temporal_track")]
use once_cell::sync::OnceCell;
#[cfg(feature = "temporal_track")]
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json;
use zstd;

use atomic_write_file::AtomicWriteFile;

use tracing::instrument;

#[cfg(feature = "parallel_segments")]
use super::{
    builder::BuildOpts,
    planner::{SegmentChunkPlan, SegmentPlanner},
    workers::SegmentWorkerPool,
};
#[cfg(feature = "temporal_track")]
use crate::TemporalTrackManifest;
use crate::analysis::auto_tag::AutoTagger;
use crate::constants::{WAL_SIZE_LARGE, WAL_SIZE_MEDIUM};
use crate::footer::CommitFooter;
use crate::io::wal::{EmbeddedWal, WalRecord};
use crate::lock::FileLock;
use crate::memvid::chunks::{plan_document_chunks, plan_text_chunks};
use crate::memvid::lifecycle::{Memvid, prepare_toc_bytes};
use crate::reader::{
    DocumentFormat, DocumentReader, PassthroughReader, ReaderDiagnostics, ReaderHint, ReaderOutput,
    ReaderRegistry,
};
#[cfg(feature = "lex")]
use crate::search::{EmbeddedLexSegment, LexWalBatch, TantivySnapshot};
use crate::triplet::TripletExtractor;
#[cfg(feature = "lex")]
use crate::types::TantivySegmentDescriptor;
use crate::types::{
    CanonicalEncoding, DocMetadata, Frame, FrameId, FrameRole, FrameStatus, PutManyOpts,
    PutOptions, SegmentCommon, TextChunkManifest, Tier,
};
#[cfg(feature = "parallel_segments")]
use crate::types::{IndexSegmentRef, SegmentKind, SegmentSpan, SegmentStats};
#[cfg(feature = "temporal_track")]
use crate::{
    AnchorSource, TemporalAnchor, TemporalContext, TemporalMention, TemporalMentionFlags,
    TemporalMentionKind, TemporalNormalizer, TemporalResolution, TemporalResolutionFlag,
    TemporalResolutionValue,
};
use crate::{
    DEFAULT_SEARCH_TEXT_LIMIT, ExtractedDocument, MemvidError, Result, TimeIndexEntry,
    TimeIndexManifest, VecIndexManifest, normalize_text, time_index_append, wal_config,
};
#[cfg(feature = "temporal_track")]
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

const MAGIC_SNIFF_BYTES: usize = 16;
const WAL_ENTRY_HEADER_SIZE: u64 = 48;
const WAL_SHIFT_BUFFER_SIZE: usize = 8 * 1024 * 1024;

#[cfg(all(test, feature = "lex"))]
#[path = "tantivy_rebuild_tests.rs"]
mod tantivy_rebuild_tests;

#[cfg(all(test, feature = "lex"))]
thread_local! {
    pub(crate) static TANTIVY_REBUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static FAIL_TANTIVY_REBUILD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
thread_local! {
    static FAIL_BEFORE_ATOMIC_PUBLISH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_BEFORE_STAGING_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_AFTER_ATOMIC_PUBLISH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_AFTER_COMPACT_TRUNCATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_AFTER_REBUILD_TRUNCATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static EXIT_AFTER_REBUILD_TRUNCATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn fail_before_atomic_publish() -> Result<()> {
    let should_fail = FAIL_BEFORE_ATOMIC_PUBLISH.with(|flag| flag.replace(false));
    if should_fail {
        return Err(std::io::Error::other("injected failure before atomic publish").into());
    }
    Ok(())
}

#[cfg(test)]
fn fail_before_staging_sync() -> Result<()> {
    let should_fail = FAIL_BEFORE_STAGING_SYNC.with(|flag| flag.replace(false));
    if should_fail {
        return Err(std::io::Error::other("injected failure before staging sync").into());
    }
    Ok(())
}

#[cfg(test)]
fn fail_after_atomic_publish() -> Result<()> {
    let should_fail = FAIL_AFTER_ATOMIC_PUBLISH.with(|flag| flag.replace(false));
    if should_fail {
        return Err(
            std::io::Error::other("injected durability failure after atomic publish").into(),
        );
    }
    Ok(())
}

#[cfg(test)]
fn fail_after_compact_truncate() -> Result<()> {
    let should_fail = FAIL_AFTER_COMPACT_TRUNCATE.with(|flag| flag.replace(false));
    if should_fail {
        return Err(std::io::Error::other("injected failure after compact truncate").into());
    }
    Ok(())
}

#[cfg(feature = "temporal_track")]
const DEFAULT_TEMPORAL_TZ: &str = "America/Chicago";

#[cfg(feature = "temporal_track")]
const STATIC_TEMPORAL_PHRASES: &[&str] = &[
    "today",
    "yesterday",
    "tomorrow",
    "two days ago",
    "in 3 days",
    "two weeks from now",
    "2 weeks from now",
    "two fridays ago",
    "last friday",
    "next friday",
    "this friday",
    "next week",
    "last week",
    "end of this month",
    "start of next month",
    "last month",
    "3 months ago",
    "in 90 minutes",
    "at 5pm today",
    "in the last 24 hours",
    "this morning",
    "on the sunday after next",
    "next daylight saving change",
    "midnight tomorrow",
    "noon next tuesday",
    "first business day of next month",
    "the first business day of next month",
    "end of q3",
    "next wednesday at 9",
    "sunday at 1:30am",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
];

struct CommitStaging {
    atomic: AtomicWriteFile,
}

impl CommitStaging {
    fn prepare(path: &Path) -> Result<Self> {
        let mut options = AtomicWriteFile::options();
        options.read(true);
        let atomic = options.open(path)?;
        Ok(Self { atomic })
    }

    fn copy_from(&mut self, source: &File) -> Result<()> {
        let mut reader = source.try_clone()?;
        reader.seek(SeekFrom::Start(0))?;

        let writer = self.atomic.as_file_mut();
        writer.set_len(0)?;
        writer.seek(SeekFrom::Start(0))?;
        std::io::copy(&mut reader, writer)?;
        writer.flush()?;
        writer.sync_all()?;
        Ok(())
    }

    fn clone_file(&self) -> Result<File> {
        Ok(self.atomic.as_file().try_clone()?)
    }

    fn commit(self) -> Result<()> {
        self.atomic.commit()?;
        #[cfg(test)]
        fail_after_atomic_publish()?;
        Ok(())
    }

    fn discard(self) -> Result<()> {
        self.atomic.discard().map_err(Into::into)
    }
}

#[derive(Debug, Default)]
struct IngestionDelta {
    inserted_frames: Vec<FrameId>,
    inserted_embeddings: Vec<(FrameId, Vec<f32>)>,
    inserted_time_entries: Vec<TimeIndexEntry>,
    mutated_frames: bool,
    #[cfg(feature = "temporal_track")]
    inserted_temporal_mentions: Vec<TemporalMention>,
    #[cfg(feature = "temporal_track")]
    inserted_temporal_anchors: Vec<TemporalAnchor>,
}

impl IngestionDelta {
    fn is_empty(&self) -> bool {
        #[allow(unused_mut)]
        let mut empty = self.inserted_frames.is_empty()
            && self.inserted_embeddings.is_empty()
            && self.inserted_time_entries.is_empty()
            && !self.mutated_frames;
        #[cfg(feature = "temporal_track")]
        {
            empty = empty
                && self.inserted_temporal_mentions.is_empty()
                && self.inserted_temporal_anchors.is_empty();
        }
        empty
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitMode {
    Full,
    Incremental,
}

impl Default for CommitMode {
    fn default() -> Self {
        Self::Full
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CommitOptions {
    pub mode: CommitMode,
    pub background: bool,
}

impl CommitOptions {
    #[must_use]
    pub fn new(mode: CommitMode) -> Self {
        Self {
            mode,
            background: false,
        }
    }

    #[must_use]
    pub fn background(mut self, background: bool) -> Self {
        self.background = background;
        self
    }
}

fn default_reader_registry() -> &'static ReaderRegistry {
    static REGISTRY: OnceLock<ReaderRegistry> = OnceLock::new();
    REGISTRY.get_or_init(ReaderRegistry::default)
}

fn infer_document_format(
    mime: Option<&str>,
    magic: Option<&[u8]>,
    uri: Option<&str>,
) -> Option<DocumentFormat> {
    // Check PDF magic bytes first
    if detect_pdf_magic(magic) {
        return Some(DocumentFormat::Pdf);
    }

    // For ZIP-based OOXML formats (DOCX, XLSX, PPTX), magic bytes are just ZIP header
    // so we need to check file extension to distinguish them
    if let Some(magic_bytes) = magic {
        if magic_bytes.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
            // It's a ZIP file - check extension to determine OOXML type
            if let Some(format) = infer_format_from_extension(uri) {
                return Some(format);
            }
        }
    }

    // Try MIME type
    if let Some(mime_str) = mime {
        let mime_lower = mime_str.trim().to_ascii_lowercase();
        let format = match mime_lower.as_str() {
            "application/pdf" => Some(DocumentFormat::Pdf),
            "text/plain" => Some(DocumentFormat::PlainText),
            "text/markdown" => Some(DocumentFormat::Markdown),
            "text/html" | "application/xhtml+xml" => Some(DocumentFormat::Html),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
                Some(DocumentFormat::Docx)
            }
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => {
                Some(DocumentFormat::Xlsx)
            }
            "application/vnd.ms-excel" => Some(DocumentFormat::Xls),
            "application/vnd.openxmlformats-officedocument.presentationml.presentation" => {
                Some(DocumentFormat::Pptx)
            }
            other if other.starts_with("text/") => Some(DocumentFormat::PlainText),
            _ => None,
        };
        if format.is_some() {
            return format;
        }
    }

    // Fall back to extension-based detection
    infer_format_from_extension(uri)
}

/// Infer document format from file extension in URI/path
fn infer_format_from_extension(uri: Option<&str>) -> Option<DocumentFormat> {
    let uri = uri?;
    let path = std::path::Path::new(uri);
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    match ext.as_str() {
        "pdf" => Some(DocumentFormat::Pdf),
        "docx" => Some(DocumentFormat::Docx),
        "xlsx" => Some(DocumentFormat::Xlsx),
        "xls" => Some(DocumentFormat::Xls),
        "pptx" => Some(DocumentFormat::Pptx),
        "txt" | "text" | "log" | "cfg" | "ini" | "json" | "yaml" | "yml" | "toml" | "csv"
        | "tsv" | "rs" | "py" | "js" | "ts" | "tsx" | "jsx" | "c" | "h" | "cpp" | "hpp" | "go"
        | "rb" | "php" | "css" | "scss" | "sh" | "bash" | "swift" | "kt" | "java" | "scala"
        | "sql" => Some(DocumentFormat::PlainText),
        "md" | "markdown" => Some(DocumentFormat::Markdown),
        "html" | "htm" => Some(DocumentFormat::Html),
        _ => None,
    }
}

fn detect_pdf_magic(magic: Option<&[u8]>) -> bool {
    let mut slice = match magic {
        Some(slice) if !slice.is_empty() => slice,
        _ => return false,
    };
    if slice.starts_with(&[0xEF, 0xBB, 0xBF]) {
        slice = &slice[3..];
    }
    while let Some((first, rest)) = slice.split_first() {
        if first.is_ascii_whitespace() {
            slice = rest;
        } else {
            break;
        }
    }
    slice.starts_with(b"%PDF")
}

#[instrument(
    target = "memvid::extract",
    skip_all,
    fields(mime = mime_hint, uri = uri)
)]
fn extract_via_registry(
    bytes: &[u8],
    mime_hint: Option<&str>,
    uri: Option<&str>,
) -> Result<ExtractedDocument> {
    let registry = default_reader_registry();
    let magic = bytes
        .get(..MAGIC_SNIFF_BYTES)
        .and_then(|slice| if slice.is_empty() { None } else { Some(slice) });
    let hint = ReaderHint::new(mime_hint, infer_document_format(mime_hint, magic, uri))
        .with_uri(uri)
        .with_magic(magic);

    let fallback_reason = if let Some(reader) = registry.find_reader(&hint) {
        let start = Instant::now();
        match reader.extract(bytes, &hint) {
            Ok(output) => {
                return Ok(finalize_reader_output(output, start));
            }
            Err(err) => {
                tracing::error!(
                    target = "memvid::extract",
                    reader = reader.name(),
                    error = %err,
                    "reader failed; falling back"
                );
                Some(format!("reader {} failed: {err}", reader.name()))
            }
        }
    } else {
        tracing::debug!(
            target = "memvid::extract",
            format = hint.format.map(super::super::reader::DocumentFormat::label),
            "no reader matched; using default extractor"
        );
        Some("no registered reader matched; using default extractor".to_string())
    };

    let start = Instant::now();
    let mut output = PassthroughReader.extract(bytes, &hint)?;
    if let Some(reason) = fallback_reason {
        output.diagnostics.track_warning(reason);
    }
    Ok(finalize_reader_output(output, start))
}

fn finalize_reader_output(output: ReaderOutput, start: Instant) -> ExtractedDocument {
    let elapsed = start.elapsed();
    let ReaderOutput {
        document,
        reader_name,
        diagnostics,
    } = output;
    log_reader_result(&reader_name, &diagnostics, elapsed);
    document
}

fn log_reader_result(reader: &str, diagnostics: &ReaderDiagnostics, elapsed: Duration) {
    let duration_ms = diagnostics
        .duration_ms
        .unwrap_or(elapsed.as_millis().try_into().unwrap_or(u64::MAX));
    let warnings = diagnostics.warnings.len();
    let pages = diagnostics.pages_processed;

    if warnings > 0 || diagnostics.fallback {
        tracing::warn!(
            target = "memvid::extract",
            reader,
            duration_ms,
            pages,
            warnings,
            fallback = diagnostics.fallback,
            "extraction completed with warnings"
        );
        for warning in &diagnostics.warnings {
            tracing::warn!(target = "memvid::extract", reader, warning = %warning);
        }
    } else {
        tracing::info!(
            target = "memvid::extract",
            reader,
            duration_ms,
            pages,
            "extraction completed"
        );
    }
}

impl Memvid {
    // -- Public ingestion entrypoints ---------------------------------------------------------

    fn with_staging_lock<F>(&mut self, op: F) -> Result<()>
    where
        F: FnOnce(&mut Self) -> Result<()>,
    {
        self.file.sync_all()?;
        let mut staging = CommitStaging::prepare(self.path())?;
        staging.copy_from(&self.file)?;

        let staging_handle = staging.clone_file()?;
        // Lock the inode that will become the destination before publishing it. The old
        // destination lock remains held until this one has been installed on `self`.
        let staging_lock = FileLock::acquire(&staging_handle, self.path())?;
        let new_wal = EmbeddedWal::open(&staging_handle, &self.header)?;
        let original_file = std::mem::replace(&mut self.file, staging_handle);
        let original_wal = std::mem::replace(&mut self.wal, new_wal);
        let original_header = self.header.clone();
        let original_toc = self.toc.clone();
        let original_data_end = self.data_end;
        let original_generation = self.generation;
        let original_dirty = self.dirty;
        let original_lex_enabled = self.lex_enabled;
        #[cfg(feature = "lex")]
        let original_tantivy_dirty = self.tantivy_dirty;

        let mut original_file = Some(original_file);
        let mut original_wal = Some(original_wal);

        let operation_result = op(self).and_then(|()| {
            #[cfg(test)]
            fail_before_staging_sync()?;
            self.file.sync_all().map_err(Into::into)
        });
        if let Err(err) = operation_result {
            let _ = staging.discard();
            if let Some(file) = original_file.take() {
                self.file = file;
            }
            if let Some(wal) = original_wal.take() {
                self.wal = wal;
            }
            self.header = original_header;
            self.toc = original_toc;
            self.data_end = original_data_end;
            self.generation = original_generation;
            self.dirty = original_dirty;
            self.lex_enabled = original_lex_enabled;
            #[cfg(feature = "lex")]
            {
                self.tantivy_dirty = original_tantivy_dirty;
            }
            self.write_disabled = Some(format!(
                "writable access disabled after staging operation failed: {err}"
            ));
            return Err(err);
        }

        #[cfg(test)]
        let publish_result = fail_before_atomic_publish().and_then(|()| staging.commit());
        #[cfg(not(test))]
        let publish_result = staging.commit();
        match publish_result {
            Ok(()) => {
                drop(original_file.take());
                drop(original_wal.take());
                let old_lock = std::mem::replace(&mut self.lock, staging_lock);
                drop(old_lock);
                Ok(())
            }
            Err(commit_err) => {
                // AtomicWriteFile performs rename before syncing the containing directory. An
                // error therefore does not say whether publication happened. Determine which
                // inode owns the destination while both possible inodes are still locked.
                let staging_is_current = FileLock::is_current_path_file(&self.file, self.path());
                let original_is_current = original_file.as_ref().map_or(Ok(false), |file| {
                    FileLock::is_current_path_file(file, self.path())
                });

                if matches!(staging_is_current, Ok(true)) {
                    drop(original_file.take());
                    drop(original_wal.take());
                    let old_lock = std::mem::replace(&mut self.lock, staging_lock);
                    drop(old_lock);
                    self.write_disabled = Some(format!(
                        "writable access disabled after publication with uncertain durability: {commit_err}"
                    ));
                    return Err(commit_err);
                }

                if matches!(staging_is_current, Ok(false))
                    && matches!(original_is_current, Ok(true))
                {
                    if let Some(file) = original_file.take() {
                        self.file = file;
                    }
                    if let Some(wal) = original_wal.take() {
                        self.wal = wal;
                    }
                    self.header = original_header;
                    self.toc = original_toc;
                    self.data_end = original_data_end;
                    self.generation = original_generation;
                    self.dirty = original_dirty;
                    self.lex_enabled = original_lex_enabled;
                    #[cfg(feature = "lex")]
                    {
                        self.tantivy_dirty = original_tantivy_dirty;
                    }
                    self.write_disabled = Some(format!(
                        "writable access disabled after atomic publication failed before replacement: {commit_err}"
                    ));
                    return Err(commit_err);
                }

                // If identity inspection itself failed, do not guess. Keep both candidate locks,
                // retain the staging state, and permanently disable writes on this handle.
                drop(original_file.take());
                drop(original_wal.take());
                let old_lock = std::mem::replace(&mut self.lock, staging_lock);
                self.publication_fallback_lock = Some(old_lock);
                self.write_disabled = Some(format!(
                    "writable access disabled because publication state is indeterminate: {commit_err}"
                ));
                Err(commit_err)
            }
        }
    }

    pub(crate) fn catalog_data_end(&self) -> u64 {
        let mut max_end = self.header.wal_offset + self.header.wal_size;

        for descriptor in &self.toc.segment_catalog.lex_segments {
            if descriptor.common.bytes_length == 0 {
                continue;
            }
            max_end = max_end.max(descriptor.common.bytes_offset + descriptor.common.bytes_length);
        }

        for descriptor in &self.toc.segment_catalog.vec_segments {
            if descriptor.common.bytes_length == 0 {
                continue;
            }
            max_end = max_end.max(descriptor.common.bytes_offset + descriptor.common.bytes_length);
        }

        for descriptor in &self.toc.segment_catalog.time_segments {
            if descriptor.common.bytes_length == 0 {
                continue;
            }
            max_end = max_end.max(descriptor.common.bytes_offset + descriptor.common.bytes_length);
        }

        #[cfg(feature = "temporal_track")]
        for descriptor in &self.toc.segment_catalog.temporal_segments {
            if descriptor.common.bytes_length == 0 {
                continue;
            }
            max_end = max_end.max(descriptor.common.bytes_offset + descriptor.common.bytes_length);
        }

        #[cfg(feature = "lex")]
        for descriptor in &self.toc.segment_catalog.tantivy_segments {
            if descriptor.common.bytes_length == 0 {
                continue;
            }
            max_end = max_end.max(descriptor.common.bytes_offset + descriptor.common.bytes_length);
        }

        if let Some(manifest) = self.toc.indexes.lex.as_ref() {
            if manifest.bytes_length != 0 {
                max_end = max_end.max(manifest.bytes_offset + manifest.bytes_length);
            }
        }

        if let Some(manifest) = self.toc.indexes.vec.as_ref() {
            if manifest.bytes_length != 0 {
                max_end = max_end.max(manifest.bytes_offset + manifest.bytes_length);
            }
        }

        if let Some(manifest) = self.toc.time_index.as_ref() {
            if manifest.bytes_length != 0 {
                max_end = max_end.max(manifest.bytes_offset + manifest.bytes_length);
            }
        }

        #[cfg(feature = "temporal_track")]
        if let Some(track) = self.toc.temporal_track.as_ref() {
            if track.bytes_length != 0 {
                max_end = max_end.max(track.bytes_offset + track.bytes_length);
            }
        }

        if let Some(manifest) = self.toc.replay_manifest.as_ref() {
            if let Some(end) = manifest.segment_offset.checked_add(manifest.segment_size) {
                max_end = max_end.max(end);
            }
        }

        max_end
    }

    fn payload_region_end(&self) -> u64 {
        self.cached_payload_end
    }

    fn append_wal_entry(&mut self, payload: &[u8]) -> Result<u64> {
        loop {
            match self.wal.append_entry(payload) {
                Ok(seq) => return Ok(seq),
                Err(MemvidError::CheckpointFailed { reason })
                    if reason == "embedded WAL region too small for entry"
                        || reason == "embedded WAL region full" =>
                {
                    // WAL is either too small for this entry or full with pending entries.
                    // Grow the WAL to accommodate - doubling ensures we have space.
                    let required = WAL_ENTRY_HEADER_SIZE
                        .saturating_add(payload.len() as u64)
                        .max(self.header.wal_size + 1);
                    self.grow_wal_region(required)?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn grow_wal_region(&mut self, required_entry_size: u64) -> Result<()> {
        let mut new_size = self.header.wal_size;
        let mut target = required_entry_size;
        if target == 0 {
            target = self.header.wal_size;
        }
        while new_size <= target {
            new_size = new_size
                .checked_mul(2)
                .ok_or_else(|| MemvidError::CheckpointFailed {
                    reason: "wal_size overflow".into(),
                })?;
        }
        let delta = new_size - self.header.wal_size;
        if delta == 0 {
            return Ok(());
        }

        self.shift_data_for_wal_growth(delta)?;
        self.header.wal_size = new_size;
        self.apply_wal_growth_offsets(delta);

        let catalog_end = self.catalog_data_end();
        self.header.footer_offset = catalog_end
            .max(self.header.footer_offset)
            .max(self.data_end);

        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        self.wal = EmbeddedWal::open(&self.file, &self.header)?;
        Ok(())
    }

    fn shift_data_for_wal_growth(&mut self, delta: u64) -> Result<()> {
        if delta == 0 {
            return Ok(());
        }
        let original_len = self.file.metadata()?.len();
        let data_start = self.header.wal_offset + self.header.wal_size;
        self.file.set_len(original_len + delta)?;

        let mut remaining = original_len.saturating_sub(data_start);
        let mut buffer = vec![0u8; WAL_SHIFT_BUFFER_SIZE];
        while remaining > 0 {
            let chunk = min(remaining, buffer.len() as u64);
            let src = data_start + remaining - chunk;
            self.file.seek(SeekFrom::Start(src))?;
            #[allow(clippy::cast_possible_truncation)]
            self.file.read_exact(&mut buffer[..chunk as usize])?;
            let dst = src + delta;
            self.file.seek(SeekFrom::Start(dst))?;
            #[allow(clippy::cast_possible_truncation)]
            self.file.write_all(&buffer[..chunk as usize])?;
            remaining -= chunk;
        }

        self.file.seek(SeekFrom::Start(data_start))?;
        let zero_buf = vec![0u8; WAL_SHIFT_BUFFER_SIZE];
        let mut remaining = delta;
        while remaining > 0 {
            let write = min(remaining, zero_buf.len() as u64);
            #[allow(clippy::cast_possible_truncation)]
            self.file.write_all(&zero_buf[..write as usize])?;
            remaining -= write;
        }
        Ok(())
    }

    fn apply_wal_growth_offsets(&mut self, delta: u64) {
        if delta == 0 {
            return;
        }

        self.header.footer_offset = self.header.footer_offset.saturating_add(delta);
        self.data_end = self.data_end.saturating_add(delta);
        // `cached_payload_end` mirrors `data_end` / footer positioning. After
        // `shift_data_for_wal_growth` every byte past the old WAL boundary
        // moved right by `delta`, so the cached payload boundary must also
        // advance. Forgetting this caused `rebuild_indexes` (which seeks to
        // `payload_region_end()`) to write embedded indexes back into the
        // grown portion of the WAL region, corrupting WAL record payloads.
        // See https://github.com/memvid/memvid/issues/230.
        self.cached_payload_end = self.cached_payload_end.saturating_add(delta);
        self.adjust_offsets_after_wal_growth(delta);
    }

    fn adjust_offsets_after_wal_growth(&mut self, delta: u64) {
        if delta == 0 {
            return;
        }

        for frame in &mut self.toc.frames {
            if frame.payload_offset != 0 {
                frame.payload_offset += delta;
            }
        }

        for segment in &mut self.toc.segments {
            if segment.bytes_offset != 0 {
                segment.bytes_offset += delta;
            }
        }

        if let Some(lex) = self.toc.indexes.lex.as_mut() {
            if lex.bytes_offset != 0 {
                lex.bytes_offset += delta;
            }
        }
        for manifest in &mut self.toc.indexes.lex_segments {
            if manifest.bytes_offset != 0 {
                manifest.bytes_offset += delta;
            }
        }
        if let Some(vec) = self.toc.indexes.vec.as_mut() {
            if vec.bytes_offset != 0 {
                vec.bytes_offset += delta;
            }
        }
        if let Some(time_index) = self.toc.time_index.as_mut() {
            if time_index.bytes_offset != 0 {
                time_index.bytes_offset += delta;
            }
        }
        if let Some(manifest) = self.toc.replay_manifest.as_mut() {
            if manifest.segment_offset != 0 {
                manifest.segment_offset = manifest.segment_offset.saturating_add(delta);
            }
        }
        #[cfg(feature = "temporal_track")]
        if let Some(track) = self.toc.temporal_track.as_mut() {
            if track.bytes_offset != 0 {
                track.bytes_offset += delta;
            }
        }

        let catalog = &mut self.toc.segment_catalog;
        for descriptor in &mut catalog.lex_segments {
            if descriptor.common.bytes_offset != 0 {
                descriptor.common.bytes_offset += delta;
            }
        }
        for descriptor in &mut catalog.vec_segments {
            if descriptor.common.bytes_offset != 0 {
                descriptor.common.bytes_offset += delta;
            }
        }
        for descriptor in &mut catalog.time_segments {
            if descriptor.common.bytes_offset != 0 {
                descriptor.common.bytes_offset += delta;
            }
        }
        #[cfg(feature = "temporal_track")]
        for descriptor in &mut catalog.temporal_segments {
            if descriptor.common.bytes_offset != 0 {
                descriptor.common.bytes_offset += delta;
            }
        }
        for descriptor in &mut catalog.tantivy_segments {
            if descriptor.common.bytes_offset != 0 {
                descriptor.common.bytes_offset += delta;
            }
        }

        #[cfg(feature = "lex")]
        if let Ok(mut storage) = self.lex_storage.write() {
            storage.adjust_offsets(delta);
        }
    }
    pub fn commit_with_options(&mut self, options: CommitOptions) -> Result<()> {
        self.ensure_writable()?;
        if options.background {
            tracing::debug!("commit background flag ignored; running synchronously");
        }
        let mode = options.mode;
        let records = self.wal.pending_records()?;
        if records.is_empty() && !self.dirty && !self.tantivy_index_pending() {
            return Ok(());
        }
        self.with_staging_lock(move |mem| mem.commit_from_records(records, mode))
    }

    pub fn commit(&mut self) -> Result<()> {
        self.ensure_writable()?;
        self.commit_with_options(CommitOptions::new(CommitMode::Full))
    }

    /// Enter batch mode for high-throughput ingestion.
    ///
    /// While batch mode is active:
    /// - WAL fsync is skipped on every append (controlled by `opts.skip_sync`)
    /// - Auto-checkpoint is suppressed (controlled by `opts.disable_auto_checkpoint`)
    /// - Compression level is lowered (controlled by `opts.compression_level`)
    /// - WAL is pre-sized to avoid expensive mid-batch growth (controlled by `opts.wal_pre_size_bytes`)
    ///
    /// **You must call [`end_batch()`](Self::end_batch) when done** to flush the WAL
    /// and restore normal operation.
    pub fn begin_batch(&mut self, opts: PutManyOpts) -> Result<()> {
        self.ensure_writable()?;
        if opts.wal_pre_size_bytes > 0 {
            self.ensure_wal_capacity(opts.wal_pre_size_bytes)?;
        }
        self.wal.set_skip_sync(opts.skip_sync);
        self.batch_opts = Some(opts);
        Ok(())
    }

    /// Pre-grow the embedded WAL to at least `min_bytes` in a single operation.
    ///
    /// When `disable_auto_checkpoint` is true, all WAL entries accumulate for
    /// the entire batch.  If the region is too small it must grow repeatedly,
    /// and every growth shifts **all** payload data — O(file_size) per shift.
    ///
    /// Calling this once before the batch eliminates all mid-batch shifts.
    /// On a freshly-created file the shift is essentially free (no data to move).
    fn ensure_wal_capacity(&mut self, min_bytes: u64) -> Result<()> {
        if min_bytes <= self.header.wal_size {
            return Ok(());
        }
        // Jump directly to the target size (next power of two for alignment)
        let target = min_bytes.next_power_of_two();
        let delta = target.saturating_sub(self.header.wal_size);
        if delta == 0 {
            return Ok(());
        }

        tracing::info!(
            current_wal = self.header.wal_size,
            target_wal = target,
            delta,
            "pre-sizing WAL for batch mode"
        );

        self.shift_data_for_wal_growth(delta)?;
        self.header.wal_size = target;
        self.apply_wal_growth_offsets(delta);

        let catalog_end = self.catalog_data_end();
        self.header.footer_offset = catalog_end
            .max(self.header.footer_offset)
            .max(self.data_end);

        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        self.wal = EmbeddedWal::open(&self.file, &self.header)?;
        Ok(())
    }

    /// Exit batch mode, flushing the WAL and restoring per-entry fsync.
    ///
    /// This performs a single `fsync` for all appends accumulated during the batch,
    /// then clears batch options so subsequent puts use default behaviour.
    pub fn end_batch(&mut self) -> Result<()> {
        self.ensure_writable()?;
        // Single fsync for the entire batch
        self.wal.flush()?;
        self.wal.set_skip_sync(false);
        self.batch_opts = None;
        Ok(())
    }

    /// Commit pending WAL records without rebuilding any indexes.
    ///
    /// Optimized for bulk ingestion: payloads and frame metadata are persisted,
    /// but time index, Tantivy, vec index, and other index structures are NOT
    /// rebuilt. The caller must do a full `commit()` (which triggers
    /// `rebuild_indexes()`) after all batches are written.
    ///
    /// Skips the staging lock for performance — not crash-safe.
    pub fn commit_skip_indexes(&mut self) -> Result<()> {
        self.ensure_writable()?;
        let records = self.wal.pending_records()?;
        if records.is_empty() && !self.dirty {
            return Ok(());
        }
        self.commit_skip_indexes_inner(records)
    }

    fn commit_skip_indexes_inner(&mut self, records: Vec<WalRecord>) -> Result<()> {
        self.generation = self.generation.wrapping_add(1);

        // Temporarily remove Tantivy engine to avoid per-frame indexing work
        // and disk reads in apply_records(). We won't persist Tantivy state anyway.
        #[cfg(feature = "lex")]
        let tantivy_backup = self.tantivy.take();

        // Preserve segment-backed vectors before compact payload placement can
        // reuse their byte ranges. finalize_indexes() will persist this cache.
        self.materialize_vec_segments_for_rebuild()?;
        let result = self.apply_records(records);

        // Restore Tantivy engine (unchanged — no dirty frames added)
        #[cfg(feature = "lex")]
        {
            self.tantivy = tantivy_backup;
            self.tantivy_dirty = false;
        }

        let _delta = result?;

        // Set footer_offset to right after payloads (no index data written)
        self.header.footer_offset = self.data_end;

        // Clear stale index manifests so next open() doesn't try to load
        // garbage data. Indexes will be rebuilt in the finalize step.
        self.toc.time_index = None;
        self.toc.indexes.lex_segments.clear();
        // Preserve vec manifest (holds dimension info) but zero out data pointers
        if let Some(vec) = self.toc.indexes.vec.as_mut() {
            vec.bytes_offset = 0;
            vec.bytes_length = 0;
            vec.vector_count = 0;
        }
        self.toc.indexes.lex = None;
        self.toc.indexes.clip = None;
        self.toc.segment_catalog.lex_segments.clear();
        self.toc.segment_catalog.vec_segments.clear();
        self.toc.segment_catalog.time_segments.clear();
        self.toc.segment_catalog.tantivy_segments.clear();
        #[cfg(feature = "temporal_track")]
        {
            self.toc.temporal_track = None;
            self.toc.segment_catalog.temporal_segments.clear();
        }
        self.toc.memories_track = None;
        self.toc.logic_mesh = None;
        self.toc.sketch_track = None;

        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        self.wal.record_checkpoint(&mut self.header)?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        self.pending_frame_inserts = 0;
        self.dirty = false;
        Ok(())
    }

    /// Rebuild all indexes (time, Tantivy, vec) and persist the TOC.
    ///
    /// Use after bulk ingestion with `commit_skip_indexes()` to build
    /// all search indexes in one O(n) pass. This is the complement to
    /// `commit_skip_indexes()` — call it once after all batches are done.
    pub fn finalize_indexes(&mut self) -> Result<()> {
        self.ensure_writable()?;
        self.with_staging_lock(Self::finalize_indexes_inner)
    }

    fn finalize_indexes_inner(&mut self) -> Result<()> {
        self.materialize_vec_segments_for_rebuild()?;
        // Finalization runs on an atomic staging copy, so rebuild the derived
        // layer at its compact boundary. Any following track must be published
        // again because a larger rebuilt vector block may overwrite its old
        // byte range even when the old file length was retained.
        self.rebuild_indexes(&[], &[], true)?;
        self.persist_sketch_track()?;
        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn commit_from_records(&mut self, records: Vec<WalRecord>, _mode: CommitMode) -> Result<()> {
        self.generation = self.generation.wrapping_add(1);

        // This must precede apply_records(): compact payload placement is
        // allowed to reuse the old derived-index ranges.
        self.materialize_vec_segments_for_rebuild()?;
        let delta = self.apply_records(records)?;
        let mut indexes_rebuilt = false;

        // Check if CLIP index has pending embeddings that need to be persisted
        let clip_needs_persist = self.clip_index.as_ref().is_some_and(|idx| !idx.is_empty());

        // Every dirty commit is already executed on an atomic staging copy.
        // Rebuild the complete derived layer there even for sketch-only (or
        // other metadata-only) changes so replacing a full snapshot never
        // appends another generation behind the old one.
        if !delta.is_empty() || clip_needs_persist || self.dirty {
            tracing::debug!(
                inserted_frames = delta.inserted_frames.len(),
                inserted_embeddings = delta.inserted_embeddings.len(),
                inserted_time_entries = delta.inserted_time_entries.len(),
                clip_needs_persist = clip_needs_persist,
                "commit applied delta"
            );
            // A normal commit runs against an atomic staging copy. Reclaim the
            // previous generation's derived-index tail in that copy before
            // publishing it; the currently committed file remains untouched
            // until AtomicWriteFile::commit performs the replacement.
            self.rebuild_indexes(&delta.inserted_embeddings, &delta.inserted_frames, true)?;
            indexes_rebuilt = true;
        }

        if !indexes_rebuilt && self.tantivy_index_pending() {
            self.flush_tantivy()?;
        }

        // Persist CLIP index if it has embeddings and wasn't already persisted by rebuild_indexes
        if !indexes_rebuilt && self.clip_enabled {
            if let Some(ref clip_index) = self.clip_index {
                if !clip_index.is_empty() {
                    self.persist_clip_index()?;
                }
            }
        }

        // Persist memories track if it has cards and wasn't already persisted by rebuild_indexes
        if !indexes_rebuilt && self.memories_track.card_count() > 0 {
            self.persist_memories_track()?;
        }

        // Persist logic mesh if it has nodes and wasn't already persisted by rebuild_indexes
        if !indexes_rebuilt && !self.logic_mesh.is_empty() {
            self.persist_logic_mesh()?;
        }

        // Always run this helper: its empty branch clears a stale manifest
        // after the compact rebuild removed the previous sketch bytes.
        self.persist_sketch_track()?;

        // flush_tantivy() and rebuild_indexes() have already set footer_offset correctly.
        // DO NOT overwrite it with catalog_data_end() as that would include orphaned segments.

        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        self.wal.record_checkpoint(&mut self.header)?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        #[cfg(feature = "parallel_segments")]
        if let Some(wal) = self.manifest_wal.as_mut() {
            wal.flush()?;
            wal.truncate()?;
        }
        self.pending_frame_inserts = 0;
        self.dirty = false;
        Ok(())
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn commit_parallel_with_opts(&mut self, opts: &BuildOpts) -> Result<()> {
        self.ensure_writable()?;
        if !self.dirty && !self.tantivy_index_pending() {
            return Ok(());
        }
        let opts = opts.clone();
        self.with_staging_lock(move |mem| mem.commit_parallel_inner(&opts))
    }

    #[cfg(feature = "parallel_segments")]
    fn commit_parallel_inner(&mut self, opts: &BuildOpts) -> Result<()> {
        if !self.dirty && !self.tantivy_index_pending() {
            return Ok(());
        }
        let records = self.wal.pending_records()?;
        let pending_vector_manifest = self
            .toc
            .indexes
            .vec
            .as_ref()
            .is_some_and(|manifest| manifest.bytes_offset == 0 && manifest.bytes_length > 0);
        let has_persisted_vectors = self
            .toc
            .indexes
            .vec
            .as_ref()
            .is_some_and(|manifest| manifest.bytes_offset > 0 && manifest.bytes_length > 0)
            || !self.toc.segment_catalog.vec_segments.is_empty();
        let has_cache_only_vectors = !has_persisted_vectors
            && self
                .vec_index
                .as_ref()
                .is_some_and(|index| index.vector_count() > 0);
        let compact_existing_derived_tail = self.header.footer_offset
            > self.frame_payload_append_offset()
            || has_cache_only_vectors
            || pending_vector_manifest;
        // Strictly load existing segments before compact payload placement can
        // reuse their ranges. When a derived tail already exists, or an earlier
        // skip-index commit left the cache as the only source of old vectors,
        // fall back to the complete staging rebuild below. Incremental segment
        // publication may invalidate the cache only when durable storage is a
        // complete source of truth.
        self.materialize_vec_segments_for_rebuild()?;
        let delta = self.apply_records(records)?;
        self.generation = self.generation.wrapping_add(1);
        let mut indexes_rebuilt = false;
        if compact_existing_derived_tail {
            self.rebuild_indexes(&delta.inserted_embeddings, &delta.inserted_frames, true)?;
            indexes_rebuilt = true;
        } else if !delta.is_empty() {
            tracing::info!(
                inserted_frames = delta.inserted_frames.len(),
                inserted_embeddings = delta.inserted_embeddings.len(),
                inserted_time_entries = delta.inserted_time_entries.len(),
                "parallel commit applied delta"
            );
            // Try to use parallel segment builder first
            let used_parallel = self.publish_parallel_delta(&delta, opts)?;
            tracing::info!(
                "parallel_commit: used_parallel={}, lex_enabled={}",
                used_parallel,
                self.lex_enabled
            );
            if used_parallel {
                // The newly published vector segments are now the complete
                // source of truth for this no-prior-tail branch. Invalidate
                // any pre-publication cache so the next search rebuilds it
                // from the catalog, including every just-published vector.
                if !delta.inserted_embeddings.is_empty() {
                    self.vec_index = None;
                }
                // Segments were written at data_end; update footer_offset so
                // rewrite_toc_footer places the TOC after the new segment data
                self.header.footer_offset = self.data_end;
                indexes_rebuilt = true;

                // OPTIMIZATION: Use incremental Tantivy indexing instead of full rebuild.
                // Only add new frames from the delta, not all frames.
                #[cfg(feature = "lex")]
                if self.lex_enabled {
                    tracing::info!(
                        "parallel_commit: incremental Tantivy update, new_frames={}, total_frames={}",
                        delta.inserted_frames.len(),
                        self.toc.frames.len()
                    );

                    // If Tantivy engine already exists, frames were already indexed during put()
                    // (at the add_frame call in put_bytes_with_options). Skip re-indexing to avoid
                    // duplicates. Only initialize and index if Tantivy wasn't present during put.
                    let tantivy_was_present = self.tantivy.is_some();
                    if self.tantivy.is_none() {
                        self.init_tantivy()?;
                    }

                    // Skip indexing if Tantivy was already present - frames were indexed during put
                    if tantivy_was_present {
                        tracing::info!(
                            "parallel_commit: skipping Tantivy indexing (already indexed during put)"
                        );
                    } else {
                        // First, collect all frames and their search text (to avoid borrow conflicts)
                        let max_payload = crate::memvid::search::max_index_payload();
                        let mut prepared_docs: Vec<(Frame, String)> = Vec::new();

                        for frame_id in &delta.inserted_frames {
                            // Look up the actual Frame from the TOC
                            let frame = match self.toc.frames.get(*frame_id as usize) {
                                Some(f) => f.clone(),
                                None => continue,
                            };

                            // Check if frame has explicit search_text first - clone it for ownership
                            let explicit_text = frame.search_text.clone();
                            if let Some(ref search_text) = explicit_text {
                                if !search_text.trim().is_empty() {
                                    prepared_docs.push((frame, search_text.clone()));
                                    continue;
                                }
                            }

                            // Get MIME type and check if text-indexable
                            let mime = frame
                                .metadata
                                .as_ref()
                                .and_then(|m| m.mime.as_deref())
                                .unwrap_or("application/octet-stream");

                            if !crate::memvid::search::is_text_indexable_mime(mime) {
                                continue;
                            }

                            if frame.payload_length > max_payload {
                                continue;
                            }

                            let text = self.frame_search_text(&frame)?;
                            if !text.trim().is_empty() {
                                prepared_docs.push((frame, text));
                            }
                        }

                        // Now add to Tantivy engine (no borrow conflict)
                        if let Some(ref mut engine) = self.tantivy {
                            for (frame, text) in &prepared_docs {
                                engine.add_frame(frame, text)?;
                            }

                            if !prepared_docs.is_empty() {
                                engine.commit()?;
                                self.tantivy_dirty = true;
                            }

                            tracing::info!(
                                "parallel_commit: Tantivy incremental update, added={}, total_docs={}",
                                prepared_docs.len(),
                                engine.num_docs()
                            );
                        } else {
                            tracing::warn!(
                                "parallel_commit: Tantivy engine is None after init_tantivy"
                            );
                        }
                    } // end of else !tantivy_was_present
                }

                // Time index stores all entries together for timeline queries.
                // Unlike Tantivy which is incremental, time index needs full rebuild.
                self.file.seek(SeekFrom::Start(self.data_end))?;
                let mut time_entries: Vec<TimeIndexEntry> = self
                    .toc
                    .frames
                    .iter()
                    .filter(|frame| {
                        frame.status == FrameStatus::Active && frame.role == FrameRole::Document
                    })
                    .map(|frame| TimeIndexEntry::new(frame.timestamp, frame.id))
                    .collect();
                let (ti_offset, ti_length, ti_checksum) =
                    time_index_append(&mut self.file, &mut time_entries)?;
                self.toc.time_index = Some(TimeIndexManifest {
                    bytes_offset: ti_offset,
                    bytes_length: ti_length,
                    entry_count: time_entries.len() as u64,
                    checksum: ti_checksum,
                });
                // Update data_end to account for the newly written time index
                self.data_end = ti_offset + ti_length;
                self.header.footer_offset = self.data_end;
                tracing::info!(
                    "parallel_commit: rebuilt time_index at offset={}, length={}, entries={}",
                    ti_offset,
                    ti_length,
                    time_entries.len()
                );
            } else {
                // Fall back to sequential rebuild if no segments were generated
                self.rebuild_indexes(&delta.inserted_embeddings, &delta.inserted_frames, true)?;
                indexes_rebuilt = true;
            }
        }

        // Flush Tantivy index if dirty (from parallel path or pending updates)
        #[cfg(feature = "lex")]
        if self.tantivy_dirty || (!indexes_rebuilt && self.tantivy_index_pending()) {
            self.flush_tantivy()?;
        }

        // Persist CLIP index if it has embeddings
        if self.clip_enabled {
            if let Some(ref clip_index) = self.clip_index {
                if !clip_index.is_empty() {
                    self.persist_clip_index()?;
                }
            }
        }

        // Persist memories track if it has cards
        if self.memories_track.card_count() > 0 {
            self.persist_memories_track()?;
        }

        // Persist logic mesh if it has nodes
        if !self.logic_mesh.is_empty() {
            self.persist_logic_mesh()?;
        }

        // Always run this helper: the compact fallback may have removed the
        // old sketch bytes, and the empty branch must clear their manifest.
        self.persist_sketch_track()?;

        // flush_tantivy() has already set footer_offset correctly
        // DO NOT overwrite with catalog_data_end()
        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        self.wal.record_checkpoint(&mut self.header)?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        if let Some(wal) = self.manifest_wal.as_mut() {
            wal.flush()?;
            wal.truncate()?;
        }
        self.pending_frame_inserts = 0;
        self.dirty = false;
        Ok(())
    }

    pub(crate) fn recover_wal(&mut self) -> Result<()> {
        let records = self.wal.records_after(self.header.wal_sequence)?;
        if records.is_empty() && !self.tantivy_index_pending() {
            return Ok(());
        }
        // Replay can overwrite derived ranges and truncate the old TOC before
        // an index rebuild succeeds. Protect both the committed snapshot and
        // its pending WAL with the same atomic publication protocol as commit.
        self.with_staging_lock(move |mem| mem.recover_wal_inner(records))
    }

    fn recover_wal_inner(&mut self, records: Vec<WalRecord>) -> Result<()> {
        if records.is_empty() {
            if self.tantivy_index_pending() {
                self.flush_tantivy()?;
            }
            return Ok(());
        }
        // Materialize source segments before compact payload placement can
        // overwrite them in staging, just as for a normal commit.
        self.materialize_vec_segments_for_rebuild()?;
        let delta = self.apply_records(records)?;
        if !delta.is_empty() {
            tracing::debug!(
                inserted_frames = delta.inserted_frames.len(),
                inserted_embeddings = delta.inserted_embeddings.len(),
                inserted_time_entries = delta.inserted_time_entries.len(),
                "recover applied delta"
            );
            self.rebuild_indexes(&delta.inserted_embeddings, &delta.inserted_frames, true)?;
            self.persist_sketch_track()?;
            self.rewrite_toc_footer()?;
            self.header.toc_checksum = self.toc.toc_checksum;
        } else if self.tantivy_index_pending() {
            self.flush_tantivy()?;
        }
        self.wal.record_checkpoint(&mut self.header)?;
        crate::persist_header(&mut self.file, &self.header)?;
        if !delta.is_empty() {
            // rebuild_indexes already flushed Tantivy, so nothing further to do.
        } else if self.tantivy_index_pending() {
            self.flush_tantivy()?;
            crate::persist_header(&mut self.file, &self.header)?;
        }
        self.file.sync_all()?;
        self.pending_frame_inserts = 0;
        self.dirty = false;
        Ok(())
    }

    fn frame_payload_append_offset(&self) -> u64 {
        let mut offset = self.payload_region_end();

        // Replay segments are not derived from frame payloads and are not
        // rewritten by a normal frame commit. The manifest is part of the
        // format even when the replay API feature is disabled, so treat its
        // bytes as an opaque preserved range in every build configuration.
        if let Some(manifest) = self.toc.replay_manifest.as_ref() {
            if let Some(end) = manifest.segment_offset.checked_add(manifest.segment_size) {
                offset = offset.max(end);
            }
        }
        offset
    }

    fn apply_records(&mut self, records: Vec<WalRecord>) -> Result<IngestionDelta> {
        let mut delta = IngestionDelta::default();
        if records.is_empty() {
            return Ok(delta);
        }

        // Frame payloads should be appended at the compact payload boundary.
        //
        // `data_end` may point past derived embedded indexes from a previous
        // commit after reopening a file. Writing new payloads there leaves the
        // old index/TOC layer stranded in the file and creates storage
        // amplification for the common `open -> append -> commit -> close`
        // lifecycle. Derived indexes are rebuilt later in this commit, so they
        // are safe to overwrite.
        let mut data_cursor = self.frame_payload_append_offset();
        let mut sequence_to_frame: HashMap<u64, FrameId> = HashMap::new();

        if !records.is_empty() {
            self.file.seek(SeekFrom::Start(data_cursor))?;
            for record in records {
                let mut entry = match decode_wal_entry(&record.payload)? {
                    WalEntry::Frame(entry) => entry,
                    #[cfg(feature = "lex")]
                    WalEntry::Lex(batch) => {
                        self.apply_lex_wal(batch)?;
                        continue;
                    }
                };

                match entry.op {
                    FrameWalOp::Insert => {
                        let frame_id = self.toc.frames.len() as u64;

                        let (
                            payload_offset,
                            payload_length,
                            checksum_bytes,
                            canonical_length_value,
                        ) = if let Some(source_id) = entry.reuse_payload_from {
                            if !entry.payload.is_empty() {
                                return Err(MemvidError::InvalidFrame {
                                    frame_id: source_id,
                                    reason: "reused payload entry contained inline bytes",
                                });
                            }
                            let source_idx = usize::try_from(source_id).map_err(|_| {
                                MemvidError::InvalidFrame {
                                    frame_id: source_id,
                                    reason: "frame id too large for memory",
                                }
                            })?;
                            let source = self.toc.frames.get(source_idx).cloned().ok_or(
                                MemvidError::InvalidFrame {
                                    frame_id: source_id,
                                    reason: "reused payload source missing",
                                },
                            )?;
                            (
                                source.payload_offset,
                                source.payload_length,
                                source.checksum,
                                entry
                                    .canonical_length
                                    .or(source.canonical_length)
                                    .unwrap_or(source.payload_length),
                            )
                        } else {
                            self.file.seek(SeekFrom::Start(data_cursor))?;
                            self.file.write_all(&entry.payload)?;
                            let checksum = hash(&entry.payload);
                            let payload_length = entry.payload.len() as u64;
                            let canonical_length =
                                if entry.canonical_encoding == CanonicalEncoding::Zstd {
                                    if let Some(len) = entry.canonical_length {
                                        len
                                    } else {
                                        let decoded = crate::decode_canonical_bytes(
                                            &entry.payload,
                                            CanonicalEncoding::Zstd,
                                            frame_id,
                                        )?;
                                        decoded.len() as u64
                                    }
                                } else {
                                    entry.canonical_length.unwrap_or(entry.payload.len() as u64)
                                };
                            let payload_offset = data_cursor;
                            data_cursor += payload_length;
                            // Keep cached_payload_end in sync (monotonically increasing)
                            self.cached_payload_end = self.cached_payload_end.max(data_cursor);
                            (
                                payload_offset,
                                payload_length,
                                *checksum.as_bytes(),
                                canonical_length,
                            )
                        };

                        let uri = entry
                            .uri
                            .clone()
                            .unwrap_or_else(|| crate::default_uri(frame_id));
                        let title = entry
                            .title
                            .clone()
                            .or_else(|| crate::infer_title_from_uri(&uri));

                        #[cfg(feature = "temporal_track")]
                        let (anchor_ts, anchor_source) =
                            self.determine_temporal_anchor(entry.timestamp);

                        let mut frame = Frame {
                            id: frame_id,
                            timestamp: entry.timestamp,
                            anchor_ts: {
                                #[cfg(feature = "temporal_track")]
                                {
                                    Some(anchor_ts)
                                }
                                #[cfg(not(feature = "temporal_track"))]
                                {
                                    None
                                }
                            },
                            anchor_source: {
                                #[cfg(feature = "temporal_track")]
                                {
                                    Some(anchor_source)
                                }
                                #[cfg(not(feature = "temporal_track"))]
                                {
                                    None
                                }
                            },
                            kind: entry.kind.clone(),
                            track: entry.track.clone(),
                            payload_offset,
                            payload_length,
                            checksum: checksum_bytes,
                            uri: Some(uri),
                            title,
                            canonical_encoding: entry.canonical_encoding,
                            canonical_length: Some(canonical_length_value),
                            metadata: entry.metadata.clone(),
                            search_text: entry.search_text.clone(),
                            tags: entry.tags.clone(),
                            labels: entry.labels.clone(),
                            extra_metadata: entry.extra_metadata.clone(),
                            content_dates: entry.content_dates.clone(),
                            chunk_manifest: entry.chunk_manifest.clone(),
                            role: entry.role,
                            parent_id: None,
                            chunk_index: entry.chunk_index,
                            chunk_count: entry.chunk_count,
                            status: FrameStatus::Active,
                            supersedes: entry.supersedes_frame_id,
                            superseded_by: None,
                            source_sha256: entry.source_sha256,
                            source_path: entry.source_path.clone(),
                            enrichment_state: entry.enrichment_state,
                        };

                        if let Some(parent_seq) = entry.parent_sequence {
                            if let Some(parent_frame_id) = sequence_to_frame.get(&parent_seq) {
                                frame.parent_id = Some(*parent_frame_id);
                            } else {
                                // Parent sequence not found in current batch - this can happen
                                // if parent was committed in a previous batch. Try to find parent
                                // by looking at recently inserted frames with matching characteristics.
                                // The parent should be the most recent Document frame that has
                                // a chunk_manifest matching this chunk's expected parent.
                                if entry.role == FrameRole::DocumentChunk {
                                    // Look backwards through recently inserted frames
                                    for &candidate_id in delta.inserted_frames.iter().rev() {
                                        if let Ok(idx) = usize::try_from(candidate_id) {
                                            if let Some(candidate) = self.toc.frames.get(idx) {
                                                if candidate.role == FrameRole::Document
                                                    && candidate.chunk_manifest.is_some()
                                                {
                                                    // Found a parent document - use it
                                                    frame.parent_id = Some(candidate_id);
                                                    tracing::debug!(
                                                        chunk_frame_id = frame_id,
                                                        parent_frame_id = candidate_id,
                                                        parent_seq = parent_seq,
                                                        "resolved chunk parent via fallback"
                                                    );
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                if frame.parent_id.is_none() {
                                    tracing::warn!(
                                        chunk_frame_id = frame_id,
                                        parent_seq = parent_seq,
                                        "chunk has parent_sequence but parent not found in batch"
                                    );
                                }
                            }
                        }

                        #[cfg(feature = "lex")]
                        let index_text = if self.tantivy.is_some() {
                            if let Some(text) = entry.search_text.clone() {
                                if text.trim().is_empty() {
                                    None
                                } else {
                                    Some(text)
                                }
                            } else {
                                Some(self.frame_content(&frame)?)
                            }
                        } else {
                            None
                        };
                        #[cfg(feature = "lex")]
                        if let (Some(engine), Some(text)) =
                            (self.tantivy.as_mut(), index_text.as_ref())
                        {
                            engine.add_frame(&frame, text)?;
                            self.tantivy_dirty = true;

                            // Generate sketch for fast candidate pre-filtering
                            // Uses the same text as tantivy indexing for consistency
                            if !text.trim().is_empty() {
                                let entry = crate::types::generate_sketch(
                                    frame_id,
                                    text,
                                    crate::types::SketchVariant::Small,
                                    None,
                                );
                                self.sketch_track.insert(entry);
                            }
                        }

                        if let Some(embedding) = entry.embedding.take() {
                            delta
                                .inserted_embeddings
                                .push((frame_id, embedding.clone()));
                        }

                        if entry.role == FrameRole::Document {
                            delta
                                .inserted_time_entries
                                .push(TimeIndexEntry::new(entry.timestamp, frame_id));
                            #[cfg(feature = "temporal_track")]
                            {
                                delta.inserted_temporal_anchors.push(TemporalAnchor::new(
                                    frame_id,
                                    anchor_ts,
                                    anchor_source,
                                ));
                                delta.inserted_temporal_mentions.extend(
                                    Self::collect_temporal_mentions(
                                        entry.search_text.as_deref(),
                                        frame_id,
                                        anchor_ts,
                                    ),
                                );
                            }
                        }

                        if let Some(predecessor) = frame.supersedes {
                            self.mark_frame_superseded(predecessor, frame_id)?;
                        }

                        self.toc.frames.push(frame);
                        delta.inserted_frames.push(frame_id);
                        sequence_to_frame.insert(record.sequence, frame_id);
                    }
                    FrameWalOp::Tombstone => {
                        let target = entry.target_frame_id.ok_or(MemvidError::InvalidFrame {
                            frame_id: 0,
                            reason: "tombstone missing frame reference",
                        })?;
                        self.mark_frame_deleted(target)?;
                        delta.mutated_frames = true;
                    }
                }
            }
            self.data_end = data_cursor;
        }

        // Second pass: resolve any orphan DocumentChunk frames that are missing parent_id.
        // This handles edge cases where chunks couldn't be linked during the first pass.
        // First, collect orphan chunks and their resolved parents to avoid borrow conflicts.
        let orphan_resolutions: Vec<(u64, u64)> = delta
            .inserted_frames
            .iter()
            .filter_map(|&frame_id| {
                let idx = usize::try_from(frame_id).ok()?;
                let frame = self.toc.frames.get(idx)?;
                if frame.role != FrameRole::DocumentChunk || frame.parent_id.is_some() {
                    return None;
                }
                // Find the most recent Document frame before this chunk that has a manifest
                for candidate_id in (0..frame_id).rev() {
                    if let Ok(idx) = usize::try_from(candidate_id) {
                        if let Some(candidate) = self.toc.frames.get(idx) {
                            if candidate.role == FrameRole::Document
                                && candidate.chunk_manifest.is_some()
                                && candidate.status == FrameStatus::Active
                            {
                                return Some((frame_id, candidate_id));
                            }
                        }
                    }
                }
                None
            })
            .collect();

        // Now apply the resolutions
        for (chunk_id, parent_id) in orphan_resolutions {
            if let Ok(idx) = usize::try_from(chunk_id) {
                if let Some(frame) = self.toc.frames.get_mut(idx) {
                    frame.parent_id = Some(parent_id);
                    tracing::debug!(
                        chunk_frame_id = chunk_id,
                        parent_frame_id = parent_id,
                        "resolved orphan chunk parent in second pass"
                    );
                }
            }
        }

        // Index rebuild now happens once per commit (Option A) instead of incremental append.
        // See commit_from_records() for where rebuild_indexes() is invoked.
        Ok(delta)
    }

    #[cfg(feature = "temporal_track")]
    fn determine_temporal_anchor(&self, timestamp: i64) -> (i64, AnchorSource) {
        (timestamp, AnchorSource::FrameTimestamp)
    }

    #[cfg(feature = "temporal_track")]
    fn collect_temporal_mentions(
        text: Option<&str>,
        frame_id: FrameId,
        anchor_ts: i64,
    ) -> Vec<TemporalMention> {
        let text = match text {
            Some(value) if !value.trim().is_empty() => value,
            _ => return Vec::new(),
        };

        let anchor = match OffsetDateTime::from_unix_timestamp(anchor_ts) {
            Ok(ts) => ts,
            Err(_) => return Vec::new(),
        };

        let context = TemporalContext::new(anchor, DEFAULT_TEMPORAL_TZ.to_string());
        let normalizer = TemporalNormalizer::new(context);
        let mut spans: Vec<(usize, usize)> = Vec::new();
        let lower = text.to_ascii_lowercase();

        for phrase in STATIC_TEMPORAL_PHRASES {
            let mut search_start = 0usize;
            while let Some(idx) = lower[search_start..].find(phrase) {
                let abs = search_start + idx;
                let end = abs + phrase.len();
                spans.push((abs, end));
                search_start = end;
            }
        }

        static NUMERIC_DATE: OnceCell<std::result::Result<Regex, String>> = OnceCell::new();
        let regex = NUMERIC_DATE.get_or_init(|| {
            Regex::new(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b").map_err(|err| err.to_string())
        });
        let regex = match regex {
            Ok(re) => re,
            Err(msg) => {
                tracing::error!(target = "memvid::temporal", error = %msg, "numeric date regex init failed");
                return Vec::new();
            }
        };
        for mat in regex.find_iter(text) {
            spans.push((mat.start(), mat.end()));
        }

        spans.sort_unstable();
        spans.dedup();

        let mut mentions: Vec<TemporalMention> = Vec::new();
        for (start, end) in spans {
            if end > text.len() || start >= end {
                continue;
            }
            let raw = &text[start..end];
            let trimmed = raw.trim_matches(|c: char| matches!(c, '"' | '\'' | '.' | ',' | ';'));
            if trimmed.is_empty() {
                continue;
            }
            let offset = raw.find(trimmed).map(|idx| start + idx).unwrap_or(start);
            let finish = offset + trimmed.len();
            match normalizer.resolve(trimmed) {
                Ok(resolution) => {
                    mentions.extend(Self::resolution_to_mentions(
                        resolution, frame_id, offset, finish,
                    ));
                }
                Err(_) => continue,
            }
        }

        mentions
    }

    #[cfg(feature = "temporal_track")]
    fn resolution_to_mentions(
        resolution: TemporalResolution,
        frame_id: FrameId,
        byte_start: usize,
        byte_end: usize,
    ) -> Vec<TemporalMention> {
        let byte_len = byte_end.saturating_sub(byte_start) as u32;
        let byte_start = byte_start.min(u32::MAX as usize) as u32;
        let mut results = Vec::new();

        let base_flags = Self::flags_from_resolution(&resolution.flags);
        match resolution.value {
            TemporalResolutionValue::Date(date) => {
                let ts = Self::date_to_timestamp(date);
                results.push(TemporalMention::new(
                    ts,
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::Date,
                    resolution.confidence,
                    0,
                    base_flags,
                ));
            }
            TemporalResolutionValue::DateTime(dt) => {
                let ts = dt.unix_timestamp();
                let tz_hint = dt.offset().whole_minutes() as i16;
                results.push(TemporalMention::new(
                    ts,
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::DateTime,
                    resolution.confidence,
                    tz_hint,
                    base_flags,
                ));
            }
            TemporalResolutionValue::DateRange { start, end } => {
                let flags = base_flags.set(TemporalMentionFlags::HAS_RANGE, true);
                let start_ts = Self::date_to_timestamp(start);
                results.push(TemporalMention::new(
                    start_ts,
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::RangeStart,
                    resolution.confidence,
                    0,
                    flags,
                ));
                let end_ts = Self::date_to_timestamp(end);
                results.push(TemporalMention::new(
                    end_ts,
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::RangeEnd,
                    resolution.confidence,
                    0,
                    flags,
                ));
            }
            TemporalResolutionValue::DateTimeRange { start, end } => {
                let flags = base_flags.set(TemporalMentionFlags::HAS_RANGE, true);
                results.push(TemporalMention::new(
                    start.unix_timestamp(),
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::RangeStart,
                    resolution.confidence,
                    start.offset().whole_minutes() as i16,
                    flags,
                ));
                results.push(TemporalMention::new(
                    end.unix_timestamp(),
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::RangeEnd,
                    resolution.confidence,
                    end.offset().whole_minutes() as i16,
                    flags,
                ));
            }
            TemporalResolutionValue::Month { year, month } => {
                let start_date = match Date::from_calendar_date(year, month, 1) {
                    Ok(date) => date,
                    Err(err) => {
                        tracing::warn!(
                            target = "memvid::temporal",
                            %err,
                            year,
                            month = month as u8,
                            "skipping invalid month resolution"
                        );
                        // Skip invalid range for this mention only.
                        return results;
                    }
                };
                let end_date = match Self::last_day_in_month(year, month) {
                    Some(date) => date,
                    None => {
                        tracing::warn!(
                            target = "memvid::temporal",
                            year,
                            month = month as u8,
                            "skipping month resolution with invalid calendar range"
                        );
                        return results;
                    }
                };
                let flags = base_flags.set(TemporalMentionFlags::HAS_RANGE, true);
                results.push(TemporalMention::new(
                    Self::date_to_timestamp(start_date),
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::RangeStart,
                    resolution.confidence,
                    0,
                    flags,
                ));
                results.push(TemporalMention::new(
                    Self::date_to_timestamp(end_date),
                    frame_id,
                    byte_start,
                    byte_len,
                    TemporalMentionKind::RangeEnd,
                    resolution.confidence,
                    0,
                    flags,
                ));
            }
        }

        results
    }

    #[cfg(feature = "temporal_track")]
    fn flags_from_resolution(flags: &[TemporalResolutionFlag]) -> TemporalMentionFlags {
        let mut result = TemporalMentionFlags::empty();
        if flags
            .iter()
            .any(|flag| matches!(flag, TemporalResolutionFlag::Ambiguous))
        {
            result = result.set(TemporalMentionFlags::AMBIGUOUS, true);
        }
        if flags
            .iter()
            .any(|flag| matches!(flag, TemporalResolutionFlag::Relative))
        {
            result = result.set(TemporalMentionFlags::DERIVED, true);
        }
        result
    }

    #[cfg(feature = "temporal_track")]
    fn date_to_timestamp(date: Date) -> i64 {
        PrimitiveDateTime::new(date, Time::MIDNIGHT)
            .assume_offset(UtcOffset::UTC)
            .unix_timestamp()
    }

    #[cfg(feature = "temporal_track")]
    fn last_day_in_month(year: i32, month: Month) -> Option<Date> {
        let mut date = Date::from_calendar_date(year, month, 1).ok()?;
        while let Some(next) = date.next_day() {
            if next.month() == month {
                date = next;
            } else {
                break;
            }
        }
        Some(date)
    }

    #[allow(dead_code)]
    fn publish_lex_delta(&mut self, delta: &IngestionDelta) -> Result<bool> {
        if delta.inserted_frames.is_empty() || !self.lex_enabled {
            return Ok(false);
        }

        let artifact = match self.build_lex_segment_from_frames(&delta.inserted_frames)? {
            Some(artifact) => artifact,
            None => return Ok(false),
        };

        let segment_id = self.toc.segment_catalog.next_segment_id;
        #[cfg(feature = "parallel_segments")]
        let span =
            self.segment_span_from_iter(delta.inserted_frames.iter().map(|frame_id| *frame_id));

        #[cfg_attr(not(feature = "parallel_segments"), allow(unused_mut))]
        let mut descriptor = self.append_lex_segment(&artifact, segment_id)?;
        #[cfg(feature = "parallel_segments")]
        if let Some(span) = span {
            Self::decorate_segment_common(&mut descriptor.common, span);
        }
        #[cfg(feature = "parallel_segments")]
        let descriptor_for_manifest = descriptor.clone();
        self.toc.segment_catalog.lex_segments.push(descriptor);
        #[cfg(feature = "parallel_segments")]
        if let Err(err) = self.record_index_segment(
            SegmentKind::Lexical,
            descriptor_for_manifest.common,
            SegmentStats {
                doc_count: artifact.doc_count,
                vector_count: 0,
                time_entries: 0,
                bytes_uncompressed: artifact.bytes.len() as u64,
                build_micros: 0,
            },
        ) {
            tracing::warn!(error = %err, "manifest WAL append failed for lex segment");
        }
        self.toc.segment_catalog.version = self.toc.segment_catalog.version.max(1);
        self.toc.segment_catalog.next_segment_id = segment_id.saturating_add(1);
        Ok(true)
    }

    #[allow(dead_code)]
    fn publish_vec_delta(&mut self, delta: &IngestionDelta) -> Result<bool> {
        if delta.inserted_embeddings.is_empty() || !self.vec_enabled {
            return Ok(false);
        }

        let artifact = match self.build_vec_segment_from_embeddings(&delta.inserted_embeddings)? {
            Some(artifact) => artifact,
            None => return Ok(false),
        };

        if let Some(existing_dim) = self.effective_vec_index_dimension()? {
            if existing_dim != artifact.dimension {
                return Err(MemvidError::VecDimensionMismatch {
                    expected: existing_dim,
                    actual: artifact.dimension as usize,
                });
            }
        }

        let segment_id = self.toc.segment_catalog.next_segment_id;
        #[cfg(feature = "parallel_segments")]
        #[cfg(feature = "parallel_segments")]
        let span = self.segment_span_from_iter(delta.inserted_embeddings.iter().map(|(id, _)| *id));

        #[cfg_attr(not(feature = "parallel_segments"), allow(unused_mut))]
        let mut descriptor = self.append_vec_segment(&artifact, segment_id)?;
        #[cfg(feature = "parallel_segments")]
        if let Some(span) = span {
            Self::decorate_segment_common(&mut descriptor.common, span);
        }
        #[cfg(feature = "parallel_segments")]
        let descriptor_for_manifest = descriptor.clone();
        self.toc.segment_catalog.vec_segments.push(descriptor);
        #[cfg(feature = "parallel_segments")]
        if let Err(err) = self.record_index_segment(
            SegmentKind::Vector,
            descriptor_for_manifest.common,
            SegmentStats {
                doc_count: 0,
                vector_count: artifact.vector_count,
                time_entries: 0,
                bytes_uncompressed: artifact.bytes_uncompressed,
                build_micros: 0,
            },
        ) {
            tracing::warn!(error = %err, "manifest WAL append failed for vec segment");
        }
        self.toc.segment_catalog.version = self.toc.segment_catalog.version.max(1);
        self.toc.segment_catalog.next_segment_id = segment_id.saturating_add(1);

        // Keep the global vec manifest in sync for auto-detection and stats.
        if self.toc.indexes.vec.is_none() {
            let empty_offset = self.data_end;
            let empty_checksum = *b"\xe3\xb0\xc4\x42\x98\xfc\x1c\x14\x9a\xfb\xf4\xc8\x99\x6f\xb9\x24\
                                    \x27\xae\x41\xe4\x64\x9b\x93\x4c\xa4\x95\x99\x1b\x78\x52\xb8\x55";
            self.toc.indexes.vec = Some(VecIndexManifest {
                vector_count: 0,
                dimension: 0,
                bytes_offset: empty_offset,
                bytes_length: 0,
                checksum: empty_checksum,
                compression_mode: self.vec_compression.clone(),
                model: self.vec_model.clone(),
            });
        }
        if let Some(manifest) = self.toc.indexes.vec.as_mut() {
            if manifest.dimension == 0 {
                manifest.dimension = artifact.dimension;
            }
            if manifest.bytes_length == 0 {
                manifest.vector_count = manifest.vector_count.saturating_add(artifact.vector_count);
                manifest.compression_mode = artifact.compression.clone();
            }
        }

        self.vec_enabled = true;
        Ok(true)
    }

    #[allow(dead_code)]
    fn publish_time_delta(&mut self, delta: &IngestionDelta) -> Result<bool> {
        if delta.inserted_time_entries.is_empty() {
            return Ok(false);
        }

        let artifact = match self.build_time_segment_from_entries(&delta.inserted_time_entries)? {
            Some(artifact) => artifact,
            None => return Ok(false),
        };

        let segment_id = self.toc.segment_catalog.next_segment_id;
        #[cfg(feature = "parallel_segments")]
        #[cfg(feature = "parallel_segments")]
        let span = self.segment_span_from_iter(
            delta
                .inserted_time_entries
                .iter()
                .map(|entry| entry.frame_id),
        );

        #[cfg_attr(not(feature = "parallel_segments"), allow(unused_mut))]
        let mut descriptor = self.append_time_segment(&artifact, segment_id)?;
        #[cfg(feature = "parallel_segments")]
        if let Some(span) = span {
            Self::decorate_segment_common(&mut descriptor.common, span);
        }
        #[cfg(feature = "parallel_segments")]
        let descriptor_for_manifest = descriptor.clone();
        self.toc.segment_catalog.time_segments.push(descriptor);
        #[cfg(feature = "parallel_segments")]
        if let Err(err) = self.record_index_segment(
            SegmentKind::Time,
            descriptor_for_manifest.common,
            SegmentStats {
                doc_count: 0,
                vector_count: 0,
                time_entries: artifact.entry_count,
                bytes_uncompressed: artifact.bytes.len() as u64,
                build_micros: 0,
            },
        ) {
            tracing::warn!(error = %err, "manifest WAL append failed for time segment");
        }
        self.toc.segment_catalog.version = self.toc.segment_catalog.version.max(1);
        self.toc.segment_catalog.next_segment_id = segment_id.saturating_add(1);
        Ok(true)
    }

    #[cfg(feature = "temporal_track")]
    #[allow(dead_code)]
    fn publish_temporal_delta(&mut self, delta: &IngestionDelta) -> Result<bool> {
        if delta.inserted_temporal_mentions.is_empty() && delta.inserted_temporal_anchors.is_empty()
        {
            return Ok(false);
        }

        debug_assert!(
            delta.inserted_temporal_mentions.len() < 1_000_000,
            "temporal delta mentions unexpectedly large: {}",
            delta.inserted_temporal_mentions.len()
        );
        debug_assert!(
            delta.inserted_temporal_anchors.len() < 1_000_000,
            "temporal delta anchors unexpectedly large: {}",
            delta.inserted_temporal_anchors.len()
        );

        let artifact = match self.build_temporal_segment_from_records(
            &delta.inserted_temporal_mentions,
            &delta.inserted_temporal_anchors,
        )? {
            Some(artifact) => artifact,
            None => return Ok(false),
        };

        let segment_id = self.toc.segment_catalog.next_segment_id;
        let descriptor = self.append_temporal_segment(&artifact, segment_id)?;
        self.toc
            .segment_catalog
            .temporal_segments
            .push(descriptor.clone());
        self.toc.segment_catalog.version = self.toc.segment_catalog.version.max(1);
        self.toc.segment_catalog.next_segment_id = segment_id.saturating_add(1);

        self.toc.temporal_track = Some(TemporalTrackManifest {
            bytes_offset: descriptor.common.bytes_offset,
            bytes_length: descriptor.common.bytes_length,
            entry_count: artifact.entry_count,
            anchor_count: artifact.anchor_count,
            checksum: artifact.checksum,
            flags: artifact.flags,
        });

        self.clear_temporal_track_cache();

        Ok(true)
    }

    fn mark_frame_superseded(&mut self, frame_id: FrameId, successor_id: FrameId) -> Result<()> {
        let index = usize::try_from(frame_id).map_err(|_| MemvidError::InvalidFrame {
            frame_id,
            reason: "frame id too large",
        })?;
        let frame = self
            .toc
            .frames
            .get_mut(index)
            .ok_or(MemvidError::InvalidFrame {
                frame_id,
                reason: "supersede target missing",
            })?;
        frame.status = FrameStatus::Superseded;
        frame.superseded_by = Some(successor_id);
        self.remove_frame_from_indexes(frame_id)
    }

    pub(crate) fn rebuild_indexes(
        &mut self,
        new_vec_docs: &[(FrameId, Vec<f32>)],
        inserted_frame_ids: &[FrameId],
        compact_derived_tail: bool,
    ) -> Result<()> {
        if self.toc.frames.is_empty() && !self.lex_enabled && !self.vec_enabled {
            return Ok(());
        }

        let payload_end = self.payload_region_end();
        // Replay is user-visible history, not a derived index. It may live
        // beyond the payload region and must remain intact while indexes are
        // replaced. New derived indexes are placed after it.
        let mut index_start = payload_end;
        if let Some(manifest) = self.toc.replay_manifest.as_ref() {
            if let Some(end) = manifest.segment_offset.checked_add(manifest.segment_size) {
                index_start = index_start.max(end);
            }
        }
        self.data_end = index_start;

        // Full index snapshots are replaceable derived data. Normal commits
        // run on an atomic staging copy, so that copy can discard the previous
        // derived tail and write the new generation at the compact boundary.
        // Callers that do not request compaction retain the old derived range;
        // the old TOC itself is replaced below. Commit and WAL replay must run
        // this operation on staging, never on their committed destination.
        let safe_truncate_len = if compact_derived_tail {
            self.header.footer_offset = index_start;
            index_start
        } else {
            self.header.footer_offset.max(index_start)
        };
        if self.file.metadata()?.len() > safe_truncate_len {
            self.file.set_len(safe_truncate_len)?;
        }
        #[cfg(test)]
        if EXIT_AFTER_REBUILD_TRUNCATE.with(|flag| flag.replace(false)) {
            // Deliberately bypass all Rust destructors, including Memvid::drop.
            std::process::exit(86);
        }
        #[cfg(test)]
        if FAIL_AFTER_REBUILD_TRUNCATE.with(|flag| flag.replace(false)) {
            return Err(std::io::Error::other("injected failure after rebuild truncate").into());
        }
        #[cfg(test)]
        if compact_derived_tail {
            fail_after_compact_truncate()?;
        }
        self.file.seek(SeekFrom::Start(index_start))?;

        // Clear legacy per-segment catalogs; full rebuild emits fresh manifests.
        self.toc.segment_catalog.lex_segments.clear();
        self.toc.segment_catalog.vec_segments.clear();
        self.toc.segment_catalog.time_segments.clear();
        #[cfg(feature = "temporal_track")]
        self.toc.segment_catalog.temporal_segments.clear();
        #[cfg(feature = "parallel_segments")]
        self.toc.segment_catalog.index_segments.clear();
        // Drop any stale Tantivy manifests so offsets are rebuilt fresh.
        self.toc.segment_catalog.tantivy_segments.clear();
        // Drop any stale embedded lex manifest entries before rebuilding Tantivy.
        self.toc.indexes.lex_segments.clear();

        let mut time_entries: Vec<TimeIndexEntry> = self
            .toc
            .frames
            .iter()
            .filter(|frame| {
                frame.status == FrameStatus::Active && frame.role == FrameRole::Document
            })
            .map(|frame| TimeIndexEntry::new(frame.timestamp, frame.id))
            .collect();
        let (ti_offset, ti_length, ti_checksum) =
            time_index_append(&mut self.file, &mut time_entries)?;
        self.toc.time_index = Some(TimeIndexManifest {
            bytes_offset: ti_offset,
            bytes_length: ti_length,
            entry_count: time_entries.len() as u64,
            checksum: ti_checksum,
        });

        let mut footer_offset = ti_offset + ti_length;

        #[cfg(feature = "temporal_track")]
        {
            self.toc.temporal_track = None;
            self.toc.segment_catalog.temporal_segments.clear();
            self.clear_temporal_track_cache();
        }

        if self.lex_enabled {
            #[cfg(feature = "lex")]
            {
                if self.tantivy_dirty {
                    // instant_index was used: frames were added to Tantivy with WAL
                    // sequence numbers as IDs, which don't match the actual frame IDs
                    // assigned during apply_records(). Must do a full rebuild to fix IDs.
                    if let Ok(mut storage) = self.lex_storage.write() {
                        storage.clear();
                        storage.set_generation(0);
                    }
                    self.rebuild_tantivy_index()?;
                } else if self.tantivy.is_some() && !inserted_frame_ids.is_empty() {
                    // Incremental path: engine exists (from open() or previous commit),
                    // instant_index was NOT used so no wrong IDs. Just add new frames.
                    // This is O(batch_size) — the key optimization for bulk ingestion.
                    //
                    // Collect frames + text first to avoid borrow conflicts with engine.
                    let max_payload = crate::memvid::search::max_index_payload();
                    let mut prepared_docs: Vec<(Frame, String)> = Vec::new();
                    for &frame_id in inserted_frame_ids {
                        let Ok(idx) = usize::try_from(frame_id) else {
                            continue;
                        };
                        let frame = match self.toc.frames.get(idx) {
                            Some(f) => f.clone(),
                            None => continue,
                        };
                        if frame.status != FrameStatus::Active {
                            continue;
                        }
                        if let Some(search_text) = frame.search_text.clone() {
                            if !search_text.trim().is_empty() {
                                prepared_docs.push((frame, search_text));
                                continue;
                            }
                        }
                        let mime = frame
                            .metadata
                            .as_ref()
                            .and_then(|m| m.mime.as_deref())
                            .unwrap_or("application/octet-stream");
                        if !crate::memvid::search::is_text_indexable_mime(mime) {
                            continue;
                        }
                        if frame.payload_length > max_payload {
                            continue;
                        }
                        let text = self.frame_search_text(&frame)?;
                        if !text.trim().is_empty() {
                            prepared_docs.push((frame, text));
                        }
                    }
                    if let Some(engine) = self.tantivy.as_mut() {
                        for (frame, text) in &prepared_docs {
                            engine.add_frame(frame, text)?;
                        }
                        engine.commit()?;
                    }
                } else {
                    // Full rebuild path: no engine or no new frames (e.g., doctor repair).
                    // Clear embedded storage to avoid carrying stale segments between rebuilds.
                    if let Ok(mut storage) = self.lex_storage.write() {
                        storage.clear();
                        storage.set_generation(0);
                    }
                    self.rebuild_tantivy_index()?;
                }

                // Set lex_enabled to ensure it persists
                self.lex_enabled = true;

                // Mark Tantivy as dirty so it gets flushed
                self.tantivy_dirty = true;

                // Position embedded Tantivy segments immediately after the time index.
                self.data_end = footer_offset;

                // Flush Tantivy segments to file
                self.flush_tantivy()?;

                // Update footer_offset after Tantivy flush
                footer_offset = self.header.footer_offset;

                // Restore data_end to payload boundary so future payload writes stay before indexes.
                self.data_end = payload_end;
            }
            #[cfg(not(feature = "lex"))]
            {
                self.toc.indexes.lex = None;
                self.toc.indexes.lex_segments.clear();
            }
        } else {
            // Lex disabled: clear everything
            self.toc.indexes.lex = None;
            self.toc.indexes.lex_segments.clear();
            #[cfg(feature = "lex")]
            if let Ok(mut storage) = self.lex_storage.write() {
                storage.clear();
            }
        }

        if let Some((artifact, index)) = self.build_vec_artifact(new_vec_docs)? {
            let vec_offset = footer_offset;
            self.file.seek(SeekFrom::Start(vec_offset))?;
            self.file.write_all(&artifact.bytes)?;
            footer_offset += artifact.bytes.len() as u64;
            self.toc.indexes.vec = Some(VecIndexManifest {
                vector_count: artifact.vector_count,
                dimension: artifact.dimension,
                bytes_offset: vec_offset,
                bytes_length: artifact.bytes.len() as u64,
                checksum: artifact.checksum,
                compression_mode: self.vec_compression.clone(),
                model: self.vec_model.clone(),
            });
            self.vec_index = Some(index);
        } else {
            // Only clear manifest if vec is disabled, keep empty placeholder if enabled
            if !self.vec_enabled {
                self.toc.indexes.vec = None;
            }
            self.vec_index = None;
        }

        // Persist CLIP index if it has embeddings
        if self.clip_enabled {
            if let Some(ref clip_index) = self.clip_index {
                if !clip_index.is_empty() {
                    let artifact = clip_index.encode()?;
                    let clip_offset = footer_offset;
                    self.file.seek(SeekFrom::Start(clip_offset))?;
                    self.file.write_all(&artifact.bytes)?;
                    footer_offset += artifact.bytes.len() as u64;
                    self.toc.indexes.clip = Some(crate::clip::ClipIndexManifest {
                        bytes_offset: clip_offset,
                        bytes_length: artifact.bytes.len() as u64,
                        vector_count: artifact.vector_count,
                        dimension: artifact.dimension,
                        checksum: artifact.checksum,
                        model_name: crate::clip::default_model_info().name.to_string(),
                    });
                    tracing::info!(
                        "rebuild_indexes: persisted CLIP index with {} vectors at offset {}",
                        artifact.vector_count,
                        clip_offset
                    );
                }
            }
        } else {
            self.toc.indexes.clip = None;
        }

        // Persist memories track if it has cards
        if self.memories_track.card_count() > 0 {
            let memories_offset = footer_offset;
            let memories_bytes = self.memories_track.serialize()?;
            let memories_checksum = blake3::hash(&memories_bytes).into();
            self.file.seek(SeekFrom::Start(memories_offset))?;
            self.file.write_all(&memories_bytes)?;
            footer_offset += memories_bytes.len() as u64;

            let stats = self.memories_track.stats();
            self.toc.memories_track = Some(crate::types::MemoriesTrackManifest {
                bytes_offset: memories_offset,
                bytes_length: memories_bytes.len() as u64,
                card_count: stats.card_count as u64,
                entity_count: stats.entity_count as u64,
                checksum: memories_checksum,
            });
        } else {
            self.toc.memories_track = None;
        }

        // Persist logic mesh if it has nodes
        if self.logic_mesh.is_empty() {
            self.toc.logic_mesh = None;
        } else {
            let mesh_offset = footer_offset;
            let mesh_bytes = self.logic_mesh.serialize()?;
            let mesh_checksum: [u8; 32] = blake3::hash(&mesh_bytes).into();
            self.file.seek(SeekFrom::Start(mesh_offset))?;
            self.file.write_all(&mesh_bytes)?;
            footer_offset += mesh_bytes.len() as u64;

            let stats = self.logic_mesh.stats();
            self.toc.logic_mesh = Some(crate::types::LogicMeshManifest {
                bytes_offset: mesh_offset,
                bytes_length: mesh_bytes.len() as u64,
                node_count: stats.node_count as u64,
                edge_count: stats.edge_count as u64,
                checksum: mesh_checksum,
            });
        }

        // This fires on every full rebuild (doctor/compaction); keep it informational to avoid noisy WARNs.
        tracing::info!(
            "rebuild_indexes: ti_offset={} ti_length={} computed_footer={} current_footer={} (before setting)",
            ti_offset,
            ti_length,
            footer_offset,
            self.header.footer_offset
        );

        // Use max() to preserve any higher footer_offset (e.g., from replay segment)
        // This prevents overwriting data like replay segments that were written after index data
        self.header.footer_offset = self.header.footer_offset.max(footer_offset);

        // Ensure the file length covers rebuilt indexes to avoid out-of-bounds manifests.
        if self.file.metadata()?.len() < self.header.footer_offset {
            self.file.set_len(self.header.footer_offset)?;
        }

        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;

        #[cfg(feature = "lex")]
        if self.lex_enabled {
            if let Some(ref engine) = self.tantivy {
                let doc_count = engine.num_docs();
                let active_frame_count = self
                    .toc
                    .frames
                    .iter()
                    .filter(|f| f.status == FrameStatus::Active)
                    .count();

                // Count frames that would actually be indexed by rebuild_tantivy_engine
                // Uses the same logic: content-type based check + size limit
                let text_indexable_count = self
                    .toc
                    .frames
                    .iter()
                    .filter(|f| crate::memvid::search::is_frame_text_indexable(f))
                    .count();

                // Only fail if we have text-indexable frames but none got indexed
                // This avoids false positives for binary files (videos, images)
                if doc_count == 0 && text_indexable_count > 0 {
                    return Err(MemvidError::Doctor {
                        reason: format!(
                            "Lex index rebuild failed: 0 documents indexed from {text_indexable_count} text-indexable frames. \
                            This indicates a critical failure in the rebuild process."
                        ),
                    });
                }

                // Success! Log it
                log::info!(
                    "✓ Doctor lex index rebuild succeeded: {doc_count} docs from {active_frame_count} frames ({text_indexable_count} text-indexable)"
                );
            }
        }

        Ok(())
    }

    /// Persist the memories track to the file without a full rebuild.
    ///
    /// This is used when the memories track has been modified but no frame
    /// changes were made (e.g., after running enrichment).
    fn persist_memories_track(&mut self) -> Result<()> {
        if self.memories_track.card_count() == 0 {
            self.toc.memories_track = None;
            return Ok(());
        }

        // Write after the current footer_offset
        let memories_offset = self.header.footer_offset;
        let memories_bytes = self.memories_track.serialize()?;
        let memories_checksum: [u8; 32] = blake3::hash(&memories_bytes).into();

        self.file.seek(SeekFrom::Start(memories_offset))?;
        self.file.write_all(&memories_bytes)?;

        let stats = self.memories_track.stats();
        self.toc.memories_track = Some(crate::types::MemoriesTrackManifest {
            bytes_offset: memories_offset,
            bytes_length: memories_bytes.len() as u64,
            card_count: stats.card_count as u64,
            entity_count: stats.entity_count as u64,
            checksum: memories_checksum,
        });

        // Update footer_offset to account for the memories track
        self.header.footer_offset = memories_offset + memories_bytes.len() as u64;

        // Ensure the file length covers the memories track
        if self.file.metadata()?.len() < self.header.footer_offset {
            self.file.set_len(self.header.footer_offset)?;
        }

        Ok(())
    }

    /// Persist the CLIP index to the file without a full rebuild.
    ///
    /// This is used when CLIP embeddings have been added but no full
    /// index rebuild is needed (e.g., in parallel segments mode).
    fn persist_clip_index(&mut self) -> Result<()> {
        if !self.clip_enabled {
            self.toc.indexes.clip = None;
            return Ok(());
        }

        let clip_index = match &self.clip_index {
            Some(idx) if !idx.is_empty() => idx,
            _ => {
                self.toc.indexes.clip = None;
                return Ok(());
            }
        };

        // Encode the CLIP index
        let artifact = clip_index.encode()?;

        // Write after the current footer_offset
        let clip_offset = self.header.footer_offset;
        self.file.seek(SeekFrom::Start(clip_offset))?;
        self.file.write_all(&artifact.bytes)?;

        self.toc.indexes.clip = Some(crate::clip::ClipIndexManifest {
            bytes_offset: clip_offset,
            bytes_length: artifact.bytes.len() as u64,
            vector_count: artifact.vector_count,
            dimension: artifact.dimension,
            checksum: artifact.checksum,
            model_name: crate::clip::default_model_info().name.to_string(),
        });

        tracing::info!(
            "persist_clip_index: persisted CLIP index with {} vectors at offset {}",
            artifact.vector_count,
            clip_offset
        );

        // Update footer_offset to account for the CLIP index
        self.header.footer_offset = clip_offset + artifact.bytes.len() as u64;

        // Ensure the file length covers the CLIP index
        if self.file.metadata()?.len() < self.header.footer_offset {
            self.file.set_len(self.header.footer_offset)?;
        }

        Ok(())
    }

    /// Persist the Logic-Mesh to the file without a full rebuild.
    ///
    /// This is used when the Logic-Mesh has been modified but no frame
    /// changes were made (e.g., after running NER enrichment).
    fn persist_logic_mesh(&mut self) -> Result<()> {
        if self.logic_mesh.is_empty() {
            self.toc.logic_mesh = None;
            return Ok(());
        }

        // Write after the current footer_offset
        let mesh_offset = self.header.footer_offset;
        let mesh_bytes = self.logic_mesh.serialize()?;
        let mesh_checksum: [u8; 32] = blake3::hash(&mesh_bytes).into();

        self.file.seek(SeekFrom::Start(mesh_offset))?;
        self.file.write_all(&mesh_bytes)?;

        let stats = self.logic_mesh.stats();
        self.toc.logic_mesh = Some(crate::types::LogicMeshManifest {
            bytes_offset: mesh_offset,
            bytes_length: mesh_bytes.len() as u64,
            node_count: stats.node_count as u64,
            edge_count: stats.edge_count as u64,
            checksum: mesh_checksum,
        });

        // Update footer_offset to account for the logic mesh
        self.header.footer_offset = mesh_offset + mesh_bytes.len() as u64;

        // Ensure the file length covers the logic mesh
        if self.file.metadata()?.len() < self.header.footer_offset {
            self.file.set_len(self.header.footer_offset)?;
        }

        Ok(())
    }

    /// Persist the sketch track to the file without a full rebuild.
    ///
    /// This is used when the sketch track has been modified (e.g., after
    /// running `sketch build`).
    fn persist_sketch_track(&mut self) -> Result<()> {
        if self.sketch_track.is_empty() {
            self.toc.sketch_track = None;
            return Ok(());
        }

        // Seek to write after the current footer_offset
        self.file.seek(SeekFrom::Start(self.header.footer_offset))?;

        // Write the sketch track and get (offset, length, checksum)
        let (sketch_offset, sketch_length, sketch_checksum) =
            crate::types::write_sketch_track(&mut self.file, &self.sketch_track)?;

        let stats = self.sketch_track.stats();
        self.toc.sketch_track = Some(crate::types::SketchTrackManifest {
            bytes_offset: sketch_offset,
            bytes_length: sketch_length,
            entry_count: stats.entry_count,
            #[allow(clippy::cast_possible_truncation)]
            entry_size: stats.variant.entry_size() as u16,
            flags: 0,
            checksum: sketch_checksum,
        });

        // Update footer_offset to account for the sketch track
        self.header.footer_offset = sketch_offset + sketch_length;

        // Ensure the file length covers the sketch track
        if self.file.metadata()?.len() < self.header.footer_offset {
            self.file.set_len(self.header.footer_offset)?;
        }

        tracing::debug!(
            "persist_sketch_track: persisted sketch track with {} entries at offset {}",
            stats.entry_count,
            sketch_offset
        );

        Ok(())
    }

    #[cfg(feature = "lex")]
    fn apply_lex_wal(&mut self, batch: LexWalBatch) -> Result<()> {
        let LexWalBatch {
            generation,
            doc_count,
            checksum,
            segments,
        } = batch;

        if let Ok(mut storage) = self.lex_storage.write() {
            storage.replace(doc_count, checksum, segments);
            storage.set_generation(generation);
        }

        self.persist_lex_manifest()
    }

    #[cfg(feature = "lex")]
    fn append_lex_batch(&mut self, batch: &LexWalBatch) -> Result<()> {
        let payload = encode_to_vec(WalEntry::Lex(batch.clone()), wal_config())?;
        self.append_wal_entry(&payload)?;
        Ok(())
    }

    #[cfg(feature = "lex")]
    fn persist_lex_manifest(&mut self) -> Result<()> {
        let (index_manifest, segments) = if let Ok(storage) = self.lex_storage.read() {
            storage.to_manifest()
        } else {
            (None, Vec::new())
        };

        // Update the manifest
        if let Some(storage_manifest) = index_manifest {
            // Old LexIndexArtifact format: set the manifest with actual offset/length
            self.toc.indexes.lex = Some(storage_manifest);
        } else {
            // Tantivy segments OR lex disabled: clear the manifest
            // Stats will check lex_segments instead of manifest
            self.toc.indexes.lex = None;
        }

        self.toc.indexes.lex_segments = segments;

        // footer_offset is already correctly set by flush_tantivy() earlier in this function.
        // DO NOT call catalog_data_end() as it would include orphaned Tantivy segments.

        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        Ok(())
    }

    #[cfg(feature = "lex")]
    pub(crate) fn update_embedded_lex_snapshot(&mut self, snapshot: TantivySnapshot) -> Result<()> {
        let TantivySnapshot {
            doc_count,
            checksum,
            segments,
        } = snapshot;

        let mut footer_offset = self.data_end;
        self.file.seek(SeekFrom::Start(footer_offset))?;

        let mut embedded_segments: Vec<EmbeddedLexSegment> = Vec::with_capacity(segments.len());
        for segment in segments {
            let bytes_length = segment.bytes.len() as u64;
            self.file.write_all(&segment.bytes)?;
            self.file.flush()?; // Flush segment data to disk
            embedded_segments.push(EmbeddedLexSegment {
                path: segment.path,
                bytes_offset: footer_offset,
                bytes_length,
                checksum: segment.checksum,
            });
            footer_offset += bytes_length;
        }
        // Set footer_offset for TOC writing, but DON'T update data_end
        // data_end stays at end of payloads, so next commit overwrites these segments
        // Use max() to never decrease footer_offset - this preserves replay segments
        // that may have been written at a higher offset
        self.header.footer_offset = self.header.footer_offset.max(footer_offset);

        let mut next_segment_id = self.toc.segment_catalog.next_segment_id;
        let mut catalog_segments: Vec<TantivySegmentDescriptor> =
            Vec::with_capacity(embedded_segments.len());
        for segment in &embedded_segments {
            let descriptor = TantivySegmentDescriptor::from_common(
                SegmentCommon::new(
                    next_segment_id,
                    segment.bytes_offset,
                    segment.bytes_length,
                    segment.checksum,
                ),
                segment.path.clone(),
            );
            catalog_segments.push(descriptor);
            next_segment_id = next_segment_id.saturating_add(1);
        }
        if catalog_segments.is_empty() {
            self.toc.segment_catalog.tantivy_segments.clear();
        } else {
            self.toc.segment_catalog.tantivy_segments = catalog_segments;
            self.toc.segment_catalog.version = self.toc.segment_catalog.version.max(1);
        }
        self.toc.segment_catalog.next_segment_id = next_segment_id;

        // REMOVED: catalog_data_end() check
        // This was causing orphaned Tantivy segments because it would see OLD segments
        // still in the catalog from previous commits, and push footer_offset forward.
        // We want Tantivy segments to overwrite at data_end, so footer_offset should
        // stay at the end of the newly written segments.

        let generation = self
            .lex_storage
            .write()
            .map_err(|_| MemvidError::Tantivy {
                reason: "embedded lex storage lock poisoned".into(),
            })
            .map(|mut storage| {
                storage.replace(doc_count, checksum, embedded_segments.clone());
                storage.generation()
            })?;

        let batch = LexWalBatch {
            generation,
            doc_count,
            checksum,
            segments: embedded_segments.clone(),
        };
        self.append_lex_batch(&batch)?;
        self.persist_lex_manifest()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        Ok(())
    }

    fn mark_frame_deleted(&mut self, frame_id: FrameId) -> Result<()> {
        let index = usize::try_from(frame_id).map_err(|_| MemvidError::InvalidFrame {
            frame_id,
            reason: "frame id too large",
        })?;
        let frame = self
            .toc
            .frames
            .get_mut(index)
            .ok_or(MemvidError::InvalidFrame {
                frame_id,
                reason: "delete target missing",
            })?;
        frame.status = FrameStatus::Deleted;
        frame.superseded_by = None;
        self.remove_frame_from_indexes(frame_id)
    }

    fn remove_frame_from_indexes(&mut self, frame_id: FrameId) -> Result<()> {
        #[cfg(feature = "lex")]
        if let Some(engine) = self.tantivy.as_mut() {
            engine.delete_frame(frame_id)?;
            self.tantivy_dirty = true;
        }
        if let Some(index) = self.lex_index.as_mut() {
            index.remove_document(frame_id);
        }
        if let Some(index) = self.vec_index.as_mut() {
            index.remove(frame_id);
        }
        Ok(())
    }

    pub(crate) fn frame_is_active(&self, frame_id: FrameId) -> bool {
        let Ok(index) = usize::try_from(frame_id) else {
            return false;
        };
        self.toc
            .frames
            .get(index)
            .is_some_and(|frame| frame.status == FrameStatus::Active)
    }

    #[cfg(feature = "parallel_segments")]
    fn segment_span_from_iter<I>(&self, iter: I) -> Option<SegmentSpan>
    where
        I: IntoIterator<Item = FrameId>,
    {
        let mut iter = iter.into_iter();
        let first_id = iter.next()?;
        let first_frame = self.toc.frames.get(first_id as usize);
        let mut min_id = first_id;
        let mut max_id = first_id;
        let mut page_start = first_frame.and_then(|frame| frame.chunk_index).unwrap_or(0);
        let mut page_end = first_frame
            .and_then(|frame| frame.chunk_count)
            .map(|count| page_start + count.saturating_sub(1))
            .unwrap_or(page_start);
        for frame_id in iter {
            if frame_id < min_id {
                min_id = frame_id;
            }
            if frame_id > max_id {
                max_id = frame_id;
            }
            if let Some(frame) = self.toc.frames.get(frame_id as usize) {
                if let Some(idx) = frame.chunk_index {
                    page_start = page_start.min(idx);
                    if let Some(count) = frame.chunk_count {
                        let end = idx + count.saturating_sub(1);
                        page_end = page_end.max(end);
                    } else {
                        page_end = page_end.max(idx);
                    }
                }
            }
        }
        Some(SegmentSpan {
            frame_start: min_id,
            frame_end: max_id,
            page_start,
            page_end,
            ..SegmentSpan::default()
        })
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn decorate_segment_common(common: &mut SegmentCommon, span: SegmentSpan) {
        common.span = Some(span);
        if common.codec_version == 0 {
            common.codec_version = 1;
        }
    }

    #[cfg(feature = "parallel_segments")]
    pub(crate) fn record_index_segment(
        &mut self,
        kind: SegmentKind,
        common: SegmentCommon,
        stats: SegmentStats,
    ) -> Result<()> {
        let entry = IndexSegmentRef {
            kind,
            common,
            stats,
        };
        self.toc.segment_catalog.index_segments.push(entry.clone());
        if let Some(wal) = self.manifest_wal.as_mut() {
            wal.append_segments(&[entry])?;
        }
        Ok(())
    }

    fn ensure_mutation_allowed(&mut self) -> Result<()> {
        self.ensure_writable()?;
        if self.toc.ticket_ref.issuer == "free-tier" {
            return Ok(());
        }
        match self.tier() {
            Tier::Free => Ok(()),
            tier => {
                if self.toc.ticket_ref.issuer.trim().is_empty() {
                    Err(MemvidError::TicketRequired { tier })
                } else {
                    Ok(())
                }
            }
        }
    }

    pub(crate) fn tier(&self) -> Tier {
        match self.toc.ticket_ref.issuer.as_str() {
            "free-tier" => return Tier::Free,
            "dev-tier" => return Tier::Dev,
            "enterprise-tier" => return Tier::Enterprise,
            "unlimited-tier" => return Tier::Unlimited,
            _ => {}
        }
        if self.toc.ticket_ref.capacity_bytes == Tier::Unlimited.capacity_bytes() {
            return Tier::Unlimited;
        }
        if self.header.wal_size >= WAL_SIZE_LARGE {
            Tier::Enterprise
        } else if self.header.wal_size >= WAL_SIZE_MEDIUM {
            Tier::Dev
        } else {
            Tier::Free
        }
    }

    pub(crate) fn capacity_limit(&self) -> u64 {
        if self.toc.ticket_ref.issuer == "unlimited-tier" {
            Tier::Unlimited.capacity_bytes()
        } else if self.toc.ticket_ref.capacity_bytes != 0 {
            self.toc.ticket_ref.capacity_bytes
        } else {
            self.tier().capacity_bytes()
        }
    }

    /// Get current storage capacity in bytes.
    ///
    /// Returns the capacity from the applied ticket, or the default unlimited tier capacity.
    #[must_use]
    pub fn get_capacity(&self) -> u64 {
        self.capacity_limit()
    }

    pub(crate) fn rewrite_toc_footer(&mut self) -> Result<()> {
        tracing::info!(
            vec_segments = self.toc.segment_catalog.vec_segments.len(),
            lex_segments = self.toc.segment_catalog.lex_segments.len(),
            time_segments = self.toc.segment_catalog.time_segments.len(),
            footer_offset = self.header.footer_offset,
            data_end = self.data_end,
            "rewrite_toc_footer: about to serialize TOC"
        );
        let toc_bytes = prepare_toc_bytes(&mut self.toc)?;
        let footer_offset = self.header.footer_offset;
        self.file.seek(SeekFrom::Start(footer_offset))?;
        self.file.write_all(&toc_bytes)?;
        let footer = CommitFooter {
            toc_len: toc_bytes.len() as u64,
            toc_hash: *hash(&toc_bytes).as_bytes(),
            generation: self.generation,
        };
        let encoded_footer = footer.encode();
        self.file.write_all(&encoded_footer)?;

        // The file must always be at least header + WAL size
        let new_len = footer_offset + toc_bytes.len() as u64 + encoded_footer.len() as u64;
        let min_len = self.header.wal_offset + self.header.wal_size;
        let final_len = new_len.max(min_len);

        if new_len < min_len {
            tracing::warn!(
                file.new_len = new_len,
                file.min_len = min_len,
                file.final_len = final_len,
                "truncation would cut into WAL region, clamping to min_len"
            );
        }

        self.file.set_len(final_len)?;
        // Ensure footer is flushed to disk so mmap-based readers can find it
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(feature = "parallel_segments")]
impl Memvid {
    fn publish_parallel_delta(&mut self, delta: &IngestionDelta, opts: &BuildOpts) -> Result<bool> {
        let chunks = self.collect_segment_chunks(delta)?;
        if chunks.is_empty() {
            return Ok(false);
        }
        let planner = SegmentPlanner::new(opts.clone());
        let plans = planner.plan_from_chunks(chunks);
        if plans.is_empty() {
            return Ok(false);
        }
        let worker_pool = SegmentWorkerPool::new(opts);
        let results = worker_pool.execute(plans)?;
        if results.is_empty() {
            return Ok(false);
        }
        self.append_parallel_segments(results)?;
        Ok(true)
    }

    fn collect_segment_chunks(&mut self, delta: &IngestionDelta) -> Result<Vec<SegmentChunkPlan>> {
        let mut embedding_map: HashMap<FrameId, Vec<f32>> =
            delta.inserted_embeddings.iter().cloned().collect();
        tracing::info!(
            inserted_frames = ?delta.inserted_frames,
            embedding_keys = ?embedding_map.keys().collect::<Vec<_>>(),
            "collect_segment_chunks: comparing frame IDs"
        );
        let mut chunks = Vec::with_capacity(delta.inserted_frames.len());
        for frame_id in &delta.inserted_frames {
            let frame = self.toc.frames.get(*frame_id as usize).cloned().ok_or(
                MemvidError::InvalidFrame {
                    frame_id: *frame_id,
                    reason: "frame id out of range while planning segments",
                },
            )?;
            let text = self.frame_content(&frame)?;
            if text.trim().is_empty() {
                continue;
            }
            let token_estimate = estimate_tokens(&text);
            let chunk_index = frame.chunk_index.unwrap_or(0) as usize;
            let chunk_count = frame.chunk_count.unwrap_or(1) as usize;
            let page_start = if frame.chunk_index.is_some() {
                chunk_index + 1
            } else {
                0
            };
            let page_end = if frame.chunk_index.is_some() {
                page_start
            } else {
                0
            };
            chunks.push(SegmentChunkPlan {
                text,
                frame_id: *frame_id,
                timestamp: frame.timestamp,
                chunk_index,
                chunk_count: chunk_count.max(1),
                token_estimate,
                token_start: 0,
                token_end: 0,
                page_start,
                page_end,
                embedding: embedding_map.remove(frame_id),
            });
        }
        Ok(chunks)
    }
}

#[cfg(feature = "parallel_segments")]
fn estimate_tokens(text: &str) -> usize {
    text.split_whitespace().count().max(1)
}

impl Memvid {
    pub(crate) fn align_footer_with_catalog(&mut self) -> Result<bool> {
        let catalog_end = self.catalog_data_end();
        if catalog_end <= self.header.footer_offset {
            return Ok(false);
        }
        self.header.footer_offset = catalog_end;
        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        Ok(true)
    }
}

impl Memvid {
    pub fn vacuum(&mut self) -> Result<()> {
        self.commit()?;
        // Vacuum rewrites payloads in place, so preserve segment-backed
        // vectors before the first compact payload write.
        self.materialize_vec_segments_for_rebuild()?;

        let mut active_payloads: HashMap<FrameId, Vec<u8>> = HashMap::new();
        let frames: Vec<Frame> = self
            .toc
            .frames
            .iter()
            .filter(|frame| frame.status == FrameStatus::Active)
            .cloned()
            .collect();
        for frame in frames {
            let bytes = self.read_frame_payload_bytes(&frame)?;
            active_payloads.insert(frame.id, bytes);
        }

        let mut cursor = self.header.wal_offset + self.header.wal_size;
        self.file.seek(SeekFrom::Start(cursor))?;
        for frame in &mut self.toc.frames {
            if frame.status == FrameStatus::Active {
                if let Some(bytes) = active_payloads.get(&frame.id) {
                    self.file.write_all(bytes)?;
                    frame.payload_offset = cursor;
                    frame.payload_length = bytes.len() as u64;
                    cursor += bytes.len() as u64;
                } else {
                    frame.payload_offset = 0;
                    frame.payload_length = 0;
                }
            } else {
                frame.payload_offset = 0;
                frame.payload_length = 0;
            }
        }

        self.data_end = cursor;
        self.cached_payload_end = cursor;

        self.toc.segments.clear();
        self.toc.indexes.lex_segments.clear();
        self.toc.segment_catalog.lex_segments.clear();
        self.toc.segment_catalog.vec_segments.clear();
        self.toc.segment_catalog.time_segments.clear();
        #[cfg(feature = "temporal_track")]
        {
            self.toc.temporal_track = None;
            self.toc.segment_catalog.temporal_segments.clear();
        }
        #[cfg(feature = "lex")]
        {
            self.toc.segment_catalog.tantivy_segments.clear();
        }
        #[cfg(feature = "parallel_segments")]
        {
            self.toc.segment_catalog.index_segments.clear();
        }

        // Clear in-memory Tantivy state so it doesn't write old segments on next commit
        #[cfg(feature = "lex")]
        {
            self.tantivy = None;
            self.tantivy_dirty = false;
        }

        // Vacuum historically mutates the live file after its preliminary
        // commit. Do not use staging-only destructive truncation here.
        self.rebuild_indexes(&[], &[], false)?;
        self.persist_sketch_track()?;
        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Preview how a document would be chunked without actually ingesting it.
    ///
    /// This is useful when you need to compute embeddings for each chunk externally
    /// before calling `put_with_chunk_embeddings()`. Returns `None` if the document
    /// is too small to be chunked (< 2400 chars after normalization).
    ///
    /// # Example
    /// ```ignore
    /// let chunks = mem.preview_chunks(b"long document text...")?;
    /// if let Some(chunk_texts) = chunks {
    ///     let embeddings = my_embedder.embed_chunks(&chunk_texts)?;
    ///     mem.put_with_chunk_embeddings(payload, None, embeddings, options)?;
    /// } else {
    ///     let embedding = my_embedder.embed_query(text)?;
    ///     mem.put_with_embedding_and_options(payload, embedding, options)?;
    /// }
    /// ```
    #[must_use]
    pub fn preview_chunks(&self, payload: &[u8]) -> Option<Vec<String>> {
        plan_document_chunks(payload).map(|plan| plan.chunks)
    }

    /// Append raw bytes as a document frame.
    pub fn put_bytes(&mut self, payload: &[u8]) -> Result<u64> {
        self.put_internal(Some(payload), None, None, None, PutOptions::default(), None)
    }

    /// Append raw bytes with explicit metadata/options.
    pub fn put_bytes_with_options(&mut self, payload: &[u8], options: PutOptions) -> Result<u64> {
        self.put_internal(Some(payload), None, None, None, options, None)
    }

    /// Append bytes and an existing embedding (bypasses on-device embedding).
    pub fn put_with_embedding(&mut self, payload: &[u8], embedding: Vec<f32>) -> Result<u64> {
        self.put_internal(
            Some(payload),
            None,
            Some(embedding),
            None,
            PutOptions::default(),
            None,
        )
    }

    pub fn put_with_embedding_and_options(
        &mut self,
        payload: &[u8],
        embedding: Vec<f32>,
        options: PutOptions,
    ) -> Result<u64> {
        self.put_internal(Some(payload), None, Some(embedding), None, options, None)
    }

    /// Ingest a document with pre-computed embeddings for both parent and chunks.
    ///
    /// This is the recommended API for high-accuracy semantic search when chunking
    /// occurs. The caller provides:
    /// - `payload`: The document bytes
    /// - `parent_embedding`: Embedding for the parent document (can be empty Vec if chunks have embeddings)
    /// - `chunk_embeddings`: Pre-computed embeddings for each chunk (matched by index)
    /// - `options`: Standard put options
    ///
    /// The number of chunk embeddings should match the number of chunks that will be
    /// created by the chunking algorithm. If fewer embeddings are provided than chunks,
    /// remaining chunks will have no embedding. If more are provided, extras are ignored.
    pub fn put_with_chunk_embeddings(
        &mut self,
        payload: &[u8],
        parent_embedding: Option<Vec<f32>>,
        chunk_embeddings: Vec<Vec<f32>>,
        options: PutOptions,
    ) -> Result<u64> {
        self.put_internal(
            Some(payload),
            None,
            parent_embedding,
            Some(chunk_embeddings),
            options,
            None,
        )
    }

    /// Replace an existing frame's payload/metadata, keeping its identity and URI.
    pub fn update_frame(
        &mut self,
        frame_id: FrameId,
        payload: Option<Vec<u8>>,
        mut options: PutOptions,
        embedding: Option<Vec<f32>>,
    ) -> Result<u64> {
        self.ensure_mutation_allowed()?;
        let existing = self.frame_by_id(frame_id)?;
        if existing.status != FrameStatus::Active {
            return Err(MemvidError::InvalidFrame {
                frame_id,
                reason: "frame is not active",
            });
        }

        if options.timestamp.is_none() {
            options.timestamp = Some(existing.timestamp);
        }
        if options.track.is_none() {
            options.track = existing.track.clone();
        }
        if options.kind.is_none() {
            options.kind = existing.kind.clone();
        }
        if options.uri.is_none() {
            options.uri = existing.uri.clone();
        }
        if options.title.is_none() {
            options.title = existing.title.clone();
        }
        if options.metadata.is_none() {
            options.metadata = existing.metadata.clone();
        }
        if options.search_text.is_none() {
            options.search_text = existing.search_text.clone();
        }
        if options.tags.is_empty() {
            options.tags = existing.tags.clone();
        }
        if options.labels.is_empty() {
            options.labels = existing.labels.clone();
        }
        if options.extra_metadata.is_empty() {
            options.extra_metadata = existing.extra_metadata.clone();
        }

        let reuse_frame = if payload.is_none() {
            options.auto_tag = false;
            options.extract_dates = false;
            Some(existing.clone())
        } else {
            None
        };

        let effective_embedding = if let Some(explicit) = embedding {
            Some(explicit)
        } else if self.vec_enabled {
            self.frame_embedding(frame_id)?
        } else {
            None
        };

        let payload_slice = payload.as_deref();
        let reuse_flag = reuse_frame.is_some();
        let replace_flag = payload_slice.is_some();
        let seq = self.put_internal(
            payload_slice,
            reuse_frame,
            effective_embedding,
            None, // No chunk embeddings for update
            options,
            Some(frame_id),
        )?;
        info!(
            "frame_update frame_id={frame_id} seq={seq} reused_payload={reuse_flag} replaced_payload={replace_flag}"
        );
        Ok(seq)
    }

    pub fn delete_frame(&mut self, frame_id: FrameId) -> Result<u64> {
        self.ensure_mutation_allowed()?;
        let frame = self.frame_by_id(frame_id)?;
        if frame.status != FrameStatus::Active {
            return Err(MemvidError::InvalidFrame {
                frame_id,
                reason: "frame is not active",
            });
        }

        let mut tombstone = WalEntryData {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(frame.timestamp),
            kind: None,
            track: None,
            payload: Vec::new(),
            embedding: None,
            uri: frame.uri.clone(),
            title: frame.title.clone(),
            canonical_encoding: frame.canonical_encoding,
            canonical_length: frame.canonical_length,
            metadata: None,
            search_text: None,
            tags: Vec::new(),
            labels: Vec::new(),
            extra_metadata: BTreeMap::new(),
            content_dates: Vec::new(),
            chunk_manifest: None,
            role: frame.role,
            parent_sequence: None,
            chunk_index: frame.chunk_index,
            chunk_count: frame.chunk_count,
            op: FrameWalOp::Tombstone,
            target_frame_id: Some(frame_id),
            supersedes_frame_id: None,
            reuse_payload_from: None,
            source_sha256: None,
            source_path: None,
            enrichment_state: crate::types::EnrichmentState::default(),
        };
        tombstone.kind = frame.kind.clone();
        tombstone.track = frame.track.clone();

        let payload_bytes = encode_to_vec(WalEntry::Frame(tombstone), wal_config())?;
        let seq = self.append_wal_entry(&payload_bytes)?;
        self.dirty = true;
        let suppress_checkpoint = self
            .batch_opts
            .as_ref()
            .is_some_and(|o| o.disable_auto_checkpoint);
        if !suppress_checkpoint && self.wal.should_checkpoint() {
            self.commit()?;
        }
        info!("frame_delete frame_id={frame_id} seq={seq}");
        Ok(seq)
    }
}

impl Memvid {
    fn put_internal(
        &mut self,
        payload: Option<&[u8]>,
        reuse_frame: Option<Frame>,
        embedding: Option<Vec<f32>>,
        chunk_embeddings: Option<Vec<Vec<f32>>>,
        mut options: PutOptions,
        supersedes: Option<FrameId>,
    ) -> Result<u64> {
        self.ensure_mutation_allowed()?;

        // Deduplication: if enabled and we have payload, check if identical content exists
        if options.dedup {
            if let Some(bytes) = payload {
                let content_hash = hash(bytes);
                if let Some(existing_frame) = self.find_frame_by_hash(content_hash.as_bytes()) {
                    // Found existing frame with same content hash, skip ingestion
                    tracing::debug!(
                        frame_id = existing_frame.id,
                        "dedup: skipping ingestion, identical content already exists"
                    );
                    // Return existing frame's sequence number (which equals frame_id for committed frames)
                    return Ok(existing_frame.id);
                }
            }
        }

        if payload.is_some() && reuse_frame.is_some() {
            let frame_id = reuse_frame
                .as_ref()
                .map(|frame| frame.id)
                .unwrap_or_default();
            return Err(MemvidError::InvalidFrame {
                frame_id,
                reason: "cannot reuse payload when bytes are provided",
            });
        }

        // If the caller supplies embeddings, enforce a single vector dimension contract
        // for the entire memory (fail fast, never silently accept mixed dimensions).
        let incoming_dimension = {
            let mut dim: Option<u32> = None;

            if let Some(ref vector) = embedding {
                if !vector.is_empty() {
                    #[allow(clippy::cast_possible_truncation)]
                    let len = vector.len() as u32;
                    dim = Some(len);
                }
            }

            if let Some(ref vectors) = chunk_embeddings {
                for vector in vectors {
                    if vector.is_empty() {
                        continue;
                    }
                    let vec_dim = u32::try_from(vector.len()).unwrap_or(0);
                    match dim {
                        None => dim = Some(vec_dim),
                        Some(existing) if existing == vec_dim => {}
                        Some(existing) => {
                            return Err(MemvidError::VecDimensionMismatch {
                                expected: existing,
                                actual: vector.len(),
                            });
                        }
                    }
                }
            }

            dim
        };

        if let Some(incoming_dimension) = incoming_dimension {
            // Embeddings imply vector search should be enabled.
            if !self.vec_enabled {
                self.enable_vec()?;
            }

            if let Some(existing_dimension) = self.effective_vec_index_dimension()? {
                if existing_dimension != incoming_dimension {
                    return Err(MemvidError::VecDimensionMismatch {
                        expected: existing_dimension,
                        actual: incoming_dimension as usize,
                    });
                }
            }

            // Persist the dimension early for better auto-detection (even before the next commit).
            if let Some(manifest) = self.toc.indexes.vec.as_mut() {
                if manifest.dimension == 0 {
                    manifest.dimension = incoming_dimension;
                }
            }
        }

        let mut prepared_payload: Option<(Vec<u8>, CanonicalEncoding, Option<u64>)> = None;
        let payload_tail = self.payload_region_end();
        let projected = if let Some(bytes) = payload {
            let (prepared, encoding, length) = if let Some(ref opts) = self.batch_opts {
                prepare_canonical_payload_with_level(bytes, opts.compression_level)?
            } else {
                prepare_canonical_payload(bytes)?
            };
            let len = prepared.len();
            prepared_payload = Some((prepared, encoding, length));
            payload_tail.saturating_add(len as u64)
        } else if reuse_frame.is_some() {
            payload_tail
        } else {
            return Err(MemvidError::InvalidFrame {
                frame_id: 0,
                reason: "payload required for frame insertion",
            });
        };

        let capacity_limit = self.capacity_limit();
        if projected > capacity_limit {
            let incoming_size = projected.saturating_sub(payload_tail);
            return Err(MemvidError::CapacityExceeded {
                current: payload_tail,
                limit: capacity_limit,
                required: incoming_size,
            });
        }
        let timestamp = options.timestamp.take().unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
        });

        #[allow(unused_assignments)]
        let mut reuse_bytes: Option<Vec<u8>> = None;
        let payload_for_processing = if let Some(bytes) = payload {
            Some(bytes)
        } else if let Some(frame) = reuse_frame.as_ref() {
            let bytes = self.frame_canonical_bytes(frame)?;
            reuse_bytes = Some(bytes);
            reuse_bytes.as_deref()
        } else {
            None
        };

        // Try to create a chunk plan from raw UTF-8 bytes first
        let raw_chunk_plan = match (payload, reuse_frame.as_ref()) {
            (Some(bytes), None) => plan_document_chunks(bytes),
            _ => None,
        };

        // For UTF-8 text chunks, we don't store the parent payload (chunks contain the text)
        // For binary documents (PDF, etc.), we store the original payload and create text chunks separately
        // For --no-raw mode, we store only the extracted text and a hash of the original binary
        let mut source_sha256: Option<[u8; 32]> = None;
        let source_path_value = options.source_path.take();

        let (storage_payload, canonical_encoding, canonical_length, reuse_payload_from) =
            if raw_chunk_plan.is_some() {
                // UTF-8 text document - chunks contain the text, no parent payload needed
                (Vec::new(), CanonicalEncoding::Plain, Some(0), None)
            } else if options.no_raw {
                // --no-raw mode: don't store the raw binary, only compute hash
                if let Some(bytes) = payload {
                    // Compute BLAKE3 hash of original binary for verification
                    let hash_result = hash(bytes);
                    source_sha256 = Some(*hash_result.as_bytes());
                    // Store empty payload - the extracted text is in search_text
                    (Vec::new(), CanonicalEncoding::Plain, Some(0), None)
                } else {
                    return Err(MemvidError::InvalidFrame {
                        frame_id: 0,
                        reason: "payload required for --no-raw mode",
                    });
                }
            } else if let Some((prepared, encoding, length)) = prepared_payload.take() {
                (prepared, encoding, length, None)
            } else if let Some(bytes) = payload {
                let (prepared, encoding, length) = if let Some(ref opts) = self.batch_opts {
                    prepare_canonical_payload_with_level(bytes, opts.compression_level)?
                } else {
                    prepare_canonical_payload(bytes)?
                };
                (prepared, encoding, length, None)
            } else if let Some(frame) = reuse_frame.as_ref() {
                (
                    Vec::new(),
                    frame.canonical_encoding,
                    frame.canonical_length,
                    Some(frame.id),
                )
            } else {
                return Err(MemvidError::InvalidFrame {
                    frame_id: 0,
                    reason: "payload required for frame insertion",
                });
            };

        // Track whether we'll create an extracted text chunk plan later
        let mut chunk_plan = raw_chunk_plan;

        let mut metadata = options.metadata.take();
        let mut search_text = options
            .search_text
            .take()
            .and_then(|text| normalize_text(&text, DEFAULT_SEARCH_TEXT_LIMIT).map(|n| n.text));
        let mut tags = std::mem::take(&mut options.tags);
        let mut labels = std::mem::take(&mut options.labels);
        let mut extra_metadata = std::mem::take(&mut options.extra_metadata);
        let mut content_dates: Vec<String> = Vec::new();

        let need_search_text = search_text
            .as_ref()
            .is_none_or(|text| text.trim().is_empty());
        let need_metadata = metadata.is_none();
        let run_extractor = need_search_text || need_metadata || options.auto_tag;

        let mut extraction_error = None;
        let mut is_skim_extraction = false; // Track if extraction was time-limited

        let extracted = if run_extractor {
            if let Some(bytes) = payload_for_processing {
                let mime_hint = metadata.as_ref().and_then(|m| m.mime.as_deref());
                let uri_hint = options.uri.as_deref();

                // Use time-budgeted extraction for instant indexing with a budget
                let use_budgeted = options.instant_index && options.extraction_budget_ms > 0;

                if use_budgeted {
                    // Time-budgeted extraction for sub-second ingestion
                    let budget = crate::extract_budgeted::ExtractionBudget::with_ms(
                        options.extraction_budget_ms,
                    );
                    match crate::extract_budgeted::extract_with_budget(
                        bytes, mime_hint, uri_hint, budget,
                    ) {
                        Ok(result) => {
                            is_skim_extraction = result.is_skim();
                            if is_skim_extraction {
                                tracing::debug!(
                                    coverage = result.coverage,
                                    elapsed_ms = result.elapsed_ms,
                                    sections = %format!("{}/{}", result.sections_extracted, result.sections_total),
                                    "time-budgeted extraction (skim)"
                                );
                            }
                            // Convert BudgetedExtractionResult to ExtractedDocument
                            let doc = crate::extract::ExtractedDocument {
                                text: if result.text.is_empty() {
                                    None
                                } else {
                                    Some(result.text)
                                },
                                metadata: serde_json::json!({
                                    "skim": is_skim_extraction,
                                    "coverage": result.coverage,
                                    "sections_extracted": result.sections_extracted,
                                    "sections_total": result.sections_total,
                                }),
                                mime_type: mime_hint.map(std::string::ToString::to_string),
                            };
                            Some(doc)
                        }
                        Err(err) => {
                            // Fall back to full extraction on budgeted extraction error
                            tracing::warn!(
                                ?err,
                                "budgeted extraction failed, trying full extraction"
                            );
                            match extract_via_registry(bytes, mime_hint, uri_hint) {
                                Ok(doc) => Some(doc),
                                Err(err) => {
                                    extraction_error = Some(err);
                                    None
                                }
                            }
                        }
                    }
                } else {
                    // Full extraction (no time budget)
                    match extract_via_registry(bytes, mime_hint, uri_hint) {
                        Ok(doc) => Some(doc),
                        Err(err) => {
                            extraction_error = Some(err);
                            None
                        }
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(err) = extraction_error {
            return Err(err);
        }

        if let Some(doc) = &extracted {
            if need_search_text {
                if let Some(text) = &doc.text {
                    if let Some(normalized) =
                        normalize_text(text, DEFAULT_SEARCH_TEXT_LIMIT).map(|n| n.text)
                    {
                        search_text = Some(normalized);
                    }
                }
            }

            // If we don't have a chunk plan from raw bytes (e.g., PDF), try to create one
            // from extracted text. This ensures large documents like PDFs get fully indexed.
            if chunk_plan.is_none() {
                if let Some(text) = &doc.text {
                    chunk_plan = plan_text_chunks(text);
                }
            }

            if let Some(mime) = doc.mime_type.as_ref() {
                if let Some(existing) = &mut metadata {
                    if existing.mime.is_none() {
                        existing.mime = Some(mime.clone());
                    }
                } else {
                    let mut doc_meta = DocMetadata::default();
                    doc_meta.mime = Some(mime.clone());
                    metadata = Some(doc_meta);
                }
            }

            // Only add extractous_metadata when auto_tag is enabled
            // This allows callers to disable metadata pollution by setting auto_tag=false
            if options.auto_tag {
                if let Some(meta_json) = (!doc.metadata.is_null()).then(|| doc.metadata.to_string())
                {
                    extra_metadata
                        .entry("extractous_metadata".to_string())
                        .or_insert(meta_json);
                }
            }
        }

        if options.auto_tag {
            if let Some(ref text) = search_text {
                if !text.trim().is_empty() {
                    let result = AutoTagger.analyse(text, options.extract_dates);
                    merge_unique(&mut tags, result.tags);
                    merge_unique(&mut labels, result.labels);
                    if options.extract_dates && content_dates.is_empty() {
                        content_dates = result.content_dates;
                    }
                }
            }
        }

        if content_dates.is_empty() {
            if let Some(frame) = reuse_frame.as_ref() {
                content_dates = frame.content_dates.clone();
            }
        }

        let metadata_ref = metadata.as_ref();
        let mut search_text = augment_search_text(
            search_text,
            options.uri.as_deref(),
            options.title.as_deref(),
            options.track.as_deref(),
            &tags,
            &labels,
            &extra_metadata,
            &content_dates,
            metadata_ref,
        );
        let mut chunk_entries: Vec<WalEntryData> = Vec::new();
        let mut parent_chunk_manifest: Option<TextChunkManifest> = None;
        let mut parent_chunk_count: Option<u32> = None;

        let kind_value = options.kind.take();
        let track_value = options.track.take();
        let uri_value = options.uri.take();
        let title_value = options.title.take();
        let should_extract_triplets = options.extract_triplets;
        // Save references for triplet extraction (after search_text is moved into WAL entry)
        let triplet_uri = uri_value.clone();
        let triplet_title = title_value.clone();

        if let Some(plan) = chunk_plan.as_ref() {
            let chunk_total = u32::try_from(plan.chunks.len()).unwrap_or(0);
            parent_chunk_manifest = Some(plan.manifest.clone());
            parent_chunk_count = Some(chunk_total);

            if let Some(first_chunk) = plan.chunks.first() {
                if let Some(normalized) =
                    normalize_text(first_chunk, DEFAULT_SEARCH_TEXT_LIMIT).map(|n| n.text)
                {
                    if !normalized.trim().is_empty() {
                        search_text = Some(normalized);
                    }
                }
            }

            let chunk_tags = tags.clone();
            let chunk_labels = labels.clone();
            let chunk_metadata = metadata.clone();
            let chunk_extra_metadata = extra_metadata.clone();
            let chunk_content_dates = content_dates.clone();

            for (idx, chunk_text) in plan.chunks.iter().enumerate() {
                let (chunk_payload, chunk_encoding, chunk_length) =
                    prepare_canonical_payload(chunk_text.as_bytes())?;
                let chunk_search_text = normalize_text(chunk_text, DEFAULT_SEARCH_TEXT_LIMIT)
                    .map(|n| n.text)
                    .filter(|text| !text.trim().is_empty());

                let chunk_uri = uri_value
                    .as_ref()
                    .map(|uri| format!("{uri}#page-{}", idx + 1));
                let chunk_title = title_value
                    .as_ref()
                    .map(|title| format!("{title} (page {}/{})", idx + 1, chunk_total));

                // Use provided chunk embedding if available, otherwise None
                let chunk_embedding = chunk_embeddings
                    .as_ref()
                    .and_then(|embeddings| embeddings.get(idx).cloned());

                chunk_entries.push(WalEntryData {
                    timestamp,
                    kind: kind_value.clone(),
                    track: track_value.clone(),
                    payload: chunk_payload,
                    embedding: chunk_embedding,
                    uri: chunk_uri,
                    title: chunk_title,
                    canonical_encoding: chunk_encoding,
                    canonical_length: chunk_length,
                    metadata: chunk_metadata.clone(),
                    search_text: chunk_search_text,
                    tags: chunk_tags.clone(),
                    labels: chunk_labels.clone(),
                    extra_metadata: chunk_extra_metadata.clone(),
                    content_dates: chunk_content_dates.clone(),
                    chunk_manifest: None,
                    role: FrameRole::DocumentChunk,
                    parent_sequence: None,
                    chunk_index: Some(u32::try_from(idx).unwrap_or(0)),
                    chunk_count: Some(chunk_total),
                    op: FrameWalOp::Insert,
                    target_frame_id: None,
                    supersedes_frame_id: None,
                    reuse_payload_from: None,
                    source_sha256: None, // Chunks don't have source references
                    source_path: None,
                    // Chunks are already extracted, so mark as Enriched
                    enrichment_state: crate::types::EnrichmentState::Enriched,
                });
            }
        }

        let parent_uri = uri_value.clone();
        let parent_title = title_value.clone();

        // Get parent_sequence from options.parent_id if provided
        // We need the WAL sequence of the parent frame to link them
        let parent_sequence = if let Some(parent_id) = options.parent_id {
            // Look up the parent frame to get its WAL sequence
            // Since frame.id corresponds to the array index, we need to find the sequence
            // For now, we'll use the frame_id + WAL_START_SEQUENCE as an approximation
            // This works because sequence numbers are assigned incrementally
            usize::try_from(parent_id)
                .ok()
                .and_then(|idx| self.toc.frames.get(idx))
                .map(|_| parent_id + 2) // WAL sequences start at 2
        } else {
            None
        };

        // Clone search_text for triplet extraction (before it's moved into WAL entry)
        let triplet_text = search_text.clone();

        // Capture values needed for instant indexing BEFORE they're moved into entry
        #[cfg(feature = "lex")]
        let instant_index_tags = if options.instant_index {
            tags.clone()
        } else {
            Vec::new()
        };
        #[cfg(feature = "lex")]
        let instant_index_labels = if options.instant_index {
            labels.clone()
        } else {
            Vec::new()
        };

        // Determine enrichment state: Searchable if needs background work, Enriched if complete
        #[cfg(feature = "lex")]
        let needs_enrichment =
            options.instant_index && (options.enable_embedding || is_skim_extraction);
        #[cfg(feature = "lex")]
        let enrichment_state = if needs_enrichment {
            crate::types::EnrichmentState::Searchable
        } else {
            crate::types::EnrichmentState::Enriched
        };
        #[cfg(not(feature = "lex"))]
        let enrichment_state = crate::types::EnrichmentState::Enriched;

        let entry = WalEntryData {
            timestamp,
            kind: kind_value,
            track: track_value,
            payload: storage_payload,
            embedding,
            uri: parent_uri,
            title: parent_title,
            canonical_encoding,
            canonical_length,
            metadata,
            search_text,
            tags,
            labels,
            extra_metadata,
            content_dates,
            chunk_manifest: parent_chunk_manifest,
            role: options.role,
            parent_sequence,
            chunk_index: None,
            chunk_count: parent_chunk_count,
            op: FrameWalOp::Insert,
            target_frame_id: None,
            supersedes_frame_id: supersedes,
            reuse_payload_from,
            source_sha256,
            source_path: source_path_value,
            enrichment_state,
        };

        let parent_bytes = encode_to_vec(WalEntry::Frame(entry), wal_config())?;
        let parent_seq = self.append_wal_entry(&parent_bytes)?;
        self.pending_frame_inserts = self.pending_frame_inserts.saturating_add(1);

        // Instant indexing: make frame searchable immediately (<1s) without full commit
        // This is Phase 1 of progressive ingestion - frame is searchable but not fully enriched
        #[cfg(feature = "lex")]
        if options.instant_index && self.tantivy.is_some() {
            // Create a minimal frame for indexing
            let frame_id = parent_seq as FrameId;

            // Use triplet_text which was cloned before entry was created
            if let Some(ref text) = triplet_text {
                if !text.trim().is_empty() {
                    // Create temporary frame for indexing (minimal fields for Tantivy)
                    let temp_frame = Frame {
                        id: frame_id,
                        timestamp,
                        anchor_ts: None,
                        anchor_source: None,
                        kind: options.kind.clone(),
                        track: options.track.clone(),
                        payload_offset: 0,
                        payload_length: 0,
                        checksum: [0u8; 32],
                        uri: options
                            .uri
                            .clone()
                            .or_else(|| Some(crate::default_uri(frame_id))),
                        title: options.title.clone(),
                        canonical_encoding: crate::types::CanonicalEncoding::default(),
                        canonical_length: None,
                        metadata: None, // Not needed for text search
                        search_text: triplet_text.clone(),
                        tags: instant_index_tags.clone(),
                        labels: instant_index_labels.clone(),
                        extra_metadata: std::collections::BTreeMap::new(), // Not needed for search
                        content_dates: Vec::new(),                         // Not needed for search
                        chunk_manifest: None,
                        role: options.role,
                        parent_id: None,
                        chunk_index: None,
                        chunk_count: None,
                        status: FrameStatus::Active,
                        supersedes,
                        superseded_by: None,
                        source_sha256: None, // Not needed for search
                        source_path: None,   // Not needed for search
                        enrichment_state: crate::types::EnrichmentState::Searchable,
                    };

                    // Get mutable reference to engine and index the frame
                    if let Some(engine) = self.tantivy.as_mut() {
                        engine.add_frame(&temp_frame, text)?;
                        engine.soft_commit()?;
                        self.tantivy_dirty = true;

                        tracing::debug!(
                            frame_id = frame_id,
                            "instant index: frame searchable immediately"
                        );
                    }
                }
            }
        }

        // Queue frame for background enrichment when using instant index path
        // Enrichment includes: embedding generation, full text re-extraction if time-limited
        // Note: enrichment_state is already set in the WAL entry, so it will be correct after replay
        #[cfg(feature = "lex")]
        if needs_enrichment {
            let frame_id = parent_seq as FrameId;
            self.toc.enrichment_queue.push(frame_id);
            tracing::debug!(
                frame_id = frame_id,
                is_skim = is_skim_extraction,
                needs_embedding = options.enable_embedding,
                "queued frame for background enrichment"
            );
        }

        for mut chunk_entry in chunk_entries {
            chunk_entry.parent_sequence = Some(parent_seq);
            let chunk_bytes = encode_to_vec(WalEntry::Frame(chunk_entry), wal_config())?;
            self.append_wal_entry(&chunk_bytes)?;
            self.pending_frame_inserts = self.pending_frame_inserts.saturating_add(1);
        }

        self.dirty = true;
        let suppress_checkpoint = self
            .batch_opts
            .as_ref()
            .is_some_and(|o| o.disable_auto_checkpoint);
        if !suppress_checkpoint && self.wal.should_checkpoint() {
            self.commit()?;
        }

        // Record the put action if a replay session is active
        #[cfg(feature = "replay")]
        if let Some(input_bytes) = payload {
            self.record_put_action(parent_seq, input_bytes);
        }

        // Extract triplets if enabled (default: true)
        // Triplets are stored as MemoryCards with entity/slot/value structure
        if should_extract_triplets {
            if let Some(ref text) = triplet_text {
                if !text.trim().is_empty() {
                    let extractor = TripletExtractor::default();
                    let frame_id = parent_seq as FrameId;
                    let (cards, _stats) = extractor.extract(
                        frame_id,
                        text,
                        triplet_uri.as_deref(),
                        triplet_title.as_deref(),
                        timestamp,
                    );

                    if !cards.is_empty() {
                        // Add cards to memories track
                        let card_ids = self.memories_track.add_cards(cards);

                        // Record enrichment for incremental processing
                        self.memories_track
                            .record_enrichment(frame_id, "rules", "1.0.0", card_ids);
                    }
                }
            }
        }

        Ok(parent_seq)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FrameWalOp {
    Insert,
    Tombstone,
}

impl Default for FrameWalOp {
    fn default() -> Self {
        Self::Insert
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum WalEntry {
    Frame(WalEntryData),
    #[cfg(feature = "lex")]
    Lex(LexWalBatch),
}

fn decode_wal_entry(bytes: &[u8]) -> Result<WalEntry> {
    if let Ok((entry, _)) = decode_from_slice::<WalEntry, _>(bytes, wal_config()) {
        return Ok(entry);
    }
    let (legacy, _) = decode_from_slice::<WalEntryData, _>(bytes, wal_config())?;
    Ok(WalEntry::Frame(legacy))
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WalEntryData {
    pub(crate) timestamp: i64,
    pub(crate) kind: Option<String>,
    pub(crate) track: Option<String>,
    pub(crate) payload: Vec<u8>,
    pub(crate) embedding: Option<Vec<f32>>,
    #[serde(default)]
    pub(crate) uri: Option<String>,
    #[serde(default)]
    pub(crate) title: Option<String>,
    #[serde(default)]
    pub(crate) canonical_encoding: CanonicalEncoding,
    #[serde(default)]
    pub(crate) canonical_length: Option<u64>,
    #[serde(default)]
    pub(crate) metadata: Option<DocMetadata>,
    #[serde(default)]
    pub(crate) search_text: Option<String>,
    #[serde(default)]
    pub(crate) tags: Vec<String>,
    #[serde(default)]
    pub(crate) labels: Vec<String>,
    #[serde(default)]
    pub(crate) extra_metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) content_dates: Vec<String>,
    #[serde(default)]
    pub(crate) chunk_manifest: Option<TextChunkManifest>,
    #[serde(default)]
    pub(crate) role: FrameRole,
    #[serde(default)]
    pub(crate) parent_sequence: Option<u64>,
    #[serde(default)]
    pub(crate) chunk_index: Option<u32>,
    #[serde(default)]
    pub(crate) chunk_count: Option<u32>,
    #[serde(default)]
    pub(crate) op: FrameWalOp,
    #[serde(default)]
    pub(crate) target_frame_id: Option<FrameId>,
    #[serde(default)]
    pub(crate) supersedes_frame_id: Option<FrameId>,
    #[serde(default)]
    pub(crate) reuse_payload_from: Option<FrameId>,
    /// SHA-256 hash of original source file (set when --no-raw is used).
    #[serde(default)]
    pub(crate) source_sha256: Option<[u8; 32]>,
    /// Original source file path (set when --no-raw is used).
    #[serde(default)]
    pub(crate) source_path: Option<String>,
    /// Enrichment state for progressive ingestion.
    #[serde(default)]
    pub(crate) enrichment_state: crate::types::EnrichmentState,
}

pub(crate) fn prepare_canonical_payload(
    payload: &[u8],
) -> Result<(Vec<u8>, CanonicalEncoding, Option<u64>)> {
    prepare_canonical_payload_with_level(payload, 3)
}

pub(crate) fn prepare_canonical_payload_with_level(
    payload: &[u8],
    level: i32,
) -> Result<(Vec<u8>, CanonicalEncoding, Option<u64>)> {
    if level == 0 {
        // No compression — store as plain text
        return Ok((
            payload.to_vec(),
            CanonicalEncoding::Plain,
            Some(payload.len() as u64),
        ));
    }
    if std::str::from_utf8(payload).is_ok() {
        let compressed = zstd::encode_all(std::io::Cursor::new(payload), level)?;
        Ok((
            compressed,
            CanonicalEncoding::Zstd,
            Some(payload.len() as u64),
        ))
    } else {
        Ok((
            payload.to_vec(),
            CanonicalEncoding::Plain,
            Some(payload.len() as u64),
        ))
    }
}

pub(crate) fn augment_search_text(
    base: Option<String>,
    uri: Option<&str>,
    title: Option<&str>,
    track: Option<&str>,
    tags: &[String],
    labels: &[String],
    extra_metadata: &BTreeMap<String, String>,
    content_dates: &[String],
    metadata: Option<&DocMetadata>,
) -> Option<String> {
    let mut segments: Vec<String> = Vec::new();
    if let Some(text) = base {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            segments.push(trimmed.to_string());
        }
    }

    if let Some(title) = title {
        if !title.trim().is_empty() {
            segments.push(format!("title: {}", title.trim()));
        }
    }

    if let Some(uri) = uri {
        if !uri.trim().is_empty() {
            segments.push(format!("uri: {}", uri.trim()));
        }
    }

    if let Some(track) = track {
        if !track.trim().is_empty() {
            segments.push(format!("track: {}", track.trim()));
        }
    }

    if !tags.is_empty() {
        segments.push(format!("tags: {}", tags.join(" ")));
    }

    if !labels.is_empty() {
        segments.push(format!("labels: {}", labels.join(" ")));
    }

    if !extra_metadata.is_empty() {
        for (key, value) in extra_metadata {
            if value.trim().is_empty() {
                continue;
            }
            segments.push(format!("{key}: {value}"));
        }
    }

    if !content_dates.is_empty() {
        segments.push(format!("dates: {}", content_dates.join(" ")));
    }

    if let Some(meta) = metadata {
        if let Ok(meta_json) = serde_json::to_string(meta) {
            segments.push(format!("metadata: {meta_json}"));
        }
    }

    if segments.is_empty() {
        None
    } else {
        Some(segments.join("\n"))
    }
}

pub(crate) fn merge_unique(target: &mut Vec<String>, additions: Vec<String>) {
    if additions.is_empty() {
        return;
    }
    let mut seen: BTreeSet<String> = target.iter().cloned().collect();
    for value in additions {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let candidate = trimmed.to_string();
        if seen.insert(candidate.clone()) {
            target.push(candidate);
        }
    }
}

#[cfg(test)]
mod commit_publication_tests {
    use super::*;
    use crate::SketchVariant;
    use crate::io::header::HeaderCodec;
    use crate::lock::LockMode;
    #[cfg(not(feature = "replay"))]
    use crate::replay::ReplayManifest;

    fn header(path: &Path) -> crate::types::Header {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        HeaderCodec::read(&mut file).unwrap()
    }

    fn fault_options(name: &str) -> PutOptions {
        PutOptions::builder()
            .uri(format!("mv2://fault/{name}"))
            .search_text(name)
            .auto_tag(false)
            .extract_dates(false)
            .extract_triplets(false)
            .instant_index(false)
            .extraction_budget_ms(0)
            .build()
    }

    #[cfg(feature = "lex")]
    fn assert_recovered_text(mem: &mut Memvid, text: &str, frame_id: FrameId) {
        let result = mem
            .search(crate::SearchRequest {
                query: text.to_string(),
                top_k: 1,
                snippet_chars: 80,
                uri: None,
                scope: None,
                cursor: None,
                #[cfg(feature = "temporal_track")]
                temporal: None,
                as_of_frame: None,
                as_of_ts: None,
                no_sketch: true,
                acl_context: None,
                acl_enforcement_mode: crate::AclEnforcementMode::Audit,
            })
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].frame_id, frame_id);
    }

    #[test]
    fn wal_recovery_failures_preserve_committed_file_and_pending_records() {
        for vectors in [false, true] {
            for failure in ["truncate", "sync", "publish"] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("recovery.mv2");
                let mut writer = Memvid::create(&path).unwrap();
                if vectors {
                    writer.enable_vec().unwrap();
                    writer
                        .put_with_embedding_and_options(
                            b"seed",
                            vec![1.0, 0.0],
                            fault_options("seed"),
                        )
                        .unwrap();
                } else {
                    writer
                        .put_bytes_with_options(b"seed", fault_options("seed"))
                        .unwrap();
                }
                writer.insert_sketch(0, "seed", SketchVariant::Small);
                writer.commit().unwrap();
                if vectors {
                    writer
                        .put_with_embedding_and_options(
                            b"alpha",
                            vec![0.0, 1.0],
                            fault_options("alpha"),
                        )
                        .unwrap();
                } else {
                    writer
                        .put_bytes_with_options(b"alpha", fault_options("alpha"))
                        .unwrap();
                }
                // Model a writer that exited after the durable WAL append without a commit.
                writer.dirty = false;
                drop(writer);
                let before = std::fs::read(&path).unwrap();
                match failure {
                    "truncate" => FAIL_AFTER_REBUILD_TRUNCATE.with(|flag| flag.set(true)),
                    "sync" => FAIL_BEFORE_STAGING_SYNC.with(|flag| flag.set(true)),
                    "publish" => FAIL_BEFORE_ATOMIC_PUBLISH.with(|flag| flag.set(true)),
                    _ => unreachable!(),
                }
                let error = Memvid::open(&path)
                    .err()
                    .expect("recovery must report the fault");
                assert!(error.to_string().contains("injected failure"), "{error}");
                assert_eq!(
                    blake3::hash(&std::fs::read(&path).unwrap()),
                    blake3::hash(&before),
                    "failed recovery changed the original: {failure}, vectors={vectors}",
                );
                let mut committed = Memvid::open_read_only(&path).unwrap();
                assert_eq!(committed.frame_count(), 1);
                assert_eq!(committed.frame_canonical_payload(0).unwrap(), b"seed");
                assert!(committed.frame_by_uri("mv2://fault/alpha").is_err());
                drop(committed);

                let mut recovered = Memvid::open(&path).expect("retry recovers the intact WAL");
                assert_eq!(recovered.frame_count(), 2);
                for (id, name) in [(0, "seed"), (1, "alpha")] {
                    assert_eq!(
                        recovered
                            .frame_by_uri(&format!("mv2://fault/{name}"))
                            .unwrap()
                            .id,
                        id
                    );
                    assert_eq!(
                        recovered.frame_canonical_payload(id).unwrap(),
                        name.as_bytes()
                    );
                }
                if vectors {
                    assert_eq!(recovered.search_vec(&[1.0, 0.0], 1).unwrap()[0].frame_id, 0);
                    assert_eq!(recovered.search_vec(&[0.0, 1.0], 1).unwrap()[0].frame_id, 1);
                }
                #[cfg(feature = "lex")]
                {
                    assert_recovered_text(&mut recovered, "seed", 0);
                    assert_recovered_text(&mut recovered, "alpha", 1);
                }
                assert!(recovered.toc.sketch_track.is_some());
                assert!(recovered.sketch_track.get(0).is_some());
                assert!(
                    recovered
                        .wal
                        .records_after(recovered.header.wal_sequence)
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    FileLock::try_acquire(&recovered.file, &path)
                        .unwrap()
                        .is_none()
                );
                drop(recovered);
                let published = std::fs::read(&path).unwrap();
                let reopened = Memvid::open(&path).unwrap();
                assert_eq!(reopened.frame_count(), 2);
                drop(reopened);
                assert_eq!(
                    std::fs::read(&path).unwrap(),
                    published,
                    "second open must not replay twice"
                );
                assert_eq!(Memvid::open_read_only(&path).unwrap().frame_count(), 2);
            }
        }
    }

    #[test]
    fn wal_recovery_process_exit_preserves_committed_file() {
        const CHILD_PATH: &str = "MEMVID_TEST_RECOVERY_EXIT_PATH";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            EXIT_AFTER_REBUILD_TRUNCATE.with(|flag| flag.set(true));
            let _ = Memvid::open(path);
            panic!("recovery did not reach the exit injection");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("interrupted-recovery.mv2");
        let mut writer = Memvid::create(&path).unwrap();
        writer
            .put_bytes_with_options(b"seed", fault_options("seed"))
            .unwrap();
        writer.commit().unwrap();
        writer
            .put_bytes_with_options(b"alpha", fault_options("alpha"))
            .unwrap();
        writer.dirty = false;
        drop(writer);
        let before = std::fs::read(&path).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "memvid::mutation::commit_publication_tests::wal_recovery_process_exit_preserves_committed_file",
                "--test-threads=1",
            ])
            .env(CHILD_PATH, &path)
            .spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("recovery child timed out");
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(status.code(), Some(86));
        assert_eq!(
            blake3::hash(&std::fs::read(&path).unwrap()),
            blake3::hash(&before)
        );
        assert_eq!(Memvid::open_read_only(&path).unwrap().frame_count(), 1);
        let mut recovered = Memvid::open(&path).unwrap();
        assert_eq!(recovered.frame_count(), 2);
        assert_eq!(recovered.frame_canonical_payload(0).unwrap(), b"seed");
        assert_eq!(recovered.frame_canonical_payload(1).unwrap(), b"alpha");
        drop(recovered);
        assert_eq!(Memvid::open_read_only(&path).unwrap().frame_count(), 2);
    }

    #[test]
    fn wal_recovery_post_publish_error_keeps_complete_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recovery-post-publish.mv2");
        let mut writer = Memvid::create(&path).unwrap();
        writer
            .put_bytes_with_options(b"seed", fault_options("seed"))
            .unwrap();
        writer.commit().unwrap();
        writer
            .put_bytes_with_options(b"alpha", fault_options("alpha"))
            .unwrap();
        writer.dirty = false;
        drop(writer);
        FAIL_AFTER_ATOMIC_PUBLISH.with(|flag| flag.set(true));
        let error = Memvid::open(&path)
            .err()
            .expect("post-publication error stays visible");
        assert!(error.to_string().contains("after atomic publish"));
        let mut snapshot = Memvid::open_read_only(&path).unwrap();
        assert_eq!(snapshot.frame_count(), 2);
        assert_eq!(snapshot.frame_canonical_payload(0).unwrap(), b"seed");
        assert_eq!(snapshot.frame_canonical_payload(1).unwrap(), b"alpha");
        drop(snapshot);
        let published = std::fs::read(&path).unwrap();
        let recovered = Memvid::open(&path).unwrap();
        assert_eq!(recovered.frame_count(), 2);
        drop(recovered);
        assert_eq!(std::fs::read(&path).unwrap(), published);
    }

    #[cfg(feature = "lex")]
    #[test]
    fn wal_recovery_lex_flush_without_records_is_staged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recovery-lex-only.mv2");
        let mut writer = Memvid::create(&path).unwrap();
        writer
            .put_bytes_with_options(b"seed", fault_options("seed"))
            .unwrap();
        writer.commit().unwrap();
        assert!(
            writer
                .wal
                .records_after(writer.header.wal_sequence)
                .unwrap()
                .is_empty()
        );
        // init_tantivy can rebuild an index on open even without pending frame records.
        writer.tantivy_dirty = true;
        let before = std::fs::read(&path).unwrap();
        FAIL_BEFORE_ATOMIC_PUBLISH.with(|flag| flag.set(true));
        let error = writer
            .recover_wal()
            .expect_err("lex-only recovery must use staging");
        assert!(error.to_string().contains("injected failure"));
        assert!(writer.write_disabled.is_some());
        drop(writer);
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let mut reader = Memvid::open_read_only(&path).unwrap();
        assert_recovered_text(&mut reader, "seed", 0);
    }

    fn assert_terminal_direct_writes_rejected(writer: &mut Memvid) {
        let before = writer
            .snapshot_fingerprint()
            .expect("fingerprint before rejected writes");
        assert!(
            writer
                .begin_batch(PutManyOpts {
                    wal_pre_size_bytes: 4 * 1024 * 1024,
                    ..Default::default()
                })
                .is_err(),
            "terminal handle must reject batch WAL growth"
        );
        assert!(
            writer.end_batch().is_err(),
            "terminal handle must reject WAL flush"
        );
        #[cfg(feature = "replay")]
        {
            writer.start_session(Some("terminal".into()), None).unwrap();
            writer.end_session().unwrap();
            assert!(
                writer.save_replay_sessions().is_err(),
                "terminal handle must reject replay publication"
            );
        }
        assert_eq!(
            writer
                .snapshot_fingerprint()
                .expect("fingerprint after rejected writes"),
            before,
            "rejected direct writes must not alter capsule bytes"
        );
    }

    #[test]
    fn failure_before_atomic_publish_keeps_committed_file_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("publish-failure.mv2");
        let mut mem = Memvid::create(&path).unwrap();
        mem.enable_vec().unwrap();
        mem.put_with_embedding_and_options(
            b"first committed document",
            vec![1.0, 0.0, 0.0, 0.0],
            PutOptions::builder()
                .uri("mv2://fault/first")
                .search_text("first committed document")
                .auto_tag(false)
                .extract_dates(false)
                .extract_triplets(false)
                .build(),
        )
        .unwrap();
        mem.commit().unwrap();

        let committed_header = header(&path);
        let committed_len = std::fs::metadata(&path).unwrap().len();

        mem.put_with_embedding_and_options(
            b"second WAL document",
            vec![0.0, 1.0, 0.0, 0.0],
            PutOptions::builder()
                .uri("mv2://fault/second")
                .search_text("second WAL document")
                .auto_tag(false)
                .extract_dates(false)
                .extract_triplets(false)
                .build(),
        )
        .unwrap();
        let file_with_pending_wal = std::fs::read(&path).unwrap();
        FAIL_BEFORE_ATOMIC_PUBLISH.with(|flag| flag.set(true));
        let err = mem
            .commit()
            .expect_err("fault injection must abort publication");
        assert!(err.to_string().contains("injected failure"));
        assert_terminal_direct_writes_rejected(&mut mem);

        // The compacted staging copy was never published: the committed file
        // still has the exact old header and length. Its WAL remains available
        // for the normal open-time recovery path.
        assert_eq!(header(&path).footer_offset, committed_header.footer_offset);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), committed_len);
        assert_eq!(std::fs::read(&path).unwrap(), file_with_pending_wal);
        let recovery_path = dir.path().join("publish-failure-recovery.mv2");
        std::fs::copy(&path, &recovery_path).unwrap();

        // Prevent Drop from consuming the one-shot fault and successfully
        // retrying commit on the source instance. Recovery below operates on
        // an isolated byte-for-byte copy of the post-failure pending-WAL state.
        mem.dirty = false;
        drop(mem);

        let mut committed = Memvid::open_read_only(&recovery_path).unwrap();
        assert_eq!(committed.stats().unwrap().frame_count, 1);
        assert!(committed.frame_by_uri("mv2://fault/first").is_ok());
        assert!(committed.frame_by_uri("mv2://fault/second").is_err());
        assert_eq!(
            committed.search_vec(&[1.0, 0.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            0
        );
        drop(committed);

        let mut recovered = Memvid::open(&recovery_path).unwrap();
        assert!(recovered.frame_by_uri("mv2://fault/first").is_ok());
        assert!(recovered.frame_by_uri("mv2://fault/second").is_ok());
        assert_eq!(
            recovered.search_vec(&[0.0, 1.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            1
        );
        recovered.commit().unwrap();
        drop(recovered);

        let mut reopened = Memvid::open_read_only(&recovery_path).unwrap();
        assert_eq!(reopened.stats().unwrap().frame_count, 2);
        assert_eq!(
            reopened.search_vec(&[0.0, 1.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            1
        );
    }

    #[test]
    fn staging_sync_failure_preserves_committed_file_and_disables_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("staging-sync-failure.mv2");
        let mut writer = Memvid::create(&path).unwrap();
        writer.enable_vec().unwrap();
        writer
            .put_with_embedding_and_options(
                b"seed",
                vec![1.0, 0.0, 0.0, 0.0],
                fault_options("seed"),
            )
            .unwrap();
        writer.commit().unwrap();
        writer
            .put_with_embedding_and_options(
                b"alpha",
                vec![0.0, 1.0, 0.0, 0.0],
                fault_options("alpha"),
            )
            .unwrap();

        FAIL_BEFORE_STAGING_SYNC.with(|flag| flag.set(true));
        let error = writer
            .commit()
            .expect_err("staging sync fault must be reported");
        assert!(error.to_string().contains("before staging sync"));
        assert!(writer.write_disabled.is_some());
        assert_eq!(writer.lock.mode(), LockMode::Exclusive);
        assert!(
            FileLock::try_acquire(&writer.file, &path)
                .unwrap()
                .is_none(),
            "original writer lock must remain physically held"
        );

        assert!(
            writer.commit().is_err(),
            "partially mutated staging state must not be retried on this handle"
        );
        assert_terminal_direct_writes_rejected(&mut writer);
        drop(writer);
        let committed = Memvid::open_read_only(&path).unwrap();
        assert!(committed.frame_by_uri("mv2://fault/seed").is_ok());
        assert!(committed.frame_by_uri("mv2://fault/alpha").is_err());
        drop(committed);

        let mut recovered = Memvid::open(&path).expect("writable reopen recovers pending WAL");
        let alpha = recovered.frame_by_uri("mv2://fault/alpha").unwrap();
        assert_eq!(
            recovered.frame_canonical_payload(alpha.id).unwrap(),
            b"alpha"
        );
        recovered.commit().unwrap();
        drop(recovered);

        let mut reopened = Memvid::open_read_only(&path).unwrap();
        let alpha = reopened.frame_by_uri("mv2://fault/alpha").unwrap();
        assert_eq!(
            reopened.frame_canonical_payload(alpha.id).unwrap(),
            b"alpha"
        );
        assert_eq!(
            reopened.search_vec(&[0.0, 1.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            alpha.id
        );
    }

    #[test]
    fn post_rename_error_keeps_new_inode_locked_and_disables_old_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("post-rename-failure.mv2");
        let mut writer = Memvid::create(&path).unwrap();
        writer.enable_vec().unwrap();
        writer
            .put_with_embedding_and_options(
                b"seed",
                vec![1.0, 0.0, 0.0, 0.0],
                fault_options("seed"),
            )
            .unwrap();
        writer.commit().unwrap();
        writer
            .put_with_embedding_and_options(
                b"alpha",
                vec![0.0, 1.0, 0.0, 0.0],
                fault_options("alpha"),
            )
            .unwrap();

        FAIL_AFTER_ATOMIC_PUBLISH.with(|flag| flag.set(true));
        let error = writer
            .commit()
            .expect_err("directory durability fault remains visible");
        assert!(error.to_string().contains("after atomic publish"));
        assert!(writer.write_disabled.is_some());
        assert_eq!(writer.lock.mode(), LockMode::Exclusive);
        assert!(writer.commit().is_err(), "retry on the handle is forbidden");
        assert_terminal_direct_writes_rejected(&mut writer);
        assert!(
            writer
                .put_with_embedding_and_options(
                    b"stale",
                    vec![0.0, 0.0, 1.0, 0.0],
                    fault_options("stale"),
                )
                .is_err(),
            "further mutations on the handle are forbidden"
        );

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter_path = path.clone();
        let waiter = std::thread::spawn(move || {
            let result = (|| -> Result<()> {
                let mut next = Memvid::open(&waiter_path)?;
                next.put_with_embedding_and_options(
                    b"beta",
                    vec![0.0, 0.0, 1.0, 0.0],
                    fault_options("beta"),
                )?;
                next.commit()
            })();
            done_tx.send(result).unwrap();
        });
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(150))
                .is_err(),
            "new destination inode must stay locked after the reported error"
        );

        drop(writer);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("waiter completed")
            .expect("waiter commit");
        waiter.join().unwrap();

        let mut reopened = Memvid::open_read_only(&path).unwrap();
        for (name, vector) in [
            ("seed", [1.0, 0.0, 0.0, 0.0]),
            ("alpha", [0.0, 1.0, 0.0, 0.0]),
            ("beta", [0.0, 0.0, 1.0, 0.0]),
        ] {
            let frame = reopened
                .frame_by_uri(&format!("mv2://fault/{name}"))
                .unwrap();
            assert_eq!(
                reopened.frame_canonical_payload(frame.id).unwrap(),
                name.as_bytes()
            );
            assert_eq!(
                reopened.search_vec(&vector, 1).unwrap()[0].frame_id,
                frame.id
            );
        }
        assert!(reopened.frame_by_uri("mv2://fault/stale").is_err());
    }

    #[test]
    fn failure_after_staging_truncate_keeps_committed_file_recoverable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncate-failure.mv2");
        let mut mem = Memvid::create(&path).unwrap();
        mem.enable_vec().unwrap();
        mem.put_with_embedding(b"first", vec![1.0, 0.0, 0.0, 0.0])
            .unwrap();
        mem.commit().unwrap();
        mem.put_with_embedding(b"second", vec![0.0, 1.0, 0.0, 0.0])
            .unwrap();

        // WAL appends happen before commit and are legitimately present in
        // the main file. Snapshot only after that append, then prove that the
        // destructive staging truncate cannot alter the committed inode.
        let file_with_pending_wal = std::fs::read(&path).unwrap();
        FAIL_AFTER_COMPACT_TRUNCATE.with(|flag| flag.set(true));
        let err = mem
            .commit()
            .expect_err("fault injection must abort after staging truncate");
        assert!(err.to_string().contains("after compact truncate"));
        assert_eq!(std::fs::read(&path).unwrap(), file_with_pending_wal);
        let recovery_path = dir.path().join("truncate-failure-recovery.mv2");
        std::fs::copy(&path, &recovery_path).unwrap();
        mem.dirty = false;
        drop(mem);

        let mut committed = Memvid::open_read_only(&recovery_path).unwrap();
        assert_eq!(committed.stats().unwrap().frame_count, 1);
        assert!(committed.frame_by_id(0).is_ok());
        assert!(committed.frame_by_id(1).is_err());
        assert_eq!(
            committed.search_vec(&[1.0, 0.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            0
        );
        drop(committed);

        let mut recovered = Memvid::open(&recovery_path).unwrap();
        assert_eq!(recovered.stats().unwrap().frame_count, 2);
        assert_eq!(
            recovered.search_vec(&[0.0, 1.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            1
        );
        assert!(recovered.frame_by_id(0).is_ok());
        assert!(recovered.frame_by_id(1).is_ok());
        drop(recovered);

        let mut reopened = Memvid::open_read_only(&recovery_path).unwrap();
        assert_eq!(reopened.stats().unwrap().frame_count, 2);
        assert_eq!(
            reopened.search_vec(&[0.0, 1.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            1
        );
    }

    #[cfg(not(feature = "replay"))]
    fn install_opaque_replay(mem: &mut Memvid, bytes: &[u8]) -> ReplayManifest {
        let segment_offset = mem.header.footer_offset;
        mem.file.seek(SeekFrom::Start(segment_offset)).unwrap();
        mem.file.write_all(bytes).unwrap();
        let manifest = ReplayManifest {
            segment_offset,
            segment_size: bytes.len() as u64,
            session_count: 1,
            total_actions: 1,
            version: crate::replay::REPLAY_SEGMENT_VERSION,
        };
        mem.toc.replay_manifest = Some(manifest.clone());
        mem.header.footer_offset = segment_offset + manifest.segment_size;
        mem.rewrite_toc_footer().unwrap();
        mem.header.toc_checksum = mem.toc.toc_checksum;
        crate::persist_header(&mut mem.file, &mem.header).unwrap();
        mem.file.sync_all().unwrap();
        manifest
    }

    #[cfg(not(feature = "replay"))]
    fn assert_opaque_replay(mem: &mut Memvid, expected: &ReplayManifest, bytes: &[u8]) {
        let actual = mem.toc.replay_manifest.as_ref().expect("replay manifest");
        assert_eq!(actual.segment_offset, expected.segment_offset);
        assert_eq!(actual.segment_size, expected.segment_size);
        let mut stored = vec![0; bytes.len()];
        mem.file
            .seek(SeekFrom::Start(actual.segment_offset))
            .unwrap();
        mem.file.read_exact(&mut stored).unwrap();
        assert_eq!(stored, bytes);
    }

    fn install_quantized_vector_fixture(path: &Path) -> Vec<Vec<f32>> {
        let vectors: Vec<Vec<f32>> = (0..8)
            .map(|index| {
                let mut vector = vec![0.0; 384];
                vector[index] = 1.0;
                vector
            })
            .collect();
        let mut mem = Memvid::create(path).unwrap();
        mem.enable_vec().unwrap();
        for index in 0..vectors.len() {
            mem.put_bytes_with_options(
                format!("quantized payload {index}").as_bytes(),
                PutOptions::builder()
                    .uri(format!("mv2://pq/{index}"))
                    .search_text(format!("quantized payload {index}"))
                    .auto_tag(false)
                    .extract_dates(false)
                    .extract_triplets(false)
                    .build(),
            )
            .unwrap();
        }
        mem.commit().unwrap();

        let mut builder = crate::vec_pq::QuantizedVecIndexBuilder::new();
        builder.train_quantizer(&vectors, 384).unwrap();
        for (frame_id, vector) in vectors.iter().enumerate() {
            builder
                .add_document(frame_id as u64, vector.clone())
                .unwrap();
        }
        let artifact = builder.finish().unwrap();
        let offset = mem.header.footer_offset;
        mem.file.seek(SeekFrom::Start(offset)).unwrap();
        mem.file.write_all(&artifact.bytes).unwrap();
        mem.toc.indexes.vec = Some(VecIndexManifest {
            bytes_offset: offset,
            bytes_length: artifact.bytes.len() as u64,
            vector_count: artifact.vector_count,
            dimension: artifact.dimension,
            checksum: artifact.checksum,
            compression_mode: crate::VectorCompression::Pq96,
            model: None,
        });
        mem.header.footer_offset = offset + artifact.bytes.len() as u64;
        mem.rewrite_toc_footer().unwrap();
        mem.header.toc_checksum = mem.toc.toc_checksum;
        crate::persist_header(&mut mem.file, &mem.header).unwrap();
        mem.file.sync_all().unwrap();
        mem.dirty = false;
        drop(mem);
        vectors
    }

    #[cfg(feature = "parallel_segments")]
    fn install_large_quantized_layout(path: &Path, segment_backed: bool) -> Vec<Vec<f32>> {
        let vectors: Vec<Vec<f32>> = (0..8)
            .map(|index| {
                let mut vector = vec![0.0; 384];
                vector[index] = 1.0;
                vector
            })
            .collect();
        let mut mem = Memvid::create(path).unwrap();
        mem.enable_vec().unwrap();
        mem.begin_batch(PutManyOpts {
            wal_pre_size_bytes: 2 * 1024 * 1024,
            disable_auto_checkpoint: true,
            skip_sync: true,
            enable_enrichment: false,
            ..Default::default()
        })
        .unwrap();
        for frame_id in 0..512_u64 {
            mem.put_bytes_with_options(
                format!("large quantized payload {frame_id}").as_bytes(),
                PutOptions::builder()
                    .uri(format!("mv2://pq-large/{frame_id}"))
                    .search_text(format!("large quantized document {frame_id}"))
                    .auto_tag(false)
                    .extract_dates(false)
                    .extract_triplets(false)
                    .instant_index(false)
                    .extraction_budget_ms(0)
                    .build(),
            )
            .unwrap();
        }
        mem.end_batch().unwrap();
        mem.commit().unwrap();

        let mut builder = crate::vec_pq::QuantizedVecIndexBuilder::new();
        builder.train_quantizer(&vectors, 384).unwrap();
        for frame_id in 0..512_u64 {
            builder
                .add_document(frame_id, vectors[frame_id as usize % vectors.len()].clone())
                .unwrap();
        }
        let artifact = builder.finish().unwrap();
        let offset = mem.header.footer_offset;
        let vector_end = if segment_backed {
            mem.data_end = offset;
            let segment = crate::memvid::segments::VecSegmentArtifact {
                bytes: artifact.bytes.clone(),
                vector_count: artifact.vector_count,
                dimension: artifact.dimension,
                checksum: artifact.checksum,
                compression: crate::VectorCompression::Pq96,
                bytes_uncompressed: 512 * 384 * 4,
            };
            let descriptor = mem.append_vec_segment(&segment, 1).unwrap();
            let end = descriptor.common.bytes_offset + descriptor.common.bytes_length;
            mem.toc.segment_catalog.vec_segments = vec![descriptor];
            mem.toc.indexes.vec = Some(VecIndexManifest {
                bytes_offset: 0,
                bytes_length: 0,
                vector_count: 512,
                dimension: 384,
                checksum: [0; 32],
                compression_mode: crate::VectorCompression::Pq96,
                model: None,
            });
            end
        } else {
            mem.file.seek(SeekFrom::Start(offset)).unwrap();
            mem.file.write_all(&artifact.bytes).unwrap();
            mem.toc.indexes.vec = Some(VecIndexManifest {
                bytes_offset: offset,
                bytes_length: artifact.bytes.len() as u64,
                vector_count: artifact.vector_count,
                dimension: artifact.dimension,
                checksum: artifact.checksum,
                compression_mode: crate::VectorCompression::Pq96,
                model: None,
            });
            offset + artifact.bytes.len() as u64
        };
        mem.header.footer_offset = vector_end;
        mem.insert_sketch(
            0,
            "persistent exact sketch after quantized vectors",
            SketchVariant::Small,
        );
        mem.persist_sketch_track().unwrap();
        mem.rewrite_toc_footer().unwrap();
        mem.header.toc_checksum = mem.toc.toc_checksum;
        crate::persist_header(&mut mem.file, &mem.header).unwrap();
        mem.file.sync_all().unwrap();
        mem.dirty = false;
        drop(mem);
        vectors
    }

    #[cfg(feature = "parallel_segments")]
    fn assert_large_quantized_layout(
        mem: &mut Memvid,
        vectors: &[Vec<f32>],
        expected_sketch: Option<&crate::types::SketchEntry>,
    ) {
        assert_eq!(mem.stats().unwrap().frame_count, 512);
        assert_eq!(mem.stats().unwrap().vector_count, 512);
        for (frame_id, payload) in [
            (0, "large quantized payload 0"),
            (255, "large quantized payload 255"),
            (511, "large quantized payload 511"),
        ] {
            assert_eq!(
                mem.frame_canonical_payload(frame_id).unwrap(),
                payload.as_bytes()
            );
        }
        for (class, vector) in vectors.iter().enumerate() {
            let exact_ids: BTreeSet<_> = mem
                .search_vec(vector, 512)
                .unwrap()
                .into_iter()
                .filter(|hit| hit.distance == 0.0)
                .map(|hit| hit.frame_id)
                .collect();
            let expected: BTreeSet<_> = (class as u64..512).step_by(vectors.len()).collect();
            assert_eq!(exact_ids, expected);
        }
        assert_eq!(mem.sketches().get(0), expected_sketch);
    }

    #[cfg(feature = "parallel_segments")]
    #[test]
    fn finalize_preserves_sketch_when_quantized_vectors_expand() {
        let dir = tempfile::tempdir().unwrap();
        for segment_backed in [false, true] {
            let base = dir
                .path()
                .join(format!("pq-layout-base-{segment_backed}.mv2"));
            let vectors = install_large_quantized_layout(&base, segment_backed);
            let before = Memvid::open_read_only(&base).unwrap();
            let sketch = before.sketches().get(0).unwrap().clone();
            drop(before);

            let preserve = dir
                .path()
                .join(format!("pq-layout-preserve-{segment_backed}.mv2"));
            std::fs::copy(&base, &preserve).unwrap();
            let mut mem = Memvid::open(&preserve).unwrap();
            mem.finalize_indexes().unwrap();
            assert_large_quantized_layout(&mut mem, &vectors, Some(&sketch));
            let first_size = std::fs::metadata(&preserve).unwrap().len();
            mem.finalize_indexes().unwrap();
            assert_large_quantized_layout(&mut mem, &vectors, Some(&sketch));
            let second_size = std::fs::metadata(&preserve).unwrap().len();
            assert_eq!(second_size, first_size);
            drop(mem);
            let mut reopened = Memvid::open_read_only(&preserve).unwrap();
            assert_large_quantized_layout(&mut reopened, &vectors, Some(&sketch));

            let empty = dir
                .path()
                .join(format!("pq-layout-empty-{segment_backed}.mv2"));
            std::fs::copy(&base, &empty).unwrap();
            let mut mem = Memvid::open(&empty).unwrap();
            *mem.sketches_mut() = crate::SketchTrack::default();
            mem.finalize_indexes().unwrap();
            // finalize persisted the requested empty track, but sketches_mut leaves the generic
            // dirty flag set. Clear it before the explicit lock downgrade just as this test did
            // before Drop below.
            mem.dirty = false;
            mem.downgrade_to_shared().unwrap();
            let mut finalized = Memvid::open_read_only(&empty).unwrap();
            assert_large_quantized_layout(&mut finalized, &vectors, None);
            assert!(finalized.toc.sketch_track.is_none());
            drop(finalized);
            // Isolate finalize itself from Drop's normal dirty retry.
            drop(mem);
            let mut reopened = Memvid::open_read_only(&empty).unwrap();
            assert_large_quantized_layout(&mut reopened, &vectors, None);
            assert!(reopened.toc.sketch_track.is_none());
        }
    }

    #[test]
    fn quantized_vectors_survive_sketch_only_compact_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pq-sketch-rebuild.mv2");
        let vectors = install_quantized_vector_fixture(&path);
        let mut mem = Memvid::open(&path).unwrap();
        let before = mem.search_vec(&vectors[4], 1).unwrap();
        assert_eq!(before[0].frame_id, 4);
        assert_eq!(before[0].distance, 0.0);
        mem.insert_sketch(4, "quantized sketch update", SketchVariant::Small);
        mem.commit().unwrap();
        let after = mem.search_vec(&vectors[4], 1).unwrap();
        assert_eq!(after[0].frame_id, 4);
        assert_eq!(after[0].distance, 0.0);
        drop(mem);

        let mut reopened = Memvid::open_read_only(&path).unwrap();
        let hit = reopened.search_vec(&vectors[4], 1).unwrap()[0].clone();
        assert_eq!(hit.frame_id, 4);
        assert_eq!(hit.distance, 0.0);
        assert_eq!(reopened.stats().unwrap().vector_count, 8);
    }

    #[test]
    fn pending_replacement_overrides_quantized_base_without_requantizing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pq-pending-replacement.mv2");
        let vectors = install_quantized_vector_fixture(&path);
        let mut replacement = vec![0.0; 384];
        replacement[20] = 1.0;

        let mut mem = Memvid::open(&path).unwrap();
        mem.add_embeddings(vec![(4, replacement.clone())]).unwrap();
        mem.commit().unwrap();
        let replaced = mem.search_vec(&replacement, 1).unwrap()[0].clone();
        assert_eq!(replaced.frame_id, 4);
        assert_eq!(replaced.distance, 0.0);
        let untouched = mem.search_vec(&vectors[5], 1).unwrap()[0].clone();
        assert_eq!(untouched.frame_id, 5);
        assert_eq!(untouched.distance, 0.0);
        drop(mem);

        let mut reopened = Memvid::open_read_only(&path).unwrap();
        let replaced = reopened.search_vec(&replacement, 1).unwrap()[0].clone();
        assert_eq!(replaced.frame_id, 4);
        assert_eq!(replaced.distance, 0.0);
        let untouched = reopened.search_vec(&vectors[5], 1).unwrap()[0].clone();
        assert_eq!(untouched.frame_id, 5);
        assert_eq!(untouched.distance, 0.0);
    }

    #[test]
    fn malformed_monolithic_vectors_fail_before_staging_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pq-invalid-monolithic.mv2");
        let mut mem = Memvid::create(&path).unwrap();
        mem.enable_vec().unwrap();
        mem.put_bytes(b"committed malformed-vector payload")
            .unwrap();
        mem.commit().unwrap();

        let malformed = crate::vec_pq::malformed_codebook_artifact();
        let offset = mem.header.footer_offset;
        mem.file.seek(SeekFrom::Start(offset)).unwrap();
        mem.file.write_all(&malformed.bytes).unwrap();
        mem.toc.indexes.vec = Some(VecIndexManifest {
            bytes_offset: offset,
            bytes_length: malformed.bytes.len() as u64,
            vector_count: malformed.vector_count,
            dimension: malformed.dimension,
            checksum: malformed.checksum,
            compression_mode: crate::VectorCompression::Pq96,
            model: None,
        });
        mem.header.footer_offset = offset + malformed.bytes.len() as u64;
        mem.rewrite_toc_footer().unwrap();
        mem.header.toc_checksum = mem.toc.toc_checksum;
        crate::persist_header(&mut mem.file, &mem.header).unwrap();
        mem.file.sync_all().unwrap();
        mem.dirty = false;
        drop(mem);

        let committed = std::fs::read(&path).unwrap();
        let mut writer = Memvid::open(&path).unwrap();
        writer.insert_sketch(0, "must not reach staging truncate", SketchVariant::Small);
        FAIL_AFTER_COMPACT_TRUNCATE.with(|flag| flag.set(true));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| writer.commit()));
        FAIL_AFTER_COMPACT_TRUNCATE.with(|flag| flag.set(false));
        assert!(result.is_ok(), "malformed monolithic commit panicked");
        let err = result.unwrap().expect_err("invalid geometry must abort");
        assert!(matches!(err, MemvidError::InvalidToc { .. }));
        assert!(!err.to_string().contains("after compact truncate"));
        assert_eq!(std::fs::read(&path).unwrap(), committed);
        writer.dirty = false;
        drop(writer);

        let mut reopened = Memvid::open_read_only(&path).unwrap();
        assert_eq!(
            reopened.frame_canonical_payload(0).unwrap(),
            b"committed malformed-vector payload"
        );
    }

    #[test]
    #[cfg(not(feature = "replay"))]
    fn build_without_replay_feature_preserves_opaque_replay_on_add_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("opaque-replay.mv2");
        let replay_bytes = b"MV2RPLY!opaque replay bytes retained without API feature";
        let mut mem = Memvid::create(&path).unwrap();
        mem.enable_vec().unwrap();
        mem.put_with_embedding_and_options(
            b"first replay-adjacent document",
            vec![1.0, 0.0, 0.0, 0.0],
            PutOptions::builder()
                .uri("mv2://opaque/first")
                .search_text("first replay-adjacent document")
                .build(),
        )
        .unwrap();
        mem.commit().unwrap();
        let manifest = install_opaque_replay(&mut mem, replay_bytes);
        drop(mem);

        let mut mem = Memvid::open(&path).unwrap();
        assert_opaque_replay(&mut mem, &manifest, replay_bytes);
        mem.put_with_embedding_and_options(
            b"second replay-adjacent document",
            vec![0.0, 1.0, 0.0, 0.0],
            PutOptions::builder()
                .uri("mv2://opaque/second")
                .search_text("second replay-adjacent document")
                .build(),
        )
        .unwrap();
        mem.commit().unwrap();
        assert_opaque_replay(&mut mem, &manifest, replay_bytes);
        mem.delete_frame(0).unwrap();
        mem.commit().unwrap();
        assert_opaque_replay(&mut mem, &manifest, replay_bytes);
        drop(mem);

        let mut reopened = Memvid::open(&path).unwrap();
        assert_opaque_replay(&mut reopened, &manifest, replay_bytes);
        assert_eq!(
            reopened.frame_by_id(0).unwrap().status,
            FrameStatus::Deleted
        );
        assert!(reopened.frame_by_uri("mv2://opaque/second").is_ok());
        assert_eq!(
            reopened.search_vec(&[0.0, 1.0, 0.0, 0.0], 1).unwrap()[0].frame_id,
            1
        );
    }

    #[cfg(feature = "lex")]
    #[test]
    fn next_atomic_commit_reclaims_layout_emitted_by_legacy_noncompact_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-layout.mv2");
        let mut mem = Memvid::create(&path).unwrap();
        mem.enable_vec().unwrap();
        for i in 0..8 {
            let text = format!("legacy writer document {i}");
            mem.put_with_embedding_and_options(
                text.as_bytes(),
                vec![i as f32 + 1.0; 256],
                PutOptions::builder()
                    .uri(format!("mv2://legacy/{i}"))
                    .search_text(text.clone())
                    .build(),
            )
            .unwrap();
        }
        mem.commit().unwrap();
        let compact_size = std::fs::metadata(&path).unwrap().len();

        // Reproduce the old writer's placement policy: preserve the previous
        // footer boundary, write another complete vector snapshot after it,
        // then append another complete sketch snapshot and footer.
        for _ in 0..6 {
            mem.rebuild_indexes(&[], &[], false).unwrap();
            mem.persist_sketch_track().unwrap();
            mem.rewrite_toc_footer().unwrap();
            mem.header.toc_checksum = mem.toc.toc_checksum;
            crate::persist_header(&mut mem.file, &mem.header).unwrap();
            mem.file.sync_all().unwrap();
        }
        let legacy_size = std::fs::metadata(&path).unwrap().len();
        assert!(legacy_size > compact_size + 40_000);
        drop(mem);

        let mut upgraded = Memvid::open(&path).unwrap();
        upgraded
            .put_with_embedding_and_options(
                b"legacy writer upgrade document",
                vec![0.5; 256],
                PutOptions::builder()
                    .uri("mv2://legacy/upgrade")
                    .search_text("legacy writer upgrade document")
                    .build(),
            )
            .unwrap();
        upgraded.commit().unwrap();
        let upgraded_size = std::fs::metadata(&path).unwrap().len();
        assert!(upgraded_size < legacy_size - 30_000);
        println!(
            "legacy layout sizes: compact={compact_size}, legacy={legacy_size}, upgraded={upgraded_size}"
        );
        drop(upgraded);

        let mut reopened = Memvid::open(&path).unwrap();
        assert_eq!(reopened.stats().unwrap().frame_count, 9);
        assert!(reopened.frame_by_uri("mv2://legacy/upgrade").is_ok());
        assert!(!reopened.search_vec(&[0.5; 256], 1).unwrap().is_empty());
        assert!(reopened.sketches().get(8).is_some());
    }
}
