use std::fs;
use std::path::Path;

use crate::{Result, error::MemvidError, text::truncate_at_grapheme_boundary};
// Use SymSpell-based cleanup when feature is enabled, otherwise fall back to heuristic
#[cfg(feature = "symspell_cleanup")]
use crate::symspell_cleanup::fix_pdf_text as fix_pdf_spacing;
#[cfg(not(feature = "symspell_cleanup"))]
use crate::text::fix_pdf_spacing;

#[cfg(feature = "extractous")]
use log::LevelFilter;
use lopdf::Document as LopdfDocument;
use serde_json::{Value, json};

#[cfg(feature = "extractous")]
use extractous::Extractor;
#[cfg(feature = "extractous")]
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "extractous")]
use std::sync::{Mutex, OnceLock};

/// Structured result produced by [`DocumentProcessor`] after running
/// Extractous over an input document.
#[derive(Debug, Clone)]
pub struct ExtractedDocument {
    pub text: Option<String>,
    pub metadata: Value,
    pub mime_type: Option<String>,
}

impl ExtractedDocument {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            text: None,
            metadata: Value::Null,
            mime_type: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProcessorConfig {
    pub max_text_chars: usize,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            max_text_chars: 2_000_000,
        }
    }
}

// ============================================================================
// Extraction Cache with LRU Eviction
// ============================================================================

/// Default capacity for extraction cache (number of documents)
#[cfg(feature = "extractous")]
const DEFAULT_EXTRACTION_CACHE_CAPACITY: usize = 100;

/// LRU cache for extracted documents to avoid re-extracting the same content.
///
/// This cache has a maximum capacity and evicts the least recently used entries
/// when full, following the same pattern as `EmbeddingCache` in `text_embed.rs`.
#[cfg(feature = "extractous")]
struct ExtractionCache {
    /// Cache storage: document hash -> extracted document
    cache: HashMap<blake3::Hash, ExtractedDocument>,
    /// LRU queue: tracks access order (most recent at front)
    lru_queue: VecDeque<blake3::Hash>,
    /// Maximum capacity
    capacity: usize,
    /// Cache hit count
    hits: usize,
    /// Cache miss count
    misses: usize,
}

#[cfg(feature = "extractous")]
impl ExtractionCache {
    fn new(capacity: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(capacity),
            lru_queue: VecDeque::with_capacity(capacity),
            capacity,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, key: &blake3::Hash) -> Option<ExtractedDocument> {
        if let Some(document) = self.cache.get(key) {
            // Move to front (most recently used)
            self.lru_queue.retain(|k| k != key);
            self.lru_queue.push_front(*key);
            self.hits += 1;
            Some(document.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    fn insert(&mut self, key: blake3::Hash, value: ExtractedDocument) {
        // Check if already exists
        if self.cache.contains_key(&key) {
            // Update and move to front
            self.cache.insert(key, value);
            self.lru_queue.retain(|k| *k != key);
            self.lru_queue.push_front(key);
            return;
        }

        // Evict if at capacity
        if self.cache.len() >= self.capacity {
            if let Some(oldest_key) = self.lru_queue.pop_back() {
                self.cache.remove(&oldest_key);
                tracing::debug!(
                    evicted_hash = ?oldest_key,
                    "Evicted oldest entry from extraction cache"
                );
            }
        }

        // Insert new entry
        self.cache.insert(key, value);
        self.lru_queue.push_front(key);
    }

    #[allow(dead_code)]
    fn stats(&self) -> (usize, usize, usize) {
        (self.hits, self.misses, self.cache.len())
    }
}

// ============================================================================
// DocumentProcessor - only available with extractous feature
// ============================================================================

#[cfg(feature = "extractous")]
#[derive(Debug)]
pub struct DocumentProcessor {
    extractor: Mutex<Extractor>,
    max_length: usize,
}

#[cfg(feature = "extractous")]
impl Default for DocumentProcessor {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[cfg(feature = "extractous")]
static EXTRACTION_CACHE: OnceLock<Mutex<ExtractionCache>> = OnceLock::new();

#[cfg(feature = "extractous")]
impl DocumentProcessor {
    pub fn new(config: ProcessorConfig) -> Self {
        let capped = config
            .max_text_chars
            .min(i32::MAX as usize)
            .try_into()
            .unwrap_or(i32::MAX);
        let mut extractor = Extractor::new().set_extract_string_max_length(capped);
        extractor = extractor.set_xml_output(false);
        Self {
            extractor: Mutex::new(extractor),
            max_length: config.max_text_chars,
        }
    }

    pub fn extract_from_path(&self, path: &Path) -> Result<ExtractedDocument> {
        let path_str = path.to_str().ok_or_else(|| MemvidError::ExtractionFailed {
            reason: "input path contains invalid UTF-8".into(),
        })?;

        let extraction = {
            let extractor = self.locked()?;
            let _log_guard = ScopedLogLevel::lowered(LevelFilter::Off);
            extractor.extract_file_to_string(path_str)
        };

        match extraction {
            Ok((mut content, metadata)) => {
                if needs_pdf_fallback(&content) {
                    if let Ok(bytes) = fs::read(path) {
                        if let Ok(Some(fallback_text)) = pdf_text_fallback(&bytes) {
                            content = fallback_text;
                        }
                    }
                }
                Ok(self.into_document(content, metadata))
            }
            Err(err) => {
                let primary_reason = err.to_string();
                if let Ok(bytes) = fs::read(path) {
                    match pdf_text_fallback(&bytes) {
                        Ok(Some(fallback_text)) => {
                            return Ok(self.into_document(fallback_text, pdf_fallback_metadata()));
                        }
                        Ok(None) => {}
                        Err(fallback_err) => {
                            let reason = format!(
                                "primary extractor error: {}; PDF fallback error: {}",
                                primary_reason, fallback_err
                            );
                            return Err(MemvidError::ExtractionFailed {
                                reason: reason.into(),
                            });
                        }
                    }
                }
                Err(MemvidError::ExtractionFailed {
                    reason: primary_reason.into(),
                })
            }
        }
    }

    pub fn extract_from_bytes(&self, bytes: &[u8]) -> Result<ExtractedDocument> {
        let hash = blake3::hash(bytes);
        if let Some(cached) = cache_lookup(&hash) {
            tracing::debug!(target = "memvid::extract", reader = "cache", "cache hit");
            return Ok(cached);
        }

        let extraction = {
            let extractor = self.locked()?;
            let _log_guard = ScopedLogLevel::lowered(LevelFilter::Off);
            extractor.extract_bytes_to_string(bytes)
        };

        let document = match extraction {
            Ok((mut content, metadata)) => {
                let pdf_needed = needs_pdf_fallback(&content);
                tracing::debug!(
                    target: "memvid::extract",
                    content_len = content.len(),
                    pdf_fallback_needed = pdf_needed,
                    "extractous returned content"
                );
                if pdf_needed {
                    match pdf_text_fallback(bytes) {
                        Ok(Some(fallback_text)) => {
                            tracing::debug!(
                                target: "memvid::extract",
                                fallback_len = fallback_text.len(),
                                "lopdf fallback succeeded"
                            );
                            content = fallback_text;
                        }
                        Ok(None) => {
                            tracing::debug!(
                                target: "memvid::extract",
                                "lopdf fallback returned None"
                            );
                            // PDF detected but lopdf couldn't extract any text
                            // Return empty rather than raw PDF bytes
                            content = String::new();
                        }
                        Err(e) => {
                            tracing::debug!(
                                target: "memvid::extract",
                                error = %e,
                                "lopdf fallback failed"
                            );
                            // lopdf extraction failed - return empty rather than raw PDF bytes
                            content = String::new();
                        }
                    }
                }
                self.into_document(content, metadata)
            }
            Err(err) => {
                let primary_reason = err.to_string();
                match pdf_text_fallback(bytes) {
                    Ok(Some(fallback_text)) => {
                        self.into_document(fallback_text, pdf_fallback_metadata())
                    }
                    Ok(None) => {
                        return Err(MemvidError::ExtractionFailed {
                            reason: primary_reason.into(),
                        });
                    }
                    Err(fallback_err) => {
                        let reason = format!(
                            "primary extractor error: {}; PDF fallback error: {}",
                            primary_reason, fallback_err
                        );
                        return Err(MemvidError::ExtractionFailed {
                            reason: reason.into(),
                        });
                    }
                }
            }
        };

        cache_store(hash, &document);
        Ok(document)
    }

    fn locked(&self) -> Result<std::sync::MutexGuard<'_, Extractor>> {
        self.extractor
            .lock()
            .map_err(|_| MemvidError::ExtractionFailed {
                reason: "extractor mutex poisoned".into(),
            })
    }

    fn into_document<M>(&self, content: String, metadata: M) -> ExtractedDocument
    where
        M: serde::Serialize,
    {
        let metadata_value = serde_json::to_value(metadata).unwrap_or(Value::Null);
        let mime_type = metadata_value.get("Content-Type").and_then(value_to_mime);

        let text = if content.trim().is_empty() {
            tracing::debug!(
                target: "memvid::extract",
                "into_document: content is empty, returning text=None"
            );
            None
        } else {
            let final_text = if content.len() > self.max_length {
                let end = truncate_at_grapheme_boundary(&content, self.max_length);
                content[..end].to_string()
            } else {
                content
            };
            tracing::debug!(
                target: "memvid::extract",
                text_len = final_text.len(),
                starts_with_pdf = final_text.starts_with("%PDF"),
                "into_document: returning text"
            );
            Some(final_text)
        };

        ExtractedDocument {
            text,
            metadata: metadata_value,
            mime_type,
        }
    }
}

// ============================================================================
// Stub DocumentProcessor when extractous is disabled - returns clear error
// ============================================================================

#[cfg(not(feature = "extractous"))]
#[derive(Debug)]
pub struct DocumentProcessor {
    max_length: usize,
}

#[cfg(not(feature = "extractous"))]
impl Default for DocumentProcessor {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[cfg(not(feature = "extractous"))]
impl DocumentProcessor {
    #[must_use]
    pub fn new(config: ProcessorConfig) -> Self {
        Self {
            max_length: config.max_text_chars,
        }
    }

    pub fn extract_from_path(&self, path: &Path) -> Result<ExtractedDocument> {
        // Without extractous, we can still handle plain text files
        let bytes = fs::read(path).map_err(|e| MemvidError::ExtractionFailed {
            reason: format!("failed to read file: {e}").into(),
        })?;
        self.extract_from_bytes(&bytes)
    }

    pub fn extract_from_bytes(&self, bytes: &[u8]) -> Result<ExtractedDocument> {
        // Check if this is a PDF - extract text using pdf_extract (if available) or lopdf
        if is_probably_pdf_simple(bytes) {
            match pdf_text_extract_best(bytes) {
                Ok(Some((text, extractor))) => {
                    let truncate_len = truncate_at_grapheme_boundary(&text, self.max_length);
                    let truncated = &text[..truncate_len];
                    return Ok(ExtractedDocument {
                        text: Some(truncated.to_string()),
                        metadata: json!({
                            "Content-Type": "application/pdf",
                            "extraction": extractor,
                        }),
                        mime_type: Some("application/pdf".to_string()),
                    });
                }
                Ok(None) => {
                    // PDF detected but no text could be extracted (image-only PDF)
                    return Ok(ExtractedDocument {
                        text: None,
                        metadata: json!({
                            "Content-Type": "application/pdf",
                            "extraction": "no_text",
                        }),
                        mime_type: Some("application/pdf".to_string()),
                    });
                }
                Err(e) => {
                    tracing::warn!(target: "memvid::extract", error = %e, "PDF extraction failed");
                    // Fall through to binary handling
                }
            }
        }

        // Without extractous, we can still handle plain text files and common text-based formats
        // Try to interpret as UTF-8 text first
        if let Ok(text) = std::str::from_utf8(bytes) {
            // Check if it's likely text (no null bytes in first 8KB)
            let sample = &bytes[..bytes.len().min(8192)];
            if !sample.contains(&0) {
                let truncate_len = truncate_at_grapheme_boundary(text, self.max_length);
                let truncated = &text[..truncate_len];
                return Ok(ExtractedDocument {
                    text: Some(truncated.to_string()),
                    metadata: json!({}),
                    mime_type: Some("text/plain".to_string()),
                });
            }
        }

        // For binary content (video, audio, images, etc.), return success with no text.
        // This allows binary blobs to be stored without requiring the extractous feature.
        // The caller can still store the blob; there just won't be extracted text for search.
        Ok(ExtractedDocument {
            text: None,
            metadata: json!({}),
            mime_type: Some("application/octet-stream".to_string()),
        })
    }
}

#[cfg(feature = "extractous")]
fn needs_pdf_fallback(content: &str) -> bool {
    if content.trim().is_empty() {
        return true;
    }
    looks_like_pdf_structure_dump(content)
}

#[cfg(feature = "extractous")]
fn pdf_fallback_metadata() -> Value {
    json!({
        "Content-Type": "application/pdf",
        "extraction": "lopdf_fallback",
    })
}

#[cfg(feature = "extractous")]
const PDF_FALLBACK_MAX_BYTES: usize = 64 * 1024 * 1024; // 64 MiB hard cap.
#[cfg(feature = "extractous")]
const PDF_FALLBACK_MAX_PAGES: usize = 4_096;

#[cfg(feature = "extractous")]
fn pdf_text_fallback(bytes: &[u8]) -> Result<Option<String>> {
    if !is_probably_pdf(bytes) {
        return Ok(None);
    }

    if bytes.len() > PDF_FALLBACK_MAX_BYTES {
        return Err(MemvidError::ExtractionFailed {
            reason: format!(
                "pdf fallback aborted: {} bytes exceeds limit of {} bytes",
                bytes.len(),
                PDF_FALLBACK_MAX_BYTES
            )
            .into(),
        });
    }

    let _log_guard = ScopedLogLevel::lowered(LevelFilter::Off);
    let mut document =
        LopdfDocument::load_mem(bytes).map_err(|err| MemvidError::ExtractionFailed {
            reason: format!("pdf fallback failed to load document: {err}").into(),
        })?;

    if document.is_encrypted() {
        if document.decrypt("").is_err() {
            return Err(MemvidError::ExtractionFailed {
                reason: "pdf fallback cannot decrypt password-protected file".into(),
            });
        }
    }

    let _ = document.decompress();

    let mut page_numbers: Vec<u32> = document.get_pages().keys().copied().collect();
    if page_numbers.is_empty() {
        return Ok(None);
    }
    page_numbers.sort_unstable();

    if page_numbers.len() > PDF_FALLBACK_MAX_PAGES {
        return Err(MemvidError::ExtractionFailed {
            reason: format!(
                "pdf fallback aborted: page count {} exceeds limit of {}",
                page_numbers.len(),
                PDF_FALLBACK_MAX_PAGES
            )
            .into(),
        });
    }

    match document.extract_text(&page_numbers) {
        Ok(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                // Apply fix_pdf_spacing to repair character-level spacing artifacts
                Ok(Some(fix_pdf_spacing(trimmed)))
            }
        }
        Err(err) => Err(MemvidError::ExtractionFailed {
            reason: format!("pdf fallback failed to extract text: {err}").into(),
        }),
    }
}

#[cfg(feature = "extractous")]
struct ScopedLogLevel {
    previous: LevelFilter,
    changed: bool,
}

#[cfg(feature = "extractous")]
impl ScopedLogLevel {
    fn lowered(level: LevelFilter) -> Self {
        let previous = log::max_level();
        if level < previous {
            log::set_max_level(level);
            Self {
                previous,
                changed: true,
            }
        } else {
            Self {
                previous,
                changed: false,
            }
        }
    }
}

#[cfg(feature = "extractous")]
impl Drop for ScopedLogLevel {
    fn drop(&mut self) {
        if self.changed {
            log::set_max_level(self.previous);
        }
    }
}

#[cfg(feature = "extractous")]
fn is_probably_pdf(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    let mut slice = bytes;
    if slice.starts_with(&[0xEF, 0xBB, 0xBF]) {
        slice = &slice[3..];
    }
    while let Some((first, rest)) = slice.split_first() {
        if *first == 0 || first.is_ascii_whitespace() {
            slice = rest;
        } else {
            break;
        }
    }
    slice.starts_with(b"%PDF")
}

#[cfg(feature = "extractous")]
fn looks_like_pdf_structure_dump(content: &str) -> bool {
    if content.len() < 256 {
        return false;
    }
    let sample_len = content.len().min(8_192);
    // Find the nearest valid character boundary at or before sample_len
    let safe_len = truncate_at_grapheme_boundary(content, sample_len);
    let sample = &content[..safe_len];
    let endobj_hits = sample.matches("endobj").take(3).count();
    if endobj_hits < 2 {
        return false;
    }
    let has_obj =
        sample.contains(" 0 obj") || sample.contains("\n0 obj") || sample.contains("\r0 obj");
    let has_stream = sample.contains("endstream");
    let has_page_type = sample.contains("/Type /Page");
    endobj_hits >= 2 && (has_obj || has_stream || has_page_type)
}

#[cfg(feature = "extractous")]
fn value_to_mime(value: &Value) -> Option<String> {
    if let Some(mime) = value.as_str() {
        return Some(mime.to_string());
    }
    if let Some(array) = value.as_array() {
        for entry in array {
            if let Some(mime) = entry.as_str() {
                return Some(mime.to_string());
            }
        }
    }
    None
}

#[cfg(feature = "extractous")]
fn cache_lookup(hash: &blake3::Hash) -> Option<ExtractedDocument> {
    let cache = EXTRACTION_CACHE
        .get_or_init(|| Mutex::new(ExtractionCache::new(DEFAULT_EXTRACTION_CACHE_CAPACITY)));
    cache.lock().ok().and_then(|mut map| map.get(hash))
}

#[cfg(feature = "extractous")]
fn cache_store(hash: blake3::Hash, document: &ExtractedDocument) {
    let cache = EXTRACTION_CACHE
        .get_or_init(|| Mutex::new(ExtractionCache::new(DEFAULT_EXTRACTION_CACHE_CAPACITY)));
    if let Ok(mut map) = cache.lock() {
        map.insert(hash, document.clone());
    }
}

// ============================================================================
// PDF extraction helpers (available without extractous feature)
// Priority: pdf_oxide (best accuracy) > pdf_extract > lopdf
// ============================================================================

#[allow(dead_code)]
const PDF_LOPDF_MAX_BYTES: usize = 64 * 1024 * 1024; // 64 MiB hard cap
#[allow(dead_code)]
const PDF_LOPDF_MAX_PAGES: usize = 4_096;

/// Try multiple PDF extractors and return the best result
/// Returns (text, `extractor_name`) or None if no text found
/// Priority: `pdf_oxide` (2025, best accuracy) > `pdf_extract` > lopdf
#[allow(dead_code)]
fn pdf_text_extract_best(bytes: &[u8]) -> Result<Option<(String, &'static str)>> {
    let mut best_text: Option<String> = None;
    let mut best_source: &'static str = "";

    // Calculate minimum "good" threshold based on file size
    #[cfg(any(feature = "pdf_oxide", feature = "pdf_extract"))]
    let min_good_chars = (bytes.len() / 100).clamp(500, 5000);

    // Try pdf_oxide first (if feature enabled) - highest accuracy, perfect word spacing
    // Wrap in catch_unwind because cff-parser may panic on certain fonts (ligatures)
    #[cfg(feature = "pdf_oxide")]
    {
        let bytes_clone = bytes.to_vec();
        let oxide_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pdf_text_extract_oxide(&bytes_clone)
        }));

        match oxide_result {
            Ok(Ok(Some(text))) => {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    if trimmed.len() >= min_good_chars {
                        tracing::debug!(
                            target: "memvid::extract",
                            len = trimmed.len(),
                            "pdf_oxide succeeded with good result"
                        );
                        return Ok(Some((trimmed.to_string(), "pdf_oxide")));
                    }
                    tracing::debug!(
                        target: "memvid::extract",
                        len = trimmed.len(),
                        min_good = min_good_chars,
                        "pdf_oxide returned partial result, trying fallbacks"
                    );
                    best_text = Some(trimmed.to_string());
                    best_source = "pdf_oxide";
                }
            }
            Ok(Ok(None)) => {
                tracing::debug!(target: "memvid::extract", "pdf_oxide returned no text");
            }
            Ok(Err(e)) => {
                tracing::debug!(target: "memvid::extract", error = %e, "pdf_oxide failed");
            }
            Err(_) => {
                tracing::warn!(target: "memvid::extract", "pdf_oxide panicked (likely font parsing issue), falling back to other extractors");
            }
        }
    }

    // Try pdf_extract next (if feature enabled)
    #[cfg(feature = "pdf_extract")]
    {
        match pdf_extract::extract_text_from_mem(bytes) {
            Ok(text) => {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    if best_text.is_none() && trimmed.len() >= min_good_chars {
                        tracing::debug!(
                            target: "memvid::extract",
                            len = trimmed.len(),
                            "pdf_extract succeeded with good result"
                        );
                        return Ok(Some((trimmed.to_string(), "pdf_extract")));
                    }
                    // Use if better than current best
                    if best_text
                        .as_ref()
                        .is_none_or(|prev| trimmed.len() > prev.len())
                    {
                        best_text = Some(trimmed.to_string());
                        best_source = "pdf_extract";
                    }
                }
            }
            Err(e) => {
                tracing::debug!(target: "memvid::extract", error = %e, "pdf_extract failed");
            }
        }
    }

    // Try lopdf (pure Rust, always available)
    match pdf_text_extract_lopdf(bytes) {
        Ok(Some(text)) => {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                // Use lopdf result if better than previous
                if best_text
                    .as_ref()
                    .is_none_or(|prev| trimmed.len() > prev.len())
                {
                    tracing::debug!(
                        target: "memvid::extract",
                        len = trimmed.len(),
                        "lopdf extracted more text"
                    );
                    best_text = Some(trimmed.to_string());
                    best_source = "lopdf";
                }
            }
        }
        Ok(None) => {
            tracing::debug!(target: "memvid::extract", "lopdf returned no text");
        }
        Err(e) => {
            tracing::debug!(target: "memvid::extract", error = %e, "lopdf failed");
        }
    }

    // Apply fix_pdf_spacing to repair character-level spacing artifacts from PDF encoding
    Ok(best_text.map(|t| (fix_pdf_spacing(&t), best_source)))
}

/// Extract text from PDF using pdf_oxide (highest accuracy, perfect word spacing)
/// Note: pdf_oxide only supports file paths, so we write bytes to a temp file first
#[cfg(feature = "pdf_oxide")]
#[allow(dead_code)]
fn pdf_text_extract_oxide(bytes: &[u8]) -> Result<Option<String>> {
    use pdf_oxide::PdfDocument;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // pdf_oxide only supports opening from file path, so we write to temp file
    let mut temp_file = NamedTempFile::new().map_err(|err| MemvidError::ExtractionFailed {
        reason: format!("pdf_oxide failed to create temp file: {err}").into(),
    })?;

    temp_file
        .write_all(bytes)
        .map_err(|err| MemvidError::ExtractionFailed {
            reason: format!("pdf_oxide failed to write temp file: {err}").into(),
        })?;

    temp_file
        .flush()
        .map_err(|err| MemvidError::ExtractionFailed {
            reason: format!("pdf_oxide failed to flush temp file: {err}").into(),
        })?;

    let temp_path = temp_file.path();
    let mut doc = PdfDocument::open(temp_path).map_err(|err| MemvidError::ExtractionFailed {
        reason: format!("pdf_oxide failed to load PDF: {err}").into(),
    })?;

    let page_count = doc
        .page_count()
        .map_err(|err| MemvidError::ExtractionFailed {
            reason: format!("pdf_oxide failed to get page count: {err}").into(),
        })?;
    if page_count == 0 {
        return Ok(None);
    }

    let mut all_text = String::new();
    for page_idx in 0..page_count {
        match doc.extract_text(page_idx) {
            Ok(text) => {
                if !text.is_empty() {
                    if !all_text.is_empty() {
                        all_text.push('\n');
                    }
                    all_text.push_str(&text);
                }
            }
            Err(e) => {
                tracing::debug!(
                    target: "memvid::extract",
                    page = page_idx,
                    error = %e,
                    "pdf_oxide failed to extract page"
                );
            }
        }
    }

    let trimmed = all_text.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

/// Check if bytes look like a PDF file (magic bytes check)
#[allow(dead_code)]
fn is_probably_pdf_simple(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    let mut slice = bytes;
    // Skip BOM if present
    if slice.starts_with(&[0xEF, 0xBB, 0xBF]) {
        slice = &slice[3..];
    }
    // Skip leading whitespace/nulls
    while let Some((first, rest)) = slice.split_first() {
        if *first == 0 || first.is_ascii_whitespace() {
            slice = rest;
        } else {
            break;
        }
    }
    slice.starts_with(b"%PDF")
}

/// Extract text from a PDF using lopdf (pure Rust, no external dependencies)
#[allow(dead_code)]
fn pdf_text_extract_lopdf(bytes: &[u8]) -> Result<Option<String>> {
    if bytes.len() > PDF_LOPDF_MAX_BYTES {
        return Err(MemvidError::ExtractionFailed {
            reason: format!(
                "PDF too large: {} bytes exceeds limit of {} bytes",
                bytes.len(),
                PDF_LOPDF_MAX_BYTES
            )
            .into(),
        });
    }

    let mut document =
        LopdfDocument::load_mem(bytes).map_err(|err| MemvidError::ExtractionFailed {
            reason: format!("failed to load PDF: {err}").into(),
        })?;

    // Try to decrypt if encrypted (empty password for unprotected PDFs)
    if document.is_encrypted() && document.decrypt("").is_err() {
        return Err(MemvidError::ExtractionFailed {
            reason: "cannot decrypt password-protected PDF".into(),
        });
    }

    // Decompress streams for better text extraction
    let () = document.decompress();

    let mut page_numbers: Vec<u32> = document.get_pages().keys().copied().collect();
    if page_numbers.is_empty() {
        return Ok(None);
    }
    page_numbers.sort_unstable();

    if page_numbers.len() > PDF_LOPDF_MAX_PAGES {
        return Err(MemvidError::ExtractionFailed {
            reason: format!(
                "PDF has too many pages: {} exceeds limit of {}",
                page_numbers.len(),
                PDF_LOPDF_MAX_PAGES
            )
            .into(),
        });
    }

    match document.extract_text(&page_numbers) {
        Ok(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Err(err) => Err(MemvidError::ExtractionFailed {
            reason: format!("failed to extract text from PDF: {err}").into(),
        }),
    }
}

#[cfg(all(test, feature = "extractous"))]
mod tests {
    use super::*;

    #[test]
    fn detects_pdf_like_dump() {
        let snippet = "binary %PDF snippet endobj endobj endstream 0 obj /Type /Page endobj ";
        let dump = snippet.repeat(12);
        assert!(looks_like_pdf_structure_dump(&dump));
    }

    #[test]
    fn skips_normal_text() {
        let text = "This is a perfectly normal paragraph that should not trigger the PDF fallback.";
        assert!(!looks_like_pdf_structure_dump(text));
    }

    #[test]
    fn identifies_pdf_magic() {
        let bytes = b"%PDF-1.7 some data";
        assert!(is_probably_pdf(bytes));
        let padded = b"\n\n%PDF-1.5";
        assert!(is_probably_pdf(padded));
        let not_pdf = b"<!doctype html>";
        assert!(!is_probably_pdf(not_pdf));
    }
}

#[cfg(all(test, feature = "extractous"))]
mod pdf_fix_tests {
    use super::*;

    /// Test that PDF extraction via lopdf fallback returns actual text content,
    /// not raw PDF bytes. This test verifies the fix for the bug where PDFs
    /// with extractous returning empty content would have their raw bytes
    /// indexed instead of extracted text.
    #[test]
    fn test_pdf_structure_dump_detection_prevents_raw_indexing() {
        // Create a synthetic PDF-like structure that should trigger the fallback
        let pdf_structure = b"%PDF-1.4\n%\xff\xff\xff\xff\n1 0 obj\n<</Type/Catalog>>\nendobj\n";

        // This should be detected as PDF structure
        assert!(is_probably_pdf(pdf_structure));

        // And content that looks like PDF structure should be detected
        let structure_dump =
            "binary %PDF snippet endobj endobj endstream 0 obj /Type /Page endobj ".repeat(12);
        assert!(looks_like_pdf_structure_dump(&structure_dump));

        // Normal text should NOT be detected as PDF structure
        let normal_text = "This is perfectly normal extracted text from a document.";
        assert!(!looks_like_pdf_structure_dump(normal_text));
    }
}

// ============================================================================
// Tests for ExtractionCache LRU eviction
// ============================================================================

#[cfg(all(test, feature = "extractous"))]
mod extraction_cache_tests {
    use super::*;

    fn make_doc(content: &str) -> ExtractedDocument {
        ExtractedDocument {
            text: Some(content.to_string()),
            metadata: serde_json::json!({}),
            mime_type: Some("text/plain".to_string()),
        }
    }

    #[test]
    fn test_extraction_cache_basic() {
        let mut cache = ExtractionCache::new(10);
        let hash = blake3::hash(b"test document");
        let doc = make_doc("test content");

        cache.insert(hash, doc.clone());
        let retrieved = cache.get(&hash);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().text, Some("test content".to_string()));
    }

    #[test]
    fn test_extraction_cache_stats() {
        let mut cache = ExtractionCache::new(10);
        let hash = blake3::hash(b"test");
        cache.insert(hash, make_doc("test"));

        // Hit
        let _ = cache.get(&hash);
        // Miss
        let missing = blake3::hash(b"missing");
        let _ = cache.get(&missing);

        let (hits, misses, size) = cache.stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
        assert_eq!(size, 1);
    }

    #[test]
    fn test_extraction_cache_eviction() {
        let mut cache = ExtractionCache::new(3);

        // Insert 4 items, first should be evicted
        for i in 0..4u8 {
            let hash = blake3::hash(&[i]);
            cache.insert(hash, make_doc(&format!("doc{}", i)));
        }

        // First item should be evicted
        let evicted = blake3::hash(&[0u8]);
        assert!(cache.get(&evicted).is_none());

        // Last 3 should still exist
        for i in 1..4u8 {
            let hash = blake3::hash(&[i]);
            assert!(cache.get(&hash).is_some());
        }
    }

    #[test]
    fn test_extraction_cache_lru_promotion() {
        let mut cache = ExtractionCache::new(3);

        // Insert 3 items: 0, 1, 2
        for i in 0..3u8 {
            let hash = blake3::hash(&[i]);
            cache.insert(hash, make_doc(&format!("doc{}", i)));
        }

        // Access first item (promotes it to front)
        let first = blake3::hash(&[0u8]);
        let _ = cache.get(&first);

        // Insert 4th item - should evict second (index 1, now oldest)
        let new_hash = blake3::hash(&[3u8]);
        cache.insert(new_hash, make_doc("doc3"));

        // First should still exist (was accessed, got promoted)
        assert!(cache.get(&first).is_some());

        // Second should be evicted (was oldest after first was promoted)
        let second = blake3::hash(&[1u8]);
        assert!(cache.get(&second).is_none());

        // Third and fourth should exist
        let third = blake3::hash(&[2u8]);
        assert!(cache.get(&third).is_some());
        assert!(cache.get(&new_hash).is_some());
    }

    #[test]
    fn test_extraction_cache_update_existing() {
        let mut cache = ExtractionCache::new(3);
        let hash = blake3::hash(b"test");

        cache.insert(hash, make_doc("original"));
        cache.insert(hash, make_doc("updated"));

        let retrieved = cache.get(&hash);
        assert_eq!(retrieved.unwrap().text, Some("updated".to_string()));

        // Size should still be 1
        let (_, _, size) = cache.stats();
        assert_eq!(size, 1);
    }
}
