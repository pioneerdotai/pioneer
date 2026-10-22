//! Time-budgeted text extraction for instant indexing.
//!
//! Extracts representative text from documents within a time budget,
//! enabling sub-second ingestion for large files. Uses pdf-extract (when available)
//! for fast full-document extraction, with lopdf as fallback for per-page extraction.

use std::time::{Duration, Instant};

use lopdf::Document as LopdfDocument;
use serde::{Deserialize, Serialize};

use crate::error::{MemvidError, Result};
// Use SymSpell-based cleanup when feature is enabled, otherwise fall back to heuristic
#[cfg(feature = "symspell_cleanup")]
use crate::symspell_cleanup::fix_pdf_text as fix_pdf_spacing;
#[cfg(not(feature = "symspell_cleanup"))]
use crate::text::fix_pdf_spacing;

/// Default extraction time budget in milliseconds.
/// Calibrated for sub-second total ingestion (<1s including I/O + indexing).
pub const DEFAULT_EXTRACTION_BUDGET_MS: u64 = 350;

/// Configuration for time-budgeted extraction.
#[derive(Debug, Clone, Copy)]
pub struct ExtractionBudget {
    /// Maximum time to spend on extraction.
    pub budget: Duration,
    /// Maximum characters to extract (safety limit).
    pub max_chars: usize,
    /// Sample interval for middle pages (extract every Nth page).
    pub sample_interval: usize,
}

impl Default for ExtractionBudget {
    fn default() -> Self {
        Self {
            budget: Duration::from_millis(DEFAULT_EXTRACTION_BUDGET_MS),
            max_chars: 100_000,
            sample_interval: 20, // Every 20th page
        }
    }
}

impl ExtractionBudget {
    /// Create a budget with custom milliseconds.
    #[must_use]
    pub fn with_ms(ms: u64) -> Self {
        Self {
            budget: Duration::from_millis(ms),
            ..Default::default()
        }
    }

    /// Create an unlimited budget (extract everything).
    #[must_use]
    pub fn unlimited() -> Self {
        Self {
            budget: Duration::from_secs(3600), // 1 hour = effectively unlimited
            max_chars: usize::MAX,
            sample_interval: 1, // Every page
        }
    }
}

/// Result of time-budgeted extraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetedExtractionResult {
    /// Extracted text (may be partial).
    pub text: String,
    /// Number of pages/sections extracted.
    pub sections_extracted: usize,
    /// Total pages/sections in document.
    pub sections_total: usize,
    /// Whether extraction completed within budget.
    pub completed: bool,
    /// Time spent on extraction.
    pub elapsed_ms: u64,
    /// Coverage ratio (0.0 to 1.0).
    pub coverage: f32,
}

impl BudgetedExtractionResult {
    /// Check if we got meaningful content.
    #[must_use]
    pub fn has_content(&self) -> bool {
        !self.text.trim().is_empty()
    }

    /// Check if this is a skim (partial) extraction.
    #[must_use]
    pub fn is_skim(&self) -> bool {
        !self.completed && self.sections_extracted < self.sections_total
    }
}

/// Extract text from a PDF with time budget.
///
/// Strategy:
/// 1. Try extractous first (best quality, handles all fonts including ligatures)
/// 2. Try pdf-extract (fast, but panics on some fonts with ligatures)
/// 3. Fall back to lopdf per-page (basic extraction, poor word spacing)
pub fn extract_pdf_budgeted(
    bytes: &[u8],
    budget: ExtractionBudget,
) -> Result<BudgetedExtractionResult> {
    let start = Instant::now();

    // Try extractous first - best quality, handles all fonts including ligatures
    #[cfg(feature = "extractous")]
    {
        use extractous::Extractor;

        // Write bytes to temp file (extractous needs a file path)
        if let Ok(mut temp_file) = tempfile::NamedTempFile::new() {
            use std::io::Write;
            if temp_file.write_all(bytes).is_ok() {
                let extractor = Extractor::new();
                let path_str = temp_file.path().to_string_lossy();
                if let Ok((text, _metadata)) = extractor.extract_file_to_string(&path_str) {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        tracing::debug!("extractous successfully extracted PDF text");
                        // Estimate page count from text (rough heuristic: ~3000 chars per page)
                        let estimated_pages = (trimmed.len() / 3000).max(1);
                        let text_len = trimmed.len();

                        // Truncate if needed
                        let final_text = if text_len > budget.max_chars {
                            truncate_at_boundary(trimmed, budget.max_chars)
                        } else {
                            trimmed.to_string()
                        };

                        let completed = final_text.len() == text_len;

                        return Ok(BudgetedExtractionResult {
                            text: final_text,
                            sections_extracted: estimated_pages,
                            sections_total: estimated_pages,
                            completed,
                            elapsed_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                            coverage: 1.0,
                        });
                    }
                    tracing::debug!("extractous returned empty text, trying pdf-extract");
                }
            }
        }
    }

    // Try pdf-extract - fast full document extraction
    // Wrap in catch_unwind because cff-parser may panic on certain fonts (ligatures like fi/fl)
    #[cfg(feature = "pdf_extract")]
    {
        let bytes_clone = bytes.to_vec();
        let extract_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pdf_extract::extract_text_from_mem(&bytes_clone)
        }));

        match extract_result {
            Ok(Ok(text)) => {
                // Clean up character spacing issues from PDF extraction
                let cleaned = fix_pdf_spacing(text.trim());
                if !cleaned.is_empty() {
                    // Estimate page count from text (rough heuristic: ~3000 chars per page)
                    let estimated_pages = (cleaned.len() / 3000).max(1);
                    let cleaned_len = cleaned.len();

                    // Truncate if needed
                    let final_text = if cleaned_len > budget.max_chars {
                        truncate_at_boundary(&cleaned, budget.max_chars)
                    } else {
                        cleaned
                    };

                    let completed = final_text.len() == cleaned_len;

                    return Ok(BudgetedExtractionResult {
                        text: final_text,
                        sections_extracted: estimated_pages,
                        sections_total: estimated_pages,
                        completed,
                        elapsed_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                        coverage: 1.0,
                    });
                }
                // pdf-extract returned empty, fall through to lopdf
                tracing::debug!("pdf-extract returned empty text, trying lopdf");
            }
            Ok(Err(e)) => {
                // pdf-extract failed, fall through to lopdf
                tracing::debug!(?e, "pdf-extract failed, trying lopdf");
            }
            Err(_) => {
                // pdf-extract panicked (cff-parser font issue), fall through to lopdf
                tracing::warn!(
                    "pdf-extract panicked (likely font parsing issue), falling back to lopdf"
                );
            }
        }
    }

    // Fall back to lopdf per-page extraction
    extract_pdf_budgeted_lopdf(bytes, budget, start)
}

/// Truncate text at a word/sentence boundary
#[allow(dead_code)]
fn truncate_at_boundary(text: &str, max_chars: usize) -> String {
    if text.len() <= max_chars {
        return text.to_string();
    }

    // Find last space before max_chars
    let truncate_at = text[..max_chars]
        .rfind(|c: char| c.is_whitespace())
        .unwrap_or(max_chars);

    text[..truncate_at].to_string()
}

/// lopdf-based per-page extraction (slower but works as fallback)
fn extract_pdf_budgeted_lopdf(
    bytes: &[u8],
    budget: ExtractionBudget,
    start: Instant,
) -> Result<BudgetedExtractionResult> {
    let deadline = start + budget.budget;

    // Load PDF
    let mut document =
        LopdfDocument::load_mem(bytes).map_err(|err| MemvidError::ExtractionFailed {
            reason: format!("failed to load PDF: {err}").into(),
        })?;

    // Handle encryption
    if document.is_encrypted() && document.decrypt("").is_err() {
        return Err(MemvidError::ExtractionFailed {
            reason: "cannot decrypt password-protected PDF".into(),
        });
    }

    // Decompress for better extraction
    let () = document.decompress();

    // Get page numbers
    let mut page_numbers: Vec<u32> = document.get_pages().keys().copied().collect();
    if page_numbers.is_empty() {
        return Ok(BudgetedExtractionResult {
            text: String::new(),
            sections_extracted: 0,
            sections_total: 0,
            completed: true,
            elapsed_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            coverage: 1.0,
        });
    }
    page_numbers.sort_unstable();

    let page_count = page_numbers.len();
    let mut extracted_pages: Vec<(u32, String)> = Vec::new();
    let mut total_chars = 0usize;

    // Priority pages: first and last
    let priority_pages: Vec<u32> = {
        let mut pages = vec![page_numbers[0]]; // First page
        if page_count > 1 {
            pages.push(page_numbers[page_count - 1]); // Last page
        }
        pages
    };

    // Extract priority pages first (always)
    for &page_num in &priority_pages {
        if total_chars >= budget.max_chars {
            break;
        }

        if let Ok(text) = document.extract_text(&[page_num]) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                total_chars += trimmed.len();
                extracted_pages.push((page_num, trimmed.to_string()));
            }
        }
    }

    // Check if we still have time
    if Instant::now() >= deadline || total_chars >= budget.max_chars {
        return finish_extraction(extracted_pages, page_count, start, false);
    }

    // Sample middle pages until deadline
    let sample_interval = budget.sample_interval.max(1);
    let middle_pages: Vec<u32> = page_numbers
        .iter()
        .enumerate()
        .filter(|(i, page)| {
            // Skip priority pages, sample every Nth page
            !priority_pages.contains(page) && (*i % sample_interval == 0)
        })
        .map(|(_, page)| *page)
        .collect();

    for &page_num in &middle_pages {
        // Check deadline before each page
        if Instant::now() >= deadline {
            return finish_extraction(extracted_pages, page_count, start, false);
        }

        if total_chars >= budget.max_chars {
            return finish_extraction(extracted_pages, page_count, start, false);
        }

        if let Ok(text) = document.extract_text(&[page_num]) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                total_chars += trimmed.len();
                extracted_pages.push((page_num, trimmed.to_string()));
            }
        }
    }

    // If we got here, we extracted all sampled pages without hitting limits
    let completed = extracted_pages.len() >= page_count;
    finish_extraction(extracted_pages, page_count, start, completed)
}

/// Extract text from plain text/markdown with time budget.
/// Text formats are fast - we typically get everything.
pub fn extract_text_budgeted(
    bytes: &[u8],
    budget: ExtractionBudget,
) -> Result<BudgetedExtractionResult> {
    let start = Instant::now();

    // Try UTF-8 decode, using lossy conversion for non-UTF8
    let text: String = match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    };

    // Truncate if needed, respecting character boundaries
    let truncated = if text.len() > budget.max_chars {
        // Find a valid character boundary
        let mut end = budget.max_chars;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        text[..end].to_string()
    } else {
        text
    };

    // Count "sections" as paragraphs (double newlines)
    let sections = truncated.split("\n\n").count();

    Ok(BudgetedExtractionResult {
        text: truncated,
        sections_extracted: sections,
        sections_total: sections,
        completed: true,
        elapsed_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        coverage: 1.0,
    })
}

/// Check if MIME type is an Office Open XML format (xlsx, docx, pptx).
fn is_ooxml_mime(mime: Option<&str>) -> bool {
    let Some(m) = mime else { return false };
    let m = m.to_lowercase();
    m.contains("spreadsheetml")
        || m.contains("wordprocessingml")
        || m.contains("presentationml")
        || m == "application/vnd.ms-excel"
        || m == "application/msword"
        || m == "application/vnd.ms-powerpoint"
}

/// Extract OOXML and legacy Office documents using the reader registry.
fn extract_ooxml_budgeted(
    bytes: &[u8],
    mime: Option<&str>,
    uri: Option<&str>,
) -> Result<BudgetedExtractionResult> {
    use crate::reader::{DocumentFormat, ReaderHint, ReaderRegistry};

    let start = Instant::now();

    // Determine the document format from MIME first, then fall back to extension
    let format = match mime.map(str::to_lowercase).as_deref() {
        Some(m) if m.contains("spreadsheetml") => Some(DocumentFormat::Xlsx),
        Some(m) if m.contains("wordprocessingml") => Some(DocumentFormat::Docx),
        Some(m) if m.contains("presentationml") => Some(DocumentFormat::Pptx),
        Some("application/vnd.ms-excel") => Some(DocumentFormat::Xls),
        _ => {
            // Fall back to extension-based detection
            uri.and_then(|u| {
                let lower = u.to_lowercase();
                if lower.ends_with(".xlsx") {
                    Some(DocumentFormat::Xlsx)
                } else if lower.ends_with(".docx") {
                    Some(DocumentFormat::Docx)
                } else if lower.ends_with(".pptx") {
                    Some(DocumentFormat::Pptx)
                } else if lower.ends_with(".xls") {
                    Some(DocumentFormat::Xls)
                } else {
                    None
                }
            })
        }
    };

    let hint = ReaderHint::new(mime, format).with_uri(uri);
    let registry = ReaderRegistry::default();

    if let Some(reader) = registry.find_reader(&hint) {
        match reader.extract(bytes, &hint) {
            Ok(output) => {
                let text = output.document.text.unwrap_or_default();
                let sections = text
                    .split("\n\n")
                    .filter(|s| !s.trim().is_empty())
                    .count()
                    .max(1);
                Ok(BudgetedExtractionResult {
                    text,
                    sections_extracted: sections,
                    sections_total: sections,
                    completed: true,
                    elapsed_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                    coverage: 1.0,
                })
            }
            Err(e) => Err(e),
        }
    } else {
        // No reader found, return empty
        Ok(BudgetedExtractionResult {
            text: String::new(),
            sections_extracted: 0,
            sections_total: 0,
            completed: true,
            elapsed_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            coverage: 1.0,
        })
    }
}

/// Determine document type and extract with budget.
pub fn extract_with_budget(
    bytes: &[u8],
    mime: Option<&str>,
    uri: Option<&str>,
    budget: ExtractionBudget,
) -> Result<BudgetedExtractionResult> {
    // Check if PDF
    let is_pdf = mime.is_some_and(|m| m.contains("pdf")) || is_pdf_magic(bytes);

    if is_pdf {
        extract_pdf_budgeted(bytes, budget)
    } else if is_ooxml_mime(mime) || is_ooxml_by_extension(uri) || is_ooxml_by_magic(bytes, uri) {
        // Handle Office Open XML formats (xlsx, docx, pptx) via reader registry
        extract_ooxml_budgeted(bytes, mime, uri)
    } else if is_binary_mime(mime) || is_binary_content(bytes) {
        // Skip extraction for binary content (video, audio, images, etc.)
        Ok(BudgetedExtractionResult {
            text: String::new(),
            sections_extracted: 0,
            sections_total: 0,
            completed: true,
            elapsed_ms: 0,
            coverage: 1.0,
        })
    } else {
        // Treat as text
        extract_text_budgeted(bytes, budget)
    }
}

/// Check if file extension indicates OOXML format
fn is_ooxml_by_extension(uri: Option<&str>) -> bool {
    let Some(u) = uri else { return false };
    let lower = u.to_lowercase();
    lower.ends_with(".docx")
        || lower.ends_with(".xlsx")
        || lower.ends_with(".pptx")
        || lower.ends_with(".doc")
        || lower.ends_with(".xls")
        || lower.ends_with(".ppt")
}

/// Check if bytes start with ZIP magic and extension indicates OOXML
fn is_ooxml_by_magic(bytes: &[u8], uri: Option<&str>) -> bool {
    // ZIP magic bytes: PK\x03\x04
    if bytes.len() >= 4 && bytes.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
        // It's a ZIP file - check if extension suggests Office format
        is_ooxml_by_extension(uri)
    } else {
        false
    }
}

/// Check if mime type indicates binary content
fn is_binary_mime(mime: Option<&str>) -> bool {
    let Some(m) = mime else { return false };
    let m = m.to_lowercase();
    m.starts_with("video/")
        || m.starts_with("audio/")
        || m.starts_with("image/")
        || m == "application/octet-stream"
        || m.contains("zip")
        || m.contains("gzip")
        || m.contains("tar")
        || m.contains("rar")
        || m.contains("7z")
}

/// Check if content appears to be binary (high ratio of non-printable bytes)
fn is_binary_content(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    // Sample first 8KB to detect binary content
    let sample_size = bytes.len().min(8192);
    let sample = &bytes[..sample_size];

    let non_text_count = sample
        .iter()
        .filter(|&&b| {
            // Non-text: null bytes, or bytes outside printable ASCII/UTF-8 range
            // Allow: tab, newline, carriage return, and printable ASCII
            b == 0 || (b < 32 && b != 9 && b != 10 && b != 13)
        })
        .count();

    // If >30% of bytes are non-text, treat as binary
    non_text_count * 100 / sample_size > 30
}

/// Check PDF magic bytes.
fn is_pdf_magic(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    let mut slice = bytes;
    // Skip BOM
    if slice.starts_with(&[0xEF, 0xBB, 0xBF]) {
        slice = &slice[3..];
    }
    // Skip whitespace
    while let Some((first, rest)) = slice.split_first() {
        if *first == 0 || first.is_ascii_whitespace() {
            slice = rest;
        } else {
            break;
        }
    }
    slice.starts_with(b"%PDF")
}

/// Finish extraction and build result.
fn finish_extraction(
    mut pages: Vec<(u32, String)>,
    total_pages: usize,
    start: Instant,
    completed: bool,
) -> Result<BudgetedExtractionResult> {
    // Sort by page number for coherent output
    pages.sort_by_key(|(num, _)| *num);

    let sections_extracted = pages.len();
    let text = pages
        .into_iter()
        .map(|(_, text)| fix_pdf_spacing(&text)) // Clean up character spacing
        .collect::<Vec<_>>()
        .join("\n\n");

    let coverage = if total_pages > 0 {
        sections_extracted as f32 / total_pages as f32
    } else {
        1.0
    };

    Ok(BudgetedExtractionResult {
        text,
        sections_extracted,
        sections_total: total_pages,
        completed,
        elapsed_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        coverage,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_extraction_budget() {
        let text = b"Hello world.\n\nThis is a test.\n\nAnother paragraph.";
        let result = extract_text_budgeted(text, ExtractionBudget::default()).unwrap();

        assert!(result.completed);
        assert!(result.has_content());
        assert_eq!(result.sections_extracted, 3);
        assert_eq!(result.coverage, 1.0);
    }

    #[test]
    fn test_text_truncation() {
        let text = "x".repeat(200_000);
        let budget = ExtractionBudget {
            max_chars: 1000,
            ..Default::default()
        };
        let result = extract_text_budgeted(text.as_bytes(), budget).unwrap();

        assert_eq!(result.text.len(), 1000);
    }

    #[test]
    fn test_pdf_magic_detection() {
        assert!(is_pdf_magic(b"%PDF-1.7"));
        assert!(is_pdf_magic(b"  \n%PDF-1.4"));
        assert!(!is_pdf_magic(b"Hello world"));
        assert!(!is_pdf_magic(b""));
    }

    #[test]
    fn test_budget_config() {
        let default = ExtractionBudget::default();
        assert_eq!(default.budget.as_millis(), 350);

        let custom = ExtractionBudget::with_ms(500);
        assert_eq!(custom.budget.as_millis(), 500);

        let unlimited = ExtractionBudget::unlimited();
        assert_eq!(unlimited.sample_interval, 1);
    }

    // PDF spacing cleanup tests are in text.rs (fix_pdf_spacing function)
}
