#[cfg(not(feature = "pdfium"))]
use std::sync::OnceLock;

use crate::{DocumentFormat, DocumentReader, ReaderHint, ReaderOutput, Result};

#[cfg(not(feature = "pdfium"))]
use crate::{DocumentProcessor, ReaderDiagnostics};

#[cfg(feature = "pdfium")]
use crate::PassthroughReader;
#[cfg(feature = "pdfium")]
use pdfium_render::prelude::*;
#[cfg(feature = "pdfium")]
use serde_json::json;
#[cfg(feature = "pdfium")]
use std::time::{Duration, Instant};

/// Primary PDF reader. Uses Pdfium when enabled, with a graceful fallback to
/// the shared document processor.
pub struct PdfReader;

#[cfg(feature = "pdfium")]
const PDFIUM_MAX_PAGES: u32 = 4_096;
#[cfg(feature = "pdfium")]
const PDFIUM_MAX_DURATION: Duration = Duration::from_secs(10);
#[cfg(feature = "pdfium")]
const PDFIUM_MAX_BYTES: usize = 128 * 1024 * 1024;

impl PdfReader {
    #[cfg(not(feature = "pdfium"))]
    fn processor() -> &'static DocumentProcessor {
        static PROCESSOR: OnceLock<DocumentProcessor> = OnceLock::new();
        PROCESSOR.get_or_init(DocumentProcessor::default)
    }

    fn supports_mime(mime: Option<&str>) -> bool {
        mime.is_some_and(|m| m.eq_ignore_ascii_case("application/pdf"))
    }

    fn supports_magic(magic: Option<&[u8]>) -> bool {
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

    #[cfg(feature = "pdfium")]
    fn extract_with_pdfium(bytes: &[u8]) -> Result<(String, u32, u64)> {
        if bytes.len() > PDFIUM_MAX_BYTES {
            return Err(crate::MemvidError::ExtractionFailed {
                reason: format!(
                    "pdfium payload exceeds limit ({} bytes > {} bytes)",
                    bytes.len(),
                    PDFIUM_MAX_BYTES
                )
                .into(),
            });
        }
        let pdfium = Pdfium::bind_to_system_library()
            .map(Pdfium::new)
            .map_err(|err| crate::MemvidError::ExtractionFailed {
                reason: format!("failed to bind pdfium: {err}").into(),
            })?;
        let start = Instant::now();
        let document = pdfium
            .load_pdf_from_byte_slice(bytes, None)
            .map_err(|err| crate::MemvidError::ExtractionFailed {
                reason: format!("pdfium failed to load pdf: {err}").into(),
            })?;

        let mut combined = String::new();
        let mut pages = 0u32;

        for index in 0..document.pages().len() {
            if pages >= PDFIUM_MAX_PAGES {
                return Err(crate::MemvidError::ExtractionFailed {
                    reason: format!("pdfium page limit reached (>{} pages)", PDFIUM_MAX_PAGES)
                        .into(),
                });
            }
            let page = document.pages().get(index).map_err(|err| {
                crate::MemvidError::ExtractionFailed {
                    reason: format!("pdfium failed to access page {index}: {err}").into(),
                }
            })?;
            let page_text = page
                .text()
                .map_err(|err| crate::MemvidError::ExtractionFailed {
                    reason: format!("pdfium failed to extract page {index} text: {err}").into(),
                })?;
            let chunk = page_text.all();
            combined.push_str(&chunk);
            combined.push('\n');
            pages += 1;
        }

        let duration_ms = start.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        let trimmed = combined.trim();
        if trimmed.is_empty() {
            return Err(crate::MemvidError::ExtractionFailed {
                reason: "pdfium produced no textual content".into(),
            });
        }

        Ok((trimmed.to_string(), pages, duration_ms))
    }
}

impl DocumentReader for PdfReader {
    fn name(&self) -> &'static str {
        "pdf"
    }

    fn supports(&self, hint: &ReaderHint<'_>) -> bool {
        matches!(hint.format, Some(DocumentFormat::Pdf))
            || Self::supports_mime(hint.mime)
            || Self::supports_magic(hint.magic_bytes)
    }

    fn extract(&self, bytes: &[u8], hint: &ReaderHint<'_>) -> Result<ReaderOutput> {
        #[cfg(feature = "pdfium")]
        {
            let result = Self::extract_with_pdfium(bytes);
            let output = match result {
                Ok((text, pages, duration_ms)) => {
                    let mut base = PassthroughReader.extract(bytes, hint)?;
                    base.reader_name = self.name().to_string();
                    base.document.text = Some(text);
                    base.diagnostics.duration_ms = Some(duration_ms);
                    base.diagnostics.pages_processed = Some(pages);
                    base.diagnostics.extra_metadata = json!({
                        "pages": pages,
                        "reader": "pdfium",
                        "duration_ms": duration_ms,
                    });
                    if Duration::from_millis(duration_ms) > PDFIUM_MAX_DURATION {
                        base.diagnostics.track_warning(format!(
                            "pdfium extraction exceeded timeout {:?}",
                            PDFIUM_MAX_DURATION
                        ));
                    }
                    base
                }
                Err(err) => {
                    let mut fallback = PassthroughReader.extract(bytes, hint)?;
                    fallback.reader_name = self.name().to_string();
                    fallback
                        .diagnostics
                        .track_warning(format!("pdfium extraction failed: {err}"));
                    fallback
                }
            };
            return Ok(output);
        }

        #[cfg(not(feature = "pdfium"))]
        {
            let _ = hint;
            let document = Self::processor().extract_from_bytes(bytes)?;
            Ok(ReaderOutput::new(document, self.name())
                .with_diagnostics(ReaderDiagnostics::default()))
        }
    }
}
