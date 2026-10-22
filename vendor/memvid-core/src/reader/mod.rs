//! Document reader traits and registry for unified format ingestion.

mod docx;
mod passthrough;
mod pdf;
mod pptx;
mod xls;
mod xlsx;
pub mod xlsx_chunker;
pub mod xlsx_ooxml;
pub mod xlsx_table_detect;

use serde_json::Value;

pub use docx::DocxReader;
pub use passthrough::PassthroughReader;
pub use pdf::PdfReader;
pub use pptx::PptxReader;
pub use xls::XlsReader;
pub use xlsx::{XlsxReader, XlsxStructuredDiagnostics, XlsxStructuredResult};
pub use xlsx_chunker::XlsxChunkingOptions;
pub use xlsx_table_detect::DetectedTable;

use crate::{ExtractedDocument, Result};

/// Soft classification of document formats used by the ingestion router.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocumentFormat {
    Pdf,
    Docx,
    Xlsx,
    Xls,
    Pptx,
    PlainText,
    Markdown,
    Html,
    Jsonl,
    Unknown,
}

impl DocumentFormat {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Pdf => "pdf",
            Self::Docx => "docx",
            Self::Xlsx => "xlsx",
            Self::Xls => "xls",
            Self::Pptx => "pptx",
            Self::PlainText => "text",
            Self::Markdown => "markdown",
            Self::Html => "html",
            Self::Jsonl => "jsonl",
            Self::Unknown => "unknown",
        }
    }
}

/// Hint provided to readers before probing/extraction.
#[derive(Debug, Clone)]
pub struct ReaderHint<'a> {
    pub mime: Option<&'a str>,
    pub format: Option<DocumentFormat>,
    pub uri: Option<&'a str>,
    pub magic_bytes: Option<&'a [u8]>,
}

impl<'a> ReaderHint<'a> {
    #[must_use]
    pub fn new(mime: Option<&'a str>, format: Option<DocumentFormat>) -> Self {
        Self {
            mime,
            format,
            uri: None,
            magic_bytes: None,
        }
    }

    #[must_use]
    pub fn with_uri(mut self, uri: Option<&'a str>) -> Self {
        self.uri = uri;
        self
    }

    #[must_use]
    pub fn with_magic(mut self, magic: Option<&'a [u8]>) -> Self {
        self.magic_bytes = magic;
        self
    }
}

/// Structured text and metadata extracted from a document, plus routing diagnostics.
#[derive(Debug, Clone)]
pub struct ReaderOutput {
    pub document: ExtractedDocument,
    pub reader_name: String,
    pub diagnostics: ReaderDiagnostics,
}

impl ReaderOutput {
    #[must_use]
    pub fn new(document: ExtractedDocument, reader_name: impl Into<String>) -> Self {
        Self {
            document,
            reader_name: reader_name.into(),
            diagnostics: ReaderDiagnostics::default(),
        }
    }

    #[must_use]
    pub fn with_diagnostics(mut self, diagnostics: ReaderDiagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

/// Metadata about a reader attempt used for observability and surfacing warnings.
#[derive(Debug, Clone, Default)]
pub struct ReaderDiagnostics {
    pub warnings: Vec<String>,
    pub fallback: bool,
    pub extra_metadata: Value,
    pub duration_ms: Option<u64>,
    pub pages_processed: Option<u32>,
}

impl ReaderDiagnostics {
    pub fn record_warning<S: Into<String>>(&mut self, warning: S) {
        self.warnings.push(warning.into());
    }

    pub fn mark_fallback(&mut self) {
        self.fallback = true;
    }

    #[must_use]
    pub fn with_metadata(mut self, value: Value) -> Self {
        self.extra_metadata = value;
        self
    }

    pub fn merge_from(&mut self, other: &ReaderDiagnostics) {
        self.warnings.extend(other.warnings.iter().cloned());
        if other.fallback {
            self.fallback = true;
        }
        if !other.extra_metadata.is_null() {
            self.extra_metadata = other.extra_metadata.clone();
        }
        if other.duration_ms.is_some() {
            self.duration_ms = other.duration_ms;
        }
        if other.pages_processed.is_some() {
            self.pages_processed = other.pages_processed;
        }
    }

    pub fn track_warning<S: Into<String>>(&mut self, warning: S) {
        self.warnings.push(warning.into());
        self.fallback = true;
    }
}

/// Trait implemented by document readers that can extract text from supported formats.
pub trait DocumentReader: Send + Sync {
    /// Human-readable name used for diagnostics (e.g., "`document_processor`", "pdfium").
    fn name(&self) -> &'static str;

    /// Return true if this reader is a good match for the provided hint.
    fn supports(&self, hint: &ReaderHint<'_>) -> bool;

    /// Extract text and metadata from the provided bytes.
    fn extract(&self, bytes: &[u8], hint: &ReaderHint<'_>) -> Result<ReaderOutput>;
}

/// Registry of document readers used by the ingestion router.
pub struct ReaderRegistry {
    readers: Vec<Box<dyn DocumentReader>>,
}

impl ReaderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            readers: Vec::new(),
        }
    }

    pub fn register<R>(&mut self, reader: R)
    where
        R: DocumentReader + 'static,
    {
        self.readers.push(Box::new(reader));
    }

    #[must_use]
    pub fn readers(&self) -> &[Box<dyn DocumentReader>] {
        &self.readers
    }

    pub fn find_reader<'a>(&'a self, hint: &ReaderHint<'_>) -> Option<&'a dyn DocumentReader> {
        self.readers
            .iter()
            .map(std::convert::AsRef::as_ref)
            .find(|reader| reader.supports(hint))
    }
}

impl Default for ReaderRegistry {
    fn default() -> Self {
        let mut registry = Self::new();
        registry.register(PdfReader);
        registry.register(DocxReader);
        registry.register(XlsxReader);
        registry.register(XlsReader);
        registry.register(PptxReader);
        registry.register(PassthroughReader);
        registry
    }
}
