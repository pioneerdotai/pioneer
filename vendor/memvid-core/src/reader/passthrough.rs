use std::sync::OnceLock;

use crate::{
    DocumentFormat, DocumentProcessor, DocumentReader, ReaderDiagnostics, ReaderHint, ReaderOutput,
    Result,
};

/// Basic reader that proxies to the global `DocumentProcessor` for formats we
/// already support via Extractous/lopdf.
pub struct PassthroughReader;

impl PassthroughReader {
    fn processor() -> &'static DocumentProcessor {
        static PROCESSOR: OnceLock<DocumentProcessor> = OnceLock::new();
        PROCESSOR.get_or_init(DocumentProcessor::default)
    }

    fn supported_format(format: Option<DocumentFormat>) -> bool {
        matches!(
            format,
            Some(
                DocumentFormat::Pdf
                    | DocumentFormat::PlainText
                    | DocumentFormat::Markdown
                    | DocumentFormat::Html
            ) | None
        )
    }
}

impl DocumentReader for PassthroughReader {
    fn name(&self) -> &'static str {
        "document_processor"
    }

    fn supports(&self, hint: &ReaderHint<'_>) -> bool {
        Self::supported_format(hint.format)
            || hint.mime.is_none_or(|mime| {
                mime.eq_ignore_ascii_case("application/pdf") || mime.starts_with("text/")
            })
    }

    fn extract(&self, bytes: &[u8], _hint: &ReaderHint<'_>) -> Result<ReaderOutput> {
        let document = Self::processor().extract_from_bytes(bytes)?;
        Ok(ReaderOutput::new(document, self.name()).with_diagnostics(ReaderDiagnostics::default()))
    }
}
