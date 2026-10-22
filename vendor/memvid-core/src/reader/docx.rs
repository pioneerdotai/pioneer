use std::io::{Cursor, Read};

use quick_xml::Reader as XmlReader;
use quick_xml::events::Event;
use zip::ZipArchive;

use crate::{
    DocumentFormat, DocumentReader, PassthroughReader, ReaderDiagnostics, ReaderHint, ReaderOutput,
    Result,
};

const DOC_XML_PATH: &str = "word/document.xml";

pub struct DocxReader;

impl DocxReader {
    fn extract_text(bytes: &[u8]) -> Result<String> {
        let cursor = Cursor::new(bytes);
        let mut archive =
            ZipArchive::new(cursor).map_err(|err| crate::MemvidError::ExtractionFailed {
                reason: format!("failed to open docx archive: {err}").into(),
            })?;

        let mut file =
            archive
                .by_name(DOC_XML_PATH)
                .map_err(|err| crate::MemvidError::ExtractionFailed {
                    reason: format!("docx missing document.xml: {err}").into(),
                })?;
        let mut xml = String::new();
        file.read_to_string(&mut xml)
            .map_err(|err| crate::MemvidError::ExtractionFailed {
                reason: format!("failed to read document.xml: {err}").into(),
            })?;

        Ok(extract_plain_text(&xml, b"w:p"))
    }
}

impl DocumentReader for DocxReader {
    fn name(&self) -> &'static str {
        "docx"
    }

    fn supports(&self, hint: &ReaderHint<'_>) -> bool {
        matches!(hint.format, Some(DocumentFormat::Docx))
            || hint.mime.is_some_and(|mime| {
                mime.eq_ignore_ascii_case(
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )
            })
    }

    fn extract(&self, bytes: &[u8], hint: &ReaderHint<'_>) -> Result<ReaderOutput> {
        match Self::extract_text(bytes) {
            Ok(text) => {
                if text.trim().is_empty() {
                    // quick-xml returned empty - try extractous as fallback
                    let mut output = PassthroughReader.extract(bytes, hint)?;
                    output.reader_name = self.name().to_string();
                    output.diagnostics.mark_fallback();
                    output.diagnostics.record_warning(
                        "docx reader produced empty text; falling back to default extractor",
                    );
                    Ok(output)
                } else {
                    // quick-xml succeeded - build output directly WITHOUT calling extractous
                    let mut document = crate::ExtractedDocument::empty();
                    document.text = Some(text);
                    document.mime_type = Some(
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                            .to_string(),
                    );
                    Ok(ReaderOutput::new(document, self.name())
                        .with_diagnostics(ReaderDiagnostics::default()))
                }
            }
            Err(err) => {
                // quick-xml failed - try extractous as fallback
                let mut fallback = PassthroughReader.extract(bytes, hint)?;
                fallback.reader_name = self.name().to_string();
                fallback.diagnostics.mark_fallback();
                fallback
                    .diagnostics
                    .record_warning(format!("docx reader error: {err}"));
                Ok(fallback)
            }
        }
    }
}

fn extract_plain_text(xml: &str, block_tag: &[u8]) -> String {
    let mut reader = XmlReader::from_str(xml);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut text = String::new();
    let mut first_block = true;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                if e.name().as_ref().ends_with(block_tag) {
                    if !first_block {
                        text.push('\n');
                    }
                    first_block = false;
                }
            }
            Ok(Event::Text(t)) => {
                if let Ok(content) = t.unescape() {
                    if !content.trim().is_empty() {
                        text.push_str(content.trim());
                        text.push(' ');
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => (),
        }
        buf.clear();
    }

    text.trim().to_string()
}
