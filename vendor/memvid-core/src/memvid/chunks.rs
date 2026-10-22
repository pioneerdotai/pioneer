//! Text chunk planning utilities shared by frame ingestion and preview code.
//!
//! This module provides both naive character-based chunking and structure-aware
//! chunking that preserves tables, code blocks, and other semantic units.
//!
//! Structure-aware chunking is automatically used when documents contain:
//! - Markdown tables (| ... |)
//! - Code blocks (```)
//!
//! Tables are split between rows with header propagation to ensure each chunk
//! maintains context about the table structure.

use crate::{
    normalize_text,
    structure::{ChunkingOptions, StructuralChunker, detect_structure},
    types::{TextChunkManifest, TextChunkRange},
};

pub(crate) const DEFAULT_CHUNK_CHARS: usize = 1_200;
pub(crate) const CHUNK_MIN_CHARS: usize = DEFAULT_CHUNK_CHARS * 2;

#[derive(Debug, Clone)]
pub(crate) struct DocumentChunkPlan {
    pub manifest: TextChunkManifest,
    pub chunks: Vec<String>,
}

pub(crate) fn plan_document_chunks(raw: &[u8]) -> Option<DocumentChunkPlan> {
    let Ok(text) = String::from_utf8(raw.to_vec()) else {
        return None;
    };
    plan_text_chunks(&text)
}

/// Plan chunks from already-extracted text (e.g., from PDF extraction).
/// This is used when the raw payload isn't valid UTF-8 but we have extracted text.
///
/// Automatically uses structure-aware chunking when tables or code blocks are
/// detected, ensuring that:
/// - Tables are split between rows (not mid-row)
/// - Table headers are propagated to continuation chunks
/// - Code blocks are kept whole when possible
pub(crate) fn plan_text_chunks(text: &str) -> Option<DocumentChunkPlan> {
    let normalized = normalize_text(text, usize::MAX)?.text;

    if normalized.chars().count() < CHUNK_MIN_CHARS {
        return None;
    }

    // Check for structural elements (tables, code blocks)
    let doc = detect_structure(&normalized);

    if doc.has_structure() {
        // Use structure-aware chunking for documents with tables/code
        plan_structural_chunks(&normalized, &doc)
    } else {
        // Fall back to naive chunking for plain text (faster)
        plan_naive_chunks(&normalized)
    }
}

/// Structure-aware chunking that preserves tables and code blocks.
fn plan_structural_chunks(
    text: &str,
    doc: &crate::structure::StructuredDocument,
) -> Option<DocumentChunkPlan> {
    let options = ChunkingOptions {
        max_chars: DEFAULT_CHUNK_CHARS,
        ..Default::default()
    };

    let chunker = StructuralChunker::new(options);
    let result = chunker.chunk(doc);

    if result.chunks.len() <= 1 {
        return None;
    }

    // Convert structural chunks to the format expected by ingestion
    let chunks: Vec<String> = result.chunks.iter().map(|c| c.text.clone()).collect();

    // Build manifest with accurate character ranges
    let manifest = build_manifest_from_structural(&result.chunks, text);

    Some(DocumentChunkPlan { manifest, chunks })
}

/// Build `TextChunkManifest` from structural chunks.
fn build_manifest_from_structural(
    chunks: &[crate::structure::StructuredChunk],
    text: &str,
) -> TextChunkManifest {
    let chunk_ranges: Vec<TextChunkRange> = chunks
        .iter()
        .map(|chunk| {
            // Use the character offsets from the structural chunk
            // Fall back to estimating if offsets seem off
            let start = chunk.char_start;
            let end = chunk.char_end.min(text.chars().count());
            TextChunkRange { start, end }
        })
        .collect();

    TextChunkManifest {
        chunk_chars: DEFAULT_CHUNK_CHARS,
        chunks: chunk_ranges,
    }
}

/// Naive character-based chunking (original implementation).
fn plan_naive_chunks(text: &str) -> Option<DocumentChunkPlan> {
    let manifest = build_chunk_manifest(text, DEFAULT_CHUNK_CHARS)?;
    if manifest.chunks.len() <= 1 {
        return None;
    }
    let chunks = manifest
        .chunks
        .iter()
        .map(|range| slice_text_range(text, range))
        .collect();
    Some(DocumentChunkPlan { manifest, chunks })
}

fn build_chunk_manifest(text: &str, chunk_chars: usize) -> Option<TextChunkManifest> {
    if chunk_chars == 0 {
        return None;
    }
    let mut char_positions: Vec<(usize, char)> = text.char_indices().collect();
    // Ensure we have an entry representing the end of the string for indexing convenience.
    char_positions.push((text.len(), '\0'));
    let total_chars = char_positions.len() - 1;
    if total_chars <= chunk_chars {
        return None;
    }

    let mut chunks: Vec<TextChunkRange> = Vec::new();
    let mut start = 0usize;
    let slack = (chunk_chars / 5).max(32);

    while start < total_chars {
        let target = (start + chunk_chars).min(total_chars);
        let end = choose_chunk_boundary(&char_positions, start, target, total_chars, slack);
        if end <= start {
            // Fallback to progress by at least one character to avoid infinite loops.
            let fallback_end = (start + chunk_chars).min(total_chars);
            chunks.push(TextChunkRange {
                start,
                end: fallback_end,
            });
            start = fallback_end;
        } else {
            chunks.push(TextChunkRange { start, end });
            start = end;
        }
    }

    Some(TextChunkManifest {
        chunk_chars,
        chunks,
    })
}

fn slice_text_range(text: &str, range: &TextChunkRange) -> String {
    if range.start >= range.end {
        return String::new();
    }
    text.chars()
        .skip(range.start)
        .take(range.end - range.start)
        .collect()
}

fn choose_chunk_boundary(
    chars: &[(usize, char)],
    start: usize,
    target: usize,
    total: usize,
    slack: usize,
) -> usize {
    if target >= total {
        return total;
    }

    let forward_limit = (target + slack).min(total);
    let mut candidates: Vec<usize> = Vec::new();

    for idx in target..forward_limit {
        let ch = chars[idx].1;
        if ch == '\n' {
            return idx + 1;
        }
        if is_sentence_terminal(ch) {
            candidates.push(idx + 1);
        }
    }

    for idx in (start..target).rev() {
        let ch = chars[idx].1;
        if ch == '\n' {
            return idx + 1;
        }
        if is_sentence_terminal(ch) {
            candidates.push(idx + 1);
            break;
        }
    }

    if let Some(choice) = candidates
        .into_iter()
        .min_by_key(|pos| pos.saturating_sub(target))
    {
        return choice;
    }

    for idx in target..forward_limit {
        if chars[idx].1.is_whitespace() {
            return idx + 1;
        }
    }

    for idx in (start..target).rev() {
        if chars[idx].1.is_whitespace() {
            return idx + 1;
        }
    }

    target
}

fn is_sentence_terminal(ch: char) -> bool {
    matches!(ch, '.' | '!' | '?')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_long_text_into_chunks() {
        let text = "Lorem ipsum dolor sit amet. ".repeat(200);
        let plan = plan_document_chunks(text.as_bytes()).expect("chunk plan");
        assert!(plan.manifest.chunks.len() > 1);
        assert_eq!(plan.manifest.chunk_chars, DEFAULT_CHUNK_CHARS);
        assert_eq!(plan.chunks.len(), plan.manifest.chunks.len());
    }

    #[test]
    fn structural_chunking_preserves_table_headers() {
        // Create a document with a large table
        let mut text = String::from("# Report\n\nThis is an introduction.\n\n");
        text.push_str("| Name | Department | Salary | Start Date |\n");
        text.push_str("|------|------------|--------|------------|\n");

        // Add many rows to ensure chunking
        for i in 1..=100 {
            text.push_str(&format!(
                "| Employee {} | Dept {} | ${} | 2024-{:02}-01 |\n",
                i,
                (i % 5) + 1,
                50000 + (i * 1000),
                (i % 12) + 1
            ));
        }
        text.push_str("\n\nThis is the conclusion.\n");

        let plan = plan_document_chunks(text.as_bytes()).expect("chunk plan");

        // Should have multiple chunks
        assert!(
            plan.chunks.len() > 1,
            "Large table should produce multiple chunks"
        );

        // Each chunk containing table data should have headers
        let table_chunks: Vec<_> = plan
            .chunks
            .iter()
            .filter(|c| c.contains("| Name |") || c.contains("| Employee"))
            .collect();

        assert!(
            table_chunks.len() > 1,
            "Table should be split into multiple chunks"
        );

        // Each table chunk should contain headers
        for chunk in &table_chunks {
            if chunk.contains("| Employee") {
                assert!(
                    chunk.contains("| Name |") || chunk.contains("Name |"),
                    "Table chunk should contain headers: {}",
                    &chunk[..chunk.len().min(200)]
                );
            }
        }
    }

    #[test]
    fn structural_chunking_keeps_small_table_whole() {
        let text = r"# Small Report

Introduction paragraph.

| Item | Price |
|------|-------|
| Apple | $1 |
| Orange | $2 |

Conclusion.
"
        .repeat(50); // Repeat to meet minimum size

        let plan = plan_document_chunks(text.as_bytes()).expect("chunk plan");

        // Small tables should not be split mid-row
        for chunk in &plan.chunks {
            // If a chunk contains a table row, it should be complete
            if chunk.contains("| Apple |") {
                // The row should be complete (not cut mid-way)
                assert!(
                    chunk.contains("| $1 |"),
                    "Table row should not be split mid-way"
                );
            }
        }
    }

    #[test]
    fn structural_chunking_detects_code_blocks() {
        let text = r"# Code Example

Here is some code:

```python
def process_data(items):
    result = []
    for item in items:
        if item.is_valid():
            result.append(item.transform())
    return result

class DataProcessor:
    def __init__(self):
        self.data = []

    def add(self, item):
        self.data.append(item)
```

More explanation here. "
            .repeat(20);

        let plan = plan_document_chunks(text.as_bytes()).expect("chunk plan");

        // Code blocks should be kept together when possible
        // Check that we have at least one chunk with a complete code block
        let has_complete_block = plan
            .chunks
            .iter()
            .any(|chunk| chunk.contains("```python") && chunk.contains("self.data.append"));

        assert!(
            has_complete_block,
            "At least one chunk should contain a complete code block"
        );
    }

    #[test]
    fn skips_short_text() {
        let text = "short snippet";
        assert!(plan_document_chunks(text.as_bytes()).is_none());
    }
}
