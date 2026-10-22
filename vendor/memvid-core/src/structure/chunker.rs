//! Structural chunker that respects document boundaries.
//!
//! The chunker takes a `StructuredDocument` and produces `StructuredChunk`s
//! that preserve semantic units. Tables are split between rows with header
//! propagation, code blocks are kept whole or split at boundaries, and
//! sections include their heading context.

use crate::types::structure::{
    ChunkType, ChunkingOptions, ChunkingResult, CodeChunkingStrategy, ElementData, StructuredChunk,
    StructuredDocument, StructuredTable, TableChunkingStrategy,
};

/// Structural chunker that respects document boundaries.
///
/// # Example
///
/// ```ignore
/// use memvid_core::structure::{StructuralChunker, ChunkingOptions, detect_structure};
///
/// let text = "| A | B |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |";
/// let doc = detect_structure(text);
///
/// let chunker = StructuralChunker::new(ChunkingOptions::default());
/// let result = chunker.chunk(&doc);
///
/// // Each chunk preserves table structure
/// for chunk in result.chunks {
///     println!("{}", chunk.text);
/// }
/// ```
pub struct StructuralChunker {
    options: ChunkingOptions,
}

impl Default for StructuralChunker {
    fn default() -> Self {
        Self::new(ChunkingOptions::default())
    }
}

impl StructuralChunker {
    /// Create a new chunker with the given options.
    #[must_use]
    pub fn new(options: ChunkingOptions) -> Self {
        Self { options }
    }

    /// Create a chunker with default options and custom max chars.
    #[must_use]
    pub fn with_max_chars(max_chars: usize) -> Self {
        Self {
            options: ChunkingOptions {
                max_chars,
                ..Default::default()
            },
        }
    }

    /// Chunk a structured document.
    #[must_use]
    pub fn chunk(&self, doc: &StructuredDocument) -> ChunkingResult {
        let mut result = ChunkingResult::empty();
        let mut current_text = String::new();
        let mut current_start = 0;
        let mut pending_heading: Option<&str> = None;

        for element in &doc.elements {
            match &element.data {
                ElementData::Table(table) => {
                    // Flush any pending text before table
                    if !current_text.trim().is_empty() {
                        self.emit_text_chunk(
                            &mut result,
                            &current_text,
                            current_start,
                            element.char_start,
                        );
                        current_text.clear();
                    }

                    // Chunk the table
                    self.chunk_table(&mut result, table, element.char_start, element.char_end);
                    current_start = element.char_end;
                }

                ElementData::CodeBlock(block) => {
                    // Flush pending text
                    if !current_text.trim().is_empty() {
                        self.emit_text_chunk(
                            &mut result,
                            &current_text,
                            current_start,
                            element.char_start,
                        );
                        current_text.clear();
                    }

                    // Chunk the code block
                    self.chunk_code_block(
                        &mut result,
                        &block.format(),
                        block.language.as_deref(),
                        element.char_start,
                        element.char_end,
                    );
                    current_start = element.char_end;
                }

                ElementData::Heading(heading) => {
                    if self.options.include_section_headers {
                        // Keep heading with following content
                        pending_heading = Some(heading.format().leak());
                    }

                    // Add heading to current text
                    if !current_text.is_empty() {
                        current_text.push('\n');
                    }
                    current_text.push_str(&heading.format());
                }

                ElementData::List(list) => {
                    if self.options.preserve_lists {
                        let list_text = list.format();
                        let combined_len = current_text.chars().count() + list_text.chars().count();

                        if combined_len > self.options.max_chars && !current_text.trim().is_empty()
                        {
                            // Flush current text before list
                            self.emit_text_chunk(
                                &mut result,
                                &current_text,
                                current_start,
                                element.char_start,
                            );
                            current_text.clear();
                            current_start = element.char_start;
                        }

                        // Add list to current text
                        if !current_text.is_empty() {
                            current_text.push_str("\n\n");
                        }
                        current_text.push_str(&list_text);
                    } else {
                        // Treat list as regular text
                        let text = element.text();
                        if !current_text.is_empty() {
                            current_text.push_str("\n\n");
                        }
                        current_text.push_str(&text);
                    }
                }

                ElementData::Paragraph { text } => {
                    let text_len = text.chars().count();
                    let current_len = current_text.chars().count();

                    if current_len + text_len > self.options.max_chars
                        && !current_text.trim().is_empty()
                    {
                        // Flush current chunk
                        self.emit_text_chunk(
                            &mut result,
                            &current_text,
                            current_start,
                            element.char_start,
                        );
                        current_text.clear();
                        current_start = element.char_start;

                        // Add pending heading context if any
                        if let Some(heading) = pending_heading.take() {
                            current_text.push_str(heading);
                            current_text.push_str("\n\n");
                        }
                    }

                    if !current_text.is_empty() && !current_text.ends_with('\n') {
                        current_text.push_str("\n\n");
                    }
                    current_text.push_str(text);
                }

                ElementData::BlockQuote { text } => {
                    if !current_text.is_empty() {
                        current_text.push_str("\n\n");
                    }
                    current_text.push_str("> ");
                    current_text.push_str(text);
                }

                ElementData::Separator => {
                    // Treat separator as a natural chunk break
                    if !current_text.trim().is_empty() {
                        self.emit_text_chunk(
                            &mut result,
                            &current_text,
                            current_start,
                            element.char_start,
                        );
                        current_text.clear();
                    }
                    current_start = element.char_end;
                    pending_heading = None;
                }

                ElementData::Raw { text } => {
                    if !current_text.is_empty() {
                        current_text.push_str("\n\n");
                    }
                    current_text.push_str(text);
                }
            }
        }

        // Flush remaining text
        if !current_text.trim().is_empty() {
            self.emit_text_chunk(&mut result, &current_text, current_start, doc.total_chars);
        }

        result
    }

    /// Emit a text chunk.
    fn emit_text_chunk(
        &self,
        result: &mut ChunkingResult,
        text: &str,
        char_start: usize,
        char_end: usize,
    ) {
        let index = result.chunks.len();
        result.chunks.push(StructuredChunk::text(
            text.trim(),
            index,
            char_start,
            char_end,
        ));
    }

    /// Chunk a table with header propagation.
    fn chunk_table(
        &self,
        result: &mut ChunkingResult,
        table: &StructuredTable,
        char_start: usize,
        char_end: usize,
    ) {
        result.tables_processed += 1;

        match self.options.table_handling {
            TableChunkingStrategy::PreserveWhole => {
                // Keep entire table as one chunk (may exceed max_chars)
                let index = result.chunks.len();
                result.chunks.push(StructuredChunk::table(
                    &table.raw_text,
                    index,
                    &table.id,
                    char_start,
                    char_end,
                ));
            }

            TableChunkingStrategy::SplitWithHeader => {
                // Split table between rows, prepend header to each chunk
                let header_text = table.format_header();
                let header_chars = header_text.chars().count();

                // If entire table fits, emit as single chunk
                if table.char_count() <= self.options.max_chars {
                    let index = result.chunks.len();
                    result.chunks.push(StructuredChunk::table(
                        &table.raw_text,
                        index,
                        &table.id,
                        char_start,
                        char_end,
                    ));
                    return;
                }

                // Split by rows
                result.tables_split += 1;
                let data_rows: Vec<_> = table.data_rows().collect();

                if data_rows.is_empty() {
                    // Only header, emit as-is
                    let index = result.chunks.len();
                    result.chunks.push(StructuredChunk::table(
                        &header_text,
                        index,
                        &table.id,
                        char_start,
                        char_end,
                    ));
                    return;
                }

                let max_rows_per_chunk = self.calculate_rows_per_chunk(table, header_chars);
                let total_parts = data_rows.len().div_ceil(max_rows_per_chunk);

                let mut part = 1;
                let mut row_idx = 0;

                while row_idx < data_rows.len() {
                    let end_idx = (row_idx + max_rows_per_chunk).min(data_rows.len());
                    let rows_in_chunk = &data_rows[row_idx..end_idx];

                    // Build chunk text: header + rows
                    let mut chunk_text = header_text.clone();
                    for row in rows_in_chunk {
                        chunk_text.push('\n');
                        chunk_text.push_str(&table.format_row(row));
                    }

                    let index = result.chunks.len();
                    if part == 1 {
                        // First part is a Table chunk
                        result.chunks.push(StructuredChunk::table(
                            &chunk_text,
                            index,
                            &table.id,
                            char_start,
                            char_end,
                        ));
                    } else {
                        // Subsequent parts are TableContinuation chunks
                        result.chunks.push(StructuredChunk::table_continuation(
                            &chunk_text,
                            index,
                            &table.id,
                            part as u32,
                            u32::try_from(total_parts).unwrap_or(0),
                            &header_text,
                            char_start,
                            char_end,
                        ));
                    }

                    row_idx = end_idx;
                    part += 1;
                }
            }

            TableChunkingStrategy::Naive => {
                // Just treat table as text (not recommended)
                let index = result.chunks.len();
                result.chunks.push(StructuredChunk::text(
                    &table.raw_text,
                    index,
                    char_start,
                    char_end,
                ));
            }
        }
    }

    /// Calculate how many rows fit per chunk given header overhead.
    fn calculate_rows_per_chunk(&self, table: &StructuredTable, header_chars: usize) -> usize {
        let available = self.options.max_chars.saturating_sub(header_chars + 10);
        if available == 0 {
            return 1;
        }

        // Estimate average row size
        let total_row_chars: usize = table
            .data_rows()
            .map(|row| {
                row.cells
                    .iter()
                    .map(|c| c.text.chars().count())
                    .sum::<usize>()
                    + row.cells.len() * 3 // | separators
            })
            .sum();

        let row_count = table.data_row_count();
        if row_count == 0 {
            return 1;
        }

        let avg_row_chars = total_row_chars / row_count;
        if avg_row_chars == 0 {
            return row_count;
        }

        (available / avg_row_chars).max(1)
    }

    /// Chunk a code block.
    fn chunk_code_block(
        &self,
        result: &mut ChunkingResult,
        formatted_text: &str,
        language: Option<&str>,
        char_start: usize,
        char_end: usize,
    ) {
        result.code_blocks_processed += 1;

        match self.options.code_handling {
            CodeChunkingStrategy::PreserveWhole => {
                // Keep entire code block as one chunk
                let index = result.chunks.len();
                result.chunks.push(StructuredChunk {
                    text: formatted_text.to_string(),
                    chunk_type: ChunkType::CodeBlock,
                    index,
                    element_id: None,
                    part: None,
                    total_parts: None,
                    context: language.map(std::string::ToString::to_string),
                    char_start,
                    char_end,
                });
            }

            CodeChunkingStrategy::SplitAtBoundaries => {
                // Try to split at function/block boundaries
                let block_chars = formatted_text.chars().count();
                if block_chars <= self.options.max_chars {
                    // Fits in one chunk
                    let index = result.chunks.len();
                    result.chunks.push(StructuredChunk {
                        text: formatted_text.to_string(),
                        chunk_type: ChunkType::CodeBlock,
                        index,
                        element_id: None,
                        part: None,
                        total_parts: None,
                        context: language.map(std::string::ToString::to_string),
                        char_start,
                        char_end,
                    });
                } else {
                    // Split at function boundaries or fall back to line boundaries
                    self.split_code_at_boundaries(
                        result,
                        formatted_text,
                        language,
                        char_start,
                        char_end,
                    );
                }
            }

            CodeChunkingStrategy::SplitWithOverlap => {
                // Split with overlap for context
                self.split_code_with_overlap(
                    result,
                    formatted_text,
                    language,
                    char_start,
                    char_end,
                );
            }
        }
    }

    /// Split code at function/block boundaries.
    fn split_code_at_boundaries(
        &self,
        result: &mut ChunkingResult,
        formatted_text: &str,
        language: Option<&str>,
        char_start: usize,
        char_end: usize,
    ) {
        // Simple heuristic: split at empty lines that likely indicate function boundaries
        let lines: Vec<&str> = formatted_text.lines().collect();
        let mut chunks = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_chars = 0;

        // Find fence markers to preserve
        let fence_start = lines.first().copied().unwrap_or("```");
        let fence_end = lines.last().copied().unwrap_or("```");
        let content_lines = &lines[1..lines.len().saturating_sub(1)];

        for (i, line) in content_lines.iter().enumerate() {
            let line_chars = line.chars().count() + 1;

            // Check for good split point (empty line or function start)
            let is_boundary = line.trim().is_empty()
                || line.trim().starts_with("fn ")
                || line.trim().starts_with("def ")
                || line.trim().starts_with("function ")
                || line.trim().starts_with("class ")
                || line.trim().starts_with("impl ");

            if is_boundary && current_chars > self.options.max_chars / 2 && i > 0 {
                // Emit current chunk
                if !current_chunk.is_empty() {
                    chunks.push(current_chunk.join("\n"));
                    current_chunk.clear();
                    current_chars = 0;
                }
            }

            current_chunk.push(*line);
            current_chars += line_chars;
        }

        // Emit remaining
        if !current_chunk.is_empty() {
            chunks.push(current_chunk.join("\n"));
        }

        // Emit as continuation chunks
        let total_parts = chunks.len();
        for (i, chunk_content) in chunks.into_iter().enumerate() {
            let index = result.chunks.len();
            let chunk_text = format!(
                "{}{}\n{}\n{}",
                fence_start,
                language.unwrap_or(""),
                chunk_content,
                fence_end
            );

            if i == 0 {
                result.chunks.push(StructuredChunk {
                    text: chunk_text,
                    chunk_type: ChunkType::CodeBlock,
                    index,
                    element_id: None,
                    part: Some(1),
                    total_parts: Some(u32::try_from(total_parts).unwrap_or(0)),
                    context: language.map(std::string::ToString::to_string),
                    char_start,
                    char_end,
                });
            } else {
                result.chunks.push(StructuredChunk {
                    text: chunk_text,
                    chunk_type: ChunkType::CodeBlockContinuation,
                    index,
                    element_id: None,
                    part: Some(u32::try_from(i + 1).unwrap_or(0)),
                    total_parts: Some(u32::try_from(total_parts).unwrap_or(0)),
                    context: language.map(std::string::ToString::to_string),
                    char_start,
                    char_end,
                });
            }
        }
    }

    /// Split code with overlap for context.
    fn split_code_with_overlap(
        &self,
        result: &mut ChunkingResult,
        formatted_text: &str,
        language: Option<&str>,
        char_start: usize,
        char_end: usize,
    ) {
        let lines: Vec<&str> = formatted_text.lines().collect();
        let overlap_lines = (self.options.overlap_chars / 40).max(2);

        // Find fence markers
        let fence_start = lines.first().copied().unwrap_or("```");
        let fence_end = lines.last().copied().unwrap_or("```");
        let content_lines = &lines[1..lines.len().saturating_sub(1)];

        let mut chunks = Vec::new();
        let mut start_line = 0;

        while start_line < content_lines.len() {
            let mut current_chars = 0;
            let mut end_line = start_line;

            while end_line < content_lines.len() {
                current_chars += content_lines[end_line].chars().count() + 1;
                if current_chars > self.options.max_chars {
                    break;
                }
                end_line += 1;
            }

            if end_line == start_line {
                end_line = start_line + 1;
            }

            let chunk_lines: Vec<&str> = content_lines[start_line..end_line].to_vec();
            chunks.push(chunk_lines.join("\n"));

            // Move forward with overlap
            start_line = if end_line >= content_lines.len() {
                content_lines.len()
            } else {
                end_line.saturating_sub(overlap_lines)
            };
        }

        // Emit chunks
        let total_parts = chunks.len();
        for (i, chunk_content) in chunks.into_iter().enumerate() {
            let index = result.chunks.len();
            let chunk_text = format!(
                "{}{}\n{}\n{}",
                fence_start,
                language.unwrap_or(""),
                chunk_content,
                fence_end
            );

            let chunk_type = if i == 0 {
                ChunkType::CodeBlock
            } else {
                ChunkType::CodeBlockContinuation
            };

            result.chunks.push(StructuredChunk {
                text: chunk_text,
                chunk_type,
                index,
                element_id: None,
                part: Some(u32::try_from(i + 1).unwrap_or(0)),
                total_parts: Some(u32::try_from(total_parts).unwrap_or(0)),
                context: language.map(std::string::ToString::to_string),
                char_start,
                char_end,
            });
        }
    }
}

/// Convenience function to chunk text with default options.
#[must_use]
pub fn chunk_structured(doc: &StructuredDocument) -> ChunkingResult {
    StructuralChunker::default().chunk(doc)
}

/// Convenience function to chunk text with custom max chars.
#[must_use]
pub fn chunk_structured_with_max(doc: &StructuredDocument, max_chars: usize) -> ChunkingResult {
    StructuralChunker::with_max_chars(max_chars).chunk(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structure::detect_structure;

    #[test]
    fn test_simple_text_chunking() {
        let text = "This is a simple paragraph.\n\nAnother paragraph here.";
        let doc = detect_structure(text);
        let result = chunk_structured(&doc);

        assert!(!result.chunks.is_empty());
        assert_eq!(result.tables_processed, 0);
    }

    #[test]
    fn test_table_preserved_when_small() {
        let text = r"Introduction.

| Name | Age |
|------|-----|
| Alice | 30 |
| Bob | 25 |

Conclusion.";

        let doc = detect_structure(text);
        let result = chunk_structured(&doc);

        // Table should be in one chunk
        let table_chunks: Vec<_> = result.chunks.iter().filter(|c| c.is_table()).collect();

        assert_eq!(table_chunks.len(), 1);
        assert_eq!(result.tables_processed, 1);
        assert_eq!(result.tables_split, 0);
    }

    #[test]
    fn test_large_table_split_with_header() {
        // Create a table that exceeds max_chars
        let mut rows = String::new();
        for i in 1..=50 {
            rows.push_str(&format!(
                "| Row {} with some data | More data here | Even more |\n",
                i
            ));
        }

        let text = format!(
            r"Introduction.

| Column A | Column B | Column C |
|----------|----------|----------|
{}
Conclusion.",
            rows
        );

        let doc = detect_structure(&text);
        let chunker = StructuralChunker::with_max_chars(500);
        let result = chunker.chunk(&doc);

        // Table should be split
        let table_chunks: Vec<_> = result.chunks.iter().filter(|c| c.is_table()).collect();

        assert!(table_chunks.len() > 1, "Large table should be split");
        assert_eq!(result.tables_split, 1);

        // Each chunk should contain header
        for chunk in &table_chunks {
            assert!(
                chunk.text.contains("| Column A |"),
                "Each table chunk should contain header"
            );
        }

        // Continuation chunks should have context
        for chunk in table_chunks.iter().skip(1) {
            assert_eq!(chunk.chunk_type, ChunkType::TableContinuation);
            assert!(chunk.context.is_some());
        }
    }

    #[test]
    fn test_code_block_preserved() {
        let text = r#"Here is code:

```rust
fn main() {
    println!("Hello!");
}
```

Done."#;

        let doc = detect_structure(text);
        let result = chunk_structured(&doc);

        let code_chunks: Vec<_> = result
            .chunks
            .iter()
            .filter(|c| matches!(c.chunk_type, ChunkType::CodeBlock))
            .collect();

        assert_eq!(code_chunks.len(), 1);
        assert!(code_chunks[0].text.contains("fn main()"));
    }

    #[test]
    fn test_mixed_content() {
        let text = r#"# Report

## Summary

This is the summary section.

| Item | Count |
|------|-------|
| A    | 10    |
| B    | 20    |

## Code

```python
def hello():
    print("Hello")
```

## Conclusion

All done."#;

        let doc = detect_structure(text);
        let result = chunk_structured(&doc);

        assert!(result.tables_processed >= 1);
        assert!(result.code_blocks_processed >= 1);
        assert!(result.chunks.len() >= 3);
    }

    #[test]
    fn test_table_header_formatting() {
        let text = r"| Col1 | Col2 | Col3 |
|------|------|------|
| A1   | A2   | A3   |
| B1   | B2   | B3   |";

        let doc = detect_structure(text);
        let table = doc.tables().next().unwrap();

        let header = table.format_header();
        assert!(header.contains("| Col1 | Col2 | Col3 |"));
        assert!(header.contains("|---|---|---|"));
    }

    #[test]
    fn test_preserve_whole_strategy() {
        let mut rows = String::new();
        for i in 1..=20 {
            rows.push_str(&format!("| Data {} | Value |\n", i));
        }

        let text = format!(
            r"| Header1 | Header2 |
|---------|---------|
{}",
            rows
        );

        let doc = detect_structure(&text);
        let chunker = StructuralChunker::new(ChunkingOptions {
            max_chars: 500,
            table_handling: TableChunkingStrategy::PreserveWhole,
            ..Default::default()
        });
        let result = chunker.chunk(&doc);

        // Table should NOT be split with PreserveWhole
        let table_chunks: Vec<_> = result.chunks.iter().filter(|c| c.is_table()).collect();

        assert_eq!(table_chunks.len(), 1);
        assert_eq!(result.tables_split, 0);
    }

    #[test]
    fn test_chunking_result_stats() {
        let text = r"| A | B |
|---|---|
| 1 | 2 |

```python
x = 1
```

| C | D |
|---|---|
| 3 | 4 |";

        let doc = detect_structure(text);
        let result = chunk_structured(&doc);

        assert_eq!(result.tables_processed, 2);
        assert_eq!(result.code_blocks_processed, 1);
    }
}
