//! Structured document types for intelligent chunking.
//!
//! This module defines types for representing document structure, enabling
//! structure-aware chunking that preserves semantic units like tables, code
//! blocks, and sections.
//!
//! # Problem
//!
//! Naive text chunking splits documents by character/token count without
//! understanding structure. This causes:
//! - Tables split mid-row, losing header context
//! - Code blocks split mid-function
//! - Lists fragmented across chunks
//!
//! # Solution
//!
//! We extract document structure first, then chunk respecting boundaries:
//! - Tables: Keep whole or split between rows with header propagation
//! - Code: Keep whole or split at function/block boundaries
//! - Sections: Include header context with content chunks
//!
//! # Example
//!
//! ```ignore
//! use memvid_core::types::structure::*;
//!
//! let doc = StructuredDocument::parse(text);
//! let chunks = StructuralChunker::new(1200).chunk(&doc);
//!
//! for chunk in chunks {
//!     match chunk.chunk_type {
//!         ChunkType::Table => println!("Table chunk with {} rows", chunk.row_count),
//!         ChunkType::TableContinuation => println!("Table part {}/{}", chunk.part, chunk.total),
//!         _ => println!("Content chunk"),
//!     }
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

/// Type of document element detected during structure analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ElementType {
    /// Regular paragraph text
    #[default]
    Paragraph,
    /// Table with rows and columns
    Table,
    /// Fenced or indented code block
    CodeBlock,
    /// Ordered or unordered list
    List,
    /// Section heading (h1-h6)
    Heading,
    /// Block quote
    BlockQuote,
    /// Horizontal rule / separator
    Separator,
    /// Raw/unknown content
    Raw,
}

impl fmt::Display for ElementType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Paragraph => write!(f, "paragraph"),
            Self::Table => write!(f, "table"),
            Self::CodeBlock => write!(f, "code_block"),
            Self::List => write!(f, "list"),
            Self::Heading => write!(f, "heading"),
            Self::BlockQuote => write!(f, "block_quote"),
            Self::Separator => write!(f, "separator"),
            Self::Raw => write!(f, "raw"),
        }
    }
}

/// Type of chunk produced by structural chunking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ChunkType {
    /// Regular text chunk (may contain multiple paragraphs)
    #[default]
    Text,
    /// Complete table (fits in one chunk)
    Table,
    /// Table continuation with header prepended
    TableContinuation,
    /// Complete code block
    CodeBlock,
    /// Code block continuation
    CodeBlockContinuation,
    /// List (complete or partial)
    List,
    /// Section heading with following content
    Section,
    /// Mixed content (fallback)
    Mixed,
}

impl fmt::Display for ChunkType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text => write!(f, "text"),
            Self::Table => write!(f, "table"),
            Self::TableContinuation => write!(f, "table_continuation"),
            Self::CodeBlock => write!(f, "code_block"),
            Self::CodeBlockContinuation => write!(f, "code_block_continuation"),
            Self::List => write!(f, "list"),
            Self::Section => write!(f, "section"),
            Self::Mixed => write!(f, "mixed"),
        }
    }
}

/// A table cell with optional span information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredCell {
    /// Text content of the cell
    pub text: String,
    /// Column index (0-based)
    pub col: usize,
    /// Row span (default 1)
    #[serde(default = "default_span")]
    pub row_span: usize,
    /// Column span (default 1)
    #[serde(default = "default_span")]
    pub col_span: usize,
}

fn default_span() -> usize {
    1
}

impl StructuredCell {
    /// Create a simple cell with text content.
    pub fn new(text: impl Into<String>, col: usize) -> Self {
        Self {
            text: text.into(),
            col,
            row_span: 1,
            col_span: 1,
        }
    }
}

/// A table row containing cells.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredRow {
    /// Row index (0-based)
    pub row: usize,
    /// Cells in this row
    pub cells: Vec<StructuredCell>,
    /// Whether this is a header row
    #[serde(default)]
    pub is_header: bool,
}

impl StructuredRow {
    /// Create a new row with cells.
    #[must_use]
    pub fn new(row: usize, cells: Vec<StructuredCell>) -> Self {
        Self {
            row,
            cells,
            is_header: false,
        }
    }

    /// Mark as header row.
    #[must_use]
    pub fn as_header(mut self) -> Self {
        self.is_header = true;
        self
    }

    /// Get cell texts as a vector.
    #[must_use]
    pub fn cell_texts(&self) -> Vec<&str> {
        self.cells.iter().map(|c| c.text.as_str()).collect()
    }
}

/// A structured table extracted from document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredTable {
    /// Unique identifier for this table
    pub id: String,
    /// Column headers (may be empty if no header row detected)
    pub headers: Vec<String>,
    /// All rows including header row
    pub rows: Vec<StructuredRow>,
    /// Number of columns
    pub n_cols: usize,
    /// Original text representation (markdown/ASCII)
    pub raw_text: String,
    /// Caption if detected
    pub caption: Option<String>,
}

impl StructuredTable {
    /// Create a new table with ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            headers: Vec::new(),
            rows: Vec::new(),
            n_cols: 0,
            raw_text: String::new(),
            caption: None,
        }
    }

    /// Get data rows (excluding header rows).
    pub fn data_rows(&self) -> impl Iterator<Item = &StructuredRow> {
        self.rows.iter().filter(|r| !r.is_header)
    }

    /// Get number of data rows.
    #[must_use]
    pub fn data_row_count(&self) -> usize {
        self.rows.iter().filter(|r| !r.is_header).count()
    }

    /// Format headers as markdown table header.
    #[must_use]
    pub fn format_header(&self) -> String {
        if self.headers.is_empty() {
            return String::new();
        }
        let header_row = format!("| {} |", self.headers.join(" | "));
        let separator = format!(
            "|{}|",
            self.headers
                .iter()
                .map(|_| "---")
                .collect::<Vec<_>>()
                .join("|")
        );
        format!("{header_row}\n{separator}")
    }

    /// Format a row as markdown.
    #[must_use]
    pub fn format_row(&self, row: &StructuredRow) -> String {
        let cells: Vec<&str> = row.cells.iter().map(|c| c.text.as_str()).collect();
        format!("| {} |", cells.join(" | "))
    }

    /// Estimate character count.
    #[must_use]
    pub fn char_count(&self) -> usize {
        self.raw_text.chars().count()
    }
}

/// A code block with language info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredCodeBlock {
    /// Programming language (if specified)
    pub language: Option<String>,
    /// Code content
    pub content: String,
    /// Whether this is indented code (vs fenced)
    #[serde(default)]
    pub is_indented: bool,
}

impl StructuredCodeBlock {
    /// Create a new code block.
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            language: None,
            content: content.into(),
            is_indented: false,
        }
    }

    /// Set language.
    pub fn with_language(mut self, lang: impl Into<String>) -> Self {
        self.language = Some(lang.into());
        self
    }

    /// Format as fenced code block.
    #[must_use]
    pub fn format(&self) -> String {
        let fence = "```";
        let lang = self.language.as_deref().unwrap_or("");
        format!("{}{}\n{}\n{}", fence, lang, self.content, fence)
    }

    /// Estimate character count.
    #[must_use]
    pub fn char_count(&self) -> usize {
        self.content.chars().count() + 10 // fences + language
    }
}

/// A list with items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredList {
    /// Whether this is an ordered list
    pub ordered: bool,
    /// List items (may contain nested content)
    pub items: Vec<String>,
    /// Starting number for ordered lists
    #[serde(default = "default_start")]
    pub start: usize,
}

fn default_start() -> usize {
    1
}

impl StructuredList {
    /// Create a new unordered list.
    #[must_use]
    pub fn unordered(items: Vec<String>) -> Self {
        Self {
            ordered: false,
            items,
            start: 1,
        }
    }

    /// Create a new ordered list.
    #[must_use]
    pub fn ordered(items: Vec<String>) -> Self {
        Self {
            ordered: true,
            items,
            start: 1,
        }
    }

    /// Format as markdown list.
    #[must_use]
    pub fn format(&self) -> String {
        self.items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                if self.ordered {
                    format!("{}. {}", self.start + i, item)
                } else {
                    format!("- {item}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Estimate character count.
    #[must_use]
    pub fn char_count(&self) -> usize {
        self.items.iter().map(|s| s.chars().count() + 3).sum()
    }
}

/// A section heading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredHeading {
    /// Heading level (1-6)
    pub level: u8,
    /// Heading text
    pub text: String,
}

impl StructuredHeading {
    /// Create a new heading.
    pub fn new(level: u8, text: impl Into<String>) -> Self {
        Self {
            level: level.min(6).max(1),
            text: text.into(),
        }
    }

    /// Format as markdown heading.
    #[must_use]
    pub fn format(&self) -> String {
        format!("{} {}", "#".repeat(self.level as usize), self.text)
    }
}

/// A document element with position information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentElement {
    /// Type of element
    pub element_type: ElementType,
    /// Character offset where element starts
    pub char_start: usize,
    /// Character offset where element ends
    pub char_end: usize,
    /// Element-specific data
    pub data: ElementData,
}

/// Element-specific data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ElementData {
    /// Paragraph text
    Paragraph { text: String },
    /// Structured table
    Table(StructuredTable),
    /// Code block
    CodeBlock(StructuredCodeBlock),
    /// List
    List(StructuredList),
    /// Heading
    Heading(StructuredHeading),
    /// Block quote
    BlockQuote { text: String },
    /// Separator
    Separator,
    /// Raw content
    Raw { text: String },
}

impl DocumentElement {
    /// Create a paragraph element.
    pub fn paragraph(text: impl Into<String>, char_start: usize, char_end: usize) -> Self {
        let text = text.into();
        Self {
            element_type: ElementType::Paragraph,
            char_start,
            char_end,
            data: ElementData::Paragraph { text },
        }
    }

    /// Create a table element.
    #[must_use]
    pub fn table(table: StructuredTable, char_start: usize, char_end: usize) -> Self {
        Self {
            element_type: ElementType::Table,
            char_start,
            char_end,
            data: ElementData::Table(table),
        }
    }

    /// Create a code block element.
    #[must_use]
    pub fn code_block(block: StructuredCodeBlock, char_start: usize, char_end: usize) -> Self {
        Self {
            element_type: ElementType::CodeBlock,
            char_start,
            char_end,
            data: ElementData::CodeBlock(block),
        }
    }

    /// Create a list element.
    #[must_use]
    pub fn list(list: StructuredList, char_start: usize, char_end: usize) -> Self {
        Self {
            element_type: ElementType::List,
            char_start,
            char_end,
            data: ElementData::List(list),
        }
    }

    /// Create a heading element.
    #[must_use]
    pub fn heading(heading: StructuredHeading, char_start: usize, char_end: usize) -> Self {
        Self {
            element_type: ElementType::Heading,
            char_start,
            char_end,
            data: ElementData::Heading(heading),
        }
    }

    /// Get element text content.
    #[must_use]
    pub fn text(&self) -> String {
        match &self.data {
            ElementData::Paragraph { text } => text.clone(),
            ElementData::Table(t) => t.raw_text.clone(),
            ElementData::CodeBlock(c) => c.format(),
            ElementData::List(l) => l.format(),
            ElementData::Heading(h) => h.format(),
            ElementData::BlockQuote { text } => text.clone(),
            ElementData::Separator => "---".to_string(),
            ElementData::Raw { text } => text.clone(),
        }
    }

    /// Get element character count.
    #[must_use]
    pub fn char_count(&self) -> usize {
        self.char_end.saturating_sub(self.char_start)
    }

    /// Check if this is a table element.
    #[must_use]
    pub fn is_table(&self) -> bool {
        self.element_type == ElementType::Table
    }

    /// Get table data if this is a table element.
    #[must_use]
    pub fn as_table(&self) -> Option<&StructuredTable> {
        match &self.data {
            ElementData::Table(t) => Some(t),
            _ => None,
        }
    }

    /// Get mutable table data if this is a table element.
    pub fn as_table_mut(&mut self) -> Option<&mut StructuredTable> {
        match &mut self.data {
            ElementData::Table(t) => Some(t),
            _ => None,
        }
    }
}

/// A document with extracted structure.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StructuredDocument {
    /// Detected elements in document order
    pub elements: Vec<DocumentElement>,
    /// Original raw text (for fallback)
    pub raw_text: String,
    /// Source filename
    pub source: Option<String>,
    /// Total character count
    pub total_chars: usize,
    /// Number of tables detected
    pub table_count: usize,
    /// Number of code blocks detected
    pub code_block_count: usize,
}

impl StructuredDocument {
    /// Create a new empty structured document.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from raw text (will be parsed for structure).
    pub fn from_text(text: impl Into<String>) -> Self {
        let raw_text = text.into();
        let total_chars = raw_text.chars().count();
        Self {
            elements: Vec::new(),
            raw_text,
            source: None,
            total_chars,
            table_count: 0,
            code_block_count: 0,
        }
    }

    /// Set source filename.
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Add an element.
    pub fn add_element(&mut self, element: DocumentElement) {
        match element.element_type {
            ElementType::Table => self.table_count += 1,
            ElementType::CodeBlock => self.code_block_count += 1,
            _ => {}
        }
        self.elements.push(element);
    }

    /// Get all tables in document.
    pub fn tables(&self) -> impl Iterator<Item = &StructuredTable> {
        self.elements.iter().filter_map(|e| e.as_table())
    }

    /// Check if document has any structure (vs plain text).
    #[must_use]
    pub fn has_structure(&self) -> bool {
        self.table_count > 0 || self.code_block_count > 0
    }

    /// Update counts after modification.
    pub fn update_counts(&mut self) {
        self.table_count = self.elements.iter().filter(|e| e.is_table()).count();
        self.code_block_count = self
            .elements
            .iter()
            .filter(|e| e.element_type == ElementType::CodeBlock)
            .count();
        self.total_chars = self.raw_text.chars().count();
    }
}

/// A chunk produced by structural chunking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredChunk {
    /// Text content of the chunk
    pub text: String,
    /// Type of chunk
    pub chunk_type: ChunkType,
    /// Index in chunk sequence (0-based)
    pub index: usize,
    /// Source element ID (for tables, code blocks)
    pub element_id: Option<String>,
    /// Part number if split from larger element (1-based)
    pub part: Option<u32>,
    /// Total parts if split from larger element
    pub total_parts: Option<u32>,
    /// Context header (e.g., table headers for continuation chunks)
    pub context: Option<String>,
    /// Character offset in original document
    pub char_start: usize,
    /// Character end offset
    pub char_end: usize,
}

impl StructuredChunk {
    /// Create a new text chunk.
    pub fn text(text: impl Into<String>, index: usize, char_start: usize, char_end: usize) -> Self {
        Self {
            text: text.into(),
            chunk_type: ChunkType::Text,
            index,
            element_id: None,
            part: None,
            total_parts: None,
            context: None,
            char_start,
            char_end,
        }
    }

    /// Create a table chunk.
    pub fn table(
        text: impl Into<String>,
        index: usize,
        table_id: impl Into<String>,
        char_start: usize,
        char_end: usize,
    ) -> Self {
        Self {
            text: text.into(),
            chunk_type: ChunkType::Table,
            index,
            element_id: Some(table_id.into()),
            part: None,
            total_parts: None,
            context: None,
            char_start,
            char_end,
        }
    }

    /// Create a table continuation chunk.
    pub fn table_continuation(
        text: impl Into<String>,
        index: usize,
        table_id: impl Into<String>,
        part: u32,
        total_parts: u32,
        header_context: impl Into<String>,
        char_start: usize,
        char_end: usize,
    ) -> Self {
        Self {
            text: text.into(),
            chunk_type: ChunkType::TableContinuation,
            index,
            element_id: Some(table_id.into()),
            part: Some(part),
            total_parts: Some(total_parts),
            context: Some(header_context.into()),
            char_start,
            char_end,
        }
    }

    /// Check if this is a table-related chunk.
    #[must_use]
    pub fn is_table(&self) -> bool {
        matches!(
            self.chunk_type,
            ChunkType::Table | ChunkType::TableContinuation
        )
    }

    /// Check if this is a continuation of a split element.
    #[must_use]
    pub fn is_continuation(&self) -> bool {
        self.part.is_some_and(|p| p > 1)
    }

    /// Get character count.
    #[must_use]
    pub fn char_count(&self) -> usize {
        self.text.chars().count()
    }
}

/// Options for structural chunking.
#[derive(Debug, Clone)]
pub struct ChunkingOptions {
    /// Maximum characters per chunk
    pub max_chars: usize,
    /// How to handle tables
    pub table_handling: TableChunkingStrategy,
    /// How to handle code blocks
    pub code_handling: CodeChunkingStrategy,
    /// Whether to preserve list structure
    pub preserve_lists: bool,
    /// Include section headers with content
    pub include_section_headers: bool,
    /// Overlap between chunks (for context)
    pub overlap_chars: usize,
}

impl Default for ChunkingOptions {
    fn default() -> Self {
        Self {
            max_chars: 1200,
            table_handling: TableChunkingStrategy::SplitWithHeader,
            code_handling: CodeChunkingStrategy::PreserveWhole,
            preserve_lists: true,
            include_section_headers: true,
            overlap_chars: 0,
        }
    }
}

/// Strategy for chunking tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableChunkingStrategy {
    /// Keep entire table in one chunk (may exceed `max_chars`)
    PreserveWhole,
    /// Split table between rows, prepend header to each chunk
    #[default]
    SplitWithHeader,
    /// Naive splitting (not recommended)
    Naive,
}

/// Strategy for chunking code blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CodeChunkingStrategy {
    /// Keep entire code block in one chunk (may exceed `max_chars`)
    #[default]
    PreserveWhole,
    /// Split at function/block boundaries (if detectable)
    SplitAtBoundaries,
    /// Naive splitting with overlap
    SplitWithOverlap,
}

/// Result of structural chunking.
#[derive(Debug, Clone, Default)]
pub struct ChunkingResult {
    /// Produced chunks
    pub chunks: Vec<StructuredChunk>,
    /// Number of tables processed
    pub tables_processed: usize,
    /// Number of tables that were split
    pub tables_split: usize,
    /// Number of code blocks processed
    pub code_blocks_processed: usize,
    /// Warnings during chunking
    pub warnings: Vec<String>,
}

impl ChunkingResult {
    /// Create empty result.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Total chunks produced.
    #[must_use]
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Add a warning.
    pub fn warn(&mut self, msg: impl Into<String>) {
        self.warnings.push(msg.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_structured_table_creation() {
        let mut table = StructuredTable::new("tbl_001");
        table.headers = vec!["Name".to_string(), "Age".to_string()];
        table.n_cols = 2;

        let row = StructuredRow::new(
            0,
            vec![
                StructuredCell::new("Alice", 0),
                StructuredCell::new("30", 1),
            ],
        );
        table.rows.push(row);

        assert_eq!(table.data_row_count(), 1);
        assert!(table.format_header().contains("Name"));
    }

    #[test]
    fn test_structured_chunk_types() {
        let text_chunk = StructuredChunk::text("Hello world", 0, 0, 11);
        assert!(!text_chunk.is_table());
        assert!(!text_chunk.is_continuation());

        let table_chunk = StructuredChunk::table_continuation(
            "| A | B |",
            1,
            "tbl_001",
            2,
            3,
            "| Col1 | Col2 |",
            100,
            150,
        );
        assert!(table_chunk.is_table());
        assert!(table_chunk.is_continuation());
        assert_eq!(table_chunk.part, Some(2));
        assert_eq!(table_chunk.total_parts, Some(3));
    }

    #[test]
    fn test_document_element() {
        let para = DocumentElement::paragraph("Test paragraph", 0, 14);
        assert_eq!(para.element_type, ElementType::Paragraph);
        assert_eq!(para.char_count(), 14);

        let table = StructuredTable::new("t1");
        let table_elem = DocumentElement::table(table, 0, 100);
        assert!(table_elem.is_table());
        assert!(table_elem.as_table().is_some());
    }

    #[test]
    fn test_structured_document() {
        let mut doc = StructuredDocument::from_text("Hello\n\n| A | B |\n|---|---|\n| 1 | 2 |");
        doc.add_element(DocumentElement::paragraph("Hello", 0, 5));

        let mut table = StructuredTable::new("t1");
        table.headers = vec!["A".to_string(), "B".to_string()];
        doc.add_element(DocumentElement::table(table, 7, 35));

        assert!(doc.has_structure());
        assert_eq!(doc.table_count, 1);
        assert_eq!(doc.tables().count(), 1);
    }

    #[test]
    fn test_code_block_formatting() {
        let block = StructuredCodeBlock::new("fn main() {}").with_language("rust");

        let formatted = block.format();
        assert!(formatted.contains("```rust"));
        assert!(formatted.contains("fn main()"));
    }

    #[test]
    fn test_list_formatting() {
        let unordered = StructuredList::unordered(vec!["Item 1".to_string(), "Item 2".to_string()]);
        assert!(unordered.format().contains("- Item 1"));

        let ordered = StructuredList::ordered(vec!["First".to_string(), "Second".to_string()]);
        assert!(ordered.format().contains("1. First"));
        assert!(ordered.format().contains("2. Second"));
    }

    #[test]
    fn test_chunking_options_default() {
        let opts = ChunkingOptions::default();
        assert_eq!(opts.max_chars, 1200);
        assert_eq!(opts.table_handling, TableChunkingStrategy::SplitWithHeader);
        assert!(opts.preserve_lists);
    }
}
