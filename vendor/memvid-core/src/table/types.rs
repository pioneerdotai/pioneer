//! Core types for table extraction and storage.
//!
//! This module defines the data structures used to represent extracted tables,
//! their quality metrics, and configuration options.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Quality tier for extracted tables.
///
/// Tables are classified by confidence in extraction accuracy:
/// - `High`: Ruled tables with clear grid structure, or native format tables
/// - `Medium`: Stream-detected tables with consistent alignment
/// - `Low`: Uncertain extraction, possibly fragmented or incomplete
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TableQuality {
    High,
    #[default]
    Medium,
    Low,
}

impl std::fmt::Display for TableQuality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::High => write!(f, "high"),
            Self::Medium => write!(f, "medium"),
            Self::Low => write!(f, "low"),
        }
    }
}

impl std::str::FromStr for TableQuality {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "high" => Ok(Self::High),
            "medium" => Ok(Self::Medium),
            "low" => Ok(Self::Low),
            _ => Err(format!("unknown table quality: {s}")),
        }
    }
}

/// Detection method used to extract the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DetectionMode {
    /// Grid lines detected (ruled tables)
    Lattice,
    /// Whitespace/alignment inferred (no visible borders)
    #[default]
    Stream,
    /// Native structure from document format (XLSX, DOCX, HTML)
    Native,
    /// Line-pattern based detection (for linearized PDF text)
    LineBased,
}

impl std::fmt::Display for DetectionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lattice => write!(f, "lattice"),
            Self::Stream => write!(f, "stream"),
            Self::Native => write!(f, "native"),
            Self::LineBased => write!(f, "line_based"),
        }
    }
}

/// A single cell in a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCell {
    /// Text content of the cell (trimmed)
    pub text: String,
    /// Column index (0-based)
    pub col_index: usize,
    /// Number of rows this cell spans (for merged cells)
    #[serde(default = "default_span")]
    pub row_span: usize,
    /// Number of columns this cell spans (for merged cells)
    #[serde(default = "default_span")]
    pub col_span: usize,
    /// Whether this cell is part of a header row
    #[serde(default)]
    pub is_header: bool,
}

fn default_span() -> usize {
    1
}

impl TableCell {
    /// Create a new simple cell with text content.
    #[must_use]
    pub fn new(text: impl Into<String>, col_index: usize) -> Self {
        Self {
            text: text.into(),
            col_index,
            row_span: 1,
            col_span: 1,
            is_header: false,
        }
    }

    /// Mark this cell as a header cell.
    #[must_use]
    pub fn as_header(mut self) -> Self {
        self.is_header = true;
        self
    }
}

/// A single row in a table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableRow {
    /// Row index within the table (0-based)
    pub row_index: usize,
    /// Source page number (1-indexed for display)
    pub page: u32,
    /// Cells in this row
    pub cells: Vec<TableCell>,
    /// Whether this is a header row
    #[serde(default)]
    pub is_header_row: bool,
}

impl TableRow {
    /// Create a new row with the given cells.
    #[must_use]
    pub fn new(row_index: usize, page: u32, cells: Vec<TableCell>) -> Self {
        Self {
            row_index,
            page,
            cells,
            is_header_row: false,
        }
    }

    /// Mark this row as a header row.
    #[must_use]
    pub fn as_header(mut self) -> Self {
        self.is_header_row = true;
        for cell in &mut self.cells {
            cell.is_header = true;
        }
        self
    }

    /// Get the text content of all cells as a vector.
    #[must_use]
    pub fn cell_texts(&self) -> Vec<&str> {
        self.cells.iter().map(|c| c.text.as_str()).collect()
    }
}

/// Complete extracted table with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedTable {
    /// Unique identifier for this table (UUID or hash-based)
    pub table_id: String,
    /// Original source filename
    pub source_file: String,
    /// URI of the source frame (if from existing MV2 content)
    pub source_uri: Option<String>,

    // Page span
    /// Starting page number (1-indexed)
    pub page_start: u32,
    /// Ending page number (1-indexed, inclusive)
    pub page_end: u32,

    // Structure
    /// Column headers (if detected)
    pub headers: Vec<String>,
    /// All rows including header rows
    pub rows: Vec<TableRow>,
    /// Number of columns
    pub n_cols: usize,
    /// Number of rows (excluding header)
    pub n_rows: usize,

    // Quality metadata
    /// Quality assessment of extraction
    pub quality: TableQuality,
    /// Detection method used
    pub detection_mode: DetectionMode,
    /// Confidence score (0.0-1.0)
    pub confidence_score: f32,

    // Extraction diagnostics
    /// Warnings generated during extraction
    pub warnings: Vec<String>,
    /// Time taken for extraction in milliseconds
    pub extraction_ms: u64,
}

impl ExtractedTable {
    /// Create a new table with minimal required fields.
    #[must_use]
    pub fn new(table_id: impl Into<String>, source_file: impl Into<String>) -> Self {
        Self {
            table_id: table_id.into(),
            source_file: source_file.into(),
            source_uri: None,
            page_start: 1,
            page_end: 1,
            headers: Vec::new(),
            rows: Vec::new(),
            n_cols: 0,
            n_rows: 0,
            quality: TableQuality::Medium,
            detection_mode: DetectionMode::Stream,
            confidence_score: 0.5,
            warnings: Vec::new(),
            extraction_ms: 0,
        }
    }

    /// Build a map of header name to cell value for a given row.
    #[must_use]
    pub fn row_as_map(&self, row: &TableRow) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        for (i, header) in self.headers.iter().enumerate() {
            let value = row.cells.get(i).map(|c| c.text.clone()).unwrap_or_default();
            map.insert(header.clone(), value);
        }
        map
    }

    /// Get all data rows (excluding header rows).
    #[must_use]
    pub fn data_rows(&self) -> Vec<&TableRow> {
        self.rows.iter().filter(|r| !r.is_header_row).collect()
    }

    /// Check if the table spans multiple pages.
    #[must_use]
    pub fn is_multi_page(&self) -> bool {
        self.page_end > self.page_start
    }

    /// Generate searchable text from all cells.
    #[must_use]
    pub fn to_search_text(&self) -> String {
        let mut parts = Vec::new();

        // Include headers
        if !self.headers.is_empty() {
            parts.push(self.headers.join(" "));
        }

        // Include all cell text
        for row in &self.rows {
            for cell in &row.cells {
                if !cell.text.is_empty() {
                    parts.push(cell.text.clone());
                }
            }
        }

        parts.join(" ")
    }
}

/// Options for table extraction.
#[derive(Debug, Clone)]
pub struct TableExtractionOptions {
    /// Extraction mode (conservative, aggressive, etc.)
    pub mode: ExtractionMode,
    /// Minimum rows to consider a valid table
    pub min_rows: usize,
    /// Minimum columns to consider a valid table
    pub min_cols: usize,
    /// Minimum quality threshold for output
    pub min_quality: TableQuality,
    /// Whether to merge tables spanning multiple pages
    pub merge_multi_page: bool,
    /// Y-position tolerance for row clustering (in points)
    pub row_clustering_threshold: f32,
    /// X-position tolerance for column clustering (in points)
    pub col_clustering_threshold: f32,
    /// Similarity threshold for merging multi-page tables (0.0-1.0)
    pub header_similarity_threshold: f32,
    /// Maximum pages to process (0 = unlimited)
    pub max_pages: usize,
}

impl Default for TableExtractionOptions {
    fn default() -> Self {
        Self {
            mode: ExtractionMode::default(),
            min_rows: 2,
            min_cols: 2,
            min_quality: TableQuality::Medium,
            merge_multi_page: true,
            row_clustering_threshold: 5.0,
            col_clustering_threshold: 10.0,
            header_similarity_threshold: 0.8,
            max_pages: 0, // unlimited
        }
    }
}

impl TableExtractionOptions {
    /// Create a builder for table extraction options.
    #[must_use]
    pub fn builder() -> TableExtractionOptionsBuilder {
        TableExtractionOptionsBuilder::default()
    }
}

/// Builder for `TableExtractionOptions`.
#[derive(Debug, Clone, Default)]
pub struct TableExtractionOptionsBuilder {
    inner: TableExtractionOptions,
}

impl TableExtractionOptionsBuilder {
    /// Set the extraction mode.
    #[must_use]
    pub fn mode(mut self, mode: ExtractionMode) -> Self {
        self.inner.mode = mode;
        self
    }

    /// Set minimum rows threshold.
    #[must_use]
    pub fn min_rows(mut self, n: usize) -> Self {
        self.inner.min_rows = n;
        self
    }

    /// Set minimum columns threshold.
    #[must_use]
    pub fn min_cols(mut self, n: usize) -> Self {
        self.inner.min_cols = n;
        self
    }

    /// Set minimum quality threshold.
    #[must_use]
    pub fn min_quality(mut self, quality: TableQuality) -> Self {
        self.inner.min_quality = quality;
        self
    }

    /// Enable or disable multi-page table merging.
    #[must_use]
    pub fn merge_multi_page(mut self, enabled: bool) -> Self {
        self.inner.merge_multi_page = enabled;
        self
    }

    /// Set row clustering threshold (Y-position tolerance).
    #[must_use]
    pub fn row_clustering_threshold(mut self, threshold: f32) -> Self {
        self.inner.row_clustering_threshold = threshold;
        self
    }

    /// Set column clustering threshold (X-position tolerance).
    #[must_use]
    pub fn col_clustering_threshold(mut self, threshold: f32) -> Self {
        self.inner.col_clustering_threshold = threshold;
        self
    }

    /// Set header similarity threshold for multi-page merging.
    #[must_use]
    pub fn header_similarity_threshold(mut self, threshold: f32) -> Self {
        self.inner.header_similarity_threshold = threshold;
        self
    }

    /// Set maximum pages to process (0 = unlimited).
    #[must_use]
    pub fn max_pages(mut self, n: usize) -> Self {
        self.inner.max_pages = n;
        self
    }

    /// Build the options.
    #[must_use]
    pub fn build(self) -> TableExtractionOptions {
        self.inner
    }
}

/// Extraction mode controls quality vs coverage tradeoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExtractionMode {
    /// Only output High/Medium quality tables
    #[default]
    Conservative,
    /// Include Low quality tables with warnings
    Aggressive,
    /// Only detect tables with visible grid lines
    LatticeOnly,
    /// Only detect tables inferred from text alignment
    StreamOnly,
}

impl std::fmt::Display for ExtractionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conservative => write!(f, "conservative"),
            Self::Aggressive => write!(f, "aggressive"),
            Self::LatticeOnly => write!(f, "lattice_only"),
            Self::StreamOnly => write!(f, "stream_only"),
        }
    }
}

/// Summary of a stored table (for listing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSummary {
    /// Table identifier
    pub table_id: String,
    /// Source filename
    pub source_file: String,
    /// Starting page number (1-indexed)
    pub page_start: u32,
    /// Ending page number (1-indexed)
    pub page_end: u32,
    /// Number of rows
    pub n_rows: usize,
    /// Number of columns
    pub n_cols: usize,
    /// Quality tier
    pub quality: TableQuality,
    /// Column headers
    pub headers: Vec<String>,
    /// Frame ID of the `table_meta` frame
    pub frame_id: u64,
}

/// Result of table extraction from a document.
#[derive(Debug, Clone)]
pub struct TableExtractionResult {
    /// Successfully extracted tables
    pub tables: Vec<ExtractedTable>,
    /// Total pages processed
    pub pages_processed: u32,
    /// Total extraction time in milliseconds
    pub total_ms: u64,
    /// Warnings from extraction process
    pub warnings: Vec<String>,
}

impl TableExtractionResult {
    /// Create an empty result.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            tables: Vec::new(),
            pages_processed: 0,
            total_ms: 0,
            warnings: Vec::new(),
        }
    }

    /// Number of tables extracted.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Check if any tables were extracted.
    #[must_use]
    pub fn has_tables(&self) -> bool {
        !self.tables.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_cell_creation() {
        let cell = TableCell::new("Hello", 0);
        assert_eq!(cell.text, "Hello");
        assert_eq!(cell.col_index, 0);
        assert_eq!(cell.row_span, 1);
        assert_eq!(cell.col_span, 1);
        assert!(!cell.is_header);

        let header_cell = cell.as_header();
        assert!(header_cell.is_header);
    }

    #[test]
    fn test_table_row_creation() {
        let cells = vec![
            TableCell::new("A", 0),
            TableCell::new("B", 1),
            TableCell::new("C", 2),
        ];
        let row = TableRow::new(0, 1, cells);
        assert_eq!(row.row_index, 0);
        assert_eq!(row.page, 1);
        assert_eq!(row.cells.len(), 3);
        assert!(!row.is_header_row);

        let header_row = row.as_header();
        assert!(header_row.is_header_row);
        assert!(header_row.cells.iter().all(|c| c.is_header));
    }

    #[test]
    fn test_extracted_table() {
        let mut table = ExtractedTable::new("tbl_001", "invoice.pdf");
        table.headers = vec!["Date".to_string(), "Amount".to_string()];
        table.n_cols = 2;
        table.page_start = 1;
        table.page_end = 1;

        let cells = vec![
            TableCell::new("2025-01-15", 0),
            TableCell::new("$100.00", 1),
        ];
        let row = TableRow::new(0, 1, cells);
        table.rows.push(row);
        table.n_rows = 1;

        let map = table.row_as_map(&table.rows[0]);
        assert_eq!(map.get("Date"), Some(&"2025-01-15".to_string()));
        assert_eq!(map.get("Amount"), Some(&"$100.00".to_string()));

        assert!(!table.is_multi_page());
        table.page_end = 2;
        assert!(table.is_multi_page());
    }

    #[test]
    fn test_quality_display() {
        assert_eq!(TableQuality::High.to_string(), "high");
        assert_eq!(TableQuality::Medium.to_string(), "medium");
        assert_eq!(TableQuality::Low.to_string(), "low");
    }

    #[test]
    fn test_options_builder() {
        let options = TableExtractionOptions::builder()
            .mode(ExtractionMode::LatticeOnly)
            .min_rows(3)
            .min_cols(2)
            .min_quality(TableQuality::High)
            .merge_multi_page(false)
            .build();

        assert_eq!(options.mode, ExtractionMode::LatticeOnly);
        assert_eq!(options.min_rows, 3);
        assert_eq!(options.min_cols, 2);
        assert_eq!(options.min_quality, TableQuality::High);
        assert!(!options.merge_multi_page);
    }
}
