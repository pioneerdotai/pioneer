//! Table extraction module for Memvid.
//!
//! This module provides comprehensive table extraction capabilities for
//! PDF documents (and in the future, DOCX, XLSX, HTML). It supports:
//!
//! - **Lattice detection**: Tables with visible grid lines
//! - **Stream detection**: Tables inferred from text alignment
//! - **Multi-page merging**: Automatic detection of tables spanning pages
//! - **Quality scoring**: Confidence-based filtering
//!
//! # Example
//!
//! ```ignore
//! use memvid_core::table::{
//!     extract_tables_from_pdf, store_table, list_tables,
//!     TableExtractionOptions, ExtractionMode,
//! };
//!
//! // Extract tables from PDF
//! let options = TableExtractionOptions::builder()
//!     .mode(ExtractionMode::Conservative)
//!     .min_rows(2)
//!     .min_cols(2)
//!     .build();
//!
//! let result = extract_tables_from_pdf(&pdf_bytes, "invoice.pdf", &options)?;
//!
//! // Store in MV2 file
//! for table in &result.tables {
//!     let (meta_id, row_ids) = store_table(&mut mem, table, true)?;
//!     println!("Stored table {} with {} rows", table.table_id, row_ids.len());
//! }
//!
//! // List stored tables
//! let tables = list_tables(&mem)?;
//! for t in tables {
//!     println!("{}: {} rows × {} cols", t.table_id, t.n_rows, t.n_cols);
//! }
//! ```
//!
//! # Architecture
//!
//! Tables are stored as frames in the MV2 file using two frame kinds:
//!
//! - `table_meta`: Contains table structure, headers, and metadata
//! - `table_row`: Contains individual row data (one frame per row)
//!
//! This allows both full table reconstruction and row-level search.

mod layout;
mod multi_page;
mod pdf_extractor;
mod storage;
mod types;

// Re-export public types
pub use layout::{LineSegment, PageLayout, TextBox, cluster_values, extract_pdf_layout};
pub use multi_page::{find_continuation_candidates, merge_multi_page_tables};
pub use pdf_extractor::extract_tables_from_pdf;
pub use storage::{
    TABLE_META_KIND, TABLE_ROW_KIND, TABLE_TRACK, export_to_csv, export_to_json, get_table,
    list_tables, store_table, store_table_with_embedder,
};
pub use types::{
    DetectionMode, ExtractedTable, ExtractionMode, TableCell, TableExtractionOptions,
    TableExtractionOptionsBuilder, TableExtractionResult, TableQuality, TableRow, TableSummary,
};

use crate::error::Result;

/// Extract tables from a document based on its format.
///
/// This is a convenience function that dispatches to the appropriate
/// extractor based on file extension or content detection.
///
/// # Arguments
/// * `bytes` - Document bytes
/// * `filename` - Original filename (used for format detection)
/// * `options` - Extraction options
///
/// # Returns
/// Extraction result with tables and diagnostics
pub fn extract_tables(
    bytes: &[u8],
    filename: &str,
    options: &TableExtractionOptions,
) -> Result<TableExtractionResult> {
    let lower = filename.to_lowercase();

    if lower.ends_with(".pdf") || is_pdf_magic(bytes) {
        extract_tables_from_pdf(bytes, filename, options)
    } else if lower.ends_with(".xlsx") || lower.ends_with(".xls") {
        Ok(TableExtractionResult::empty())
    } else if lower.ends_with(".docx") || lower.ends_with(".doc") {
        Ok(TableExtractionResult::empty())
    } else if lower.ends_with(".html") || lower.ends_with(".htm") {
        Ok(TableExtractionResult::empty())
    } else {
        // Unknown format
        Ok(TableExtractionResult::empty())
    }
}

/// Check if bytes start with PDF magic number.
fn is_pdf_magic(bytes: &[u8]) -> bool {
    // PDF magic: %PDF (after optional BOM/whitespace)
    let trimmed = bytes
        .iter()
        .skip_while(|&&b| b == 0xEF || b == 0xBB || b == 0xBF || b.is_ascii_whitespace())
        .take(4)
        .copied()
        .collect::<Vec<_>>();

    trimmed.starts_with(b"%PDF")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pdf_magic_detection() {
        assert!(is_pdf_magic(b"%PDF-1.4"));
        assert!(is_pdf_magic(b"\xEF\xBB\xBF%PDF-1.7")); // With BOM
        assert!(is_pdf_magic(b"  %PDF-1.5")); // With whitespace
        assert!(!is_pdf_magic(b"PK\x03\x04")); // ZIP/DOCX
        assert!(!is_pdf_magic(b"<html>"));
    }

    #[test]
    fn test_extraction_options_builder() {
        let options = TableExtractionOptions::builder()
            .mode(ExtractionMode::LatticeOnly)
            .min_rows(3)
            .min_cols(2)
            .min_quality(TableQuality::High)
            .merge_multi_page(false)
            .max_pages(10)
            .build();

        assert_eq!(options.mode, ExtractionMode::LatticeOnly);
        assert_eq!(options.min_rows, 3);
        assert_eq!(options.min_cols, 2);
        assert_eq!(options.min_quality, TableQuality::High);
        assert!(!options.merge_multi_page);
        assert_eq!(options.max_pages, 10);
    }

    #[test]
    fn test_default_options() {
        let options = TableExtractionOptions::default();

        assert_eq!(options.mode, ExtractionMode::Conservative);
        assert_eq!(options.min_rows, 2);
        assert_eq!(options.min_cols, 2);
        assert_eq!(options.min_quality, TableQuality::Medium);
        assert!(options.merge_multi_page);
        assert_eq!(options.max_pages, 0);
    }
}
