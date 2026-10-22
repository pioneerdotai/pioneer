//! Structure-aware document processing for intelligent chunking.
//!
//! This module provides detection and chunking of document structures like tables,
//! code blocks, lists, and sections. It ensures that semantic units are preserved
//! during chunking, with features like table header propagation.
//!
//! # Architecture
//!
//! ```text
//! Raw Text ──► Detector ──► StructuredDocument ──► Chunker ──► StructuredChunks
//!                               │
//!                               ├── Tables (with headers/rows)
//!                               ├── Code blocks (with language)
//!                               ├── Lists (ordered/unordered)
//!                               ├── Headings (h1-h6)
//!                               └── Paragraphs
//! ```
//!
//! # Example
//!
//! ```ignore
//! use memvid_core::structure::{detect_structure, StructuralChunker, ChunkingOptions};
//!
//! let text = "# Report\n\n| Name | Value |\n|---|---|\n| A | 1 |\n| B | 2 |";
//! let doc = detect_structure(text);
//!
//! let chunker = StructuralChunker::new(ChunkingOptions::default());
//! let result = chunker.chunk(&doc);
//!
//! for chunk in result.chunks {
//!     println!("{}: {}", chunk.chunk_type, chunk.text);
//! }
//! ```

mod chunker;
mod detector;

pub use chunker::{StructuralChunker, chunk_structured, chunk_structured_with_max};
pub use detector::{detect_ascii_tables, detect_structure};

// Re-export types for convenience
pub use crate::types::structure::{
    ChunkType, ChunkingOptions, ChunkingResult, CodeChunkingStrategy, DocumentElement, ElementData,
    ElementType, StructuredCell, StructuredChunk, StructuredCodeBlock, StructuredDocument,
    StructuredHeading, StructuredList, StructuredRow, StructuredTable, TableChunkingStrategy,
};
