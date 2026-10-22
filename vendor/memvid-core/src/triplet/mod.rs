//! Automatic triplet extraction for structured knowledge storage.
//!
//! This module provides automatic Subject-Predicate-Object (SPO) triplet
//! extraction during document ingestion. Triplets are stored as `MemoryCard`s,
//! which already have the SPO structure: entity=Subject, slot=Predicate, value=Object.
//!
//! # Architecture
//!
//! ```text
//! Document → TripletExtractor → MemoryCard → MemoriesTrack → SlotIndex
//!                  ↓
//!        ┌────────┴────────┐
//!        │  RulesEngine    │ ← Fast, offline pattern matching
//!        │  (LLM Engine)   │ ← When configured (future)
//!        └─────────────────┘
//! ```
//!
//! # Usage
//!
//! Triplet extraction is enabled by default during `put` operations.
//! Use the `--no-extract-triplets` CLI flag to disable it.
//!
//! ```ignore
//! use memvid_core::triplet::{TripletExtractor, ExtractionMode};
//!
//! // Default: rules-based extraction
//! let extractor = TripletExtractor::default();
//!
//! // Extract triplets from text
//! let (cards, stats) = extractor.extract(
//!     frame_id,
//!     "Alice works at Acme Corp. She lives in San Francisco.",
//!     Some("mv2://docs/bio.txt"),
//!     Some("Alice's Bio"),
//!     1700000000,
//! );
//!
//! // cards contain:
//! // - MemoryCard { entity: "user", slot: "employer", value: "Acme Corp" }
//! // - MemoryCard { entity: "user", slot: "location", value: "San Francisco" }
//! ```
//!
//! # Extraction Modes
//!
//! - **Rules** (default): Fast regex-based pattern matching. No external dependencies.
//! - **Llm**: LLM-based extraction for complex sentences. Requires model configuration.
//! - **Hybrid**: Run both rules and LLM, deduplicate results. Auto-enabled when LLM configured.
//! - **Disabled**: No extraction.

mod extractor;
mod types;

pub use extractor::TripletExtractor;
pub use types::{ExtractionMode, ExtractionStats};
