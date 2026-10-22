#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use super::storage::EmbeddedLexSegment;

/// Placeholder WAL payload describing Tantivy changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LexWalBatch {
    pub generation: u64,
    pub doc_count: u64,
    pub checksum: [u8; 32],
    pub segments: Vec<EmbeddedLexSegment>,
}
