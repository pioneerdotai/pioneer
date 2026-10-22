//! Tantivy-backed lexical search integration.

mod engine;
mod query;
mod schema;
mod storage;
mod util;
mod wal;

#[allow(unused_imports)]
pub use engine::{TantivyDocHit, TantivyEngine, TantivySnapshot};
#[allow(unused_imports)]
pub(crate) use storage::{EmbeddedLexSegment, EmbeddedLexStorage};
#[allow(unused_imports)]
pub(crate) use wal::LexWalBatch;
