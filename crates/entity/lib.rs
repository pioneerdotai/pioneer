//! Stable crate root; SeaORM CLI owns all files in src/.
#![recursion_limit = "256"]

#[path = "src/lib.rs"]
mod generated;

pub use generated::*;
