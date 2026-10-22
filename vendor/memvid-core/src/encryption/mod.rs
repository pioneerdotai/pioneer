//! Password-based encryption capsules for `.mv2` files (`.mv2e`).
//!
//! This module is feature-gated (`encryption`) to keep the default memvid-core
//! binary size small and avoid pulling crypto dependencies into users that don't
//! need encrypted-at-rest capsules.

mod capsule;
mod capsule_stream;
mod constants;
mod crypto;
mod error;
mod types;

pub use capsule::{lock_file, unlock_file};
pub use capsule_stream::{lock_file_stream, unlock_file_stream};
pub use constants::*;
pub use error::EncryptionError;
pub use types::{CipherAlgorithm, KdfAlgorithm, Mv2eHeader};
