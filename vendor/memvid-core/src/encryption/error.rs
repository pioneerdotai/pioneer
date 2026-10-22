use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum EncryptionError {
    #[error("I/O error: {source}")]
    Io {
        source: std::io::Error,
        path: Option<PathBuf>,
    },

    #[error("Invalid magic header: expected {expected:?}, found {found:?}")]
    InvalidMagic { expected: [u8; 4], found: [u8; 4] },

    #[error("Unsupported encryption format version: {version}")]
    UnsupportedVersion { version: u16 },

    #[error("Unsupported key derivation function: {id}")]
    UnsupportedKdf { id: u8 },

    #[error("Unsupported cipher algorithm: {id}")]
    UnsupportedCipher { id: u8 },

    #[error("Key derivation failed: {reason}")]
    KeyDerivation { reason: String },

    #[error("Cipher initialization failed: {reason}")]
    CipherInit { reason: String },

    #[error("Encryption failed: {reason}")]
    Encryption { reason: String },

    #[error("Decryption failed - invalid password or corrupted file")]
    Decryption { reason: String },

    #[error("Size mismatch: expected {expected} bytes, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },

    #[error("Not an MV2 file: {path}")]
    NotMv2File { path: PathBuf },

    #[error("Corrupted decryption - output is not a valid MV2 file")]
    CorruptedDecryption,
}

impl From<std::io::Error> for EncryptionError {
    fn from(source: std::io::Error) -> Self {
        EncryptionError::Io { source, path: None }
    }
}
