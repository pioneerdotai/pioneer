use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeystoreError {
    InvalidSecretId(String),
    OpenFailed(String),
    ReadFailed(String),
    WriteFailed(String),
    DeleteFailed(String),
    ListFailed(String),
    PermissionFailed(String),
    MetadataDecodeFailed(String),
}

pub type Result<T> = std::result::Result<T, KeystoreError>;

impl fmt::Display for KeystoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeystoreError::InvalidSecretId(message) => write!(f, "invalid secret id: {message}"),
            KeystoreError::OpenFailed(message) => write!(f, "failed to open keystore: {message}"),
            KeystoreError::ReadFailed(message) => write!(f, "failed to read secret: {message}"),
            KeystoreError::WriteFailed(message) => write!(f, "failed to write secret: {message}"),
            KeystoreError::DeleteFailed(message) => {
                write!(f, "failed to delete secret: {message}")
            }
            KeystoreError::ListFailed(message) => write!(f, "failed to list secrets: {message}"),
            KeystoreError::PermissionFailed(message) => {
                write!(f, "failed to harden keystore permissions: {message}")
            }
            KeystoreError::MetadataDecodeFailed(message) => {
                write!(f, "failed to decode secret metadata: {message}")
            }
        }
    }
}

impl std::error::Error for KeystoreError {}
