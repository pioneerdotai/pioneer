use std::fmt;
use std::io;
use std::path::PathBuf;

use pioneer_crud::ArtifactCrudError;
use sea_orm::DbErr;

pub type ArtifactResult<T> = Result<T, ArtifactError>;

#[derive(Debug)]
pub enum ArtifactError {
    EmptyWorkspaceId,
    InvalidWorkspaceId {
        workspace_id: String,
    },
    InvalidStorageKey {
        storage_key: String,
    },
    StorageKeyTraversal {
        storage_key: String,
    },
    TempWriteFailed {
        path: PathBuf,
        source: io::Error,
    },
    FinalRenameFailed {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },
    ExistingBlobCorruption {
        storage_key: String,
        expected_sha256: String,
        expected_size_bytes: u64,
        actual_sha256: Option<String>,
        actual_size_bytes: Option<u64>,
    },
    ReadMissingBlob {
        storage_key: String,
    },
    MaterializedPathEscape {
        path: PathBuf,
        root: PathBuf,
    },
    InvalidRequest {
        message: String,
    },
    NotFound {
        message: String,
    },
    Database {
        message: String,
        source: DbErr,
    },
    CrudStore {
        message: String,
        source: anyhow::Error,
    },
    Json {
        message: String,
        source: serde_json::Error,
    },
    LocalPathRejected {
        message: String,
    },
    LocalPathReadFailed {
        path: PathBuf,
        source: io::Error,
    },
    QuotaExceeded {
        message: String,
        current_bytes: u64,
        limit_bytes: u64,
    },
    Io {
        message: String,
        source: io::Error,
    },
}

impl From<anyhow::Error> for ArtifactError {
    fn from(source: anyhow::Error) -> Self {
        let source = match source.downcast::<ArtifactCrudError>() {
            Ok(error) => return ArtifactError::from(error),
            Err(source) => source,
        };
        ArtifactError::CrudStore {
            message: "artifact CRUD operation failed".to_owned(),
            source,
        }
    }
}

impl From<ArtifactCrudError> for ArtifactError {
    fn from(error: ArtifactCrudError) -> Self {
        match error {
            ArtifactCrudError::InvalidRequest { message } => {
                ArtifactError::InvalidRequest { message }
            }
            ArtifactCrudError::NotFound { message } => ArtifactError::NotFound { message },
            ArtifactCrudError::Database { message, source } => {
                ArtifactError::Database { message, source }
            }
            ArtifactCrudError::Json { message, source } => ArtifactError::Json { message, source },
        }
    }
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArtifactError::EmptyWorkspaceId => write!(f, "empty workspace id"),
            ArtifactError::InvalidWorkspaceId { workspace_id } => {
                write!(f, "invalid workspace id: {workspace_id}")
            }
            ArtifactError::InvalidStorageKey { storage_key } => {
                write!(f, "invalid storage key: {storage_key}")
            }
            ArtifactError::StorageKeyTraversal { storage_key } => {
                write!(f, "storage key traversal rejected: {storage_key}")
            }
            ArtifactError::TempWriteFailed { path, source } => {
                write!(
                    f,
                    "failed to write temp artifact blob {}: {source}",
                    path.display()
                )
            }
            ArtifactError::FinalRenameFailed { from, to, source } => write!(
                f,
                "failed to move artifact blob from {} to {}: {source}",
                from.display(),
                to.display()
            ),
            ArtifactError::ExistingBlobCorruption {
                storage_key,
                expected_sha256,
                expected_size_bytes,
                actual_sha256,
                actual_size_bytes,
            } => write!(
                f,
                "existing artifact blob mismatch for {storage_key}: expected sha256={expected_sha256} size={expected_size_bytes}, actual sha256={} size={}",
                actual_sha256.as_deref().unwrap_or("<unavailable>"),
                actual_size_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "<unavailable>".to_string())
            ),
            ArtifactError::ReadMissingBlob { storage_key } => {
                write!(f, "artifact blob not found: {storage_key}")
            }
            ArtifactError::MaterializedPathEscape { path, root } => write!(
                f,
                "materialized artifact path escapes root: {} outside {}",
                path.display(),
                root.display()
            ),
            ArtifactError::InvalidRequest { message } => {
                write!(f, "invalid artifact request: {message}")
            }
            ArtifactError::NotFound { message } => write!(f, "artifact not found: {message}"),
            ArtifactError::Database { message, source } => write!(f, "{message}: {source}"),
            ArtifactError::CrudStore { message, source } => write!(f, "{message}: {source}"),
            ArtifactError::Json { message, source } => write!(f, "{message}: {source}"),
            ArtifactError::LocalPathRejected { message } => {
                write!(f, "local artifact path rejected: {message}")
            }
            ArtifactError::LocalPathReadFailed { path, source } => {
                write!(
                    f,
                    "failed to read local artifact path {}: {source}",
                    path.display()
                )
            }
            ArtifactError::QuotaExceeded {
                message,
                current_bytes,
                limit_bytes,
            } => write!(
                f,
                "artifact quota exceeded: {message} (current_bytes={current_bytes}, limit_bytes={limit_bytes})"
            ),
            ArtifactError::Io { message, source } => write!(f, "{message}: {source}"),
        }
    }
}

impl std::error::Error for ArtifactError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ArtifactError::TempWriteFailed { source, .. }
            | ArtifactError::FinalRenameFailed { source, .. }
            | ArtifactError::Io { source, .. } => Some(source),
            ArtifactError::Database { source, .. } => Some(source),
            ArtifactError::CrudStore { source, .. } => Some(source.root_cause()),
            ArtifactError::Json { source, .. } => Some(source),
            ArtifactError::LocalPathReadFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}
