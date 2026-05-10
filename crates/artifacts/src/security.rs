use std::path::{Path, PathBuf};

use tokio::fs;

use crate::error::{ArtifactError, ArtifactResult};
use crate::mime::{infer_mime_from_path, is_safe_visible_name, sanitize_display_name};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactLocalPathPolicy {
    pub allowed_roots: Vec<PathBuf>,
    pub max_file_bytes: u64,
    pub follow_symlinks: bool,
}

impl ArtifactLocalPathPolicy {
    pub const DEFAULT_MAX_FILE_BYTES: u64 = 50 * 1024 * 1024;

    pub fn new(allowed_roots: Vec<PathBuf>) -> Self {
        Self {
            allowed_roots,
            max_file_bytes: Self::DEFAULT_MAX_FILE_BYTES,
            follow_symlinks: false,
        }
    }
}

impl Default for ArtifactLocalPathPolicy {
    fn default() -> Self {
        Self {
            allowed_roots: Vec::new(),
            max_file_bytes: Self::DEFAULT_MAX_FILE_BYTES,
            follow_symlinks: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedLocalFile {
    pub canonical_path: PathBuf,
    pub display_name: String,
    pub original_file_name: Option<String>,
    pub mime_type: String,
    pub bytes: Vec<u8>,
}

pub async fn read_validated_local_file(
    requested_path: &Path,
    policy: &ArtifactLocalPathPolicy,
) -> ArtifactResult<ValidatedLocalFile> {
    let allowed_roots = canonical_allowed_roots(policy).await?;
    let symlink_metadata = fs::symlink_metadata(requested_path)
        .await
        .map_err(|source| ArtifactError::LocalPathReadFailed {
            path: requested_path.to_path_buf(),
            source,
        })?;

    if symlink_metadata.file_type().is_symlink() && !policy.follow_symlinks {
        return Err(ArtifactError::LocalPathRejected {
            message: format!("symlink is not allowed: {}", requested_path.display()),
        });
    }

    let canonical_path = fs::canonicalize(requested_path).await.map_err(|source| {
        ArtifactError::LocalPathReadFailed {
            path: requested_path.to_path_buf(),
            source,
        }
    })?;
    if !allowed_roots
        .iter()
        .any(|allowed_root| canonical_path.starts_with(allowed_root))
    {
        return Err(ArtifactError::LocalPathRejected {
            message: format!(
                "path is outside allowed roots: {}",
                requested_path.display()
            ),
        });
    }

    let metadata = fs::metadata(&canonical_path).await.map_err(|source| {
        ArtifactError::LocalPathReadFailed {
            path: canonical_path.clone(),
            source,
        }
    })?;
    if !metadata.is_file() {
        return Err(ArtifactError::LocalPathRejected {
            message: format!("path is not a regular file: {}", requested_path.display()),
        });
    }
    if metadata.len() > policy.max_file_bytes {
        return Err(ArtifactError::LocalPathRejected {
            message: format!(
                "file size {} exceeds limit {}",
                metadata.len(),
                policy.max_file_bytes
            ),
        });
    }

    let bytes =
        fs::read(&canonical_path)
            .await
            .map_err(|source| ArtifactError::LocalPathReadFailed {
                path: canonical_path.clone(),
                source,
            })?;
    if bytes.len() as u64 > policy.max_file_bytes {
        return Err(ArtifactError::LocalPathRejected {
            message: format!(
                "file size after read {} exceeds limit {}",
                bytes.len(),
                policy.max_file_bytes
            ),
        });
    }

    let original_file_name = requested_path
        .file_name()
        .and_then(|value| value.to_str())
        .map(ToOwned::to_owned);
    let display_name = canonical_path
        .file_name()
        .and_then(|value| value.to_str())
        .map(sanitize_display_name)
        .unwrap_or_else(|| "artifact".to_owned());
    let original_file_name = original_file_name
        .as_deref()
        .filter(|value| is_safe_visible_name(value))
        .map(ToOwned::to_owned);
    let mime_type = infer_mime_from_path(&canonical_path);

    Ok(ValidatedLocalFile {
        canonical_path,
        display_name,
        original_file_name,
        mime_type,
        bytes,
    })
}

async fn canonical_allowed_roots(policy: &ArtifactLocalPathPolicy) -> ArtifactResult<Vec<PathBuf>> {
    if policy.allowed_roots.is_empty() {
        return Err(ArtifactError::LocalPathRejected {
            message: "allowed roots are required".to_owned(),
        });
    }
    if policy.max_file_bytes == 0 {
        return Err(ArtifactError::LocalPathRejected {
            message: "max_file_bytes must be greater than zero".to_owned(),
        });
    }

    let mut roots = Vec::with_capacity(policy.allowed_roots.len());
    for root in &policy.allowed_roots {
        let canonical =
            fs::canonicalize(root)
                .await
                .map_err(|source| ArtifactError::LocalPathReadFailed {
                    path: root.clone(),
                    source,
                })?;
        let metadata = fs::metadata(&canonical).await.map_err(|source| {
            ArtifactError::LocalPathReadFailed {
                path: canonical.clone(),
                source,
            }
        })?;
        if !metadata.is_dir() {
            return Err(ArtifactError::LocalPathRejected {
                message: format!("allowed root is not a directory: {}", root.display()),
            });
        }
        roots.push(canonical);
    }
    Ok(roots)
}
