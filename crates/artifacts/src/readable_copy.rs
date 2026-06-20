use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::fs;

use crate::error::{ArtifactError, ArtifactResult};
use crate::ids::{path_segment, workspace_segment};

pub const ARTIFACT_READABLE_COPY_DIR_NAME: &str = "materialized";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactReadableCopyGcCandidate {
    pub copy_id: String,
    pub relative_path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct ArtifactReadableCopyGcPlan {
    pub candidates: Vec<ArtifactReadableCopyGcCandidate>,
    pub estimated_bytes_reclaimable: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct ArtifactReadableCopyGcReport {
    pub plan: ArtifactReadableCopyGcPlan,
    pub deleted_dirs: u64,
}

pub fn artifact_readable_copy_workspace_root(
    artifact_root: &Path,
    workspace_id: &str,
) -> ArtifactResult<PathBuf> {
    Ok(artifact_root
        .join("workspaces")
        .join(workspace_segment(workspace_id)?)
        .join(ARTIFACT_READABLE_COPY_DIR_NAME))
}

pub async fn plan_readable_copy_gc(
    artifact_root: &Path,
    workspace_id: &str,
    now_unix_ms: i64,
    ttl_secs: u64,
) -> ArtifactResult<ArtifactReadableCopyGcPlan> {
    let root = artifact_readable_copy_workspace_root(artifact_root, workspace_id)?;
    if !fs::try_exists(root.as_path())
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!(
                "failed to inspect artifact readable copy root {}",
                root.display()
            ),
            source,
        })?
    {
        return Ok(ArtifactReadableCopyGcPlan::default());
    }

    let cutoff = unix_ms_to_system_time(now_unix_ms)
        .checked_sub(Duration::from_secs(ttl_secs))
        .unwrap_or(UNIX_EPOCH);
    let mut plan = ArtifactReadableCopyGcPlan::default();
    let mut entries = fs::read_dir(root.as_path())
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!(
                "failed to read artifact readable copy root {}",
                root.display()
            ),
            source,
        })?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!(
                "failed to iterate artifact readable copy root {}",
                root.display()
            ),
            source,
        })?
    {
        let path = entry.path();
        if !entry_is_dir(entry.file_type().await, path.as_path())? {
            continue;
        }
        let metadata = fs::metadata(path.as_path())
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to inspect artifact readable copy dir {}",
                    path.display()
                ),
                source,
            })?;
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        if modified > cutoff {
            continue;
        }
        let size_bytes = dir_size_bytes(path.as_path()).await?;
        plan.estimated_bytes_reclaimable =
            plan.estimated_bytes_reclaimable.saturating_add(size_bytes);
        let copy_id = entry.file_name().to_string_lossy().to_string();
        plan.candidates.push(ArtifactReadableCopyGcCandidate {
            copy_id: copy_id.clone(),
            relative_path: copy_id,
            size_bytes,
        });
    }

    Ok(plan)
}

pub async fn execute_readable_copy_gc(
    artifact_root: &Path,
    workspace_id: &str,
    now_unix_ms: i64,
    ttl_secs: u64,
) -> ArtifactResult<ArtifactReadableCopyGcReport> {
    let plan = plan_readable_copy_gc(artifact_root, workspace_id, now_unix_ms, ttl_secs).await?;
    let root = artifact_readable_copy_workspace_root(artifact_root, workspace_id)?;
    let mut deleted_dirs = 0;
    for candidate in &plan.candidates {
        let path = root.join(path_segment("copy_id", candidate.copy_id.as_str())?);
        match fs::remove_dir_all(path.as_path()).await {
            Ok(()) => deleted_dirs += 1,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ArtifactError::Io {
                    message: format!(
                        "failed to remove artifact readable copy dir {}",
                        path.display()
                    ),
                    source,
                });
            }
        }
    }

    Ok(ArtifactReadableCopyGcReport { plan, deleted_dirs })
}

fn unix_ms_to_system_time(value: i64) -> SystemTime {
    if value <= 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::from_millis(value as u64)
}

fn entry_is_dir(
    file_type: std::io::Result<std::fs::FileType>,
    path: &Path,
) -> ArtifactResult<bool> {
    let file_type = file_type.map_err(|source| ArtifactError::Io {
        message: format!(
            "failed to inspect artifact readable copy entry {}",
            path.display()
        ),
        source,
    })?;
    Ok(file_type.is_dir())
}

async fn dir_size_bytes(path: &Path) -> ArtifactResult<u64> {
    let mut total = 0_u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries =
            fs::read_dir(dir.as_path())
                .await
                .map_err(|source| ArtifactError::Io {
                    message: format!(
                        "failed to read artifact readable copy dir {}",
                        dir.display()
                    ),
                    source,
                })?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to iterate artifact readable copy dir {}",
                    dir.display()
                ),
                source,
            })?
        {
            let entry_path = entry.path();
            let metadata = fs::symlink_metadata(entry_path.as_path())
                .await
                .map_err(|source| ArtifactError::Io {
                    message: format!(
                        "failed to inspect artifact readable copy entry {}",
                        entry_path.display()
                    ),
                    source,
                })?;
            if metadata.file_type().is_dir() {
                stack.push(entry_path);
            } else if metadata.file_type().is_file() {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readable_copy_gc_removes_expired_dirs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact_root = temp.path().join("artifacts");
        let root = artifact_readable_copy_workspace_root(&artifact_root, "ws_gc").expect("root");
        let copy_dir = root.join("materialized-old");
        fs::create_dir_all(copy_dir.as_path())
            .await
            .expect("create copy dir");
        fs::write(copy_dir.join("input.txt"), b"input")
            .await
            .expect("write copy");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis() as i64
            + 60_000;
        let report = execute_readable_copy_gc(&artifact_root, "ws_gc", now_ms, 0)
            .await
            .expect("gc");

        assert_eq!(report.deleted_dirs, 1);
        assert_eq!(report.plan.candidates.len(), 1);
        assert!(!fs::try_exists(copy_dir).await.expect("exists check"));
    }

    #[tokio::test]
    async fn readable_copy_gc_keeps_recent_dirs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact_root = temp.path().join("artifacts");
        let root = artifact_readable_copy_workspace_root(&artifact_root, "ws_gc").expect("root");
        let copy_dir = root.join("materialized-recent");
        fs::create_dir_all(copy_dir.as_path())
            .await
            .expect("create copy dir");
        fs::write(copy_dir.join("input.txt"), b"input")
            .await
            .expect("write copy");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis() as i64;
        let report = execute_readable_copy_gc(&artifact_root, "ws_gc", now_ms, 24 * 60 * 60)
            .await
            .expect("gc");

        assert_eq!(report.deleted_dirs, 0);
        assert!(fs::try_exists(copy_dir).await.expect("exists check"));
    }
}
