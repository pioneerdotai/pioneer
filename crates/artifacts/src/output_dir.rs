use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::fs;

use crate::error::{ArtifactError, ArtifactResult};
use crate::ids::{path_segment, workspace_segment};

pub const PIONEER_ARTIFACT_OUTPUT_DIR_ENV: &str = "PIONEER_ARTIFACT_OUTPUT_DIR";
pub const ARTIFACT_OUTPUT_DIR_NAME: &str = "output";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactOutputDir {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub path: PathBuf,
    _active_lease: Arc<ActiveArtifactOutputLease>,
}

#[derive(Debug, PartialEq, Eq)]
struct ActiveArtifactOutputLease {
    path: PathBuf,
}

fn active_output_dirs() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static ACTIVE: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashMap::new()))
}

impl ActiveArtifactOutputLease {
    fn reserve(path: PathBuf) -> ArtifactResult<Self> {
        let mut active = active_output_dirs().lock().map_err(|_| ArtifactError::Io {
            message: "artifact output activity registry is unavailable".to_owned(),
            source: std::io::Error::other("artifact output activity registry lock poisoned"),
        })?;
        *active.entry(path.clone()).or_insert(0) += 1;
        Ok(Self { path })
    }
}

impl Drop for ActiveArtifactOutputLease {
    fn drop(&mut self) {
        let Ok(mut active) = active_output_dirs().lock() else {
            return;
        };
        let Some(count) = active.get_mut(&self.path) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            active.remove(&self.path);
        }
    }
}

fn output_dir_is_active(path: &Path) -> bool {
    let path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    active_output_dirs()
        .lock()
        .map(|active| active.contains_key(&path))
        .unwrap_or(true)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactOutputDirGcCandidate {
    pub thread_id: String,
    pub turn_id: String,
    pub relative_path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct ArtifactOutputDirGcPlan {
    pub candidates: Vec<ArtifactOutputDirGcCandidate>,
    pub estimated_bytes_reclaimable: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct ArtifactOutputDirGcReport {
    pub plan: ArtifactOutputDirGcPlan,
    pub deleted_dirs: u64,
}

pub fn artifact_output_workspace_root(
    artifact_root: &Path,
    workspace_id: &str,
) -> ArtifactResult<PathBuf> {
    Ok(artifact_root
        .join("workspaces")
        .join(workspace_segment(workspace_id)?)
        .join(ARTIFACT_OUTPUT_DIR_NAME))
}

pub fn artifact_output_dir_path(
    artifact_root: &Path,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> ArtifactResult<PathBuf> {
    Ok(artifact_output_workspace_root(artifact_root, workspace_id)?
        .join(path_segment("thread_id", thread_id)?)
        .join(path_segment("turn_id", turn_id)?))
}

pub async fn create_artifact_output_dir(
    artifact_root: &Path,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> ArtifactResult<ArtifactOutputDir> {
    let path = artifact_output_dir_path(artifact_root, workspace_id, thread_id, turn_id)?;
    fs::create_dir_all(path.as_path())
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!("failed to create artifact output dir {}", path.display()),
            source,
        })?;
    let path = fs::canonicalize(path.as_path())
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!("failed to resolve artifact output dir {}", path.display()),
            source,
        })?;
    let active_lease = Arc::new(ActiveArtifactOutputLease::reserve(path.clone())?);
    Ok(ArtifactOutputDir {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        path,
        _active_lease: active_lease,
    })
}

pub async fn cleanup_artifact_output_file(path: &Path) -> ArtifactResult<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ArtifactError::Io {
            message: format!("failed to remove artifact output file {}", path.display()),
            source,
        }),
    }
}

pub async fn plan_output_dir_gc(
    artifact_root: &Path,
    workspace_id: &str,
    now_unix_ms: i64,
    ttl_secs: u64,
) -> ArtifactResult<ArtifactOutputDirGcPlan> {
    let output_root = artifact_output_workspace_root(artifact_root, workspace_id)?;
    if !fs::try_exists(output_root.as_path())
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!(
                "failed to inspect artifact output root {}",
                output_root.display()
            ),
            source,
        })?
    {
        return Ok(ArtifactOutputDirGcPlan::default());
    }

    let cutoff = unix_ms_to_system_time(now_unix_ms)
        .checked_sub(Duration::from_secs(ttl_secs))
        .unwrap_or(UNIX_EPOCH);
    let mut plan = ArtifactOutputDirGcPlan::default();
    let mut thread_entries = fs::read_dir(output_root.as_path())
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!(
                "failed to read artifact output root {}",
                output_root.display()
            ),
            source,
        })?;
    while let Some(thread_entry) =
        thread_entries
            .next_entry()
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to iterate artifact output root {}",
                    output_root.display()
                ),
                source,
            })?
    {
        let thread_path = thread_entry.path();
        if !entry_is_dir(thread_entry.file_type().await, thread_path.as_path())? {
            continue;
        }
        let thread_id = thread_entry.file_name().to_string_lossy().to_string();
        let mut turn_entries = fs::read_dir(thread_path.as_path())
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!(
                    "failed to read artifact output thread dir {}",
                    thread_path.display()
                ),
                source,
            })?;
        while let Some(turn_entry) =
            turn_entries
                .next_entry()
                .await
                .map_err(|source| ArtifactError::Io {
                    message: format!(
                        "failed to iterate artifact output thread dir {}",
                        thread_path.display()
                    ),
                    source,
                })?
        {
            let turn_path = turn_entry.path();
            if !entry_is_dir(turn_entry.file_type().await, turn_path.as_path())? {
                continue;
            }
            if output_dir_is_active(turn_path.as_path()) {
                continue;
            }
            let metadata =
                fs::metadata(turn_path.as_path())
                    .await
                    .map_err(|source| ArtifactError::Io {
                        message: format!(
                            "failed to inspect artifact output turn dir {}",
                            turn_path.display()
                        ),
                        source,
                    })?;
            let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
            if modified > cutoff {
                continue;
            }
            let size_bytes = dir_size_bytes(turn_path.as_path()).await?;
            plan.estimated_bytes_reclaimable =
                plan.estimated_bytes_reclaimable.saturating_add(size_bytes);
            let turn_id = turn_entry.file_name().to_string_lossy().to_string();
            plan.candidates.push(ArtifactOutputDirGcCandidate {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                relative_path: format!("{thread_id}/{turn_id}"),
                size_bytes,
            });
        }
    }
    Ok(plan)
}

pub async fn execute_output_dir_gc(
    artifact_root: &Path,
    workspace_id: &str,
    now_unix_ms: i64,
    ttl_secs: u64,
) -> ArtifactResult<ArtifactOutputDirGcReport> {
    let plan = plan_output_dir_gc(artifact_root, workspace_id, now_unix_ms, ttl_secs).await?;
    let output_root = artifact_output_workspace_root(artifact_root, workspace_id)?;
    let mut deleted_dirs = 0;
    for candidate in &plan.candidates {
        let path = output_root
            .join(path_segment("thread_id", candidate.thread_id.as_str())?)
            .join(path_segment("turn_id", candidate.turn_id.as_str())?);
        let removal_path = path.clone();
        let removal = tokio::task::spawn_blocking(move || {
            let active = active_output_dirs().lock().map_err(|_| {
                std::io::Error::other("artifact output activity registry lock poisoned")
            })?;
            let canonical = std::fs::canonicalize(removal_path.as_path())
                .unwrap_or_else(|_| removal_path.clone());
            if active.contains_key(&canonical) {
                return Ok(false);
            }
            std::fs::remove_dir_all(removal_path.as_path()).map(|()| true)
        })
        .await
        .map_err(|source| ArtifactError::Io {
            message: format!(
                "artifact output cleanup worker failed for {}",
                path.display()
            ),
            source: std::io::Error::other(source.to_string()),
        })?;
        match removal {
            Ok(true) => deleted_dirs += 1,
            Ok(false) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(ArtifactError::Io {
                    message: format!("failed to remove artifact output dir {}", path.display()),
                    source,
                });
            }
        }
    }
    Ok(ArtifactOutputDirGcReport { plan, deleted_dirs })
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
            "failed to inspect artifact output dir entry {}",
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
                    message: format!("failed to read artifact output dir {}", dir.display()),
                    source,
                })?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| ArtifactError::Io {
                message: format!("failed to iterate artifact output dir {}", dir.display()),
                source,
            })?
        {
            let entry_path = entry.path();
            let metadata = fs::symlink_metadata(entry_path.as_path())
                .await
                .map_err(|source| ArtifactError::Io {
                    message: format!(
                        "failed to inspect artifact output entry {}",
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
    use std::time::SystemTime;

    #[tokio::test]
    async fn artifact_output_dir_path_is_workspace_thread_turn_scoped() {
        let root = PathBuf::from("/tmp/runtime/artifacts");
        let path = artifact_output_dir_path(&root, "ws_1", "thr-1", "turn_1").expect("path");

        assert_eq!(
            path,
            PathBuf::from("/tmp/runtime/artifacts/workspaces/ws_1/output/thr-1/turn_1")
        );
    }

    #[tokio::test]
    async fn gc_removes_expired_artifact_output_dirs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact_root = temp.path().join("artifacts");
        let dir = create_artifact_output_dir(&artifact_root, "ws_gc", "thr_gc", "turn_old")
            .await
            .expect("create output dir");
        fs::write(dir.path.join("result.txt"), b"result")
            .await
            .expect("write output");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis() as i64
            + 60_000;
        let output_path = dir.path.clone();
        drop(dir);
        let report = execute_output_dir_gc(&artifact_root, "ws_gc", now_ms, 0)
            .await
            .expect("gc");

        assert_eq!(report.deleted_dirs, 1);
        assert_eq!(report.plan.candidates.len(), 1);
        assert!(!fs::try_exists(output_path).await.expect("exists check"));
    }

    #[tokio::test]
    async fn gc_keeps_recent_artifact_output_dirs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact_root = temp.path().join("artifacts");
        let dir = create_artifact_output_dir(&artifact_root, "ws_gc", "thr_gc", "turn_recent")
            .await
            .expect("create output dir");
        fs::write(dir.path.join("result.txt"), b"result")
            .await
            .expect("write output");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis() as i64;
        let report = execute_output_dir_gc(&artifact_root, "ws_gc", now_ms, 24 * 60 * 60)
            .await
            .expect("gc");

        assert_eq!(report.deleted_dirs, 0);
        assert!(fs::try_exists(dir.path).await.expect("exists check"));
    }

    #[tokio::test]
    async fn gc_never_removes_an_active_artifact_output_dir() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifact_root = temp.path().join("artifacts");
        let dir = create_artifact_output_dir(&artifact_root, "ws_gc", "thr_gc", "turn_active")
            .await
            .expect("create active output dir");
        fs::write(dir.path.join("result.txt"), b"result")
            .await
            .expect("write output");

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis() as i64
            + 60_000;
        let report = execute_output_dir_gc(&artifact_root, "ws_gc", now_ms, 0)
            .await
            .expect("gc");

        assert_eq!(report.deleted_dirs, 0);
        assert!(report.plan.candidates.is_empty());
        assert!(fs::try_exists(dir.path).await.expect("exists check"));
    }
}
