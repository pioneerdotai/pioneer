//! One storage mutation boundary covers put+registration and checked unlink.
//! Local stores coordinate independent Gateway/operations processes as well as
//! service instances. No database capacity is acquired while waiting here.
use crate::{ArtifactError, ArtifactResult, ids::workspace_segment};
use std::{
    fs::{File, OpenOptions, TryLockError},
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone)]
pub enum ArtifactBlobMutationGuard {
    Local { _file: Arc<File> },
    Memory { _guard: Arc<OwnedMutexGuard<()>> },
}

pub(crate) async fn acquire(
    root: Option<PathBuf>,
    workspace: &str,
) -> ArtifactResult<ArtifactBlobMutationGuard> {
    let Some(root) = root else {
        static GATE: OnceLock<Arc<Mutex<()>>> = OnceLock::new();
        return Ok(ArtifactBlobMutationGuard::Memory {
            _guard: Arc::new(
                GATE.get_or_init(Default::default)
                    .clone()
                    .lock_owned()
                    .await,
            ),
        });
    };
    let directory = root.join("workspaces").join(workspace_segment(workspace)?);
    loop {
        let directory = directory.clone();
        let file = pioneer_observability::work::spawn_blocking(
            move || -> std::io::Result<Option<File>> {
                std::fs::create_dir_all(&directory)?;
                // This file is permanent coordination metadata. Unlinking a lock
                // file would let another process lock a different inode.
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(directory.join(".blob-mutations.lock"))?;
                match file.try_lock() {
                    Ok(()) => Ok(Some(file)),
                    Err(TryLockError::WouldBlock) => Ok(None),
                    Err(TryLockError::Error(error)) => Err(error),
                }
            },
        )
        .await
        .map_err(|_| ArtifactError::ContentInvariant {
            reason: "artifact mutation lock worker failed",
        })?
        .map_err(|source| ArtifactError::Io {
            message: "failed to acquire artifact mutation lock".into(),
            source,
        })?;
        if let Some(file) = file {
            return Ok(ArtifactBlobMutationGuard::Local {
                _file: Arc::new(file),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
