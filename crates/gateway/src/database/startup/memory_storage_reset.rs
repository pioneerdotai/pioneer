//! Finite cleanup owned by startup maintenance, under Gateway WriterOwnership.
use anyhow::{Context, Result, ensure};
use pioneer_crud::{CrudStore, MemoryPayloadResetOutcome};
use std::path::{Path, PathBuf};

pub(super) async fn run(store: &CrudStore, runtime_home: &Path) -> Result<()> {
    let cancellation = super::maintenance_cancellation();
    loop {
        let outcome = tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(super::StartupMaintenanceCancelled.into()),
            result = store.reset_legacy_memory_payload_quantum(chrono::Utc::now().timestamp()) => result?,
        };
        match outcome {
            MemoryPayloadResetOutcome::Progress => super::maintenance_checkpoint().await?,
            MemoryPayloadResetOutcome::FilesPending | MemoryPayloadResetOutcome::Complete => break,
        }
    }
    if store.legacy_memory_tree_cleanup_complete().await? {
        return Ok(());
    }
    // Refuse a linked memory parent: resolving capsules through it would leave
    // the instance ownership boundary even though the final name is fixed.
    match tokio::fs::symlink_metadata(runtime_home.join("memory")).await {
        Ok(metadata) => ensure!(
            !metadata.file_type().is_symlink() && metadata.is_dir(),
            "legacy memory parent is not an owned directory"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect memory parent"),
    }
    // Only this fixed, exclusively Memvid-owned tree is disposable. Current
    // refill locks live in sibling memory/locks and are never unlinked here.
    clean_owned_capsules(&runtime_home.join("memory/capsules")).await?;
    super::maintenance_checkpoint().await?;
    store
        .complete_legacy_memory_reset(chrono::Utc::now().timestamp())
        .await
}

async fn remove_legacy_file(path: &Path, is_regular: bool) -> Result<()> {
    if is_regular
        && path
            .extension()
            .is_some_and(|extension| extension == "lock")
    {
        // Startup's cooperative DB WriterOwnership excludes supported legacy
        // writers for this instance. Also refuse a visibly held legacy inode.
        // No support for concurrent old Memvid backends or arbitrary custom roots.
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).context("open legacy lock for cleanup"),
        };
        fs4::FileExt::try_lock(&file)
            .map_err(|_| anyhow::anyhow!("legacy memory lock is held or unavailable"))?;
        // Release our probe before unlink. Never unlink an inode we still hold.
        fs4::FileExt::unlock(&file).context("release legacy lock probe")?;
        drop(file);
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).context("remove owned legacy memory entry"),
    }
}

async fn clean_owned_capsules(root: &Path) -> Result<()> {
    let metadata = match tokio::fs::symlink_metadata(root).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("inspect legacy memory tree"),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return remove_legacy_file(root, metadata.is_file()).await;
    }
    let cancellation = super::maintenance_cancellation();
    let mut stack: Vec<(PathBuf, tokio::fs::ReadDir)> =
        vec![(root.to_owned(), tokio::fs::read_dir(root).await?)];
    let mut quantum = 0;
    while let Some((_, directory)) = stack.last_mut() {
        let entry = tokio::select! { biased;
            _ = cancellation.cancelled() => return Err(super::StartupMaintenanceCancelled.into()),
            result = directory.next_entry() => result.context("read legacy memory directory")?,
        };
        if let Some(entry) = entry {
            let kind = entry
                .file_type()
                .await
                .context("inspect legacy memory entry")?;
            if kind.is_dir() && !kind.is_symlink() {
                // Bound open descriptors, but never silently skip an obligation.
                ensure!(
                    stack.len() < 128,
                    "legacy memory tree exceeds cleanup depth budget"
                );
                stack.push((entry.path(), tokio::fs::read_dir(entry.path()).await?));
            } else {
                // Symlinks are unlinked themselves, never followed. All regular
                // service artifacts in this dedicated tree are obsolete.
                remove_legacy_file(&entry.path(), kind.is_file()).await?;
            }
        } else {
            let (path, directory) = stack.pop().expect("nonempty cleanup stack");
            drop(directory);
            match tokio::fs::remove_dir(path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("remove legacy memory directory"),
            }
        }
        quantum += 1;
        if quantum == 64 {
            quantum = 0;
            super::maintenance_checkpoint().await?;
        }
    }
    Ok(())
}
