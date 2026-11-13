//! Shared archive delivery policy. Installation paths belong to each domain.
use super::*;

#[derive(Clone)]
pub(crate) struct PackageDeliveryContext {
    pub upload_root: PathBuf,
    pub materialized_root: PathBuf,
    pub upload_ttl_secs: u64,
    pub upload_recommended_chunk_size_bytes: usize,
    pub upload_max_chunk_size_bytes: usize,
    pub max_upload_compressed_bytes: usize,
    pub max_upload_uncompressed_bytes: usize,
    pub max_upload_archive_entries: usize,
    pub max_file_bytes: usize,
}
impl PackageDeliveryContext {
    pub(super) fn plugin_limits(&self) -> pioneer_plugins::Limits {
        pioneer_plugins::Limits {
            expanded_bytes: self.max_upload_uncompressed_bytes,
            entry_bytes: self.max_file_bytes,
            entries: self.max_upload_archive_entries,
            // A path cannot have more directories than admitted entries.
            depth: self.max_upload_archive_entries,
            components: self.max_upload_archive_entries,
        }
    }
}
impl MessageProcessor {
    pub(crate) fn package_delivery_context(&self) -> PackageDeliveryContext {
        // Retain existing transfer limits/wire contracts. Resolving delivery
        // never loads a Skills catalog or requires configured Skills roots.
        let limits = &self.tool_loop_config.skills.security;
        PackageDeliveryContext {
            upload_root: self.artifact_runtime_home.join("package-delivery/uploads"),
            materialized_root: self
                .artifact_runtime_home
                .join("package-delivery/materialized"),
            upload_ttl_secs: limits.upload_ttl_secs.max(60),
            upload_recommended_chunk_size_bytes: limits.upload_recommended_chunk_size_bytes.max(1),
            upload_max_chunk_size_bytes: limits.upload_max_chunk_size_bytes.max(1),
            max_upload_compressed_bytes: limits.max_install_archive_compressed_bytes.max(1),
            max_upload_uncompressed_bytes: limits.max_install_archive_uncompressed_bytes.max(1),
            max_upload_archive_entries: limits.max_install_archive_entries.max(1),
            max_file_bytes: limits.max_install_file_bytes,
        }
    }
    pub(crate) fn plugin_storage_root(&self, workspace: &str) -> PathBuf {
        use sha2::{Digest, Sha256};
        // Existing parents retain their authoritative absolute package/data
        // paths, including pending staging and native locators. Only a NEW
        // identity uses this root. No filesystem migration or name matching.
        let key = hex::encode(Sha256::digest(workspace.as_bytes()));
        self.artifact_runtime_home.join("plugins").join(key)
    }
}

/// Remove an unpublished temporary tree when its owner is dropped.
/// Only arm after creating a new, exclusively owned directory. Published
/// package/data paths must never be armed here. No filesystem work runs in Drop.
pub(super) struct TemporaryDirectory {
    path: Option<PathBuf>,
    runtime: tokio::runtime::Handle,
}
impl TemporaryDirectory {
    pub(super) fn new() -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| anyhow::anyhow!("package_delivery_failed"))?;
        Ok(Self {
            path: None,
            runtime,
        })
    }
    pub(super) fn remove_on_drop(&mut self, path: PathBuf) {
        debug_assert!(self.path.is_none());
        self.path = Some(path);
    }
    /// The durable installation now owns this directory.
    pub(super) fn preserve(&mut self) {
        self.path = None;
    }
}
impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = self.runtime.spawn_blocking(move || {
                let _ = std::fs::remove_dir_all(path);
            });
        }
    }
}

/// Filesystem preparation. The closure owns its temporary output, so
/// dropping the async waiter drops the eventual output and its cleanup owner.
/// No DB/native publication may be dispatched from this closure.
pub(crate) async fn prepare<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> anyhow::Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| anyhow::anyhow!("package_delivery_failed"))
}

/// Cancel only discardable preparation. The waiter owns the cancellation
/// guard; the blocking job retains captured source admissions until it returns.
/// Durable filesystem changes use prepare.
pub(super) async fn prepare_cancellable<T: Send + 'static>(
    work: impl FnOnce(&tokio_util::sync::CancellationToken) -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    prepare(move || {
        check_preparation(&cancellation)?;
        let output = work(&cancellation)?;
        check_preparation(&cancellation)?;
        Ok(output)
    })
    .await?
}

pub(super) fn check_preparation(
    cancellation: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    anyhow::ensure!(!cancellation.is_cancelled(), "package_delivery_cancelled");
    Ok(())
}

/// Bound each read/checkpoint, including tar payload, header/skip reads and
/// hashing. Use a non-Interrupted error: io::copy retries Interrupted errors.
pub(super) fn preparation_reader<R: std::io::Read>(
    inner: R,
    cancellation: &tokio_util::sync::CancellationToken,
) -> impl std::io::Read {
    PreparationReader {
        inner,
        cancellation: cancellation.clone(),
    }
}
struct PreparationReader<R> {
    inner: R,
    cancellation: tokio_util::sync::CancellationToken,
}
impl<R: std::io::Read> std::io::Read for PreparationReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::other("package_delivery_cancelled"));
        }
        let length = buffer.len().min(64 * 1024);
        self.inner.read(&mut buffer[..length])
    }
}

pub(in crate::message) mod upload;

#[cfg(test)]
mod tests {
    use super::*;

    // NOT_RUN / NOT_COMPILED. One cleanup-owner contract, no Gateway/DB/native
    // fixture. The blocking pool is gated so an executor-thread recursive Drop
    // would remove the directory before the assertions and fail deterministically.
    #[test]
    fn temporary_cleanup_runs_off_executor_and_after_cancelled_preparation() {
        let root = tempfile::tempdir().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let scratch = |name: &str| {
                let mut owner = TemporaryDirectory::new().unwrap();
                let path = root.path().join(name);
                std::fs::create_dir(&path).unwrap();
                std::fs::write(path.join("payload"), b"temporary").unwrap();
                owner.remove_on_drop(path.clone());
                (path, owner)
            };
            let (temporary_path, temporary) = scratch("temporary");
            let (preserved_path, mut preserved) = scratch("preserved");
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.await.unwrap();
            preserved.preserve();
            drop(preserved);
            drop(temporary);
            assert!(temporary_path.exists(), "cleanup ran on the executor");
            assert!(preserved_path.exists());
            release_tx.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while temporary_path.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("temporary directory cleanup timed out");

            // Cancelling the waiter leaves the blocking job's output owned.
            let (cancelled_path, owner) = scratch("cancelled-output");
            let (prepared_tx, prepared_rx) = tokio::sync::oneshot::channel();
            let (finish_tx, finish_rx) = std::sync::mpsc::channel();
            let waiter = tokio::spawn(prepare(move || {
                prepared_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                owner
            }));
            prepared_rx.await.unwrap();
            waiter.abort();
            let cancelled = match waiter.await {
                Err(error) => error,
                Ok(_) => panic!("cancelled waiter completed"),
            };
            assert!(cancelled.is_cancelled());
            assert!(cancelled_path.exists());
            finish_tx.send(()).unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while cancelled_path.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled preparation cleanup timed out");
            assert!(preserved_path.exists());
        });
    }
    // NOT_RUN / NOT_COMPILED. Uses the actual waiter owner and bounded reader,
    // pausing after a real first block; no timer, transport or production hook.
    #[test]
    fn cancelled_preparation_stops_reading_after_its_current_checkpoint() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            use std::io::Read;
            let bytes = vec![7u8; 3 * 64 * 1024];
            let complete = bytes.clone();
            let success = prepare_cancellable(move |cancellation| {
                let mut reader = preparation_reader(std::io::Cursor::new(complete), cancellation);
                let mut output = Vec::new();
                std::io::copy(&mut reader, &mut output)?;
                Ok(output)
            })
            .await
            .unwrap();
            assert_eq!(success, bytes);

            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
            let waiter = tokio::spawn(prepare_cancellable(move |cancellation| {
                let mut input = std::io::Cursor::new(bytes);
                let mut reader = preparation_reader(&mut input, cancellation);
                let mut first = [0u8; 64 * 1024];
                let read = reader.read(&mut first)?;
                ready_tx.send(read).unwrap();
                release_rx.recv().unwrap();
                let stopped = std::io::copy(&mut reader, &mut std::io::sink()).is_err();
                drop(reader);
                finished_tx.send((stopped, input.position())).unwrap();
                Ok(())
            }));
            assert_eq!(ready_rx.await.unwrap(), 64 * 1024);
            waiter.abort();
            let cancelled = match waiter.await {
                Err(error) => error,
                Ok(_) => panic!("cancelled waiter completed"),
            };
            assert!(cancelled.is_cancelled());
            release_tx.send(()).unwrap();
            let (stopped, position) = finished_rx.await.unwrap();
            assert!(stopped);
            assert_eq!(position, 64 * 1024);
            // Following work can still complete through the same owner.
            assert_eq!(prepare_cancellable(|_| Ok(42)).await.unwrap(), 42);
        });
    }
}
