//! Periodic model metadata refresh, owned by Gateway's post-startup scope.
use super::{CatalogStore, ModelCatalog, catalog_store, fetch, generator};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub const REFRESH_INTERVAL: Duration = Duration::from_secs(30 * 60);
const CACHE_FILE: &str = "catalog.json";
const MAX_CACHE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct SavedCatalog {
    version: u32,
    updated_at: String,
    catalog: generator::GeneratedCatalog,
}
impl SavedCatalog {
    fn validate(&self) -> Result<ModelCatalog> {
        ensure!(self.version == 1, "unsupported model catalog cache version");
        chrono::DateTime::parse_from_rfc3339(&self.updated_at)?;
        self.catalog.validate()?;
        ModelCatalog::parse(
            &serde_json::to_string(&self.catalog.models)?,
            &serde_json::to_string(&self.catalog.provenance)?,
        )
    }
}

fn load_cache(directory: &Path) -> Result<Option<ModelCatalog>> {
    let file = match fs::File::open(directory.join(CACHE_FILE)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        file.metadata()?.len() <= MAX_CACHE_BYTES,
        "model catalog cache too large"
    );
    let mut bytes = Vec::new();
    file.take(MAX_CACHE_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_CACHE_BYTES,
        "model catalog cache too large"
    );
    let saved: SavedCatalog = serde_json::from_slice(&bytes)?;
    saved.validate().map(Some)
}

/// Restore previously downloaded JSON without fetching. Run on a blocking worker.
/// Missing cache leaves the catalog unavailable; malformed cache is an error.
pub fn restore_cached_catalog(directory: &Path) -> Result<()> {
    restore_cache(catalog_store(), directory)
}
fn restore_cache(store: &CatalogStore, directory: &Path) -> Result<()> {
    if let Some(catalog) = load_cache(directory)? {
        store.publish(catalog);
    }
    Ok(())
}

// All transforms and filesystem work finish on one bounded blocking task. The
// owner awaits it during shutdown; no detached writer can publish after exit.
fn prepare_update(directory: &Path, snapshot: generator::SourceSnapshot) -> Result<ModelCatalog> {
    let saved = SavedCatalog {
        version: 1,
        updated_at: snapshot.captured_at.clone(),
        catalog: generator::generate(&snapshot, false)?,
    };
    let catalog = saved.validate()?;
    let bytes = serde_json::to_vec(&saved)?;
    ensure!(
        bytes.len() as u64 <= MAX_CACHE_BYTES,
        "model catalog cache too large"
    );
    fs::create_dir_all(directory)?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(directory.join(CACHE_FILE))?;
    Ok(catalog)
}

#[async_trait]
trait CatalogSource: Send + Sync {
    async fn fetch(&self) -> Result<generator::SourceSnapshot>;
}
struct PublicSources;
#[async_trait]
impl CatalogSource for PublicSources {
    async fn fetch(&self) -> Result<generator::SourceSnapshot> {
        fetch::fetch_snapshot().await
    }
}

/// Starts with saved JSON if available, refreshes immediately and then
/// every 30 minutes. Metadata updates require neither credentials nor an LLM.
/// Call once per Gateway process from its owned post-startup task scope.
pub async fn run_catalog_updates(directory: PathBuf, cancellation: CancellationToken) {
    if cancellation.is_cancelled() {
        return;
    }
    let saved_directory = directory.clone();
    if !matches!(
        tokio::task::spawn_blocking(move || restore_cached_catalog(&saved_directory)).await,
        Ok(Ok(()))
    ) {
        tracing::warn!("model catalog cache unavailable; awaiting successful download");
    }
    run_updates(
        catalog_store(),
        directory,
        cancellation,
        &PublicSources,
        REFRESH_INTERVAL,
    )
    .await;
}

async fn run_updates(
    store: &CatalogStore,
    directory: PathBuf,
    cancellation: CancellationToken,
    source: &dyn CatalogSource,
    interval: Duration,
) {
    if cancellation.is_cancelled() {
        return;
    }
    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = timer.tick() => {},
        }
        let snapshot = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = source.fetch() => result,
        };
        let result = match snapshot {
            Ok(snapshot) => {
                let directory = directory.clone();
                tokio::task::spawn_blocking(move || prepare_update(&directory, snapshot))
                    .await
                    .context("model catalog update task")
                    .and_then(|result| result)
            }
            Err(error) => Err(error),
        };
        if cancellation.is_cancelled() {
            return;
        }
        match result {
            Ok(catalog) => {
                store.publish(catalog);
                tracing::debug!("model catalog refreshed");
            }
            Err(_) => tracing::warn!("model catalog refresh failed; retaining previous metadata"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    fn snapshot(label: &str) -> generator::SourceSnapshot {
        let mut source: generator::SourceSnapshot =
            serde_json::from_str(include_str!("../../tests/fixtures/catalog/sources.json"))
                .unwrap();
        source
            .sources
            .get_mut(generator::SOURCE_URLS[0])
            .unwrap()
            .body["openai"]["models"]["gpt-5-nano"]["name"] = label.into();
        let models = &mut source
            .sources
            .get_mut(generator::SOURCE_URLS[0])
            .unwrap()
            .body["openai"]["models"];
        models["runtime-new-model"] = models["gpt-5-nano"].clone();
        source
    }
    fn label(store: &CatalogStore) -> String {
        store
            .snapshot()
            .unwrap()
            .model("openai", "gpt-5-nano")
            .unwrap()
            .name
            .clone()
    }
    #[test]
    fn saved_catalog_survives_restart_and_invalid_update_keeps_previous_json() {
        let dir = tempfile::tempdir().unwrap();
        let store = CatalogStore::default();
        assert!(store.snapshot().is_err());
        store.publish(prepare_update(dir.path(), snapshot("previous model")).unwrap());
        let old_reader = store.snapshot().unwrap();
        store.publish(prepare_update(dir.path(), snapshot("fresh model")).unwrap());
        assert_eq!(label(&store), "fresh model");
        assert!(
            store
                .snapshot()
                .unwrap()
                .model("openai", "runtime-new-model")
                .is_some()
        );
        assert_eq!(
            old_reader
                .model("openai", "runtime-new-model")
                .unwrap()
                .name,
            "previous model"
        );
        assert_ne!(
            old_reader.model("openai", "gpt-5-nano").unwrap().name,
            "fresh model"
        );
        let bytes = fs::read(dir.path().join(CACHE_FILE)).unwrap();
        let mut invalid = snapshot("bad update");
        invalid
            .sources
            .get_mut(generator::SOURCE_URLS[1])
            .unwrap()
            .status = 503;
        assert!(prepare_update(dir.path(), invalid).is_err());
        assert_eq!(fs::read(dir.path().join(CACHE_FILE)).unwrap(), bytes);
        let restarted = CatalogStore::default();
        restarted.publish(load_cache(dir.path()).unwrap().unwrap());
        assert_eq!(label(&restarted), "fresh model");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
    #[test]
    fn absent_corrupt_and_oversized_cache_leave_catalog_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_cache(dir.path()).unwrap().is_none());
        fs::write(dir.path().join(CACHE_FILE), b"broken").unwrap();
        assert!(load_cache(dir.path()).is_err());
        fs::File::create(dir.path().join(CACHE_FILE))
            .unwrap()
            .set_len(MAX_CACHE_BYTES + 1)
            .unwrap();
        assert!(load_cache(dir.path()).is_err());
        let store = CatalogStore::default();
        assert!(restore_cache(&store, dir.path()).is_err());
        assert!(store.snapshot().is_err());
    }
    struct FakeSource {
        calls: Arc<AtomicUsize>,
        values: Mutex<std::collections::VecDeque<Result<generator::SourceSnapshot>>>,
    }
    #[async_trait]
    impl CatalogSource for FakeSource {
        async fn fetch(&self) -> Result<generator::SourceSnapshot> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.values
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(anyhow::anyhow!("offline")))
        }
    }
    #[tokio::test]
    async fn first_launch_refreshes_periodically_after_failure_and_stops_on_shutdown() {
        assert_eq!(REFRESH_INTERVAL, Duration::from_secs(1800));
        let dir = tempfile::tempdir().unwrap();
        let directory = dir.path().to_owned();
        let store = Arc::new(CatalogStore::default());
        let worker_store = store.clone();
        let cancellation = CancellationToken::new();
        let worker_cancel = cancellation.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let source = FakeSource {
            calls: calls.clone(),
            values: Mutex::new(
                [
                    Err(anyhow::anyhow!("offline")),
                    Ok(snapshot("online again")),
                ]
                .into(),
            ),
        };
        let worker = tokio::spawn(async move {
            run_updates(
                &worker_store,
                directory,
                worker_cancel,
                &source,
                Duration::from_millis(30),
            )
            .await;
        });
        tokio::time::timeout(Duration::from_secs(30), async {
            while store.snapshot().is_err() || label(&store) != "online again" {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        worker.await.unwrap();
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert_eq!(label(&store), "online again");
        assert_eq!(
            load_cache(dir.path())
                .unwrap()
                .unwrap()
                .model("openai", "gpt-5-nano")
                .unwrap()
                .name,
            "online again"
        );
        let stopped = calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(calls.load(Ordering::SeqCst), stopped);
    }
    struct PendingSource(tokio::sync::Notify);
    #[async_trait]
    impl CatalogSource for PendingSource {
        async fn fetch(&self) -> Result<generator::SourceSnapshot> {
            self.0.notify_one();
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn first_launch_without_cache_stays_unavailable_while_download_is_pending() {
        let dir = tempfile::tempdir().unwrap();
        let store = CatalogStore::default();
        restore_cache(&store, dir.path()).unwrap();
        assert!(store.snapshot().is_err());
        let source = PendingSource(tokio::sync::Notify::new());
        let cancellation = CancellationToken::new();
        let run = run_updates(
            &store,
            dir.path().to_owned(),
            cancellation.clone(),
            &source,
            REFRESH_INTERVAL,
        );
        let stop = async {
            source.0.notified().await;
            assert!(store.snapshot().is_err());
            cancellation.cancel();
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(run, stop);
        })
        .await
        .unwrap();
        assert!(store.snapshot().is_err());
        assert!(!dir.path().join(CACHE_FILE).exists());
    }
    #[tokio::test]
    async fn startup_loads_saved_catalog_and_shutdown_cancels_pending_fetch() {
        let dir = tempfile::tempdir().unwrap();
        prepare_update(dir.path(), snapshot("saved at shutdown")).unwrap();
        let store = CatalogStore::default();
        restore_cache(&store, dir.path()).unwrap();
        let source = PendingSource(tokio::sync::Notify::new());
        let cancellation = CancellationToken::new();
        let run = run_updates(
            &store,
            dir.path().to_owned(),
            cancellation.clone(),
            &source,
            REFRESH_INTERVAL,
        );
        let stop = async {
            source.0.notified().await;
            assert_eq!(label(&store), "saved at shutdown");
            cancellation.cancel();
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(run, stop);
        })
        .await
        .unwrap();
        assert_eq!(label(&store), "saved at shutdown");
    }
}
