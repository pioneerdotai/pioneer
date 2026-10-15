//! Periodic model metadata refresh, owned by Gateway's post-startup scope.
use super::{CatalogStore, ModelCatalog, catalog_store, fetch, generator};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{OnceLock, RwLock},
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub const REFRESH_INTERVAL: Duration = Duration::from_secs(30 * 60);
const CACHE_FILE: &str = "catalog.json";
const MAX_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const BUNDLED_CATALOG: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/bundled_model_catalog.json"));

#[derive(Default)]
struct RefreshControl {
    proxy_url: RwLock<Option<String>>,
    changed: Notify,
}

fn refresh_control() -> &'static RefreshControl {
    static CONTROL: OnceLock<RefreshControl> = OnceLock::new();
    CONTROL.get_or_init(RefreshControl::default)
}

/// Update the process-wide catalog transport without restarting Gateway.
/// The raw URL is retained only in memory and is never logged.
pub fn normalize_refresh_proxy(proxy_url: Option<String>) -> Result<Option<String>> {
    proxy_url
        .map(|value| crate::http::validate_proxy_url(&value))
        .transpose()
}

pub fn configure_refresh_proxy(proxy_url: Option<String>) -> Result<()> {
    let normalized = normalize_refresh_proxy(proxy_url)?;
    *refresh_control()
        .proxy_url
        .write()
        .expect("catalog proxy lock") = normalized;
    refresh_control().changed.notify_one();
    Ok(())
}

fn refresh_proxy() -> Option<String> {
    refresh_control()
        .proxy_url
        .read()
        .expect("catalog proxy lock")
        .clone()
}

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
        ModelCatalog::parse_with_capabilities(
            &serde_json::to_string(&self.catalog.models)?,
            &serde_json::to_string(&self.catalog.provenance)?,
            self.catalog.tool_capabilities.clone(),
        )
    }
}

fn load_saved_cache(directory: &Path) -> Result<Option<SavedCatalog>> {
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
    saved.validate()?;
    Ok(Some(saved))
}

fn load_cache(directory: &Path) -> Result<Option<ModelCatalog>> {
    load_saved_cache(directory)?
        .map(|saved| saved.validate())
        .transpose()
}

fn validate_saved_bytes(bytes: &[u8]) -> Result<ModelCatalog> {
    ensure!(!bytes.is_empty(), "bundled model catalog is unavailable");
    ensure!(
        bytes.len() as u64 <= MAX_CACHE_BYTES,
        "model catalog cache too large"
    );
    serde_json::from_slice::<SavedCatalog>(bytes)?.validate()
}

fn persist_bytes(directory: &Path, bytes: &[u8]) -> Result<()> {
    fs::create_dir_all(directory)?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(directory.join(CACHE_FILE))?;
    Ok(())
}

/// Publish the last-good cache, or atomically install the catalog embedded by
/// the release build into the exact same path used by network refreshes.
pub fn restore_or_install_catalog(directory: &Path) -> Result<()> {
    restore_or_install_catalog_from_bytes(catalog_store(), directory, BUNDLED_CATALOG)
}

fn restore_or_install_catalog_from_bytes(
    store: &CatalogStore,
    directory: &Path,
    bundled_catalog: &[u8],
) -> Result<()> {
    match load_cache(directory) {
        Ok(Some(catalog)) => {
            store.publish(catalog);
            return Ok(());
        }
        Ok(None) => {}
        Err(error) if bundled_catalog.is_empty() => return Err(error),
        Err(_) => {}
    }
    let catalog = validate_saved_bytes(bundled_catalog)?;
    persist_bytes(directory, bundled_catalog)?;
    store.publish(catalog);
    Ok(())
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
    let mut generated = generator::generate(&snapshot, false)?;
    if generator::SOURCE_URLS
        .iter()
        .any(|url| snapshot.source_body(url).is_none())
    {
        match load_saved_cache(directory) {
            Ok(Some(previous)) => {
                retain_unavailable_sources(&mut generated, previous.catalog, &snapshot)
            }
            Ok(None) => {}
            Err(_) => {
                tracing::warn!("previous model catalog cache invalid; using available sources")
            }
        }
    }
    if !generated.diagnostics.is_empty() {
        tracing::warn!(diagnostics = ?generated.diagnostics,
            "model catalog refresh isolated unavailable sources or invalid records");
    }
    let saved = SavedCatalog {
        version: 1,
        updated_at: snapshot.captured_at.clone(),
        catalog: generated,
    };
    let catalog = saved.validate()?;
    let bytes = serde_json::to_vec(&saved)?;
    ensure!(
        bytes.len() as u64 <= MAX_CACHE_BYTES,
        "model catalog cache too large"
    );
    persist_bytes(directory, &bytes)?;
    Ok(catalog)
}

/// Keep last-good evidence only for sources that actually failed. Successful
/// native listings (including capability removals) replace their old evidence.
/// Retained entries keep their original pricing timestamp and provenance.
fn retain_unavailable_sources(
    current: &mut generator::GeneratedCatalog,
    previous: generator::GeneratedCatalog,
    snapshot: &generator::SourceSnapshot,
) {
    use generator::SOURCE_URLS;
    for (provider, models) in previous.models {
        for (id, model) in models {
            let source = model["pricingSource"]["url"].as_str();
            let unavailable = source.is_some_and(|url| {
                SOURCE_URLS.contains(&url) && snapshot.source_body(url).is_none()
            }) || provider == "nvidia"
                && snapshot.source_body(SOURCE_URLS[3]).is_none();
            if !unavailable {
                continue;
            }
            if provider == "nvidia"
                && let Some(native) = snapshot.source_body(SOURCE_URLS[3])
                && !native["data"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|model| model["id"] == id)
            {
                // models.dev can fail while the native NVIDIA inventory
                // succeeds. Its removals still govern last-good supplements.
                continue;
            }
            let Some(origins) = previous
                .provenance
                .get(&provider)
                .and_then(|models| models.get(&id))
            else {
                continue;
            };
            current
                .models
                .entry(provider.clone())
                .or_default()
                .insert(id.clone(), model);
            current
                .provenance
                .entry(provider.clone())
                .or_default()
                .insert(id, origins.clone());
        }
    }
    let capabilities = current
        .tool_capabilities
        .get_or_insert_with(Default::default);
    for (provider, models) in previous.tool_capabilities.unwrap_or_default() {
        let native_failed = match provider.as_str() {
            "openrouter" => snapshot.source_body(SOURCE_URLS[1]).is_none(),
            "vercel-ai-gateway" => snapshot.source_body(SOURCE_URLS[2]).is_none(),
            _ => false,
        };
        if native_failed {
            capabilities.insert(provider, models);
        } else if !matches!(provider.as_str(), "openrouter" | "vercel-ai-gateway")
            && (snapshot.source_body(SOURCE_URLS[0]).is_none()
                || provider == "nvidia" && snapshot.source_body(SOURCE_URLS[3]).is_none())
        {
            let entries = capabilities.entry(provider).or_default();
            for (id, supported) in models {
                entries.entry(id).or_insert(supported);
            }
        }
    }
}

/// Release-pipeline entry point. Network access happens only when this is
/// called explicitly; ordinary local Cargo builds never fetch metadata.
pub async fn generate_catalog_file(output: &Path, proxy_url: Option<&str>) -> Result<()> {
    let snapshot = fetch::fetch_snapshot(proxy_url).await?;
    let parent = output
        .parent()
        .context("model catalog output path has no parent")?;
    let saved = SavedCatalog {
        version: 1,
        updated_at: snapshot.captured_at.clone(),
        catalog: generator::generate(&snapshot, false)?,
    };
    saved.validate()?;
    let bytes = serde_json::to_vec(&saved)?;
    ensure!(
        bytes.len() as u64 <= MAX_CACHE_BYTES,
        "model catalog cache too large"
    );
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(output)?;
    Ok(())
}

#[async_trait]
trait CatalogSource: Send + Sync {
    async fn fetch(&self) -> Result<generator::SourceSnapshot>;
}
struct PublicSources;
#[async_trait]
impl CatalogSource for PublicSources {
    async fn fetch(&self) -> Result<generator::SourceSnapshot> {
        let proxy_url = refresh_proxy();
        fetch::fetch_snapshot(proxy_url.as_deref()).await
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
        tokio::task::spawn_blocking(move || restore_or_install_catalog(&saved_directory)).await,
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
            _ = refresh_control().changed.notified() => {},
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
    fn partial_refresh_preserves_failed_source_evidence_and_updates_healthy_sources() {
        for failed_url in generator::SOURCE_URLS {
            let dir = tempfile::tempdir().unwrap();
            let source = super::super::tool_tests::source_snapshot();
            prepare_update(dir.path(), source.clone()).unwrap();
            let before = load_saved_cache(dir.path()).unwrap().unwrap();
            let mut newer = source;
            newer.captured_at = "2026-10-08T12:00:00Z".into();
            newer.sources.get_mut(failed_url).unwrap().status = 503;
            newer
                .sources
                .get_mut(generator::SOURCE_URLS[0])
                .unwrap()
                .body["openai"]["models"]["gpt-5-nano"]["name"] =
                serde_json::json!("healthy update");
            let catalog = prepare_update(dir.path(), newer).unwrap();
            let after = load_saved_cache(dir.path()).unwrap().unwrap();
            after.validate().unwrap();
            for (provider, models) in &before.catalog.models {
                for (id, model) in models {
                    if model["pricingSource"]["url"] == failed_url
                        || provider == "nvidia" && failed_url == generator::SOURCE_URLS[3]
                    {
                        assert_eq!(&after.catalog.models[provider][id], model);
                        assert_eq!(
                            after.catalog.provenance[provider][id],
                            before.catalog.provenance[provider][id]
                        );
                    }
                }
            }
            assert_eq!(
                catalog.tool_support("openrouter", "g03-negative"),
                Some(false)
            );
            assert_eq!(catalog.tool_support("openai", "g03-negative"), Some(false));
            if failed_url != generator::SOURCE_URLS[0] {
                assert_eq!(
                    catalog.model("openai", "gpt-5-nano").unwrap().name,
                    "healthy update"
                );
            }
            assert!(
                after
                    .catalog
                    .diagnostics
                    .iter()
                    .any(|d| d.source == failed_url && d.code == "source_unavailable")
            );
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }

    #[test]
    fn cold_partial_catalog_and_healthy_native_removals_do_not_require_failed_sources() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = super::super::tool_tests::source_snapshot();
        source.sources.remove(generator::SOURCE_URLS[0]);
        let catalog = prepare_update(dir.path(), source.clone()).unwrap();
        assert_eq!(
            catalog.tool_support("openrouter", "g03-negative"),
            Some(false)
        );
        assert!(
            !load_saved_cache(dir.path())
                .unwrap()
                .unwrap()
                .catalog
                .models["openrouter"]
                .is_empty()
        );
        source
            .sources
            .get_mut(generator::SOURCE_URLS[1])
            .unwrap()
            .body["data"]
            .as_array_mut()
            .unwrap()
            .retain(|model| model["id"] != "g03-negative");
        let catalog = prepare_update(dir.path(), source).unwrap();
        assert_eq!(catalog.tool_support("openrouter", "g03-negative"), None);
    }
    #[test]
    fn capability_supplement_round_trips_and_refresh_replaces_snapshot_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store = CatalogStore::default();
        let source = super::super::tool_tests::source_snapshot();
        store.publish(prepare_update(dir.path(), source.clone()).unwrap());
        let old_reader = store.snapshot().unwrap();
        assert_eq!(
            old_reader.tool_support("openai", "g03-negative"),
            Some(false)
        );
        let restored = CatalogStore::default();
        restore_cache(&restored, dir.path()).unwrap();
        assert_eq!(
            restored
                .snapshot()
                .unwrap()
                .tool_support("azure_openai", "g03-negative"),
            Some(false)
        );
        let mut newer = source;
        newer
            .sources
            .get_mut(generator::SOURCE_URLS[0])
            .unwrap()
            .body["openai"]["models"]["g03-negative"]["tool_call"] = serde_json::json!(true);
        store.publish(prepare_update(dir.path(), newer.clone()).unwrap());
        assert_eq!(
            store
                .snapshot()
                .unwrap()
                .tool_support("openai", "g03-negative"),
            Some(true)
        );
        assert_eq!(
            old_reader.tool_support("openai", "g03-negative"),
            Some(false)
        );
        let saved_bytes = fs::read(dir.path().join(CACHE_FILE)).unwrap();
        for response in newer.sources.values_mut() {
            response.status = 503;
        }
        assert!(prepare_update(dir.path(), newer).is_err());
        assert_eq!(fs::read(dir.path().join(CACHE_FILE)).unwrap(), saved_bytes);
        let mut legacy: serde_json::Value = serde_json::from_slice(&saved_bytes).unwrap();
        legacy["catalog"]
            .as_object_mut()
            .unwrap()
            .remove("tool_capabilities");
        assert!(validate_saved_bytes(&serde_json::to_vec(&legacy).unwrap()).is_ok());
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
        for response in invalid.sources.values_mut() {
            response.status = 503;
        }
        assert!(prepare_update(dir.path(), invalid).is_err());
        assert_eq!(fs::read(dir.path().join(CACHE_FILE)).unwrap(), bytes);
        let restarted = CatalogStore::default();
        restarted.publish(load_cache(dir.path()).unwrap().unwrap());
        assert_eq!(label(&restarted), "fresh model");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }
    #[test]
    fn bundled_catalog_materializes_to_cache_path_and_existing_cache_wins() {
        let source_dir = tempfile::tempdir().unwrap();
        prepare_update(source_dir.path(), snapshot("bundled model")).unwrap();
        let bundled = fs::read(source_dir.path().join(CACHE_FILE)).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let store = CatalogStore::default();
        restore_or_install_catalog_from_bytes(&store, dir.path(), &bundled).unwrap();
        assert_eq!(label(&store), "bundled model");
        assert_eq!(fs::read(dir.path().join(CACHE_FILE)).unwrap(), bundled);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);

        prepare_update(dir.path(), snapshot("downloaded model")).unwrap();
        let restarted = CatalogStore::default();
        restore_or_install_catalog_from_bytes(&restarted, dir.path(), &bundled).unwrap();
        assert_eq!(label(&restarted), "downloaded model");
    }

    #[test]
    fn corrupt_cache_is_replaced_only_by_a_valid_bundle() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(CACHE_FILE), b"broken").unwrap();
        let store = CatalogStore::default();
        assert!(restore_or_install_catalog_from_bytes(&store, dir.path(), b"broken").is_err());
        assert_eq!(fs::read(dir.path().join(CACHE_FILE)).unwrap(), b"broken");

        let source_dir = tempfile::tempdir().unwrap();
        prepare_update(source_dir.path(), snapshot("fallback model")).unwrap();
        let bundled = fs::read(source_dir.path().join(CACHE_FILE)).unwrap();
        restore_or_install_catalog_from_bytes(&store, dir.path(), &bundled).unwrap();
        assert_eq!(label(&store), "fallback model");
        assert_eq!(fs::read(dir.path().join(CACHE_FILE)).unwrap(), bundled);
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
