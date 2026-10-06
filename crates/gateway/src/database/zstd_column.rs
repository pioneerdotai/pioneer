use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{
    ConnectionTrait, Statement, TransactionTrait, entity::prelude::DateTimeWithTimeZone,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use zstd::dict::EncoderDictionary;

pub(crate) const PERIODIC_MAINTENANCE_INTERVAL_SECONDS: u64 = 300;
pub(crate) const PERIODIC_BACKLOG_RECHECK_MILLIS: u64 = 250;
pub(crate) const PERIODIC_MAINTENANCE_SECONDS: f64 = 10.0;
#[cfg(test)]
pub(crate) const PERIODIC_MAINTENANCE_SLICE_SECONDS: f64 = 0.25;
pub(crate) const PERIODIC_TARGET_DB_LOAD: f64 = 0.25;

// A batch is bounded before payload bytes enter memory. A single oversized
// row is still admitted so it cannot starve forever; Pioneer already bounds
// individual persisted payloads at their domain ingress.
const COMPRESSION_BATCH_MAX_ROWS: usize = 32;
const COMPRESSION_BATCH_MAX_SOURCE_BYTES: usize = 1024 * 1024;
const DICTIONARY_SAMPLE_MAX_ROWS: usize = 2048;
const DICTIONARY_SAMPLE_MAX_SOURCE_BYTES: usize = 8 * 1024 * 1024;
const DICTIONARY_MIN_SOURCE_BYTES: usize = 500_000;
const DICTIONARY_MIN_SAMPLE_ROWS: usize = 8;
const DICTIONARY_MIN_BYTES: usize = 5_000;
const DICTIONARY_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ZstdColumnConfig {
    pub(crate) projection_key: &'static str,
    pub(crate) projection_version: i64,
    pub(crate) table: &'static str,
    pub(crate) column: &'static str,
    pub(crate) backing_table: &'static str,
    pub(crate) dict_column: &'static str,
    pub(crate) dict_chooser: &'static str,
    pub(crate) dictionary_key: &'static str,
    pub(crate) compression_level: i32,
    pub(crate) count_source: ZstdColumnCountSource,
}

impl ZstdColumnConfig {
    fn label(self) -> String {
        format!("{}.{}", self.table, self.column)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZstdColumnCountSource {
    TurnEvent,
    TurnItem,
}

pub(crate) const TURN_EVENT_PAYLOAD: ZstdColumnConfig = ZstdColumnConfig {
    projection_key: "turn_event_payload_zstd_compression",
    projection_version: 1,
    table: "turn_event",
    column: "payload",
    backing_table: "_turn_event_zstd",
    dict_column: "_payload_dict",
    dict_chooser: "'turn_event.payload'",
    dictionary_key: "turn_event.payload",
    compression_level: 19,
    count_source: ZstdColumnCountSource::TurnEvent,
};

pub(crate) const TURN_ITEM_PAYLOAD: ZstdColumnConfig = ZstdColumnConfig {
    projection_key: "turn_item_payload_zstd_compression",
    projection_version: 1,
    table: "turn_item",
    column: "payload",
    backing_table: "_turn_item_zstd",
    dict_column: "_payload_dict",
    dict_chooser: "'turn_item.payload'",
    dictionary_key: "turn_item.payload",
    compression_level: 19,
    count_source: ZstdColumnCountSource::TurnItem,
};

pub(crate) const ZSTD_PAYLOAD_COLUMNS: &[ZstdColumnConfig] =
    &[TURN_EVENT_PAYLOAD, TURN_ITEM_PAYLOAD];

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ZstdColumnCompressionSummary {
    pub(crate) table: &'static str,
    pub(crate) column: &'static str,
    pub(crate) enabled_now: bool,
    pub(crate) already_enabled: bool,
    pub(crate) skipped_empty: bool,
    pub(crate) total_rows: u64,
    pub(crate) pending_before: u64,
    pub(crate) pending_after: u64,
    pub(crate) compressed_rows: u64,
    pub(crate) stale_rows: u64,
    pub(crate) source_bytes: u64,
    pub(crate) maintenance_more_pending: bool,
    /// Exact row counts are intentionally collected only by explicit test and
    /// diagnostic entry points. Production cooperative maintenance must not
    /// turn telemetry into an unbounded full-table scan while it owns the
    /// Gateway's single SQLite connection.
    pub(crate) counts_exact: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ZstdPeriodicMaintenanceOutcome {
    pub(crate) summaries: Vec<ZstdColumnCompressionSummary>,
    pub(crate) deferred: bool,
    pub(crate) cancelled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CooperativeMaintenanceOutcome {
    deferred: bool,
    cancelled: bool,
    columns: Vec<(ZstdColumnConfig, ColumnMaintenanceProgress)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ColumnMaintenanceProgress {
    observed_rows: u64,
    applied_rows: u64,
    stale_rows: u64,
    source_bytes: u64,
    more_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingPayloadRow {
    rowid: i64,
    id: String,
    payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompressionDictionary {
    id: i64,
    bytes: Option<Vec<u8>>,
}

// Owned by one database's maintenance worker, outside its outer cycle loop.
// There is one active generation per column. Batches are awaited sequentially;
// their slot is moved (not cloned) into the sole in-flight blocking job and
// restored only after joining it. Replacement drops the old CDict before copy:
// at most two native dictionaries exist, including in-flight references.
// Cancellation drains the join before worker shutdown; no blocking job writes DB.
pub(crate) struct PreparedDictionaryCache {
    database_identity: usize,
    entries: [Option<PreparedDictionaryEntry>; 2],
    #[cfg(test)]
    preparations: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    after_cpu: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    after_training: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    pub(crate) after_cycle: Option<Arc<dyn Fn(&Self) + Send + Sync>>,
}

impl PreparedDictionaryCache {
    pub(crate) fn new(store: &CrudStore) -> Self {
        Self {
            database_identity: store.database_connection().runtime_identity(),
            entries: [None, None],
            #[cfg(test)]
            preparations: Arc::default(),
            #[cfg(test)]
            after_cpu: None,
            #[cfg(test)]
            after_training: None,
            #[cfg(test)]
            after_cycle: None,
        }
    }

    fn retained_native_bytes(&self) -> usize {
        self.entries
            .iter()
            .flatten()
            .map(|entry| entry.dictionary.as_cdict().sizeof())
            .sum()
    }
}

#[derive(PartialEq, Eq)]
struct PreparedDictionaryKey {
    database_identity: usize,
    table: &'static str,
    column: &'static str,
    dictionary_id: i64,
    compression_level: i32,
    bytes_digest: [u8; 32],
}

struct PreparedDictionaryEntry {
    key: PreparedDictionaryKey,
    dictionary: Arc<EncoderDictionary<'static>>,
}

#[derive(Debug)]
struct PreparedPayloadRow {
    rowid: i64,
    id: String,
    original_payload: String,
    compressed_payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CompressionBatchOutcome {
    observed_rows: u64,
    applied_rows: u64,
    stale_rows: u64,
    source_bytes: u64,
    more_pending: bool,
}

#[derive(Debug)]
struct DictionaryResolution {
    dictionary: Option<CompressionDictionary>,
    observed_rows: u64,
    source_bytes: u64,
    more_pending: bool,
}

#[derive(Debug, Clone, Copy)]
struct EnsureCompressionResult {
    total_rows: u64,
    was_enabled: bool,
    enabled_now: bool,
    skipped_empty: bool,
    counts_exact: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowInspection {
    Exact,
    Bounded,
}

#[cfg(test)]
pub(crate) async fn run_startup_once(
    crud_store: &CrudStore,
    config: ZstdColumnConfig,
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
) -> Result<ZstdColumnCompressionSummary> {
    let mut cache = PreparedDictionaryCache::new(crud_store);
    run_periodic_maintenance(
        crud_store,
        &mut cache,
        std::slice::from_ref(&config),
        maintenance_seconds,
        target_db_load,
        None,
        RowInspection::Exact,
    )
    .await?
    .summaries
    .into_iter()
    .next()
    .context("zstd maintenance returned no column summary")
}

#[cfg(test)]
pub(crate) async fn run_periodic_maintenance_once(
    crud_store: &CrudStore,
    configs: &[ZstdColumnConfig],
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
) -> Result<ZstdPeriodicMaintenanceOutcome> {
    let mut cache = PreparedDictionaryCache::new(crud_store);
    run_periodic_maintenance(
        crud_store,
        &mut cache,
        configs,
        maintenance_seconds,
        target_db_load,
        None,
        RowInspection::Exact,
    )
    .await
}

pub(crate) async fn run_cooperative_maintenance_cycle(
    crud_store: &CrudStore,
    cache: &mut PreparedDictionaryCache,
    configs: &[ZstdColumnConfig],
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<ZstdPeriodicMaintenanceOutcome> {
    run_periodic_maintenance(
        crud_store,
        cache,
        configs,
        maintenance_seconds,
        target_db_load,
        Some(cancellation),
        RowInspection::Bounded,
    )
    .await
}

/// Installs or verifies transparent-column schema only. Historical payload
/// compression is deliberately excluded from Gateway readiness and is left
/// to the post-startup maintenance worker.
pub(crate) async fn ensure_compression_schema(
    crud_store: &CrudStore,
    configs: &[ZstdColumnConfig],
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<Vec<ZstdColumnCompressionSummary>> {
    let crud_store = crud_store.with_maintenance_access();
    let db = crud_store.database_connection();
    zstd_database_phase(Some(cancellation), verify_sqlite_zstd_registered(&db)).await?;

    let mut summaries = Vec::with_capacity(configs.len());
    for config in configs {
        if cancellation.is_cancelled() {
            break;
        }
        let ensure = zstd_database_quantum(&crud_store, Some(cancellation), || {
            let db = db.clone();
            async move {
                ensure_compression_enabled(&db, *config, RowInspection::Bounded, Some(cancellation))
                    .await
            }
        })
        .await?;
        summaries.push(summary_without_maintenance(*config, ensure));
    }
    Ok(summaries)
}

#[derive(Debug)]
struct ZstdDatabaseCancelled;

impl std::fmt::Display for ZstdDatabaseCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("zstd database phase cancelled")
    }
}

impl std::error::Error for ZstdDatabaseCancelled {}

fn check_database_cancellation(
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<()> {
    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Err(ZstdDatabaseCancelled.into());
    }
    Ok(())
}

// Only DB futures may cross this boundary. Dropping them releases queued
// reservations/read permits and uses the runtime's transaction cleanup. An
// in-progress commit may have an unknown outcome; cancellation never retries it.
// Blocking CPU jobs are deliberately awaited outside this helper.
async fn zstd_database_phase<T>(
    cancellation: Option<&tokio_util::sync::CancellationToken>,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if let Some(cancellation) = cancellation {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ZstdDatabaseCancelled.into()),
            result = operation => result,
        }
    } else {
        operation.await
    }
}

async fn zstd_database_quantum<T, F, Fut>(
    store: &CrudStore,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    // The outer boundary cancels retry backoff too. The inner boundary checks
    // every attempt, so a lock-race retry cannot start DB work after cancellation.
    zstd_database_phase(
        cancellation,
        store.run_background_database_quantum(|| zstd_database_phase(cancellation, operation())),
    )
    .await
}

async fn run_periodic_maintenance(
    crud_store: &CrudStore,
    cache: &mut PreparedDictionaryCache,
    configs: &[ZstdColumnConfig],
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
    row_inspection: RowInspection,
) -> Result<ZstdPeriodicMaintenanceOutcome> {
    let result = crate::database::attribution::scope_database_workload_result(
        pioneer_observability::DatabaseWorkload::ZstdMaintenance,
        run_periodic_maintenance_inner(
            crud_store,
            cache,
            configs,
            maintenance_seconds,
            target_db_load,
            cancellation,
            row_inspection,
        ),
    )
    .await;
    match result {
        Err(error) if error.is::<ZstdDatabaseCancelled>() => Ok(ZstdPeriodicMaintenanceOutcome {
            cancelled: true,
            ..Default::default()
        }),
        result => result,
    }
}

async fn run_periodic_maintenance_inner(
    crud_store: &CrudStore,
    cache: &mut PreparedDictionaryCache,
    configs: &[ZstdColumnConfig],
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
    row_inspection: RowInspection,
) -> Result<ZstdPeriodicMaintenanceOutcome> {
    let crud_store = crud_store.with_maintenance_access();
    let crud_store = &crud_store;
    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(ZstdPeriodicMaintenanceOutcome {
            cancelled: true,
            ..Default::default()
        });
    }
    let db = crud_store.database_connection();
    anyhow::ensure!(
        cache.database_identity == db.runtime_identity(),
        "zstd cache belongs to a different database worker"
    );
    zstd_database_phase(cancellation, verify_sqlite_zstd_registered(&db)).await?;

    let configs = configs.to_vec();
    let before = zstd_database_quantum(crud_store, cancellation, || {
        let db = db.clone();
        let configs = configs.clone();
        async move {
            let mut before = Vec::with_capacity(configs.len());
            for config in configs {
                let ensure =
                    ensure_compression_enabled(&db, config, row_inspection, cancellation).await?;
                let pending_before = match row_inspection {
                    RowInspection::Exact => {
                        zstd_database_phase(cancellation, pending_uncompressed_rows(&db, config))
                            .await?
                    }
                    RowInspection::Bounded => 0,
                };
                before.push((config, ensure, pending_before));
            }
            Ok(before)
        }
    })
    .await?;

    let enabled_configs = before
        .iter()
        .filter_map(|(config, ensure, _)| {
            (ensure.was_enabled || ensure.enabled_now).then_some(*config)
        })
        .collect::<Vec<_>>();
    let maintenance = if !enabled_configs.is_empty() {
        run_cooperative_maintenance(
            crud_store,
            cache,
            &db,
            enabled_configs.as_slice(),
            maintenance_seconds,
            target_db_load,
            cancellation,
        )
        .await?
    } else {
        CooperativeMaintenanceOutcome::default()
    };

    if maintenance.cancelled {
        return Ok(ZstdPeriodicMaintenanceOutcome {
            summaries: Vec::new(),
            deferred: false,
            cancelled: true,
        });
    }

    let mut summaries = Vec::with_capacity(before.len());
    for (config, ensure, exact_pending_before) in before {
        let progress = maintenance
            .columns
            .iter()
            .find_map(|(candidate, progress)| (*candidate == config).then_some(*progress))
            .unwrap_or_default();
        let pending_before = match row_inspection {
            RowInspection::Exact => exact_pending_before,
            RowInspection::Bounded => progress.observed_rows,
        };
        let pending_after = match row_inspection {
            RowInspection::Exact => {
                zstd_database_phase(cancellation, pending_uncompressed_rows(&db, config)).await?
            }
            RowInspection::Bounded => progress.observed_rows.saturating_sub(progress.applied_rows),
        };
        summaries.push(ZstdColumnCompressionSummary {
            table: config.table,
            column: config.column,
            enabled_now: ensure.enabled_now,
            already_enabled: ensure.was_enabled,
            skipped_empty: ensure.skipped_empty,
            total_rows: ensure.total_rows,
            pending_before,
            pending_after,
            compressed_rows: progress.applied_rows,
            stale_rows: progress.stale_rows,
            source_bytes: progress.source_bytes,
            maintenance_more_pending: progress.more_pending,
            counts_exact: ensure.counts_exact,
        });
    }

    Ok(ZstdPeriodicMaintenanceOutcome {
        summaries,
        deferred: maintenance.deferred,
        cancelled: false,
    })
}

async fn run_cooperative_maintenance(
    crud_store: &CrudStore,
    cache: &mut PreparedDictionaryCache,
    db: &SqliteDatabase,
    configs: &[ZstdColumnConfig],
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<CooperativeMaintenanceOutcome> {
    if !target_db_load.is_finite() || !(0.0 < target_db_load && target_db_load <= 1.0) {
        anyhow::bail!("zstd maintenance target DB load must be in (0, 1]");
    }

    let deadline = maintenance_seconds
        .map(|seconds| Instant::now() + Duration::from_secs_f64(seconds.max(0.0)));
    let mut columns = configs
        .iter()
        .copied()
        .map(|config| (config, ColumnMaintenanceProgress::default()))
        .collect::<Vec<_>>();
    let mut active = vec![true; configs.len()];

    loop {
        let mut attempted = false;
        let mut made_progress = false;
        for (index, config) in configs.iter().copied().enumerate() {
            if !active[index] {
                continue;
            }
            if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
                return Ok(CooperativeMaintenanceOutcome {
                    deferred: false,
                    cancelled: true,
                    columns,
                });
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(CooperativeMaintenanceOutcome {
                    deferred: false,
                    cancelled: false,
                    columns,
                });
            }

            attempted = true;
            let started_at = Instant::now();
            let batch =
                run_one_compression_batch(crud_store, cache, db, config, cancellation).await?;
            let progress = &mut columns[index].1;
            progress.observed_rows = progress.observed_rows.saturating_add(batch.observed_rows);
            progress.applied_rows = progress.applied_rows.saturating_add(batch.applied_rows);
            progress.stale_rows = progress.stale_rows.saturating_add(batch.stale_rows);
            progress.source_bytes = progress.source_bytes.saturating_add(batch.source_bytes);
            progress.more_pending = batch.more_pending;
            // A batch that could not make progress (for example because a
            // dictionary does not yet have enough bounded training input, or
            // every CAS became stale) remains pending but is not retried in a
            // tight loop during the same maintenance cycle.
            active[index] = batch.more_pending && batch.applied_rows != 0;
            made_progress |= batch.applied_rows != 0;

            if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
                return Ok(CooperativeMaintenanceOutcome {
                    deferred: false,
                    cancelled: true,
                    columns,
                });
            }
            if batch.more_pending {
                pause_after_batch(started_at.elapsed(), target_db_load, deadline, cancellation)
                    .await?;
            }
        }

        if !active.iter().any(|active| *active) || !attempted || !made_progress {
            return Ok(CooperativeMaintenanceOutcome {
                deferred: false,
                cancelled: false,
                columns,
            });
        }
    }
}

async fn run_one_compression_batch(
    crud_store: &CrudStore,
    cache: &mut PreparedDictionaryCache,
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<CompressionBatchOutcome> {
    let dictionary = resolve_compression_dictionary(
        crud_store,
        db,
        config,
        cancellation,
        #[cfg(test)]
        cache.after_training.clone(),
    )
    .await?;
    let Some(dictionary) = dictionary.dictionary else {
        return Ok(CompressionBatchOutcome {
            observed_rows: dictionary.observed_rows,
            source_bytes: dictionary.source_bytes,
            more_pending: dictionary.more_pending,
            ..Default::default()
        });
    };
    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(CompressionBatchOutcome {
            more_pending: true,
            ..Default::default()
        });
    }

    let rows = zstd_database_quantum(crud_store, cancellation, || {
        let db = db.clone();
        async move {
            load_pending_payload_rows(
                &db,
                config,
                COMPRESSION_BATCH_MAX_ROWS,
                COMPRESSION_BATCH_MAX_SOURCE_BYTES,
            )
            .await
        }
    })
    .await?;
    if rows.is_empty() {
        return Ok(CompressionBatchOutcome::default());
    }
    let observed_rows = rows.len() as u64;
    let source_bytes = rows.iter().map(|row| row.payload.len() as u64).sum::<u64>();
    let dictionary_id = dictionary.id;
    let Some(prepared) =
        prepare_payload_batch(cache, db, config, rows, dictionary, cancellation).await?
    else {
        return Ok(CompressionBatchOutcome {
            observed_rows,
            source_bytes,
            more_pending: true,
            ..Default::default()
        });
    };

    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(CompressionBatchOutcome {
            observed_rows,
            source_bytes,
            more_pending: true,
            ..Default::default()
        });
    }
    let (applied_rows, stale_rows) = zstd_database_quantum(crud_store, cancellation, || {
        let db = db.clone();
        let prepared = &prepared;
        async move {
            apply_prepared_payload_rows(&db, config, dictionary_id, prepared, cancellation).await
        }
    })
    .await?;
    let more_pending = zstd_database_quantum(crud_store, cancellation, || {
        let db = db.clone();
        async move { has_pending_uncompressed_rows(&db, config).await }
    })
    .await?;

    Ok(CompressionBatchOutcome {
        observed_rows,
        applied_rows,
        stale_rows,
        source_bytes,
        more_pending,
    })
}

async fn resolve_compression_dictionary(
    crud_store: &CrudStore,
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
    #[cfg(test)] after_training: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Result<DictionaryResolution> {
    if let Some(dictionary) = zstd_database_quantum(crud_store, cancellation, || {
        let db = db.clone();
        async move { load_compression_dictionary(&db, config).await }
    })
    .await?
    {
        return Ok(DictionaryResolution {
            dictionary: Some(dictionary),
            observed_rows: 0,
            source_bytes: 0,
            more_pending: true,
        });
    }

    let sample = zstd_database_quantum(crud_store, cancellation, || {
        let db = db.clone();
        async move {
            load_pending_payload_rows(
                &db,
                config,
                DICTIONARY_SAMPLE_MAX_ROWS,
                DICTIONARY_SAMPLE_MAX_SOURCE_BYTES,
            )
            .await
        }
    })
    .await?;
    if sample.is_empty() {
        return Ok(DictionaryResolution {
            dictionary: None,
            observed_rows: 0,
            source_bytes: 0,
            more_pending: false,
        });
    }
    let sample_rows = sample.len();
    let sample_bytes = sample.iter().map(|row| row.payload.len()).sum::<usize>();
    if sample_rows < DICTIONARY_MIN_SAMPLE_ROWS {
        return Ok(DictionaryResolution {
            dictionary: Some(CompressionDictionary {
                id: -1,
                bytes: None,
            }),
            observed_rows: sample_rows as u64,
            source_bytes: sample_bytes as u64,
            more_pending: true,
        });
    }
    if sample_bytes < DICTIONARY_MIN_SOURCE_BYTES && sample_rows < DICTIONARY_SAMPLE_MAX_ROWS {
        return Ok(DictionaryResolution {
            dictionary: None,
            observed_rows: sample_rows as u64,
            source_bytes: sample_bytes as u64,
            more_pending: true,
        });
    }

    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(DictionaryResolution {
            dictionary: None,
            observed_rows: sample_rows as u64,
            source_bytes: sample_bytes as u64,
            more_pending: true,
        });
    }
    let wanted_size = (sample_bytes / 100).clamp(DICTIONARY_MIN_BYTES, DICTIONARY_MAX_BYTES);
    let samples = sample
        .into_iter()
        .map(|row| row.payload.into_bytes())
        .collect::<Vec<_>>();
    let trained = tokio::task::spawn_blocking(move || {
        let result = pioneer_sqlite::zstd::train_dictionary(samples.as_slice(), wanted_size);
        #[cfg(test)]
        if let Some(after_training) = after_training {
            after_training();
        }
        result
    })
    .await
    .context("zstd dictionary training worker failed to join")?;
    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(DictionaryResolution {
            dictionary: None,
            observed_rows: sample_rows as u64,
            source_bytes: sample_bytes as u64,
            more_pending: true,
        });
    }
    let candidate = match trained {
        Ok(candidate) => candidate,
        Err(_) => {
            tracing::warn!(
                table = config.table,
                column = config.column,
                "bounded zstd dictionary training failed; compressing this batch without a dictionary"
            );
            return Ok(DictionaryResolution {
                dictionary: Some(CompressionDictionary {
                    id: -1,
                    bytes: None,
                }),
                observed_rows: sample_rows as u64,
                source_bytes: sample_bytes as u64,
                more_pending: true,
            });
        }
    };
    let dictionary = zstd_database_quantum(crud_store, cancellation, || {
        let db = db.clone();
        let candidate = candidate.clone();
        async move { persist_compression_dictionary(&db, config, candidate, cancellation).await }
    })
    .await?;
    Ok(DictionaryResolution {
        dictionary: Some(dictionary),
        observed_rows: sample_rows as u64,
        source_bytes: sample_bytes as u64,
        more_pending: true,
    })
}

// No DB capacity is retained here. Await the started job even on cancellation:
// native compression cannot be interrupted, and returning early would detach it.
async fn prepare_payload_batch(
    cache: &mut PreparedDictionaryCache,
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    rows: Vec<PendingPayloadRow>,
    dictionary: CompressionDictionary,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<Option<Vec<PreparedPayloadRow>>> {
    anyhow::ensure!(
        cache.database_identity == db.runtime_identity(),
        "zstd cache belongs to a different database worker"
    );
    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(None);
    }
    let index = match config.count_source {
        ZstdColumnCountSource::TurnEvent => 0,
        ZstdColumnCountSource::TurnItem => 1,
    };
    let mut entry = cache.entries[index].take();
    let database_identity = cache.database_identity;
    let cancellation = cancellation.cloned();
    #[cfg(test)]
    let preparations = cache.preparations.clone();
    #[cfg(test)]
    let after_cpu = cache.after_cpu.clone();
    let (entry, result) = tokio::task::spawn_blocking(move || {
        let result = (|| {
            if cancellation
                .as_ref()
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
            {
                return Ok(None);
            }
            match dictionary
                .bytes
                .as_deref()
                .filter(|bytes| !bytes.is_empty())
            {
                Some(bytes) => {
                    let key = PreparedDictionaryKey {
                        database_identity,
                        table: config.table,
                        column: config.column,
                        dictionary_id: dictionary.id,
                        compression_level: config.compression_level,
                        bytes_digest: Sha256::digest(bytes).into(),
                    };
                    if entry.as_ref().is_none_or(|entry| entry.key != key) {
                        // Release the previous generation before allocating its replacement.
                        drop(entry.take());
                        let prepared =
                            Arc::new(EncoderDictionary::copy(bytes, config.compression_level));
                        #[cfg(test)]
                        preparations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tracing::debug!(
                            native_bytes = prepared.as_cdict().sizeof(),
                            "zstd prepared dictionary cache miss"
                        );
                        entry = Some(PreparedDictionaryEntry {
                            key,
                            dictionary: prepared,
                        });
                    }
                }
                None => entry = None,
            }
            // The Arc owner and this explicit borrow both live through the entire
            // batch; the compressor is dropped before the entry leaves this job.
            let prepared: Option<&EncoderDictionary<'static>> =
                entry.as_ref().map(|entry| entry.dictionary.as_ref());
            let rows = prepare_payload_rows(rows, config.compression_level, prepared)?;
            #[cfg(test)]
            if let Some(after_cpu) = after_cpu {
                after_cpu();
            }
            Ok(Some(rows))
        })();
        (entry, result)
    })
    .await
    .context("zstd payload compression worker failed to join")?;
    cache.entries[index] = entry;
    tracing::debug!(
        retained_native_bytes = cache.retained_native_bytes(),
        "zstd prepared dictionary cache retained"
    );
    result
}

fn prepare_payload_rows(
    rows: Vec<PendingPayloadRow>,
    compression_level: i32,
    dictionary: Option<&EncoderDictionary<'static>>,
) -> Result<Vec<PreparedPayloadRow>> {
    let mut compressor = match dictionary {
        Some(dictionary) => {
            pioneer_sqlite::zstd::ColumnValueCompressor::with_prepared_dictionary(dictionary)
        }
        None => pioneer_sqlite::zstd::ColumnValueCompressor::new(compression_level, None),
    }
    .context("failed to prepare bounded zstd payload batch compressor")?;
    rows.into_iter()
        .map(|row| {
            let compressed_payload = compressor
                .compress(row.payload.as_bytes())
                .context("failed to compress bounded zstd payload row")?;
            Ok(PreparedPayloadRow {
                rowid: row.rowid,
                id: row.id,
                original_payload: row.payload,
                compressed_payload,
            })
        })
        .collect()
}

async fn load_pending_payload_rows(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    max_rows: usize,
    max_source_bytes: usize,
) -> Result<Vec<PendingPayloadRow>> {
    #[derive(Debug)]
    struct Candidate {
        rowid: i64,
        payload_bytes: usize,
    }

    let candidate_rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                "SELECT rowid AS zstd_rowid, length(CAST({column} AS BLOB)) AS payload_bytes \
                 FROM {table} WHERE {dict_column} IS NULL \
                 ORDER BY rowid LIMIT ?",
                column = config.column,
                table = config.backing_table,
                dict_column = config.dict_column,
            ),
            [(max_rows.saturating_add(1) as i64).into()],
        ))
        .await
        .with_context(|| {
            format!(
                "failed to inspect bounded zstd batch for {}",
                config.label()
            )
        })?;

    let mut candidates = Vec::with_capacity(max_rows.min(candidate_rows.len()));
    let mut admitted_bytes = 0usize;
    for row in candidate_rows.into_iter().take(max_rows) {
        let payload_bytes = row
            .try_get::<i64>("", "payload_bytes")
            .context("failed to decode bounded zstd payload length")?
            .max(0) as usize;
        if !candidates.is_empty() && admitted_bytes.saturating_add(payload_bytes) > max_source_bytes
        {
            break;
        }
        admitted_bytes = admitted_bytes.saturating_add(payload_bytes);
        candidates.push(Candidate {
            rowid: row
                .try_get::<i64>("", "zstd_rowid")
                .context("failed to decode bounded zstd rowid")?,
            payload_bytes,
        });
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let selected_values = std::iter::repeat_n("(?, ?)", candidates.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = Vec::with_capacity(candidates.len() * 2);
    for candidate in &candidates {
        values.push(candidate.rowid.into());
        values.push((candidate.payload_bytes as i64).into());
    }
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            format!(
                "WITH selected(zstd_rowid, payload_bytes) AS (VALUES {selected_values}) \
                 SELECT target.rowid AS zstd_rowid, target.id AS id, \
                        target.{column} AS payload \
                 FROM {table} AS target \
                 JOIN selected ON selected.zstd_rowid = target.rowid \
                              AND selected.payload_bytes = length(CAST(target.{column} AS BLOB)) \
                 WHERE target.{dict_column} IS NULL ORDER BY target.rowid",
                column = config.column,
                table = config.backing_table,
                dict_column = config.dict_column,
            ),
            values,
        ))
        .await
        .with_context(|| format!("failed to load bounded zstd batch for {}", config.label()))?;
    rows.into_iter()
        .map(|row| {
            Ok(PendingPayloadRow {
                rowid: row
                    .try_get::<i64>("", "zstd_rowid")
                    .context("failed to decode bounded zstd rowid")?,
                id: row
                    .try_get::<String>("", "id")
                    .context("failed to decode bounded zstd row id")?,
                payload: row
                    .try_get::<String>("", "payload")
                    .context("failed to decode bounded zstd payload")?,
            })
        })
        .collect()
}

async fn load_compression_dictionary(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
) -> Result<Option<CompressionDictionary>> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT id, dict FROM _zstd_dicts WHERE chooser_key = ? LIMIT 1",
            [config.dictionary_key.into()],
        ))
        .await
        .with_context(|| format!("failed to load zstd dictionary for {}", config.label()))?;
    row.map(|row| {
        Ok(CompressionDictionary {
            id: row
                .try_get::<i64>("", "id")
                .context("failed to decode zstd dictionary id")?,
            bytes: Some(
                row.try_get::<Vec<u8>>("", "dict")
                    .context("failed to decode zstd dictionary")?,
            ),
        })
    })
    .transpose()
}

async fn persist_compression_dictionary(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    candidate: Vec<u8>,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<CompressionDictionary> {
    check_database_cancellation(cancellation)?;
    let transaction = db.begin().await.with_context(|| {
        format!(
            "failed to begin zstd dictionary commit for {}",
            config.label()
        )
    })?;
    check_database_cancellation(cancellation)?;
    transaction
        .execute_raw(Statement::from_sql_and_values(
            transaction.get_database_backend(),
            "INSERT INTO _zstd_dicts (chooser_key, dict) VALUES (?, ?) \
             ON CONFLICT(chooser_key) DO NOTHING",
            [config.dictionary_key.into(), candidate.into()],
        ))
        .await
        .with_context(|| format!("failed to commit zstd dictionary for {}", config.label()))?;
    check_database_cancellation(cancellation)?;
    let row = transaction
        .query_one_raw(Statement::from_sql_and_values(
            transaction.get_database_backend(),
            "SELECT id, dict FROM _zstd_dicts WHERE chooser_key = ? LIMIT 1",
            [config.dictionary_key.into()],
        ))
        .await
        .with_context(|| {
            format!(
                "failed to revalidate zstd dictionary for {}",
                config.label()
            )
        })?
        .context("persisted zstd dictionary was not found")?;
    let dictionary = CompressionDictionary {
        id: row
            .try_get::<i64>("", "id")
            .context("failed to decode persisted zstd dictionary id")?,
        bytes: Some(
            row.try_get::<Vec<u8>>("", "dict")
                .context("failed to decode persisted zstd dictionary")?,
        ),
    };
    check_database_cancellation(cancellation)?;
    transaction.commit().await.with_context(|| {
        format!(
            "failed to finish zstd dictionary commit for {}",
            config.label()
        )
    })?;
    Ok(dictionary)
}

async fn apply_prepared_payload_rows(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    dictionary_id: i64,
    rows: &[PreparedPayloadRow],
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<(u64, u64)> {
    check_database_cancellation(cancellation)?;
    let transaction = db
        .begin()
        .await
        .with_context(|| format!("failed to begin bounded zstd commit for {}", config.label()))?;
    check_database_cancellation(cancellation)?;
    let mut applied = 0u64;
    let mut stale = 0u64;
    for row in rows {
        check_database_cancellation(cancellation)?;
        let result = transaction
            .execute_raw(Statement::from_sql_and_values(
                transaction.get_database_backend(),
                format!(
                    "UPDATE {table} SET {column} = ?, {dict_column} = ? \
                     WHERE rowid = ? AND id = ? AND {dict_column} IS NULL \
                       AND {column} = ?",
                    table = config.backing_table,
                    column = config.column,
                    dict_column = config.dict_column,
                ),
                [
                    row.compressed_payload.clone().into(),
                    dictionary_id.into(),
                    row.rowid.into(),
                    row.id.clone().into(),
                    row.original_payload.clone().into(),
                ],
            ))
            .await
            .with_context(|| format!("failed to apply bounded zstd row for {}", config.label()))?;
        match result.rows_affected() {
            0 => stale = stale.saturating_add(1),
            1 => applied = applied.saturating_add(1),
            affected => {
                anyhow::bail!("bounded zstd CAS affected {affected} rows for a single primary key")
            }
        }
    }
    check_database_cancellation(cancellation)?;
    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit bounded zstd batch for {}", config.label()))?;
    Ok((applied, stale))
}

/// Exercise the real bounded compression prepare/CAS path between history
/// capture and replay, including tiny fixtures below dictionary-training size.
#[cfg(test)]
pub(crate) async fn compress_history_payloads_for_test(store: &CrudStore) -> Result<u64> {
    let db = store.with_maintenance_access().database_connection();
    let mut total = 0;
    for &config in ZSTD_PAYLOAD_COLUMNS {
        if !compression_is_enabled(&db, config, None).await? {
            enable_transparent_compression(&db, config).await?;
        }
        for _ in 0..32 {
            let rows = load_pending_payload_rows(
                &db,
                config,
                COMPRESSION_BATCH_MAX_ROWS,
                COMPRESSION_BATCH_MAX_SOURCE_BYTES,
            )
            .await?;
            if rows.is_empty() {
                break;
            }
            let prepared = prepare_payload_rows(rows, 3, None)?;
            let (applied, _) =
                apply_prepared_payload_rows(&db, config, -1, &prepared, None).await?;
            total += applied;
        }
        anyhow::ensure!(
            !has_pending_uncompressed_rows(&db, config).await?,
            "test compression exceeded its bounded fixture size"
        );
    }
    Ok(total)
}

async fn has_pending_uncompressed_rows(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
) -> Result<bool> {
    query_i64(
        db,
        format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE {} IS NULL LIMIT 1) AS value",
            config.backing_table, config.dict_column
        )
        .as_str(),
        "failed to inspect pending sqlite-zstd rows",
    )
    .await
    .map(|value| value != 0)
}

async fn pause_after_batch(
    elapsed: Duration,
    target_db_load: f64,
    deadline: Option<Instant>,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<()> {
    let desired_total = elapsed.div_f64(target_db_load);
    let mut pause = desired_total.saturating_sub(elapsed);
    if let Some(deadline) = deadline {
        pause = pause.min(deadline.saturating_duration_since(Instant::now()));
    }
    if pause.is_zero() {
        tokio::task::yield_now().await;
        return Ok(());
    }
    if let Some(cancellation) = cancellation {
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = tokio::time::sleep(pause) => {}
        }
    } else {
        tokio::time::sleep(pause).await;
    }
    Ok(())
}

async fn ensure_compression_enabled(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    row_inspection: RowInspection,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<EnsureCompressionResult> {
    let was_enabled = zstd_database_phase(
        cancellation,
        compression_is_enabled(db, config, cancellation),
    )
    .await?;
    let (total_rows, has_rows) = match row_inspection {
        RowInspection::Exact => {
            let total_rows = zstd_database_phase(cancellation, row_count(db, config)).await?;
            (total_rows, total_rows != 0)
        }
        RowInspection::Bounded if was_enabled => (0, true),
        RowInspection::Bounded => (
            0,
            zstd_database_phase(cancellation, table_has_rows(db, config)).await?,
        ),
    };
    let mut enabled_now = false;
    let mut skipped_empty = false;

    if !was_enabled {
        if !has_rows {
            skipped_empty = true;
        } else {
            zstd_database_phase(cancellation, mark_compression_backfilling(db, config)).await?;
            if let Err(error) = zstd_database_phase(
                cancellation,
                enable_transparent_compression(&db.maintenance(), config),
            )
            .await
            {
                check_database_cancellation(cancellation)?;
                zstd_database_phase(cancellation, mark_compression_failed(db, config, &error))
                    .await?;
                return Err(error);
            }
            zstd_database_phase(
                cancellation,
                mark_compression_complete(db, config, total_rows),
            )
            .await?;
            enabled_now = true;
        }
    } else {
        zstd_database_phase(
            cancellation,
            mark_existing_compression_complete(db, config, total_rows, cancellation),
        )
        .await?;
    }

    Ok(EnsureCompressionResult {
        total_rows,
        was_enabled,
        enabled_now,
        skipped_empty,
        counts_exact: row_inspection == RowInspection::Exact,
    })
}

fn summary_without_maintenance(
    config: ZstdColumnConfig,
    ensure: EnsureCompressionResult,
) -> ZstdColumnCompressionSummary {
    ZstdColumnCompressionSummary {
        table: config.table,
        column: config.column,
        enabled_now: ensure.enabled_now,
        already_enabled: ensure.was_enabled,
        skipped_empty: ensure.skipped_empty,
        total_rows: ensure.total_rows,
        pending_before: 0,
        pending_after: 0,
        compressed_rows: 0,
        stale_rows: 0,
        source_bytes: 0,
        maintenance_more_pending: false,
        counts_exact: ensure.counts_exact,
    }
}

async fn verify_sqlite_zstd_registered(db: &SqliteDatabase) -> Result<()> {
    query_i64(
        db,
        "SELECT length(zstd_compress('pioneer-sqlite', 1)) AS value",
        "failed to verify sqlite-zstd functions are registered",
    )
    .await
    .map(|_| ())
}

async fn compression_is_enabled(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<bool> {
    let backing_table = zstd_database_phase(
        cancellation,
        query_i64(
            db,
            format!(
                "SELECT COUNT(*) AS value \
         FROM sqlite_master \
         WHERE type = 'table' AND name = '{}'",
                config.backing_table
            )
            .as_str(),
            "failed to detect sqlite-zstd backing table",
        ),
    )
    .await?;
    let compressed_view = zstd_database_phase(
        cancellation,
        query_i64(
            db,
            format!(
                "SELECT COUNT(*) AS value \
         FROM sqlite_master \
         WHERE type = 'view' AND name = '{}'",
                config.table
            )
            .as_str(),
            "failed to detect sqlite-zstd view",
        ),
    )
    .await?;

    Ok(backing_table > 0 && compressed_view > 0)
}

async fn enable_transparent_compression(
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
) -> Result<()> {
    let sqlite_zstd_config = json!({
        "table": config.table,
        "column": config.column,
        "compression_level": config.compression_level,
        "dict_chooser": config.dict_chooser
    });
    db.query_one_write_raw(Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT zstd_enable_transparent(?) AS value",
        [sqlite_zstd_config.to_string().into()],
    ))
    .await
    .with_context(|| {
        format!(
            "failed to enable sqlite-zstd transparent compression for {}",
            config.label()
        )
    })?;
    Ok(())
}

async fn pending_uncompressed_rows(db: &SqliteDatabase, config: ZstdColumnConfig) -> Result<u64> {
    if !compression_is_enabled(db, config, None).await? {
        return Ok(0);
    }
    query_i64(
        db,
        format!(
            "SELECT COUNT(*) AS value FROM {} WHERE {} IS NULL",
            config.backing_table, config.dict_column
        )
        .as_str(),
        "failed to count pending sqlite-zstd rows",
    )
    .await
    .map(|value| value.max(0) as u64)
}

async fn row_count(db: &SqliteDatabase, config: ZstdColumnConfig) -> Result<u64> {
    query_i64(
        db,
        format!("SELECT COUNT(*) AS value FROM {}", config.table).as_str(),
        "failed to count zstd target rows",
    )
    .await
    .map(|value| value.max(0) as u64)
}

async fn table_has_rows(db: &SqliteDatabase, config: ZstdColumnConfig) -> Result<bool> {
    query_i64(
        db,
        format!(
            "SELECT EXISTS(SELECT 1 FROM {} LIMIT 1) AS value",
            config.table
        )
        .as_str(),
        "failed to inspect zstd target rows",
    )
    .await
    .map(|value| value != 0)
}

async fn query_i64<C>(db: &C, sql: &str, error_context: &'static str) -> Result<i64>
where
    C: ConnectionTrait,
{
    let row = db
        .query_one_raw(Statement::from_string(
            db.get_database_backend(),
            sql.to_owned(),
        ))
        .await
        .context(error_context)?
        .context("query unexpectedly returned no rows")?;
    row.try_get::<i64>("", "value")
        .with_context(|| format!("{error_context}: failed to decode value"))
}

async fn mark_compression_backfilling<C>(db: &C, config: ZstdColumnConfig) -> Result<()>
where
    C: ConnectionTrait,
{
    let now = now_datetime();
    upsert_projection_meta(
        db,
        projection_meta_record(
            config,
            PROJECTION_META_STATUS_BACKFILLING,
            0,
            None,
            Some(now),
            None,
            now,
        ),
    )
    .await
}

async fn mark_compression_complete<C>(
    db: &C,
    config: ZstdColumnConfig,
    total_rows: u64,
) -> Result<()>
where
    C: ConnectionTrait,
{
    let now = now_datetime();
    upsert_projection_meta(
        db,
        projection_meta_record(
            config,
            PROJECTION_META_STATUS_COMPLETE,
            total_rows,
            None,
            Some(now),
            Some(now),
            now,
        ),
    )
    .await
}

async fn mark_existing_compression_complete<C: ConnectionTrait>(
    db: &C,
    config: ZstdColumnConfig,
    total_rows: u64,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<()> {
    let Some(meta) = zstd_database_phase(
        cancellation,
        find_projection_meta(db, config.projection_key),
    )
    .await?
    else {
        return zstd_database_phase(
            cancellation,
            mark_compression_complete(db, config, total_rows),
        )
        .await;
    };

    if meta.projection_version == config.projection_version
        && meta.status == PROJECTION_META_STATUS_COMPLETE
    {
        return Ok(());
    }

    zstd_database_phase(
        cancellation,
        mark_compression_complete(db, config, total_rows),
    )
    .await
}

async fn mark_compression_failed<C: ConnectionTrait>(
    db: &C,
    config: ZstdColumnConfig,
    error: &anyhow::Error,
) -> Result<()> {
    let now = now_datetime();
    upsert_projection_meta(
        db,
        projection_meta_record(
            config,
            PROJECTION_META_STATUS_FAILED,
            0,
            Some(format!("{error:#}")),
            None,
            None,
            now,
        ),
    )
    .await
}

fn projection_meta_record(
    config: ZstdColumnConfig,
    status: &str,
    total_rows: u64,
    last_error: Option<String>,
    backfill_started_at: Option<DateTimeWithTimeZone>,
    backfilled_at: Option<DateTimeWithTimeZone>,
    now: DateTimeWithTimeZone,
) -> ProjectionMetaRecord {
    let mut source_turn_item_count = 0;
    let mut source_turn_event_count = 0;
    match config.count_source {
        ZstdColumnCountSource::TurnEvent => source_turn_event_count = total_rows as i64,
        ZstdColumnCountSource::TurnItem => source_turn_item_count = total_rows as i64,
    }
    ProjectionMetaRecord {
        projection_key: config.projection_key.to_owned(),
        projection_version: config.projection_version,
        status: status.to_owned(),
        source_thread_count: 0,
        source_turn_count: 0,
        source_turn_item_count,
        source_turn_event_count,
        last_error,
        backfill_started_at,
        backfilled_at,
        created_at: now,
        updated_at: now,
    }
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::{
        COMPRESSION_BATCH_MAX_ROWS, COMPRESSION_BATCH_MAX_SOURCE_BYTES, DICTIONARY_SAMPLE_MAX_ROWS,
        DICTIONARY_SAMPLE_MAX_SOURCE_BYTES, PERIODIC_MAINTENANCE_SLICE_SECONDS, TURN_EVENT_PAYLOAD,
        TURN_ITEM_PAYLOAD, ZSTD_PAYLOAD_COLUMNS, apply_prepared_payload_rows,
        ensure_compression_schema, load_compression_dictionary, load_pending_payload_rows,
        prepare_payload_rows, run_periodic_maintenance_once, run_startup_once,
    };
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, find_projection_meta};
    use pioneer_protocol::{AgentMessagePhase, TurnItem};
    use sea_orm::{
        ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement, TransactionTrait,
    };
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[derive(Default)]
    struct BudgetSchedulingObserver {
        reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>,
        writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
        writer_queued: Notify,
        reader_queued: Notify,
        cancel_on_writer_admission: std::sync::Mutex<Option<tokio_util::sync::CancellationToken>>,
    }

    impl pioneer_sqlite::SqliteReadObserver for BudgetSchedulingObserver {
        fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
            self.reads.lock().unwrap().push(event);
            if matches!(event, pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued { queue_depth, active, .. } if queue_depth != 0 && active != 0)
            {
                self.reader_queued.notify_one();
            }
        }
    }

    impl pioneer_sqlite::SqliteWriteObserver for BudgetSchedulingObserver {
        fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
            self.writes.lock().unwrap().push(event);
            match event {
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    queue,
                } if queue.maintenance != 0 => {
                    self.writer_queued.notify_one();
                }
                pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                } => {
                    if let Some(token) = self.cancel_on_writer_admission.lock().unwrap().take() {
                        token.cancel();
                    }
                }
                _ => {}
            }
        }
    }

    impl BudgetSchedulingObserver {
        async fn wait_for_writer_queue(&self) {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if self.writes.lock().unwrap().iter().any(|event| {
                        matches!(event,
                            pioneer_sqlite::SqliteWriteEvent::Enqueued {
                                class: pioneer_sqlite::SqliteWriteClass::Maintenance, queue,
                            } if queue.maintenance == 1
                        )
                    }) {
                        return;
                    }
                    self.writer_queued.notified().await;
                }
            })
            .await
            .unwrap();
        }

        fn assert_writer_queue_cancelled(&self) {
            let events = self.writes.lock().unwrap();
            assert!(
                events.iter().any(|event| matches!(event,
                    pioneer_sqlite::SqliteWriteEvent::Cancelled {
                        class: pioneer_sqlite::SqliteWriteClass::Maintenance, queue, ..
                    } if queue.maintenance == 0
                )),
                "RAII must remove the queued reservation while the writer is still held"
            );
            assert!(!events.iter().any(|event| matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                }
            )));
        }
    }

    struct BudgetFixture {
        _directory: tempfile::TempDir,
        store: CrudStore,
        observer: Arc<BudgetSchedulingObserver>,
        payloads: Vec<Vec<String>>,
    }

    async fn budget_fixture(texts: &[&str]) -> BudgetFixture {
        use pioneer_sqlite::{SqliteDatabase, SqliteWriteExecutor};
        use sea_orm::ConnectOptions;

        pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("zstd-byte-budget.sqlite");
        let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
        options.max_connections(1).sqlx_logging(false);
        let writer = Database::connect(options).await.unwrap();
        Migrator::up(&writer, None).await.unwrap();
        writer
            .execute_unprepared("PRAGMA journal_mode=WAL")
            .await
            .unwrap();
        insert_turn_events(&writer, texts.len() as i64).await;
        insert_turn_items(&writer, texts.len() as i64).await;
        let mut options =
            ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options
            .max_connections(2)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
        let reader = Database::connect(options).await.unwrap();
        let observer = Arc::new(BudgetSchedulingObserver::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        assert!(database.reader_query_only_enabled().await.unwrap());
        let store = CrudStore::new(database.clone());
        let mut payloads = Vec::new();
        for &config in ZSTD_PAYLOAD_COLUMNS {
            let mut expected = Vec::new();
            for (index, &text) in texts.iter().enumerate() {
                let (id, payload) =
                    if config == TURN_EVENT_PAYLOAD {
                        (format!("event_{index}"), serde_json::json!({
                        "kind": "test_event", "payload": {"sequence": index, "content": text}
                    }).to_string())
                    } else {
                        (
                            format!("turn_item_zstd_item_{index}"),
                            serde_json::to_string(&TurnItem::AgentMessage {
                                id: format!("item_{index}"),
                                text: text.to_owned(),
                                phase: AgentMessagePhase::FinalAnswer,
                                markdown: None,
                                markdown_version: None,
                            })
                            .unwrap(),
                        )
                    };
                database
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        format!("UPDATE {} SET payload=? WHERE id=?", config.table),
                        [payload.clone().into(), id.into()],
                    ))
                    .await
                    .unwrap();
                expected.push(payload);
            }
            payloads.push(expected);
        }
        ensure_compression_schema(
            &store,
            ZSTD_PAYLOAD_COLUMNS,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        BudgetFixture {
            _directory: directory,
            store,
            observer,
            payloads,
        }
    }

    // Observe actual CDict copies in the blocking miss path, rather than
    // counting calls to cache/compressor constructors.
    async fn prepare_cached_fixture(
        cache: &mut super::PreparedDictionaryCache,
        db: &pioneer_sqlite::SqliteDatabase,
        config: super::ZstdColumnConfig,
        id: i64,
        bytes: Option<&[u8]>,
    ) -> Vec<super::PreparedPayloadRow> {
        super::prepare_payload_batch(
            cache,
            db,
            config,
            vec![super::PendingPayloadRow {
                rowid: 1,
                id: "fixture".into(),
                payload: "shared payload dictionary content".repeat(32),
            }],
            super::CompressionDictionary {
                id,
                bytes: bytes.map(<[u8]>::to_vec),
            },
            None,
        )
        .await
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn prepared_dictionaries_reuse_across_batches_and_outer_cycles() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };
        let fixture = budget_fixture(&vec!["shared payload dictionary content"; 65]).await;
        let db = fixture
            .store
            .with_maintenance_access()
            .database_connection();
        let bytes = b"shared payload dictionary content";
        for &config in ZSTD_PAYLOAD_COLUMNS {
            super::persist_compression_dictionary(&db, config, bytes.to_vec(), None)
                .await
                .unwrap();
        }
        let mut cache = super::PreparedDictionaryCache::new(&fixture.store);
        let preparations = cache.preparations.clone();
        let cancellation = tokio_util::sync::CancellationToken::new();
        let cycles = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(Notify::new());
        let owners = Arc::new(Mutex::new(Vec::new()));
        let (observed_cycles, cycle_completed, observed_owners, cycle_cancellation) = (
            cycles.clone(),
            completed.clone(),
            owners.clone(),
            cancellation.clone(),
        );
        cache.after_cycle = Some(Arc::new(move |cache| {
            // Observe the real worker's outer loop after each column has run
            // three bounded batches, including the sleep/recheck boundary.
            let cycle = observed_cycles.fetch_add(1, Ordering::SeqCst);
            assert_eq!(cache.preparations.load(Ordering::SeqCst), 2);
            assert_eq!(cache.entries.iter().flatten().count(), 2);
            let actual_native_bytes = cache
                .entries
                .iter()
                .flatten()
                .map(|entry| entry.dictionary.as_cdict().sizeof())
                .sum::<usize>();
            assert_eq!(cache.retained_native_bytes(), actual_native_bytes);
            assert!(actual_native_bytes > 2 * bytes.len());
            let mut owners = observed_owners.lock().unwrap();
            for (index, entry) in cache.entries.iter().enumerate() {
                let entry = entry.as_ref().unwrap();
                assert_eq!(
                    Arc::strong_count(&entry.dictionary),
                    1,
                    "completed jobs retain no extra Arc"
                );
                if cycle == 0 {
                    owners.push(Arc::downgrade(&entry.dictionary));
                } else {
                    assert!(owners[index].ptr_eq(&Arc::downgrade(&entry.dictionary)));
                }
            }
            cycle_completed.notify_one();
            if cycle == 1 {
                cycle_cancellation.cancel();
            }
        }));
        let worker = tokio::spawn(crate::database::maintenance::run_zstd_worker_for_test(
            Arc::new(fixture.store.clone()),
            cancellation,
            cache,
        ));
        tokio::time::timeout(std::time::Duration::from_secs(5), completed.notified())
            .await
            .unwrap();
        for (index, &config) in ZSTD_PAYLOAD_COLUMNS.iter().enumerate() {
            let rows = db
                .query_all_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    format!("SELECT payload FROM {}", config.table),
                ))
                .await
                .unwrap();
            let mut actual = rows
                .iter()
                .map(|row| row.try_get::<String>("", "payload").unwrap())
                .collect::<Vec<_>>();
            let mut expected = fixture.payloads[index].clone();
            actual.sort();
            expected.sort();
            assert_eq!(
                actual, expected,
                "existing domain fixtures decode through the unchanged view"
            );
            assert!(
                load_pending_payload_rows(&db, config, 32, 1024 * 1024)
                    .await
                    .unwrap()
                    .is_empty()
            );
            db.execute_unprepared(&format!("UPDATE {} SET payload = payload", config.table))
                .await
                .unwrap();
        }
        // Skip the worker's actual idle delay after repopulating both sources.
        // Resume wall-clock timers before DB/CPU waits, so test timeouts do not
        // auto-advance while SQLite's native threads complete their work.
        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(
            super::PERIODIC_MAINTENANCE_INTERVAL_SECONDS,
        ))
        .await;
        tokio::time::resume();
        tokio::time::timeout(std::time::Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cycles.load(Ordering::SeqCst), 2);
        assert_eq!(preparations.load(Ordering::SeqCst), 2);
        for &config in ZSTD_PAYLOAD_COLUMNS {
            assert!(
                load_pending_payload_rows(&db, config, 32, 1024 * 1024)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(
            owners
                .lock()
                .unwrap()
                .iter()
                .all(|owner| owner.upgrade().is_none()),
            "actual worker shutdown releases CDicts"
        );
    }

    #[tokio::test]
    async fn prepared_cache_replaces_identity_and_releases_old_generations() {
        use std::sync::atomic::Ordering;
        let fixture = budget_fixture(&["shared payload dictionary content"]).await;
        let db = fixture.store.database_connection();
        let mut cache = super::PreparedDictionaryCache::new(&fixture.store);
        let first_bytes = b"shared payload dictionary content";
        prepare_cached_fixture(&mut cache, &db, TURN_EVENT_PAYLOAD, 1, Some(first_bytes)).await;
        let first = Arc::downgrade(&cache.entries[0].as_ref().unwrap().dictionary);
        prepare_cached_fixture(&mut cache, &db, TURN_EVENT_PAYLOAD, 1, Some(first_bytes)).await;
        assert_eq!(cache.preparations.load(Ordering::SeqCst), 1);
        assert!(first.ptr_eq(&Arc::downgrade(
            &cache.entries[0].as_ref().unwrap().dictionary
        )));
        // New ID with identical bytes still changes the identity.
        prepare_cached_fixture(&mut cache, &db, TURN_EVENT_PAYLOAD, 2, Some(first_bytes)).await;
        assert!(first.upgrade().is_none());
        let rotated = Arc::downgrade(&cache.entries[0].as_ref().unwrap().dictionary);
        // No DB mutation here: changing persisted bytes would also invalidate
        // historical decoding. This exercises a changed read snapshot's key.
        prepare_cached_fixture(
            &mut cache,
            &db,
            TURN_EVENT_PAYLOAD,
            2,
            Some(b"different shared content"),
        )
        .await;
        assert!(rotated.upgrade().is_none());
        let changed = Arc::downgrade(&cache.entries[0].as_ref().unwrap().dictionary);
        let different_level = super::ZstdColumnConfig {
            compression_level: 18,
            ..TURN_EVENT_PAYLOAD
        };
        prepare_cached_fixture(
            &mut cache,
            &db,
            different_level,
            2,
            Some(b"different shared content"),
        )
        .await;
        assert!(changed.upgrade().is_none());
        assert_eq!(cache.preparations.load(Ordering::SeqCst), 4);
        prepare_cached_fixture(&mut cache, &db, TURN_ITEM_PAYLOAD, 2, Some(first_bytes)).await;
        assert_eq!(cache.entries.iter().flatten().count(), 2);
        assert_eq!(cache.preparations.load(Ordering::SeqCst), 5);
        let event = Arc::downgrade(&cache.entries[0].as_ref().unwrap().dictionary);
        let item = Arc::downgrade(&cache.entries[1].as_ref().unwrap().dictionary);
        let raw = prepare_cached_fixture(&mut cache, &db, TURN_EVENT_PAYLOAD, -1, None).await;
        assert!(event.upgrade().is_none());
        assert_eq!(
            raw[0].compressed_payload,
            pioneer_sqlite::zstd::compress_column_value(
                raw[0].original_payload.as_bytes(),
                19,
                None
            )
            .unwrap()
        );
        prepare_cached_fixture(&mut cache, &db, TURN_ITEM_PAYLOAD, -1, Some(b"")).await;
        assert!(item.upgrade().is_none());
        assert_eq!(
            cache.preparations.load(Ordering::SeqCst),
            5,
            "no CDict for empty/no dictionary"
        );
        assert_eq!(cache.retained_native_bytes(), 0);
    }

    #[tokio::test]
    async fn prepared_cache_is_private_to_its_database_worker() {
        use std::sync::atomic::Ordering;
        let first = budget_fixture(&["shared content"]).await;
        let second = budget_fixture(&["shared content"]).await;
        let mut first_cache = super::PreparedDictionaryCache::new(&first.store);
        let mut second_cache = super::PreparedDictionaryCache::new(&second.store);
        let first_db = first.store.database_connection();
        let second_db = second.store.database_connection();
        for (cache, db) in [
            (&mut first_cache, &first_db),
            (&mut second_cache, &second_db),
        ] {
            prepare_cached_fixture(cache, db, TURN_EVENT_PAYLOAD, 1, Some(b"shared content")).await;
            assert_eq!(cache.preparations.load(Ordering::SeqCst), 1);
        }
        assert!(!Arc::ptr_eq(
            &first_cache.entries[0].as_ref().unwrap().dictionary,
            &second_cache.entries[0].as_ref().unwrap().dictionary
        ));
        let error = super::prepare_payload_batch(
            &mut first_cache,
            &second_db,
            TURN_EVENT_PAYLOAD,
            Vec::new(),
            super::CompressionDictionary {
                id: 1,
                bytes: Some(b"shared content".to_vec()),
            },
            None,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("different database worker"));
        assert_eq!(first_cache.preparations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_before_cpu_does_not_prepare_or_apply() {
        use std::sync::atomic::Ordering;
        let fixture = budget_fixture(&["shared content"]).await;
        let mut cache = super::PreparedDictionaryCache::new(&fixture.store);
        let db = fixture.store.database_connection();
        let rows = load_pending_payload_rows(&db, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
            .await
            .unwrap();
        let cancellation = tokio_util::sync::CancellationToken::new();
        cancellation.cancel();
        assert!(
            super::prepare_payload_batch(
                &mut cache,
                &db,
                TURN_EVENT_PAYLOAD,
                rows,
                super::CompressionDictionary {
                    id: 1,
                    bytes: Some(b"shared content".to_vec())
                },
                Some(&cancellation),
            )
            .await
            .unwrap()
            .is_none()
        );
        let outcome = super::run_cooperative_maintenance_cycle(
            &fixture.store,
            &mut cache,
            ZSTD_PAYLOAD_COLUMNS,
            None,
            1.0,
            &cancellation,
        )
        .await
        .unwrap();
        assert!(outcome.cancelled);
        assert_eq!(cache.preparations.load(Ordering::SeqCst), 0);
        assert_eq!(cache.retained_native_bytes(), 0);
        assert_eq!(
            load_pending_payload_rows(&db, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(fixture.observer.writes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancellation_after_cpu_drains_join_without_db_apply_or_arc_leak() {
        use std::sync::{Mutex, atomic::Ordering};
        let fixture = budget_fixture(&["shared content"]).await;
        let store = fixture.store.clone().with_maintenance_access();
        let db = store.database_connection();
        let dictionary = super::persist_compression_dictionary(
            &db,
            TURN_EVENT_PAYLOAD,
            b"shared content".to_vec(),
            None,
        )
        .await
        .unwrap();
        let mut cache = super::PreparedDictionaryCache::new(&store);
        prepare_cached_fixture(
            &mut cache,
            &db,
            TURN_EVENT_PAYLOAD,
            dictionary.id,
            dictionary.bytes.as_deref(),
        )
        .await;
        let owner = Arc::downgrade(&cache.entries[0].as_ref().unwrap().dictionary);
        fixture.observer.writes.lock().unwrap().clear();
        let entered = Arc::new(Notify::new());
        let (release, receiver) = std::sync::mpsc::channel::<()>();
        let receiver = Mutex::new(receiver);
        let cpu_entered = entered.clone();
        cache.after_cpu = Some(Arc::new(move || {
            cpu_entered.notify_one();
            // Sender drop also releases this job on test panic.
            let _ = receiver
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(10));
        }));
        let preparations = cache.preparations.clone();
        let (token_send, token_receive) = tokio::sync::oneshot::channel();
        let (outcome_send, outcome_receive) = tokio::sync::oneshot::channel();
        let mut supervisor =
            crate::post_startup::PostStartupSupervisor::start(move |scope| async move {
                let job_cancellation = scope.cancellation();
                token_send.send(job_cancellation.clone()).unwrap();
                let outcome = super::run_one_compression_batch(
                    &store,
                    &mut cache,
                    &db,
                    TURN_EVENT_PAYLOAD,
                    Some(&job_cancellation),
                )
                .await;
                assert_eq!(cache.entries.iter().flatten().count(), 1);
                outcome_send.send(outcome).unwrap();
            });
        let cancellation = token_receive.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        let shutdown = tokio::spawn(async move {
            supervisor.shutdown().await;
        });
        cancellation.cancelled().await;
        assert!(
            !shutdown.is_finished(),
            "started native work must still be owned and awaited"
        );
        assert_eq!(
            owner.strong_count(),
            1,
            "the in-flight slot retains its CDict"
        );
        // Both DB contours remain available while CPU/join is blocked.
        let interactive_db = fixture.store.database_connection();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            assert_eq!(
                load_pending_payload_rows(&interactive_db, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            interactive_db
                .execute_unprepared("UPDATE turn_event SET payload = payload WHERE id = 'event_0'")
                .await
                .unwrap();
        })
        .await
        .unwrap();
        drop(release);
        tokio::time::timeout(std::time::Duration::from_secs(5), shutdown)
            .await
            .unwrap()
            .unwrap();
        let outcome = outcome_receive.await.unwrap().unwrap();
        assert_eq!(outcome.applied_rows, 0);
        assert!(outcome.more_pending);
        assert_eq!(preparations.load(Ordering::SeqCst), 1);
        assert!(
            fixture
                .observer
                .writes
                .lock()
                .unwrap()
                .iter()
                .all(|event| !matches!(
                    event,
                    pioneer_sqlite::SqliteWriteEvent::Acquired {
                        class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                        ..
                    }
                )),
            "cancelled preparation must never submit its maintenance apply"
        );
        assert_eq!(
            load_pending_payload_rows(&interactive_db, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            owner.upgrade().is_none(),
            "supervisor shutdown releases the worker cache and completed CPU references"
        );
    }

    #[tokio::test]
    async fn cancellation_of_queued_apply_finishes_shutdown_before_writer_release() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let fixture = budget_fixture(&["shared content"]).await;
        let store = fixture.store.clone().with_maintenance_access();
        let db = store.database_connection();
        let dictionary = super::persist_compression_dictionary(
            &db,
            TURN_EVENT_PAYLOAD,
            b"shared content".to_vec(),
            None,
        )
        .await
        .unwrap();
        let mut cache = super::PreparedDictionaryCache::new(&store);
        prepare_cached_fixture(
            &mut cache,
            &db,
            TURN_EVENT_PAYLOAD,
            dictionary.id,
            dictionary.bytes.as_deref(),
        )
        .await;
        let owner = Arc::downgrade(&cache.entries[0].as_ref().unwrap().dictionary);
        let cpu_completed = Arc::new(AtomicBool::new(false));
        let observed_cpu = cpu_completed.clone();
        cache.after_cpu = Some(Arc::new(move || {
            observed_cpu.store(true, Ordering::SeqCst);
        }));
        let interactive = fixture.store.database_connection();
        let held_writer = interactive.begin().await.unwrap();
        fixture.observer.writes.lock().unwrap().clear();
        let (result_send, result_receive) = tokio::sync::oneshot::channel();
        let mut supervisor =
            crate::post_startup::PostStartupSupervisor::start(move |scope| async move {
                let cancellation = scope.cancellation();
                let result = super::run_one_compression_batch(
                    &store,
                    &mut cache,
                    &db,
                    TURN_EVENT_PAYLOAD,
                    Some(&cancellation),
                )
                .await;
                result_send.send(result).unwrap();
            });
        fixture.observer.wait_for_writer_queue().await;
        assert!(cpu_completed.load(Ordering::SeqCst));
        // Shutdown must complete without releasing the occupied writer.
        tokio::time::timeout(std::time::Duration::from_secs(5), supervisor.shutdown())
            .await
            .unwrap();
        let error = result_receive.await.unwrap().unwrap_err();
        assert!(error.is::<super::ZstdDatabaseCancelled>());
        fixture.observer.assert_writer_queue_cancelled();
        assert!(owner.upgrade().is_none());
        let pending = load_pending_payload_rows(&interactive, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].payload, fixture.payloads[0][0]);
        held_writer.rollback().await.unwrap();
        // A subsequent real writer operation also proves that no cancelled
        // reservation can be dispatched after the held writer releases.
        interactive
            .execute_unprepared("UPDATE turn_event SET payload = payload WHERE id = 'event_0'")
            .await
            .unwrap();
        fixture.observer.assert_writer_queue_cancelled();
        let pending = load_pending_payload_rows(&interactive, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(pending.len(), 1); // Includes _payload_dict IS NULL.
        assert_eq!(pending[0].payload, fixture.payloads[0][0]);
    }

    #[tokio::test]
    async fn cancellation_of_queued_trained_dictionary_does_not_persist() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let text =
            "shared dictionary training content with repeated field names and values ".repeat(1000);
        let fixture = budget_fixture(&vec![text.as_str(); 16]).await;
        let store = fixture.store.clone().with_maintenance_access();
        let db = store.database_connection();
        let mut cache = super::PreparedDictionaryCache::new(&store);
        let training_completed = Arc::new(AtomicBool::new(false));
        let observed_training = training_completed.clone();
        cache.after_training = Some(Arc::new(move || {
            observed_training.store(true, Ordering::SeqCst);
        }));
        let preparations = cache.preparations.clone();
        let interactive = fixture.store.database_connection();
        let held_writer = interactive.begin().await.unwrap();
        fixture.observer.writes.lock().unwrap().clear();
        let (result_send, result_receive) = tokio::sync::oneshot::channel();
        let mut supervisor =
            crate::post_startup::PostStartupSupervisor::start(move |scope| async move {
                let cancellation = scope.cancellation();
                // Stop at the real resolver: training failure must not make
                // this regression accidentally observe a no-dictionary apply.
                let result = super::resolve_compression_dictionary(
                    &store,
                    &db,
                    TURN_EVENT_PAYLOAD,
                    Some(&cancellation),
                    cache.after_training.clone(),
                )
                .await;
                result_send.send(result).unwrap();
            });
        fixture.observer.wait_for_writer_queue().await;
        assert!(training_completed.load(Ordering::SeqCst));
        tokio::time::timeout(std::time::Duration::from_secs(5), supervisor.shutdown())
            .await
            .unwrap();
        assert!(
            result_receive
                .await
                .unwrap()
                .unwrap_err()
                .is::<super::ZstdDatabaseCancelled>()
        );
        fixture.observer.assert_writer_queue_cancelled();
        assert_eq!(preparations.load(Ordering::SeqCst), 0);
        assert!(
            load_compression_dictionary(&interactive, TURN_EVENT_PAYLOAD)
                .await
                .unwrap()
                .is_none()
        );
        held_writer.rollback().await.unwrap();
        let next_writer = interactive.begin().await.unwrap();
        next_writer.rollback().await.unwrap();
        fixture.observer.assert_writer_queue_cancelled();
        assert!(
            load_compression_dictionary(&interactive, TURN_EVENT_PAYLOAD)
                .await
                .unwrap()
                .is_none()
        );
        let pending =
            load_pending_payload_rows(&interactive, TURN_EVENT_PAYLOAD, 32, 1024 * 1024 * 2)
                .await
                .unwrap();
        assert_eq!(pending.len(), 16);
        assert_eq!(pending[0].payload, fixture.payloads[0][0]);
    }

    #[tokio::test]
    async fn cancellation_of_queued_schema_read_stops_worker_before_cpu_or_next_quantum() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let fixture = budget_fixture(&["shared content"]).await;
        let store = Arc::new(fixture.store.clone().with_maintenance_access());
        let db = store.database_connection();
        let held_reader = db.begin_read().await.unwrap();
        fixture.observer.reads.lock().unwrap().clear();
        fixture.observer.writes.lock().unwrap().clear();
        let mut cache = super::PreparedDictionaryCache::new(&store);
        let preparations = cache.preparations.clone();
        let trainings = Arc::new(AtomicUsize::new(0));
        let observed_trainings = trainings.clone();
        cache.after_training = Some(Arc::new(move || {
            observed_trainings.fetch_add(1, Ordering::SeqCst);
        }));
        let mut supervisor =
            crate::post_startup::PostStartupSupervisor::start(move |scope| async move {
                crate::database::maintenance::run_zstd_worker_for_test(
                    store,
                    scope.cancellation(),
                    cache,
                )
                .await;
            });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.observer.reader_queued.notified(),
        )
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), supervisor.shutdown())
            .await
            .unwrap();
        {
            let events = fixture.observer.reads.lock().unwrap();
            assert!(events.iter().any(|event| matches!(
                event,
                pioneer_sqlite::SqliteReadEvent::AdmissionCancelled {
                    queue_depth: 0,
                    active: 1,
                    ..
                }
            )));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        pioneer_sqlite::SqliteReadEvent::AdmissionEnqueued { .. }
                    ))
                    .count(),
                1,
                "no next DB phase or repeated quantum after cancellation"
            );
        }
        assert!(fixture.observer.writes.lock().unwrap().is_empty());
        assert_eq!(preparations.load(Ordering::SeqCst), 0);
        assert_eq!(trainings.load(Ordering::SeqCst), 0);
        held_reader.rollback().await.unwrap();
        // Runtime read capacity remains reusable after the queued wait is dropped.
        let pending = load_pending_payload_rows(&db, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(pending[0].payload, fixture.payloads[0][0]);
    }

    #[tokio::test]
    async fn cancellation_at_writer_admission_guards_both_mutation_paths() {
        let fixture = budget_fixture(&["shared content"]).await;
        let store = fixture.store.clone().with_maintenance_access();
        let db = store.database_connection();
        let rows = load_pending_payload_rows(&db, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
            .await
            .unwrap();
        let prepared = prepare_payload_rows(rows, 19, None).unwrap();
        for dictionary_commit in [false, true] {
            let cancellation = tokio_util::sync::CancellationToken::new();
            *fixture.observer.cancel_on_writer_admission.lock().unwrap() =
                Some(cancellation.clone());
            // Await the admitted transaction itself, so this exercises the
            // explicit post-admission guard rather than the outer select.
            let result = if dictionary_commit {
                super::persist_compression_dictionary(
                    &db,
                    TURN_EVENT_PAYLOAD,
                    b"candidate".to_vec(),
                    Some(&cancellation),
                )
                .await
                .map(|_| ())
            } else {
                apply_prepared_payload_rows(
                    &db,
                    TURN_EVENT_PAYLOAD,
                    -1,
                    &prepared,
                    Some(&cancellation),
                )
                .await
                .map(|_| ())
            };
            assert!(result.unwrap_err().is::<super::ZstdDatabaseCancelled>());
            let pending = load_pending_payload_rows(&db, TURN_EVENT_PAYLOAD, 32, 1024 * 1024)
                .await
                .unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].payload, fixture.payloads[0][0]);
            assert!(
                load_compression_dictionary(&db, TURN_EVENT_PAYLOAD)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn bounded_zstd_utf8_budgets_cover_compression_and_dictionary_sampling() {
        let ascii = "a".repeat(420_000);
        let utf8 = "汉😀".repeat(60_000);
        let texts = (0..22)
            .map(|index| {
                if index % 2 == 0 {
                    ascii.as_str()
                } else {
                    utf8.as_str()
                }
            })
            .collect::<Vec<_>>();
        let fixture = budget_fixture(&texts).await;
        let db = fixture
            .store
            .with_maintenance_access()
            .database_connection();
        for (index, &config) in ZSTD_PAYLOAD_COLUMNS.iter().enumerate() {
            for (max_rows, max_bytes) in [
                (
                    COMPRESSION_BATCH_MAX_ROWS,
                    COMPRESSION_BATCH_MAX_SOURCE_BYTES,
                ),
                (
                    DICTIONARY_SAMPLE_MAX_ROWS,
                    DICTIONARY_SAMPLE_MAX_SOURCE_BYTES,
                ),
            ] {
                let rows = load_pending_payload_rows(&db, config, max_rows, max_bytes)
                    .await
                    .unwrap();
                let mut bytes = 0;
                let expected = fixture.payloads[index]
                    .iter()
                    .take(max_rows)
                    .take_while(|payload| {
                        bytes += payload.len();
                        bytes <= max_bytes
                    })
                    .collect::<Vec<_>>();
                assert!(
                    rows.iter().map(|row| row.payload.len()).sum::<usize>() <= max_bytes,
                    "{} must obey its byte budget",
                    config.label()
                );
                assert_eq!(
                    rows.iter().map(|row| &row.payload).collect::<Vec<_>>(),
                    expected,
                    "both discovery and payload revalidation must count UTF-8 bytes"
                );
            }
        }
        assert!(
            fixture.observer.writes.lock().unwrap().is_empty(),
            "bounded discovery must use the reader"
        );
        let reads = fixture.observer.reads.lock().unwrap();
        assert!(reads.iter().any(|event| matches!(
            event,
            pioneer_sqlite::SqliteReadEvent::OperationFinished {
                class: pioneer_sqlite::SqliteReadClass::Maintenance,
                ..
            }
        )));
        assert!(reads.iter().all(|event| !matches!(
            event,
            pioneer_sqlite::SqliteReadEvent::OperationFinished {
                class: pioneer_sqlite::SqliteReadClass::Interactive,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn bounded_zstd_utf8_oversized_row_progress_is_restart_safe() {
        let oversized = "汉😀".repeat(1_000);
        let fixture = budget_fixture(&[&oversized, "following ASCII row", "后续😀行"]).await;
        let db = fixture
            .store
            .with_maintenance_access()
            .database_connection();
        for (index, &config) in ZSTD_PAYLOAD_COLUMNS.iter().enumerate() {
            let first = load_pending_payload_rows(&db, config, COMPRESSION_BATCH_MAX_ROWS, 4096)
                .await
                .unwrap();
            assert_eq!(
                first.len(),
                1,
                "an oversized first row must be admitted alone"
            );
            assert_eq!(first[0].payload, fixture.payloads[index][0]);
            assert!(first[0].payload.len() > 4096);
            let prepared = prepare_payload_rows(first, 1, None).unwrap();
            assert_eq!(
                apply_prepared_payload_rows(&db, config, -1, &prepared, None)
                    .await
                    .unwrap(),
                (1, 0)
            );
            assert_eq!(
                apply_prepared_payload_rows(&db, config, -1, &prepared, None)
                    .await
                    .unwrap(),
                (0, 1),
                "replaying the prepared row must not apply it twice"
            );
            // A new store resumes from the durable dictionary markers, rather
            // than an in-memory cursor that could strand the remaining rows.
            let restarted =
                CrudStore::new(fixture.store.database_connection()).with_maintenance_access();
            let tail = load_pending_payload_rows(
                &restarted.database_connection(),
                config,
                COMPRESSION_BATCH_MAX_ROWS,
                4096,
            )
            .await
            .unwrap();
            assert_eq!(
                tail.iter().map(|row| &row.payload).collect::<Vec<_>>(),
                fixture.payloads[index][1..].iter().collect::<Vec<_>>()
            );
            let prepared = prepare_payload_rows(tail, 1, None).unwrap();
            let failing_id = if config == TURN_EVENT_PAYLOAD {
                "event_2"
            } else {
                "turn_item_zstd_item_2"
            };
            db.execute_unprepared(&format!(
                "CREATE TRIGGER zstd_budget_fail BEFORE UPDATE OF payload ON {} \
                 WHEN OLD.id='{failing_id}' BEGIN SELECT RAISE(ABORT, 'test batch rollback'); END",
                config.backing_table,
            ))
            .await
            .unwrap();
            assert!(
                apply_prepared_payload_rows(&db, config, -1, &prepared, None)
                    .await
                    .is_err()
            );
            db.execute_unprepared("DROP TRIGGER zstd_budget_fail")
                .await
                .unwrap();
            let retry = load_pending_payload_rows(&db, config, COMPRESSION_BATCH_MAX_ROWS, 4096)
                .await
                .unwrap();
            assert_eq!(
                retry.iter().map(|row| &row.payload).collect::<Vec<_>>(),
                fixture.payloads[index][1..].iter().collect::<Vec<_>>(),
                "a failure on the second row must roll back the first row too"
            );
            assert_eq!(
                apply_prepared_payload_rows(&db, config, -1, &prepared, None)
                    .await
                    .unwrap(),
                (2, 0)
            );
            let rows = db
                .query_all_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    format!("SELECT payload FROM {} ORDER BY id", config.table),
                ))
                .await
                .unwrap();
            assert_eq!(
                rows.iter()
                    .map(|row| row.try_get::<String>("", "payload").unwrap())
                    .collect::<Vec<_>>(),
                fixture.payloads[index]
            );
            assert!(
                load_pending_payload_rows(&db, config, COMPRESSION_BATCH_MAX_ROWS, 4096)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        let writes = fixture.observer.writes.lock().unwrap();
        assert!(writes.iter().any(|event| matches!(
            event,
            pioneer_sqlite::SqliteWriteEvent::Acquired {
                class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                ..
            }
        )));
        assert!(writes.iter().all(|event| !matches!(event,
            pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } if *class != pioneer_sqlite::SqliteWriteClass::Maintenance
        )));
    }

    #[tokio::test]
    async fn startup_schema_enables_transparent_reads_without_compressing_the_backlog() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        insert_turn_events(&connection, 120).await;

        let store = CrudStore::new(connection.clone());
        let cancellation = tokio_util::sync::CancellationToken::new();
        let summaries = ensure_compression_schema(
            &store,
            std::slice::from_ref(&TURN_EVENT_PAYLOAD),
            &cancellation,
        )
        .await
        .expect("startup should install only transparent schema");

        assert_eq!(summaries.len(), 1);
        assert!(summaries[0].enabled_now);
        let pending = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_event_zstd WHERE _payload_dict IS NULL",
        )
        .await;
        let readable = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM turn_event WHERE json_valid(payload)",
        )
        .await;
        assert_eq!(pending, 120, "startup must not compress historical rows");
        assert_eq!(
            readable, 120,
            "raw rows must remain readable through the view"
        );

        let maintenance_db = store.with_maintenance_access().database_connection();
        let batch = load_pending_payload_rows(
            &maintenance_db,
            TURN_EVENT_PAYLOAD,
            COMPRESSION_BATCH_MAX_ROWS,
            COMPRESSION_BATCH_MAX_SOURCE_BYTES,
        )
        .await
        .expect("bounded maintenance read should work");
        assert_eq!(batch.len(), COMPRESSION_BATCH_MAX_ROWS);
    }

    #[tokio::test]
    async fn startup_compression_converts_turn_event_payload_to_zstd_view_and_preserves_reads() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        insert_turn_events(&connection, 120).await;
        let before_rows = query_i64(&connection, "SELECT COUNT(*) AS value FROM turn_event").await;
        let before_payload_bytes = query_i64(
            &connection,
            "SELECT SUM(length(payload)) AS value FROM turn_event",
        )
        .await;

        let store = CrudStore::new(connection.clone());
        let summary = run_startup_once(&store, TURN_EVENT_PAYLOAD, None, 1.0)
            .await
            .expect("compression should complete");

        assert!(summary.enabled_now);
        assert!(!summary.already_enabled);
        assert_eq!(summary.total_rows, before_rows as u64);
        assert_eq!(summary.pending_after, 0);
        assert!(!summary.maintenance_more_pending);

        let after_rows = query_i64(&connection, "SELECT COUNT(*) AS value FROM turn_event").await;
        let valid_json_rows = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM turn_event WHERE json_valid(payload)",
        )
        .await;
        let json_extract_rows = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM turn_event \
             WHERE json_extract(payload, '$.payload.sequence') IS NOT NULL",
        )
        .await;
        let backing_payload_bytes = query_i64(
            &connection,
            "SELECT SUM(length(payload)) AS value FROM _turn_event_zstd",
        )
        .await;
        let view_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_event'",
        )
        .await;

        assert_eq!(after_rows, before_rows);
        assert_eq!(valid_json_rows, before_rows);
        assert_eq!(json_extract_rows, before_rows);
        assert_eq!(view_count, 1);
        assert!(
            backing_payload_bytes < before_payload_bytes,
            "expected compressed backing payload {backing_payload_bytes} < original {before_payload_bytes}"
        );

        connection
            .execute_unprepared("DELETE FROM turn_event WHERE id = 'event_0'")
            .await
            .expect("turn_event view should support delete");
        let rows_after_delete =
            query_i64(&connection, "SELECT COUNT(*) AS value FROM turn_event").await;
        let backing_rows_after_delete = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_event_zstd",
        )
        .await;
        assert_eq!(rows_after_delete, before_rows - 1);
        assert_eq!(backing_rows_after_delete, before_rows - 1);

        let meta = find_projection_meta(&connection, TURN_EVENT_PAYLOAD.projection_key)
            .await
            .expect("meta lookup should work")
            .expect("meta should exist");
        assert_eq!(meta.status, pioneer_crud::PROJECTION_META_STATUS_COMPLETE);
    }

    #[tokio::test]
    async fn startup_compression_handles_empty_new_database_and_later_inserts() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let summary = run_startup_once(&store, TURN_EVENT_PAYLOAD, None, 1.0)
            .await
            .expect("compression startup should skip empty database");

        assert!(!summary.enabled_now);
        assert!(!summary.already_enabled);
        assert!(summary.skipped_empty);
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.pending_before, 0);
        assert_eq!(summary.pending_after, 0);

        let table_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = 'turn_event'",
        )
        .await;
        let view_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_event'",
        )
        .await;
        let backing_table_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = '_turn_event_zstd'",
        )
        .await;
        assert_eq!(table_count, 1);
        assert_eq!(view_count, 0);
        assert_eq!(backing_table_count, 0);

        insert_turn_events(&connection, 3).await;
        let rows_after_insert =
            query_i64(&connection, "SELECT COUNT(*) AS value FROM turn_event").await;
        let json_extract_rows = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM turn_event \
             WHERE json_extract(payload, '$.payload.sequence') IS NOT NULL",
        )
        .await;
        let backing_table_after_insert = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = '_turn_event_zstd'",
        )
        .await;
        assert_eq!(rows_after_insert, 3);
        assert_eq!(json_extract_rows, 3);
        assert_eq!(backing_table_after_insert, 0);

        insert_turn_events_with_offset(&connection, 120, 1_000).await;
        let compression = run_startup_once(&store, TURN_EVENT_PAYLOAD, None, 1.0)
            .await
            .expect("compression should enable after a new database accumulates rows");
        assert!(compression.enabled_now);
        assert!(!compression.already_enabled);
        assert!(!compression.skipped_empty);
        assert_eq!(compression.total_rows, 123);
        assert_eq!(compression.pending_before, 123);
        assert_eq!(compression.pending_after, 0);

        let view_count_after_compression = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_event'",
        )
        .await;
        assert_eq!(view_count_after_compression, 1);
    }

    #[tokio::test]
    async fn maintenance_compresses_rows_inserted_after_transparent_enable() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        insert_turn_events(&connection, 120).await;
        let store = CrudStore::new(connection.clone());
        run_startup_once(&store, TURN_EVENT_PAYLOAD, None, 1.0)
            .await
            .expect("initial compression should complete");

        insert_turn_events_with_offset(&connection, 20, 1_000).await;
        let pending_before = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_event_zstd WHERE _payload_dict IS NULL",
        )
        .await;
        assert_eq!(pending_before, 20);

        let summary = run_startup_once(&store, TURN_EVENT_PAYLOAD, None, 1.0)
            .await
            .expect("maintenance should compress later inserts");

        assert!(!summary.enabled_now);
        assert!(summary.already_enabled);
        assert_eq!(summary.pending_before, 20);
        assert_eq!(summary.pending_after, 0);
    }

    #[tokio::test]
    async fn startup_compression_converts_turn_item_payload_to_zstd_view_and_preserves_crud_paths()
    {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        insert_turn_items(&connection, 120).await;
        let before_rows = query_i64(&connection, "SELECT COUNT(*) AS value FROM turn_item").await;
        let before_payload_bytes = query_i64(
            &connection,
            "SELECT SUM(length(payload)) AS value FROM turn_item",
        )
        .await;

        let store = CrudStore::new(connection.clone());
        let summary = run_startup_once(&store, TURN_ITEM_PAYLOAD, None, 1.0)
            .await
            .expect("turn_item compression should complete");

        assert!(summary.enabled_now);
        assert!(!summary.already_enabled);
        assert_eq!(summary.total_rows, before_rows as u64);
        assert_eq!(summary.pending_after, 0);

        let valid_json_rows = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM turn_item WHERE json_valid(payload)",
        )
        .await;
        let backing_payload_bytes = query_i64(
            &connection,
            "SELECT SUM(length(payload)) AS value FROM _turn_item_zstd",
        )
        .await;
        let view_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_item'",
        )
        .await;

        assert_eq!(valid_json_rows, before_rows);
        assert_eq!(view_count, 1);
        assert!(
            backing_payload_bytes < before_payload_bytes,
            "expected compressed backing payload {backing_payload_bytes} < original {before_payload_bytes}"
        );

        let item = store
            .get_turn_item("turn_item_zstd", "item_0")
            .await
            .expect("turn_item should read through CrudStore")
            .expect("turn_item should exist");
        let TurnItem::AgentMessage { text, .. } = item else {
            panic!("expected agent message item");
        };
        assert!(text.contains("turn item payload 0"));

        let items = store
            .list_turn_items_by_type("turn_item_zstd", "agent_message")
            .await
            .expect("turn_item list by type should read through CrudStore");
        assert_eq!(items.len(), 120);

        upsert_agent_message_turn_item(
            &connection,
            "turn_item_zstd",
            "item_0",
            "updated turn item payload",
        )
        .await
        .expect("turn_item upsert should work through sqlite-zstd view");

        let updated = store
            .get_turn_item("turn_item_zstd", "item_0")
            .await
            .expect("updated turn_item should read through CrudStore")
            .expect("updated turn_item should exist");
        let TurnItem::AgentMessage { text, .. } = updated else {
            panic!("expected updated agent message item");
        };
        assert!(text.contains("updated turn item payload"));

        let pending_after_update = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_item_zstd WHERE _payload_dict IS NULL",
        )
        .await;
        assert_eq!(pending_after_update, 1);

        let maintenance = run_startup_once(&store, TURN_ITEM_PAYLOAD, None, 1.0)
            .await
            .expect("turn_item maintenance should compress updated rows");
        assert!(!maintenance.enabled_now);
        assert!(maintenance.already_enabled);
        assert_eq!(maintenance.pending_before, 1);
        assert_eq!(maintenance.pending_after, 0);

        let meta = find_projection_meta(&connection, TURN_ITEM_PAYLOAD.projection_key)
            .await
            .expect("meta lookup should work")
            .expect("meta should exist");
        assert_eq!(meta.status, pioneer_crud::PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(meta.source_turn_item_count, before_rows);
    }

    #[tokio::test]
    async fn bounded_zstd_cas_never_overwrites_a_concurrent_turn_item_update() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        insert_turn_items(&connection, 120).await;

        let store = CrudStore::new(connection.clone());
        run_startup_once(&store, TURN_ITEM_PAYLOAD, None, 1.0)
            .await
            .expect("initial compression should create a dictionary");
        upsert_agent_message_turn_item(
            &connection,
            "turn_item_zstd",
            "item_0",
            "payload prepared before race",
        )
        .await
        .expect("first update should work");

        let maintenance_db = store.with_maintenance_access().database_connection();
        let rows = load_pending_payload_rows(
            &maintenance_db,
            TURN_ITEM_PAYLOAD,
            COMPRESSION_BATCH_MAX_ROWS,
            COMPRESSION_BATCH_MAX_SOURCE_BYTES,
        )
        .await
        .expect("pending row should load");
        assert_eq!(rows.len(), 1);
        let dictionary = load_compression_dictionary(&maintenance_db, TURN_ITEM_PAYLOAD)
            .await
            .expect("dictionary lookup should work")
            .expect("dictionary should exist");
        let dictionary_id = dictionary.id;
        let mut cache = super::PreparedDictionaryCache::new(&store);
        let prepared = super::prepare_payload_batch(
            &mut cache,
            &maintenance_db,
            TURN_ITEM_PAYLOAD,
            rows,
            dictionary,
            None,
        )
        .await
        .expect("payload preparation should work")
        .unwrap();

        upsert_agent_message_turn_item(
            &connection,
            "turn_item_zstd",
            "item_0",
            "payload committed during race",
        )
        .await
        .expect("concurrent update should work");
        let (applied, stale) = apply_prepared_payload_rows(
            &maintenance_db,
            TURN_ITEM_PAYLOAD,
            dictionary_id,
            prepared.as_slice(),
            None,
        )
        .await
        .expect("stale maintenance commit should be harmless");
        assert_eq!(applied, 0);
        assert_eq!(stale, 1);
        let pending = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_item_zstd WHERE id = 'turn_item_zstd_item_0' AND _payload_dict IS NULL",
        )
        .await;
        assert_eq!(pending, 1);

        let item = store
            .get_turn_item("turn_item_zstd", "item_0")
            .await
            .expect("turn_item read should work")
            .expect("turn_item should exist");
        let TurnItem::AgentMessage { text, .. } = item else {
            panic!("expected agent message item");
        };
        assert!(text.contains("payload committed during race"));

        run_startup_once(&store, TURN_ITEM_PAYLOAD, None, 1.0)
            .await
            .expect("next bounded cycle should compress the current value");
        let pending_after_retry = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_item_zstd WHERE id = 'turn_item_zstd_item_0' AND _payload_dict IS NULL",
        )
        .await;
        assert_eq!(pending_after_retry, 0);
    }

    #[tokio::test]
    async fn startup_compression_handles_empty_turn_item_table_and_later_upserts() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        let summary = run_startup_once(&store, TURN_ITEM_PAYLOAD, None, 1.0)
            .await
            .expect("turn_item compression startup should skip empty database");

        assert!(!summary.enabled_now);
        assert!(!summary.already_enabled);
        assert!(summary.skipped_empty);
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.pending_before, 0);
        assert_eq!(summary.pending_after, 0);

        let table_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = 'turn_item'",
        )
        .await;
        let view_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_item'",
        )
        .await;
        let backing_table_count = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = '_turn_item_zstd'",
        )
        .await;
        assert_eq!(table_count, 1);
        assert_eq!(view_count, 0);
        assert_eq!(backing_table_count, 0);

        insert_turn_items(&connection, 3).await;
        let item = store
            .get_turn_item("turn_item_zstd", "item_0")
            .await
            .expect("new turn_item should read through CrudStore")
            .expect("new turn_item should exist");
        assert!(matches!(item, TurnItem::AgentMessage { .. }));

        let backing_table_after_insert = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = '_turn_item_zstd'",
        )
        .await;
        assert_eq!(backing_table_after_insert, 0);

        insert_turn_items_with_offset(&connection, 120, 1_000).await;
        let compression = run_startup_once(&store, TURN_ITEM_PAYLOAD, None, 1.0)
            .await
            .expect("turn_item compression should enable after enough rows accumulate");
        assert!(compression.enabled_now);
        assert!(!compression.already_enabled);
        assert!(!compression.skipped_empty);
        assert_eq!(compression.total_rows, 123);
        assert_eq!(compression.pending_before, 123);
        assert_eq!(compression.pending_after, 0);

        let view_count_after_compression = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_item'",
        )
        .await;
        assert_eq!(view_count_after_compression, 1);
    }

    #[tokio::test]
    async fn periodic_maintenance_enables_compression_after_empty_startup_gets_rows() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        let store = CrudStore::new(connection.clone());
        for config in ZSTD_PAYLOAD_COLUMNS {
            let summary = run_startup_once(&store, *config, None, 1.0)
                .await
                .expect("empty startup should skip compression");
            assert!(summary.skipped_empty);
        }

        let turn_event_table = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = 'turn_event'",
        )
        .await;
        let turn_item_table = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'table' AND name = 'turn_item'",
        )
        .await;
        assert_eq!(turn_event_table, 1);
        assert_eq!(turn_item_table, 1);

        insert_turn_events(&connection, 120).await;
        insert_turn_items(&connection, 120).await;

        let outcome = run_periodic_maintenance_once(&store, ZSTD_PAYLOAD_COLUMNS, None, 1.0)
            .await
            .expect("periodic maintenance should enable compression without restart");
        assert!(!outcome.deferred);
        let summaries = outcome.summaries;
        assert_eq!(summaries.len(), 2);
        for summary in summaries {
            assert!(summary.enabled_now);
            assert!(!summary.already_enabled);
            assert!(!summary.skipped_empty);
            assert_eq!(summary.total_rows, 120);
            assert_eq!(summary.pending_before, 120);
            assert_eq!(summary.pending_after, 0);
        }

        let turn_event_view = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_event'",
        )
        .await;
        let turn_item_view = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_item'",
        )
        .await;
        let turn_event_rows =
            query_i64(&connection, "SELECT COUNT(*) AS value FROM turn_event").await;
        let turn_item_rows =
            query_i64(&connection, "SELECT COUNT(*) AS value FROM turn_item").await;
        assert_eq!(turn_event_view, 1);
        assert_eq!(turn_item_view, 1);
        assert_eq!(turn_event_rows, 120);
        assert_eq!(turn_item_rows, 120);
    }

    #[tokio::test]
    async fn periodic_maintenance_queues_behind_interactive_write_and_then_progresses() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        insert_turn_events(&connection, 3).await;

        let store = CrudStore::new(connection);
        let interactive_store = store.clone();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let interactive = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                let database = interactive_store
                    .database_connection()
                    .with_interactive_writes();
                let transaction = database.begin().await.expect("begin interactive writer");
                entered.notify_one();
                release.notified().await;
                transaction
                    .commit()
                    .await
                    .expect("commit interactive writer");
            }
        });

        entered.notified().await;
        let maintenance_store = store.clone();
        let maintenance = tokio::spawn(async move {
            run_periodic_maintenance_once(
                &maintenance_store,
                ZSTD_PAYLOAD_COLUMNS,
                Some(PERIODIC_MAINTENANCE_SLICE_SECONDS),
                1.0,
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(!maintenance.is_finished());

        release.notify_one();
        interactive.await.expect("interactive task should join");
        let outcome = maintenance
            .await
            .expect("maintenance task should join")
            .expect("queued periodic maintenance should succeed");
        assert!(!outcome.deferred);
        assert!(!outcome.summaries.is_empty());
    }

    #[tokio::test]
    async fn startup_compression_enables_all_payload_columns_in_same_database() {
        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd auto-extension should register");
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");

        insert_turn_events(&connection, 120).await;
        insert_turn_items(&connection, 120).await;

        let store = CrudStore::new(connection.clone());
        for config in ZSTD_PAYLOAD_COLUMNS {
            let summary = run_startup_once(&store, *config, None, 1.0)
                .await
                .expect("payload compression should enable for all configured columns");
            assert!(summary.enabled_now);
            assert_eq!(summary.pending_after, 0);
        }

        let turn_event_view = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_event'",
        )
        .await;
        let turn_item_view = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM sqlite_master WHERE type = 'view' AND name = 'turn_item'",
        )
        .await;
        let turn_event_pending = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_event_zstd WHERE _payload_dict IS NULL",
        )
        .await;
        let turn_item_pending = query_i64(
            &connection,
            "SELECT COUNT(*) AS value FROM _turn_item_zstd WHERE _payload_dict IS NULL",
        )
        .await;

        assert_eq!(turn_event_view, 1);
        assert_eq!(turn_item_view, 1);
        assert_eq!(turn_event_pending, 0);
        assert_eq!(turn_item_pending, 0);
    }

    async fn insert_turn_events(connection: &DatabaseConnection, count: i64) {
        insert_turn_events_with_offset(connection, count, 0).await;
    }

    async fn insert_turn_events_with_offset(
        connection: &DatabaseConnection,
        count: i64,
        offset: i64,
    ) {
        for index in 0..count {
            let sequence = offset + index;
            let payload = large_payload(sequence);
            let sql = format!(
                "INSERT INTO turn_event (id, thread_id, turn_id, sequence, event_type, payload, created_at) \
                 VALUES ('event_{sequence}', 'thread_zstd', 'turn_zstd', {sequence}, 'test/event', '{payload}', '2026-01-01 00:00:00 +00:00')"
            );
            connection
                .execute_unprepared(sql.as_str())
                .await
                .expect("turn_event should insert");
        }
    }

    fn large_payload(sequence: i64) -> String {
        let repeated = "abc123xyz ".repeat(512);
        format!(
            r#"{{"kind":"test_event","payload":{{"sequence":{sequence},"content":"{repeated}"}}}}"#
        )
    }

    async fn insert_turn_items(connection: &DatabaseConnection, count: i64) {
        insert_turn_items_with_offset(connection, count, 0).await;
    }

    async fn insert_turn_items_with_offset(
        connection: &DatabaseConnection,
        count: i64,
        offset: i64,
    ) {
        for index in 0..count {
            let sequence = offset + index;
            let item_id = format!("item_{sequence}");
            let marker = format!("turn item payload {sequence}");
            upsert_agent_message_turn_item(connection, "turn_item_zstd", &item_id, &marker)
                .await
                .expect("turn_item should upsert");
        }
    }

    async fn upsert_agent_message_turn_item(
        connection: &DatabaseConnection,
        turn_id: &str,
        item_id: &str,
        marker: &str,
    ) -> Result<(), sea_orm::DbErr> {
        let payload_json = serde_json::to_string(&agent_message_item(item_id, marker))
            .expect("turn item payload should serialize");
        let existing = query_i64(
            connection,
            format!(
                "SELECT COUNT(*) AS value FROM turn_item WHERE turn_id = '{turn_id}' AND item_id = '{item_id}'"
            )
            .as_str(),
        )
        .await;
        if existing > 0 {
            let sql = r#"
                UPDATE turn_item
                SET
                    item_type = 'agent_message',
                    status = 'completed',
                    payload = ?,
                    updated_at = '2026-01-01 00:00:00 +00:00'
                WHERE turn_id = ? AND item_id = ?
            "#;
            return connection
                .execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    sql,
                    [
                        payload_json.into(),
                        turn_id.to_owned().into(),
                        item_id.to_owned().into(),
                    ],
                ))
                .await
                .map(|_| ());
        }

        let sql = r#"
            INSERT INTO turn_item (
                id,
                turn_id,
                item_id,
                item_type,
                status,
                payload,
                active_attempt_number,
                active_attempt_status,
                active_attempt_id,
                last_heartbeat_at,
                lease_expires_at,
                created_at,
                updated_at
            )
            VALUES (
                ?,
                ?,
                ?,
                'agent_message',
                'completed',
                ?,
                0,
                NULL,
                NULL,
                NULL,
                NULL,
                '2026-01-01 00:00:00 +00:00',
                '2026-01-01 00:00:00 +00:00'
            )
        "#;
        connection
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                sql,
                [
                    format!("turn_item_zstd_{item_id}").into(),
                    turn_id.to_owned().into(),
                    item_id.to_owned().into(),
                    payload_json.into(),
                ],
            ))
            .await
            .map(|_| ())
    }

    fn agent_message_item(item_id: &str, marker: &str) -> TurnItem {
        TurnItem::AgentMessage {
            id: item_id.to_owned(),
            text: format!("{} {}", marker, "abc123xyz ".repeat(512)),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        }
    }

    async fn query_i64(connection: &DatabaseConnection, sql: &str) -> i64 {
        let row = connection
            .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql.to_owned()))
            .await
            .expect("query should execute")
            .expect("query should return row");
        row.try_get::<i64>("", "value")
            .expect("value should decode")
    }
}
