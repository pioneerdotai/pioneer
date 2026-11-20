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
    compression_level: 6,
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
    compression_level: 6,
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
    /// Runnable continuation differs from data awaiting a source change.
    #[serde(skip)]
    pub(crate) maintenance_runnable: bool,
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
    runnable: bool,
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
        pioneer_observability::DatabaseOperationSite::ZstdMaintenance,
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
            maintenance_runnable: progress.runnable,
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
        .map(|config| {
            (
                config,
                ColumnMaintenanceProgress {
                    more_pending: true,
                    runnable: true,
                    ..Default::default()
                },
            )
        })
        .collect::<Vec<_>>();

    loop {
        let mut attempted = false;
        let mut made_progress = false;
        for (index, config) in configs.iter().copied().enumerate() {
            if !columns[index].1.runnable {
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
            progress.runnable = batch.more_pending && batch.applied_rows != 0;
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

        if !columns.iter().any(|(_, progress)| progress.runnable) || !attempted || !made_progress {
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
        // Completed Turns may never supply more samples. Compress their finite
        // batch without a dictionary rather than waiting for unrelated Turns.
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
    let trained = pioneer_observability::work::spawn_blocking(move || {
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
    let (entry, result) = pioneer_observability::work::spawn_blocking(move || {
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

// These are extension-owned backing tables (no Entity). Reuse one owner
// predicate for dictionary samples, source reads, backlog checks and commit CAS.
// The projection watermark survives receipt cleanup; absence of receipts alone
// is not evidence that the final event has been projected. A missing stream
// only admits legacy Turns with no canonical events.
fn compression_eligibility_sql() -> &'static str {
    "target.portable_identity='' AND EXISTS(SELECT 1 FROM turn AS owner WHERE owner.id=target.turn_id \
     AND owner.status IN ('completed','failed','interrupted') \
     AND NOT EXISTS(SELECT 1 FROM turn_event AS pending \
       WHERE pending.turn_id=owner.id AND pending.sequence > COALESCE(\
         (SELECT stream.projected_through_sequence FROM turn_event_projection_stream_state AS stream WHERE stream.turn_id=owner.id),-1)))"
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
                 FROM {table} AS target WHERE {dict_column} IS NULL AND {eligibility} \
                 ORDER BY rowid LIMIT ?",
                column = config.column,
                table = config.backing_table,
                dict_column = config.dict_column,
                eligibility = compression_eligibility_sql(),
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
                 WHERE target.{dict_column} IS NULL AND {eligibility} ORDER BY target.rowid",
                column = config.column,
                table = config.backing_table,
                dict_column = config.dict_column,
                eligibility = compression_eligibility_sql(),
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
                    "UPDATE {table} AS target SET {column} = ?, {dict_column} = ? \
                     WHERE rowid = ? AND id = ? AND {dict_column} IS NULL \
                       AND {column} = ? AND {eligibility}",
                    table = config.backing_table,
                    column = config.column,
                    dict_column = config.dict_column,
                    eligibility = compression_eligibility_sql(),
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
    use sea_orm::DbBackend;

    let db = store.with_maintenance_access().database_connection();
    let mut total = 0;
    for &config in ZSTD_PAYLOAD_COLUMNS {
        if !compression_is_enabled(&db, config, None).await? {
            enable_transparent_compression(&db, config).await?;
        }
        // Concurrent task completion can append history during this fixture.
        // Compress the finite prefix present now, rather than require a live
        // table to stay empty between the final batch and the drain assertion.
        let high_water = query_i64(
            &db,
            &format!(
                "SELECT COALESCE(MAX(rowid), 0) AS value FROM {}",
                config.backing_table
            ),
            "failed to capture physical fixture compression boundary",
        )
        .await?;
        let mut drained = false;
        for _ in 0..32 {
            // Pre-policy physical-format fixture, including active history.
            // Production admission is exercised by the actual worker tests.
            let rows = db.query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite, format!(
                "SELECT rowid AS zstd_rowid,id,payload FROM {} WHERE {} IS NULL AND rowid <= ? ORDER BY rowid LIMIT 32",
                config.backing_table, config.dict_column,
            ), [high_water.into()])).await?.into_iter().map(|row| Ok(PendingPayloadRow {
                rowid: row.try_get("", "zstd_rowid")?, id: row.try_get("", "id")?, payload: row.try_get("", "payload")?,
            })).collect::<Result<Vec<_>>>()?;
            if rows.is_empty() {
                // This read proves the prefix was drained. A later write can
                // legitimately make an existing row uncompressed again.
                drained = true;
                break;
            }
            let prepared = prepare_payload_rows(rows, 3, None)?;
            let tx = db.begin().await?;
            for row in prepared {
                total += tx.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite, format!(
                    "UPDATE {} SET payload=?,{}=-1 WHERE rowid=? AND id=? AND {} IS NULL AND payload=?",
                    config.backing_table, config.dict_column, config.dict_column,
                ), [row.compressed_payload.into(),row.rowid.into(),row.id.into(),row.original_payload.into()])).await?.rows_affected();
            }
            tx.commit().await?;
        }
        anyhow::ensure!(
            drained || query_i64(
                &db,
                &format!(
                    "SELECT COUNT(*) AS value FROM {} WHERE {} IS NULL AND rowid <= {high_water}",
                    config.backing_table, config.dict_column
                ),
                "legacy physical fixture must drain"
            )
            .await?
                == 0,
            "test compression did not drain {} through rowid {high_water}",
            config.label()
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
            "SELECT EXISTS(SELECT 1 FROM {} AS target WHERE {} IS NULL AND {} LIMIT 1) AS value",
            config.backing_table,
            config.dict_column,
            compression_eligibility_sql()
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
        // Extension-owned metadata has no Entity. Patch only the level so an
        // existing installation keeps its dictionary/training configuration;
        // already-compressed payloads and their dictionary IDs stay untouched.
        zstd_database_phase(cancellation, async {
            db.execute_raw(Statement::from_sql_and_values(
                db.get_database_backend(),
                "UPDATE _zstd_configs \
                 SET config = json_set(config, '$.compression_level', ?) \
                 WHERE json_extract(config, '$.table') = ? \
                   AND json_extract(config, '$.column') = ? \
                   AND json_extract(config, '$.compression_level') IS NOT ?",
                [
                    config.compression_level.into(),
                    config.table.into(),
                    config.column.into(),
                    config.compression_level.into(),
                ],
            ))
            .await
            .with_context(|| format!("failed to reconcile zstd level for {}", config.label()))?;
            Ok(())
        })
        .await?;
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
        maintenance_runnable: false,
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
            "SELECT COUNT(*) AS value FROM {} AS target WHERE {} IS NULL AND {}",
            config.backing_table,
            config.dict_column,
            compression_eligibility_sql()
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
