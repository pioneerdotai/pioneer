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
use std::time::{Duration, Instant};

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
    run_periodic_maintenance(
        crud_store,
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
    run_periodic_maintenance(
        crud_store,
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
    configs: &[ZstdColumnConfig],
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<ZstdPeriodicMaintenanceOutcome> {
    run_periodic_maintenance(
        crud_store,
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
    verify_sqlite_zstd_registered(&db).await?;

    let mut summaries = Vec::with_capacity(configs.len());
    for config in configs {
        if cancellation.is_cancelled() {
            break;
        }
        let ensure = crud_store
            .run_background_database_quantum(|| {
                let db = db.clone();
                async move { ensure_compression_enabled(&db, *config, RowInspection::Bounded).await }
            })
            .await?;
        summaries.push(summary_without_maintenance(*config, ensure));
    }
    Ok(summaries)
}

async fn run_periodic_maintenance(
    crud_store: &CrudStore,
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
    verify_sqlite_zstd_registered(&db).await?;

    let configs = configs.to_vec();
    let before = crud_store
        .run_background_database_quantum(|| {
            let db = db.clone();
            let configs = configs.clone();
            async move {
                let mut before = Vec::with_capacity(configs.len());
                for config in configs {
                    let ensure = ensure_compression_enabled(&db, config, row_inspection).await?;
                    let pending_before = match row_inspection {
                        RowInspection::Exact => pending_uncompressed_rows(&db, config).await?,
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
            RowInspection::Exact => pending_uncompressed_rows(&db, config).await?,
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
            let batch = run_one_compression_batch(crud_store, db, config, cancellation).await?;
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
    db: &SqliteDatabase,
    config: ZstdColumnConfig,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<CompressionBatchOutcome> {
    let dictionary = resolve_compression_dictionary(crud_store, db, config).await?;
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

    let rows = crud_store
        .run_background_database_quantum(|| {
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
    let prepared = tokio::task::spawn_blocking(move || {
        prepare_payload_rows(rows, config.compression_level, dictionary.bytes.as_deref())
    })
    .await
    .context("zstd payload compression worker failed to join")??;

    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(CompressionBatchOutcome {
            observed_rows,
            source_bytes,
            more_pending: true,
            ..Default::default()
        });
    }
    let (applied_rows, stale_rows) = crud_store
        .run_background_database_quantum(|| {
            let db = db.clone();
            let prepared = &prepared;
            async move { apply_prepared_payload_rows(&db, config, dictionary_id, prepared).await }
        })
        .await?;
    let more_pending = crud_store
        .run_background_database_quantum(|| {
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
) -> Result<DictionaryResolution> {
    if let Some(dictionary) = crud_store
        .run_background_database_quantum(|| {
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

    let sample = crud_store
        .run_background_database_quantum(|| {
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

    let wanted_size = (sample_bytes / 100).clamp(DICTIONARY_MIN_BYTES, DICTIONARY_MAX_BYTES);
    let samples = sample
        .into_iter()
        .map(|row| row.payload.into_bytes())
        .collect::<Vec<_>>();
    let trained = tokio::task::spawn_blocking(move || {
        pioneer_sqlite::zstd::train_dictionary(samples.as_slice(), wanted_size)
    })
    .await
    .context("zstd dictionary training worker failed to join")?;
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
    let dictionary = crud_store
        .run_background_database_quantum(|| {
            let db = db.clone();
            let candidate = candidate.clone();
            async move { persist_compression_dictionary(&db, config, candidate).await }
        })
        .await?;
    Ok(DictionaryResolution {
        dictionary: Some(dictionary),
        observed_rows: sample_rows as u64,
        source_bytes: sample_bytes as u64,
        more_pending: true,
    })
}

fn prepare_payload_rows(
    rows: Vec<PendingPayloadRow>,
    compression_level: i32,
    dictionary: Option<&[u8]>,
) -> Result<Vec<PreparedPayloadRow>> {
    rows.into_iter()
        .map(|row| {
            let compressed_payload = pioneer_sqlite::zstd::compress_column_value(
                row.payload.as_bytes(),
                compression_level,
                dictionary,
            )
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
                "SELECT rowid AS zstd_rowid, length({column}) AS payload_bytes \
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
                              AND selected.payload_bytes = length(target.{column}) \
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
) -> Result<CompressionDictionary> {
    let transaction = db.begin().await.with_context(|| {
        format!(
            "failed to begin zstd dictionary commit for {}",
            config.label()
        )
    })?;
    transaction
        .execute_raw(Statement::from_sql_and_values(
            transaction.get_database_backend(),
            "INSERT INTO _zstd_dicts (chooser_key, dict) VALUES (?, ?) \
             ON CONFLICT(chooser_key) DO NOTHING",
            [config.dictionary_key.into(), candidate.into()],
        ))
        .await
        .with_context(|| format!("failed to commit zstd dictionary for {}", config.label()))?;
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
) -> Result<(u64, u64)> {
    let transaction = db
        .begin()
        .await
        .with_context(|| format!("failed to begin bounded zstd commit for {}", config.label()))?;
    let mut applied = 0u64;
    let mut stale = 0u64;
    for row in rows {
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
    transaction
        .commit()
        .await
        .with_context(|| format!("failed to commit bounded zstd batch for {}", config.label()))?;
    Ok((applied, stale))
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
) -> Result<EnsureCompressionResult> {
    let was_enabled = compression_is_enabled(db, config).await?;
    let (total_rows, has_rows) = match row_inspection {
        RowInspection::Exact => {
            let total_rows = row_count(db, config).await?;
            (total_rows, total_rows != 0)
        }
        RowInspection::Bounded if was_enabled => (0, true),
        RowInspection::Bounded => (0, table_has_rows(db, config).await?),
    };
    let mut enabled_now = false;
    let mut skipped_empty = false;

    if !was_enabled {
        if !has_rows {
            skipped_empty = true;
        } else {
            mark_compression_backfilling(db, config).await?;
            if let Err(error) = enable_transparent_compression(&db.maintenance(), config).await {
                mark_compression_failed(db, config, &error).await?;
                return Err(error);
            }
            mark_compression_complete(db, config, total_rows).await?;
            enabled_now = true;
        }
    } else {
        mark_existing_compression_complete(db, config, total_rows).await?;
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

async fn compression_is_enabled(db: &SqliteDatabase, config: ZstdColumnConfig) -> Result<bool> {
    let backing_table = query_i64(
        db,
        format!(
            "SELECT COUNT(*) AS value \
         FROM sqlite_master \
         WHERE type = 'table' AND name = '{}'",
            config.backing_table
        )
        .as_str(),
        "failed to detect sqlite-zstd backing table",
    )
    .await?;
    let compressed_view = query_i64(
        db,
        format!(
            "SELECT COUNT(*) AS value \
         FROM sqlite_master \
         WHERE type = 'view' AND name = '{}'",
            config.table
        )
        .as_str(),
        "failed to detect sqlite-zstd view",
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
    if !compression_is_enabled(db, config).await? {
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
) -> Result<()> {
    let Some(meta) = find_projection_meta(db, config.projection_key).await? else {
        return mark_compression_complete(db, config, total_rows).await;
    };

    if meta.projection_version == config.projection_version
        && meta.status == PROJECTION_META_STATUS_COMPLETE
    {
        return Ok(());
    }

    mark_compression_complete(db, config, total_rows).await
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
        COMPRESSION_BATCH_MAX_ROWS, COMPRESSION_BATCH_MAX_SOURCE_BYTES,
        PERIODIC_MAINTENANCE_SLICE_SECONDS, TURN_EVENT_PAYLOAD, TURN_ITEM_PAYLOAD,
        ZSTD_PAYLOAD_COLUMNS, apply_prepared_payload_rows, ensure_compression_schema,
        load_compression_dictionary, load_pending_payload_rows, prepare_payload_rows,
        run_periodic_maintenance_once, run_startup_once,
    };
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, find_projection_meta};
    use pioneer_protocol::{AgentMessagePhase, TurnItem};
    use sea_orm::{
        ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement, TransactionTrait,
    };
    use std::sync::Arc;
    use tokio::sync::Notify;

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
        assert!(
            batch.iter().map(|row| row.payload.len()).sum::<usize>()
                <= COMPRESSION_BATCH_MAX_SOURCE_BYTES
        );
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
        let prepared = prepare_payload_rows(
            rows,
            TURN_ITEM_PAYLOAD.compression_level,
            dictionary.bytes.as_deref(),
        )
        .expect("payload preparation should work");

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
