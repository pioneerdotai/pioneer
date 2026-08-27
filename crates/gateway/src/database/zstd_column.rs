use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, ProjectionMetaRecord, find_projection_meta,
    upsert_projection_meta,
};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, Statement, entity::prelude::DateTimeWithTimeZone,
};
use serde_json::json;
use std::time::{Duration, Instant};

pub(crate) const STARTUP_MAINTENANCE_SECONDS: f64 = 60.0;
pub(crate) const STARTUP_TARGET_DB_LOAD: f64 = 1.0;
pub(crate) const PERIODIC_MAINTENANCE_INTERVAL_SECONDS: u64 = 300;
pub(crate) const PERIODIC_MAINTENANCE_SECONDS: f64 = 10.0;
pub(crate) const PERIODIC_MAINTENANCE_SLICE_SECONDS: f64 = 0.25;
pub(crate) const PERIODIC_TARGET_DB_LOAD: f64 = 0.25;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ZstdColumnConfig {
    pub(crate) projection_key: &'static str,
    pub(crate) projection_version: i64,
    pub(crate) table: &'static str,
    pub(crate) column: &'static str,
    pub(crate) backing_table: &'static str,
    pub(crate) dict_column: &'static str,
    pub(crate) dict_chooser: &'static str,
    pub(crate) count_source: ZstdColumnCountSource,
}

impl ZstdColumnConfig {
    fn label(self) -> String {
        format!("{}.{}", self.table, self.column)
    }
}

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CooperativeMaintenanceOutcome {
    more_pending: bool,
    deferred: bool,
    cancelled: bool,
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
    let db = crud_store.database_connection();
    verify_sqlite_zstd_registered(&db).await?;

    let ensure = ensure_compression_enabled(&db, config, RowInspection::Exact).await?;
    if ensure.skipped_empty {
        return Ok(summary_without_maintenance(config, ensure));
    }

    let pending_before = pending_uncompressed_rows(&db, config).await?;
    let maintenance_more_pending =
        run_maintenance(&db, maintenance_seconds, target_db_load).await?;
    let pending_after = pending_uncompressed_rows(&db, config).await?;

    Ok(ZstdColumnCompressionSummary {
        table: config.table,
        column: config.column,
        enabled_now: ensure.enabled_now,
        already_enabled: ensure.was_enabled,
        skipped_empty: ensure.skipped_empty,
        total_rows: ensure.total_rows,
        pending_before,
        pending_after,
        maintenance_more_pending,
        counts_exact: ensure.counts_exact,
    })
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

async fn run_periodic_maintenance(
    crud_store: &CrudStore,
    configs: &[ZstdColumnConfig],
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
    row_inspection: RowInspection,
) -> Result<ZstdPeriodicMaintenanceOutcome> {
    if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Ok(ZstdPeriodicMaintenanceOutcome {
            cancelled: true,
            ..Default::default()
        });
    }
    let db = crud_store.database_connection();
    verify_sqlite_zstd_registered(&db).await?;

    let configs = configs.to_vec();
    let Some(before) = crud_store
        .try_run_low_priority_write(|| {
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
        .await?
    else {
        return Ok(ZstdPeriodicMaintenanceOutcome {
            summaries: Vec::new(),
            deferred: true,
            cancelled: false,
        });
    };

    let should_run_maintenance = before
        .iter()
        .any(|(_, ensure, _)| ensure.was_enabled || ensure.enabled_now);
    let maintenance = if should_run_maintenance {
        run_cooperative_maintenance(
            crud_store,
            &db,
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
    for (config, ensure, pending_before) in before {
        let pending_after = match row_inspection {
            RowInspection::Exact => pending_uncompressed_rows(&db, config).await?,
            RowInspection::Bounded => 0,
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
            maintenance_more_pending: maintenance.more_pending,
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
    db: &DatabaseConnection,
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
    cancellation: Option<&tokio_util::sync::CancellationToken>,
) -> Result<CooperativeMaintenanceOutcome> {
    let Some(total_seconds) = maintenance_seconds else {
        let result = crud_store
            .try_run_low_priority_write(|| {
                let db = db.clone();
                async move { run_maintenance(&db, None, target_db_load).await }
            })
            .await?;
        return Ok(match result {
            Some(more_pending) => CooperativeMaintenanceOutcome {
                more_pending,
                deferred: false,
                cancelled: false,
            },
            None => CooperativeMaintenanceOutcome {
                more_pending: true,
                deferred: true,
                cancelled: false,
            },
        });
    };

    let budget = Duration::from_secs_f64(total_seconds.max(0.0));
    let deadline = Instant::now() + budget;
    let mut more_pending = true;
    while more_pending {
        if cancellation.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            return Ok(CooperativeMaintenanceOutcome {
                more_pending: true,
                deferred: false,
                cancelled: true,
            });
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let slice_seconds = remaining
            .as_secs_f64()
            .min(PERIODIC_MAINTENANCE_SLICE_SECONDS);
        let result = crud_store
            .try_run_low_priority_write(|| {
                let db = db.clone();
                async move { run_maintenance(&db, Some(slice_seconds), target_db_load).await }
            })
            .await?;
        let Some(slice_more_pending) = result else {
            return Ok(CooperativeMaintenanceOutcome {
                more_pending: true,
                deferred: true,
                cancelled: false,
            });
        };
        more_pending = slice_more_pending;
        if more_pending {
            if let Some(cancellation) = cancellation {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return Ok(CooperativeMaintenanceOutcome {
                            more_pending: true,
                            deferred: false,
                            cancelled: true,
                        });
                    }
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                }
            } else {
                tokio::task::yield_now().await;
            }
        }
    }

    Ok(CooperativeMaintenanceOutcome {
        more_pending,
        deferred: false,
        cancelled: false,
    })
}

async fn ensure_compression_enabled(
    db: &DatabaseConnection,
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
            if let Err(error) = enable_transparent_compression(db, config).await {
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

#[cfg(test)]
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
        maintenance_more_pending: false,
        counts_exact: ensure.counts_exact,
    }
}

async fn verify_sqlite_zstd_registered(db: &DatabaseConnection) -> Result<()> {
    query_i64(
        db,
        "SELECT length(zstd_compress('pioneer-sqlite', 1)) AS value",
        "failed to verify sqlite-zstd functions are registered",
    )
    .await
    .map(|_| ())
}

async fn compression_is_enabled(db: &DatabaseConnection, config: ZstdColumnConfig) -> Result<bool> {
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

async fn enable_transparent_compression<C>(db: &C, config: ZstdColumnConfig) -> Result<()>
where
    C: ConnectionTrait,
{
    let sqlite_zstd_config = json!({
        "table": config.table,
        "column": config.column,
        "compression_level": 19,
        "dict_chooser": config.dict_chooser
    });
    db.query_one_raw(Statement::from_sql_and_values(
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

async fn run_maintenance(
    db: &DatabaseConnection,
    maintenance_seconds: Option<f64>,
    target_db_load: f64,
) -> Result<bool> {
    let sql = match maintenance_seconds {
        Some(seconds) => {
            format!("SELECT zstd_incremental_maintenance({seconds}, {target_db_load}) AS value")
        }
        None => format!("SELECT zstd_incremental_maintenance(NULL, {target_db_load}) AS value"),
    };
    let result = query_i64(db, sql.as_str(), "failed to run sqlite-zstd maintenance").await?;
    Ok(result != 0)
}

async fn pending_uncompressed_rows(
    db: &DatabaseConnection,
    config: ZstdColumnConfig,
) -> Result<u64> {
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

async fn row_count(db: &DatabaseConnection, config: ZstdColumnConfig) -> Result<u64> {
    query_i64(
        db,
        format!("SELECT COUNT(*) AS value FROM {}", config.table).as_str(),
        "failed to count zstd target rows",
    )
    .await
    .map(|value| value.max(0) as u64)
}

async fn table_has_rows(db: &DatabaseConnection, config: ZstdColumnConfig) -> Result<bool> {
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

async fn mark_existing_compression_complete(
    db: &DatabaseConnection,
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

async fn mark_compression_failed(
    db: &DatabaseConnection,
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
        PERIODIC_MAINTENANCE_SLICE_SECONDS, TURN_EVENT_PAYLOAD, TURN_ITEM_PAYLOAD,
        ZSTD_PAYLOAD_COLUMNS, run_periodic_maintenance_once, run_startup_once,
    };
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{CrudStore, find_projection_meta};
    use pioneer_protocol::{AgentMessagePhase, TurnItem};
    use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
    use std::sync::Arc;
    use tokio::sync::Notify;

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
    async fn periodic_maintenance_defers_while_foreground_write_is_active() {
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
        let foreground_store = store.clone();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let foreground = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                foreground_store
                    .try_run_low_priority_write(|| async {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                    .await
            }
        });

        entered.notified().await;
        let outcome = run_periodic_maintenance_once(
            &store,
            ZSTD_PAYLOAD_COLUMNS,
            Some(PERIODIC_MAINTENANCE_SLICE_SECONDS),
            1.0,
        )
        .await
        .expect("busy foreground writer should defer periodic maintenance");
        assert!(outcome.deferred);
        assert!(outcome.summaries.is_empty());

        release.notify_one();
        assert_eq!(
            foreground
                .await
                .expect("foreground task should join")
                .expect("foreground write should succeed"),
            Some(())
        );
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
