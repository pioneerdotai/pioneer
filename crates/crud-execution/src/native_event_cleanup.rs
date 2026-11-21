//! One Gateway worker owns this queue. Reader selections are advisory: the
//! serialized DELETE transaction revalidates readiness, ownership and bytes.
use anyhow::{Context, Result};
use pioneer_entity::{
    cli_runtime_native_event as event, native_event_cleanup_job as job,
    native_event_cleanup_scheduler as scheduler,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, TransactionTrait,
};
use std::time::Instant;

pub(crate) const PAGE_ROWS: usize = 128;
pub(crate) const PAYLOAD_BUDGET: i64 = 256 * 1024;
const RETRY_DELAY_MICROS: i64 = 60_000_000;
#[path = "native_event_cleanup/queries.rs"]
mod queries;

/// Statement counts exclude database triggers and BEGIN/COMMIT. Elapsed times include
/// queue waiting; actual writer hold/wait is recorded by database observers.
#[derive(Debug, Default)]
pub struct NativeEventCleanupMetrics {
    pub jobs_examined: u64,
    pub candidate_rows_fetched: u64,
    pub events_selected: u64,
    pub selected_bytes: i64,
    pub events_revalidated: u64,
    pub events_deleted: u64,
    pub deleted_bytes: i64,
    pub prepare_reads: u64,
    pub apply_reads: u64,
    pub apply_writes: u64,
    pub queue_rows_changed: u64,
    pub scheduler_rows_changed: u64,
    pub errors_deferred: u64,
    pub prepare_elapsed_us: u64,
    pub apply_elapsed_us: u64,
}
#[derive(FromQueryResult)]
struct Job {
    turn_id: String,
    regular_lane: Option<String>,
}
#[derive(FromQueryResult)]
struct Candidate {
    runtime_id: String,
    id: Option<String>,
    payload_bytes: Option<i64>,
}
struct Prepared {
    job: Job,
    runtime_id: Option<String>,
    ids: Vec<String>,
}
#[derive(FromQueryResult)]
struct Remaining {
    current_runtime_remaining: bool,
    any_remaining: bool,
}
fn elapsed_us(start: Instant) -> u64 {
    start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
}

async fn candidates<C: ConnectionTrait>(
    db: &C,
    turn: &str,
    selected: Option<&[String]>,
) -> Result<Vec<Candidate>> {
    Ok(Candidate::find_by_statement(
        db.get_database_backend()
            .build(&queries::candidates(turn, selected)),
    )
    .all(db)
    .await?)
}
fn bounded_ids(rows: Vec<Candidate>) -> (Vec<String>, i64) {
    let mut ids = Vec::new();
    let mut bytes = 0_i64;
    for row in rows.into_iter().take(PAGE_ROWS) {
        if let (Some(id), Some(size)) = (row.id, row.payload_bytes) {
            if bytes.saturating_add(size) > PAYLOAD_BUDGET {
                break;
            }
            bytes += size;
            ids.push(id);
        }
    }
    (ids, bytes)
}
pub async fn run_quantum(db: &SqliteDatabase) -> Result<NativeEventCleanupMetrics> {
    use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass};
    if db.read_class() != SqliteReadClass::Maintenance
        || db.write_class() != SqliteWriteClass::Maintenance
    {
        anyhow::bail!("background database quantum requires maintenance-scoped reads and writes");
    }
    pioneer_sqlite::retry_with_backoff(
        || run(db, chrono::Utc::now().timestamp_micros()),
        pioneer_sqlite::is_retryable_database_conflict,
        pioneer_sqlite::DEFAULT_LOCK_RETRY_ATTEMPTS,
        std::time::Duration::from_millis(pioneer_sqlite::DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

pub async fn run(db: &SqliteDatabase, now: i64) -> Result<NativeEventCleanupMetrics> {
    let now = std::cmp::max(now, 1);
    let started = Instant::now();
    let mut metrics = NativeEventCleanupMetrics {
        prepare_reads: 1,
        ..Default::default()
    };
    let Some(job) =
        Job::find_by_statement(db.get_database_backend().build(&queries::discovery(now)))
            .one(db)
            .await?
    else {
        metrics.prepare_elapsed_us = elapsed_us(started);
        return Ok(metrics);
    };
    metrics.jobs_examined = 1;
    metrics.prepare_reads += 1;
    let rows = match candidates(db, &job.turn_id, None).await {
        Ok(rows) => rows,
        Err(_) => {
            metrics.prepare_elapsed_us = elapsed_us(started);
            let started = Instant::now();
            defer(db, &job.turn_id, now, "prepare_failed", &mut metrics).await?;
            metrics.apply_elapsed_us = elapsed_us(started);
            return Ok(metrics);
        }
    };
    let runtime_id = rows.first().map(|row| row.runtime_id.clone());
    metrics.candidate_rows_fetched = rows.iter().filter(|row| row.id.is_some()).count() as u64;
    let (ids, bytes) = bounded_ids(rows);
    metrics.events_selected = ids.len() as u64;
    metrics.selected_bytes = bytes;
    metrics.prepare_elapsed_us = elapsed_us(started);
    let prepared = Prepared {
        job,
        runtime_id,
        ids,
    };
    let started = Instant::now();
    if apply(db, &prepared, now, &mut metrics).await.is_err() {
        // Failed transactions roll back: don't count their DELETEs as committed.
        metrics.events_deleted = 0;
        metrics.deleted_bytes = 0;
        metrics.queue_rows_changed = 0;
        metrics.scheduler_rows_changed = 0;
        defer(db, &prepared.job.turn_id, now, "apply_failed", &mut metrics).await?;
    }
    metrics.apply_elapsed_us = elapsed_us(started);
    Ok(metrics)
}
async fn defer(
    db: &SqliteDatabase,
    turn: &str,
    now: i64,
    reason: &str,
    metrics: &mut NativeEventCleanupMetrics,
) -> Result<()> {
    metrics.apply_writes += 1;
    metrics.queue_rows_changed += job::Entity::update_many()
        .col_expr(job::Column::State, Expr::val("queued"))
        .col_expr(
            job::Column::AvailableAt,
            Expr::val(now.saturating_add(RETRY_DELAY_MICROS)),
        )
        .col_expr(job::Column::LastError, Expr::val(reason))
        .col_expr(
            job::Column::Revision,
            Expr::col(job::Column::Revision).add(1),
        )
        .filter(job::Column::TurnId.eq(turn))
        .exec(db)
        .await?
        .rows_affected;
    metrics.errors_deferred += 1;
    Ok(())
}
async fn apply(
    db: &SqliteDatabase,
    prepared: &Prepared,
    now: i64,
    metrics: &mut NativeEventCleanupMetrics,
) -> Result<()> {
    let tx = db.begin().await?;
    metrics.apply_reads += 1;
    let rows = candidates(&tx, &prepared.job.turn_id, Some(&prepared.ids)).await?;
    let runtime_id = rows.first().map(|row| row.runtime_id.clone());
    if runtime_id.is_some() && runtime_id == prepared.runtime_id {
        metrics.events_revalidated = rows.iter().filter(|row| row.id.is_some()).count() as u64;
        let (ids, bytes) = bounded_ids(rows);
        if !ids.is_empty() {
            metrics.apply_writes += 1;
            metrics.events_deleted = event::Entity::delete_many()
                .filter(event::Column::Id.is_in(ids))
                .exec(&tx)
                .await?
                .rows_affected;
            metrics.deleted_bytes = bytes;
        }
    }
    // Late writes are either visible below or run their triggers after commit.
    metrics.apply_reads += 1;
    let remaining = Remaining::find_by_statement(tx.get_database_backend().build(
        &queries::remaining(&prepared.job.turn_id, runtime_id.as_deref()),
    ))
    .one(&tx)
    .await?
    .context("missing remaining-candidate result")?;
    metrics.apply_writes += 1;
    metrics.queue_rows_changed += if remaining.any_remaining {
        let state = if runtime_id.is_some() && remaining.current_runtime_remaining {
            "queued"
        } else {
            "waiting"
        };
        job::Entity::update_many()
            .col_expr(job::Column::State, Expr::val(state))
            .col_expr(job::Column::AvailableAt, Expr::val(0_i64))
            .col_expr(job::Column::LastServedAt, Expr::val(now))
            .col_expr(job::Column::LastError, Expr::val(None::<String>))
            .col_expr(
                job::Column::Revision,
                Expr::col(job::Column::Revision).add(1),
            )
            .filter(job::Column::TurnId.eq(&prepared.job.turn_id))
            .exec(&tx)
            .await?
            .rows_affected
    } else {
        job::Entity::delete_by_id(prepared.job.turn_id.clone())
            .exec(&tx)
            .await?
            .rows_affected
    };
    if let Some(lane) = prepared.job.regular_lane.as_deref() {
        let update = scheduler::Entity::update_many().filter(scheduler::Column::Singleton.eq(1));
        let counter = scheduler::Column::NewJobsSinceServed;
        let update = match lane {
            "new" => update
                .col_expr(counter, Expr::col(counter).add(1))
                .filter(counter.lt(4)),
            "served" => update
                .col_expr(counter, Expr::val(0_i64))
                .filter(counter.ne(0)),
            _ => anyhow::bail!("invalid cleanup scheduler lane"),
        };
        metrics.apply_writes += 1;
        metrics.scheduler_rows_changed += update.exec(&tx).await?.rows_affected;
    }
    tx.commit().await?;
    Ok(())
}
#[cfg(test)]
#[path = "native_event_cleanup/tests.rs"]
mod tests;

/// Earliest queued retry, rounded up to its first eligible second.
pub async fn deadline<C: ConnectionTrait>(db: &C) -> Result<Option<i64>> {
    let due = job::Entity::find()
        .select_only()
        .column(job::Column::AvailableAt)
        .filter(job::Column::State.eq("queued"))
        .filter(job::Column::AvailableAt.gt(0_i64))
        .order_by_asc(job::Column::AvailableAt)
        .into_tuple::<i64>()
        .one(db)
        .await?;
    Ok(due
        .map(|micros| micros.div_euclid(1_000_000) + i64::from(micros.rem_euclid(1_000_000) != 0)))
}

#[cfg(test)]
#[path = "native_event_cleanup/retention_tests.rs"]
mod retention_tests;
