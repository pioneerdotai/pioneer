//! One Gateway worker owns this queue. Reader selections are advisory: the
//! serialized DELETE transaction revalidates readiness, ownership and bytes.
use anyhow::{Context, Result};
use pioneer_entity::{
    cli_runtime_native_event as event, native_event_cleanup_bootstrap as bootstrap_state,
    native_event_cleanup_job as job, native_event_cleanup_scheduler as scheduler,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, Set, TransactionTrait,
};
use std::{collections::BTreeSet, time::Instant};

pub(crate) const PAGE_ROWS: usize = 128;
pub(crate) const PAYLOAD_BUDGET: i64 = 256 * 1024;
const RETRY_DELAY_MICROS: i64 = 60_000_000;
#[path = "native_event_cleanup_queries.rs"]
mod queries;

/// Statement counts exclude trigger VM and BEGIN/COMMIT. Elapsed times include
/// queue waiting; actual writer hold/wait is recorded by SQLite observers.
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
#[derive(Debug, Default)]
pub struct NativeEventCleanupBootstrap {
    pub rows_scanned: u64,
    pub jobs_inserted: u64,
    pub complete: bool,
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
pub(crate) async fn run(db: &SqliteDatabase, now: i64) -> Result<NativeEventCleanupMetrics> {
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
#[derive(FromQueryResult)]
struct BootstrapRow {
    id: String,
    turn_id: Option<String>,
    eligible: bool,
}
pub(crate) async fn bootstrap(db: &SqliteDatabase) -> Result<NativeEventCleanupBootstrap> {
    let state = bootstrap_state::Entity::find_by_id(1_i64)
        .one(db)
        .await?
        .context("missing cleanup bootstrap marker")?;
    if state.complete {
        return Ok(NativeEventCleanupBootstrap {
            complete: true,
            ..Default::default()
        });
    }
    let mut query = event::Entity::find()
        .select_only()
        .columns([event::Column::Id, event::Column::TurnId])
        .column_as(queries::candidate_predicate(None), "eligible")
        .order_by_asc(event::Column::Id)
        .limit(PAGE_ROWS as u64);
    if let Some(after) = state.cursor_id.as_deref() {
        query = query.filter(event::Column::Id.gt(after));
    }
    let rows = query.into_model::<BootstrapRow>().all(db).await?;
    let turns = rows
        .iter()
        .filter(|row| row.eligible)
        .filter_map(|row| row.turn_id.as_deref())
        .collect::<BTreeSet<_>>();
    let mut result = NativeEventCleanupBootstrap {
        rows_scanned: rows.len() as u64,
        complete: rows.is_empty(),
        ..Default::default()
    };
    let tx = db.begin().await?;
    // CAS prevents an overlapping bootstrap caller from moving the cursor back.
    let cursor = match state.cursor_id.as_deref() {
        Some(id) => bootstrap_state::Column::CursorId.eq(id),
        None => bootstrap_state::Column::CursorId.is_null(),
    };
    let updated = bootstrap_state::Entity::update_many()
        .col_expr(
            bootstrap_state::Column::CursorId,
            Expr::val(rows.last().map(|row| row.id.clone()).or(state.cursor_id)),
        )
        .col_expr(
            bootstrap_state::Column::Complete,
            Expr::val(result.complete),
        )
        .filter(bootstrap_state::Column::Singleton.eq(1))
        .filter(bootstrap_state::Column::Complete.eq(false))
        .filter(cursor)
        .exec(&tx)
        .await?
        .rows_affected;
    if updated == 0 {
        tx.rollback().await?;
        return Ok(NativeEventCleanupBootstrap::default());
    }
    for turn in turns {
        result.jobs_inserted += job::Entity::insert(job::ActiveModel {
            turn_id: Set(turn.to_owned()),
            state: Set("queued".to_owned()),
            ..Default::default()
        })
        .on_conflict(
            OnConflict::column(job::Column::TurnId)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(&tx)
        .await?;
    }
    tx.commit().await?;
    Ok(result)
}
#[cfg(test)]
#[path = "native_event_cleanup_tests.rs"]
mod tests;
