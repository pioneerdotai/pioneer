//! Persistent dirty locators for Task occurrence contracts. No domain payloads
//! or historical discovery. All bookkeeping inherits Maintenance scope.
use anyhow::Result;
use pioneer_entity::{
    task_occurrence_reconcile_pending as pending, task_occurrence_reconcile_scope as scope,
    task_occurrence_reconcile_seed as seed, task_occurrence_reconcile_sequence as sequence,
    task_run,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Expr, OnConflict, Order, Query, SelectStatement};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, ExprTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, Set, TransactionTrait,
};

pub const TASK_OCCURRENCE_RECONCILE_BUDGET: u64 = 64;
// Policy choices, not measured timings: tolerate short fanout gaps, then keep
// poison candidates retryable without polling them on every two-second pass.
pub const TASK_OCCURRENCE_RECONCILE_INITIAL_BACKOFF_SECS: i64 = 5;
pub const TASK_OCCURRENCE_RECONCILE_MAX_BACKOFF_SECS: i64 = 300;
pub const TASK_OCCURRENCE_RECONCILE_MAX_ATTEMPT_COUNT: i64 = 16;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct TaskOccurrenceReconcileCandidate {
    pub run_id: String,
    pub generation: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskOccurrenceReconcileClaim {
    pub run_id: String,
    pub generation: i64,
    pub claim_token: String,
    pub attempt_count: i64,
    pub next_attempt_at: i64,
}

/// The clock is read after writer admission. Implementations must be immediate,
/// nonblocking clock reads, with no I/O or database work.
pub type TaskOccurrenceClock<'a> = dyn Fn() -> i64 + Send + Sync + 'a;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskOccurrenceClaimFailurePhase {
    AdvisoryRead,
    Reservation,
    CommitOutcomeUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskOccurrenceClaimDeferral {
    NoSnapshot,
    Deferred,
    StateChanged,
    Failed,
}

#[derive(Debug)]
pub struct TaskOccurrenceClaimFailure {
    pub phase: TaskOccurrenceClaimFailurePhase,
    pub deferral: TaskOccurrenceClaimDeferral,
    pub error: anyhow::Error,
    pub deferral_error: Option<anyhow::Error>,
}

impl std::fmt::Display for TaskOccurrenceClaimFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Task occurrence contract claim failed")
    }
}

impl std::error::Error for TaskOccurrenceClaimFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

pub(crate) struct FailedClaim {
    pub error: anyhow::Error,
    pub phase: TaskOccurrenceClaimFailurePhase,
    // Exact pre-attempt state. This must never be replaced by a fresh read on
    // error: that could defer another holder or a newly generated candidate.
    pub snapshot: Option<pending::Model>,
}

pub(crate) async fn has_pending<C: ConnectionTrait>(db: &C) -> Result<bool> {
    let query = Query::select()
        .expr(Expr::val(1))
        .from(pending::Entity)
        .limit(1)
        .to_owned();
    if db
        .query_one_raw(db.get_database_backend().build(&query))
        .await?
        .is_some()
    {
        return Ok(true);
    }
    if scope::Entity::find()
        .select_only()
        .column(scope::Column::TaskId)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    let seed = seed::Entity::find_by_id(1)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("task occurrence seed missing"))?;
    Ok(seed.status_index < 5)
}

pub(crate) fn due_query(now: i64, limit: u64) -> SelectStatement {
    Query::select()
        .columns([pending::Column::RunId, pending::Column::Generation])
        .from(pending::Entity)
        .and_where(Expr::col(pending::Column::NextAttemptAt).lte(now))
        .order_by(pending::Column::NextAttemptAt, Order::Asc)
        .order_by(pending::Column::Generation, Order::Asc)
        .order_by(pending::Column::RunId, Order::Asc)
        .limit(std::cmp::min(limit, TASK_OCCURRENCE_RECONCILE_BUDGET))
        .to_owned()
}

pub(crate) async fn discover<C: ConnectionTrait>(
    db: &C,
    now: i64,
    limit: u64,
) -> Result<Vec<TaskOccurrenceReconcileCandidate>> {
    Ok(TaskOccurrenceReconcileCandidate::find_by_statement(
        db.get_database_backend().build(&due_query(now, limit)),
    )
    .all(db)
    .await?)
}

pub(crate) async fn owns<C: ConnectionTrait>(
    db: &C,
    claim: &TaskOccurrenceReconcileClaim,
) -> Result<bool> {
    Ok(pending::Entity::find_by_id(claim.run_id.clone())
        .filter(pending::Column::Generation.eq(claim.generation))
        .filter(pending::Column::ClaimToken.eq(claim.claim_token.clone()))
        .one(db)
        .await?
        .is_some())
}

/// Called only after a fresh metadata predicate, inside the same writer as
/// repair. Passing the post-repair snapshot accounts for our own trigger.
pub(crate) async fn acknowledge<C: ConnectionTrait>(db: &C, row: &pending::Model) -> Result<()> {
    pending::Entity::delete_many()
        .filter(snapshot_filter(row))
        .exec(db)
        .await?;
    Ok(())
}

pub(crate) fn retry_delay(attempt: i64) -> i64 {
    std::cmp::min(
        TASK_OCCURRENCE_RECONCILE_INITIAL_BACKOFF_SECS
            .saturating_mul(1_i64 << (attempt.saturating_sub(1).clamp(0, 15) as u32)),
        TASK_OCCURRENCE_RECONCILE_MAX_BACKOFF_SECS,
    )
}

fn snapshot_filter(row: &pending::Model) -> sea_orm::Condition {
    let condition = sea_orm::Condition::all()
        .add(pending::Column::RunId.eq(row.run_id.clone()))
        .add(pending::Column::Generation.eq(row.generation))
        .add(pending::Column::NextAttemptAt.eq(row.next_attempt_at))
        .add(pending::Column::AttemptCount.eq(row.attempt_count));
    match row.claim_token.as_ref() {
        Some(token) => condition.add(pending::Column::ClaimToken.eq(token.clone())),
        None => condition.add(pending::Column::ClaimToken.is_null()),
    }
}

fn next_attempt_count(row: &pending::Model) -> i64 {
    std::cmp::min(
        row.attempt_count.saturating_add(1),
        TASK_OCCURRENCE_RECONCILE_MAX_ATTEMPT_COUNT,
    )
}

pub(crate) async fn claim(
    db: &SqliteDatabase,
    candidate: &TaskOccurrenceReconcileCandidate,
    token: String,
    clock: &TaskOccurrenceClock<'_>,
) -> std::result::Result<Option<TaskOccurrenceReconcileClaim>, FailedClaim> {
    let row = pending::Entity::find_by_id(candidate.run_id.clone())
        .one(db)
        .await
        .map_err(|error| FailedClaim {
            error: error.into(),
            phase: TaskOccurrenceClaimFailurePhase::AdvisoryRead,
            snapshot: None,
        })?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.generation != candidate.generation || row.next_attempt_at > clock() {
        return Ok(None);
    }
    let attempt_count = next_attempt_count(&row);
    let delay = retry_delay(attempt_count);
    let failure = |error: sea_orm::DbErr, phase| FailedClaim {
        error: error.into(),
        phase,
        snapshot: Some(row.clone()),
    };
    let tx = db
        .begin()
        .await
        .map_err(|error| failure(error, TaskOccurrenceClaimFailurePhase::Reservation))?;
    // Admission may have waited arbitrarily long. Only this immediate clock
    // read and bounded arithmetic happen under the writer; delay policy/token
    // were prepared before admission. Each candidate uses its own fresh time.
    let reserved_at = clock();
    let next_attempt_at = reserved_at.saturating_add(delay);
    let update = pending::Entity::update_many()
        .col_expr(pending::Column::ClaimToken, Expr::val(token.clone()))
        .col_expr(pending::Column::AttemptCount, Expr::val(attempt_count))
        .col_expr(pending::Column::NextAttemptAt, Expr::val(next_attempt_at))
        .filter(snapshot_filter(&row))
        .filter(pending::Column::NextAttemptAt.lte(reserved_at));
    let affected = match update.exec(&tx).await {
        Ok(result) => result.rows_affected,
        Err(error) => {
            // Release writer capacity before the separate deferral. Even if
            // rollback fails, its CAS can only change the original snapshot.
            let _ = tx.rollback().await;
            return Err(failure(error, TaskOccurrenceClaimFailurePhase::Reservation));
        }
    };
    tx.commit()
        .await
        .map_err(|error| failure(error, TaskOccurrenceClaimFailurePhase::CommitOutcomeUnknown))?;
    Ok((affected == 1).then_some(TaskOccurrenceReconcileClaim {
        run_id: row.run_id,
        generation: row.generation,
        claim_token: token,
        attempt_count,
        next_attempt_at,
    }))
}

pub(crate) async fn defer_failed_claim(
    db: &SqliteDatabase,
    snapshot: &pending::Model,
    clock: &TaskOccurrenceClock<'_>,
) -> Result<TaskOccurrenceClaimDeferral> {
    let attempt_count = next_attempt_count(snapshot);
    let delay = retry_delay(attempt_count);
    let tx = db.begin().await?;
    let reserved_at = clock();
    // Deliberately do not SET claim_token. A candidate-specific trigger may
    // reject token writes while ordinary due/count bookkeeping still works.
    // Exact snapshot CAS also makes ambiguous successful claim commits safe:
    // their new token/due/count cannot match, so no new holder is postponed.
    let affected = pending::Entity::update_many()
        .col_expr(pending::Column::AttemptCount, Expr::val(attempt_count))
        .col_expr(
            pending::Column::NextAttemptAt,
            Expr::val(reserved_at.saturating_add(delay)),
        )
        .filter(snapshot_filter(snapshot))
        .filter(pending::Column::NextAttemptAt.lte(reserved_at))
        .exec(&tx)
        .await?
        .rows_affected;
    // No retry with a refreshed snapshot, including an ambiguous commit here.
    tx.commit().await?;
    Ok(if affected == 1 {
        TaskOccurrenceClaimDeferral::Deferred
    } else {
        TaskOccurrenceClaimDeferral::StateChanged
    })
}

// One shared 64-input quantum: 16 scope inputs, 16 seed inputs, 32 run
// locators. The scope/seed checkpoint itself consumes one input; pages have
// no lookahead. Full pages finish on the next bounded observation.
pub const TASK_OCCURRENCE_SCOPE_QUOTA: u64 = 16;
pub const TASK_OCCURRENCE_SEED_QUOTA: u64 = 16;
pub const TASK_OCCURRENCE_RUN_QUOTA: u64 = 32;
const UNFINISHED: [&str; 5] = ["queued", "starting", "running", "waiting", "waiting_review"];

pub(crate) async fn next_generation<C: ConnectionTrait>(db: &C) -> Result<i64> {
    let row = sequence::Entity::find_by_id(1)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("task occurrence sequence missing"))?;
    let generation = row
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("task occurrence generation exhausted"))?;
    let updated = sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(generation))
        .filter(sequence::Column::Singleton.eq(1))
        .filter(sequence::Column::Generation.eq(row.generation))
        .exec(db)
        .await?;
    anyhow::ensure!(
        updated.rows_affected == 1,
        "task occurrence sequence changed"
    );
    Ok(generation)
}

async fn enqueue<C: ConnectionTrait>(db: &C, ids: &[String], now: i64) -> Result<()> {
    for id in ids {
        let generation = next_generation(db).await?;
        pending::Entity::insert(pending::ActiveModel {
            run_id: Set(id.clone()),
            generation: Set(generation),
            next_attempt_at: Set(now),
            attempt_count: Set(0),
            claim_token: Set(None),
        })
        .on_conflict(
            OnConflict::column(pending::Column::RunId)
                // Source/scope refresh invalidates the handler, preserves retry delay.
                .update_columns([pending::Column::Generation, pending::Column::ClaimToken])
                .to_owned(),
        )
        .exec_without_returning(db)
        .await?;
    }
    Ok(())
}

pub(crate) fn scope_page_query(row: &scope::Model, limit: u64) -> SelectStatement {
    let mut query = Query::select();
    query
        .column(task_run::Column::Id)
        .from(task_run::Entity)
        .and_where(Expr::col(task_run::Column::TaskId).eq(row.task_id.clone()))
        .and_where(Expr::col(task_run::Column::Id).lte(row.upper_run_id.clone()))
        .order_by(task_run::Column::Id, Order::Asc)
        .limit(limit);
    if let Some(after) = &row.after_run_id {
        query.and_where(Expr::col(task_run::Column::Id).gt(after.clone()));
    }
    query.to_owned()
}

#[derive(FromQueryResult)]
struct RunId {
    id: String,
}

pub(crate) async fn expand_scope(
    db: &SqliteDatabase,
    clock: &TaskOccurrenceClock<'_>,
) -> Result<u64> {
    let row = scope::Entity::find()
        .order_by_asc(scope::Column::Generation)
        .order_by_asc(scope::Column::TaskId)
        .one(db)
        .await?;
    let Some(row) = row else {
        return Ok(0);
    };
    let result = expand_scope_page(db, &row, clock).await;
    if result.is_err() {
        // A locator-specific enqueue failure must not hold every later Task
        // scope behind this one. Rotate only the exact failed checkpoint; an
        // unknown successful commit or source refresh cannot match it.
        let tx = db.begin().await?;
        if scope::Entity::find_by_id(row.task_id.clone())
            .one(&tx)
            .await?
            .as_ref()
            == Some(&row)
        {
            let generation = next_generation(&tx).await?;
            scope::Entity::update_many()
                .col_expr(scope::Column::Generation, Expr::val(generation))
                .filter(scope::Column::TaskId.eq(row.task_id.clone()))
                .filter(scope::Column::Generation.eq(row.generation))
                .exec(&tx)
                .await?;
        }
        tx.commit().await?;
    }
    result
}

async fn expand_scope_page(
    db: &SqliteDatabase,
    row: &scope::Model,
    clock: &TaskOccurrenceClock<'_>,
) -> Result<u64> {
    let tx = db.begin().await?;
    let now = clock();
    let result = async {
        // Fence a source refresh while waiting for writer admission.
        let current = scope::Entity::find_by_id(row.task_id.clone())
            .filter(scope::Column::Generation.eq(row.generation))
            .one(&tx)
            .await?;
        if current.as_ref() != Some(row) {
            return Ok(1);
        }
        let limit = TASK_OCCURRENCE_SCOPE_QUOTA - 1;
        let ids = if row.upper_run_id.is_some() {
            RunId::find_by_statement(
                tx.get_database_backend()
                    .build(&scope_page_query(row, limit)),
            )
            .all(&tx)
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        enqueue(&tx, &ids, now).await?;
        if ids.len() < limit as usize {
            scope::Entity::delete_many()
                .filter(scope::Column::TaskId.eq(row.task_id.clone()))
                .filter(scope::Column::Generation.eq(row.generation))
                .exec(&tx)
                .await?;
        } else {
            // Rotate successful pages behind other scopes. The unique generation
            // also fences another handler of this prefix, without a second cursor.
            let generation = next_generation(&tx).await?;
            scope::Entity::update_many()
                .col_expr(scope::Column::AfterRunId, Expr::val(ids.last().cloned()))
                .col_expr(scope::Column::Generation, Expr::val(generation))
                .filter(scope::Column::TaskId.eq(row.task_id.clone()))
                .filter(scope::Column::Generation.eq(row.generation))
                .exec(&tx)
                .await?;
        }
        Ok(1 + ids.len() as u64)
    }
    .await;
    match result {
        Ok(consumed) => {
            tx.commit().await?;
            Ok(consumed)
        }
        Err(error) => {
            // Release the writer/rollback the prefix before conditional rotation.
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

pub(crate) fn seed_page_query(row: &seed::Model, limit: u64) -> SelectStatement {
    let mut query = Query::select();
    query
        .column(task_run::Column::Id)
        .from(task_run::Entity)
        .and_where(Expr::col(task_run::Column::Status).eq(UNFINISHED[row.status_index as usize]))
        .and_where(Expr::col(task_run::Column::Id).lte(row.upper_run_id.clone()))
        .order_by(task_run::Column::Id, Order::Asc)
        .limit(limit);
    if let Some(after) = &row.after_run_id {
        query.and_where(Expr::col(task_run::Column::Id).gt(after.clone()));
    }
    query.to_owned()
}

pub(crate) async fn seed_unfinished(
    db: &SqliteDatabase,
    clock: &TaskOccurrenceClock<'_>,
) -> Result<u64> {
    let row = seed::Entity::find_by_id(1)
        .one(db)
        .await?
        .ok_or_else(|| anyhow::anyhow!("task occurrence seed missing"))?;
    if row.status_index == 5 {
        return Ok(0);
    }
    let tx = db.begin().await?;
    let now = clock();
    let current = seed::Entity::find_by_id(1).one(&tx).await?;
    if current.as_ref() != Some(&row) {
        tx.commit().await?;
        return Ok(1);
    }
    let limit = TASK_OCCURRENCE_SEED_QUOTA - 1;
    let ids = if row.upper_run_id.is_some() {
        RunId::find_by_statement(
            tx.get_database_backend()
                .build(&seed_page_query(&row, limit)),
        )
        .all(&tx)
        .await?
        .into_iter()
        .map(|row| row.id)
        .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    enqueue(&tx, &ids, now).await?;
    let full_page = ids.len() == limit as usize;
    seed::Entity::update_many()
        .col_expr(
            seed::Column::StatusIndex,
            Expr::val(if full_page {
                row.status_index
            } else {
                row.status_index + 1
            }),
        )
        .col_expr(
            seed::Column::AfterRunId,
            Expr::val(if full_page { ids.last().cloned() } else { None }),
        )
        .filter(seed::Column::Singleton.eq(1))
        .exec(&tx)
        .await?;
    tx.commit().await?;
    Ok(1 + ids.len() as u64)
}
