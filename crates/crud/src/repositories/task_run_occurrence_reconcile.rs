//! Exact-row tracking operations. The caller supplies scoped handles; claim
//! bookkeeping never borrows the critical scope of the domain repair.
use anyhow::Result;
use pioneer_entity::{task_run, task_run_occurrence_reconcile_pending as pending, turn, workspace};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, Order, Query, SelectStatement, SimpleExpr};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, TransactionTrait,
};

pub const OCCURRENCE_RECONCILE_BUDGET: u64 = 64;
// Policy choices, not measured timings: tolerate short fanout gaps, then keep
// poison candidates retryable without polling them on every two-second pass.
pub const OCCURRENCE_RECONCILE_INITIAL_BACKOFF_SECS: i64 = 5;
pub const OCCURRENCE_RECONCILE_MAX_BACKOFF_SECS: i64 = 300;
pub const OCCURRENCE_RECONCILE_MAX_ATTEMPT_COUNT: i32 = 16;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct TaskRunOccurrenceReconcileCandidate {
    pub run_id: String,
    pub generation: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskRunOccurrenceReconcileClaim {
    pub run_id: String,
    pub generation: i64,
    pub claim_token: String,
    pub attempt_count: i32,
    pub next_attempt_at: i64,
}

/// The clock is read after writer admission. Implementations must be immediate,
/// nonblocking clock reads, with no I/O or database work.
pub type TaskRunOccurrenceClock<'a> = dyn Fn() -> i64 + Send + Sync + 'a;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskRunOccurrenceClaimFailurePhase {
    AdvisoryRead,
    Reservation,
    CommitOutcomeUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskRunOccurrenceClaimDeferral {
    NoSnapshot,
    Deferred,
    StateChanged,
    Failed,
}

#[derive(Debug)]
pub struct TaskRunOccurrenceClaimFailure {
    pub phase: TaskRunOccurrenceClaimFailurePhase,
    pub deferral: TaskRunOccurrenceClaimDeferral,
    pub error: anyhow::Error,
    pub deferral_error: Option<anyhow::Error>,
}

impl std::fmt::Display for TaskRunOccurrenceClaimFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TaskRun occurrence claim failed")
    }
}

impl std::error::Error for TaskRunOccurrenceClaimFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

pub(crate) struct FailedClaim {
    pub error: anyhow::Error,
    pub phase: TaskRunOccurrenceClaimFailurePhase,
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
    Ok(db
        .query_one_raw(db.get_database_backend().build(&query))
        .await?
        .is_some())
}

pub(crate) fn due_query(now: i64, limit: u64) -> SelectStatement {
    Query::select()
        .columns([pending::Column::RunId, pending::Column::Generation])
        .from(pending::Entity)
        .and_where(Expr::col(pending::Column::NextAttemptAt).lte(now))
        .order_by(pending::Column::NextAttemptAt, Order::Asc)
        .order_by(pending::Column::Generation, Order::Asc)
        .order_by(pending::Column::RunId, Order::Asc)
        .limit(std::cmp::min(limit, OCCURRENCE_RECONCILE_BUDGET))
        .to_owned()
}

pub(crate) async fn discover<C: ConnectionTrait>(
    db: &C,
    now: i64,
    limit: u64,
) -> Result<Vec<TaskRunOccurrenceReconcileCandidate>> {
    Ok(TaskRunOccurrenceReconcileCandidate::find_by_statement(
        db.get_database_backend().build(&due_query(now, limit)),
    )
    .all(db)
    .await?)
}

fn mismatch_query(run_id: &str) -> SelectStatement {
    let r = |column| Expr::col((task_run::Entity, column));
    let t = |column| Expr::col((turn::Entity, column));
    Query::select()
        .expr(Expr::val(1))
        .from(task_run::Entity)
        .join(
            JoinType::InnerJoin,
            turn::Entity,
            r(task_run::Column::Id).eq(t(turn::Column::Id)),
        )
        .and_where(r(task_run::Column::Id).eq(run_id))
        .and_where(t(turn::Column::TurnKind).eq("task_run"))
        .and_where(mismatch_status_predicate(
            r(task_run::Column::Status).into(),
            t(turn::Column::Status).into(),
        ))
        .to_owned()
}

// A separate expression keeps NULL/unknown semantics testable without
// weakening the production tables' NOT NULL constraints.
pub(crate) fn mismatch_status_predicate(run: SimpleExpr, turn: SimpleExpr) -> SimpleExpr {
    run.clone()
        .eq("succeeded")
        .and(turn.clone().ne("completed"))
        .or(run
            .clone()
            .is_in(["failed", "timed_out"])
            .and(turn.clone().ne("failed")))
        .or(run.clone().eq("blocked").and(turn.clone().ne("blocked")))
        .or(run.eq("cancelled").and(turn.ne("interrupted")))
}

#[cfg(test)]
pub(crate) async fn is_mismatch<C: ConnectionTrait>(db: &C, run_id: &str) -> Result<bool> {
    Ok(db
        .query_one_raw(db.get_database_backend().build(&mismatch_query(run_id)))
        .await?
        .is_some())
}

pub(crate) async fn workspace_exists<C: ConnectionTrait>(db: &C, id: &str) -> Result<bool> {
    Ok(workspace::Entity::find_by_id(id.to_owned())
        .one(db)
        .await?
        .is_some())
}

pub(crate) async fn owns<C: ConnectionTrait>(
    db: &C,
    claim: &TaskRunOccurrenceReconcileClaim,
) -> Result<bool> {
    Ok(pending::Entity::find_by_id(claim.run_id.clone())
        .filter(pending::Column::Generation.eq(claim.generation))
        .filter(pending::Column::ClaimToken.eq(claim.claim_token.clone()))
        .one(db)
        .await?
        .is_some())
}

pub(crate) async fn remove_if_consistent<C: ConnectionTrait>(
    db: &C,
    claim: &TaskRunOccurrenceReconcileClaim,
) -> Result<()> {
    pending::Entity::delete_many()
        .filter(pending::Column::RunId.eq(claim.run_id.clone()))
        .filter(pending::Column::Generation.eq(claim.generation))
        .filter(pending::Column::ClaimToken.eq(claim.claim_token.clone()))
        .filter(Expr::exists(mismatch_query(&claim.run_id)).not())
        .exec(db)
        .await?;
    Ok(())
}

pub(crate) fn retry_delay(attempt: i32) -> i64 {
    std::cmp::min(
        OCCURRENCE_RECONCILE_INITIAL_BACKOFF_SECS
            .saturating_mul(1_i64 << (attempt.saturating_sub(1).clamp(0, 15) as u32)),
        OCCURRENCE_RECONCILE_MAX_BACKOFF_SECS,
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

fn next_attempt_count(row: &pending::Model) -> i32 {
    std::cmp::min(
        row.attempt_count.saturating_add(1),
        OCCURRENCE_RECONCILE_MAX_ATTEMPT_COUNT,
    )
}

pub(crate) async fn claim(
    db: &SqliteDatabase,
    candidate: &TaskRunOccurrenceReconcileCandidate,
    token: String,
    clock: &TaskRunOccurrenceClock<'_>,
) -> std::result::Result<Option<TaskRunOccurrenceReconcileClaim>, FailedClaim> {
    let row = pending::Entity::find_by_id(candidate.run_id.clone())
        .one(db)
        .await
        .map_err(|error| FailedClaim {
            error: error.into(),
            phase: TaskRunOccurrenceClaimFailurePhase::AdvisoryRead,
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
        .map_err(|error| failure(error, TaskRunOccurrenceClaimFailurePhase::Reservation))?;
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
            return Err(failure(
                error,
                TaskRunOccurrenceClaimFailurePhase::Reservation,
            ));
        }
    };
    tx.commit().await.map_err(|error| {
        failure(
            error,
            TaskRunOccurrenceClaimFailurePhase::CommitOutcomeUnknown,
        )
    })?;
    Ok((affected == 1).then_some(TaskRunOccurrenceReconcileClaim {
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
    clock: &TaskRunOccurrenceClock<'_>,
) -> Result<TaskRunOccurrenceClaimDeferral> {
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
        TaskRunOccurrenceClaimDeferral::Deferred
    } else {
        TaskRunOccurrenceClaimDeferral::StateChanged
    })
}
