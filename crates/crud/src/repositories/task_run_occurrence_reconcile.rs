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

pub(crate) async fn claim(
    db: &SqliteDatabase,
    candidate: &TaskRunOccurrenceReconcileCandidate,
    token: String,
    now: i64,
) -> Result<Option<TaskRunOccurrenceReconcileClaim>> {
    // Fetch and compute the backoff before acquiring the writer. The complete
    // advisory row is fenced by the conditional update in the short transaction.
    let Some(row) = pending::Entity::find_by_id(candidate.run_id.clone())
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    if row.generation != candidate.generation || row.next_attempt_at > now {
        return Ok(None);
    }
    let attempt_count = std::cmp::min(
        row.attempt_count.saturating_add(1),
        OCCURRENCE_RECONCILE_MAX_ATTEMPT_COUNT,
    );
    let next_attempt_at = now.saturating_add(retry_delay(attempt_count));
    let claim = TaskRunOccurrenceReconcileClaim {
        run_id: row.run_id.clone(),
        generation: row.generation,
        claim_token: token,
        attempt_count,
        next_attempt_at,
    };
    let tx = db.begin().await?;
    let mut update = pending::Entity::update_many()
        .col_expr(
            pending::Column::ClaimToken,
            Expr::val(claim.claim_token.clone()),
        )
        .col_expr(pending::Column::AttemptCount, Expr::val(attempt_count))
        .col_expr(pending::Column::NextAttemptAt, Expr::val(next_attempt_at))
        .filter(pending::Column::RunId.eq(row.run_id))
        .filter(pending::Column::Generation.eq(row.generation))
        .filter(pending::Column::NextAttemptAt.eq(row.next_attempt_at))
        .filter(pending::Column::NextAttemptAt.lte(now))
        .filter(pending::Column::AttemptCount.eq(row.attempt_count));
    update = match row.claim_token {
        Some(token) => update.filter(pending::Column::ClaimToken.eq(token)),
        None => update.filter(pending::Column::ClaimToken.is_null()),
    };
    let affected = update.exec(&tx).await?.rows_affected;
    tx.commit().await?;
    Ok((affected == 1).then_some(claim))
}
