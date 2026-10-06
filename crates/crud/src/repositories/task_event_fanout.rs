//! Durable frontier for the single Gateway dispatcher. No payloads in discovery.
use super::read_model_repair as checkpoint;
use anyhow::{Result, bail};
use pioneer_entity::{
    task_event, task_event_fanout_cursor as cursor, task_event_fanout_pending as pending,
    task_event_fanout_sequence as sequence,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionTrait,
};

pub const TASK_EVENT_FANOUT_TASK_BUDGET: u64 = 64;
pub const TASK_EVENT_FANOUT_EVENT_BUDGET: usize = 128;
pub const TASK_EVENT_FANOUT_BYTE_BUDGET: usize = 1024 * 1024;
const REPAIR_KEY: &str = "task_event_fanout_frontier";
const VERSION: i64 = 1;
/// Raw repository rows and decoded CRUD events share the same page boundary.
#[derive(Debug)]
pub enum TaskEventFanoutPage<T> {
    Prefix { events: Vec<T>, bytes: usize },
    BudgetDeferred,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskEventFanoutOutcome {
    Delivered,
    BudgetDeferred,
    Failed,
}
#[derive(Clone, Debug)]
pub struct TaskEventFanoutClaim {
    pub task_id: String,
    pub generation: i64,
    pub token: String,
    pub retry_at: i64,
    prior_due_at: i64,
    prior_attempts: i64,
}

fn snapshot(row: &pending::Model) -> sea_orm::Condition {
    let c = sea_orm::Condition::all()
        .add(pending::Column::TaskId.eq(row.task_id.clone()))
        .add(pending::Column::Generation.eq(row.generation))
        .add(pending::Column::NewestSequence.eq(row.newest_sequence))
        .add(pending::Column::DueAt.eq(row.due_at))
        .add(pending::Column::Attempts.eq(row.attempts));
    match &row.claim_token {
        Some(t) => c.add(pending::Column::ClaimToken.eq(t.clone())),
        None => c.add(pending::Column::ClaimToken.is_null()),
    }
}
fn holder(claim: &TaskEventFanoutClaim) -> sea_orm::Condition {
    sea_orm::Condition::all()
        .add(pending::Column::TaskId.eq(claim.task_id.clone()))
        .add(pending::Column::ClaimToken.eq(claim.token.clone()))
}
fn retry(attempts: i64) -> i64 {
    (5_i64.saturating_mul(1_i64 << (attempts - 1).clamp(0, 15))).min(300)
}
pub(crate) async fn due<C: ConnectionTrait>(
    db: &C,
    now: i64,
    limit: u64,
) -> Result<Vec<pending::Model>> {
    Ok(pending::Entity::find()
        .filter(pending::Column::DueAt.lte(now))
        .order_by_asc(pending::Column::DueAt)
        .order_by_asc(pending::Column::Generation)
        .order_by_asc(pending::Column::TaskId)
        .limit(limit.min(64))
        .all(db)
        .await?)
}
pub(crate) async fn has_pending<C: ConnectionTrait>(db: &C) -> Result<bool> {
    Ok(pending::Entity::find()
        .select_only()
        .column(pending::Column::TaskId)
        .limit(1)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}
/// Reserve before preparation. Failed reservation defers only the complete old
/// snapshot; ambiguous commit never permits emission or another claim.
pub(crate) async fn claim(
    db: &SqliteDatabase,
    row: &pending::Model,
    token: String,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<Option<TaskEventFanoutClaim>> {
    let attempts = row.attempts.saturating_add(1).min(16);
    let delay = retry(attempts);
    let tx = match db.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            defer(db, row, attempts, delay, clock).await?;
            return Err(e.into());
        }
    };
    let now = clock();
    let retry_at = now.saturating_add(delay);
    let result = pending::Entity::update_many()
        .col_expr(pending::Column::ClaimToken, Expr::val(token.clone()))
        .col_expr(pending::Column::Attempts, Expr::val(attempts))
        .col_expr(pending::Column::DueAt, Expr::val(retry_at))
        .filter(snapshot(row))
        .filter(pending::Column::DueAt.lte(now))
        .exec(&tx)
        .await;
    let affected = match result {
        Ok(r) => r.rows_affected,
        Err(e) => {
            let _ = tx.rollback().await;
            defer(db, row, attempts, delay, clock).await?;
            return Err(e.into());
        }
    };
    tx.commit().await?;
    Ok((affected == 1).then_some(TaskEventFanoutClaim {
        task_id: row.task_id.clone(),
        generation: row.generation,
        token,
        retry_at,
        prior_due_at: row.due_at,
        prior_attempts: row.attempts,
    }))
}
async fn defer(
    db: &SqliteDatabase,
    row: &pending::Model,
    attempts: i64,
    delay: i64,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<()> {
    let tx = db.begin().await?;
    let now = clock();
    pending::Entity::update_many()
        .col_expr(pending::Column::Attempts, Expr::val(attempts))
        .col_expr(pending::Column::DueAt, Expr::val(now.saturating_add(delay)))
        .filter(snapshot(row))
        .filter(pending::Column::DueAt.lte(now))
        .exec(&tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
/// Appends keep the token. Generation fences errors, not successful immutable
/// prefixes. Renewal is only for this active Task, never another candidate.
pub(crate) async fn renew(
    db: &SqliteDatabase,
    claim: &TaskEventFanoutClaim,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<bool> {
    let tx = db.begin().await?;
    let now = clock();
    let n = pending::Entity::update_many()
        .col_expr(
            pending::Column::DueAt,
            sea_orm::sea_query::Func::cust(sea_orm::sea_query::Alias::new("max"))
                .args([
                    Expr::col(pending::Column::DueAt),
                    Expr::val(now.saturating_add(5)),
                ])
                .into(),
        )
        .filter(holder(claim))
        .exec(&tx)
        .await?
        .rows_affected;
    tx.commit().await?;
    Ok(n == 1)
}
pub(crate) async fn ack(
    db: &SqliteDatabase,
    claim: &TaskEventFanoutClaim,
    sequence: i64,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<()> {
    let tx = db.begin().await?;
    let now = clock();
    // This update intentionally has no generation/token predicate. A successful
    // selected prefix remains processed even when a new append changes frontier.
    let advanced = cursor::Entity::update_many()
        .col_expr(cursor::Column::LastSequence, Expr::val(sequence))
        .col_expr(
            cursor::Column::UpdatedAt,
            Expr::val(crate::util::unix_to_datetime(now)),
        )
        .filter(cursor::Column::TaskId.eq(claim.task_id.clone()))
        .filter(cursor::Column::LastSequence.lt(sequence))
        .exec(&tx)
        .await?
        .rows_affected;
    if advanced == 1 {
        pending::Entity::update_many()
            .col_expr(pending::Column::Attempts, Expr::val(0_i64))
            .col_expr(pending::Column::DueAt, Expr::val(now))
            .filter(holder(claim))
            .exec(&tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}
pub(crate) async fn release(
    db: &SqliteDatabase,
    claim: &TaskEventFanoutClaim,
    outcome: TaskEventFanoutOutcome,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<()> {
    let tx = db.begin().await?;
    let now = clock();
    let mut update = pending::Entity::update_many()
        .col_expr(
            pending::Column::ClaimToken,
            Expr::val(Option::<String>::None),
        )
        .filter(holder(claim));
    // Budget is admission, not an error. Undo only this unconsumed reservation:
    // no renewal/ACK/error may have changed its bookkeeping. Appends retain the
    // active token and these fields, so they do not defeat budget fairness.
    // A replacement holder or cursor reset changes/clears the unique token.
    if outcome == TaskEventFanoutOutcome::BudgetDeferred {
        update = update
            .filter(pending::Column::DueAt.eq(claim.retry_at))
            .filter(pending::Column::Attempts.eq(claim.prior_attempts.saturating_add(1).min(16)))
            .col_expr(pending::Column::DueAt, Expr::val(claim.prior_due_at))
            .col_expr(pending::Column::Attempts, Expr::val(claim.prior_attempts));
    } else if outcome == TaskEventFanoutOutcome::Failed {
        let row = pending::Entity::find_by_id(claim.task_id.clone())
            .filter(holder(claim))
            .filter(pending::Column::Generation.eq(claim.generation))
            .one(&tx)
            .await?;
        if let Some(row) = row {
            update = update
                .filter(pending::Column::Generation.eq(claim.generation))
                .col_expr(pending::Column::Attempts, Expr::val(row.attempts.max(1)))
                .col_expr(
                    pending::Column::DueAt,
                    Expr::val(now.saturating_add(retry(row.attempts.max(1)))),
                );
        } else {
            tx.commit().await?;
            return Ok(());
        }
    }
    update.exec(&tx).await?;
    tx.commit().await?;
    Ok(())
}

// Cursor PK pages, no mismatch filtering. Latest sequence is a point seek.
// Enqueue and checkpoint advance share the Maintenance writer transaction.
pub(crate) async fn bootstrap(db: &SqliteDatabase, limit: u64) -> Result<(usize, bool)> {
    let tx = db.begin().await?;
    let mut cp = checkpoint::load_checkpoint(&tx, REPAIR_KEY).await?;
    if cp.is_none() {
        let high = cursor::Entity::find()
            .select_only()
            .column(cursor::Column::TaskId)
            .order_by_desc(cursor::Column::TaskId)
            .limit(1)
            .into_tuple::<String>()
            .one(&tx)
            .await?;
        checkpoint::reset_full_scan(&tx, REPAIR_KEY, VERSION, high.as_deref()).await?;
        cp = checkpoint::load_checkpoint(&tx, REPAIR_KEY).await?;
    }
    let cp = cp.expect("checkpoint inserted");
    if cp.algorithm_version != VERSION {
        bail!("unsupported task fanout bootstrap version");
    }
    if cp.full_scan_status == checkpoint::STATUS_COMPLETED {
        tx.commit().await?;
        return Ok((0, true));
    }
    let Some(high) = cp.full_scan_high_watermark_id else {
        checkpoint::complete_full_scan(&tx, REPAIR_KEY, VERSION).await?;
        tx.commit().await?;
        return Ok((0, true));
    };
    let mut query = cursor::Entity::find().filter(cursor::Column::TaskId.lte(high.clone()));
    if let Some(after) = cp.full_scan_cursor_id {
        query = query.filter(cursor::Column::TaskId.gt(after));
    }
    let rows = query
        .order_by_asc(cursor::Column::TaskId)
        .limit(limit.min(64))
        .all(&tx)
        .await?;
    for row in &rows {
        let latest = task_event::Entity::find()
            .select_only()
            .column(task_event::Column::Sequence)
            .filter(task_event::Column::TaskId.eq(row.task_id.clone()))
            .order_by_desc(task_event::Column::Sequence)
            .limit(1)
            .into_tuple::<i64>()
            .one(&tx)
            .await?;
        if let Some(latest) = latest.filter(|s| *s > row.last_sequence) {
            // Already enqueued by a trigger: keep its holder and retry delay.
            if pending::Entity::find_by_id(row.task_id.clone())
                .one(&tx)
                .await?
                .is_none()
            {
                let generation = sequence::Entity::find_by_id(1_i64)
                    .one(&tx)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("fanout sequence missing"))?
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("fanout generation exhausted"))?;
                sequence::Entity::update_many()
                    .col_expr(sequence::Column::Generation, Expr::val(generation))
                    .exec(&tx)
                    .await?;
                pending::Entity::insert(pending::ActiveModel {
                    task_id: Set(row.task_id.clone()),
                    newest_sequence: Set(latest),
                    generation: Set(generation),
                    due_at: Set(chrono::Utc::now().timestamp()),
                    claim_token: Set(None),
                    attempts: Set(0),
                })
                .exec(&tx)
                .await?;
            }
        }
    }
    let completed =
        rows.len() < (limit.min(64) as usize) || rows.last().is_some_and(|r| r.task_id == high);
    if completed {
        checkpoint::complete_full_scan(&tx, REPAIR_KEY, VERSION).await?;
    } else if let Some(last) = rows.last() {
        checkpoint::advance_full_scan_cursor(&tx, REPAIR_KEY, VERSION, &last.task_id).await?;
    }
    tx.commit().await?;
    Ok((rows.len(), completed))
}
