//! Transactional publishers for child work. Deadlines use Unix seconds.

use anyhow::{Result, ensure};
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use pioneer_protocol::generate_id;
use sea_orm::sea_query::{Alias, Expr, Func, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionSession, TransactionTrait,
};

const POINT_PAGE: u64 = 32;
const POINT_LEASE_SECONDS: i64 = 60;

pub async fn enqueue_point<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    now: i64,
) -> Result<()> {
    point::Entity::insert(point::ActiveModel {
        turn_id: Set(turn_id.to_owned()),
        thread_id: Set(thread_id.to_owned()),
        revision: Set(generate_id(32)),
        next_attempt_at: Set(Some(now)),
        attempt_count: Set(0),
        claim_token: Set(None),
    })
    .on_conflict(
        OnConflict::column(point::Column::TurnId)
            .update_columns([
                point::Column::ThreadId,
                point::Column::Revision,
                point::Column::NextAttemptAt,
                point::Column::AttemptCount,
                point::Column::ClaimToken,
            ])
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(())
}

pub async fn enqueue_scope<C: ConnectionTrait>(
    db: &C,
    kind: &str,
    key: &str,
    now: i64,
) -> Result<()> {
    let deadline = Expr::col((scope::Entity, scope::Column::NextAttemptAt));
    let requested_deadline = Expr::col((Alias::new("excluded"), scope::Column::NextAttemptAt));
    // Keep an active claim's deadline. Otherwise wake a parked pass or advance
    // a deferred pass. PostgreSQL LEAST picks the present argument for NULL.
    let next_deadline = Expr::case(scope::Column::ClaimToken.is_not_null(), deadline.clone())
        .finally(Func::cust(Alias::new("least")).args([deadline, requested_deadline]));
    scope::Entity::insert(scope::ActiveModel {
        scope_kind: Set(kind.to_owned()),
        scope_key: Set(key.to_owned()),
        incarnation_token: Set(generate_id(32)),
        requested_revision: Set(generate_id(32)),
        pass_revision: Set(None),
        cursor: Set(None),
        upper_watermark: Set(None),
        next_attempt_at: Set(Some(now)),
        attempt_count: Set(0),
        claim_token: Set(None),
    })
    .on_conflict(
        OnConflict::columns([scope::Column::ScopeKind, scope::Column::ScopeKey])
            .update_column(scope::Column::RequestedRevision)
            .value(scope::Column::NextAttemptAt, next_deadline)
            .to_owned(),
    )
    .exec_without_returning(db)
    .await?;
    Ok(())
}

pub fn point_fence(row: &point::Model) -> sea_orm::Condition {
    let mut c = sea_orm::Condition::all()
        .add(point::Column::TurnId.eq(&row.turn_id))
        .add(point::Column::Revision.eq(&row.revision));
    c = if let Some(t) = &row.claim_token {
        c.add(point::Column::ClaimToken.eq(t))
    } else {
        c.add(point::Column::ClaimToken.is_null())
    };
    if let Some(due) = row.next_attempt_at {
        c.add(point::Column::NextAttemptAt.eq(due))
    } else {
        c.add(point::Column::NextAttemptAt.is_null())
    }
}
pub async fn discover_child_points(
    db: &impl ConnectionTrait,
    now: i64,
    limit: u64,
) -> Result<Vec<point::Model>> {
    discover_child_points_after(db, now, limit, None).await
}
/// Rotate through the indexed due set even when a poison head cannot be
/// deferred. The caller retains this advisory cursor only, never ownership.
pub async fn discover_child_points_after(
    db: &impl ConnectionTrait,
    now: i64,
    limit: u64,
    after: Option<(i64, &str)>,
) -> Result<Vec<point::Model>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let query = point::Entity::find()
        .filter(point::Column::NextAttemptAt.lte(now))
        .order_by_asc(point::Column::NextAttemptAt)
        .order_by_asc(point::Column::TurnId)
        .limit(limit.min(POINT_PAGE));
    if let Some((due, id)) = after {
        let rows = query
            .clone()
            .filter(
                sea_orm::Condition::any()
                    .add(point::Column::NextAttemptAt.gt(due))
                    .add(
                        sea_orm::Condition::all()
                            .add(point::Column::NextAttemptAt.eq(due))
                            .add(point::Column::TurnId.gt(id)),
                    ),
            )
            .all(db)
            .await?;
        if !rows.is_empty() {
            return Ok(rows);
        }
    }
    Ok(query.all(db).await?)
}
/// Finite advisory pass boundary. No ownership or writer admission.
pub async fn child_point_due_upper(
    db: &impl ConnectionTrait,
    now: i64,
) -> Result<Option<(i64, String)>> {
    Ok(point::Entity::find()
        .select_only()
        .columns([point::Column::NextAttemptAt, point::Column::TurnId])
        .filter(point::Column::NextAttemptAt.lte(now))
        .order_by_desc(point::Column::NextAttemptAt)
        .order_by_desc(point::Column::TurnId)
        .into_tuple::<(i64, String)>()
        .one(db)
        .await?)
}
/// An empty suffix ends this pass; it never silently wraps into new growth.
pub async fn discover_child_points_in_pass(
    db: &impl ConnectionTrait,
    now: i64,
    limit: u64,
    after: Option<&(i64, String)>,
    upper: &(i64, String),
) -> Result<Vec<point::Model>> {
    let mut query = point::Entity::find()
        .filter(point::Column::NextAttemptAt.lte(now))
        .filter(
            sea_orm::Condition::any()
                .add(point::Column::NextAttemptAt.lt(upper.0))
                .add(
                    sea_orm::Condition::all()
                        .add(point::Column::NextAttemptAt.eq(upper.0))
                        .add(point::Column::TurnId.lte(&upper.1)),
                ),
        );
    if let Some((due, id)) = after {
        query = query.filter(
            sea_orm::Condition::any()
                .add(point::Column::NextAttemptAt.gt(*due))
                .add(
                    sea_orm::Condition::all()
                        .add(point::Column::NextAttemptAt.eq(*due))
                        .add(point::Column::TurnId.gt(id)),
                ),
        );
    }
    Ok(query
        .order_by_asc(point::Column::NextAttemptAt)
        .order_by_asc(point::Column::TurnId)
        .limit(limit.min(POINT_PAGE))
        .all(db)
        .await?)
}
/// Exact-snapshot independent deferral after claim/ACK failure. An unknown
/// committed claim has a different token/deadline and cannot be cleared by
/// its old preclaim snapshot. No row is inserted/recreated by this helper.
pub async fn defer_child_point_snapshot<F: Fn() -> i64 + Sync>(
    db: &(impl ConnectionTrait + TransactionTrait),
    snapshot: &point::Model,
    clock: &F,
) -> Result<bool> {
    let tx = db.begin().await?;
    let due = clock().saturating_add(2);
    let changed = point::Entity::update_many()
        .filter(point_fence(snapshot))
        .col_expr(point::Column::NextAttemptAt, Expr::value(due))
        .col_expr(
            point::Column::ClaimToken,
            Expr::value(Option::<String>::None),
        )
        .exec(&tx)
        .await?
        .rows_affected;
    tx.commit().await?;
    Ok(changed == 1)
}
pub async fn claim_child_point(
    db: &(impl ConnectionTrait + TransactionTrait),
    candidate: &point::Model,
    now: i64,
) -> Result<Option<point::Model>> {
    claim_child_point_with_clock(db, candidate, &|| now).await
}
pub async fn claim_child_point_with_clock<F: Fn() -> i64 + Sync>(
    db: &(impl ConnectionTrait + TransactionTrait),
    candidate: &point::Model,
    clock: &F,
) -> Result<Option<point::Model>> {
    if candidate.next_attempt_at.is_none_or(|due| due > clock()) {
        return Ok(None);
    }
    let claim = generate_id(32);
    let tx = db.begin().await?;
    let reserved_at = clock();
    let until = reserved_at.saturating_add(POINT_LEASE_SECONDS);
    let affected = point::Entity::update_many()
        .filter(point_fence(candidate))
        .filter(point::Column::NextAttemptAt.lte(reserved_at))
        .col_expr(point::Column::ClaimToken, Expr::value(&claim))
        .col_expr(point::Column::NextAttemptAt, Expr::value(until))
        .col_expr(
            point::Column::AttemptCount,
            Expr::value(candidate.attempt_count.saturating_add(1)),
        )
        .exec(&tx)
        .await?
        .rows_affected;
    tx.commit().await?;
    Ok((affected == 1).then(|| {
        let mut row = candidate.clone();
        row.claim_token = Some(claim);
        row.next_attempt_at = Some(until);
        row.attempt_count = row.attempt_count.saturating_add(1);
        row
    }))
}
/// None parks a proven durable dependency. Runtime-local waits use a timed
/// retry. An ACK from an older producer revision cannot remove new work.
pub async fn finish_child_point(
    db: &impl ConnectionTrait,
    claim: &point::Model,
    retry_at: Option<i64>,
    done: bool,
) -> Result<bool> {
    ensure!(claim.claim_token.is_some(), "unclaimed child ACK");
    let rows = if done {
        point::Entity::delete_many()
            .filter(point_fence(claim))
            .exec(db)
            .await?
            .rows_affected
    } else {
        point::Entity::update_many()
            .filter(point_fence(claim))
            .col_expr(
                point::Column::ClaimToken,
                Expr::value(Option::<String>::None),
            )
            .col_expr(point::Column::NextAttemptAt, Expr::value(retry_at))
            .exec(db)
            .await?
            .rows_affected
    };
    Ok(rows == 1)
}
