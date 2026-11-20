//! Transactional publishers for child work. Deadlines use Unix seconds.

use anyhow::Result;
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use pioneer_protocol::generate_id;
use sea_orm::sea_query::{Alias, Expr, Func, OnConflict};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, Set};

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
