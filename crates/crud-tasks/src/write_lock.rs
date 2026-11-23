use anyhow::{Context, Result};
use pioneer_entity::task_write_lock;
use pioneer_protocol::{TaskWriteLock, TaskWriteLockStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::extension::postgres::PgExpr;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Func, OnConflict, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, Set,
};

use crate::status::{
    task_concurrency_conflict_policy_from_db, task_concurrency_conflict_policy_to_db,
    task_write_lock_scope_kind_from_db, task_write_lock_scope_kind_to_db,
    task_write_lock_status_from_db, task_write_lock_status_to_db,
};
use crate::values::unix_to_datetime;

pub async fn upsert_lock<C: ConnectionTrait>(db: &C, lock: &TaskWriteLock) -> Result<()> {
    task_write_lock::Entity::insert(active_model_from_lock(lock))
        .on_conflict(
            OnConflict::column(task_write_lock::Column::Id)
                .update_columns([
                    task_write_lock::Column::WorkspaceId,
                    task_write_lock::Column::TaskId,
                    task_write_lock::Column::RunId,
                    task_write_lock::Column::ScopeKind,
                    task_write_lock::Column::ScopePath,
                    task_write_lock::Column::Status,
                    task_write_lock::Column::AcquiredAt,
                    task_write_lock::Column::ExpiresAt,
                    task_write_lock::Column::ReleasedAt,
                    task_write_lock::Column::ConflictPolicy,
                    task_write_lock::Column::Reason,
                    task_write_lock::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task write lock")?;
    Ok(())
}

pub async fn list_locks_by_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Vec<task_write_lock::Model>> {
    task_write_lock::Entity::find()
        .filter(task_write_lock::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_write_lock::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task write locks by task")
}

pub async fn list_locks_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_write_lock::Model>> {
    task_write_lock::Entity::find()
        .filter(task_write_lock::Column::RunId.eq(run_id.to_owned()))
        .order_by_asc(task_write_lock::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list task write locks by run")
}

/// Terminal batches are bounded by the atomic event quantum. Read one extra
/// lock to reject an oversized transition before reserving the writer.
pub async fn list_terminal_locks_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Vec<task_write_lock::Model>> {
    let rows = task_write_lock::Entity::find()
        .filter(task_write_lock::Column::RunId.eq(run_id.to_owned()))
        .filter(task_write_lock::Column::Status.is_in(["pending", "acquired", "blocked"]))
        .order_by_asc(task_write_lock::Column::CreatedAt)
        .limit(65)
        .all(db)
        .await?;
    if rows.len() > 64 {
        anyhow::bail!("terminal task transition exceeds its lock quantum");
    }
    Ok(rows)
}

pub async fn list_active_locks_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<task_write_lock::Model>> {
    task_write_lock::Entity::find()
        .filter(task_write_lock::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            task_write_lock::Column::Status
                .eq(task_write_lock_status_to_db(TaskWriteLockStatus::Acquired)),
        )
        .filter(
            task_write_lock::Column::ExpiresAt
                .is_null()
                .or(task_write_lock::Column::ExpiresAt.gt(now)),
        )
        .order_by_asc(task_write_lock::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list active task write locks")
}

/// Return the complete active lock set used by an ownership transition.
///
/// Ordinary diagnostics may use a bounded page, but resume fencing must not
/// miss a conflicting lock merely because it falls beyond an arbitrary
/// pagination guard.
pub async fn list_all_active_locks_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Vec<task_write_lock::Model>> {
    task_write_lock::Entity::find()
        .filter(task_write_lock::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            task_write_lock::Column::Status
                .eq(task_write_lock_status_to_db(TaskWriteLockStatus::Acquired)),
        )
        .filter(
            task_write_lock::Column::ExpiresAt
                .is_null()
                .or(task_write_lock::Column::ExpiresAt.gt(now)),
        )
        .order_by_asc(task_write_lock::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list complete active task write lock set")
}

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
pub struct TaskWriteLockConflict {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
}

/// Return the oldest active lock that overlaps an owned path on component boundaries.
pub async fn find_active_conflict_for_run<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    run_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<TaskWriteLockConflict>> {
    use task_write_lock::{Column as Field, Entity as Locks};
    let owned_path = Expr::col(("owned", Field::ScopePath));
    let candidate_path = Expr::col((Locks, Field::ScopePath));
    // PostgreSQL starts_with compares text literally, including SQL wildcard characters.
    let is_below = |path: Expr, parent: Expr| {
        Expr::from(Func::cust(Alias::new("starts_with")).args([path, parent.concat("/")]))
    };
    let owned_overlap = Query::select()
        .expr(1)
        .from_as(Locks, "owned")
        .and_where(Expr::col(("owned", Field::RunId)).eq(run_id))
        .cond_where(
            Condition::any()
                .add(owned_path.clone().eq("."))
                .add(candidate_path.clone().eq("."))
                .add(owned_path.clone().eq(candidate_path.clone()))
                .add(is_below(owned_path.clone(), candidate_path.clone()))
                .add(is_below(candidate_path, owned_path)),
        )
        .to_owned();
    Locks::find()
        .select_only()
        .columns([Field::Id, Field::TaskId, Field::RunId])
        .filter(Field::WorkspaceId.eq(workspace_id))
        .filter(Field::RunId.ne(run_id))
        .filter(Field::Status.eq("acquired"))
        .filter(
            Condition::any()
                .add(Field::ExpiresAt.is_null())
                .add(Field::ExpiresAt.gt(now)),
        )
        .filter(Expr::exists(owned_overlap))
        .order_by_asc(Field::CreatedAt)
        .order_by_asc(Field::Id)
        .limit(1)
        .into_model::<TaskWriteLockConflict>()
        .one(db)
        .await
        .context("failed to find conflicting active task write lock")
}

/// Reopens every lock belonging to a run with one bounded SQL statement.
pub async fn reopen_locks_for_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    acquired_at: DateTimeWithTimeZone,
    expires_at: DateTimeWithTimeZone,
) -> Result<u64> {
    task_write_lock::Entity::update_many()
        .filter(task_write_lock::Column::RunId.eq(run_id.to_owned()))
        .col_expr(
            task_write_lock::Column::Status,
            Expr::value(task_write_lock_status_to_db(TaskWriteLockStatus::Acquired)),
        )
        .col_expr(
            task_write_lock::Column::AcquiredAt,
            Expr::value(acquired_at),
        )
        .col_expr(
            task_write_lock::Column::ExpiresAt,
            Expr::value(Some(expires_at)),
        )
        .col_expr(
            task_write_lock::Column::ReleasedAt,
            Expr::value(None::<DateTimeWithTimeZone>),
        )
        .col_expr(task_write_lock::Column::Reason, Expr::value(None::<String>))
        .col_expr(task_write_lock::Column::UpdatedAt, Expr::value(acquired_at))
        .exec(db)
        .await
        .context("failed to reopen task write locks by run")
        .map(|result| result.rows_affected)
}

pub async fn list_stale_locks<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<task_write_lock::Model>> {
    task_write_lock::Entity::find()
        .filter(
            task_write_lock::Column::Status
                .eq(task_write_lock_status_to_db(TaskWriteLockStatus::Acquired)),
        )
        .filter(task_write_lock::Column::ExpiresAt.is_not_null())
        .filter(task_write_lock::Column::ExpiresAt.lte(now))
        .order_by_asc(task_write_lock::Column::ExpiresAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list stale task write locks")
}

pub async fn update_lock_status<C: ConnectionTrait>(
    db: &C,
    lock_id: &str,
    status: TaskWriteLockStatus,
    released_at: Option<DateTimeWithTimeZone>,
    reason: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    task_write_lock::Entity::update_many()
        .filter(task_write_lock::Column::Id.eq(lock_id.to_owned()))
        .col_expr(
            task_write_lock::Column::Status,
            Expr::value(task_write_lock_status_to_db(status)),
        )
        .col_expr(
            task_write_lock::Column::ReleasedAt,
            Expr::value(released_at),
        )
        .col_expr(task_write_lock::Column::Reason, Expr::value(reason))
        .col_expr(task_write_lock::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to update task write lock status")?;
    Ok(())
}

fn active_model_from_lock(lock: &TaskWriteLock) -> task_write_lock::ActiveModel {
    task_write_lock::ActiveModel {
        id: Set(lock.id.clone()),
        workspace_id: Set(lock.workspace_id.clone()),
        task_id: Set(lock.task_id.clone()),
        run_id: Set(lock.run_id.clone()),
        scope_kind: Set(task_write_lock_scope_kind_to_db(lock.scope_kind).to_owned()),
        scope_path: Set(lock.scope_path.clone()),
        status: Set(task_write_lock_status_to_db(lock.status).to_owned()),
        acquired_at: Set(unix_to_datetime(lock.acquired_at)),
        expires_at: Set(lock.expires_at.map(unix_to_datetime)),
        released_at: Set(lock.released_at.map(unix_to_datetime)),
        conflict_policy: Set(
            task_concurrency_conflict_policy_to_db(lock.conflict_policy).to_owned()
        ),
        reason: Set(lock.reason.clone()),
        created_at: Set(unix_to_datetime(lock.created_at)),
        updated_at: Set(unix_to_datetime(lock.updated_at)),
    }
}

pub fn task_write_lock_from_db_model(
    model: pioneer_entity::task_write_lock::Model,
) -> Result<TaskWriteLock> {
    let scope_kind = task_write_lock_scope_kind_from_db(model.scope_kind.as_str())
        .with_context(|| format!("unknown task write lock scope kind `{}`", model.scope_kind))?;
    let status = task_write_lock_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task write lock status `{}`", model.status))?;
    let conflict_policy = task_concurrency_conflict_policy_from_db(model.conflict_policy.as_str())
        .with_context(|| {
            format!(
                "unknown task write lock conflict policy `{}`",
                model.conflict_policy
            )
        })?;
    Ok(TaskWriteLock {
        id: model.id,
        workspace_id: model.workspace_id,
        task_id: model.task_id,
        run_id: model.run_id,
        scope_kind,
        scope_path: model.scope_path,
        status,
        acquired_at: model.acquired_at.timestamp(),
        expires_at: model.expires_at.map(|value| value.timestamp()),
        released_at: model.released_at.map(|value| value.timestamp()),
        conflict_policy,
        reason: model.reason,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}
