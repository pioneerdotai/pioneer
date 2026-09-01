#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_write_lock;
use pioneer_protocol::{TaskWriteLock, TaskWriteLockStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set, Statement,
};

use crate::convention::{
    task_concurrency_conflict_policy_to_db, task_write_lock_scope_kind_to_db,
    task_write_lock_status_to_db,
};
use crate::util::unix_to_datetime;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskWriteLockConflict {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
}

/// Finds one active lock whose path overlaps a lock owned by `run_id`.
///
/// Path overlap is evaluated by SQLite using exact component boundaries. The
/// query is bounded to one row so ownership transitions never load an
/// unbounded workspace lock set while holding the writer.
pub async fn find_active_conflict_for_run<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    run_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<TaskWriteLockConflict>> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT candidate.id, candidate.task_id, candidate.run_id \
             FROM task_write_lock AS owned \
             JOIN task_write_lock AS candidate \
               ON candidate.workspace_id = ? \
              AND candidate.run_id <> ? \
              AND candidate.status = ? \
              AND (candidate.expires_at IS NULL OR candidate.expires_at > ?) \
              AND ( \
                    owned.scope_path = '.' \
                 OR candidate.scope_path = '.' \
                 OR owned.scope_path = candidate.scope_path \
                 OR substr(owned.scope_path, 1, length(candidate.scope_path) + 1) \
                        = candidate.scope_path || '/' \
                 OR substr(candidate.scope_path, 1, length(owned.scope_path) + 1) \
                        = owned.scope_path || '/' \
              ) \
             WHERE owned.run_id = ? \
             ORDER BY candidate.created_at ASC, candidate.id ASC \
             LIMIT 1"
                .to_owned(),
            [
                workspace_id.into(),
                run_id.into(),
                task_write_lock_status_to_db(TaskWriteLockStatus::Acquired).into(),
                now.into(),
                run_id.into(),
            ],
        ))
        .await
        .context("failed to find conflicting active task write lock")?;

    row.map(|row| {
        Ok(TaskWriteLockConflict {
            id: row.try_get("", "id")?,
            task_id: row.try_get("", "task_id")?,
            run_id: row.try_get("", "run_id")?,
        })
    })
    .transpose()
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
