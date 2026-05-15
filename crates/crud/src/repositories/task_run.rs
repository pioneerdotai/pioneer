#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_run;
use pioneer_protocol::{TaskError, TaskResult, TaskRun, TaskRunStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    is_terminal_task_run_status, task_executor_kind_to_db, task_run_status_from_db,
    task_run_status_to_db,
};
use crate::repositories::ProjectionWriteOutcome;
use crate::util::{optional_typed_json_to_db, unix_to_datetime};

pub async fn upsert_run<C: ConnectionTrait>(db: &C, run: &TaskRun) -> Result<()> {
    task_run::Entity::insert(active_model_from_run(run)?)
        .on_conflict(
            OnConflict::column(task_run::Column::Id)
                .update_columns([
                    task_run::Column::TaskId,
                    task_run::Column::TriggerId,
                    task_run::Column::ParentRunId,
                    task_run::Column::RunGroupId,
                    task_run::Column::AttemptNumber,
                    task_run::Column::RetryOfRunId,
                    task_run::Column::ReadyAt,
                    task_run::Column::RunNumber,
                    task_run::Column::Status,
                    task_run::Column::ExecutorKind,
                    task_run::Column::StartedAt,
                    task_run::Column::CompletedAt,
                    task_run::Column::HeartbeatAt,
                    task_run::Column::LockedBy,
                    task_run::Column::LockExpiresAt,
                    task_run::Column::ResultJson,
                    task_run::Column::ErrorJson,
                    task_run::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task run")?;
    Ok(())
}

pub async fn find_run_by_id<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<task_run::Model>> {
    task_run::Entity::find_by_id(run_id.to_owned())
        .one(db)
        .await
        .context("failed to query task run by id")
}

pub async fn list_runs_by_task<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Vec<task_run::Model>> {
    task_run::Entity::find()
        .filter(task_run::Column::TaskId.eq(task_id.to_owned()))
        .order_by_asc(task_run::Column::RunNumber)
        .all(db)
        .await
        .context("failed to list task runs")
}

pub async fn list_runs_by_status<C: ConnectionTrait>(
    db: &C,
    status: TaskRunStatus,
    limit: u64,
) -> Result<Vec<task_run::Model>> {
    task_run::Entity::find()
        .filter(task_run::Column::Status.eq(task_run_status_to_db(status)))
        .order_by_asc(task_run::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list task runs by status")
}

pub async fn list_due_retry_runs<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<task_run::Model>> {
    task_run::Entity::find()
        .filter(task_run::Column::Status.eq(task_run_status_to_db(TaskRunStatus::Queued)))
        .filter(task_run::Column::RetryOfRunId.is_not_null())
        .filter(
            task_run::Column::ReadyAt
                .is_null()
                .or(task_run::Column::ReadyAt.lte(now)),
        )
        .order_by_asc(task_run::Column::ReadyAt)
        .order_by_asc(task_run::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list due retry task runs")
}

pub async fn claim_run_for_dispatch<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    claimed_at: DateTimeWithTimeZone,
) -> Result<Option<task_run::Model>> {
    let result = task_run::Entity::update_many()
        .filter(task_run::Column::Id.eq(run_id.to_owned()))
        .filter(task_run::Column::Status.eq(task_run_status_to_db(TaskRunStatus::Queued)))
        .col_expr(
            task_run::Column::Status,
            Expr::value(task_run_status_to_db(TaskRunStatus::Starting)),
        )
        .col_expr(task_run::Column::HeartbeatAt, Expr::value(claimed_at))
        .col_expr(task_run::Column::UpdatedAt, Expr::value(claimed_at))
        .exec(db)
        .await
        .context("failed to claim task run for dispatch")?;
    if result.rows_affected == 0 {
        return Ok(None);
    }
    find_run_by_id(db, run_id).await
}

pub async fn next_run_number<C: ConnectionTrait>(db: &C, task_id: &str) -> Result<i64> {
    let max_run_number = db
        .query_one(
            &Query::select()
                .expr_as(
                    Expr::cust("COALESCE(MAX(run_number), 0)"),
                    sea_orm::sea_query::Alias::new("max_run_number"),
                )
                .from(sea_orm::sea_query::Alias::new("task_run"))
                .and_where(Expr::col(sea_orm::sea_query::Alias::new("task_id")).eq(task_id))
                .to_owned(),
        )
        .await
        .context("failed to query max task run number")?
        .and_then(|row| {
            row.try_get::<Option<i64>>("", "max_run_number")
                .ok()
                .flatten()
        })
        .unwrap_or(0);

    Ok(max_run_number + 1)
}

pub async fn update_run_status<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    status: TaskRunStatus,
    updated_at: DateTimeWithTimeZone,
) -> Result<ProjectionWriteOutcome> {
    let Some(model) = find_run_by_id(db, run_id).await? else {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` missing for status update"),
        });
    };
    if let Some(outcome) = terminal_run_transition_guard(run_id, model.status.as_str(), status) {
        return Ok(outcome);
    }

    let result = task_run::Entity::update_many()
        .filter(task_run::Column::Id.eq(run_id.to_owned()))
        .col_expr(
            task_run::Column::Status,
            Expr::value(task_run_status_to_db(status)),
        )
        .col_expr(task_run::Column::UpdatedAt, Expr::value(updated_at))
        .exec(db)
        .await
        .context("failed to update task run status")?;
    if result.rows_affected == 0 {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` status update affected zero rows"),
        });
    }
    Ok(ProjectionWriteOutcome::Applied)
}

pub async fn update_run_started<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    started_at: DateTimeWithTimeZone,
) -> Result<ProjectionWriteOutcome> {
    let Some(model) = find_run_by_id(db, run_id).await? else {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` missing for run start"),
        });
    };
    if task_run_status_from_db(model.status.as_str()) == Some(TaskRunStatus::Running) {
        return Ok(ProjectionWriteOutcome::NoopAlreadyStarted);
    }
    if let Some(outcome) =
        terminal_run_transition_guard(run_id, model.status.as_str(), TaskRunStatus::Running)
    {
        return Ok(outcome);
    }

    let result = task_run::Entity::update_many()
        .filter(task_run::Column::Id.eq(run_id.to_owned()))
        .col_expr(
            task_run::Column::Status,
            Expr::value(task_run_status_to_db(TaskRunStatus::Running)),
        )
        .col_expr(task_run::Column::StartedAt, Expr::value(started_at))
        .col_expr(task_run::Column::HeartbeatAt, Expr::value(started_at))
        .col_expr(task_run::Column::UpdatedAt, Expr::value(started_at))
        .exec(db)
        .await
        .context("failed to mark task run started")?;
    if result.rows_affected == 0 {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` start update affected zero rows"),
        });
    }
    Ok(ProjectionWriteOutcome::Applied)
}

pub async fn update_run_result<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    status: TaskRunStatus,
    result: Option<&TaskResult>,
    completed_at: DateTimeWithTimeZone,
) -> Result<ProjectionWriteOutcome> {
    let Some(model) = find_run_by_id(db, run_id).await? else {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` missing for result update"),
        });
    };
    if let Some(outcome) = terminal_run_transition_guard(run_id, model.status.as_str(), status) {
        return Ok(outcome);
    }

    let result_json = result
        .map(|value| serde_json::to_string(value).context("failed to serialize task run result"))
        .transpose()?;

    let result = task_run::Entity::update_many()
        .filter(task_run::Column::Id.eq(run_id.to_owned()))
        .col_expr(
            task_run::Column::Status,
            Expr::value(task_run_status_to_db(status)),
        )
        .col_expr(task_run::Column::ResultJson, Expr::value(result_json))
        .col_expr(task_run::Column::CompletedAt, Expr::value(completed_at))
        .col_expr(task_run::Column::UpdatedAt, Expr::value(completed_at))
        .exec(db)
        .await
        .context("failed to update task run result")?;
    if result.rows_affected == 0 {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` result update affected zero rows"),
        });
    }
    Ok(ProjectionWriteOutcome::Applied)
}

pub async fn update_run_error<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    status: TaskRunStatus,
    error: Option<&TaskError>,
    completed_at: DateTimeWithTimeZone,
) -> Result<ProjectionWriteOutcome> {
    let Some(model) = find_run_by_id(db, run_id).await? else {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` missing for error update"),
        });
    };
    if let Some(outcome) = terminal_run_transition_guard(run_id, model.status.as_str(), status) {
        return Ok(outcome);
    }

    let error_json = error
        .map(|value| serde_json::to_string(value).context("failed to serialize task run error"))
        .transpose()?;

    let result = task_run::Entity::update_many()
        .filter(task_run::Column::Id.eq(run_id.to_owned()))
        .col_expr(
            task_run::Column::Status,
            Expr::value(task_run_status_to_db(status)),
        )
        .col_expr(task_run::Column::ErrorJson, Expr::value(error_json))
        .col_expr(task_run::Column::CompletedAt, Expr::value(completed_at))
        .col_expr(task_run::Column::UpdatedAt, Expr::value(completed_at))
        .exec(db)
        .await
        .context("failed to update task run error")?;
    if result.rows_affected == 0 {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` error update affected zero rows"),
        });
    }
    Ok(ProjectionWriteOutcome::Applied)
}

fn terminal_run_transition_guard(
    run_id: &str,
    current_status_db: &str,
    next_status: TaskRunStatus,
) -> Option<ProjectionWriteOutcome> {
    let Some(current_status) = task_run_status_from_db(current_status_db) else {
        return Some(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task run `{run_id}` has unknown status `{current_status_db}`"),
        });
    };
    if !is_terminal_task_run_status(current_status) {
        return None;
    }
    if current_status == next_status {
        return Some(ProjectionWriteOutcome::NoopDuplicateTerminal);
    }
    if !is_terminal_task_run_status(next_status) {
        return Some(ProjectionWriteOutcome::NoopAlreadyTerminal);
    }
    Some(ProjectionWriteOutcome::InvariantViolation {
        reason: format!(
            "task run `{run_id}` cannot transition from terminal `{current_status_db}` to `{}`",
            task_run_status_to_db(next_status)
        ),
    })
}

fn active_model_from_run(run: &TaskRun) -> Result<task_run::ActiveModel> {
    Ok(task_run::ActiveModel {
        id: Set(run.id.clone()),
        task_id: Set(run.task_id.clone()),
        trigger_id: Set(run.trigger_id.clone()),
        parent_run_id: Set(run.parent_run_id.clone()),
        run_group_id: Set(run.run_group_id.clone()),
        attempt_number: Set(i64::from(run.attempt_number)),
        retry_of_run_id: Set(run.retry_of_run_id.clone()),
        ready_at: Set(run.ready_at.map(unix_to_datetime)),
        run_number: Set(run.run_number),
        status: Set(task_run_status_to_db(run.status).to_owned()),
        executor_kind: Set(task_executor_kind_to_db(run.executor_kind).to_owned()),
        started_at: Set(run.started_at.map(unix_to_datetime)),
        completed_at: Set(run.completed_at.map(unix_to_datetime)),
        heartbeat_at: Set(run.heartbeat_at.map(unix_to_datetime)),
        locked_by: Set(run.locked_by.clone()),
        lock_expires_at: Set(run.lock_expires_at.map(unix_to_datetime)),
        result_json: Set(optional_typed_json_to_db(&run.result)?),
        error_json: Set(optional_typed_json_to_db(&run.error)?),
        created_at: Set(unix_to_datetime(run.created_at)),
        updated_at: Set(unix_to_datetime(run.updated_at)),
    })
}
