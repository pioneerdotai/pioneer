#![allow(dead_code)]

use anyhow::{Context, Result};
use pioneer_entity::task_run_execution;
use pioneer_protocol::{TaskError, TaskExecutorKind, TaskResult, TaskRunExecutionStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Condition, Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, Statement,
};

use crate::convention::{
    is_terminal_task_run_execution_status, task_executor_kind_to_db,
    task_run_execution_status_from_db, task_run_execution_status_to_db,
};
use crate::util::{optional_typed_json_to_db, unix_to_datetime};

pub struct NewTaskRunExecution {
    pub id: String,
    pub task_id: String,
    pub task_run_id: String,
    pub executor_kind: TaskExecutorKind,
    pub status: TaskRunExecutionStatus,
    pub worker_id: Option<String>,
    pub lease_until: Option<i64>,
    pub heartbeat_at: Option<i64>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub result: Option<TaskResult>,
    pub error: Option<TaskError>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Bounded public observation facts, keyed by the exact TaskRun. The resource
/// state join selects the occurrence's current agent attempt, so a previous
/// attempt's progress cannot appear as current activity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunExecutionObservationRecord {
    pub run_id: String,
    pub status: TaskRunExecutionStatus,
    pub heartbeat_at: Option<i64>,
    pub last_activity_at: Option<i64>,
}

pub async fn list_execution_observations_for_runs<C: ConnectionTrait>(
    db: &C,
    run_ids: &[String],
) -> Result<Vec<TaskRunExecutionObservationRecord>> {
    if run_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = vec!["?"; run_ids.len()].join(", ");
    let sql = format!(
        "SELECT execution.task_run_id AS run_id, execution.status AS status, \
                CASE WHEN execution.executor_kind = 'agent' \
                     THEN resource.last_heartbeat_at ELSE execution.heartbeat_at END AS heartbeat_at, \
                resource.last_progress_at AS last_activity_at \
         FROM task_run_execution AS execution \
         LEFT JOIN task_occurrence_contract AS occurrence \
           ON occurrence.run_id = execution.task_run_id \
          AND occurrence.agent_execution_id = execution.id \
         LEFT JOIN agent_execution_resource_state AS resource \
           ON resource.execution_id = execution.id \
          AND resource.attempt_generation = occurrence.retry_attempt + 1 \
         WHERE execution.task_run_id IN ({placeholders})"
    );
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            run_ids.iter().cloned().map(sea_orm::Value::from),
        ))
        .await
        .context("failed to observe task run executions")?;
    rows.into_iter()
        .map(|row| {
            let status: String = row.try_get("", "status")?;
            Ok(TaskRunExecutionObservationRecord {
                run_id: row.try_get("", "run_id")?,
                status: task_run_execution_status_from_db(status.as_str())
                    .with_context(|| format!("unknown task run execution status `{status}`"))?,
                heartbeat_at: row
                    .try_get::<Option<DateTimeWithTimeZone>>("", "heartbeat_at")?
                    .map(|value| value.timestamp()),
                last_activity_at: row
                    .try_get::<Option<DateTimeWithTimeZone>>("", "last_activity_at")?
                    .map(|value| value.timestamp()),
            })
        })
        .collect()
}

pub async fn insert_execution_if_absent<C: ConnectionTrait>(
    db: &C,
    execution: NewTaskRunExecution,
) -> Result<()> {
    task_run_execution::Entity::insert(active_model_from_new_execution(execution)?)
        .on_conflict(
            OnConflict::column(task_run_execution::Column::TaskRunId)
                .do_nothing()
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to insert task run execution")?;
    Ok(())
}

pub async fn find_execution_by_id<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
) -> Result<Option<task_run_execution::Model>> {
    task_run_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to query task run execution by id")
}

pub async fn find_execution_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
) -> Result<Option<task_run_execution::Model>> {
    task_run_execution::Entity::find()
        .filter(task_run_execution::Column::TaskRunId.eq(run_id.to_owned()))
        .one(db)
        .await
        .context("failed to query task run execution by run")
}

pub async fn count_executions_by_run<C: ConnectionTrait>(db: &C, run_id: &str) -> Result<u64> {
    task_run_execution::Entity::find()
        .filter(task_run_execution::Column::TaskRunId.eq(run_id.to_owned()))
        .count(db)
        .await
        .context("failed to count task run executions by run")
}

pub async fn list_executions_by_status<C: ConnectionTrait>(
    db: &C,
    status: TaskRunExecutionStatus,
    limit: u64,
) -> Result<Vec<task_run_execution::Model>> {
    task_run_execution::Entity::find()
        .filter(task_run_execution::Column::Status.eq(task_run_execution_status_to_db(status)))
        .order_by_asc(task_run_execution::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list task run executions by status")
}

pub async fn claim_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    worker_id: &str,
    now: DateTimeWithTimeZone,
    lease_until: DateTimeWithTimeZone,
) -> Result<Option<task_run_execution::Model>> {
    let terminal_statuses = terminal_status_values();
    let reserved = task_run_execution_status_to_db(TaskRunExecutionStatus::Reserved);
    let result = task_run_execution::Entity::update_many()
        .filter(task_run_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(task_run_execution::Column::Status.is_not_in(terminal_statuses))
        .filter(
            Condition::any()
                .add(task_run_execution::Column::Status.eq(reserved))
                .add(task_run_execution::Column::WorkerId.eq(worker_id.to_owned()))
                .add(task_run_execution::Column::LeaseUntil.is_null())
                .add(task_run_execution::Column::LeaseUntil.lte(now)),
        )
        .col_expr(
            task_run_execution::Column::Status,
            Expr::value(task_run_execution_status_to_db(
                TaskRunExecutionStatus::Starting,
            )),
        )
        .col_expr(
            task_run_execution::Column::WorkerId,
            Expr::value(Some(worker_id.to_owned())),
        )
        .col_expr(
            task_run_execution::Column::LeaseUntil,
            Expr::value(Some(lease_until)),
        )
        .col_expr(
            task_run_execution::Column::HeartbeatAt,
            Expr::cust("CURRENT_TIMESTAMP"),
        )
        .col_expr(
            task_run_execution::Column::UpdatedAt,
            Expr::cust("CURRENT_TIMESTAMP"),
        )
        .exec(db)
        .await
        .context("failed to claim task run execution")?;
    if result.rows_affected == 0 {
        return Ok(None);
    }
    find_execution_by_id(db, execution_id).await
}

pub async fn mark_execution_running<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    started_at: DateTimeWithTimeZone,
    lease_until: Option<DateTimeWithTimeZone>,
) -> Result<Option<task_run_execution::Model>> {
    let terminal_statuses = terminal_status_values();
    let mut update = task_run_execution::Entity::update_many()
        .filter(task_run_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(task_run_execution::Column::Status.is_not_in(terminal_statuses))
        .col_expr(
            task_run_execution::Column::Status,
            Expr::value(task_run_execution_status_to_db(
                TaskRunExecutionStatus::Running,
            )),
        )
        .col_expr(
            task_run_execution::Column::StartedAt,
            Expr::value(Some(started_at)),
        )
        .col_expr(
            task_run_execution::Column::HeartbeatAt,
            Expr::value(Some(started_at)),
        )
        .col_expr(
            task_run_execution::Column::UpdatedAt,
            Expr::value(started_at),
        );
    if let Some(lease_until) = lease_until {
        update = update.col_expr(
            task_run_execution::Column::LeaseUntil,
            Expr::value(Some(lease_until)),
        );
    }
    let result = update
        .exec(db)
        .await
        .context("failed to mark task run execution running")?;
    if result.rows_affected == 0 {
        return Ok(None);
    }
    find_execution_by_id(db, execution_id).await
}

pub async fn mark_execution_waiting_review_by_run<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    entered_at: DateTimeWithTimeZone,
) -> Result<Option<task_run_execution::Model>> {
    let terminal_statuses = terminal_status_values();
    let result = task_run_execution::Entity::update_many()
        .filter(task_run_execution::Column::TaskRunId.eq(run_id.to_owned()))
        .filter(task_run_execution::Column::Status.is_not_in(terminal_statuses))
        .col_expr(
            task_run_execution::Column::Status,
            Expr::value(task_run_execution_status_to_db(
                TaskRunExecutionStatus::WaitingReview,
            )),
        )
        .col_expr(
            task_run_execution::Column::LeaseUntil,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            task_run_execution::Column::HeartbeatAt,
            Expr::value(Some(entered_at)),
        )
        .col_expr(
            task_run_execution::Column::UpdatedAt,
            Expr::value(entered_at),
        )
        .exec(db)
        .await
        .context("failed to mark task run execution waiting review")?;
    if result.rows_affected == 0 {
        return Ok(None);
    }
    find_execution_by_run(db, run_id).await
}

pub async fn mark_execution_terminal<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    status: TaskRunExecutionStatus,
    completed_at: DateTimeWithTimeZone,
    result: Option<&TaskResult>,
    error: Option<&TaskError>,
) -> Result<Option<task_run_execution::Model>> {
    debug_assert!(is_terminal_task_run_execution_status(status));
    let terminal_statuses = terminal_status_values();
    let result_json = optional_typed_json_to_db(&result.cloned())?;
    let error_json = optional_typed_json_to_db(&error.cloned())?;
    let update_result = task_run_execution::Entity::update_many()
        .filter(task_run_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(task_run_execution::Column::Status.is_not_in(terminal_statuses))
        .col_expr(
            task_run_execution::Column::Status,
            Expr::value(task_run_execution_status_to_db(status)),
        )
        .col_expr(
            task_run_execution::Column::CompletedAt,
            Expr::value(Some(completed_at)),
        )
        .col_expr(
            task_run_execution::Column::LeaseUntil,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            task_run_execution::Column::ResultJson,
            Expr::value(result_json),
        )
        .col_expr(
            task_run_execution::Column::ErrorJson,
            Expr::value(error_json),
        )
        .col_expr(
            task_run_execution::Column::UpdatedAt,
            Expr::value(completed_at),
        )
        .exec(db)
        .await
        .context("failed to mark task run execution terminal")?;
    if update_result.rows_affected == 0 {
        return Ok(None);
    }
    find_execution_by_id(db, execution_id).await
}

pub async fn heartbeat_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    heartbeat_at: DateTimeWithTimeZone,
    lease_until: Option<DateTimeWithTimeZone>,
) -> Result<Option<task_run_execution::Model>> {
    let terminal_statuses = terminal_status_values();
    let mut update = task_run_execution::Entity::update_many()
        .filter(task_run_execution::Column::Id.eq(execution_id.to_owned()))
        .filter(task_run_execution::Column::Status.is_not_in(terminal_statuses))
        .col_expr(
            task_run_execution::Column::HeartbeatAt,
            Expr::value(Some(heartbeat_at)),
        )
        .col_expr(
            task_run_execution::Column::UpdatedAt,
            Expr::value(heartbeat_at),
        );
    if let Some(lease_until) = lease_until {
        update = update.col_expr(
            task_run_execution::Column::LeaseUntil,
            Expr::value(Some(lease_until)),
        );
    }
    let result = update
        .exec(db)
        .await
        .context("failed to heartbeat task run execution")?;
    if result.rows_affected == 0 {
        return Ok(None);
    }
    find_execution_by_id(db, execution_id).await
}

fn active_model_from_new_execution(
    execution: NewTaskRunExecution,
) -> Result<task_run_execution::ActiveModel> {
    Ok(task_run_execution::ActiveModel {
        id: Set(execution.id),
        task_id: Set(execution.task_id),
        task_run_id: Set(execution.task_run_id),
        executor_kind: Set(task_executor_kind_to_db(execution.executor_kind).to_owned()),
        status: Set(task_run_execution_status_to_db(execution.status).to_owned()),
        worker_id: Set(execution.worker_id),
        lease_until: Set(execution.lease_until.map(unix_to_datetime)),
        heartbeat_at: Set(execution.heartbeat_at.map(unix_to_datetime)),
        started_at: Set(execution.started_at.map(unix_to_datetime)),
        completed_at: Set(execution.completed_at.map(unix_to_datetime)),
        result_json: Set(optional_typed_json_to_db(&execution.result)?),
        error_json: Set(optional_typed_json_to_db(&execution.error)?),
        created_at: Set(unix_to_datetime(execution.created_at)),
        updated_at: Set(unix_to_datetime(execution.updated_at)),
    })
}

fn terminal_status_values() -> [&'static str; 5] {
    [
        task_run_execution_status_to_db(TaskRunExecutionStatus::Succeeded),
        task_run_execution_status_to_db(TaskRunExecutionStatus::Failed),
        task_run_execution_status_to_db(TaskRunExecutionStatus::Blocked),
        task_run_execution_status_to_db(TaskRunExecutionStatus::Cancelled),
        task_run_execution_status_to_db(TaskRunExecutionStatus::TimedOut),
    ]
}
