use anyhow::{Context, Result};
use pioneer_entity::task;
use pioneer_protocol::{Task, TaskError, TaskResult, TaskStatus, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    is_terminal_task_status, task_executor_kind_to_db, task_owner_kind_to_db, task_status_from_db,
    task_status_to_db,
};
use crate::repositories::ProjectionWriteOutcome;
use crate::util::{optional_typed_json_to_db, unix_to_datetime};

const DB_ID_LEN: usize = 21;

pub async fn upsert_task<C: ConnectionTrait>(db: &C, task_model: &Task) -> Result<()> {
    task::Entity::insert(active_model_from_task(task_model)?)
        .on_conflict(
            OnConflict::column(task::Column::Id)
                .update_columns([
                    task::Column::WorkspaceId,
                    task::Column::OwnerKind,
                    task::Column::OwnerId,
                    task::Column::CreatedByThreadId,
                    task::Column::CreatedByTurnId,
                    task::Column::RootTaskId,
                    task::Column::ParentTaskId,
                    task::Column::ExecutorKind,
                    task::Column::Status,
                    task::Column::Title,
                    task::Column::Goal,
                    task::Column::Priority,
                    task::Column::LifecyclePolicyJson,
                    task::Column::DeliveryPolicyJson,
                    task::Column::RetryPolicyJson,
                    task::Column::TimeoutPolicyJson,
                    task::Column::ConcurrencyPolicyJson,
                    task::Column::MetadataJson,
                    task::Column::ResultJson,
                    task::Column::ErrorJson,
                    task::Column::Revision,
                    task::Column::UpdatedAt,
                    task::Column::CompletedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert task")?;
    Ok(())
}

pub async fn find_task_by_id<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Option<task::Model>> {
    task::Entity::find_by_id(task_id.to_owned())
        .one(db)
        .await
        .context("failed to query task by id")
}

pub async fn list_tasks_by_workspace_status<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    status: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<task::Model>> {
    let mut query = task::Entity::find()
        .filter(task::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_desc(task::Column::UpdatedAt);

    if let Some(status) = status {
        query = query.filter(task::Column::Status.eq(status.to_owned()));
    }
    if let Some(limit) = limit {
        query = query.limit(limit);
    }

    query
        .all(db)
        .await
        .context("failed to list workspace tasks")
}

pub async fn list_tasks_by_owner<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    owner_kind: &str,
    owner_id: Option<&str>,
    limit: Option<u64>,
) -> Result<Vec<task::Model>> {
    let mut query = task::Entity::find()
        .filter(task::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(task::Column::OwnerKind.eq(owner_kind.to_owned()))
        .order_by_desc(task::Column::UpdatedAt);

    query = match owner_id {
        Some(owner_id) => query.filter(task::Column::OwnerId.eq(owner_id.to_owned())),
        None => query.filter(task::Column::OwnerId.is_null()),
    };
    if let Some(limit) = limit {
        query = query.limit(limit);
    }

    query.all(db).await.context("failed to list owner tasks")
}

pub async fn list_tasks_by_parent<C: ConnectionTrait>(
    db: &C,
    parent_task_id: &str,
) -> Result<Vec<task::Model>> {
    task::Entity::find()
        .filter(task::Column::ParentTaskId.eq(parent_task_id.to_owned()))
        .order_by_asc(task::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list child tasks")
}

pub async fn list_tasks_by_root<C: ConnectionTrait>(
    db: &C,
    root_task_id: &str,
) -> Result<Vec<task::Model>> {
    task::Entity::find()
        .filter(task::Column::RootTaskId.eq(root_task_id.to_owned()))
        .order_by_asc(task::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list root task tree")
}

pub async fn list_tasks_by_creator_turns<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_ids: &[String],
) -> Result<Vec<task::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }
    task::Entity::find()
        .filter(task::Column::CreatedByThreadId.eq(thread_id.to_owned()))
        .filter(task::Column::CreatedByTurnId.is_in(turn_ids.to_vec()))
        .order_by_asc(task::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list tasks by creator turns")
}

pub async fn update_task_status<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    status: TaskStatus,
    updated_at: DateTimeWithTimeZone,
    completed_at: Option<DateTimeWithTimeZone>,
) -> Result<ProjectionWriteOutcome> {
    let Some(model) = find_task_by_id(db, task_id).await? else {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task `{task_id}` missing for status update"),
        });
    };
    if let Some(outcome) = terminal_task_transition_guard(task_id, model.status.as_str(), status) {
        return Ok(outcome);
    }

    let result = task::Entity::update_many()
        .filter(task::Column::Id.eq(task_id.to_owned()))
        .col_expr(task::Column::Status, Expr::value(task_status_to_db(status)))
        .col_expr(task::Column::UpdatedAt, Expr::value(updated_at))
        .col_expr(task::Column::CompletedAt, Expr::value(completed_at))
        .exec(db)
        .await
        .context("failed to update task status")?;
    if result.rows_affected == 0 {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task `{task_id}` status update affected zero rows"),
        });
    }
    Ok(ProjectionWriteOutcome::Applied)
}

pub async fn update_task_result<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    status: TaskStatus,
    result: Option<&TaskResult>,
    updated_at: DateTimeWithTimeZone,
    completed_at: Option<DateTimeWithTimeZone>,
) -> Result<ProjectionWriteOutcome> {
    let Some(model) = find_task_by_id(db, task_id).await? else {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task `{task_id}` missing for result update"),
        });
    };
    if let Some(outcome) = terminal_task_transition_guard(task_id, model.status.as_str(), status) {
        return Ok(outcome);
    }

    let result_json = result
        .map(|value| serde_json::to_string(value).context("failed to serialize task result"))
        .transpose()?;

    let result = task::Entity::update_many()
        .filter(task::Column::Id.eq(task_id.to_owned()))
        .col_expr(task::Column::Status, Expr::value(task_status_to_db(status)))
        .col_expr(task::Column::ResultJson, Expr::value(result_json))
        .col_expr(task::Column::UpdatedAt, Expr::value(updated_at))
        .col_expr(task::Column::CompletedAt, Expr::value(completed_at))
        .col_expr(task::Column::Revision, Expr::cust("revision + 1"))
        .exec(db)
        .await
        .context("failed to update task result")?;
    if result.rows_affected == 0 {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task `{task_id}` result update affected zero rows"),
        });
    }
    Ok(ProjectionWriteOutcome::Applied)
}

pub async fn update_task_error<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    status: TaskStatus,
    error: Option<&TaskError>,
    updated_at: DateTimeWithTimeZone,
    completed_at: Option<DateTimeWithTimeZone>,
) -> Result<ProjectionWriteOutcome> {
    let Some(model) = find_task_by_id(db, task_id).await? else {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task `{task_id}` missing for error update"),
        });
    };
    if let Some(outcome) = terminal_task_transition_guard(task_id, model.status.as_str(), status) {
        return Ok(outcome);
    }

    let error_json = error
        .map(|value| serde_json::to_string(value).context("failed to serialize task error"))
        .transpose()?;

    let result = task::Entity::update_many()
        .filter(task::Column::Id.eq(task_id.to_owned()))
        .col_expr(task::Column::Status, Expr::value(task_status_to_db(status)))
        .col_expr(task::Column::ErrorJson, Expr::value(error_json))
        .col_expr(task::Column::UpdatedAt, Expr::value(updated_at))
        .col_expr(task::Column::CompletedAt, Expr::value(completed_at))
        .col_expr(task::Column::Revision, Expr::cust("revision + 1"))
        .exec(db)
        .await
        .context("failed to update task error")?;
    if result.rows_affected == 0 {
        return Ok(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task `{task_id}` error update affected zero rows"),
        });
    }
    Ok(ProjectionWriteOutcome::Applied)
}

fn terminal_task_transition_guard(
    task_id: &str,
    current_status_db: &str,
    next_status: TaskStatus,
) -> Option<ProjectionWriteOutcome> {
    let Some(current_status) = task_status_from_db(current_status_db) else {
        return Some(ProjectionWriteOutcome::InvariantViolation {
            reason: format!("task `{task_id}` has unknown status `{current_status_db}`"),
        });
    };

    if !is_terminal_task_status(current_status) {
        return None;
    }
    if current_status == next_status {
        return Some(ProjectionWriteOutcome::NoopDuplicateTerminal);
    }
    if !is_terminal_task_status(next_status) {
        return Some(ProjectionWriteOutcome::NoopAlreadyTerminal);
    }

    Some(ProjectionWriteOutcome::InvariantViolation {
        reason: format!(
            "task `{task_id}` cannot transition from terminal `{current_status_db}` to `{}`",
            task_status_to_db(next_status)
        ),
    })
}

fn active_model_from_task(task_model: &Task) -> Result<task::ActiveModel> {
    Ok(task::ActiveModel {
        id: Set(if task_model.id.is_empty() {
            generate_id(DB_ID_LEN)
        } else {
            task_model.id.clone()
        }),
        workspace_id: Set(task_model.workspace_id.clone()),
        owner_kind: Set(task_owner_kind_to_db(task_model.owner_kind).to_owned()),
        owner_id: Set(task_model.owner_id.clone()),
        created_by_thread_id: Set(task_model.created_by_thread_id.clone()),
        created_by_turn_id: Set(task_model.created_by_turn_id.clone()),
        root_task_id: Set(task_model.root_task_id.clone()),
        parent_task_id: Set(task_model.parent_task_id.clone()),
        executor_kind: Set(task_executor_kind_to_db(task_model.executor_kind).to_owned()),
        status: Set(task_status_to_db(task_model.status).to_owned()),
        title: Set(task_model.title.clone()),
        goal: Set(task_model.goal.clone()),
        priority: Set(task_model.priority),
        lifecycle_policy_json: Set(optional_typed_json_to_db(&task_model.lifecycle_policy)?),
        delivery_policy_json: Set(optional_typed_json_to_db(&task_model.delivery_policy)?),
        retry_policy_json: Set(optional_typed_json_to_db(&task_model.retry_policy)?),
        timeout_policy_json: Set(optional_typed_json_to_db(&task_model.timeout_policy)?),
        concurrency_policy_json: Set(optional_typed_json_to_db(&task_model.concurrency_policy)?),
        metadata_json: Set(optional_typed_json_to_db(&task_model.metadata)?),
        result_json: Set(optional_typed_json_to_db(&task_model.result)?),
        error_json: Set(optional_typed_json_to_db(&task_model.error)?),
        revision: Set(task_model.revision),
        created_at: Set(unix_to_datetime(task_model.created_at)),
        updated_at: Set(unix_to_datetime(task_model.updated_at)),
        completed_at: Set(task_model.completed_at.map(unix_to_datetime)),
    })
}
