use anyhow::{Context, Result};
use pioneer_protocol::{TaskError, TaskErrorClass, TaskRunStatus, TaskStatus, TaskValue};
use sea_orm::ConnectionTrait;
use std::collections::BTreeMap;
use tracing::warn;

use crate::convention::{is_terminal_task_status_db, task_run_status_from_db};
use crate::repositories::{
    ProjectionWriteOutcome, task, task_agent_spec, task_delivery, task_dependency, task_run,
    task_trigger, task_write_lock, thread_lineage,
};
use crate::task_events::{AppendedTaskEvent, TaskEventPayload};
use crate::util::unix_to_datetime;

#[derive(Clone, Default)]
pub struct TaskProjector;

impl TaskProjector {
    pub fn new() -> Self {
        Self
    }

    pub async fn project<C: ConnectionTrait>(
        &self,
        db: &C,
        event: &AppendedTaskEvent,
    ) -> Result<()> {
        match &event.payload {
            TaskEventPayload::TaskCreated { task: task_model } => {
                task::upsert_task(db, task_model).await
            }
            TaskEventPayload::TriggerCreated { trigger } => {
                task_trigger::upsert_trigger(db, trigger).await
            }
            TaskEventPayload::DependencyCreated { dependency } => {
                task_dependency::upsert_dependency(db, dependency).await
            }
            TaskEventPayload::AgentSpecCreated { agent_spec } => {
                task_agent_spec::upsert_agent_spec(db, agent_spec).await
            }
            TaskEventPayload::TaskScheduled {
                task_id,
                trigger_id,
                next_fire_at,
            } => {
                task_trigger::update_trigger_schedule(
                    db,
                    trigger_id,
                    pioneer_protocol::TaskTriggerStatus::Active,
                    next_fire_at.map(unix_to_datetime),
                    None,
                    event.created_at,
                )
                .await?;
                let outcome = task::update_task_status(
                    db,
                    task_id,
                    TaskStatus::Scheduled,
                    event.created_at,
                    None,
                )
                .await?;
                handle_projection_outcome("task_scheduled", task_id, &outcome);
                Ok(())
            }
            TaskEventPayload::TaskQueued { task_id, .. } => {
                let outcome = task::update_task_status(
                    db,
                    task_id,
                    TaskStatus::Queued,
                    event.created_at,
                    None,
                )
                .await?;
                handle_projection_outcome("task_queued", task_id, &outcome);
                Ok(())
            }
            TaskEventPayload::RunCreated { run, agent_spec } => {
                task_run::upsert_run(db, run).await?;
                if let Some(agent_spec) = agent_spec {
                    task_agent_spec::upsert_agent_spec(db, agent_spec).await?;
                }
                Ok(())
            }
            TaskEventPayload::RunStarted {
                task_id,
                run_id,
                started_at,
            } => {
                let started_at = unix_to_datetime(*started_at);
                let run_outcome = task_run::update_run_started(db, run_id, started_at)
                    .await
                    .with_context(|| format!("failed to project task run `{run_id}` started"))?;
                handle_projection_outcome("run_started", run_id, &run_outcome);
                if matches!(run_outcome, ProjectionWriteOutcome::Applied) {
                    let task_outcome = task::update_task_status(
                        db,
                        task_id,
                        TaskStatus::Running,
                        started_at,
                        None,
                    )
                    .await?;
                    handle_projection_outcome("run_started_task_status", task_id, &task_outcome);
                }
                Ok(())
            }
            TaskEventPayload::Progress { .. } => Ok(()),
            TaskEventPayload::RunCompleted {
                task_id: _,
                run_id,
                result,
                completed_at,
            } => {
                let completed_at = unix_to_datetime(*completed_at);
                let outcome = task_run::update_run_result(
                    db,
                    run_id,
                    TaskRunStatus::Succeeded,
                    result.as_ref(),
                    completed_at,
                )
                .await?;
                handle_projection_outcome("run_completed", run_id, &outcome);
                Ok(())
            }
            TaskEventPayload::RunFailed {
                task_id: _,
                run_id,
                error,
                completed_at,
            } => {
                let completed_at = unix_to_datetime(*completed_at);
                let status = if task_error_is_cancellation(error.as_ref()) {
                    TaskRunStatus::Cancelled
                } else {
                    TaskRunStatus::Failed
                };
                let outcome =
                    task_run::update_run_error(db, run_id, status, error.as_ref(), completed_at)
                        .await?;
                handle_projection_outcome("run_failed", run_id, &outcome);
                Ok(())
            }
            TaskEventPayload::RunRetryScheduled { retry_run, .. } => {
                task_run::upsert_run(db, retry_run).await?;
                let outcome = task::update_task_status(
                    db,
                    retry_run.task_id.as_str(),
                    TaskStatus::Queued,
                    event.created_at,
                    None,
                )
                .await?;
                handle_projection_outcome(
                    "run_retry_scheduled",
                    retry_run.task_id.as_str(),
                    &outcome,
                );
                Ok(())
            }
            TaskEventPayload::RunRetryExhausted { .. } => Ok(()),
            TaskEventPayload::RunCancelled {
                task_id: _,
                run_id,
                reason,
                cancelled_at,
            } => {
                let cancelled_at = unix_to_datetime(*cancelled_at);
                let error = reason.as_ref().map(|reason| TaskError {
                    code: "task_run_cancelled".to_owned(),
                    message: reason.clone(),
                    class: TaskErrorClass::Cancelled,
                    details: None,
                    failed_run_id: Some(run_id.clone()),
                });
                let outcome = task_run::update_run_error(
                    db,
                    run_id,
                    TaskRunStatus::Cancelled,
                    error.as_ref(),
                    cancelled_at,
                )
                .await?;
                handle_projection_outcome("run_cancelled", run_id, &outcome);
                Ok(())
            }
            TaskEventPayload::TaskCompleted {
                task_id,
                result,
                completed_at,
            } => {
                let completed_at = unix_to_datetime(*completed_at);
                let outcome = task::update_task_result(
                    db,
                    task_id,
                    TaskStatus::Completed,
                    result.as_ref(),
                    completed_at,
                    Some(completed_at),
                )
                .await?;
                handle_projection_outcome("task_completed", task_id, &outcome);
                Ok(())
            }
            TaskEventPayload::TaskFailed {
                task_id,
                error,
                completed_at,
            } => {
                let completed_at = unix_to_datetime(*completed_at);
                let status = if task_error_is_cancellation(error.as_ref()) {
                    TaskStatus::Cancelled
                } else {
                    TaskStatus::Failed
                };
                let outcome = task::update_task_error(
                    db,
                    task_id,
                    status,
                    error.as_ref(),
                    completed_at,
                    Some(completed_at),
                )
                .await?;
                handle_projection_outcome("task_failed", task_id, &outcome);
                Ok(())
            }
            TaskEventPayload::TaskCancelled {
                task_id,
                reason,
                completed_at,
            } => {
                let completed_at = unix_to_datetime(*completed_at);
                let error = reason.as_ref().map(|reason| TaskError {
                    code: "task_cancelled".to_owned(),
                    message: reason.clone(),
                    class: TaskErrorClass::Cancelled,
                    details: None,
                    failed_run_id: None,
                });
                let outcome = task::update_task_error(
                    db,
                    task_id,
                    TaskStatus::Cancelled,
                    error.as_ref(),
                    completed_at,
                    Some(completed_at),
                )
                .await?;
                handle_projection_outcome("task_cancelled", task_id, &outcome);
                Ok(())
            }
            TaskEventPayload::TaskDetached {
                task: task_model, ..
            } => {
                if task_is_terminal_db(db, task_model.id.as_str()).await? {
                    return Ok(());
                }
                task::upsert_task(db, task_model).await
            }
            TaskEventPayload::TaskRescheduled { trigger, .. } => {
                task_trigger::upsert_trigger(db, trigger).await?;
                if task_has_nonterminal_run_db(db, trigger.task_id.as_str()).await? {
                    return Ok(());
                }
                let status = if trigger.next_fire_at.is_some() {
                    TaskStatus::Scheduled
                } else {
                    TaskStatus::Queued
                };
                let outcome = task::update_task_status(
                    db,
                    trigger.task_id.as_str(),
                    status,
                    event.created_at,
                    None,
                )
                .await?;
                handle_projection_outcome("task_rescheduled", trigger.task_id.as_str(), &outcome);
                Ok(())
            }
            TaskEventPayload::TaskPaused {
                task: task_model,
                triggers,
                ..
            }
            | TaskEventPayload::TaskResumed {
                task: task_model,
                triggers,
                ..
            } => {
                if task_is_terminal_db(db, task_model.id.as_str()).await? {
                    return Ok(());
                }
                task::upsert_task(db, task_model).await?;
                for trigger in triggers {
                    task_trigger::upsert_trigger(db, trigger).await?;
                }
                Ok(())
            }
            TaskEventPayload::TaskRecovered { .. } => Ok(()),
            TaskEventPayload::ChildThreadLinked { lineage } => {
                thread_lineage::upsert_lineage(db, lineage).await
            }
            TaskEventPayload::DepthLimitExceeded {
                task_id,
                run_id,
                depth,
                max_depth,
            } => {
                let error = TaskError {
                    code: "task_depth_limit_exceeded".to_owned(),
                    message: format!("task depth {depth} exceeds max depth {max_depth}"),
                    class: TaskErrorClass::Policy,
                    details: Some(TaskValue::Object(BTreeMap::from([
                        ("depth".to_owned(), TaskValue::Integer(*depth)),
                        ("maxDepth".to_owned(), TaskValue::Integer(*max_depth)),
                    ]))),
                    failed_run_id: run_id.clone(),
                };
                if let Some(run_id) = run_id {
                    let outcome = task_run::update_run_error(
                        db,
                        run_id,
                        TaskRunStatus::Failed,
                        Some(&error),
                        event.created_at,
                    )
                    .await?;
                    handle_projection_outcome("depth_limit_run_failed", run_id, &outcome);
                }
                let outcome = task::update_task_error(
                    db,
                    task_id,
                    TaskStatus::Failed,
                    Some(&error),
                    event.created_at,
                    Some(event.created_at),
                )
                .await?;
                handle_projection_outcome("depth_limit_task_failed", task_id, &outcome);
                Ok(())
            }
            TaskEventPayload::DeliveryQueued { delivery }
            | TaskEventPayload::DeliveryCancelled { delivery, .. } => {
                task_delivery::upsert_delivery(db, delivery).await
            }
            TaskEventPayload::DeliveryStarted { delivery, attempt }
            | TaskEventPayload::DeliveryDelivered { delivery, attempt }
            | TaskEventPayload::DeliveryFailed { delivery, attempt } => {
                task_delivery::upsert_delivery(db, delivery).await?;
                task_delivery::upsert_attempt(db, attempt).await
            }
            TaskEventPayload::WriteLockAcquired { lock }
            | TaskEventPayload::WriteLockReleased { lock, .. }
            | TaskEventPayload::WriteLockExpired { lock, .. } => {
                task_write_lock::upsert_lock(db, lock).await
            }
            TaskEventPayload::WriteLockBlocked { .. } => Ok(()),
        }
    }
}

async fn task_is_terminal_db<C: ConnectionTrait>(db: &C, task_id: &str) -> Result<bool> {
    Ok(task::find_task_by_id(db, task_id)
        .await?
        .is_some_and(|model| is_terminal_task_status_db(model.status.as_str())))
}

async fn task_has_nonterminal_run_db<C: ConnectionTrait>(db: &C, task_id: &str) -> Result<bool> {
    let runs = task_run::list_runs_by_task(db, task_id).await?;
    Ok(runs.into_iter().any(|run| {
        task_run_status_from_db(run.status.as_str()).is_some_and(|status| !status.is_terminal())
    }))
}

fn task_error_is_cancellation(error: Option<&TaskError>) -> bool {
    let Some(error) = error else {
        return false;
    };
    if error.class == TaskErrorClass::Cancelled {
        return true;
    }
    let code = error.code.to_ascii_lowercase();
    matches!(
        code.as_str(),
        "task_cancelled" | "task_run_cancelled" | "child_turn_cancelled" | "cancelled"
    ) || code.contains("cancel")
}

fn handle_projection_outcome(scope: &str, entity_id: &str, outcome: &ProjectionWriteOutcome) {
    match outcome {
        ProjectionWriteOutcome::Applied
        | ProjectionWriteOutcome::NoopAlreadyTerminal
        | ProjectionWriteOutcome::NoopDuplicateTerminal => {}
        ProjectionWriteOutcome::InvariantViolation { reason } => {
            warn!(
                scope = scope,
                entity_id = entity_id,
                reason = reason.as_str(),
                "task projector invariant violation"
            );
        }
    }
}
