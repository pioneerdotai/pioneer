use anyhow::{Context, Result};
use pioneer_protocol::{
    TaskError, TaskErrorClass, TaskResult, TaskResultCandidate, TaskResultCandidateStatus,
    TaskResultReviewDecision, TaskResultReviewEventKind, TaskResultReviewerKind, TaskRunStatus,
    TaskRunThreadBindingKind, TaskRunTurnKind, TaskRunTurnStatus, TaskStatus, TaskValue,
};
use sea_orm::ConnectionTrait;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use tracing::warn;

use crate::convention::{is_terminal_task_status_db, task_run_status_from_db};
use crate::repositories::{
    ProjectionWriteOutcome, task, task_agent_spec, task_delivery, task_dependency,
    task_result_candidate, task_result_review_event, task_run, task_run_execution,
    task_run_thread_binding, task_run_turn, task_trigger, task_write_lock, thread_lineage,
};
use crate::task_events::{AppendedTaskEvent, TaskEventPayload};
use crate::util::unix_to_datetime;

type ProjectFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

// Keep each event branch out of the outer projector future; task payloads are large.
fn project_future<'a, F>(future: F) -> ProjectFuture<'a>
where
    F: Future<Output = Result<()>> + Send + 'a,
{
    Box::pin(future)
}

#[derive(Clone, Default)]
pub struct TaskProjector;

impl TaskProjector {
    pub fn new() -> Self {
        Self
    }

    pub async fn project<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        event: &AppendedTaskEvent,
    ) -> Result<()> {
        let created_at = event.created_at;
        let future: ProjectFuture<'_> = match &event.payload {
            TaskEventPayload::TaskCreated { task: task_model } => {
                project_future(task::upsert_task(db, task_model))
            }
            TaskEventPayload::TriggerCreated { trigger } => {
                project_future(task_trigger::upsert_trigger(db, trigger))
            }
            TaskEventPayload::DependencyCreated { dependency } => {
                project_future(task_dependency::upsert_dependency(db, dependency))
            }
            TaskEventPayload::AgentSpecCreated { agent_spec } => {
                project_future(task_agent_spec::upsert_agent_spec(db, agent_spec))
            }
            TaskEventPayload::TaskScheduled {
                task_id,
                trigger_id,
                next_fire_at,
            } => project_future(async move {
                task_trigger::update_trigger_schedule(
                    db,
                    trigger_id,
                    pioneer_protocol::TaskTriggerStatus::Active,
                    next_fire_at.map(unix_to_datetime),
                    None,
                    created_at,
                )
                .await?;
                let outcome =
                    task::update_task_status(db, task_id, TaskStatus::Scheduled, created_at, None)
                        .await?;
                handle_projection_outcome("task_scheduled", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskQueued { task_id, .. } => project_future(async move {
                let outcome =
                    task::update_task_status(db, task_id, TaskStatus::Queued, created_at, None)
                        .await?;
                handle_projection_outcome("task_queued", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::RunCreated { run, agent_spec } => project_future(async move {
                task_run::upsert_run(db, run).await?;
                if let Some(agent_spec) = agent_spec {
                    task_agent_spec::upsert_agent_spec(db, agent_spec).await?;
                }
                Ok(())
            }),
            TaskEventPayload::RunStarted {
                task_id,
                run_id,
                started_at,
            } => project_future(async move {
                let started_at = unix_to_datetime(*started_at);
                let run_outcome = task_run::update_run_started(db, run_id, started_at)
                    .await
                    .with_context(|| format!("failed to project task run `{run_id}` started"))?;
                handle_projection_outcome("run_started", run_id, &run_outcome)?;
                if matches!(run_outcome, ProjectionWriteOutcome::Applied) {
                    let task_outcome = task::update_task_status(
                        db,
                        task_id,
                        TaskStatus::Running,
                        started_at,
                        None,
                    )
                    .await?;
                    handle_projection_outcome("run_started_task_status", task_id, &task_outcome)?;
                }
                Ok(())
            }),
            TaskEventPayload::Progress { .. } => project_future(async { Ok(()) }),
            TaskEventPayload::RunCompleted {
                task_id,
                run_id,
                result,
                completed_at,
            } => project_future(async move {
                let completed_at = unix_to_datetime(*completed_at);
                let outcome = task_run::update_run_result(
                    db,
                    run_id,
                    TaskRunStatus::Succeeded,
                    result.as_ref(),
                    completed_at,
                )
                .await?;
                handle_projection_outcome("run_completed", run_id, &outcome)?;
                if let Some(result) = result {
                    project_legacy_auto_accepted_candidate(
                        db,
                        task_id,
                        run_id,
                        result,
                        completed_at.timestamp(),
                    )
                    .await?;
                }
                Ok(())
            }),
            TaskEventPayload::RunFailed {
                task_id: _,
                run_id,
                error,
                completed_at,
            } => project_future(async move {
                let completed_at = unix_to_datetime(*completed_at);
                let status = if task_error_is_cancellation(error.as_ref()) {
                    TaskRunStatus::Cancelled
                } else {
                    TaskRunStatus::Failed
                };
                let outcome =
                    task_run::update_run_error(db, run_id, status, error.as_ref(), completed_at)
                        .await?;
                handle_projection_outcome("run_failed", run_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::RunRetryScheduled { retry_run, .. } => project_future(async move {
                task_run::upsert_run(db, retry_run).await?;
                let outcome = task::update_task_status(
                    db,
                    retry_run.task_id.as_str(),
                    TaskStatus::Queued,
                    created_at,
                    None,
                )
                .await?;
                handle_projection_outcome(
                    "run_retry_scheduled",
                    retry_run.task_id.as_str(),
                    &outcome,
                )?;
                Ok(())
            }),
            TaskEventPayload::RunRetryExhausted { .. } => project_future(async { Ok(()) }),
            TaskEventPayload::RunCancelled {
                task_id: _,
                run_id,
                reason,
                cancelled_at,
            } => project_future(async move {
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
                handle_projection_outcome("run_cancelled", run_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskCompleted {
                task_id,
                result,
                completed_at,
            } => project_future(async move {
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
                handle_projection_outcome("task_completed", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskFailed {
                task_id,
                error,
                completed_at,
            } => project_future(async move {
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
                handle_projection_outcome("task_failed", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskCancelled {
                task_id,
                reason,
                completed_at,
            } => project_future(async move {
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
                handle_projection_outcome("task_cancelled", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskDetached {
                task: task_model, ..
            } => project_future(async move {
                if task_is_terminal_db(db, task_model.id.as_str()).await? {
                    return Ok(());
                }
                task::upsert_task(db, task_model).await
            }),
            TaskEventPayload::TaskUpdated {
                task: task_model,
                trigger,
                agent_spec,
                ..
            } => project_future(async move {
                if task_is_terminal_db(db, task_model.id.as_str()).await? {
                    return Ok(());
                }
                task::upsert_task(db, task_model).await?;
                if let Some(trigger) = trigger {
                    task_trigger::upsert_trigger(db, trigger).await?;
                }
                if let Some(agent_spec) = agent_spec {
                    task_agent_spec::upsert_agent_spec(db, agent_spec).await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskRescheduled { trigger, .. } => project_future(async move {
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
                    created_at,
                    None,
                )
                .await?;
                handle_projection_outcome("task_rescheduled", trigger.task_id.as_str(), &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskPaused {
                task: task_model,
                triggers,
                ..
            }
            | TaskEventPayload::TaskResumed {
                task: task_model,
                triggers,
                ..
            } => project_future(async move {
                if task_is_terminal_db(db, task_model.id.as_str()).await? {
                    return Ok(());
                }
                task::upsert_task(db, task_model).await?;
                for trigger in triggers {
                    task_trigger::upsert_trigger(db, trigger).await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskRecovered { .. } => project_future(async { Ok(()) }),
            TaskEventPayload::ChildThreadLinked { lineage } => project_future(async move {
                thread_lineage::upsert_lineage(db, lineage).await?;
                let execution =
                    task_run_execution::find_execution_by_run(db, lineage.task_run_id.as_str())
                        .await?;
                let execution_id = execution.map(|execution| execution.id);
                task_run_thread_binding::upsert_binding(
                    db,
                    task_run_thread_binding::NewTaskRunThreadBinding {
                        id: format!("trb_primary_{}", lineage.task_run_id),
                        task_id: lineage.task_id.clone(),
                        run_id: lineage.task_run_id.clone(),
                        execution_id: execution_id.clone(),
                        thread_id: lineage.child_thread_id.clone(),
                        binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
                        created_at: lineage.created_at,
                    },
                )
                .await?;
                task_run_turn::upsert_turn(
                    db,
                    task_run_turn::NewTaskRunTurn {
                        id: format!("trt_{}", lineage.child_turn_id),
                        task_id: lineage.task_id.clone(),
                        run_id: lineage.task_run_id.clone(),
                        execution_id,
                        thread_id: lineage.child_thread_id.clone(),
                        turn_id: lineage.child_turn_id.clone(),
                        kind: TaskRunTurnKind::Initial,
                        round: 0,
                        sequence: 0,
                        status: TaskRunTurnStatus::InProgress,
                        reviews_candidate_id: None,
                        requested_by_candidate_id: None,
                        requested_by_review_event_id: None,
                        created_at: lineage.created_at,
                        started_at: Some(lineage.created_at),
                        completed_at: None,
                    },
                )
                .await?;
                Ok(())
            }),
            TaskEventPayload::TaskRunThreadBindingCreated { binding } => {
                project_future(async move {
                    task_run_thread_binding::upsert_binding(
                        db,
                        task_run_thread_binding::NewTaskRunThreadBinding {
                            id: binding.id.clone(),
                            task_id: binding.task_id.clone(),
                            run_id: binding.run_id.clone(),
                            execution_id: binding.execution_id.clone(),
                            thread_id: binding.thread_id.clone(),
                            binding_kind: binding.binding_kind,
                            created_at: binding.created_at,
                        },
                    )
                    .await
                })
            }
            TaskEventPayload::TaskRunTurnStarted { task_run_turn }
            | TaskEventPayload::TaskRunTurnCompleted { task_run_turn }
            | TaskEventPayload::TaskRunTurnFailed { task_run_turn, .. } => {
                project_future(async move {
                    task_run_turn::upsert_turn(
                        db,
                        task_run_turn::NewTaskRunTurn {
                            id: task_run_turn.id.clone(),
                            task_id: task_run_turn.task_id.clone(),
                            run_id: task_run_turn.run_id.clone(),
                            execution_id: task_run_turn.execution_id.clone(),
                            thread_id: task_run_turn.thread_id.clone(),
                            turn_id: task_run_turn.turn_id.clone(),
                            kind: task_run_turn.kind,
                            round: task_run_turn.round,
                            sequence: task_run_turn.sequence,
                            status: task_run_turn.status,
                            reviews_candidate_id: task_run_turn.reviews_candidate_id.clone(),
                            requested_by_candidate_id: task_run_turn
                                .requested_by_candidate_id
                                .clone(),
                            requested_by_review_event_id: task_run_turn
                                .requested_by_review_event_id
                                .clone(),
                            created_at: task_run_turn.created_at,
                            started_at: task_run_turn.started_at,
                            completed_at: task_run_turn.completed_at,
                        },
                    )
                    .await
                })
            }
            TaskEventPayload::TaskResultCandidateCreated { candidate } => {
                project_future(async move { upsert_task_result_candidate(db, candidate).await })
            }
            TaskEventPayload::TaskResultReviewEventRecorded { review_event } => {
                project_future(async move {
                    task_result_review_event::upsert_review_event(
                        db,
                        task_result_review_event::NewTaskResultReviewEvent {
                            id: review_event.id.clone(),
                            candidate_id: review_event.candidate_id.clone(),
                            task_id: review_event.task_id.clone(),
                            run_id: review_event.run_id.clone(),
                            task_run_turn_id: review_event.task_run_turn_id.clone(),
                            reviewer_kind: review_event.reviewer_kind,
                            reviewer_thread_id: review_event.reviewer_thread_id.clone(),
                            reviewer_turn_id: review_event.reviewer_turn_id.clone(),
                            reviewer_user_id: review_event.reviewer_user_id.clone(),
                            reviewer_agent_spec_id: review_event.reviewer_agent_spec_id.clone(),
                            event_kind: review_event.event_kind,
                            decision: review_event.decision,
                            feedback_text: review_event.feedback_text.clone(),
                            feedback: review_event.feedback.clone(),
                            confidence: review_event.confidence,
                            supersedes_review_event_id: review_event
                                .supersedes_review_event_id
                                .clone(),
                            next_task_run_turn_id: review_event.next_task_run_turn_id.clone(),
                            created_at: review_event.created_at,
                        },
                    )
                    .await
                })
            }
            TaskEventPayload::TaskResultCandidateAccepted {
                candidate,
                review_event_id,
            } => project_future(async move {
                upsert_task_result_candidate(db, candidate).await?;
                let resolved_at = candidate.resolved_at.unwrap_or(created_at.timestamp());
                task_result_candidate::update_candidate_resolution(
                    db,
                    candidate.id.as_str(),
                    TaskResultCandidateStatus::Accepted,
                    Some(review_event_id.as_str()),
                    Some(resolved_at),
                    candidate.updated_at.max(resolved_at),
                )
                .await?;
                Ok(())
            }),
            TaskEventPayload::TaskResultCandidateRejected {
                candidate,
                review_event_id,
            } => project_future(async move {
                upsert_task_result_candidate(db, candidate).await?;
                let resolved_at = candidate.resolved_at.unwrap_or(created_at.timestamp());
                task_result_candidate::update_candidate_resolution(
                    db,
                    candidate.id.as_str(),
                    TaskResultCandidateStatus::Rejected,
                    Some(review_event_id.as_str()),
                    Some(resolved_at),
                    candidate.updated_at.max(resolved_at),
                )
                .await?;
                Ok(())
            }),
            TaskEventPayload::TaskRevisionRequested {
                task_id,
                run_id,
                previous_candidate_id,
                requested_by_review_event_id,
                task_run_turn_id,
                thread_id,
                turn_id,
                round,
                requested_at,
                ..
            } => project_future(async move {
                task_run_turn::upsert_turn(
                    db,
                    task_run_turn::NewTaskRunTurn {
                        id: task_run_turn_id.clone(),
                        task_id: task_id.clone(),
                        run_id: run_id.clone(),
                        execution_id: None,
                        thread_id: thread_id.clone(),
                        turn_id: turn_id.clone(),
                        kind: TaskRunTurnKind::Revision,
                        round: *round,
                        sequence: *round,
                        status: TaskRunTurnStatus::InProgress,
                        reviews_candidate_id: None,
                        requested_by_candidate_id: Some(previous_candidate_id.clone()),
                        requested_by_review_event_id: Some(requested_by_review_event_id.clone()),
                        created_at: *requested_at,
                        started_at: Some(*requested_at),
                        completed_at: None,
                    },
                )
                .await
            }),
            TaskEventPayload::TaskRunEnteredReview {
                run_id, entered_at, ..
            } => project_future(async move {
                let outcome = task_run::update_run_status(
                    db,
                    run_id,
                    TaskRunStatus::Waiting,
                    unix_to_datetime(*entered_at),
                )
                .await?;
                handle_projection_outcome("task_run_entered_review", run_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::DepthLimitExceeded {
                task_id,
                run_id,
                depth,
                max_depth,
            } => project_future(async move {
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
                        created_at,
                    )
                    .await?;
                    handle_projection_outcome("depth_limit_run_failed", run_id, &outcome)?;
                }
                let outcome = task::update_task_error(
                    db,
                    task_id,
                    TaskStatus::Failed,
                    Some(&error),
                    created_at,
                    Some(created_at),
                )
                .await?;
                handle_projection_outcome("depth_limit_task_failed", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::DeliveryQueued { delivery }
            | TaskEventPayload::DeliveryCancelled { delivery, .. } => {
                project_future(task_delivery::upsert_delivery(db, delivery))
            }
            TaskEventPayload::DeliveryStarted { delivery, attempt }
            | TaskEventPayload::DeliveryDelivered { delivery, attempt }
            | TaskEventPayload::DeliveryFailed { delivery, attempt } => {
                project_future(async move {
                    task_delivery::upsert_delivery(db, delivery).await?;
                    task_delivery::upsert_attempt(db, attempt).await
                })
            }
            TaskEventPayload::WriteLockAcquired { lock }
            | TaskEventPayload::WriteLockReleased { lock, .. }
            | TaskEventPayload::WriteLockExpired { lock, .. } => {
                project_future(task_write_lock::upsert_lock(db, lock))
            }
            TaskEventPayload::WriteLockBlocked { .. } => project_future(async { Ok(()) }),
        };
        future.await
    }
}

async fn task_is_terminal_db<C: ConnectionTrait + Sync>(db: &C, task_id: &str) -> Result<bool> {
    Ok(task::find_task_by_id(db, task_id)
        .await?
        .is_some_and(|model| is_terminal_task_status_db(model.status.as_str())))
}

async fn task_has_nonterminal_run_db<C: ConnectionTrait + Sync>(
    db: &C,
    task_id: &str,
) -> Result<bool> {
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

async fn upsert_task_result_candidate<C: ConnectionTrait>(
    db: &C,
    candidate: &TaskResultCandidate,
) -> Result<()> {
    task_result_candidate::upsert_candidate(
        db,
        task_result_candidate::NewTaskResultCandidate {
            id: candidate.id.clone(),
            task_id: candidate.task_id.clone(),
            run_id: candidate.run_id.clone(),
            task_run_turn_id: candidate.task_run_turn_id.clone(),
            thread_id: candidate.thread_id.clone(),
            turn_id: candidate.turn_id.clone(),
            round: candidate.round,
            status: candidate.status,
            result: candidate.result.clone(),
            extraction_error: candidate.extraction_error.clone(),
            summary: candidate.summary.clone(),
            diagnostics: candidate.diagnostics.clone(),
            final_review_event_id: candidate.final_review_event_id.clone(),
            created_at: candidate.created_at,
            updated_at: candidate.updated_at,
            resolved_at: candidate.resolved_at,
        },
    )
    .await
}

async fn project_legacy_auto_accepted_candidate<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    run_id: &str,
    result: &TaskResult,
    completed_at: i64,
) -> Result<()> {
    if task_result_candidate::find_candidate_by_run_and_status(
        db,
        run_id,
        TaskResultCandidateStatus::Accepted,
    )
    .await?
    .is_some()
    {
        return Ok(());
    }

    let Some(task_run_turn) = task_run_turn::find_latest_turn_by_run(db, run_id).await? else {
        return Ok(());
    };
    if task_result_candidate::find_candidate_by_turn(db, task_run_turn.id.as_str())
        .await?
        .is_some()
    {
        return Ok(());
    }

    let candidate_id = format!("trc_{run_id}");
    let review_event_id = format!("trre_auto_{run_id}");
    task_result_candidate::upsert_candidate(
        db,
        task_result_candidate::NewTaskResultCandidate {
            id: candidate_id.clone(),
            task_id: task_id.to_owned(),
            run_id: run_id.to_owned(),
            task_run_turn_id: task_run_turn.id.clone(),
            thread_id: task_run_turn.thread_id.clone(),
            turn_id: task_run_turn.turn_id.clone(),
            round: u32::try_from(task_run_turn.round)
                .context("legacy task run turn round is out of range")?,
            status: TaskResultCandidateStatus::Accepted,
            result: Some(result.clone()),
            extraction_error: None,
            summary: result.summary.clone(),
            diagnostics: Vec::new(),
            final_review_event_id: Some(review_event_id.clone()),
            created_at: completed_at,
            updated_at: completed_at,
            resolved_at: Some(completed_at),
        },
    )
    .await?;
    task_result_review_event::upsert_review_event(
        db,
        task_result_review_event::NewTaskResultReviewEvent {
            id: review_event_id,
            candidate_id,
            task_id: task_id.to_owned(),
            run_id: run_id.to_owned(),
            task_run_turn_id: task_run_turn.id,
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: completed_at,
        },
    )
    .await
}

fn handle_projection_outcome(
    scope: &str,
    entity_id: &str,
    outcome: &ProjectionWriteOutcome,
) -> Result<()> {
    match outcome {
        ProjectionWriteOutcome::Applied
        | ProjectionWriteOutcome::NoopAlreadyStarted
        | ProjectionWriteOutcome::NoopAlreadyTerminal
        | ProjectionWriteOutcome::NoopDuplicateTerminal => Ok(()),
        ProjectionWriteOutcome::InvariantViolation { reason } => {
            warn!(
                scope = scope,
                entity_id = entity_id,
                reason = reason.as_str(),
                "task projector invariant violation"
            );
            anyhow::bail!(
                "task projector invariant violation in {scope} for `{entity_id}`: {reason}"
            )
        }
    }
}
