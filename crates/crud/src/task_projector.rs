use anyhow::{Context, Result};
use pioneer_protocol::{
    PersistedActorRef, TaskAgentReviewPolicy, TaskDelivery, TaskDeliveryMode, TaskDeliveryStatus,
    TaskError, TaskErrorClass, TaskResultCandidateStatus, TaskResultReviewerRef, TaskRunStatus,
    TaskRunThreadBindingKind, TaskRunTurnKind, TaskRunTurnStatus, TaskStatus, TaskThreadLineage,
    TaskValue, ThreadLineage,
};
use sea_orm::ConnectionTrait;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use tracing::warn;

use crate::convention::{is_terminal_task_status_db, task_run_status_from_db, task_status_from_db};
use crate::repositories::{
    ProjectionWriteOutcome, native_terminal_effect_outbox, task, task_actor_contract,
    task_agent_spec, task_delivery, task_dependency, task_result_candidate,
    task_result_review_event, task_run, task_run_execution, task_run_thread_binding, task_run_turn,
    task_trigger, task_write_lock, thread_lineage,
};
use crate::task_events::{AppendedTaskEvent, TaskEventPayload};
use crate::util::{optional_typed_json_from_db, unix_to_datetime};

type ProjectFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

// Keep each event branch out of the outer projector future; task payloads are large.
fn project_future<'a, F>(future: F) -> ProjectFuture<'a>
where
    F: Future<Output = Result<()>> + Send + 'a,
{
    Box::pin(future)
}

fn target_lineage_from_legacy(lineage: &ThreadLineage) -> TaskThreadLineage {
    TaskThreadLineage {
        child_thread_id: lineage.child_thread_id.clone(),
        parent_thread_id: lineage.parent_thread_id.clone(),
        root_thread_id: lineage.root_thread_id.clone(),
        depth: lineage.depth,
        origin_kind: Some("task_run".to_owned()),
        created_by_thread_id: Some(lineage.parent_thread_id.clone()),
        created_by_turn_id: lineage.parent_turn_id.clone(),
        created_at: lineage.created_at,
    }
}

#[derive(Clone, Default)]
pub struct TaskProjector;

#[derive(Clone, Debug, Default)]
pub(crate) struct PreparedTaskProjection {
    task: Option<task::PreparedTaskProjection>,
    triggers: Vec<task_trigger::PreparedTaskTriggerProjection>,
    dependency: Option<task_dependency::PreparedTaskDependencyProjection>,
    agent_specs: Vec<task_agent_spec::PreparedTaskAgentSpecProjection>,
    run: Option<task_run::PreparedTaskRunProjection>,
    run_result_json: Option<String>,
    run_error_json: Option<String>,
    run_error_status: Option<TaskRunStatus>,
    task_result_json: Option<String>,
    task_error_json: Option<String>,
    task_error_status: Option<TaskStatus>,
    delivery: Option<task_delivery::PreparedTaskDeliveryProjection>,
    delivery_attempt: Option<task_delivery::PreparedTaskDeliveryAttemptProjection>,
}

impl PreparedTaskProjection {
    pub(crate) fn prepare(payload: &TaskEventPayload) -> Result<Self> {
        let mut prepared = Self::default();
        match payload {
            TaskEventPayload::TaskCreated { task: task_model }
            | TaskEventPayload::TaskDetached {
                task: task_model, ..
            } => {
                prepared.task = Some(task::prepare_task_projection(task_model)?);
            }
            TaskEventPayload::TriggerCreated { trigger }
            | TaskEventPayload::TaskRescheduled { trigger, .. } => {
                prepared
                    .triggers
                    .push(task_trigger::prepare_trigger_projection(trigger)?);
            }
            TaskEventPayload::DependencyCreated { dependency } => {
                prepared.dependency =
                    Some(task_dependency::prepare_dependency_projection(dependency)?);
            }
            TaskEventPayload::AgentSpecCreated { agent_spec } => {
                prepared
                    .agent_specs
                    .push(task_agent_spec::prepare_agent_spec_projection(agent_spec)?);
            }
            TaskEventPayload::RunCreated { run, agent_spec } => {
                prepared.run = Some(task_run::prepare_run_projection(run)?);
                if let Some(agent_spec) = agent_spec {
                    prepared
                        .agent_specs
                        .push(task_agent_spec::prepare_agent_spec_projection(agent_spec)?);
                }
            }
            TaskEventPayload::RunRetryScheduled { retry_run, .. } => {
                prepared.run = Some(task_run::prepare_run_projection(retry_run)?);
            }
            TaskEventPayload::RunCompleted { result, .. } => {
                prepared.run_result_json = task_run::prepare_run_result_json(result.as_ref())?;
            }
            TaskEventPayload::RunFailed { error, .. }
            | TaskEventPayload::RunBlocked { error, .. } => {
                prepared.run_error_json = task_run::prepare_run_error_json(error.as_ref())?;
                prepared.run_error_status =
                    Some(if matches!(payload, TaskEventPayload::RunBlocked { .. }) {
                        TaskRunStatus::Blocked
                    } else if task_error_is_cancellation(error.as_ref()) {
                        TaskRunStatus::Cancelled
                    } else {
                        TaskRunStatus::Failed
                    });
            }
            TaskEventPayload::RunCancelled { run_id, reason, .. } => {
                let error = reason.as_ref().map(|reason| TaskError {
                    code: "task_run_cancelled".to_owned(),
                    message: reason.clone(),
                    class: TaskErrorClass::Cancelled,
                    details: None,
                    failed_run_id: Some(run_id.clone()),
                });
                prepared.run_error_json = task_run::prepare_run_error_json(error.as_ref())?;
                prepared.run_error_status = Some(TaskRunStatus::Cancelled);
            }
            TaskEventPayload::TaskCompleted { result, .. } => {
                prepared.task_result_json = task::prepare_task_result_json(result.as_ref())?;
            }
            TaskEventPayload::TaskFailed { error, .. }
            | TaskEventPayload::TaskBlocked { error, .. } => {
                prepared.task_error_json = task::prepare_task_error_json(error.as_ref())?;
                prepared.task_error_status =
                    Some(if matches!(payload, TaskEventPayload::TaskBlocked { .. }) {
                        TaskStatus::Blocked
                    } else if task_error_is_cancellation(error.as_ref()) {
                        TaskStatus::Cancelled
                    } else {
                        TaskStatus::Failed
                    });
            }
            TaskEventPayload::TaskCancelled { reason, .. } => {
                let error = reason.as_ref().map(|reason| TaskError {
                    code: "task_cancelled".to_owned(),
                    message: reason.clone(),
                    class: TaskErrorClass::Cancelled,
                    details: None,
                    failed_run_id: None,
                });
                prepared.task_error_json = task::prepare_task_error_json(error.as_ref())?;
                prepared.task_error_status = Some(TaskStatus::Cancelled);
            }
            TaskEventPayload::TaskUpdated {
                task: task_model,
                trigger,
                agent_spec,
                ..
            } => {
                prepared.task = Some(task::prepare_task_projection(task_model)?);
                if let Some(trigger) = trigger {
                    prepared
                        .triggers
                        .push(task_trigger::prepare_trigger_projection(trigger)?);
                }
                if let Some(agent_spec) = agent_spec {
                    prepared
                        .agent_specs
                        .push(task_agent_spec::prepare_agent_spec_projection(agent_spec)?);
                }
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
                prepared.task = Some(task::prepare_task_projection(task_model)?);
                prepared.triggers = triggers
                    .iter()
                    .map(task_trigger::prepare_trigger_projection)
                    .collect::<Result<Vec<_>>>()?;
            }
            TaskEventPayload::DepthLimitExceeded {
                run_id,
                depth,
                max_depth,
                ..
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
                let error_json = task::prepare_task_error_json(Some(&error))?;
                prepared.task_error_json = error_json.clone();
                prepared.run_error_json = error_json;
                prepared.task_error_status = Some(TaskStatus::Failed);
                prepared.run_error_status = Some(TaskRunStatus::Failed);
            }
            TaskEventPayload::DeliveryQueued { delivery } => {
                prepared.delivery = Some(task_delivery::prepare_delivery_projection(delivery)?);
            }
            TaskEventPayload::DeliveryCancelled {
                delivery, attempt, ..
            } => {
                prepared.delivery = Some(task_delivery::prepare_delivery_projection(delivery)?);
                prepared.delivery_attempt = attempt
                    .as_ref()
                    .map(task_delivery::prepare_attempt_projection);
            }
            TaskEventPayload::DeliveryStarted { delivery, attempt }
            | TaskEventPayload::DeliveryDelivered { delivery, attempt }
            | TaskEventPayload::DeliveryFailed { delivery, attempt } => {
                prepared.delivery = Some(task_delivery::prepare_delivery_projection(delivery)?);
                prepared.delivery_attempt =
                    Some(task_delivery::prepare_attempt_projection(attempt));
            }
            _ => {}
        }
        Ok(prepared)
    }
}

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
        let candidate_gate_resolution = event.candidate_gate_resolution.clone();
        let candidate_projection = event.candidate_projection.clone();
        let review_projection = event.review_projection.clone();
        let delivery_authority = event.delivery_authority.clone();
        let task_projection = event.projection.task.clone();
        let trigger_projections = event.projection.triggers.clone();
        let dependency_projection = event.projection.dependency.clone();
        let agent_spec_projections = event.projection.agent_specs.clone();
        let run_projection = event.projection.run.clone();
        let run_result_json = event.projection.run_result_json.clone();
        let run_error_json = event.projection.run_error_json.clone();
        let run_error_status = event.projection.run_error_status;
        let task_result_json = event.projection.task_result_json.clone();
        let task_error_json = event.projection.task_error_json.clone();
        let task_error_status = event.projection.task_error_status;
        let delivery_projection = event.projection.delivery.clone();
        let delivery_attempt_projection = event.projection.delivery_attempt.clone();
        let future: ProjectFuture<'_> = match &event.payload {
            TaskEventPayload::TaskCreated { task: task_model } => project_future(async move {
                task::upsert_prepared_task(
                    db,
                    task_projection.with_context(|| {
                        format!("task `{}` projection was not prepared", task_model.id)
                    })?,
                )
                .await
            }),
            TaskEventPayload::TriggerCreated { trigger } => project_future(async move {
                task_trigger::upsert_prepared_trigger(
                    db,
                    trigger_projections.into_iter().next().with_context(|| {
                        format!("task trigger `{}` projection was not prepared", trigger.id)
                    })?,
                )
                .await
            }),
            TaskEventPayload::DependencyCreated { dependency } => project_future(async move {
                task_dependency::upsert_prepared_dependency(
                    db,
                    dependency_projection.with_context(|| {
                        format!(
                            "task dependency `{}` projection was not prepared",
                            dependency.id
                        )
                    })?,
                )
                .await
            }),
            TaskEventPayload::AgentSpecCreated { agent_spec } => project_future(async move {
                task_agent_spec::upsert_prepared_agent_spec(
                    db,
                    agent_spec_projections.into_iter().next().with_context(|| {
                        format!(
                            "task agent spec `{}` projection was not prepared",
                            agent_spec.id
                        )
                    })?,
                )
                .await
            }),
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
                task_run::upsert_prepared_run(
                    db,
                    run_projection.with_context(|| {
                        format!("task run `{}` projection was not prepared", run.id)
                    })?,
                )
                .await?;
                if agent_spec.is_some() {
                    task_agent_spec::upsert_prepared_agent_spec(
                        db,
                        agent_spec_projections
                            .into_iter()
                            .next()
                            .context("task run agent spec projection was not prepared")?,
                    )
                    .await?;
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
                task_id: _,
                run_id,
                result,
                completed_at,
            } => project_future(async move {
                let completed_at = unix_to_datetime(*completed_at);
                let outcome = task_run::update_run_result_json(
                    db,
                    run_id,
                    TaskRunStatus::Succeeded,
                    run_result_json,
                    completed_at,
                )
                .await?;
                handle_projection_outcome("run_completed", run_id, &outcome)?;
                if result.is_some()
                    && project_legacy_auto_accepted_candidate(
                        db,
                        run_id,
                        candidate_projection,
                        review_projection,
                    )
                    .await?
                    && let Some(prepared) = candidate_gate_resolution
                {
                    native_terminal_effect_outbox::apply_prepared_gate_resolution(db, prepared)
                        .await?;
                }
                Ok(())
            }),
            TaskEventPayload::RunFailed {
                task_id: _,
                run_id,
                error: _,
                completed_at,
            } => project_future(async move {
                let completed_at = unix_to_datetime(*completed_at);
                let status = run_error_status
                    .context("task run failure status projection was not prepared")?;
                let outcome = task_run::update_run_error_json(
                    db,
                    run_id,
                    status,
                    run_error_json,
                    completed_at,
                )
                .await?;
                handle_projection_outcome("run_failed", run_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::RunBlocked {
                task_id: _,
                run_id,
                error: _,
                blocked_at,
            } => project_future(async move {
                let blocked_at = unix_to_datetime(*blocked_at);
                let outcome = task_run::update_run_error_json(
                    db,
                    run_id,
                    run_error_status
                        .context("blocked task run status projection was not prepared")?,
                    run_error_json,
                    blocked_at,
                )
                .await?;
                handle_projection_outcome("run_blocked", run_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::RunRetryScheduled { retry_run, .. } => project_future(async move {
                task_run::upsert_prepared_run(
                    db,
                    run_projection.with_context(|| {
                        format!(
                            "retry task run `{}` projection was not prepared",
                            retry_run.id
                        )
                    })?,
                )
                .await?;
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
                reason: _,
                cancelled_at,
            } => project_future(async move {
                let cancelled_at = unix_to_datetime(*cancelled_at);
                let outcome = task_run::update_run_error_json(
                    db,
                    run_id,
                    run_error_status
                        .context("cancelled task run status projection was not prepared")?,
                    run_error_json,
                    cancelled_at,
                )
                .await?;
                handle_projection_outcome("run_cancelled", run_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskCompleted {
                task_id,
                result: _,
                completed_at,
            } => project_future(async move {
                let completed_at = unix_to_datetime(*completed_at);
                let outcome = task::update_task_result_json(
                    db,
                    task_id,
                    TaskStatus::Completed,
                    task_result_json,
                    completed_at,
                    Some(completed_at),
                )
                .await?;
                handle_projection_outcome("task_completed", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskFailed {
                task_id,
                error: _,
                completed_at,
            } => project_future(async move {
                let completed_at = unix_to_datetime(*completed_at);
                let status =
                    task_error_status.context("task failure status projection was not prepared")?;
                let outcome = task::update_task_error_json(
                    db,
                    task_id,
                    status,
                    task_error_json,
                    completed_at,
                    Some(completed_at),
                )
                .await?;
                handle_projection_outcome("task_failed", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::TaskBlocked {
                task_id,
                error: _,
                blocked_at,
            } => project_future(async move {
                let blocked_at = unix_to_datetime(*blocked_at);
                let outcome = task::update_task_error_json(
                    db,
                    task_id,
                    task_error_status.context("blocked task status projection was not prepared")?,
                    task_error_json,
                    blocked_at,
                    Some(blocked_at),
                )
                .await?;
                handle_projection_outcome("task_blocked", task_id, &outcome)?;
                if matches!(
                    &outcome,
                    ProjectionWriteOutcome::Applied | ProjectionWriteOutcome::NoopDuplicateTerminal
                ) {
                    task_trigger::pause_active_triggers_for_blocked_task(db, task_id, blocked_at)
                        .await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskCancelled {
                task_id,
                reason: _,
                completed_at,
            } => project_future(async move {
                let completed_at = unix_to_datetime(*completed_at);
                let outcome = task::update_task_error_json(
                    db,
                    task_id,
                    task_error_status
                        .context("cancelled task status projection was not prepared")?,
                    task_error_json,
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
                task::upsert_prepared_task(
                    db,
                    task_projection.with_context(|| {
                        format!(
                            "detached task `{}` projection was not prepared",
                            task_model.id
                        )
                    })?,
                )
                .await
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
                task::upsert_prepared_task(
                    db,
                    task_projection.with_context(|| {
                        format!(
                            "updated task `{}` projection was not prepared",
                            task_model.id
                        )
                    })?,
                )
                .await?;
                let mut trigger_projections = trigger_projections.into_iter();
                if trigger.is_some() {
                    task_trigger::upsert_prepared_trigger(
                        db,
                        trigger_projections
                            .next()
                            .context("updated task trigger projection was not prepared")?,
                    )
                    .await?;
                }
                if agent_spec.is_some() {
                    task_agent_spec::upsert_prepared_agent_spec(
                        db,
                        agent_spec_projections
                            .into_iter()
                            .next()
                            .context("updated task agent spec projection was not prepared")?,
                    )
                    .await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskRescheduled { trigger, .. } => project_future(async move {
                task_trigger::upsert_prepared_trigger(
                    db,
                    trigger_projections.into_iter().next().with_context(|| {
                        format!(
                            "rescheduled trigger `{}` projection was not prepared",
                            trigger.id
                        )
                    })?,
                )
                .await?;
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
            } => project_future(async move {
                if task_is_terminal_db(db, task_model.id.as_str()).await? {
                    return Ok(());
                }
                task::upsert_prepared_task(
                    db,
                    task_projection.with_context(|| {
                        format!(
                            "paused task `{}` projection was not prepared",
                            task_model.id
                        )
                    })?,
                )
                .await?;
                if trigger_projections.len() != triggers.len() {
                    anyhow::bail!("paused task trigger projections were not prepared completely");
                }
                for trigger in trigger_projections {
                    task_trigger::upsert_prepared_trigger(db, trigger).await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskResumed {
                task: task_model,
                triggers,
                ..
            } => project_future(async move {
                let current_status = task_status_db(db, task_model.id.as_str()).await?;
                if current_status
                    .is_some_and(|status| status.is_terminal() && status != TaskStatus::Blocked)
                {
                    return Ok(());
                }
                task::upsert_prepared_task(
                    db,
                    task_projection.with_context(|| {
                        format!(
                            "resumed task `{}` projection was not prepared",
                            task_model.id
                        )
                    })?,
                )
                .await?;
                if trigger_projections.len() != triggers.len() {
                    anyhow::bail!("resumed task trigger projections were not prepared completely");
                }
                for trigger in trigger_projections {
                    task_trigger::upsert_prepared_trigger(db, trigger).await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskRecovered { .. } => project_future(async { Ok(()) }),
            TaskEventPayload::ChildThreadLinked { lineage } => project_future(async move {
                thread_lineage::upsert_lineage(db, &target_lineage_from_legacy(lineage)).await?;
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
            TaskEventPayload::TaskThreadLineageCreated { lineage, .. } => {
                project_future(thread_lineage::upsert_lineage(db, lineage))
            }
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
            | TaskEventPayload::TaskRunTurnFailed { task_run_turn, .. }
            | TaskEventPayload::TaskRunTurnBlocked { task_run_turn, .. } => {
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
                project_future(async move {
                    task_result_candidate::upsert_candidate(
                        db,
                        candidate_projection.with_context(|| {
                            format!(
                                "task result candidate `{}` projection was not prepared before writer admission",
                                candidate.id
                            )
                        })?,
                    )
                    .await?;
                    if let Some(prepared) = candidate_gate_resolution {
                        native_terminal_effect_outbox::apply_prepared_gate_resolution(db, prepared)
                            .await?;
                    }
                    Ok(())
                })
            }
            TaskEventPayload::TaskResultReviewEventRecorded { review_event } => {
                project_future(async move {
                    task_result_review_event::upsert_review_event(
                        db,
                        review_projection.with_context(|| {
                            format!(
                                "task result review event `{}` projection was not prepared before writer admission",
                                review_event.id
                            )
                        })?,
                    )
                    .await
                })
            }
            TaskEventPayload::TaskResultCandidateAccepted {
                candidate,
                review_event_id,
            } => project_future(async move {
                task_result_candidate::upsert_candidate(
                    db,
                    candidate_projection.with_context(|| {
                        format!(
                            "accepted task result candidate `{}` projection was not prepared before writer admission",
                            candidate.id
                        )
                    })?,
                )
                .await?;
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
                if let Some(prepared) = candidate_gate_resolution {
                    native_terminal_effect_outbox::apply_prepared_gate_resolution(db, prepared)
                        .await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskResultCandidateRejected {
                candidate,
                review_event_id,
            } => project_future(async move {
                task_result_candidate::upsert_candidate(
                    db,
                    candidate_projection.with_context(|| {
                        format!(
                            "rejected task result candidate `{}` projection was not prepared before writer admission",
                            candidate.id
                        )
                    })?,
                )
                .await?;
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
                if let Some(prepared) = candidate_gate_resolution {
                    native_terminal_effect_outbox::apply_prepared_gate_resolution(db, prepared)
                        .await?;
                }
                Ok(())
            }),
            TaskEventPayload::TaskResultCandidateCancelled {
                candidate,
                review_event_id,
            } => project_future(async move {
                task_result_candidate::upsert_candidate(
                    db,
                    candidate_projection.with_context(|| {
                        format!(
                            "cancelled task result candidate `{}` projection was not prepared before writer admission",
                            candidate.id
                        )
                    })?,
                )
                .await?;
                let resolved_at = candidate.resolved_at.unwrap_or(created_at.timestamp());
                task_result_candidate::update_candidate_resolution(
                    db,
                    candidate.id.as_str(),
                    TaskResultCandidateStatus::Cancelled,
                    Some(review_event_id.as_str()),
                    Some(resolved_at),
                    candidate.updated_at.max(resolved_at),
                )
                .await?;
                if let Some(prepared) = candidate_gate_resolution {
                    native_terminal_effect_outbox::apply_prepared_gate_resolution(db, prepared)
                        .await?;
                }
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
                .await?;
                let requested_at = unix_to_datetime(*requested_at);
                let run_outcome =
                    task_run::update_run_status(db, run_id, TaskRunStatus::Running, requested_at)
                        .await?;
                handle_projection_outcome(
                    "task_revision_requested_run_status",
                    run_id,
                    &run_outcome,
                )?;
                let task_outcome =
                    task::update_task_status(db, task_id, TaskStatus::Running, requested_at, None)
                        .await?;
                handle_projection_outcome(
                    "task_revision_requested_task_status",
                    task_id,
                    &task_outcome,
                )?;
                Ok(())
            }),
            TaskEventPayload::TaskRunEnteredReview {
                task_id,
                run_id,
                entered_at,
                ..
            } => project_future(async move {
                let entered_at = unix_to_datetime(*entered_at);
                let run_outcome = task_run::update_run_status(
                    db,
                    run_id,
                    TaskRunStatus::WaitingReview,
                    entered_at,
                )
                .await?;
                handle_projection_outcome("task_run_entered_review", run_id, &run_outcome)?;
                let task_outcome = task::update_task_status(
                    db,
                    task_id,
                    TaskStatus::WaitingReview,
                    entered_at,
                    None,
                )
                .await?;
                handle_projection_outcome("task_entered_review", task_id, &task_outcome)?;
                task_run_execution::mark_execution_waiting_review_by_run(db, run_id, entered_at)
                    .await?;
                Ok(())
            }),
            TaskEventPayload::DepthLimitExceeded {
                task_id,
                run_id,
                depth: _,
                max_depth: _,
            } => project_future(async move {
                if let Some(run_id) = run_id {
                    let outcome = task_run::update_run_error_json(
                        db,
                        run_id,
                        run_error_status
                            .context("depth-limit task run status projection was not prepared")?,
                        run_error_json,
                        created_at,
                    )
                    .await?;
                    handle_projection_outcome("depth_limit_run_failed", run_id, &outcome)?;
                }
                let outcome = task::update_task_error_json(
                    db,
                    task_id,
                    task_error_status
                        .context("depth-limit task status projection was not prepared")?,
                    task_error_json,
                    created_at,
                    Some(created_at),
                )
                .await?;
                handle_projection_outcome("depth_limit_task_failed", task_id, &outcome)?;
                Ok(())
            }),
            TaskEventPayload::DeliveryQueued { delivery } => project_future(project_task_delivery(
                db,
                delivery,
                delivery_projection.context("queued delivery projection was not prepared")?,
                delivery_authority.context("queued delivery authority was not prepared")?,
            )),
            TaskEventPayload::DeliveryCancelled {
                delivery, attempt, ..
            } => project_future(async move {
                project_task_delivery(
                    db,
                    delivery,
                    delivery_projection
                        .context("cancelled delivery projection was not prepared")?,
                    delivery_authority.context("cancelled delivery authority was not prepared")?,
                )
                .await?;
                if attempt.is_some() {
                    task_delivery::upsert_prepared_attempt(
                        db,
                        delivery_attempt_projection
                            .context("cancelled delivery attempt projection was not prepared")?,
                    )
                    .await?;
                }
                Ok(())
            }),
            TaskEventPayload::DeliveryStarted {
                delivery,
                attempt: _,
            }
            | TaskEventPayload::DeliveryDelivered {
                delivery,
                attempt: _,
            }
            | TaskEventPayload::DeliveryFailed {
                delivery,
                attempt: _,
            } => project_future(async move {
                project_task_delivery(
                    db,
                    delivery,
                    delivery_projection.context("delivery projection was not prepared")?,
                    delivery_authority.context("delivery authority was not prepared")?,
                )
                .await?;
                task_delivery::upsert_prepared_attempt(
                    db,
                    delivery_attempt_projection
                        .context("delivery attempt projection was not prepared")?,
                )
                .await
            }),
            TaskEventPayload::WriteLockAcquired { lock }
            | TaskEventPayload::WriteLockExtended { lock, .. }
            | TaskEventPayload::WriteLockReleased { lock, .. }
            | TaskEventPayload::WriteLockExpired { lock, .. } => {
                project_future(task_write_lock::upsert_lock(db, lock))
            }
            TaskEventPayload::WriteLockBlocked { .. } => project_future(async { Ok(()) }),
        };
        future.await
    }
}

async fn project_task_delivery<C: ConnectionTrait + Sync>(
    db: &C,
    _delivery: &TaskDelivery,
    prepared_delivery: task_delivery::PreparedTaskDeliveryProjection,
    prepared_authority: task_actor_contract::PreparedTaskDeliveryAuthority,
) -> Result<()> {
    task_delivery::upsert_prepared_delivery(db, prepared_delivery).await?;
    task_actor_contract::upsert_prepared_task_delivery_authority(db, prepared_authority).await
}

pub(crate) async fn prepare_task_delivery_authority<C: ConnectionTrait>(
    db: &C,
    delivery: &TaskDelivery,
) -> Result<task_actor_contract::PreparedTaskDeliveryAuthority> {
    let contract = task_actor_contract::find_task_actor_contract(db, &delivery.task_id)
        .await?
        .context("Task delivery is missing its immutable actor contract")?;
    contract
        .delivery
        .validate()
        .map_err(|error| anyhow::anyhow!("Task delivery actor contract is invalid: {error:?}"))?;
    if contract.workspace_id != delivery.workspace_id || !contract.delivery.enabled {
        anyhow::bail!("Task delivery differs from its immutable actor contract");
    }
    let exact_destination = match delivery.mode {
        TaskDeliveryMode::Thread => {
            delivery.target_thread_id == contract.delivery.destination_thread_id
        }
        TaskDeliveryMode::UserNotification => {
            delivery.target_user_id == contract.delivery.destination_user_id
        }
        TaskDeliveryMode::Webhook => {
            delivery.webhook_url_fingerprint
                == contract.delivery.destination_webhook_url_fingerprint
        }
        TaskDeliveryMode::None => false,
    };
    if !exact_destination {
        anyhow::bail!("Task delivery rewrites its immutable destination");
    }
    let task = task::find_task_by_id(db, &delivery.task_id)
        .await?
        .context("Task delivery has no Task")?;
    let author = if task.executor_kind == "agent" && delivery.error_snapshot.is_none() {
        let occurrence = task_actor_contract::find_task_occurrence_by_run_id(db, &delivery.run_id)
            .await?
            .context("Agent Task delivery has no exact occurrence")?;
        if occurrence.task_id != delivery.task_id {
            anyhow::bail!("Agent Task delivery occurrence belongs to another Task");
        }
        let execution_id = occurrence
            .agent_execution_id
            .context("Agent Task result has no exact execution author")?;
        PersistedActorRef::AgentExecution(
            pioneer_protocol::AgentExecutionId::new(execution_id).map_err(|error| {
                anyhow::anyhow!("Task delivery execution id is invalid: {error:?}")
            })?,
        )
    } else {
        PersistedActorRef::System
    };
    let review_required = delivery.error_snapshot.is_none()
        && task_delivery_requires_final_review(db, &delivery.task_id, &delivery.run_id).await?;
    let reviewer = if !review_required {
        None
    } else {
        let candidate = task_result_candidate::find_candidate_by_run_and_status(
            db,
            &delivery.run_id,
            TaskResultCandidateStatus::Accepted,
        )
        .await?;
        let review_id = candidate
            .and_then(|candidate| candidate.final_review_event_id)
            .context("Task result delivery has no exact final review event")?;
        let event = task_result_review_event::find_review_event_by_id(db, &review_id)
            .await?
            .context("Task delivery final review event disappeared")?;
        if event.task_id != delivery.task_id
            || event.run_id != delivery.run_id
            || event.decision != "accept"
        {
            anyhow::bail!("Task delivery final reviewer has different immutable lineage");
        }
        Some(serde_json::from_str::<TaskResultReviewerRef>(
            event.reviewer_ref_json.as_str(),
        )?)
    };
    let status = match delivery.status {
        TaskDeliveryStatus::Pending => "pending",
        TaskDeliveryStatus::Delivering => "delivering",
        TaskDeliveryStatus::Delivered => "delivered",
        TaskDeliveryStatus::Failed => "failed",
        TaskDeliveryStatus::Cancelled => "cancelled",
    };
    let author_json = serde_json::to_string(&author)?;
    let reviewer_json = reviewer.as_ref().map(serde_json::to_string).transpose()?;
    task_actor_contract::prepare_task_delivery_authority(
        &delivery.id,
        &delivery.task_id,
        &delivery.run_id,
        &author_json,
        reviewer_json.as_deref(),
        contract.delivery.route_id.as_deref(),
        contract.delivery.route_receipt_json.as_deref(),
        contract.delivery.disclosure_generation,
        &delivery.delivery_key,
        status,
        delivery.updated_at,
    )
}

async fn task_delivery_requires_final_review<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    run_id: &str,
) -> Result<bool> {
    let agent_spec = match task_agent_spec::find_agent_spec_by_run(db, run_id).await? {
        Some(agent_spec) => Some(agent_spec),
        None => task_agent_spec::find_latest_agent_spec_by_task(db, task_id).await?,
    };
    let Some(agent_spec) = agent_spec else {
        return Ok(false);
    };
    if agent_spec.task_id != task_id {
        anyhow::bail!("Task delivery Agent spec belongs to another Task");
    }
    let review_policy: Option<TaskAgentReviewPolicy> =
        optional_typed_json_from_db(agent_spec.review_policy_json)?;
    Ok(review_policy
        .as_ref()
        .is_some_and(TaskAgentReviewPolicy::is_enabled))
}

async fn task_is_terminal_db<C: ConnectionTrait + Sync>(db: &C, task_id: &str) -> Result<bool> {
    Ok(task::find_task_by_id(db, task_id)
        .await?
        .is_some_and(|model| is_terminal_task_status_db(model.status.as_str())))
}

async fn task_status_db<C: ConnectionTrait + Sync>(
    db: &C,
    task_id: &str,
) -> Result<Option<TaskStatus>> {
    task::find_task_by_id(db, task_id)
        .await?
        .map(|model| {
            task_status_from_db(model.status.as_str()).ok_or_else(|| {
                anyhow::anyhow!(
                    "task `{task_id}` has unknown persisted status `{}`",
                    model.status
                )
            })
        })
        .transpose()
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

async fn project_legacy_auto_accepted_candidate<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    candidate: Option<task_result_candidate::PreparedTaskResultCandidate>,
    review: Option<task_result_review_event::PreparedTaskResultReviewEvent>,
) -> Result<bool> {
    if task_result_candidate::find_candidate_by_run_and_status(
        db,
        run_id,
        TaskResultCandidateStatus::Accepted,
    )
    .await?
    .is_some()
    {
        return Ok(true);
    }

    let Some(task_run_turn) = task_run_turn::find_latest_turn_by_run(db, run_id).await? else {
        // Legacy runs without a linked TaskRunTurn never materialized a
        // result candidate. Preserve that compatibility behavior without
        // requiring a synthetic projection plan.
        return Ok(false);
    };
    let candidate = candidate.context(
        "legacy task result candidate projection was not prepared before writer admission",
    )?;
    let expected = candidate.expected();
    if task_run_turn.id != expected.task_run_turn_id
        || task_run_turn.thread_id != expected.thread_id
        || task_run_turn.turn_id != expected.turn_id
        || task_run_turn.round != i64::from(expected.round)
    {
        anyhow::bail!("legacy task result candidate task run turn changed after preparation");
    }
    if task_result_candidate::find_candidate_by_turn(db, task_run_turn.id.as_str())
        .await?
        .is_some()
    {
        return Ok(false);
    }

    task_result_candidate::upsert_candidate(db, candidate).await?;
    task_result_review_event::upsert_review_event(
        db,
        review.context("legacy task result review projection was not prepared")?,
    )
    .await?;
    Ok(true)
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
