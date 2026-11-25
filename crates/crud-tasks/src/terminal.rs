//! Prepare terminal Task decisions on readers, then validate and apply them atomically.
use crate::delivery::task_delivery_from_db_model;
use crate::event_pipeline::{
    MAX_ATOMIC_TASK_EVENT_BATCH_SIZE, append_task_events_in_connection_with_delivery_policy,
    prepare_task_events_for_write, refresh_task_event_preflight,
};
use crate::event_records::{AppendedTaskEvent, DeferredAppendedTaskEvent, materialize_task_events};
use crate::event_store::TaskEventStore;
use crate::status::{
    task_run_execution_status_from_db, task_run_execution_status_to_db, task_run_status_from_db,
    task_status_from_db,
};
use crate::task::task_from_db_model;
use crate::task_agent_spec::task_agent_spec_from_db_model;
use crate::task_projector::TaskOutputBinder;
use crate::task_run::task_run_from_db_model;
use crate::task_run_turn::task_run_turn_from_db_model;
use crate::task_trigger::task_trigger_from_db_model;
use crate::terminal_delivery::{delivery_for_terminal_run, task_delivery_result_snapshot};
use crate::terminal_policy::{prepare_retry, terminal_facts};
use crate::values::unix_to_datetime;
use crate::write_lock::task_write_lock_from_db_model;
use crate::{
    delivery as task_delivery, task as task_repository, task_actor_contract, task_agent_spec,
    task_event, task_run, task_run_execution, task_run_turn, task_trigger,
    write_lock as task_write_lock,
};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud_execution::execution_reads::load_agent_execution_resource_state;
use pioneer_protocol::{
    TaskActorContract, TaskCompletionBehavior, TaskErrorClass, TaskEventPayload, TaskGetResponse,
    TaskOccurrenceContract, TaskOccurrenceStatus, TaskRescheduleReason, TaskRunExecutionStatus,
    TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus,
};
use sea_orm::{
    ConnectionTrait, DatabaseTransaction, TransactionTrait, prelude::DateTimeWithTimeZone,
};

/// A rejected domain transition must propagate without a database-conflict retry.
#[derive(Debug)]
pub struct TaskTerminalConflict(pub &'static str);

impl std::fmt::Display for TaskTerminalConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "terminal Task conflict: {}", self.0)
    }
}
impl std::error::Error for TaskTerminalConflict {}

#[derive(Debug)]
struct FinishTerminalRace;
impl std::fmt::Display for FinishTerminalRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("finish admission or sibling outcome changed during terminal preparation")
    }
}
impl std::error::Error for FinishTerminalRace {}

#[derive(Debug)]
struct WebhookProgressTerminalRace;
impl std::fmt::Display for WebhookProgressTerminalRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("webhook queue or sibling activity changed during terminal preparation")
    }
}
impl std::error::Error for WebhookProgressTerminalRace {}

fn conflict<T>(reason: &'static str) -> Result<T> {
    Err(TaskTerminalConflict(reason).into())
}

#[derive(Debug)]
struct AdditionalRunsTerminalRace;
impl std::fmt::Display for AdditionalRunsTerminalRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("additional Task runs changed during terminal preparation")
    }
}
impl std::error::Error for AdditionalRunsTerminalRace {}

#[derive(Clone, Debug, PartialEq)]
struct TerminalState {
    task: pioneer_entity::task::Model,
    run: pioneer_entity::task_run::Model,
    trigger: Option<pioneer_entity::task_trigger::Model>,
    latest_run_number: i64,
    additional_runs: Option<task_run::AdditionalCompletionRuns>,
    agent_spec: Option<pioneer_entity::task_agent_spec::Model>,
    locks: Vec<pioneer_entity::task_write_lock::Model>,
    actor: TaskActorContract,
    occurrence: TaskOccurrenceContract,
    execution_fence: Option<(String, Option<i64>)>,
    finish_first_run: Option<i64>,
    finish_runs: Option<crate::finish::FinishRuns>,
}

/// Opaque, bounded preparation. It carries the facts read on the reader path;
/// commit never trusts those facts without checking the writer snapshot.
#[derive(Clone, Debug)]
pub struct PreparedTaskTerminalTransition {
    state: TerminalState,
    events: Vec<task_event::PreparedTaskEvent>,
    terminal: TaskEventPayload,
    result_json: Option<String>,
    error_json: Option<String>,
    terminal_reason: Option<String>,
    delivery: Option<task_delivery::PreparedTaskDeliveryProjection>,
    at: i64,
    retry_scheduled: bool,
    terminal_revision: Option<TaskRunTurn>,
}

impl PreparedTaskTerminalTransition {
    /// Keep a live handle's previously pinned agent attempt authoritative even
    /// if the occurrence was resumed before terminal preparation began.
    pub fn with_pinned_agent_attempt(self, expected: Option<i64>) -> Result<Self> {
        if let Some(expected) = expected
            && self
                .state
                .execution_fence
                .as_ref()
                .and_then(|(_, generation)| *generation)
                != Some(expected)
        {
            return conflict("live handle belongs to a superseded agent attempt");
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskTerminalCommitStatus {
    Applied,
    Replayed,
    TaskAlreadyTerminal,
}

#[derive(Debug)]
pub struct TaskTerminalCommitOutcome {
    pub status: TaskTerminalCommitStatus,
    pub events: Vec<AppendedTaskEvent>,
    pub completed_at: i64,
    pub delivery_id: Option<String>,
    pub retry_scheduled: bool,
}

#[derive(Debug)]
struct DeferredTaskTerminalCommitOutcome {
    pub status: TaskTerminalCommitStatus,
    pub events: Vec<DeferredAppendedTaskEvent>,
    pub completed_at: i64,
    pub delivery_id: Option<String>,
    pub retry_scheduled: bool,
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    /// Add the exact reserved revision to the existing terminal batch. Inputs
    /// come from the dispatch snapshot, not a fresh adoption of the latest turn.
    /// Only immutable identity and the terminal projection are prepared here;
    /// the writer revalidates every field and latest-revision ownership.
    pub async fn prepare_task_terminal_with_revision(
        &self,
        mut prepared: PreparedTaskTerminalTransition,
        revision: TaskRunTurn,
    ) -> Result<PreparedTaskTerminalTransition> {
        let (status, error) = match &prepared.terminal {
            TaskEventPayload::RunBlocked { error, .. } => (TaskRunTurnStatus::Blocked, error),
            TaskEventPayload::RunFailed { error, .. } => (TaskRunTurnStatus::Failed, error),
            _ => return conflict("revision requires RunBlocked or RunFailed"),
        };
        if prepared.events.len() >= MAX_ATOMIC_TASK_EVENT_BATCH_SIZE {
            bail!("revision exceeds the atomic Task event batch limit");
        }
        if revision.task_id != prepared.state.task.id
            || revision.run_id != prepared.state.run.id
            || revision.kind != TaskRunTurnKind::Revision
            || revision.status != TaskRunTurnStatus::InProgress
            || revision.completed_at.is_some()
            || revision.execution_id.as_deref()
                != prepared
                    .state
                    .execution_fence
                    .as_ref()
                    .map(|(id, _)| id.as_str())
            || prepared.terminal_revision.is_some()
        {
            return conflict("revision does not belong to the dispatch attempt");
        }
        let mut terminal_turn = revision.clone();
        terminal_turn.status = status;
        terminal_turn.completed_at = Some(prepared.at);
        let event = match status {
            TaskRunTurnStatus::Blocked => TaskEventPayload::TaskRunTurnBlocked {
                task_run_turn: terminal_turn,
                error: error.clone(),
            },
            TaskRunTurnStatus::Failed => TaskEventPayload::TaskRunTurnFailed {
                task_run_turn: terminal_turn,
                error: error.clone(),
            },
            _ => unreachable!(),
        };
        let mut events = prepare_task_events_for_write(&self.connection, vec![event]).await?;
        events.append(&mut prepared.events);
        prepared.events = events;
        prepared.terminal_revision = Some(revision);
        Ok(prepared)
    }

    pub async fn prepare_task_terminal_transition(
        &self,
        terminal: TaskEventPayload,
    ) -> Result<PreparedTaskTerminalTransition> {
        // Terminal orchestration is also polled from nested Gateway failure
        // paths. Keep its database futures out of the caller's future frame.
        Box::pin(self.prepare_task_terminal_transition_inner(terminal)).await
    }

    async fn prepare_task_terminal_transition_inner(
        &self,
        terminal: TaskEventPayload,
    ) -> Result<PreparedTaskTerminalTransition> {
        let (at, result, error, lock_status, mut lock_reason) = terminal_facts(&terminal)?;
        let state = load_terminal_state(
            &self.connection,
            terminal.task_id(),
            terminal
                .run_id()
                .context("terminal transition has no run")?,
        )
        .await?;
        if task_status_from_db(&state.task.status).is_some_and(|s| s.is_terminal())
            && !task_run_status_from_db(&state.run.status).is_some_and(|s| s.is_terminal())
        {
            lock_reason = match terminal {
                TaskEventPayload::RunCompleted { .. } => {
                    Some("run completed after task terminal".into())
                }
                TaskEventPayload::RunFailed { .. } => Some("run failed after task terminal".into()),
                TaskEventPayload::RunBlocked { .. } => {
                    Some("run blocked after task terminal".into())
                }
                _ => lock_reason,
            };
        }
        let mut task = task_from_db_model(state.task.clone())?;
        if error.as_ref().is_some_and(|error| {
            pioneer_protocol::TaskRevisionRetryBlocker::from_code(&error.code).is_some()
        }) {
            // A terminal sibling may own the aggregate result. It must
            // never appear in the obsolete revision's blocked delivery.
            task.result = None;
        }
        let run = task_run_from_db_model(state.run.clone())?;
        let response = TaskGetResponse {
            task,
            runs: vec![run],
            triggers: state
                .trigger
                .clone()
                .map(task_trigger_from_db_model)
                .transpose()?
                .into_iter()
                .collect(),
            agent_specs: state
                .agent_spec
                .clone()
                .map(task_agent_spec_from_db_model)
                .transpose()?
                .into_iter()
                .collect(),
            dependencies: Vec::new(),
            write_locks: Vec::new(),
            thread_lineage: Vec::new(),
            task_run_thread_bindings: Vec::new(),
            task_run_turns: Vec::new(),
            result_candidates: Vec::new(),
            result_review_events: Vec::new(),
        };
        let result_json = task_run::prepare_run_result_json(result.as_ref())?;
        let error_json = task_run::prepare_run_error_json(error.as_ref())?;
        let terminal_reason = error.as_ref().map(|e| e.message.clone());
        let mut events = vec![terminal.clone()];
        for model in &state.locks {
            let mut lock = task_write_lock_from_db_model(model.clone())?;
            lock.status = lock_status;
            lock.released_at = Some(at);
            lock.reason = lock_reason.clone();
            lock.updated_at = at;
            events.push(TaskEventPayload::WriteLockReleased {
                lock,
                released_at: at,
            });
        }
        let suppressed =
            response.task.status.is_terminal() && !response.runs[0].status.is_terminal();
        let retry_scheduled = !suppressed
            && state.finish_first_run.is_none()
            && matches!(terminal, TaskEventPayload::RunFailed { .. })
            && prepare_retry(
                &response,
                state.latest_run_number,
                &mut events,
                error.clone(),
                at,
            )?;
        let revision_retry_pending = error
            .as_ref()
            .is_some_and(|error| error.code == pioneer_protocol::TASK_REVISION_RETRY_PENDING);
        let mut delivery = None;
        let implicit_result = result.is_none();
        let implicit_error = error.is_none();
        if !retry_scheduled && !suppressed {
            let finishing = state.finish_first_run.is_some();
            let finish_ready = state
                .finish_runs
                .as_ref()
                .is_some_and(|runs| runs.active.is_none());
            if finish_ready
                || (!finishing
                    // Accepted extra work must drain in either execution order.
                    // A future/paused planned fire still owns work as well. Tasks
                    // without extra runs retain their original lifecycle behavior.
                    && state.additional_runs.as_ref().is_none_or(|runs| {
                        runs.pending_run_id.is_none()
                            && !response.triggers.iter().any(|trigger| {
                                matches!(trigger.status, pioneer_protocol::TaskTriggerStatus::Active | pioneer_protocol::TaskTriggerStatus::Paused)
                                    && trigger.next_fire_at.is_some()
                            })
                    })
                    && response
                        .task
                        .lifecycle_policy
                        .as_ref()
                        .map(|p| p.completion)
                        .unwrap_or(TaskCompletionBehavior::CompleteOnTerminalRun)
                        == TaskCompletionBehavior::CompleteOnTerminalRun)
            {
                let outcome = match &terminal {
                    TaskEventPayload::RunCompleted {
                        task_id, result, ..
                    } => TaskEventPayload::TaskCompleted {
                        task_id: task_id.clone(),
                        result: result.clone(),
                        completed_at: at,
                    },
                    TaskEventPayload::RunFailed { task_id, error, .. } => {
                        TaskEventPayload::TaskFailed {
                            task_id: task_id.clone(),
                            error: error.clone(),
                            completed_at: at,
                        }
                    }
                    TaskEventPayload::RunBlocked { task_id, error, .. } => {
                        TaskEventPayload::TaskBlocked {
                            task_id: task_id.clone(),
                            error: error.clone(),
                            blocked_at: at,
                        }
                    }
                    TaskEventPayload::RunCancelled {
                        task_id, reason, ..
                    } => TaskEventPayload::TaskCancelled {
                        task_id: task_id.clone(),
                        reason: reason.clone(),
                        completed_at: at,
                    },
                    _ => unreachable!(),
                };
                // A successful last sibling cannot erase an earlier real
                // failure, rejection, or cancellation from the draining set.
                let prior_failure = if finishing {
                    state
                        .finish_runs
                        .as_ref()
                        .and_then(|runs| runs.outcome.as_ref())
                        .filter(|run| run.status != "succeeded")
                } else {
                    state
                        .additional_runs
                        .as_ref()
                        .and_then(|runs| runs.outcome.as_ref())
                };
                events.push(
                    if matches!(terminal, TaskEventPayload::RunCompleted { .. })
                        && prior_failure.is_some()
                    {
                        crate::finish::outcome_event(&state.task.id, prior_failure, at)?
                    } else {
                        outcome
                    },
                );
            } else if !finishing && let Some(mut trigger) = response.triggers.first().cloned() {
                // A webhook's trigger facts describe admission, not completion.
                // Keep them stable while parallel runs independently terminalize;
                // the refresh event still updates aggregate status on the writer
                // after the last live sibling ends. All snapshot fences remain.
                if !matches!(&trigger.spec, pioneer_protocol::TaskTriggerSpec::External {
                    source, event_type: None, filter: None
                } if source == "webhook")
                {
                    trigger.updated_at = at;
                }
                events.push(TaskEventPayload::TaskRescheduled {
                    task_id: response.task.id.clone(),
                    trigger,
                    rescheduled_at: at,
                    reason: TaskRescheduleReason::RunTerminalStatusRefresh,
                });
            }
            let policy = state
                .occurrence
                .delivery_plan
                .as_ref()
                .map(|p| &p.policy)
                .or(response.task.delivery_policy.as_ref());
            if !revision_retry_pending
                && let Some(mut queued) = delivery_for_terminal_run(
                    &response,
                    policy,
                    &state.run.id,
                    at,
                    result,
                    error,
                    state.actor.delivery.destination_user_id.as_deref(),
                )
            {
                if state.occurrence.retry_attempt > 0 {
                    queued.delivery_key = format!(
                        "retry:{}:{}",
                        state.occurrence.retry_attempt, queued.delivery_key
                    );
                }
                // A prior terminal run owns its already-frozen delivery
                // fallback. Task.result/error can now belong to a later run.
                // Read and decode that bounded exact row on the reader path;
                // commit will compare the serialized facts again on the writer.
                if task_run_status_from_db(&state.run.status).is_some_and(|s| s.is_terminal())
                    && let Some(existing) =
                        task_delivery::find_delivery_by_key(&self.connection, &queued.delivery_key)
                            .await?
                {
                    let existing = task_delivery_from_db_model(existing)?;
                    if implicit_result && let Some(policy) = policy.filter(|p| p.include_result) {
                        queued.result_snapshot = existing
                            .result_snapshot
                            .map(|r| task_delivery_result_snapshot(r, policy.format));
                    }
                    if implicit_error {
                        queued.error_snapshot = existing.error_snapshot;
                    }
                }
                state
                    .actor
                    .delivery
                    .validate()
                    .map_err(|e| anyhow!("invalid task delivery authority: {e:?}"))?;
                delivery = Some(task_delivery::prepare_delivery_projection(&queued)?);
                events.push(TaskEventPayload::DeliveryQueued { delivery: queued });
            }
        }
        let events = prepare_task_events_for_write(&self.connection, events).await?;
        Ok(PreparedTaskTerminalTransition {
            state,
            events,
            terminal,
            result_json,
            error_json,
            terminal_reason,
            delivery,
            at,
            retry_scheduled,
            terminal_revision: None,
        })
    }

    /// Resolve the exact pending retry without changing the terminal run's
    /// identity, retry generation, sibling state or trigger. The prior block
    /// suppressed delivery, so settlement owns the one final delivery.
    pub async fn stop_pending_task_revision_retry(
        &self,
        revision: &TaskRunTurn,
        blocker: pioneer_protocol::TaskRevisionRetryBlocker,
        now: i64,
    ) -> Result<bool> {
        use pioneer_entity::{task, task_run as run};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
        let change =
            crate::work_change::TaskWorkChange::for_source(&self.connection, "r", &revision.run_id);
        let outcome = self
            .retry_write_on_conflict(|| async {
                let Some(old_run) =
                    task_run::find_run_by_id(&self.connection, &revision.run_id).await?
                else {
                    return Ok((true, false));
                };
                let model = task_run_from_db_model(old_run.clone())?;
                let Some(mut error) = model
                    .error
                    .filter(|e| e.code == pioneer_protocol::TASK_REVISION_RETRY_PENDING)
                else {
                    return Ok((true, false));
                };
                blocker.apply(&mut error);
                let mut prepared = self
                    .prepare_task_terminal_transition(TaskEventPayload::RunBlocked {
                        task_id: revision.task_id.clone(),
                        run_id: revision.run_id.clone(),
                        error: Some(error),
                        blocked_at: now,
                    })
                    .await?;
                prepared.events.retain(|event| {
                    matches!(event.payload(), TaskEventPayload::DeliveryQueued { .. })
                });
                prepared.events.extend(
                    prepare_task_events_for_write(
                        &self.connection,
                        vec![TaskEventPayload::TaskRecovered {
                            task_id: revision.task_id.clone(),
                            run_id: Some(revision.run_id.clone()),
                            message: blocker.message().to_owned(),
                            recovered_at: now,
                        }],
                    )
                    .await?,
                );

                let tx = self.connection.begin().await?;
                crate::event_log::lock_task(&tx, &revision.task_id).await?;
                let current = load_terminal_state(&tx, &revision.task_id, &revision.run_id).await?;
                let latest = task_run_turn::find_latest_turn_by_run(&tx, &revision.run_id)
                    .await?
                    .map(task_run_turn_from_db_model)
                    .transpose()?;
                if current.run.status != "blocked" || latest.as_ref() != Some(revision) {
                    tx.rollback().await?;
                    return Ok((true, false));
                }
                if current.run.error_json != old_run.error_json
                    || !same_transition_state(&prepared.state, &current)
                    || (blocker == pioneer_protocol::TaskRevisionRetryBlocker::NewerRun
                        && current.latest_run_number <= current.run.run_number)
                {
                    tx.rollback().await?;
                    return Ok((false, false));
                }
                let expected_error = old_run
                    .error_json
                    .as_deref()
                    .context("pending retry error disappeared")?;
                let sequence = crate::completion_inbox::next_completion_sequence(&tx).await?;
                let updated = run::Entity::update_many()
                    .filter(run::Column::Id.eq(&revision.run_id))
                    .filter(run::Column::Status.eq("blocked"))
                    .filter(run::Column::ErrorJson.eq(expected_error))
                    .col_expr(
                        run::Column::ErrorJson,
                        Expr::value(prepared.error_json.clone()),
                    )
                    .col_expr(run::Column::UpdatedAt, Expr::value(unix_to_datetime(now)))
                    // The logical completion occurs at settlement. Inbox
                    // eligibility and the page cursor both use this timestamp.
                    .col_expr(run::Column::CompletedAt, Expr::value(unix_to_datetime(now)))
                    .col_expr(run::Column::InboxSequence, Expr::value(sequence))
                    .exec(&tx)
                    .await?;
                if updated.rows_affected == 0 {
                    tx.rollback().await?;
                    return Ok((false, false));
                }
                if current.task.status == "blocked" && current.task.error_json == old_run.error_json
                {
                    task::Entity::update_many()
                        .filter(task::Column::Id.eq(&revision.task_id))
                        .filter(task::Column::Revision.eq(current.task.revision))
                        .filter(task::Column::Status.eq("blocked"))
                        .filter(task::Column::ErrorJson.eq(expected_error))
                        .col_expr(task::Column::ErrorJson, Expr::value(prepared.error_json))
                        .col_expr(
                            task::Column::CompletedAt,
                            Expr::value(unix_to_datetime(now)),
                        )
                        .col_expr(task::Column::UpdatedAt, Expr::value(unix_to_datetime(now)))
                        .exec(&tx)
                        .await?;
                }
                let new_run = task_run::find_run_by_id(&tx, &revision.run_id).await?;
                crate::task_occurrence_producers::run_change(
                    &tx,
                    Some(&current.run),
                    new_run.as_ref(),
                )
                .await?;
                append_task_events_in_connection_with_delivery_policy(
                    &tx,
                    prepared.events,
                    now,
                    false,
                    &self.outputs,
                )
                .await?;
                tx.commit().await?;
                Ok((true, true))
            })
            .await;
        if let Ok((_, changed)) = &outcome {
            change.completed(*changed);
        }
        outcome.map(|(done, _)| done)
    }

    pub async fn commit_task_terminal_transition(
        &self,
        prepared: PreparedTaskTerminalTransition,
    ) -> Result<TaskTerminalCommitOutcome> {
        let change = crate::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            [prepared.state.task.id.clone()],
        );
        // Keep the existing bounded terminal state machine off the worker stack.
        let result = Box::pin(self.commit_task_terminal_transition_inner(prepared)).await;
        if let Ok(outcome) = &result {
            change.completed(crate::work_change::inserted_task_events(&outcome.events));
        }
        result
    }

    async fn commit_task_terminal_transition_inner(
        &self,
        prepared: PreparedTaskTerminalTransition,
    ) -> Result<TaskTerminalCommitOutcome> {
        let fence = prepared.state.execution_fence.clone();
        let terminal = prepared.terminal.clone();
        let terminal_revision = prepared.terminal_revision.clone();
        let mut prepared = prepared;
        for _ in 0..16 {
            match self
                .retry_write_on_conflict(|| {
                    self.commit_task_terminal_transition_once(prepared.clone())
                })
                .await
            {
                Err(error) if error.downcast_ref::<FinishTerminalRace>().is_some() => {
                    prepared = self
                        .prepare_task_terminal_transition(terminal.clone())
                        .await?;
                    if prepared.state.execution_fence != fence {
                        return conflict("execution fence changed while reconciling finish");
                    }
                    if let Some(revision) = &terminal_revision {
                        prepared = self
                            .prepare_task_terminal_with_revision(prepared, revision.clone())
                            .await?;
                    }
                }
                Err(error) if error.downcast_ref::<AdditionalRunsTerminalRace>().is_some() => {
                    let refreshed = self
                        .prepare_task_terminal_transition(terminal.clone())
                        .await?;
                    validate_terminal_policy(&prepared.state, &refreshed.state)?;
                    if !same_after_additional_progress(&prepared.state, &refreshed.state) {
                        return conflict(
                            "protected terminal facts changed while reloading additional work",
                        );
                    }
                    prepared = if let Some(revision) = &terminal_revision {
                        self.prepare_task_terminal_with_revision(refreshed, revision.clone())
                            .await?
                    } else {
                        refreshed
                    };
                }
                Err(error)
                    if error
                        .downcast_ref::<WebhookProgressTerminalRace>()
                        .is_some() =>
                {
                    let refreshed = self
                        .prepare_task_terminal_transition(terminal.clone())
                        .await?;
                    // The reader may race again after the rejected writer. Do
                    // not adopt a changed policy, run/occurrence, resume, actor
                    // or attempt merely because preparation was repeated.
                    validate_terminal_policy(&prepared.state, &refreshed.state)?;
                    if !same_after_webhook_progress(&prepared.state, &refreshed.state) {
                        return conflict(
                            "protected terminal facts changed while reloading webhook progress",
                        );
                    }
                    prepared = if let Some(revision) = &terminal_revision {
                        self.prepare_task_terminal_with_revision(refreshed, revision.clone())
                            .await?
                    } else {
                        refreshed
                    };
                }
                outcome => return outcome,
            }
        }
        conflict("finish, additional runs or webhook progress changed repeatedly")
    }

    async fn commit_task_terminal_transition_once(
        &self,
        mut prepared: PreparedTaskTerminalTransition,
    ) -> Result<TaskTerminalCommitOutcome> {
        // A retry may receive a clone of the original prepared transition.
        // Refresh semantic caches before begin while retaining domain fences.
        prepared.events = refresh_task_event_preflight(&self.connection, prepared.events).await?;
        let tx = self
            .connection
            .begin()
            .await
            .context("failed to begin terminal Task transition")?;
        let outcome = self.commit_task_terminal_in_connection(&tx, prepared).await;
        match outcome {
            Ok(outcome) => {
                tx.commit().await?;
                Ok(TaskTerminalCommitOutcome {
                    status: outcome.status,
                    events: materialize_task_events(outcome.events),
                    completed_at: outcome.completed_at,
                    delivery_id: outcome.delivery_id,
                    retry_scheduled: outcome.retry_scheduled,
                })
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    async fn finalize_task_terminal_in_connection(
        &self,
        db: &DatabaseTransaction,
        prepared: &PreparedTaskTerminalTransition,
        completed_at: i64,
    ) -> Result<()> {
        let (status, agent_status, occurrence_status) = match &prepared.terminal {
            TaskEventPayload::RunCompleted { .. } => (
                TaskRunExecutionStatus::Succeeded,
                "succeeded",
                TaskOccurrenceStatus::Delivered,
            ),
            TaskEventPayload::RunFailed { error, .. }
                if error
                    .as_ref()
                    .is_some_and(|e| e.class == TaskErrorClass::Timeout) =>
            {
                (
                    TaskRunExecutionStatus::TimedOut,
                    "timed_out",
                    TaskOccurrenceStatus::Failed,
                )
            }
            TaskEventPayload::RunFailed { .. } => (
                TaskRunExecutionStatus::Failed,
                "failed",
                TaskOccurrenceStatus::Failed,
            ),
            TaskEventPayload::RunBlocked { .. } => (
                TaskRunExecutionStatus::Blocked,
                "blocked",
                TaskOccurrenceStatus::Failed,
            ),
            TaskEventPayload::RunCancelled { .. } => (
                TaskRunExecutionStatus::Cancelled,
                "cancelled",
                TaskOccurrenceStatus::Cancelled,
            ),
            _ => unreachable!(),
        };
        let at = unix_to_datetime(completed_at);
        if let Some(execution) =
            task_run_execution::find_execution_by_run(db, &prepared.state.run.id).await?
        {
            let agent = if execution.executor_kind == "agent" {
                pioneer_crud_execution::agent_execution::load_agent_execution(db, &execution.id)
                    .await?
            } else {
                None
            };
            if let Some(agent) = agent.as_ref() {
                let committed_status = match agent.status.as_str() {
                    "completed" => "succeeded",
                    other => other,
                };
                if matches!(
                    committed_status,
                    "succeeded" | "failed" | "blocked" | "cancelled" | "timed_out"
                ) && committed_status != agent_status
                {
                    return conflict("different committed agent execution outcome");
                }
            }
            if execution.executor_kind == "agent"
                && agent.is_none()
                && (prepared.state.occurrence.agent_execution_id.is_some()
                    || prepared
                        .state
                        .occurrence
                        .work_graph_root_execution_id
                        .is_some()
                    || prepared.state.occurrence.root_resource_scope_id.is_some()
                    || prepared
                        .state
                        .execution_fence
                        .as_ref()
                        .and_then(|(_, generation)| *generation)
                        .is_some()
                    || status == TaskRunExecutionStatus::Succeeded)
            {
                return conflict("admitted agent execution disappeared");
            }
            if task_run_execution_status_from_db(&execution.status)
                .context("invalid execution status")?
                .is_terminal()
            {
                if matches!(prepared.terminal, TaskEventPayload::RunCancelled { .. })
                    && execution.status == "cancelled"
                    && execution.result_json.is_none()
                    && execution.error_json.is_none()
                    && agent.is_some()
                {
                    // Work-graph cancellation fences Task executions before
                    // Task events are emitted. That fence intentionally has no
                    // result/error payload. Recognize it even when RunCancelled
                    // has no reason and its payload already matches execution.
                    let occurrence = task_actor_contract::find_task_occurrence_by_run_id(
                        db,
                        &prepared.state.run.id,
                    )
                    .await?
                    .context("terminal Task run has no occurrence")?;
                    let graph_cancelled =
                        matches!(prepared.terminal, TaskEventPayload::RunCancelled { .. })
                            && execution.status == "cancelled"
                            && execution.completed_at.is_some()
                            && execution.result_json.is_none()
                            && execution.error_json.is_none()
                            && occurrence.status == TaskOccurrenceStatus::Cancelled
                            && occurrence.agent_execution_id.as_deref()
                                == Some(execution.id.as_str())
                            && agent.as_ref().is_some_and(|agent| {
                                agent.status == "cancelled"
                                    && agent.finished_at.is_some()
                                    && agent.workspace_id == prepared.state.task.workspace_id
                                    && agent.parent_task_id.as_deref()
                                        == Some(prepared.state.task.id.as_str())
                                    && u64::try_from(agent.execution_generation).ok()
                                        == Some(occurrence.execution_generation)
                                    && occurrence.work_graph_root_execution_id.as_deref()
                                        == Some(agent.work_graph_root_execution_id.as_str())
                                    && occurrence.root_resource_scope_id.as_deref()
                                        == Some(agent.work_graph_root_execution_id.as_str())
                            });
                    if !graph_cancelled {
                        return conflict("unconfirmed agent work graph cancellation fence");
                    }
                    // The graph fence owns its original completion time and
                    // reason. Both execution and occurrence are finalized.
                    return Ok(());
                }
                if execution.status != task_run_execution_status_to_db(status)
                    || execution.result_json != prepared.result_json
                    || execution.error_json != prepared.error_json
                {
                    return conflict("different committed execution outcome");
                }
            } else {
                let transitioned = task_run_execution::mark_execution_terminal_json(
                    db,
                    &execution.id,
                    status,
                    at,
                    prepared.result_json.clone(),
                    prepared.error_json.clone(),
                )
                .await?;
                if transitioned.is_none() {
                    return conflict("execution changed during terminal finalization");
                }
                if execution.executor_kind == "agent" {
                    if agent.is_some() {
                        finalize_agent_execution(db, &execution.id, agent_status, at).await?;
                    }
                    // Reservation/claim precedes graph admission. A startup
                    // failure or cancellation can close that unbound execution
                    // without inventing an AgentExecution or ignoring a lost one.
                }
            }
        }
        task_actor_contract::finalize_task_occurrence(
            db,
            &prepared.state.occurrence.run_id,
            occurrence_status,
            prepared.terminal_reason.clone(),
            at,
        )
        .await
    }

    pub async fn settle_finished_retry_in_connection(
        &self,
        db: &DatabaseTransaction,
        mut prepared: PreparedTaskTerminalTransition,
    ) -> Result<(Vec<DeferredAppendedTaskEvent>, Option<String>)> {
        let current =
            load_terminal_state(db, &prepared.state.task.id, &prepared.state.run.id).await?;
        validate_terminal_policy(&prepared.state, &current)?;
        if current.finish_first_run.is_none()
            || !matches!(current.run.status.as_str(), "failed" | "cancelled")
            || current.run.error_json != prepared.error_json
            || current.run.result_json != prepared.result_json
        {
            return conflict("stopped retry source outcome changed");
        }
        let retry = task_run::find_retry_successor(db, &current.run.id)
            .await?
            .context("stopped retry successor disappeared")?;
        if retry.task_id != current.task.id
            || retry.status != "cancelled"
            || retry.started_at.is_some()
        {
            return conflict("retry successor was not discarded before starting");
        }
        let retry = task_run_from_db_model(retry)?;
        if retry.error.as_ref().map(|e| e.message.as_str())
            != Some("task finished before this run started")
        {
            return conflict("retry successor was cancelled for another reason");
        }
        let at = current
            .run
            .completed_at
            .context("failed source has no completion time")?
            .timestamp();
        let queued = task_event::find_queued_delivery_for_run(
            db,
            &current.run.id,
            current.occurrence.retry_attempt,
        )
        .await?;
        let mut events = Vec::new();
        let delivery_id = match (queued, prepared.delivery.take()) {
            (Some(event), Some(candidate)) => {
                let persisted = task_delivery::find_delivery_by_key(db, candidate.delivery_key())
                    .await?
                    .ok_or(TaskTerminalConflict(
                        "different stopped retry delivery destination",
                    ))?;
                if event.idempotency_key.as_deref()
                    != Some(format!("delivery:{}:queued", persisted.id).as_str())
                {
                    return conflict("stopped retry delivery identity changed");
                }
                let persisted = task_delivery::resolve_queue_replay(persisted, candidate).context(
                    TaskTerminalConflict("different stopped retry delivery facts"),
                )?;
                let authority =
                    crate::task_projector::prepare_task_delivery_authority(db, &persisted).await?;
                task_actor_contract::validate_delivery_authority_replay(db, &authority).await?;
                Some(persisted.id)
            }
            (None, Some(_)) => {
                let delivery_events = std::mem::take(&mut prepared.events)
                    .into_iter()
                    .filter(|event| {
                        matches!(event.payload(), TaskEventPayload::DeliveryQueued { .. })
                    })
                    .collect();
                events = append_task_events_in_connection_with_delivery_policy(
                    db,
                    delivery_events,
                    at,
                    false,
                    &self.outputs,
                )
                .await?;
                events
                    .iter()
                    .find_map(|event| match event.payload.as_ref() {
                        TaskEventPayload::DeliveryQueued { delivery } => Some(delivery.id.clone()),
                        _ => None,
                    })
            }
            (None, None) => None,
            _ => return conflict("different stopped retry delivery plan"),
        };
        self.finalize_task_terminal_in_connection(db, &prepared, at)
            .await?;
        Ok((events, delivery_id))
    }

    async fn commit_task_terminal_in_connection(
        &self,
        db: &DatabaseTransaction,
        mut prepared: PreparedTaskTerminalTransition,
    ) -> Result<DeferredTaskTerminalCommitOutcome> {
        // These exact, bounded reads happen BEFORE RunCompleted/Failed/etc.
        // Serialization, random identity allocation and retry calculation have
        // already finished. No provider work or notification runs in this scope.
        let current =
            load_terminal_state(db, &prepared.state.task.id, &prepared.state.run.id).await?;
        validate_terminal_policy(&prepared.state, &current)?;
        if prepared.state.finish_first_run != current.finish_first_run
            || (current.finish_first_run.is_some()
                && prepared.state.finish_runs != current.finish_runs)
        {
            return Err(FinishTerminalRace.into());
        }
        let run_terminal = task_run_status_from_db(&current.run.status)
            .context("invalid terminal run status")?
            .is_terminal();
        if let Some(expected) = &prepared.terminal_revision {
            if !run_terminal
                && task_status_from_db(&current.task.status)
                    .is_some_and(|status| status.is_terminal())
            {
                return conflict("task ended before revision terminal commit");
            }
            let latest = task_run_turn::find_latest_turn_by_run(db, &current.run.id)
                .await?
                .context("revision disappeared")?;
            let latest = task_run_turn_from_db_model(latest)?;
            let mut expected = expected.clone();
            if run_terminal {
                // Replay may observe only this already committed terminal turn.
                // Run outcome/payload/idempotency are checked below as usual.
                expected.status = match prepared.terminal {
                    TaskEventPayload::RunBlocked { .. } => TaskRunTurnStatus::Blocked,
                    TaskEventPayload::RunFailed { .. } => TaskRunTurnStatus::Failed,
                    _ => unreachable!(),
                };
                expected.completed_at = current.run.completed_at.map(|at| at.timestamp());
            }
            if latest != expected {
                return conflict("revision changed before terminal commit");
            }
        }
        if run_terminal
            && current.finish_first_run.is_some()
            && matches!(current.run.status.as_str(), "failed" | "cancelled")
            && let Some(retry) = task_run::find_retry_successor(db, &current.run.id).await?
            && retry.status == "cancelled"
            && retry.started_at.is_none()
            && task_run_from_db_model(retry)?
                .error
                .as_ref()
                .map(|e| e.message.as_str())
                == Some("task finished before this run started")
        {
            let completed_at = current
                .run
                .completed_at
                .context("failed source has no completion time")?
                .timestamp();
            let (events, delivery_id) = self
                .settle_finished_retry_in_connection(db, prepared)
                .await?;
            return Ok(DeferredTaskTerminalCommitOutcome {
                status: TaskTerminalCommitStatus::Applied,
                events,
                completed_at,
                delivery_id,
                retry_scheduled: false,
            });
        }
        if run_terminal {
            let expected_status = match prepared.terminal {
                TaskEventPayload::RunCompleted { .. } => "succeeded",
                TaskEventPayload::RunFailed { .. } => "failed",
                TaskEventPayload::RunBlocked { .. } => "blocked",
                TaskEventPayload::RunCancelled { .. } => "cancelled",
                _ => unreachable!(),
            };
            let terminal_key = task_event::terminal_idempotency_key(db, &prepared.terminal)
                .await?
                .context("terminal Task outcome has no idempotency key")?;
            let event =
                task_event::find_event_by_idempotency_key(db, &current.task.id, &terminal_key)
                    .await?
                    .context("terminal Task run has no terminal event")?;
            if current.run.status != expected_status
                || event.event_type != prepared.terminal.event_type()
                || current.run.result_json != prepared.result_json
                || current.run.error_json != prepared.error_json
            {
                return conflict("different committed run outcome");
            }
            let queued = task_event::find_queued_delivery_for_run(
                db,
                &current.run.id,
                current.occurrence.retry_attempt,
            )
            .await?;
            let delivery_id = match (queued, prepared.delivery.take()) {
                (Some(event), Some(candidate)) => {
                    let persisted =
                        task_delivery::find_delivery_by_key(db, candidate.delivery_key())
                            .await?
                            .ok_or(TaskTerminalConflict(
                                "different committed delivery destination",
                            ))?;
                    if event.idempotency_key.as_deref()
                        != Some(format!("delivery:{}:queued", persisted.id).as_str())
                    {
                        return conflict("delivery identity differs from its queued event");
                    }
                    let persisted = task_delivery::resolve_queue_replay(persisted, candidate)
                        .context(TaskTerminalConflict("different committed delivery facts"))?;
                    // Validate authority with the original id and current state;
                    // never upsert a progressed delivery or authority on replay.
                    let authority =
                        crate::task_projector::prepare_task_delivery_authority(db, &persisted)
                            .await?;
                    task_actor_contract::validate_delivery_authority_replay(db, &authority)
                        .await
                        .context(TaskTerminalConflict(
                            "different committed delivery authority",
                        ))?;
                    Some(persisted.id)
                }
                (None, None) => None,
                _ => return conflict("different committed delivery plan"),
            };
            // Retry batches do not finalize execution/occurrence in the old
            // contract. Query their durable successor, not a new random retry id.
            let retry_scheduled = task_run::find_retry_successor(db, &current.run.id)
                .await?
                .is_some();
            if retry_scheduled != prepared.retry_scheduled {
                return conflict("different retry decision");
            }
            let completed_at = current
                .run
                .completed_at
                .context("terminal run has no completion time")?
                .timestamp();
            if !retry_scheduled {
                self.finalize_task_terminal_in_connection(db, &prepared, completed_at)
                    .await?;
            }
            return Ok(DeferredTaskTerminalCommitOutcome {
                status: TaskTerminalCommitStatus::Replayed,
                events: Vec::new(),
                completed_at,
                delivery_id,
                retry_scheduled,
            });
        }
        if task_status_from_db(&current.task.status)
            .context("invalid task status")?
            .is_terminal()
        {
            // Preserve the existing late-run rule: release locks, do not change
            // the terminal task or finalize its still nonterminal run.
            if current.locks != prepared.state.locks {
                return conflict("locks changed during preparation");
            }
            let events = prepared
                .events
                .into_iter()
                .filter(|e| matches!(e.payload(), TaskEventPayload::WriteLockReleased { .. }))
                .collect();
            let events = append_task_events_in_connection_with_delivery_policy(
                db,
                events,
                prepared.at,
                false,
                &self.outputs,
            )
            .await?;
            return Ok(DeferredTaskTerminalCommitOutcome {
                status: TaskTerminalCommitStatus::TaskAlreadyTerminal,
                events,
                completed_at: prepared.at,
                delivery_id: None,
                retry_scheduled: false,
            });
        }
        if !same_transition_state(&prepared.state, &current) {
            if same_after_additional_progress(&prepared.state, &current) {
                return Err(AdditionalRunsTerminalRace.into());
            }
            if same_after_webhook_progress(&prepared.state, &current)
                && webhook_progress_is_proven(db, &prepared.state, &current).await?
            {
                return Err(WebhookProgressTerminalRace.into());
            }
            return conflict("transition facts changed during preparation");
        }
        let delivery_id = if let Some(delivery) =
            prepared.events.iter().find_map(|e| match e.payload() {
                TaskEventPayload::DeliveryQueued { delivery } => Some(delivery),
                _ => None,
            }) {
            if task_delivery::find_delivery_by_key(db, &delivery.delivery_key)
                .await?
                .is_some()
            {
                // An existing delivery without the committed terminal batch is
                // an inconsistent domain fact, not an insertion to ignore.
                return conflict("delivery exists without terminal run");
            }
            Some(delivery.id.clone())
        } else {
            None
        };
        let events = append_task_events_in_connection_with_delivery_policy(
            db,
            std::mem::take(&mut prepared.events),
            prepared.at,
            false,
            &self.outputs,
        )
        .await?;
        if matches!(prepared.terminal, TaskEventPayload::RunCancelled { .. })
            && let Some(actor) = crate::task_cancellation_actor::principal(
                db,
                &prepared.state.task.workspace_id,
                &prepared.state.task.id,
                &prepared.state.run.id,
            )
            .await?
        {
            crate::completion_inbox::mark_cancelled_task_run_read(
                db,
                &actor,
                &prepared.state.task.workspace_id,
                &prepared.state.task.id,
                &prepared.state.run.id,
                &chrono::Utc::now().to_rfc3339(),
            )
            .await?;
        }
        if !prepared.retry_scheduled {
            self.finalize_task_terminal_in_connection(db, &prepared, prepared.at)
                .await?;
        }
        Ok(DeferredTaskTerminalCommitOutcome {
            status: TaskTerminalCommitStatus::Applied,
            events,
            completed_at: prepared.at,
            delivery_id,
            retry_scheduled: prepared.retry_scheduled,
        })
    }
}

async fn load_terminal_state<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    run_id: &str,
) -> Result<TerminalState> {
    // Capture the queue watermark before mutable aggregate/trigger facts. On
    // the preparation reader these are separate reads: an admission between
    // them must still count as queue growth at the final writer comparison.
    let latest_run_number = task_run::find_latest_numbered_run(db, task_id)
        .await?
        .map(|r| r.run_number)
        .unwrap_or(0);
    let task = task_repository::find_task_by_id(db, task_id)
        .await?
        .context("terminal Task disappeared")?;
    let run = task_run::find_run_by_id(db, run_id)
        .await?
        .context("terminal Task run disappeared")?;
    if run.task_id != task.id {
        bail!("terminal Task run belongs to another task");
    }
    let trigger = match run.trigger_id.as_deref() {
        Some(id) => task_trigger::find_trigger_by_id(db, id).await?,
        // Manual occurrences have no schedule-fire identity, but completion
        // must still refresh aggregate status from the current schedule.
        None => task_trigger::find_latest_trigger_by_task(db, task_id).await?,
    };
    let agent_spec = task_agent_spec::find_terminal_agent_spec(db, task_id, run_id).await?;
    let locks = task_write_lock::list_terminal_locks_by_run(db, run_id).await?;
    let actor = task_actor_contract::find_task_actor_contract(db, task_id)
        .await?
        .context("terminal Task has no actor contract")?;
    let execution_fence = match task_run_execution::find_execution_by_run(db, run_id).await? {
        Some(execution) => {
            let generation = if execution.executor_kind == "agent" {
                load_agent_execution_resource_state(db, &execution.id)
                    .await?
                    .map(|s| s.attempt_generation)
            } else {
                None
            };
            Some((execution.id, generation))
        }
        None => None,
    };
    let mut occurrence = task_actor_contract::task_occurrence_contract_from_model(
        task_actor_contract::find_terminal_occurrence_row(db, run_id)
            .await?
            .context("terminal Task run has no occurrence")?,
    )?;
    // Progress/finalization are not frozen routing or fencing inputs.
    occurrence.status = TaskOccurrenceStatus::Running;
    occurrence.terminal_reason = None;
    occurrence.queue_position = None;
    let finish_first_run = task_repository::finish_first_run(db, task_id).await?;
    let finish_runs = match finish_first_run {
        Some(first) => Some(crate::finish::finish_runs(db, task_id, first, Some(run_id)).await?),
        None => None,
    };
    // Preparation and the writer both read these same bounded allocation and
    // completion facts. No sibling list or unbounded history is materialized.
    let completion = crate::values::optional_json_value_from_db::<
        pioneer_protocol::TaskLifecyclePolicy,
    >(task.lifecycle_policy_json.clone())?
    .map(|policy| policy.completion)
    .unwrap_or(TaskCompletionBehavior::CompleteOnTerminalRun);
    let additional_runs = if completion == TaskCompletionBehavior::CompleteOnTerminalRun {
        task_run::additional_completion_runs(db, task_id, run_id).await?
    } else {
        None
    };
    Ok(TerminalState {
        finish_first_run,
        finish_runs,
        task,
        run,
        trigger,
        latest_run_number,
        additional_runs,
        agent_spec,
        locks,
        actor,
        occurrence,
        execution_fence,
    })
}

fn validate_terminal_policy(expected: &TerminalState, current: &TerminalState) -> Result<()> {
    if expected.task.executor_kind != current.task.executor_kind
        || expected.task.workspace_id != current.task.workspace_id
        || expected.task.owner_kind != current.task.owner_kind
        || expected.task.owner_id != current.task.owner_id
        || expected.task.lifecycle_policy_json != current.task.lifecycle_policy_json
        || expected.task.delivery_policy_json != current.task.delivery_policy_json
        || expected.task.retry_policy_json != current.task.retry_policy_json
        || expected.actor != current.actor
        || expected.occurrence != current.occurrence
        || expected.execution_fence != current.execution_fence
    {
        return conflict("policy, authority or execution fence changed");
    }
    Ok(())
}

fn same_transition_state(expected: &TerminalState, current: &TerminalState) -> bool {
    let mut expected = expected.clone();
    let mut current = current.clone();
    // Heartbeats/progress do not change eligibility. Resume changes revision,
    // occurrence execution_generation, status and/or run start fencing facts.
    expected.run.heartbeat_at = None;
    current.run.heartbeat_at = None;
    expected.run.lock_expires_at = None;
    current.run.lock_expires_at = None;
    expected.run.updated_at = current.run.updated_at;
    expected.task.updated_at = current.task.updated_at;
    if current.finish_first_run.is_some()
        && task_status_from_db(&expected.task.status).is_some_and(|status| !status.is_terminal())
        && task_status_from_db(&current.task.status).is_some_and(|status| !status.is_terminal())
    {
        // A sibling entering review changes only the task's aggregate activity
        // label; the exact run, policy and execution fence remain authoritative.
        expected.task.status = current.task.status.clone();
    }
    expected == current
}

/// Only sibling admission/terminal progress may invalidate this drain decision.
/// Policy, current run/attempt, locks, task revision and schedule configuration
/// remain fenced. A finish change is handled by its existing reconciliation.
fn same_after_additional_progress(expected: &TerminalState, current: &TerminalState) -> bool {
    if expected.additional_runs.is_none() && current.additional_runs.is_none() {
        return false;
    }
    if current.latest_run_number < expected.latest_run_number
        || !matches!(
            expected.task.status.as_str(),
            "queued" | "running" | "waiting" | "waiting_review" | "scheduled"
        )
        || !matches!(
            current.task.status.as_str(),
            "queued" | "running" | "waiting" | "waiting_review" | "scheduled"
        )
    {
        return false;
    }
    let mut normalized = expected.clone();
    normalized.additional_runs = current.additional_runs.clone();
    normalized.latest_run_number = current.latest_run_number;
    normalized.task.status = current.task.status.clone();
    if let (Some(before), Some(after)) = (&mut normalized.trigger, &current.trigger) {
        // A planned fire can admit a sibling during preparation. Accept only
        // monotonic consumption of the same assignment, never pause, resume or
        // replacement of its identity/spec. The full row is still compared.
        if before.status == "active"
            && matches!(after.status.as_str(), "active" | "exhausted")
            && after.last_fire_at.is_some()
            && after.last_fire_at >= before.last_fire_at
            && before
                .next_fire_at
                .is_some_and(|due| after.last_fire_at.is_some_and(|fired| fired >= due))
            && (after.next_fire_at > before.next_fire_at
                || (after.status == "exhausted" && after.next_fire_at.is_none()))
        {
            before.status = after.status.clone();
            before.next_fire_at = after.next_fire_at;
            before.last_fire_at = after.last_fire_at;
        }
        // A terminal refresh changes only this timestamp.
        before.updated_at = after.updated_at;
    }
    same_transition_state(&normalized, current)
}

/// Permit only webhook queue growth and the sibling activity labels below. The
/// existing snapshot comparison still owns every other field, including the
/// source run, frozen spec/occurrence, locks, task revision and attempt fence.
fn same_after_webhook_progress(expected: &TerminalState, current: &TerminalState) -> bool {
    let queue_grew = current.latest_run_number > expected.latest_run_number;
    let sibling_activity =
        matches!(
            expected.task.status.as_str(),
            "queued" | "running" | "waiting_review"
        ) && matches!(current.task.status.as_str(), "running" | "waiting_review");
    if current.latest_run_number < expected.latest_run_number
        || current.finish_first_run != expected.finish_first_run
        || !(current.task.status == expected.task.status
            || (queue_grew && current.task.status == "queued")
            || sibling_activity)
    {
        return false;
    }
    let (Some(before), Some(after)) = (&expected.trigger, &current.trigger) else {
        return false;
    };
    // Match the exact durable webhook spec without decoding a body or allowing
    // pause/resume/reschedule/cancellation to masquerade as queue growth.
    let Ok(pioneer_protocol::TaskTriggerSpec::External {
        source,
        event_type: None,
        filter: None,
    }) = serde_json::from_value::<pioneer_protocol::TaskTriggerSpec>(before.spec_json.clone())
    else {
        return false;
    };
    if source != "webhook"
        || before.status != "active"
        || after.status != "active"
        || before.webhook_secret_ref.is_none()
        || before.next_fire_at.is_some()
        || after.next_fire_at.is_some()
        || after.last_fire_at.is_none()
        || after.last_fire_at < before.last_fire_at
        || after.updated_at < before.updated_at
    {
        return false;
    }
    let mut normalized = expected.clone();
    normalized.latest_run_number = current.latest_run_number;
    normalized.task.status = current.task.status.clone();
    if queue_grew {
        let trigger = normalized.trigger.as_mut().expect("checked trigger");
        trigger.last_fire_at = after.last_fire_at;
        trigger.updated_at = after.updated_at;
    }
    same_transition_state(&normalized, current)
}

/// These bounded proofs are read in the same writer snapshot as the protected
/// comparison, before any terminal projection. They do not deserialize bodies.
async fn webhook_progress_is_proven<C: ConnectionTrait>(
    db: &C,
    expected: &TerminalState,
    current: &TerminalState,
) -> Result<bool> {
    let trigger_id = &current
        .trigger
        .as_ref()
        .expect("validated webhook trigger")
        .id;
    let queue_grew = current.latest_run_number > expected.latest_run_number;
    if queue_grew
        && !task_run::is_webhook_queue_run(
            db,
            &current.task.id,
            trigger_id,
            &current.run.id,
            current.latest_run_number,
        )
        .await?
    {
        return Ok(false);
    }
    if current.task.status != expected.task.status
        && !(queue_grew && current.task.status == "queued")
    {
        return task_run::has_webhook_sibling_activity(
            db,
            &current.task.id,
            trigger_id,
            &current.run.id,
            &current.task.status,
        )
        .await;
    }
    Ok(true)
}

/// Close one durable agent domain execution and release only the resources
/// owned by that execution attempt.  This is deliberately idempotent: task
/// reconciliation may observe the same terminal child more than once after a
/// restart.  A sibling execution in the same root is never changed.
pub async fn finalize_agent_execution(
    db: &DatabaseTransaction,
    execution_id: &str,
    terminal_status: &str,
    finished_at: DateTimeWithTimeZone,
) -> Result<bool> {
    use pioneer_crud_execution::agent_execution::{
        finalize_agent_execution_status, release_finalized_agent_execution,
    };
    let finalized =
        finalize_agent_execution_status(db, execution_id, terminal_status, finished_at).await?;
    crate::task_occurrence_producers::agent_change(
        db,
        Some(&finalized.previous),
        Some(&finalized.current),
    )
    .await?;
    release_finalized_agent_execution(db, &finalized, finished_at).await?;
    Ok(true)
}
