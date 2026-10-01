//! Terminal orchestration prepares CPU work on readers, then validates all
//! state-dependent decisions before the first event in one writer transaction.
use crate::convention::{
    task_run_execution_status_from_db, task_run_execution_status_to_db, task_run_status_from_db,
    task_status_from_db,
};
use crate::repositories::{
    task as task_repository, task_actor_contract, task_agent_spec, task_delivery, task_event,
    task_run, task_run_execution, task_trigger, task_write_lock,
};
use crate::util::unix_to_datetime;
use crate::{
    AppendedTaskEvent, CrudStore, load_agent_execution_resource_state,
    task_agent_spec_from_db_model, task_delivery_from_db_model, task_from_db_model,
    task_run_from_db_model, task_trigger_from_db_model, task_write_lock_from_db_model,
};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_protocol::{
    TaskActorContract, TaskCompletionBehavior, TaskDeliveryMode, TaskDeliveryStatus, TaskError,
    TaskErrorClass, TaskEventPayload, TaskGetResponse, TaskOccurrenceContract,
    TaskOccurrenceStatus, TaskRescheduleReason, TaskResult, TaskRetryBackoffKind, TaskRun,
    TaskRunExecutionStatus, TaskRunStatus, TaskWriteLockStatus, generate_id,
};
use sea_orm::{ConnectionTrait, TransactionTrait};
use sha2::{Digest, Sha256};

const ID_LEN: usize = 21;

/// A rejected domain transition is not a transient SQLite insertion failure.
#[derive(Debug)]
pub struct TaskTerminalConflict(pub &'static str);

impl std::fmt::Display for TaskTerminalConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "terminal Task conflict: {}", self.0)
    }
}
impl std::error::Error for TaskTerminalConflict {}

fn conflict<T>(reason: &'static str) -> Result<T> {
    Err(TaskTerminalConflict(reason).into())
}

#[derive(Clone, Debug, PartialEq)]
struct TerminalState {
    task: pioneer_entity::task::Model,
    run: pioneer_entity::task_run::Model,
    trigger: Option<pioneer_entity::task_trigger::Model>,
    latest_run_number: i64,
    agent_spec: Option<pioneer_entity::task_agent_spec::Model>,
    locks: Vec<pioneer_entity::task_write_lock::Model>,
    actor: TaskActorContract,
    occurrence: TaskOccurrenceContract,
    execution_fence: Option<(String, Option<i64>)>,
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

impl CrudStore {
    pub async fn prepare_task_terminal_transition(
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
        let task = task_from_db_model(state.task.clone())?;
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
            && matches!(terminal, TaskEventPayload::RunFailed { .. })
            && prepare_retry(
                &response,
                state.latest_run_number,
                &mut events,
                error.clone(),
                at,
            )?;
        let mut delivery = None;
        let implicit_result = result.is_none();
        let implicit_error = error.is_none();
        if !retry_scheduled && !suppressed {
            if response
                .task
                .lifecycle_policy
                .as_ref()
                .map(|p| p.completion)
                .unwrap_or(TaskCompletionBehavior::CompleteOnTerminalRun)
                == TaskCompletionBehavior::CompleteOnTerminalRun
            {
                events.push(match &terminal {
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
                });
            } else if let Some(mut trigger) = response.triggers.first().cloned() {
                trigger.updated_at = at;
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
            if let Some(mut queued) = delivery_for_terminal_run(
                &response,
                policy,
                &state.run.id,
                at,
                result,
                error,
                state.actor.delivery.destination_user_id.as_deref(),
            ) {
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
        let events = self.prepare_task_events_for_write(events).await?;
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
        })
    }

    pub async fn commit_task_terminal_transition(
        &self,
        prepared: PreparedTaskTerminalTransition,
    ) -> Result<TaskTerminalCommitOutcome> {
        self.run_serialized_write(|| self.commit_task_terminal_transition_once(prepared.clone()))
            .await
    }

    async fn commit_task_terminal_transition_once(
        &self,
        prepared: PreparedTaskTerminalTransition,
    ) -> Result<TaskTerminalCommitOutcome> {
        let tx = self
            .connection
            .begin()
            .await
            .context("failed to begin terminal Task transition")?;
        let outcome = self.commit_task_terminal_in_connection(&tx, prepared).await;
        match outcome {
            Ok(outcome) => {
                tx.commit().await?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    async fn finalize_task_terminal_in_connection<C: ConnectionTrait>(
        &self,
        db: &C,
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
                crate::repositories::agent_domain::load_agent_execution(db, &execution.id).await?
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
                if execution.status != task_run_execution_status_to_db(status)
                    || execution.result_json != prepared.result_json
                    || execution.error_json != prepared.error_json
                {
                    // Work-graph cancellation fences Task executions before
                    // Task events are emitted. That fence intentionally has no
                    // result/error payload; retain it when closing the Task.
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
                        return conflict("different committed execution outcome");
                    }
                    // The graph fence owns its original completion time and
                    // reason. Both execution and occurrence are finalized.
                    return Ok(());
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
                        crate::repositories::agent_domain::finalize_agent_execution(
                            db,
                            &execution.id,
                            agent_status,
                            at,
                        )
                        .await?;
                    }
                    // Reservation/claim precedes graph admission. A startup
                    // failure or cancellation can close that unbound execution
                    // without inventing an AgentExecution or ignoring a lost one.
                }
            }
        }
        task_actor_contract::finalize_task_occurrence(
            db,
            &prepared.state.run.id,
            occurrence_status,
            prepared.terminal_reason.clone(),
            at,
        )
        .await
    }

    async fn commit_task_terminal_in_connection<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        mut prepared: PreparedTaskTerminalTransition,
    ) -> Result<TaskTerminalCommitOutcome> {
        // These exact, bounded reads happen BEFORE RunCompleted/Failed/etc.
        // Serialization, random identity allocation and retry calculation have
        // already finished. No provider work or notification runs in this scope.
        let current =
            load_terminal_state(db, &prepared.state.task.id, &prepared.state.run.id).await?;
        validate_terminal_policy(&prepared.state, &current)?;
        let run_terminal = task_run_status_from_db(&current.run.status)
            .context("invalid terminal run status")?
            .is_terminal();
        if run_terminal {
            let expected_status = match prepared.terminal {
                TaskEventPayload::RunCompleted { .. } => "succeeded",
                TaskEventPayload::RunFailed { .. } => "failed",
                TaskEventPayload::RunBlocked { .. } => "blocked",
                TaskEventPayload::RunCancelled { .. } => "cancelled",
                _ => unreachable!(),
            };
            let event = task_event::find_event_by_idempotency_key(
                db,
                &current.task.id,
                &format!("run:{}:terminal", current.run.id),
            )
            .await?
            .context("terminal Task run has no terminal event")?;
            if current.run.status != expected_status
                || event.event_type != prepared.terminal.event_type()
                || current.run.result_json != prepared.result_json
                || current.run.error_json != prepared.error_json
            {
                return conflict("different committed run outcome");
            }
            let queued = task_event::find_queued_delivery_for_run(db, &current.run.id).await?;
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
            return Ok(TaskTerminalCommitOutcome {
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
            let events = self
                .append_task_events_in_connection(db, events, prepared.at)
                .await?;
            return Ok(TaskTerminalCommitOutcome {
                status: TaskTerminalCommitStatus::TaskAlreadyTerminal,
                events,
                completed_at: prepared.at,
                delivery_id: None,
                retry_scheduled: false,
            });
        }
        if !same_transition_state(&prepared.state, &current) {
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
        let events = self
            .append_task_events_in_connection(db, std::mem::take(&mut prepared.events), prepared.at)
            .await?;
        if !prepared.retry_scheduled {
            self.finalize_task_terminal_in_connection(db, &prepared, prepared.at)
                .await?;
        }
        Ok(TaskTerminalCommitOutcome {
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
        None => None,
    };
    let latest_run_number = task_run::find_latest_numbered_run(db, task_id)
        .await?
        .map(|r| r.run_number)
        .unwrap_or(0);
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
    let mut occurrence = task_actor_contract::find_task_occurrence_by_run_id(db, run_id)
        .await?
        .context("terminal Task run has no occurrence")?;
    // Progress/finalization are not frozen routing or fencing inputs.
    occurrence.status = TaskOccurrenceStatus::Running;
    occurrence.terminal_reason = None;
    occurrence.queue_position = None;
    Ok(TerminalState {
        task,
        run,
        trigger,
        latest_run_number,
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
    expected == current
}

fn terminal_facts(
    event: &TaskEventPayload,
) -> Result<(
    i64,
    Option<TaskResult>,
    Option<TaskError>,
    TaskWriteLockStatus,
    Option<String>,
)> {
    Ok(match event {
        TaskEventPayload::RunCompleted {
            result,
            completed_at,
            ..
        } => (
            *completed_at,
            result.clone(),
            None,
            TaskWriteLockStatus::Released,
            Some("run completed".into()),
        ),
        TaskEventPayload::RunFailed {
            error,
            completed_at,
            ..
        } => (
            *completed_at,
            None,
            error.clone(),
            TaskWriteLockStatus::Released,
            Some("run failed".into()),
        ),
        TaskEventPayload::RunBlocked {
            error, blocked_at, ..
        } => (
            *blocked_at,
            None,
            error.clone(),
            TaskWriteLockStatus::Released,
            Some("run blocked".into()),
        ),
        TaskEventPayload::RunCancelled {
            run_id,
            reason,
            cancelled_at,
            ..
        } => (
            *cancelled_at,
            None,
            reason.as_ref().map(|message| TaskError {
                recovery_diagnostic: None,
                code: "task_run_cancelled".into(),
                message: message.clone(),
                class: TaskErrorClass::Cancelled,
                details: None,
                failed_run_id: Some(run_id.clone()),
            }),
            TaskWriteLockStatus::Cancelled,
            reason.clone(),
        ),
        _ => bail!("expected a terminal Task run event"),
    })
}

fn delivery_target_fingerprint(target: &str) -> String {
    hex::encode(Sha256::digest(target.as_bytes()))
}

fn prepare_retry(
    task_response: &TaskGetResponse,
    latest_run_number: i64,
    events: &mut Vec<TaskEventPayload>,
    error: Option<TaskError>,
    completed_at: i64,
) -> Result<bool> {
    let failed_run = task_response
        .runs
        .first()
        .context("missing prepared run")?
        .clone();
    let Some(policy) = task_response.task.retry_policy.as_ref() else {
        return Ok(false);
    };
    let error_class = error
        .as_ref()
        .map(|error| error.class)
        .unwrap_or(pioneer_protocol::TaskErrorClass::Unknown);
    if !policy.retry_on.iter().any(|class| *class == error_class) {
        return Ok(false);
    }
    if policy.max_attempts <= failed_run.attempt_number {
        events.push(TaskEventPayload::RunRetryExhausted {
            task_id: task_response.task.id.clone(),
            run_group_id: failed_run.run_group_id.clone(),
            final_run_id: failed_run.id.clone(),
            error,
            exhausted_at: completed_at,
        });
        return Ok(false);
    }

    let next_attempt = failed_run.attempt_number.saturating_add(1);
    let delay_seconds = retry_delay_seconds(policy, next_attempt)?;
    let ready_at = completed_at.saturating_add(delay_seconds);
    let retry_run = TaskRun {
        id: generate_id(ID_LEN),
        task_id: task_response.task.id.clone(),
        trigger_id: failed_run.trigger_id.clone(),
        parent_run_id: Some(failed_run.id.clone()),
        run_group_id: failed_run.run_group_id.clone(),
        attempt_number: next_attempt,
        retry_of_run_id: Some(failed_run.id.clone()),
        ready_at: Some(ready_at),
        run_number: latest_run_number.saturating_add(1),
        status: TaskRunStatus::Queued,
        executor_kind: failed_run.executor_kind,
        started_at: None,
        completed_at: None,
        heartbeat_at: None,
        locked_by: None,
        lock_expires_at: None,
        result: None,
        error: None,
        created_at: completed_at,
        updated_at: completed_at,
    };
    let agent_spec = task_response
        .agent_specs
        .iter()
        .rev()
        .find(|spec| spec.run_id.as_deref() == Some(failed_run.id.as_str()))
        .or_else(|| {
            task_response
                .agent_specs
                .iter()
                .rev()
                .find(|spec| spec.run_id.is_none())
        })
        .cloned()
        .map(|mut spec| {
            spec.run_id = Some(retry_run.id.clone());
            spec.updated_at = completed_at;
            spec
        });
    events.push(TaskEventPayload::TaskQueued {
        task_id: task_response.task.id.clone(),
        run_id: Some(retry_run.id.clone()),
    });
    events.push(TaskEventPayload::RunRetryScheduled {
        task_id: task_response.task.id.clone(),
        failed_run_id: failed_run.id,
        retry_run: retry_run.clone(),
        next_attempt_at: ready_at,
        reason: error,
    });
    events.push(TaskEventPayload::RunCreated {
        run: retry_run,
        agent_spec,
    });
    Ok(true)
}
fn retry_delay_seconds(
    policy: &pioneer_protocol::TaskRetryPolicy,
    next_attempt_number: u32,
) -> Result<i64> {
    let delay = match policy.backoff {
        TaskRetryBackoffKind::None => 0,
        TaskRetryBackoffKind::Fixed => policy.initial_delay_seconds.unwrap_or(0),
        TaskRetryBackoffKind::Exponential => {
            let initial = policy.initial_delay_seconds.unwrap_or(1).max(1);
            let exponent = next_attempt_number.saturating_sub(2).min(30);
            let multiplier = 1_i64.checked_shl(exponent).unwrap_or(i64::MAX);
            initial.saturating_mul(multiplier)
        }
    };
    let capped = policy
        .max_delay_seconds
        .map(|max_delay| delay.min(max_delay.max(0)))
        .unwrap_or(delay);
    Ok(capped.max(0))
}

fn delivery_for_terminal_run(
    task_response: &TaskGetResponse,
    policy: Option<&pioneer_protocol::TaskDeliveryPolicy>,
    run_id: &str,
    event_timestamp_secs: i64,
    result_snapshot: Option<TaskResult>,
    error_snapshot: Option<TaskError>,
    exact_notification_recipient: Option<&str>,
) -> Option<pioneer_protocol::TaskDelivery> {
    let policy = policy?;
    if policy.mode == TaskDeliveryMode::None {
        return None;
    }
    let thread_target = (policy.mode == TaskDeliveryMode::Thread)
        .then_some(policy.thread_target)
        .flatten();
    if policy.mode == TaskDeliveryMode::Thread && thread_target.is_none() {
        return None;
    }
    let target_thread_id = match policy.mode {
        TaskDeliveryMode::Thread => policy.thread_id.clone(),
        _ => None,
    };
    let target_user_id = (policy.mode == TaskDeliveryMode::UserNotification)
        .then(|| {
            exact_notification_recipient.map(str::to_owned).or_else(|| {
                (task_response.task.owner_kind == pioneer_protocol::TaskOwnerKind::User)
                    .then(|| task_response.task.owner_id.clone())
                    .flatten()
            })
        })
        .flatten();
    let webhook_url = (policy.mode == TaskDeliveryMode::Webhook)
        .then(|| policy.webhook_url.clone())
        .flatten();
    if policy.mode == TaskDeliveryMode::Thread && target_thread_id.is_none() {
        return None;
    }
    if policy.mode == TaskDeliveryMode::UserNotification && target_user_id.is_none() {
        return None;
    }
    if policy.mode == TaskDeliveryMode::Webhook && webhook_url.is_none() {
        return None;
    }
    let run = task_response.runs.iter().rev().find(|run| run.id == run_id);
    let result_snapshot = if policy.include_result {
        result_snapshot
            .or_else(|| run.and_then(|run| run.result.clone()))
            .or_else(|| task_response.task.result.clone())
            .map(|result| task_delivery_result_snapshot(result, policy.format))
    } else {
        None
    };
    let error_snapshot = error_snapshot.or_else(|| {
        run.and_then(|run| run.error.clone())
            .or_else(|| task_response.task.error.clone())
    });
    let target = target_thread_id
        .clone()
        .or_else(|| target_user_id.clone())
        .or_else(|| webhook_url.clone())
        .unwrap_or_else(|| "none".to_owned());
    let delivery_key = match thread_target {
        Some(thread_target) => format!(
            "{}:{}:{}:{}:{}",
            task_response.task.id,
            run_id,
            delivery_mode_key(policy.mode),
            delivery_thread_target_key(thread_target),
            target
        ),
        None => format!(
            "{}:{}:{}:{}",
            task_response.task.id,
            run_id,
            delivery_mode_key(policy.mode),
            target
        ),
    };
    Some(pioneer_protocol::TaskDelivery {
        id: generate_id(ID_LEN),
        workspace_id: task_response.task.workspace_id.clone(),
        task_id: task_response.task.id.clone(),
        run_id: run_id.to_owned(),
        delivery_key,
        mode: policy.mode,
        thread_target,
        target_thread_id,
        target_user_id,
        webhook_url: webhook_url.clone(),
        webhook_url_fingerprint: webhook_url.as_deref().map(delivery_target_fingerprint),
        status: TaskDeliveryStatus::Pending,
        next_attempt_at: Some(event_timestamp_secs),
        attempt_count: 0,
        // Every surface is an idempotent outbox delivery. A crash after the
        // destination commit but before acknowledgement must retry the same
        // deterministic receipt instead of stranding it after one attempt.
        max_attempts: 3,
        result_snapshot,
        error_snapshot,
        delivered_turn_id: None,
        delivered_notification_id: None,
        delivered_at: None,
        last_error: None,
        created_at: event_timestamp_secs,
        updated_at: event_timestamp_secs,
    })
}

fn task_delivery_result_snapshot(
    result: TaskResult,
    format: pioneer_protocol::TaskDeliveryFormat,
) -> TaskResult {
    match format {
        pioneer_protocol::TaskDeliveryFormat::FullResult => result,
        pioneer_protocol::TaskDeliveryFormat::Summary => TaskResult {
            summary: result.summary,
            data: None,
            artifacts: Vec::new(),
            completed_by_run_id: result.completed_by_run_id,
        },
    }
}

fn delivery_mode_key(mode: TaskDeliveryMode) -> &'static str {
    match mode {
        TaskDeliveryMode::None => "none",
        TaskDeliveryMode::Thread => "thread",
        TaskDeliveryMode::UserNotification => "user_notification",
        TaskDeliveryMode::Webhook => "webhook",
    }
}

fn delivery_thread_target_key(target: pioneer_protocol::TaskDeliveryThreadTarget) -> &'static str {
    match target {
        pioneer_protocol::TaskDeliveryThreadTarget::OriginThread => "origin_thread",
        pioneer_protocol::TaskDeliveryThreadTarget::CurrentThread => "current_thread",
        pioneer_protocol::TaskDeliveryThreadTarget::CollaborationRoot => "collaboration_root",
        pioneer_protocol::TaskDeliveryThreadTarget::ExactThread => "exact_thread",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{TaskArtifact, TaskDeliveryFormat, TaskValue};
    use std::collections::BTreeMap;

    #[test]
    fn summary_delivery_snapshot_drops_full_data_and_artifacts() {
        let result = TaskResult {
            summary: Some("safe summary".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([(
                "secret".to_owned(),
                TaskValue::String("must-not-cross".to_owned()),
            )]))),
            artifacts: vec![TaskArtifact {
                artifact_id: Some("artifact-1".to_owned()),
                version_id: Some("version-1".to_owned()),
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some("run-1".to_owned()),
        };

        let projected = task_delivery_result_snapshot(result, TaskDeliveryFormat::Summary);
        assert_eq!(projected.summary.as_deref(), Some("safe summary"));
        assert!(projected.data.is_none());
        assert!(projected.artifacts.is_empty());
        assert_eq!(projected.completed_by_run_id.as_deref(), Some("run-1"));
    }
}
