//! Resume a blocked child by reopening its complete Task-owned execution.
use crate::event_pipeline::{
    append_task_events_in_connection_with_delivery_policy, prepare_task_events_for_write,
};
use crate::event_store::TaskEventStore;
use crate::execution_store::task_run_execution_from_db_model;
use crate::status::{
    task_run_execution_status_to_db, task_run_status_to_db, task_run_turn_status_to_db,
    task_status_from_db, task_status_to_db,
};
use crate::task as task_repository;
use crate::task::task_from_db_model;
use crate::task_execution_admission::{self, NewTaskExecutionAdmission};
use crate::task_projector::TaskOutputBinder;
use crate::task_run::task_run_from_db_model;
use crate::task_run_turn::task_run_turn_from_db_model;
use crate::task_trigger::task_trigger_from_db_model;
use crate::values::unix_to_datetime;
use crate::work_change::TaskWorkChange;
use crate::{
    task_actor_contract, task_child_producers, task_occurrence_producers, task_run,
    task_run_execution, task_run_turn, task_trigger, write_lock as task_write_lock,
};
use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_conversations::{
    turn_item::turn_item_type_from_db, turn_reads as turn, turn_writes,
};
use pioneer_crud_execution::{
    execution_admission_lease::ExecutionAdmissionQuotaExhausted,
    execution_heartbeat::Registries,
    gateway_control::{GatewayControlWake, control_work_change},
    recovery_job::{self, RecoveryJobRecord, recovery_job_record_from_model},
    recovery_store::RecoveryChange,
    recovery_terminalization_outbox, turn_execution, turn_runtime_snapshot,
};
use pioneer_protocol::{
    RecoveryAction, RecoveryJobStatus, TASK_REVISION_AUTOMATIC_RETRIES,
    TASK_REVISION_RETRY_PENDING, Task, TaskEventPayload, TaskRun, TaskRunExecution,
    TaskRunExecutionStatus, TaskRunStatus, TaskRunTurn, TaskRunTurnStatus, TaskStatus,
    TurnItemType, TurnStatus, is_missing_cli_mcp_activation_block,
};
use sea_orm::{
    ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect, TransactionTrait,
    entity::prelude::DateTimeWithTimeZone, sea_query::Expr,
};
use std::sync::Arc;
use tokio::sync::Notify;

/// Result of reopening a task-owned blocked child turn.  This is deliberately
/// separate from the generic recovery-job result: a task child has an
/// aggregate owner (Task/TaskRun/TaskRunTurn/execution) that must be reopened
/// in the same durable transaction as the child Turn and recovery job.
#[derive(Debug, Clone)]
pub enum TaskOwnedTurnResumeOutcome {
    Resumed {
        /// Existing committed TaskRecovered sequence; diagnostic lifecycle fence only.
        observation_sequence: i64,
        recovery_job: RecoveryJobRecord,
        task: Task,
        run: TaskRun,
        task_run_turn: TaskRunTurn,
        execution: Option<TaskRunExecution>,
    },
    NotFound,
    MissingRuntimeSnapshot {
        recovery_job_id: String,
    },
    Conflict {
        reason: String,
    },
    RevisionRejected(pioneer_protocol::TaskRevisionRetryBlocker),
    RevisionQuotaExhausted(ExecutionAdmissionQuotaExhausted),
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub async fn resume_task_owned_turn(
        &self,
        heartbeats: &Registries,
        recovery_work: &Arc<Notify>,
        thread_id: &str,
        turn_id: &str,
        recovery_job_id: Option<&str>,
        now_unix: i64,
        execution_owner_id: &str,
        execution_lease_until_unix: i64,
    ) -> Result<Option<TaskOwnedTurnResumeOutcome>> {
        self.resume_task_owned_turn_inner(
            heartbeats,
            recovery_work,
            thread_id,
            turn_id,
            recovery_job_id,
            now_unix,
            execution_owner_id,
            execution_lease_until_unix,
            None,
            |_| Box::pin(async { Ok(()) }),
        )
        .await
    }

    /// Replace the execution Turn of a settled revision while keeping the
    /// review link, TaskRun, occurrence, and frozen Task admission.
    pub async fn retry_task_revision_execution(
        &self,
        heartbeats: &Registries,
        recovery_work: &Arc<Notify>,
        thread_id: &str,
        turn_id: &str,
        automatic: bool,
        admission: &NewTaskExecutionAdmission,
        now_unix: i64,
        commit_action: impl for<'a> Fn(&'a DatabaseTransaction) -> BoxFuture<'a, Result<()>>
        + Send
        + Sync,
    ) -> Result<Option<TaskOwnedTurnResumeOutcome>> {
        self.resume_task_owned_turn_inner(
            heartbeats,
            recovery_work,
            thread_id,
            turn_id,
            None,
            now_unix,
            "",
            now_unix,
            Some((automatic, admission)),
            commit_action,
        )
        .await
    }

    async fn resume_task_owned_turn_inner(
        &self,
        heartbeats: &Registries,
        recovery_work: &Arc<Notify>,
        thread_id: &str,
        turn_id: &str,
        recovery_job_id: Option<&str>,
        now_unix: i64,
        execution_owner_id: &str,
        execution_lease_until_unix: i64,
        revision_retry: Option<(bool, &NewTaskExecutionAdmission)>,
        commit_action: impl for<'a> Fn(&'a DatabaseTransaction) -> BoxFuture<'a, Result<()>>
        + Send
        + Sync,
    ) -> Result<Option<TaskOwnedTurnResumeOutcome>> {
        let control_wakes = vec![
            GatewayControlWake::Turn(turn_id.to_owned()),
            GatewayControlWake::Resources,
            GatewayControlWake::Actions,
            GatewayControlWake::Supervision,
        ];
        let task_change = TaskWorkChange::for_source(&self.connection, "y", turn_id);
        let control_change =
            control_work_change(&self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let _recovery_change = RecoveryChange(recovery_work.clone());
            let change = self.connection.work_signal("recovery.due").changing();
            let result = async {
                let ownership_change = revision_retry.is_none().then(|| heartbeats.changing(execution_owner_id));
                self.retry_write_on_conflict(|| async {
            let now = unix_to_datetime(now_unix);
            let Some(task_run_turn_model) =
                task_run_turn::find_turn_by_thread_and_turn(&self.connection, thread_id, turn_id)
                    .await?
            else {
                return Ok(None);
            };
            let task_run_turn = task_run_turn_from_db_model(task_run_turn_model.clone())?;
            if task_run_turn.status != TaskRunTurnStatus::Blocked {
                return Ok(Some(TaskOwnedTurnResumeOutcome::NotFound));
            }

            let Some(run_model) =
                task_run::find_run_by_id(&self.connection, task_run_turn.run_id.as_str()).await?
            else {
                return Ok(Some(TaskOwnedTurnResumeOutcome::NotFound));
            };
            let run = task_run_from_db_model(run_model.clone())?;
            if run.status != TaskRunStatus::Blocked {
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: format!(
                        "task run `{}` is {:?}; only a blocked run can be resumed",
                        run.id, run.status
                    ),
                }));
            }

            let next_revision_turn = if let Some((automatic, _)) = revision_retry {
                let Some(next_turn) = next_revision_execution(
                    &task_run_turn, &run, automatic, now_unix,
                ) else {
                    return Ok(Some(TaskOwnedTurnResumeOutcome::NotFound));
                };
                Some(next_turn)
            } else {
                None
            };

            let Some(task_model) =
                task_repository::find_task_by_id(&self.connection, run.task_id.as_str()).await?
            else {
                return Ok(Some(TaskOwnedTurnResumeOutcome::NotFound));
            };
            if next_revision_turn.is_some() && task_from_db_model(task_model.clone())?.finish_request().is_some() {
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: "Task has a permanent stop request".to_owned(),
                }));
            }
            let current_task_status = task_status_from_db(task_model.status.as_str())
                .with_context(|| format!("unknown task status `{}`", task_model.status))?;
            let allowed_task_statuses = if next_revision_turn.is_some() {
                &[TaskStatus::Blocked, TaskStatus::Scheduled, TaskStatus::Waiting, TaskStatus::Queued][..]
            } else {
                &[TaskStatus::Blocked, TaskStatus::Scheduled][..]
            };
            if !allowed_task_statuses.contains(&current_task_status) {
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: format!(
                        "task `{}` is {:?}; it cannot be reopened for this child",
                        task_model.id, current_task_status
                    ),
                }));
            }

            if next_revision_turn.is_some()
                && revision_has_newer_run(&self.connection, &run).await?
            {
                return Ok(Some(TaskOwnedTurnResumeOutcome::RevisionRejected(
                    pioneer_protocol::TaskRevisionRetryBlocker::NewerRun,
                )));
            }

            let occurrence = task_actor_contract::find_task_occurrence_by_run_id(
                &self.connection,
                run.id.as_str(),
            )
            .await?
            .with_context(|| {
                format!(
                    "blocked Task run `{}` has no durable occurrence contract",
                    run.id
                )
            })?;
            anyhow::ensure!(
                occurrence.task_id == task_model.id && occurrence.run_id == run.id,
                "blocked Task run occurrence crosses its aggregate boundary"
            );

            if let Some(successor) = task_run::find_conflicting_successor_run(
                &self.connection,
                run.task_id.as_str(),
                run.id.as_str(),
                run_model.run_number,
            )
            .await?
            {
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: format!(
                        "task `{}` has a conflicting newer or active run `{}`",
                        run.task_id, successor.id
                    ),
                }));
            }

            if let Some(conflict) = task_write_lock::find_active_conflict_for_run(
                &self.connection,
                task_model.workspace_id.as_str(),
                run.id.as_str(),
                now,
            )
            .await?
            {
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: format!(
                        "task `{}` write scope conflicts with lock `{}` held by task `{}` run `{}`",
                        run.task_id, conflict.id, conflict.task_id, conflict.run_id
                    ),
                }));
            }

            let Some(job) =
                recovery_job::find_blocked_job_by_turn(&self.connection, turn_id, recovery_job_id)
                    .await?
            else {
                return Ok(Some(TaskOwnedTurnResumeOutcome::NotFound));
            };
            let item_type = turn_item_type_from_db(job.item_type.as_str())
                .unwrap_or(TurnItemType::DynamicToolCall);
            let requires_runtime_snapshot = next_revision_turn.is_none() && item_type.is_tool_item();
            if requires_runtime_snapshot
                && turn_runtime_snapshot::find_turn_runtime_snapshot(&self.connection, turn_id)
                    .await?
                    .is_none()
            {
                return Ok(Some(TaskOwnedTurnResumeOutcome::MissingRuntimeSnapshot {
                    recovery_job_id: job.id.clone(),
                }));
            }

            let turn_model = turn::find_turn_by_thread_and_id(&self.connection, thread_id, turn_id)
                .await?
                .context("task child Turn disappeared before resume")?;
            if turn_status_from_db(turn_model.status.as_str()) != Some(TurnStatus::Blocked) {
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: format!("child Turn `{turn_id}` changed before resume was committed"),
                }));
            }

            let missing_mcp_contract = turn_model
                .error.as_deref().is_some_and(is_missing_cli_mcp_activation_block);
            if next_revision_turn.is_some() && !missing_mcp_contract {
                return Ok(Some(TaskOwnedTurnResumeOutcome::NotFound));
            }
            if next_revision_turn.is_none() && missing_mcp_contract && task_run_turn.revision_execution_retry(turn_id).is_some() {
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: "Use task_get to check whether the blocked revision allows a fresh execution".to_owned(),
                }));
            }

            let execution_model =
                task_run_execution::find_execution_by_run(&self.connection, run.id.as_str())
                    .await?;
            if let Some(execution) = execution_model.as_ref()
                && execution.executor_kind == "agent"
            {
                let agent_execution_id = occurrence
                    .agent_execution_id
                    .as_deref()
                    .context("blocked Agent Task occurrence has no exact AgentExecution")?;
                let root_execution_id = occurrence
                    .work_graph_root_execution_id
                    .as_deref()
                    .context("blocked Agent Task occurrence has no work-graph root")?;
                anyhow::ensure!(
                    agent_execution_id == execution.id,
                    "blocked Agent Task occurrence points at a different execution"
                );
                anyhow::ensure!(
                    occurrence.root_resource_scope_id.as_deref() == Some(root_execution_id),
                    "blocked Agent Task occurrence has an inconsistent resource root"
                );
            }

            let retry_attempt = occurrence
                .retry_attempt
                .checked_add(1)
                .context("Task occurrence retry generation exhausted")?;
            let mut resumed_occurrence = occurrence.retry(retry_attempt).map_err(|error| {
                anyhow::anyhow!("failed to advance Task occurrence retry: {error:?}")
            })?;
            resumed_occurrence.status = pioneer_protocol::TaskOccurrenceStatus::Recovering;
            let prepared_occurrence_retry =
                task_actor_contract::prepare_task_occurrence_retry(
                    &occurrence,
                    &resumed_occurrence,
                    now_unix,
                )?;

            let mut resumed_task = task_from_db_model(task_model.clone())?;
            resumed_task.status = TaskStatus::Running;
            resumed_task.completed_at = None;
            resumed_task.result = None;
            resumed_task.error = None;
            resumed_task.revision = resumed_task
                .revision
                .checked_add(1)
                .context("Task revision is exhausted")?;
            resumed_task.updated_at = now_unix;
            let triggers =
                task_trigger::list_triggers_by_task(&self.connection, resumed_task.id.as_str())
                    .await?
                    .into_iter()
                    .map(task_trigger_from_db_model)
                    .collect::<Result<Vec<_>>>()?;
            let mut resume_events = vec![
                    TaskEventPayload::TaskResumed {
                        task: resumed_task.clone(),
                        triggers,
                        reason: Some("blocked task child resumed".to_owned()),
                        resumed_at: now_unix,
                    },
                    TaskEventPayload::TaskRecovered {
                        task_id: resumed_task.id.clone(),
                        run_id: Some(run.id.clone()),
                            message: if next_revision_turn.is_some() {
                            "fresh revision execution reserved".to_owned()
                        } else {
                            "blocked task child aggregate reopened".to_owned()
                        },
                        recovered_at: now_unix,
                    },
                ];
            if let Some(turn) = &next_revision_turn {
                resume_events.push(TaskEventPayload::TaskRunTurnStarted {
                    task_run_turn: turn.clone(),
                });
            }
            let prepared_events = prepare_task_events_for_write(&self.connection, resume_events).await?;

            let max_attempts = job.max_attempts.max(job.run_count.saturating_add(1));
            let lock_expires_at = unix_to_datetime(now_unix.saturating_add(3600));
            // Quota counts must see contenders that committed while this
            // transaction was waiting for the bucket's advisory lock.
            let isolation = revision_retry.map(|_| sea_orm::IsolationLevel::ReadCommitted);
            let tx = self
                .connection
                .begin_with_config(isolation, None)
                .await
                .context("failed to begin task-owned turn resume transaction")?;

            // The normal event pipeline takes this lock before touching Task
            // rows. Resume must follow that order even though it writes the
            // aggregate before appending its events.
            crate::event_log::lock_task(&tx, &run.task_id).await?;

            let stable_task_run_turn =
                pioneer_entity::task_run_turn::Entity::find_by_id(&task_run_turn_model.id)
                    .lock_exclusive()
                    .one(&tx)
                    .await?
                    == Some(task_run_turn_model.clone());
            let stable_run = pioneer_entity::task_run::Entity::find_by_id(&run.id)
                .lock_exclusive()
                .one(&tx)
                .await?
                == Some(run_model.clone());
            let stable_task = pioneer_entity::task::Entity::find_by_id(&run.task_id)
                .lock_exclusive()
                .one(&tx)
                .await?
                == Some(task_model.clone());
            let stable_job =
                recovery_job::find_blocked_job_by_turn(&tx, turn_id, Some(job.id.as_str())).await?
                    == Some(job.clone());
            let stable_turn = pioneer_entity::turn::Entity::find_by_id(turn_id)
                .filter(pioneer_entity::turn::Column::ThreadId.eq(thread_id))
                .lock_exclusive()
                .one(&tx)
                .await?
                == Some(turn_model.clone());
            let stable_execution = pioneer_entity::task_run_execution::Entity::find()
                .filter(pioneer_entity::task_run_execution::Column::TaskRunId.eq(&run.id))
                .lock_exclusive()
                .one(&tx)
                .await?
                == execution_model;
            if !(stable_task_run_turn
                && stable_run
                && stable_task
                && stable_job
                && stable_turn
                && stable_execution)
            {
                tx.rollback()
                    .await
                    .context("failed to rollback stale task-owned turn resume")?;
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: "task-owned child aggregate changed while resume was preparing"
                        .to_owned(),
                }));
            }
            if next_revision_turn.is_some() && revision_has_newer_run(&tx, &run).await? {
                tx.rollback().await?;
                return Ok(Some(TaskOwnedTurnResumeOutcome::RevisionRejected(
                    pioneer_protocol::TaskRevisionRetryBlocker::NewerRun,
                )));
            }
            if let Some(successor) = task_run::find_conflicting_successor_run(
                &tx,
                run.task_id.as_str(),
                run.id.as_str(),
                run_model.run_number,
            )
            .await?
            {
                tx.rollback()
                    .await
                    .context("failed to rollback successor-conflict task resume transaction")?;
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: format!(
                        "task `{}` has a conflicting newer or active run `{}`",
                        run.task_id, successor.id
                    ),
                }));
            }
            if let Some(conflict) = task_write_lock::find_active_conflict_for_run(
                &tx,
                task_model.workspace_id.as_str(),
                run.id.as_str(),
                now,
            )
            .await?
            {
                tx.rollback()
                    .await
                    .context("failed to rollback lock-conflict task resume transaction")?;
                return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                    reason: format!(
                        "task `{}` write scope conflicts with lock `{}` held by task `{}` run `{}`",
                        run.task_id, conflict.id, conflict.task_id, conflict.run_id
                    ),
                }));
            }
            if requires_runtime_snapshot
                && turn_runtime_snapshot::find_turn_runtime_snapshot(&tx, turn_id)
                    .await?
                    .is_none()
            {
                tx.rollback()
                    .await
                    .context("failed to rollback task tool resume without runtime snapshot")?;
                return Ok(Some(TaskOwnedTurnResumeOutcome::MissingRuntimeSnapshot {
                    recovery_job_id: job.id.clone(),
                }));
            }

            if let Some((_, admission)) = revision_retry {
                // The old Turn stays terminal. This transaction only publishes
                // a fresh reservation once durable CLI control has settled.
                use pioneer_entity::turn_cli_runtime_binding as cli;
                let binding = cli::Entity::find_by_id(turn_id)
                    .lock_exclusive()
                    .one(&tx).await?;
                let owner = pioneer_entity::turn_execution::Entity::find_by_id(turn_id)
                    .lock_exclusive()
                    .one(&tx).await?;
                let latest = task_run_turn::find_latest_turn_by_run(&tx, &run.id).await?;
                if !binding.is_some_and(|binding| {
                    binding.status == "blocked"
                        && binding.thread_id == thread_id
                        && binding.continuation_thread_id == thread_id
                        && binding.workspace_id == task_model.workspace_id
                        // Fresh CLI launch must persist MCP activation before
                        // native dispatch. Neither marker may exist for this
                        // pre-dispatch recovery failure.
                        && binding.mcp_session_generation.is_none()
                        && binding.native_turn_id.is_none()
                }) || !owner.is_some_and(|owner| {
                    owner.status == turn_execution::TurnExecutionStatus::Blocked.as_str()
                        && owner.executor_kind == turn_execution::TurnExecutorKind::CliRuntime.as_str()
                        && owner.thread_id == thread_id
                        && owner.workspace_id == task_model.workspace_id
                }) || latest.as_ref() != Some(&task_run_turn_model)
                    || !execution_model.as_ref().is_some_and(|execution| {
                        execution.status == "blocked" && execution.executor_kind == "agent"
                    })
                {
                    tx.rollback().await?;
                    return Ok(Some(TaskOwnedTurnResumeOutcome::Conflict {
                        reason: "The previous revision execution is not safely settled".to_owned(),
                    }));
                }
                anyhow::ensure!(
                    admission.task_id == run.task_id && admission.workspace_id == task_model.workspace_id,
                    "revision retry admission belongs to a different Task"
                );
                // Quota, the sole successor link and the action receipt commit
                // together. The Task row lock also fences terminal release.
                if let Err(error) =
                    task_execution_admission::readmit_revision(&tx, admission, now).await
                {
                    tx.rollback().await?;
                    return match error.downcast::<ExecutionAdmissionQuotaExhausted>() {
                        Ok(quota) => Ok(Some(TaskOwnedTurnResumeOutcome::RevisionQuotaExhausted(quota))),
                        Err(error) => Err(error),
                    };
                }
            }

            if !recovery_job::resume_blocked_job(
                &tx,
                &job,
                RecoveryAction::RestartTurn,
                max_attempts,
                now,
            )
            .await?
            {
                tx.rollback()
                    .await
                    .context("failed to rollback stale task recovery resume transaction")?;
                return Ok(Some(TaskOwnedTurnResumeOutcome::NotFound));
            }
            recovery_terminalization_outbox::cancel_undelivered_for_job(
                &tx,
                job.id.as_str(),
                "superseded by task-owned execution continuation",
                now,
            )
            .await?;
            anyhow::ensure!(
                recovery_job::mark_job_terminal(
                    &tx,
                    job.id.as_str(),
                    RecoveryJobStatus::Succeeded,
                    None,
                    now,
                )
                .await?,
                "task-owned recovery job `{}` disappeared while resume was committing",
                job.id
            );

            if next_revision_turn.is_none() {
                crate::turn_sources::clear_confirmed_blocked_for_resume(
                    &tx,
                    thread_id,
                    turn_id,
                    now,
                    &turn_model,
                )
                .await?;
                anyhow::ensure!(
                    crate::turn_sources::update_turn_status(
                        &tx,
                        thread_id,
                        turn_id,
                        TurnStatus::InProgress,
                        None,
                        now
                    )
                    .await?,
                    "task child Turn `{turn_id}` was not updated while resuming"
                );
                turn_writes::append_turn_status_history(
                    &tx,
                    turn_id,
                    TurnStatus::InProgress,
                    Some("blocked task child resumed with its TaskRun aggregate".to_owned()),
                    now,
                )
                .await?;
            }

            let blocked = task_run_status_to_db(TaskRunStatus::Blocked);
            let reopened = task_run_status_to_db(TaskRunStatus::Running);
            let run_update = pioneer_entity::task_run::Entity::update_many()
                .filter(pioneer_entity::task_run::Column::Id.eq(run.id.clone()))
                .filter(pioneer_entity::task_run::Column::Status.eq(blocked))
                .filter(pioneer_entity::task_run::Column::UpdatedAt.eq(run_model.updated_at))
                .col_expr(
                    pioneer_entity::task_run::Column::Status,
                    Expr::value(reopened),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::CompletedAt,
                    Expr::value(None::<DateTimeWithTimeZone>),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::HeartbeatAt,
                    Expr::value(Some(now)),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::LockedBy,
                    Expr::value(None::<String>),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::LockExpiresAt,
                    Expr::value(None::<DateTimeWithTimeZone>),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::ResultJson,
                    Expr::value(None::<String>),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::ErrorJson,
                    Expr::value(None::<String>),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::UpdatedAt,
                    Expr::value(now),
                )
                .exec(&tx)
                .await?;
            anyhow::ensure!(
                run_update.rows_affected == 1,
                "task run `{}` changed while resume was committing",
                run.id
            );

            // A Stop that lost to Blocked must not attribute a later
            // cancellation after this run/turn is reopened. Retire only that
            // operation's pending attribution, atomically with actual resume;
            // durable personal read receipts remain untouched.
            {
                use pioneer_entity::task_run_cancellation_actor as stop;
                stop::Entity::delete_many()
                    .filter(stop::Column::RunId.eq(&run.id))
                    .filter(stop::Column::TaskId.eq(&task_model.id))
                    .filter(stop::Column::WorkspaceId.eq(&task_model.workspace_id))
                    .exec(&tx)
                    .await?;
            }

            if let Some(execution) = execution_model.as_ref() {
                let execution_update = pioneer_entity::task_run_execution::Entity::update_many()
                    .filter(pioneer_entity::task_run_execution::Column::Id.eq(execution.id.clone()))
                    .filter(
                        pioneer_entity::task_run_execution::Column::UpdatedAt
                            .eq(execution.updated_at),
                    )
                    .filter(
                        pioneer_entity::task_run_execution::Column::Status.is_in(vec![
                            task_run_execution_status_to_db(TaskRunExecutionStatus::Blocked),
                            task_run_execution_status_to_db(TaskRunExecutionStatus::Failed),
                            task_run_execution_status_to_db(TaskRunExecutionStatus::TimedOut),
                        ]),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::Status,
                        Expr::value(task_run_execution_status_to_db(
                            TaskRunExecutionStatus::Reserved,
                        )),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::WorkerId,
                        Expr::value(None::<String>),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::LeaseUntil,
                        Expr::value(None::<DateTimeWithTimeZone>),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::HeartbeatAt,
                        Expr::value(None::<DateTimeWithTimeZone>),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::CompletedAt,
                        Expr::value(None::<DateTimeWithTimeZone>),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::ResultJson,
                        Expr::value(None::<String>),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::ErrorJson,
                        Expr::value(None::<String>),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::UpdatedAt,
                        Expr::value(now),
                    )
                    .exec(&tx)
                    .await?;
                anyhow::ensure!(
                    execution_update.rows_affected == 1,
                    "task execution `{}` changed while resume was committing",
                    execution.id
                );

                if execution.executor_kind == "agent" {
                    let root_execution_id = occurrence
                        .work_graph_root_execution_id
                        .as_deref()
                        .context("blocked Agent Task occurrence has no work-graph root")?;
                    anyhow::ensure!(
                        crate::agent_execution_binding::reopen_agent_execution_for_retry(
                            &tx,
                            execution.id.as_str(),
                            root_execution_id,
                            now,
                        )
                        .await?,
                        "AgentExecution `{}` changed while resume was committing",
                        execution.id
                    );
                }
            }

            anyhow::ensure!(
                task_actor_contract::apply_prepared_task_occurrence_retry(
                    &tx,
                    &prepared_occurrence_retry,
                )
                .await?,
                "Task occurrence changed while resume was committing"
            );

            if next_revision_turn.is_none() {
                let turn_update = pioneer_entity::task_run_turn::Entity::update_many()
                    .filter(pioneer_entity::task_run_turn::Column::Id.eq(task_run_turn.id.clone()))
                    .filter(
                        pioneer_entity::task_run_turn::Column::Status
                            .eq(task_run_turn_status_to_db(TaskRunTurnStatus::Blocked)),
                    )
                    .col_expr(
                        pioneer_entity::task_run_turn::Column::Status,
                        Expr::value(task_run_turn_status_to_db(TaskRunTurnStatus::InProgress)),
                    )
                    .col_expr(
                        pioneer_entity::task_run_turn::Column::CompletedAt,
                        Expr::value(None::<DateTimeWithTimeZone>),
                    )
                    .exec(&tx)
                    .await?;
                anyhow::ensure!(
                    turn_update.rows_affected == 1,
                    "task run turn `{}` changed while resume was committing",
                    task_run_turn.id
                );
            }

            // One statement reopens the run's complete lock set. Runs with no
            // prior lock rows use TaskExecutor's normal acquisition path.
            if next_revision_turn.is_none() {
                task_write_lock::reopen_locks_for_run(&tx, run.id.as_str(), now, lock_expires_at)
                    .await?;
            }

            let task_update = pioneer_entity::task::Entity::update_many()
                .filter(pioneer_entity::task::Column::Id.eq(run.task_id.clone()))
                .filter(pioneer_entity::task::Column::Revision.eq(task_model.revision))
                .filter(pioneer_entity::task::Column::UpdatedAt.eq(task_model.updated_at))
                .filter(pioneer_entity::task::Column::Status.is_in(
                    allowed_task_statuses.iter().copied().map(task_status_to_db),
                ))
                .col_expr(
                    pioneer_entity::task::Column::Status,
                    Expr::value(task_status_to_db(TaskStatus::Running)),
                )
                .col_expr(
                    pioneer_entity::task::Column::CompletedAt,
                    Expr::value(None::<DateTimeWithTimeZone>),
                )
                .col_expr(
                    pioneer_entity::task::Column::ResultJson,
                    Expr::value(None::<String>),
                )
                .col_expr(
                    pioneer_entity::task::Column::ErrorJson,
                    Expr::value(None::<String>),
                )
                .col_expr(
                    pioneer_entity::task::Column::Revision,
                    Expr::cust("revision + 1"),
                )
                .col_expr(pioneer_entity::task::Column::UpdatedAt, Expr::value(now))
                .exec(&tx)
                .await?;
            anyhow::ensure!(
                task_update.rows_affected == 1,
                "task `{}` changed while resume was committing",
                run.task_id
            );

            let resume_events = append_task_events_in_connection_with_delivery_policy(&tx, prepared_events, now_unix, false, &self.outputs)
                .await?;
            let observation_sequence = resume_events.iter().filter(|event| event.append_status.is_inserted()).map(|event| event.sequence).max().unwrap_or(0);
            if next_revision_turn.is_none() {
                anyhow::ensure!(
                    turn_execution::reacquire_blocked(
                        &tx,
                        turn_id,
                        execution_owner_id,
                        now,
                        unix_to_datetime(execution_lease_until_unix),
                    )
                    .await?,
                    "task child Turn execution ownership changed while resume was committing"
                );
            }

            // Direct resume writes bypass ordinary repository transitions.
            let new_task = task_repository::find_task_by_id(&tx, &run.task_id).await?;
            task_child_producers::task_change(
                &tx,
                Some(&task_model),
                new_task.as_ref(),
            )
            .await?;
            let new_run = task_run::find_run_by_id(&tx, &run.id).await?;
            task_occurrence_producers::run_change(
                &tx,
                Some(&run_model),
                new_run.as_ref(),
            )
            .await?;
            let occurrence_new_execution =
                task_run_execution::find_execution_by_run(&tx, &run.id).await?;
            task_occurrence_producers::execution_change(
                &tx,
                execution_model.as_ref(),
                occurrence_new_execution.as_ref(),
            )
            .await?;
            task_child_producers::run_change(&tx, Some(&run_model), new_run.as_ref())
                .await?;
            let acquired_execution = if next_revision_turn.is_none() {
                let new_link = task_run_turn::find_turn_by_id(&tx, &task_run_turn.id).await?;
                task_child_producers::link_change(
                    &tx, Some(&task_run_turn_model), new_link.as_ref(),
                ).await?;
                turn_execution::find(&tx, turn_id).await?
            } else {
                commit_action(&tx).await?;
                None
            };
            tx.commit()
                .await
                .context("failed to commit task-owned turn resume transaction")?;
            if let (Some(change), Some(execution)) = (&ownership_change, acquired_execution.as_ref()) {
                change.register(execution);
            }

            // Decode and assemble the response after releasing the physical
            // writer. The durable transaction above contains only PostgreSQL
            // reads, optimistic fences, and writes.
            let recovery_job = recovery_job::find_job_by_id(&self.connection, job.id.as_str())
                .await?
                .map(recovery_job_record_from_model)
                .context("recovery job disappeared after task-owned resume")?;
            let task = task_repository::find_task_by_id(&self.connection, run.task_id.as_str())
                .await?
                .context("task disappeared after task-owned resume update")
                .and_then(task_from_db_model)?;
            let updated_run = task_run::find_run_by_id(&self.connection, run.id.as_str())
                .await?
                .context("task run disappeared after task-owned resume")
                .and_then(task_run_from_db_model)?;
            let updated_turn =
                task_run_turn::find_turn_by_id(&self.connection, task_run_turn.id.as_str())
                    .await?
                    .context("task run turn disappeared after task-owned resume")
                    .and_then(task_run_turn_from_db_model)?;
            let updated_execution =
                task_run_execution::find_execution_by_run(&self.connection, run.id.as_str())
                    .await?
                    .map(task_run_execution_from_db_model)
                    .transpose()?;

            Ok(Some(TaskOwnedTurnResumeOutcome::Resumed {
                observation_sequence,
                recovery_job,
                task,
                run: updated_run,
                task_run_turn: updated_turn,
                execution: updated_execution,
            }))
        })
        .await
            }
            .await;
            if let Ok(outcome) = &result {
                change.completed(matches!(outcome, Some(TaskOwnedTurnResumeOutcome::Resumed { .. })));
            }
            result
        }
        .await;
        if let Ok(value) = &control_result {
            let changed = matches!(value, Some(TaskOwnedTurnResumeOutcome::Resumed { .. }));
            task_change.completed(changed);
            control_change.completed(changed);
        }
        control_result
    }
}

async fn revision_has_newer_run(db: &impl sea_orm::ConnectionTrait, run: &TaskRun) -> Result<bool> {
    Ok(task_run::find_latest_numbered_run(db, &run.task_id)
        .await?
        .is_some_and(|latest| latest.run_number > run.run_number))
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    /// Permanent blockers only. An older active sibling, a live CLI session
    /// being drained and write-lock contention are resolved at admission.
    pub async fn task_revision_retry_blocker(
        &self,
        task: &Task,
        run: &TaskRun,
        revision: &TaskRunTurn,
    ) -> Result<Option<pioneer_protocol::TaskRevisionRetryBlocker>> {
        use pioneer_entity::turn_cli_runtime_binding as cli;
        use pioneer_protocol::TaskRevisionRetryBlocker as Blocker;
        if revision_has_newer_run(&self.connection, run).await? {
            return Ok(Some(Blocker::NewerRun));
        }
        if task.finish_request().is_some()
            || matches!(
                task.status,
                TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
            )
        {
            return Ok(Some(Blocker::TaskClosed));
        }
        let Some(binding) = cli::Entity::find_by_id(&revision.turn_id)
            .one(&self.connection)
            .await?
        else {
            return Ok(Some(Blocker::MissingBinding));
        };
        if binding.thread_id != revision.thread_id
            || binding.continuation_thread_id != revision.thread_id
            || binding.workspace_id != task.workspace_id
        {
            return Ok(Some(Blocker::BindingScopeChanged));
        }
        if binding.native_turn_id.is_some() || binding.mcp_session_generation.is_some() {
            return Ok(Some(Blocker::NativeExecutionRecorded));
        }
        let already_blocked = revision.status == TaskRunTurnStatus::Blocked;
        if !turn_execution::find(&self.connection, &revision.turn_id)
            .await?
            .is_some_and(|owner| {
                owner.thread_id == revision.thread_id
                    && owner.workspace_id == task.workspace_id
                    && owner.executor_kind == turn_execution::TurnExecutorKind::CliRuntime
                    && (!already_blocked
                        || owner.status == turn_execution::TurnExecutionStatus::Blocked)
            })
            || !task_run_execution::find_execution_by_run(&self.connection, &run.id)
                .await?
                .is_some_and(|execution| {
                    execution.executor_kind == "agent"
                        && revision.execution_id.as_deref() == Some(execution.id.as_str())
                        && (!already_blocked || execution.status == "blocked")
                })
        {
            return Ok(Some(Blocker::MissingExecutionOwner));
        }
        if recovery_job::find_blocked_job_by_turn(&self.connection, &revision.turn_id, None)
            .await?
            .is_none()
        {
            return Ok(Some(Blocker::MissingRecoveryJob));
        }
        if task_execution_admission::find_by_task(&self.connection, &task.id)
            .await?
            .is_none()
        {
            return Ok(Some(Blocker::MissingTaskAdmission));
        }
        Ok(None)
    }
}

fn next_revision_execution(
    current: &TaskRunTurn,
    run: &TaskRun,
    automatic: bool,
    now: i64,
) -> Option<TaskRunTurn> {
    let number = current.revision_execution_retry(&current.turn_id)?;
    let pending = run
        .error
        .as_ref()
        .is_some_and(|error| error.code == TASK_REVISION_RETRY_PENDING);
    if (automatic
        && (!pending
            || number >= TASK_REVISION_AUTOMATIC_RETRIES
            || current.is_manual_revision_retry()))
        || (!automatic && pending)
    {
        return None;
    }
    let retry = number.checked_add(1)?;

    Some(TaskRunTurn {
        turn_id: current.revision_retry_turn_id(retry, automatic)?,
        status: TaskRunTurnStatus::InProgress,
        started_at: Some(now),
        completed_at: None,
        ..current.clone()
    })
}

#[cfg(test)]
#[path = "task_resume_tests.rs"]
mod tests;
