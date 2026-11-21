//! Atomic scheduler admission, due-trigger validation, and run dispatch.
use crate::event_pipeline::{
    MAX_ATOMIC_TASK_EVENT_BATCH_SIZE, append_task_events_in_connection_with_delivery_policy,
    load_task_event_metadata, prepare_task_events_for_write,
};
use crate::event_records::{AppendedTaskEvent, DeferredAppendedTaskEvent, materialize_task_events};
use crate::event_store::TaskEventStore;
use crate::execution_store::reserve_execution_for_run_in_connection;
use crate::status::{task_run_status_from_db, task_status_from_db};
use crate::task as task_repository;
use crate::task_projector::{TaskOutputBinder, TaskProjector};
use crate::task_run::task_run_from_db_model;
use crate::task_trigger::task_trigger_from_db_model;
use crate::values::{optional_typed_json_from_db, unix_to_datetime};
use crate::work_change::{TaskWorkChange, inserted_task_events};
use crate::{task_actor_contract, task_event, task_run, task_trigger};
use anyhow::{Context, Result};
use pioneer_protocol::{
    TaskError, TaskEventPayload, TaskExecutorKind, TaskRescheduleReason, TaskRun, TaskRunStatus,
    TaskStatus, TaskTrigger, TaskTriggerStatus,
};
use sea_orm::{ConnectionTrait, TransactionTrait};

#[derive(Debug, Clone)]
pub struct DueTaskTriggerReconciliation {
    pub task_id: String,
    pub trigger_id: String,
    /// `None` identifies an orphan trigger whose parent Task row is absent.
    pub task_status: Option<TaskStatus>,
    pub trigger_status: TaskTriggerStatus,
    pub appended_events: Vec<AppendedTaskEvent>,
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    async fn append_in_scheduler_transaction<C: ConnectionTrait + Sync>(
        &self,
        db: &C,
        events: Vec<task_event::PreparedTaskEvent>,
        at: i64,
    ) -> Result<Vec<DeferredAppendedTaskEvent>> {
        append_task_events_in_connection_with_delivery_policy(db, events, at, false, &self.outputs)
            .await
    }

    pub async fn append_due_trigger_task_events(
        &self,
        trigger: &TaskTrigger,
        now: i64,
        events: Vec<TaskEventPayload>,
        occurrence_contracts: Vec<pioneer_protocol::TaskOccurrenceContract>,
        reserve_executions: Vec<(String, TaskExecutorKind)>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = TaskWorkChange::for_tasks(
            &self.connection,
            events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            if trigger.status != TaskTriggerStatus::Active {
                return Ok(Vec::new());
            }
            let Some(expected_next_fire_at) = trigger.next_fire_at else {
                return Ok(Vec::new());
            };
            // Schedule calculation and observation happen outside the writer.
            // Revalidate their complete input, including same-time reschedules,
            // in the existing atomic trigger + occurrence + Run transaction.
            let expected_snapshot = (trigger.clone(), serde_json::to_value(&trigger.spec)?);
            self.run_serialized_write(|| {
                self.append_due_trigger_task_events_once(
                    trigger.id.clone(),
                    expected_next_fire_at,
                    expected_snapshot.clone(),
                    now,
                    events.clone(),
                    occurrence_contracts.clone(),
                    reserve_executions.clone(),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(inserted_task_events(value));
        }
        result
    }

    pub async fn block_task_for_due_trigger_failure(
        &self,
        trigger: &TaskTrigger,
        error: TaskError,
        now: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = TaskWorkChange::for_tasks(&self.connection, [trigger.task_id.clone()]);
        let result = async {
            let Some(expected_next_fire_at) = trigger.next_fire_at else {
                return Ok(Vec::new());
            };
            let expected_snapshot = (trigger.clone(), serde_json::to_value(&trigger.spec)?);
            let events = vec![TaskEventPayload::TaskBlocked {
                task_id: trigger.task_id.clone(),
                error: Some(error),
                blocked_at: now,
            }];
            self.run_serialized_write(|| {
                self.append_due_trigger_task_events_once(
                    trigger.id.clone(),
                    expected_next_fire_at,
                    expected_snapshot.clone(),
                    now,
                    events.clone(),
                    Vec::new(),
                    Vec::new(),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(inserted_task_events(value));
        }
        result
    }

    pub async fn reconcile_due_trigger_for_nonexecuting_task(
        &self,
        trigger_id: &str,
        expected_next_fire_at: i64,
        now: i64,
    ) -> Result<Option<DueTaskTriggerReconciliation>> {
        let change = TaskWorkChange::for_source(&self.connection, "g", trigger_id);
        let result = self
            .run_serialized_write(|| {
                self.reconcile_due_trigger_for_nonexecuting_task_once(
                    trigger_id.to_owned(),
                    expected_next_fire_at,
                    now,
                )
            })
            .await;
        if let Ok(value) = &result {
            change.completed(value.is_some());
        }
        result
    }

    pub async fn claim_task_run_for_dispatch_with_event(
        &self,
        run_id: &str,
        claimed_at: i64,
    ) -> Result<Option<(TaskRun, AppendedTaskEvent)>> {
        let change = TaskWorkChange::for_source(&self.connection, "r", run_id);
        let result = async {
            self.run_serialized_write(|| {
                self.claim_task_run_for_dispatch_once(run_id.to_owned(), claimed_at, None)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(
                value
                    .as_ref()
                    .is_some_and(|(_, event)| event.append_status.is_inserted()),
            );
        }
        result
    }

    pub async fn claim_task_retry_for_dispatch_with_event(
        &self,
        run_id: &str,
        claimed_at: i64,
        occurrence: pioneer_protocol::TaskOccurrenceContract,
    ) -> Result<Option<(TaskRun, AppendedTaskEvent)>> {
        let change = TaskWorkChange::for_source(&self.connection, "r", run_id);
        let result = async {
            anyhow::ensure!(
                occurrence.run_id == run_id,
                "retry occurrence belongs to another run"
            );
            self.run_serialized_write(|| {
                self.claim_task_run_for_dispatch_once(
                    run_id.to_owned(),
                    claimed_at,
                    Some(occurrence.clone()),
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(
                value
                    .as_ref()
                    .is_some_and(|(_, event)| event.append_status.is_inserted()),
            );
        }
        result
    }

    async fn claim_task_run_for_dispatch_once(
        &self,
        run_id: String,
        claimed_at: i64,
        retry_occurrence: Option<pioneer_protocol::TaskOccurrenceContract>,
    ) -> Result<Option<(TaskRun, AppendedTaskEvent)>> {
        // Prepare serialization before reserving the writer. Task identity is
        // revalidated inside the atomic claim + durable fanout write set.
        let Some(candidate) = task_run::find_run_by_id(&self.connection, &run_id).await? else {
            return Ok(None);
        };
        // The optional body can be large. Decode on the reader path, then
        // compare its immutable serialized representation during the claim.
        let trigger_input = optional_typed_json_from_db(candidate.trigger_input_json.clone())?;
        let prepared = prepare_task_events_for_write(
            &self.connection,
            vec![TaskEventPayload::RunStarting {
                task_id: candidate.task_id.clone(),
                run_id: run_id.clone(),
                claimed_at,
            }],
        )
        .await?;
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin Task dispatch claim transaction")?;
        let result = async {
            let Some(current) = task_run::find_run_by_id(&transaction, run_id.as_str()).await?
            else {
                return Ok(None);
            };
            anyhow::ensure!(
                current.task_id == candidate.task_id,
                "dispatch claim task identity changed"
            );
            let Some(task) =
                crate::dispatch::lock_task_for_dispatch(&transaction, &current.task_id).await?
            else {
                return Ok(None);
            };
            if task_repository::finish_requested(&transaction, &current.task_id).await? {
                return Ok(None);
            }
            if task_status_from_db(&task.status).is_none_or(|status| status.is_terminal()) {
                return Ok(None);
            }
            let max_parallel_runs = crate::values::optional_json_value_from_db::<
                pioneer_protocol::TaskConcurrencyPolicy,
            >(task.concurrency_policy_json)?
            .map(|policy| policy.max_parallel_runs)
            .unwrap_or(1);
            if !crate::dispatch::has_dispatch_capacity(
                &transaction,
                &current,
                max_parallel_runs,
                unix_to_datetime(claimed_at),
            )
            .await?
            {
                return Ok(None);
            }
            let claimed = task_run::claim_run_for_dispatch(
                &transaction,
                run_id.as_str(),
                unix_to_datetime(claimed_at),
            )
            .await?
            .map(|mut model| {
                anyhow::ensure!(
                    model.trigger_input_json == candidate.trigger_input_json,
                    "queued run input changed during claim"
                );
                model.trigger_input_json = None;
                task_run_from_db_model(model)
            })
            .transpose()?;
            let Some(run) = claimed else { return Ok(None) };
            if let Some(occurrence) = retry_occurrence {
                anyhow::ensure!(
                    occurrence.task_id == current.task_id && current.retry_of_run_id.is_some(),
                    "invalid retry admission"
                );
                task_actor_contract::upsert_task_occurrence_contract(
                    &transaction,
                    &occurrence,
                    claimed_at,
                )
                .await?;
            }
            let event = self
                .append_in_scheduler_transaction(&transaction, prepared, claimed_at)
                .await?
                .pop()
                .context("dispatch claim event missing")?;
            Ok(Some((run, event)))
        }
        .await;
        match result {
            Ok(mut run) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit Task dispatch claim transaction")?;
                if let Some((run, _)) = run.as_mut() {
                    run.trigger_input = trigger_input;
                }
                Ok(run.map(|(run, event)| (run, event.into_owned())))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn append_task_run_started_once(
        &self,
        task_id: String,
        run_id: String,
        started_at: i64,
    ) -> Result<Option<AppendedTaskEvent>> {
        let change = TaskWorkChange::for_tasks(&self.connection, [task_id.clone()]);
        let result = async {
            self.run_serialized_write(|| {
                self.append_task_run_started_once_inner(task_id.clone(), run_id.clone(), started_at)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(
                value
                    .as_ref()
                    .is_some_and(|event| event.append_status.is_inserted()),
            );
        }
        result
    }

    async fn append_task_run_started_once_inner(
        &self,
        task_id: String,
        run_id: String,
        started_at: i64,
    ) -> Result<Option<AppendedTaskEvent>> {
        let created_at = unix_to_datetime(started_at);
        let payload = TaskEventPayload::RunStarted {
            task_id,
            run_id: run_id.clone(),
            started_at,
        };
        let prepared_event = task_event::PreparedTaskEvent::prepare(payload)?
            .preflight_idempotency(&self.connection)
            .await?;
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin task run started transaction")?;

        let Some(run_model) = task_run::find_run_by_id(&transaction, run_id.as_str()).await? else {
            transaction
                .rollback()
                .await
                .context("failed to rollback missing task run started transaction")?;
            return Ok(None);
        };
        let Some(status) = task_run_status_from_db(run_model.status.as_str()) else {
            transaction
                .rollback()
                .await
                .context("failed to rollback invalid task run started transaction")?;
            anyhow::bail!(
                "task run `{}` has unknown status `{}`",
                run_id,
                run_model.status
            );
        };
        if matches!(status, TaskRunStatus::Running) || status.is_terminal() {
            transaction
                .rollback()
                .await
                .context("failed to rollback duplicate task run started transaction")?;
            return Ok(None);
        }
        if !matches!(status, TaskRunStatus::Queued | TaskRunStatus::Starting) {
            transaction
                .rollback()
                .await
                .context("failed to rollback non-startable task run started transaction")?;
            return Ok(None);
        }

        if status == TaskRunStatus::Queued
            && task_repository::finish_requested(&transaction, &run_model.task_id).await?
        {
            transaction.rollback().await?;
            return Ok(None);
        }

        let mut appended_event =
            match task_event::append_prepared_event(&transaction, prepared_event, created_at).await
            {
                Ok(event) => event,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };

        if appended_event.append_status.is_inserted() {
            if let Err(error) = TaskProjector::new()
                .project(&transaction, &appended_event, &self.outputs)
                .await
                .context("failed to project task run started event")
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        }

        (
            appended_event.workspace_id,
            appended_event.root_task_id,
            appended_event.parent_task_id,
        ) = match load_task_event_metadata(&transaction, &appended_event.task_id)
            .await
            .context("failed to hydrate task run started event metadata")
        {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        transaction
            .commit()
            .await
            .context("failed to commit task run started transaction")?;

        Ok(Some(appended_event.into_owned()))
    }

    async fn reconcile_due_trigger_for_nonexecuting_task_once(
        &self,
        trigger_id: String,
        expected_next_fire_at: i64,
        now: i64,
    ) -> Result<Option<DueTaskTriggerReconciliation>> {
        let Some(prepared_trigger_model) =
            task_trigger::find_trigger_by_id(&self.connection, trigger_id.as_str()).await?
        else {
            return Ok(None);
        };
        if prepared_trigger_model.status != "active"
            || prepared_trigger_model
                .next_fire_at
                .map(|value| value.timestamp())
                != Some(expected_next_fire_at)
            || expected_next_fire_at > now
        {
            return Ok(None);
        }
        let prepared_task_model = task_repository::find_task_by_id(
            &self.connection,
            prepared_trigger_model.task_id.as_str(),
        )
        .await?;
        let task_status = prepared_task_model
            .as_ref()
            .map(|task_model| {
                task_status_from_db(task_model.status.as_str()).with_context(|| {
                    format!(
                        "Task `{}` has unknown status `{}` during trigger reconciliation",
                        task_model.id, task_model.status
                    )
                })
            })
            .transpose()?;
        if task_status.is_some_and(|status| !status.is_terminal()) {
            return Ok(None);
        }
        let trigger_status = match task_status {
            Some(TaskStatus::Blocked) => TaskTriggerStatus::Paused,
            Some(TaskStatus::Cancelled) | None => TaskTriggerStatus::Cancelled,
            Some(TaskStatus::Completed | TaskStatus::Failed) => TaskTriggerStatus::Exhausted,
            Some(_) => return Ok(None),
        };
        let task_id = prepared_trigger_model.task_id.clone();
        let mut trigger = task_trigger_from_db_model(prepared_trigger_model.clone())?;
        trigger.status = trigger_status;
        trigger.next_fire_at = None;
        trigger.updated_at = now;
        let prepared_events = if task_status.is_some() {
            prepare_task_events_for_write(
                &self.connection,
                vec![TaskEventPayload::TaskRescheduled {
                    task_id: task_id.clone(),
                    trigger: trigger.clone(),
                    rescheduled_at: now,
                    reason: TaskRescheduleReason::RunTerminalStatusRefresh,
                }],
            )
            .await?
        } else {
            Vec::new()
        };

        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin due Task trigger reconciliation transaction")?;
        let result = async {
            let Some(trigger_model) =
                task_trigger::find_trigger_by_id(&transaction, trigger_id.as_str()).await?
            else {
                return Ok(None);
            };
            if trigger_model.status != "active"
                || trigger_model.next_fire_at.map(|value| value.timestamp())
                    != Some(expected_next_fire_at)
                || trigger_model.updated_at != prepared_trigger_model.updated_at
                || expected_next_fire_at > now
            {
                return Ok(None);
            }

            let task_model =
                task_repository::find_task_by_id(&transaction, trigger_model.task_id.as_str())
                    .await?;
            if task_model
                .as_ref()
                .map(|task| (&task.status, task.updated_at))
                != prepared_task_model
                    .as_ref()
                    .map(|task| (&task.status, task.updated_at))
            {
                anyhow::bail!("Task changed during trigger reconciliation preparation");
            }

            let appended_events = if task_status.is_some() {
                self.append_in_scheduler_transaction(&transaction, prepared_events, now)
                    .await?
            } else {
                task_trigger::update_trigger_schedule(
                    &transaction,
                    trigger.id.as_str(),
                    trigger.status,
                    None,
                    trigger.last_fire_at.map(unix_to_datetime),
                    unix_to_datetime(now),
                )
                .await?;
                Vec::new()
            };

            Ok(Some((
                task_id,
                trigger.id,
                task_status,
                trigger_status,
                appended_events,
            )))
        }
        .await;

        match result {
            Ok(outcome) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit due Task trigger reconciliation transaction")?;
                Ok(outcome.map(
                    |(task_id, trigger_id, task_status, trigger_status, events)| {
                        DueTaskTriggerReconciliation {
                            task_id,
                            trigger_id,
                            task_status,
                            trigger_status,
                            appended_events: materialize_task_events(events),
                        }
                    },
                ))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn append_due_trigger_task_events_once(
        &self,
        trigger_id: String,
        expected_next_fire_at: i64,
        expected_snapshot: (TaskTrigger, serde_json::Value),
        now: i64,
        events: Vec<TaskEventPayload>,
        occurrence_contracts: Vec<pioneer_protocol::TaskOccurrenceContract>,
        reserve_executions: Vec<(String, TaskExecutorKind)>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        // A manual/webhook admission can win after scheduler preparation.
        // Revalidate allocation facts inside the same writer as consumption;
        // a stale plan leaves the trigger due for the next scheduler pass.
        let expected_run_number = events.iter().find_map(|event| match event {
            TaskEventPayload::RunCreated { run, .. } => Some(run.run_number),
            _ => None,
        });
        let expected_generation = occurrence_contracts
            .first()
            .map(|contract| contract.execution_generation);
        let events = prepare_task_events_for_write(&self.connection, events).await?;
        if occurrence_contracts.len() > MAX_ATOMIC_TASK_EVENT_BATCH_SIZE
            || reserve_executions.len() > MAX_ATOMIC_TASK_EVENT_BATCH_SIZE
        {
            anyhow::bail!(
                "due trigger write-set exceeds {MAX_ATOMIC_TASK_EVENT_BATCH_SIZE} records"
            );
        }
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin due task trigger materialization transaction")?;

        let (expected_trigger, expected_spec) = expected_snapshot;
        let result = async {
            crate::event_log::lock_task(&transaction, &expected_trigger.task_id).await?;
            let Some(trigger) =
                task_trigger::find_trigger_by_id(&transaction, trigger_id.as_str()).await?
            else {
                return Ok(Vec::new());
            };
            if trigger.status != "active"
                || trigger.next_fire_at.map(|value| value.timestamp())
                    != Some(expected_next_fire_at)
                || expected_next_fire_at > now
                || trigger.task_id != expected_trigger.task_id
                || trigger.spec_json != expected_spec
                || trigger.last_fire_at.map(|value| value.timestamp())
                    != expected_trigger.last_fire_at
                || trigger.created_at.timestamp() != expected_trigger.created_at
                || trigger.updated_at.timestamp() != expected_trigger.updated_at
            {
                return Ok(Vec::new());
            }

            if let Some(expected) = expected_run_number
                && task_run::next_run_number(&transaction, &trigger.task_id).await? != expected
            {
                return Ok(Vec::new());
            }
            if let Some(expected) = expected_generation
                && task_actor_contract::next_task_occurrence_execution_generation(
                    &transaction,
                    &trigger.task_id,
                )
                .await?
                    != expected
            {
                return Ok(Vec::new());
            }
            if task_repository::finish_requested(&transaction, &trigger.task_id).await? {
                return Ok(Vec::new());
            }
            let appended_events = self
                .append_in_scheduler_transaction(&transaction, events, now)
                .await?;
            for occurrence in occurrence_contracts {
                task_actor_contract::upsert_task_occurrence_contract(
                    &transaction,
                    &occurrence,
                    now,
                )
                .await?;
            }
            for (run_id, executor_kind) in reserve_executions {
                let _ = reserve_execution_for_run_in_connection(
                    &transaction,
                    run_id,
                    executor_kind,
                    now,
                )
                .await?;
            }
            Ok(appended_events)
        }
        .await;

        match result {
            Ok(appended_events) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit due task trigger materialization transaction")?;
                Ok(materialize_task_events(appended_events))
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}
