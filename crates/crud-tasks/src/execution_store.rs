//! Scoped Task-run execution claims, occurrence transitions and lease maintenance.
use crate::status::{
    task_executor_kind_from_db, task_executor_kind_to_db, task_run_execution_status_from_db,
    task_run_status_to_db, task_status_from_db,
};
use crate::task_run::task_run_from_db_model;
use crate::task_run_execution::TaskRunExecutionObservationRecord;
use crate::values::{optional_typed_json_from_db, unix_to_datetime};
use crate::work_change::TaskWorkChange;
use crate::{task_actor_contract, task_run, task_run_execution};
use anyhow::{Context, Result, bail};
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_execution::execution_admission_lease;
use pioneer_crud_execution::gateway_control::{GatewayControlWake, control_work_change};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    retry_with_backoff,
};
use pioneer_protocol::TurnStatus;
use pioneer_protocol::{
    TaskError, TaskExecutorKind, TaskResult, TaskRunExecution, TaskRunExecutionStatus,
    TaskRunStatus, generate_id,
};
use sea_orm::{ConnectionTrait, EntityTrait, QuerySelect, TransactionTrait};
use std::{future::Future, time::Duration};

pub struct TaskExecutionStore<'a> {
    connection: &'a PostgresDatabase,
}

impl<'a> TaskExecutionStore<'a> {
    pub fn new(connection: &'a PostgresDatabase) -> Self {
        Self { connection }
    }

    async fn retry_write_on_conflict<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            crate::retry::is_retryable,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn claim_task_run_execution_for_dispatch(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.claim_task_run_execution_with_occurrence(
            run_id,
            executor_kind,
            worker_id,
            now,
            lease_until,
            TaskExecutionOccurrenceClaimMode::Dispatch,
        )
        .await
    }

    pub async fn claim_task_run_execution_for_recovery(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.claim_task_run_execution_with_occurrence(
            run_id,
            executor_kind,
            worker_id,
            now,
            lease_until,
            TaskExecutionOccurrenceClaimMode::Recovery,
        )
        .await
    }

    async fn claim_task_run_execution_with_occurrence(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        worker_id: &str,
        now: i64,
        lease_until: i64,
        mode: TaskExecutionOccurrenceClaimMode,
    ) -> Result<Option<TaskRunExecution>> {
        let change = TaskWorkChange::for_source(self.connection, "r", run_id);
        let result = async {
            let run_id = run_id.to_owned();
            let worker_id = worker_id.to_owned();
            self.retry_write_on_conflict(|| {
                self.claim_task_run_execution_with_occurrence_once(
                    run_id.clone(),
                    executor_kind,
                    worker_id.clone(),
                    now,
                    lease_until,
                    mode,
                )
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(value.is_some());
        }
        result
    }

    async fn claim_task_run_execution_with_occurrence_once(
        &self,
        run_id: String,
        executor_kind: TaskExecutorKind,
        worker_id: String,
        now: i64,
        lease_until: i64,
        mode: TaskExecutionOccurrenceClaimMode,
    ) -> Result<Option<TaskRunExecution>> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin Task execution/occurrence claim transaction")?;
        let result = async {
            let Some(run) = task_run::find_run_by_id(&transaction, run_id.as_str()).await? else {
                return Ok(None);
            };
            if !mode.accepts_run_status(run.status.as_str())
                || run.executor_kind != task_executor_kind_to_db(executor_kind)
            {
                return Ok(None);
            }
            let Some(mut occurrence) =
                task_actor_contract::find_task_occurrence_by_run_id(&transaction, run_id.as_str())
                    .await?
            else {
                return Ok(None);
            };
            if occurrence.task_id != run.task_id
                || occurrence.run_id != run.id
                || task_actor_contract::is_terminal_task_occurrence_status(&occurrence.status)
            {
                return Ok(None);
            }

            let execution = reserve_execution_for_run_in_connection(
                &transaction,
                run_id.clone(),
                executor_kind,
                now,
            )
            .await?;
            if occurrence
                .agent_execution_id
                .as_deref()
                .is_some_and(|execution_id| execution_id != execution.id)
            {
                return Ok(None);
            }
            let Some(execution) = task_run_execution::claim_execution(
                &transaction,
                execution.id.as_str(),
                worker_id.as_str(),
                unix_to_datetime(now),
                unix_to_datetime(lease_until),
            )
            .await?
            else {
                return Ok(None);
            };
            occurrence.status = mode.occurrence_status();
            task_actor_contract::upsert_task_occurrence_contract(&transaction, &occurrence, now)
                .await?;
            task_run_execution_from_db_model(execution).map(Some)
        }
        .await;
        match result {
            Ok(Some(execution)) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit Task execution/occurrence claim transaction")?;
                Ok(Some(execution))
            }
            Ok(None) => {
                transaction
                    .rollback()
                    .await
                    .context("failed to roll back rejected Task execution/occurrence claim")?;
                Ok(None)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn revalidate_and_sync_active_task_occurrence(
        &self,
        run_id: &str,
        expected_run_status: TaskRunStatus,
        now: i64,
    ) -> Result<bool> {
        let expected_run_status_db = task_run_status_to_db(expected_run_status).to_owned();
        let desired_occurrence_status = match expected_run_status {
            TaskRunStatus::Queued => pioneer_protocol::TaskOccurrenceStatus::Queued,
            TaskRunStatus::WaitingReview => pioneer_protocol::TaskOccurrenceStatus::WaitingReview,
            _ => bail!("active Task occurrence sync requires queued or waiting-review status"),
        };
        let run_id = run_id.to_owned();
        self.retry_write_on_conflict(|| {
            let run_id = run_id.clone();
            let expected_run_status_db = expected_run_status_db.clone();
            let desired_occurrence_status = desired_occurrence_status.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin active Task occurrence sync transaction")?;
                let result = async {
                    let Some(run) = task_run::find_run_by_id(&transaction, run_id.as_str()).await?
                    else {
                        return Ok(false);
                    };
                    if run.status != expected_run_status_db {
                        return Ok(false);
                    }
                    let Some(mut occurrence) = task_actor_contract::find_task_occurrence_by_run_id(
                        &transaction,
                        run_id.as_str(),
                    )
                    .await?
                    else {
                        return Ok(false);
                    };
                    if occurrence.task_id != run.task_id
                        || occurrence.run_id != run.id
                        || task_actor_contract::is_terminal_task_occurrence_status(
                            &occurrence.status,
                        )
                    {
                        return Ok(false);
                    }
                    if occurrence.status != desired_occurrence_status {
                        occurrence.status = desired_occurrence_status.clone();
                        task_actor_contract::upsert_task_occurrence_contract(
                            &transaction,
                            &occurrence,
                            now,
                        )
                        .await?;
                    }
                    Ok(true)
                }
                .await;
                match result {
                    Ok(revalidated) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit active Task occurrence sync transaction")?;
                        Ok(revalidated)
                    }
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            }
        })
        .await
    }

    pub async fn reserve_execution_for_run(
        &self,
        run_id: &str,
        executor_kind: TaskExecutorKind,
        now: i64,
    ) -> Result<TaskRunExecution> {
        self.retry_write_on_conflict(|| {
            self.reserve_execution_for_run_once(run_id.to_owned(), executor_kind, now)
        })
        .await
    }

    async fn reserve_execution_for_run_once(
        &self,
        run_id: String,
        executor_kind: TaskExecutorKind,
        now: i64,
    ) -> Result<TaskRunExecution> {
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin task run execution reservation transaction")?;

        let result =
            reserve_execution_for_run_in_connection(&transaction, run_id, executor_kind, now).await;

        match result {
            Ok(execution) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit task run execution reservation transaction")?;
                Ok(execution)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn list_task_run_execution_observations(
        &self,
        run_ids: &[String],
    ) -> Result<Vec<TaskRunExecutionObservationRecord>> {
        let mut observations = Vec::new();
        for chunk in run_ids.chunks(128) {
            observations.extend(
                task_run_execution::list_execution_observations_for_runs(self.connection, chunk)
                    .await?,
            );
        }
        Ok(observations)
    }

    pub async fn load_execution_for_run(&self, run_id: &str) -> Result<Option<TaskRunExecution>> {
        task_run_execution::find_execution_by_run(self.connection, run_id)
            .await?
            .map(task_run_execution_from_db_model)
            .transpose()
    }

    pub async fn claim_execution(
        &self,
        execution_id: &str,
        worker_id: &str,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.claim_execution_at(execution_id, worker_id, lease_until, lease_until)
            .await
    }

    pub async fn claim_execution_at(
        &self,
        execution_id: &str,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        let change = TaskWorkChange::for_source(self.connection, "e", execution_id);
        let before = task_run_execution::find_execution_by_id(self.connection, execution_id)
            .await?
            .map(task_run_execution_from_db_model)
            .transpose()?;
        let result = async {
            self.retry_write_on_conflict(|| async {
                let tx = self.connection.begin().await?;
                let outcome = task_run_execution::claim_execution(
                    &tx,
                    execution_id,
                    worker_id,
                    unix_to_datetime(now),
                    unix_to_datetime(lease_until),
                )
                .await?
                .map(task_run_execution_from_db_model)
                .transpose()?;
                tx.commit().await?;
                Ok(outcome)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(before.as_ref() != value.as_ref());
        }
        result
    }

    pub async fn mark_execution_running(
        &self,
        execution_id: &str,
        started_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let change = TaskWorkChange::for_source(self.connection, "e", execution_id);
            let before = task_run_execution::find_execution_by_id(self.connection, execution_id)
                .await?
                .map(task_run_execution_from_db_model)
                .transpose()?;
            let result = async {
                self.retry_write_on_conflict(|| async {
                    let tx = self.connection.begin().await?;
                    let outcome = task_run_execution::mark_execution_running(
                        &tx,
                        execution_id,
                        unix_to_datetime(started_at),
                        lease_until.map(unix_to_datetime),
                    )
                    .await?
                    .map(task_run_execution_from_db_model)
                    .transpose()?;
                    tx.commit().await?;
                    Ok(outcome)
                })
                .await
            }
            .await;
            if let Ok(value) = &result {
                change.completed(before.as_ref() != value.as_ref());
            }
            result
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(value.is_some());
        }
        control_result
    }

    pub async fn mark_execution_terminal(
        &self,
        execution_id: &str,
        status: TaskRunExecutionStatus,
        completed_at: i64,
        result: Option<&TaskResult>,
        error: Option<&TaskError>,
    ) -> Result<Option<TaskRunExecution>> {
        let control_wakes = vec![
            GatewayControlWake::Resources,
            GatewayControlWake::Actions,
            GatewayControlWake::Ledger,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let change = TaskWorkChange::for_source(self.connection, "e", execution_id);
            let before = task_run_execution::find_execution_by_id(self.connection, execution_id)
                .await?
                .map(task_run_execution_from_db_model)
                .transpose()?;
            let result = async {
                self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin terminal execution transition")?;
            let completed_at = unix_to_datetime(completed_at);
            let task_execution = task_run_execution::mark_execution_terminal(
                &transaction,
                execution_id,
                status,
                completed_at,
                result,
                error,
            )
            .await?;
            if let Some(task_execution) = task_execution.as_ref() {
                let mut occurrence =
                    task_actor_contract::find_task_occurrence_by_run_id(
                        &transaction,
                        task_execution.task_run_id.as_str(),
                    )
                    .await?
                    .with_context(|| {
                        format!(
                            "Task execution `{}` has no durable occurrence contract",
                            task_execution.id
                        )
                    })?;
                occurrence.status = match status {
                    TaskRunExecutionStatus::Succeeded => {
                        pioneer_protocol::TaskOccurrenceStatus::Delivered
                    }
                    TaskRunExecutionStatus::Cancelled => {
                        pioneer_protocol::TaskOccurrenceStatus::Cancelled
                    }
                    TaskRunExecutionStatus::Failed
                    | TaskRunExecutionStatus::Blocked
                    | TaskRunExecutionStatus::TimedOut => {
                        pioneer_protocol::TaskOccurrenceStatus::Failed
                    }
                    TaskRunExecutionStatus::Reserved
                    | TaskRunExecutionStatus::Starting
                    | TaskRunExecutionStatus::Running
                    | TaskRunExecutionStatus::WaitingReview => {
                        bail!("non-terminal task execution status cannot finalize an occurrence")
                    }
                };
                occurrence.terminal_reason = error.map(|error| error.message.clone());
                task_actor_contract::upsert_task_occurrence_contract(
                    &transaction,
                    &occurrence,
                    completed_at.timestamp(),
                )
                .await?;
                if task_execution.executor_kind == "agent" {
                    let agent_status = match status {
                        TaskRunExecutionStatus::Succeeded => "succeeded",
                        TaskRunExecutionStatus::Failed => "failed",
                        TaskRunExecutionStatus::Blocked => "blocked",
                        TaskRunExecutionStatus::Cancelled => "cancelled",
                        TaskRunExecutionStatus::TimedOut => "timed_out",
                        TaskRunExecutionStatus::Reserved
                        | TaskRunExecutionStatus::Starting
                        | TaskRunExecutionStatus::Running
                        | TaskRunExecutionStatus::WaitingReview => bail!(
                            "non-terminal task execution status cannot finalize an agent execution"
                        ),
                    };
                    crate::terminal::finalize_agent_execution(
                        &transaction,
                        execution_id,
                        agent_status,
                        completed_at,
                    )
                    .await?;
                }
            }
            transaction
                .commit()
                .await
                .context("failed to commit terminal execution transition")?;
            task_execution
                .map(task_run_execution_from_db_model)
                .transpose()
        })
        .await
            }
            .await;
            if let Ok(value) = &result {
                change.completed(before.as_ref() != value.as_ref());
            }
            result
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(value.is_some());
        }
        control_result
    }

    pub async fn heartbeat_execution(
        &self,
        execution_id: &str,
        heartbeat_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        self.heartbeat_execution_with_agent_attempt(execution_id, None, heartbeat_at, lease_until)
            .await
    }

    pub async fn heartbeat_execution_for_agent_attempt(
        &self,
        execution_id: &str,
        expected_attempt_generation: i64,
        heartbeat_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        self.heartbeat_execution_with_agent_attempt(
            execution_id,
            Some(expected_attempt_generation),
            heartbeat_at,
            lease_until,
        )
        .await
    }

    async fn heartbeat_execution_with_agent_attempt(
        &self,
        execution_id: &str,
        expected_attempt_generation: Option<i64>,
        heartbeat_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        let change = self.connection.work_signal("tasks.lifecycle").changing();
        let result = self
            .retry_write_on_conflict(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin execution heartbeat transition")?;
                if let Some(expected_attempt_generation) = expected_attempt_generation {
                    let current =
                        pioneer_crud_execution::agent_execution::heartbeat_agent_execution(
                            &transaction,
                            execution_id,
                            expected_attempt_generation,
                            unix_to_datetime(heartbeat_at),
                            lease_until.map(unix_to_datetime),
                        )
                        .await?;
                    if !current {
                        transaction
                            .rollback()
                            .await
                            .context("failed to roll back stale agent heartbeat")?;
                        return Ok((None, false));
                    }
                }
                // Compare under the same writer transaction as the heartbeat CAS.
                let before =
                    task_run_execution::find_execution_by_id(&transaction, execution_id).await?;
                let task_execution = task_run_execution::heartbeat_execution(
                    &transaction,
                    execution_id,
                    unix_to_datetime(heartbeat_at),
                    lease_until.map(unix_to_datetime),
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit execution heartbeat transition")?;
                let earlier = before
                    .as_ref()
                    .and_then(|row| row.lease_until)
                    .zip(task_execution.as_ref().and_then(|row| row.lease_until))
                    .is_some_and(|(old, new)| new < old);
                Ok((
                    task_execution
                        .map(task_run_execution_from_db_model)
                        .transpose()?,
                    earlier,
                ))
            })
            .await;
        if let Ok((_, earlier)) = &result {
            change.completed(*earlier);
        }
        result.map(|(execution, _)| execution)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskExecutionOccurrenceClaimMode {
    Dispatch,
    Recovery,
}

impl TaskExecutionOccurrenceClaimMode {
    fn accepts_run_status(self, status: &str) -> bool {
        match self {
            Self::Dispatch => matches!(status, "starting"),
            Self::Recovery => matches!(status, "starting" | "running"),
        }
    }

    const fn occurrence_status(self) -> pioneer_protocol::TaskOccurrenceStatus {
        match self {
            Self::Dispatch => pioneer_protocol::TaskOccurrenceStatus::Running,
            Self::Recovery => pioneer_protocol::TaskOccurrenceStatus::Recovering,
        }
    }
}

pub async fn reserve_execution_for_run_in_connection<C: ConnectionTrait>(
    db: &C,
    run_id: String,
    executor_kind: TaskExecutorKind,
    now: i64,
) -> Result<TaskRunExecution> {
    // A reserved execution has a unique run key. Lock the parent before
    // reading the reservation so first-time concurrent callers converge.
    pioneer_entity::task_run::Entity::find_by_id(&run_id)
        .select_only()
        .column(pioneer_entity::task_run::Column::Id)
        .lock(sea_orm::sea_query::LockType::NoKeyUpdate)
        .into_tuple::<String>()
        .one(db)
        .await?
        .with_context(|| format!("task run `{run_id}` not found for execution reservation"))?;
    if let Some(existing) = task_run_execution::find_execution_by_run(db, run_id.as_str()).await? {
        let execution = task_run_execution_from_db_model(existing)?;
        if execution.executor_kind != executor_kind {
            anyhow::bail!(
                "task run execution `{}` already exists for executor kind `{:?}`, requested `{:?}`",
                execution.id,
                execution.executor_kind,
                executor_kind
            );
        }
        return Ok(execution);
    }

    let Some(run_model) = task_run::find_run_by_id(db, run_id.as_str()).await? else {
        anyhow::bail!("task run `{run_id}` not found for execution reservation");
    };
    let run = task_run_from_db_model(run_model)?;
    if run.executor_kind != executor_kind {
        anyhow::bail!(
            "task run `{}` has executor kind `{:?}`, requested `{:?}`",
            run.id,
            run.executor_kind,
            executor_kind
        );
    }

    task_run_execution::insert_execution_if_absent(
        db,
        task_run_execution::NewTaskRunExecution {
            id: generate_id(21),
            task_id: run.task_id.clone(),
            task_run_id: run.id.clone(),
            executor_kind,
            status: TaskRunExecutionStatus::Reserved,
            worker_id: None,
            lease_until: None,
            heartbeat_at: None,
            started_at: None,
            completed_at: None,
            result: None,
            error: None,
            created_at: now,
            updated_at: now,
        },
    )
    .await?;

    let execution = task_run_execution::find_execution_by_run(db, run.id.as_str())
        .await?
        .context("task run execution missing after reservation")?;
    task_run_execution_from_db_model(execution)
}

pub fn task_run_execution_from_db_model(
    model: pioneer_entity::task_run_execution::Model,
) -> Result<TaskRunExecution> {
    let executor_kind =
        task_executor_kind_from_db(model.executor_kind.as_str()).with_context(|| {
            format!(
                "unknown task run execution executor kind `{}`",
                model.executor_kind
            )
        })?;
    let status = task_run_execution_status_from_db(model.status.as_str())
        .with_context(|| format!("unknown task run execution status `{}`", model.status))?;

    Ok(TaskRunExecution {
        id: model.id,
        task_id: model.task_id,
        task_run_id: model.task_run_id,
        executor_kind,
        status,
        worker_id: model.worker_id,
        lease_until: model.lease_until.map(|value| value.timestamp()),
        heartbeat_at: model.heartbeat_at.map(|value| value.timestamp()),
        started_at: model.started_at.map(|value| value.timestamp()),
        completed_at: model.completed_at.map(|value| value.timestamp()),
        result: optional_typed_json_from_db(model.result_json)?,
        error: optional_typed_json_from_db(model.error_json)?,
        created_at: model.created_at.timestamp(),
        updated_at: model.updated_at.timestamp(),
    })
}

#[cfg(test)]
#[path = "execution_store_tests.rs"]
mod tests;

impl TaskExecutionStore<'_> {
    pub async fn reconcile_execution_admission_leases(&self) -> Result<usize> {
        let leases = execution_admission_lease::list_active(self.connection).await?;
        let released_at = chrono::Utc::now().fixed_offset();
        let mut released = 0usize;
        for lease in leases {
            if lease.subject_kind == "task" {
                let tx = self.connection.begin().await?;
                // Task terminalization and readmission use this same row
                // lock. A stale observation cannot release a reacquired slot.
                let terminal_or_missing =
                    pioneer_entity::task::Entity::find_by_id(&lease.subject_id)
                        .lock_exclusive()
                        .one(&tx)
                        .await?
                        .is_none_or(|task| {
                            task_status_from_db(task.status.as_str())
                                .is_none_or(|status| status.is_terminal())
                        });
                let changed = terminal_or_missing
                    && execution_admission_lease::release_by_subject(
                        &tx,
                        "task",
                        &lease.subject_id,
                        released_at,
                    )
                    .await?;
                tx.commit().await?;
                released += usize::from(changed);
                continue;
            }
            let terminal_or_missing = match lease.subject_kind.as_str() {
                "turn" => pioneer_entity::turn::Entity::find_by_id(lease.subject_id.clone())
                    .one(self.connection)
                    .await?
                    .is_none_or(|turn| {
                        turn_status_from_db(turn.status.as_str())
                            .is_none_or(|status| status != TurnStatus::InProgress)
                    }),
                _ => true,
            };
            if terminal_or_missing
                && execution_admission_lease::release_by_subject(
                    self.connection,
                    lease.subject_kind.as_str(),
                    lease.subject_id.as_str(),
                    released_at,
                )
                .await?
            {
                released = released.saturating_add(1);
            }
        }
        Ok(released)
    }
}
