use crate::TaskRuntimeResult;
use crate::event_bus::TaskEventBus;
use crate::executor::{TaskExecutionContext, TaskExecutionHandle, TaskExecutorRegistry};
use crate::projector::TaskProjector;
use crate::scheduler::{TASK_EXECUTION_LEASE_SECONDS, TaskSchedulerHandle};
use crate::task_boundary::task_fresh_task;
use anyhow::Context;
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    TaskEventPayload, TaskOccurrenceStatus, TaskRun, TaskRunStatus, TaskTriggerStatus, generate_id,
};
use std::sync::Arc;
use tracing::warn;

const DORMANT_TRIGGER_RECOVERY_MESSAGE: &str =
    "active trigger has no next_fire_at; scheduler will keep it dormant";
const DISPATCHABLE_RUN_RECOVERY_MESSAGE: &str = "queued run is dispatchable after startup";
const ID_LEN: usize = 21;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconciliationReport {
    pub active_triggers: usize,
    pub queued_runs: usize,
    pub starting_runs: usize,
    pub running_runs: usize,
    pub waiting_review_runs: usize,
    pub recovered_events: usize,
}

pub struct TaskStartupReconciler {
    store: Arc<CrudStore>,
    projector: TaskProjector,
    event_bus: Arc<TaskEventBus>,
    executors: Arc<TaskExecutorRegistry>,
    scheduler: TaskSchedulerHandle,
}

impl TaskStartupReconciler {
    pub fn new(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        executors: Arc<TaskExecutorRegistry>,
        scheduler: TaskSchedulerHandle,
    ) -> Self {
        let store = Arc::new(store.with_maintenance_access());
        let projector = TaskProjector::new(store.clone());
        Self {
            store,
            projector,
            event_bus,
            executors,
            scheduler,
        }
    }

    pub async fn reconcile(&self, now: i64) -> TaskRuntimeResult<ReconciliationReport> {
        let active_triggers = self.store.list_active_task_triggers().await?;
        let queued_runs = self
            .store
            .list_task_runs_by_status(TaskRunStatus::Queued, 1024)
            .await?;
        let starting_runs = self
            .store
            .list_task_runs_by_status(TaskRunStatus::Starting, 1024)
            .await?;
        let running_runs = self
            .store
            .list_task_runs_by_status(TaskRunStatus::Running, 1024)
            .await?;
        let waiting_review_runs = self
            .store
            .list_task_runs_by_status(TaskRunStatus::WaitingReview, 1024)
            .await?;
        let mut recovered_events = 0usize;

        for trigger in &active_triggers {
            if trigger.status != TaskTriggerStatus::Active {
                continue;
            }
            if trigger.next_fire_at.is_none() {
                if self
                    .recovery_event_exists(
                        trigger.task_id.as_str(),
                        None,
                        DORMANT_TRIGGER_RECOVERY_MESSAGE,
                    )
                    .await?
                {
                    continue;
                }
                let appended = self
                    .projector
                    .append_event(
                        TaskEventPayload::TaskRecovered {
                            task_id: trigger.task_id.clone(),
                            run_id: None,
                            message: DORMANT_TRIGGER_RECOVERY_MESSAGE.to_owned(),
                            recovered_at: now,
                        },
                        now,
                    )
                    .await?;
                self.event_bus.publish(appended).await;
                recovered_events = recovered_events.saturating_add(1);
            }
        }

        for run in queued_runs
            .iter()
            .chain(starting_runs.iter())
            .chain(running_runs.iter())
            .chain(waiting_review_runs.iter())
        {
            if self.recover_run(run, now).await? {
                recovered_events = recovered_events.saturating_add(1);
            }
        }

        self.scheduler.wake();
        Ok(ReconciliationReport {
            active_triggers: active_triggers.len(),
            queued_runs: queued_runs.len(),
            starting_runs: starting_runs.len(),
            running_runs: running_runs.len(),
            waiting_review_runs: waiting_review_runs.len(),
            recovered_events,
        })
    }

    async fn recover_run(&self, run: &TaskRun, now: i64) -> TaskRuntimeResult<bool> {
        let Some(executor) = self.executors.get(run.executor_kind).await else {
            return Ok(false);
        };
        let Some(task_response) = self.store.get_task(run.task_id.as_str()).await? else {
            return Ok(false);
        };
        let mut occurrence = self
            .store
            .get_task_occurrence_contract_by_run(run.id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "recoverable Task run `{}` has no durable occurrence contract",
                    run.id
                )
            })?;
        occurrence.status = match run.status {
            TaskRunStatus::Queued => TaskOccurrenceStatus::Queued,
            TaskRunStatus::Starting | TaskRunStatus::Running => TaskOccurrenceStatus::Recovering,
            TaskRunStatus::WaitingReview => TaskOccurrenceStatus::WaitingReview,
            _ => occurrence.status.clone(),
        };
        self.store
            .upsert_task_occurrence_contract(&occurrence, now)
            .await?;
        let recovery_message = match run.status {
            TaskRunStatus::Queued => DISPATCHABLE_RUN_RECOVERY_MESSAGE,
            TaskRunStatus::Starting | TaskRunStatus::Running => {
                "in-flight run is recoverable after startup"
            }
            TaskRunStatus::WaitingReview => "waiting-review run is recoverable after startup",
            _ => return Ok(false),
        };
        if run.status == TaskRunStatus::Queued {
            return self
                .emit_recovery_event_if_missing(run, recovery_message, now)
                .await;
        }
        if run.status == TaskRunStatus::WaitingReview {
            let emitted_recovery_event = self
                .emit_recovery_event_if_missing(run, recovery_message, now)
                .await?;
            let handle = TaskExecutionHandle::new(
                self.store.clone(),
                self.event_bus.clone(),
                run.task_id.clone(),
                run.id.clone(),
            );
            if let Err(_error) = executor
                .recover_run(
                    TaskExecutionContext {
                        workspace_id: task_response.task.workspace_id,
                        task_id: run.task_id.clone(),
                        execution_id: None,
                        worker_id: format!("task-review-recovery-{}", generate_id(ID_LEN)),
                    },
                    run.clone(),
                    handle,
                )
                .await
            {
                warn!(
                    task_id = %run.task_id,
                    run_id = %run.id,
                    failure_class = "task_waiting_review_recovery_failed",
                    "task executor waiting-review recovery failed"
                );
            }
            return Ok(emitted_recovery_event);
        }

        let execution = self
            .store
            .reserve_execution_for_run(run.id.as_str(), run.executor_kind, now)
            .await?;
        let worker_id = format!("task-recovery-{}", generate_id(ID_LEN));
        let lease_until = now.saturating_add(TASK_EXECUTION_LEASE_SECONDS);
        let Some(execution) = self
            .store
            .claim_execution_at(execution.id.as_str(), worker_id.as_str(), now, lease_until)
            .await?
        else {
            // A valid execution lease proves that this run is currently
            // owned. Live reconciliation must not label or restart it as
            // recovered merely because its status is Running.
            return Ok(false);
        };
        let emitted_recovery_event = self
            .emit_recovery_event_if_missing(run, recovery_message, now)
            .await?;
        let handle = TaskExecutionHandle::new(
            self.store.clone(),
            self.event_bus.clone(),
            run.task_id.clone(),
            run.id.clone(),
        );
        let recovery_context = TaskExecutionContext {
            workspace_id: task_response.task.workspace_id,
            task_id: run.task_id.clone(),
            execution_id: Some(execution.id),
            worker_id,
        };
        let recovery_run = run.clone();
        // A recovered executor run is an independently owned durable unit of
        // work. Keep its (potentially deep) implementation out of the startup
        // reconciler's poll stack while still awaiting it in deterministic
        // order and cancelling it if reconciliation is cancelled.
        if let Err(error) = task_fresh_task(
            async move {
                executor
                    .recover_run(recovery_context, recovery_run, handle)
                    .await
            },
            "task executor recovery task did not finish",
        )
        .await
        {
            warn!(
                task_id = %run.task_id,
                run_id = %run.id,
                error = %error,
                failure_class = "task_execution_recovery_failed",
                "task executor run recovery failed"
            );
        }

        Ok(emitted_recovery_event)
    }

    async fn emit_recovery_event_if_missing(
        &self,
        run: &TaskRun,
        recovery_message: &str,
        now: i64,
    ) -> TaskRuntimeResult<bool> {
        if self
            .recovery_event_exists(
                run.task_id.as_str(),
                Some(run.id.as_str()),
                recovery_message,
            )
            .await?
        {
            return Ok(false);
        }
        let appended = self
            .projector
            .append_event(
                TaskEventPayload::TaskRecovered {
                    task_id: run.task_id.clone(),
                    run_id: Some(run.id.clone()),
                    message: recovery_message.to_owned(),
                    recovered_at: now,
                },
                now,
            )
            .await?;
        self.event_bus.publish(appended).await;
        Ok(true)
    }

    async fn recovery_event_exists(
        &self,
        task_id: &str,
        run_id: Option<&str>,
        message: &str,
    ) -> TaskRuntimeResult<bool> {
        let events = self.store.get_task_events(task_id, None).await?;
        Ok(events.events.iter().any(|event| {
            matches!(
                &event.payload,
                TaskEventPayload::TaskRecovered {
                    run_id: existing_run_id,
                    message: existing_message,
                    ..
                } if existing_run_id.as_deref() == run_id && existing_message == message
            )
        }))
    }
}
