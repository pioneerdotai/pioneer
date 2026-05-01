use crate::TaskRuntimeResult;
use crate::event_bus::TaskEventBus;
use crate::executor::{TaskExecutionContext, TaskExecutionHandle, TaskExecutorRegistry};
use crate::projector::TaskProjector;
use crate::scheduler::TaskSchedulerHandle;
use pioneer_crud::CrudStore;
use pioneer_protocol::{TaskEventPayload, TaskRun, TaskRunStatus, TaskTriggerStatus};
use std::sync::Arc;
use tracing::warn;

const DORMANT_TRIGGER_RECOVERY_MESSAGE: &str =
    "active trigger has no next_fire_at; scheduler will keep it dormant";
const DISPATCHABLE_RUN_RECOVERY_MESSAGE: &str = "queued run is dispatchable after startup";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconciliationReport {
    pub active_triggers: usize,
    pub queued_runs: usize,
    pub starting_runs: usize,
    pub running_runs: usize,
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
        let recovery_message = match run.status {
            TaskRunStatus::Queued => DISPATCHABLE_RUN_RECOVERY_MESSAGE,
            TaskRunStatus::Starting | TaskRunStatus::Running => {
                "in-flight run is recoverable after startup"
            }
            _ => return Ok(false),
        };
        let mut emitted_recovery_event = false;
        if !self
            .recovery_event_exists(
                run.task_id.as_str(),
                Some(run.id.as_str()),
                recovery_message,
            )
            .await?
        {
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
            emitted_recovery_event = true;
        }

        let handle = TaskExecutionHandle::new(
            self.store.clone(),
            self.event_bus.clone(),
            run.task_id.clone(),
            run.id.clone(),
        );
        if let Err(error) = executor
            .recover_run(
                TaskExecutionContext {
                    workspace_id: task_response.task.workspace_id,
                    task_id: run.task_id.clone(),
                },
                run.clone(),
                handle,
            )
            .await
        {
            warn!(
                task_id = %run.task_id,
                run_id = %run.id,
                error = %format!("{error:#}"),
                "task executor run recovery failed"
            );
        }

        Ok(emitted_recovery_event)
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
