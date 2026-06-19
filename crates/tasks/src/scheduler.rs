use crate::TaskRuntimeResult;
use crate::event_bus::TaskEventBus;
use crate::executor::{
    TaskExecutionContext, TaskExecutionHandle, TaskExecutorRegistry, TaskExecutorStartOutcome,
};
use crate::projector::TaskProjector;
use crate::service::{is_terminal_task, now_timestamp_secs, task_error};
use crate::trigger::TaskTriggerCalculator;
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    TaskErrorClass, TaskEventPayload, TaskExecutorKind, TaskRescheduleReason, TaskRun,
    TaskRunStatus, TaskTrigger, TaskTriggerKind, TaskTriggerStatus, generate_id,
};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, is_anyhow_sqlite_transient_open,
    retry_with_backoff,
};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, sleep};
use tracing::{debug, error};

const ID_LEN: usize = 21;
pub const TASK_EXECUTION_LEASE_SECONDS: i64 = 300;
const TASK_SCHEDULER_MAX_SLEEP_SECONDS: u64 = 60;

#[derive(Clone)]
pub struct TaskSchedulerHandle {
    notify: Arc<Notify>,
}

impl TaskSchedulerHandle {
    pub fn wake(&self) {
        self.notify.notify_waiters();
    }
}

pub struct TaskScheduler {
    store: Arc<CrudStore>,
    projector: TaskProjector,
    event_bus: Arc<TaskEventBus>,
    executors: Arc<TaskExecutorRegistry>,
    notify: Arc<Notify>,
    due_queue: Mutex<Vec<TaskTriggerQueueItem>>,
    process_lock: Mutex<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskTriggerQueueItem {
    task_id: String,
    wake_id: String,
    next_fire_at: i64,
}

impl TaskScheduler {
    pub fn new(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        executors: Arc<TaskExecutorRegistry>,
    ) -> Self {
        let projector = TaskProjector::new(store.clone());
        Self {
            store,
            projector,
            event_bus,
            executors,
            notify: Arc::new(Notify::new()),
            due_queue: Mutex::new(Vec::new()),
            process_lock: Mutex::new(()),
        }
    }

    pub fn handle(&self) -> TaskSchedulerHandle {
        TaskSchedulerHandle {
            notify: self.notify.clone(),
        }
    }

    pub fn wake(&self) {
        self.notify.notify_waiters();
    }

    pub async fn run(self: Arc<Self>) {
        let mut events = self.event_bus.subscribe(Default::default());
        loop {
            let now = now_timestamp_secs();
            let processing = self
                .process_due_once_with_transient_storage_retry(now)
                .await;
            let sleep_duration = match processing {
                Ok(_) => self.next_sleep_duration(now).await,
                Err(error) => {
                    let is_transient_storage = is_anyhow_sqlite_transient_open(&error);
                    error!(error = %format!("{error:#}"), "task scheduler due processing failed");
                    if is_transient_storage {
                        Duration::from_secs(TASK_SCHEDULER_MAX_SLEEP_SECONDS)
                    } else {
                        self.next_sleep_duration(now).await
                    }
                }
            };
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = events.recv() => {}
                _ = sleep(sleep_duration) => {}
            }
        }
    }

    async fn process_due_once_with_transient_storage_retry(
        &self,
        now: i64,
    ) -> TaskRuntimeResult<usize> {
        retry_with_backoff(
            || self.process_due_once(now),
            is_anyhow_sqlite_transient_open,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn process_due_once(&self, now: i64) -> TaskRuntimeResult<usize> {
        let _guard = self.process_lock.lock().await;
        self.rebuild_due_queue().await?;
        let due = self.store.list_due_active_task_triggers(now).await?;
        let mut created = 0usize;
        for trigger in due {
            created = created.saturating_add(self.process_trigger(trigger, now).await?);
        }
        for run in self.store.list_due_retry_task_runs(now, 1024).await? {
            if self.process_retry_run(run, now).await? {
                created = created.saturating_add(1);
            }
        }
        for run in self
            .store
            .list_task_runs_by_status(TaskRunStatus::Queued, 1024)
            .await?
            .into_iter()
            .filter(|run| run.retry_of_run_id.is_none())
            .filter(|run| run.ready_at.is_none_or(|ready_at| ready_at <= now))
        {
            if self.process_queued_run(run).await? {
                created = created.saturating_add(1);
            }
        }
        self.rebuild_due_queue().await?;
        Ok(created)
    }

    async fn rebuild_due_queue(&self) -> TaskRuntimeResult<()> {
        let mut items = self
            .store
            .list_active_task_triggers()
            .await?
            .into_iter()
            .filter_map(|trigger| {
                let next_fire_at = trigger.next_fire_at?;
                Some(TaskTriggerQueueItem {
                    task_id: trigger.task_id,
                    wake_id: trigger.id,
                    next_fire_at,
                })
            })
            .collect::<Vec<_>>();
        items.extend(
            self.store
                .list_task_runs_by_status(TaskRunStatus::Queued, 1024)
                .await?
                .into_iter()
                .filter(|run| run.retry_of_run_id.is_some())
                .filter_map(|run| {
                    let ready_at = run.ready_at?;
                    Some(TaskTriggerQueueItem {
                        task_id: run.task_id,
                        wake_id: run.id,
                        next_fire_at: ready_at,
                    })
                }),
        );
        items.sort_by(|left, right| {
            left.next_fire_at
                .cmp(&right.next_fire_at)
                .then_with(|| left.task_id.cmp(&right.task_id))
                .then_with(|| left.wake_id.cmp(&right.wake_id))
        });
        *self.due_queue.lock().await = items;
        Ok(())
    }

    async fn next_sleep_duration(&self, now: i64) -> Duration {
        self.due_queue
            .lock()
            .await
            .first()
            .map(|item| {
                if item.next_fire_at <= now {
                    Duration::from_secs(0)
                } else {
                    Duration::from_secs(
                        u64::try_from(item.next_fire_at.saturating_sub(now)).unwrap_or(u64::MAX),
                    )
                }
            })
            .map(|duration| duration.min(Duration::from_secs(TASK_SCHEDULER_MAX_SLEEP_SECONDS)))
            .unwrap_or_else(|| Duration::from_secs(TASK_SCHEDULER_MAX_SLEEP_SECONDS))
    }

    async fn process_trigger(&self, trigger: TaskTrigger, now: i64) -> TaskRuntimeResult<usize> {
        let Some(task_response) = self.store.get_task(trigger.task_id.as_str()).await? else {
            return Ok(0);
        };
        if is_terminal_task(task_response.task.status)
            || trigger.status != TaskTriggerStatus::Active
        {
            return Ok(0);
        }
        if task_response.runs.iter().any(|run| {
            run.trigger_id.as_deref() == Some(trigger.id.as_str()) && !is_recurring(&trigger)
        }) {
            return Ok(0);
        }
        let active_run_count = task_response
            .runs
            .iter()
            .filter(|run| !crate::service::is_terminal_run(run.status))
            .count();
        let max_parallel_runs = task_response
            .task
            .concurrency_policy
            .as_ref()
            .map(|policy| usize::try_from(policy.max_parallel_runs.max(1)).unwrap_or(1))
            .unwrap_or(1);
        if active_run_count >= max_parallel_runs {
            if is_recurring(&trigger) && trigger.next_fire_at.is_some_and(|next| next <= now) {
                self.skip_recurring_fire_for_active_run(
                    task_response.task.id.clone(),
                    trigger.clone(),
                    now,
                )
                .await?;
            }
            return Ok(0);
        }

        let catch_up = TaskTriggerCalculator::catch_up_plan(&trigger, now)?;
        let available_run_slots = max_parallel_runs.saturating_sub(active_run_count).max(1);
        let planned_fire_count = catch_up.fire_times.len();
        let mut fire_times = catch_up.fire_times;
        if fire_times.len() > available_run_slots {
            fire_times.truncate(available_run_slots);
        }
        let mut updated_trigger = trigger.clone();
        updated_trigger.last_fire_at = fire_times.last().copied().or(catch_up.last_fire_at);
        updated_trigger.updated_at = now;
        if fire_times.len() < planned_fire_count {
            updated_trigger.next_fire_at = match fire_times.last().copied() {
                Some(fire_at) => TaskTriggerCalculator::next_after_fire(&trigger, fire_at)?,
                None => catch_up.next_fire_at,
            };
            updated_trigger.status = TaskTriggerStatus::Active;
        } else if catch_up.exhausted {
            updated_trigger.next_fire_at = None;
            updated_trigger.status = TaskTriggerStatus::Exhausted;
        } else if is_recurring(&trigger) {
            updated_trigger.next_fire_at = catch_up.next_fire_at;
            updated_trigger.status = TaskTriggerStatus::Active;
        } else {
            updated_trigger.next_fire_at = None;
            updated_trigger.status = TaskTriggerStatus::Exhausted;
        }

        let mut runs = Vec::new();
        let mut events = Vec::new();
        let mut next_run_number = task_response
            .runs
            .last()
            .map(|run| run.run_number.saturating_add(1))
            .unwrap_or(1);
        let mut parent_run_id = task_response.runs.last().map(|run| run.id.clone());
        for _fire_at in fire_times {
            let run = TaskRun {
                id: generate_id(ID_LEN),
                task_id: task_response.task.id.clone(),
                trigger_id: Some(trigger.id.clone()),
                parent_run_id: parent_run_id.clone(),
                run_group_id: generate_id(ID_LEN),
                attempt_number: 1,
                retry_of_run_id: None,
                ready_at: Some(now),
                run_number: next_run_number,
                status: TaskRunStatus::Queued,
                executor_kind: task_response.task.executor_kind,
                started_at: None,
                completed_at: None,
                heartbeat_at: None,
                locked_by: None,
                lock_expires_at: None,
                result: None,
                error: None,
                created_at: now,
                updated_at: now,
            };
            events.push(TaskEventPayload::TaskQueued {
                task_id: task_response.task.id.clone(),
                run_id: Some(run.id.clone()),
            });
            events.push(TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: task_response.agent_specs.last().cloned().map(|mut spec| {
                    spec.run_id = Some(run.id.clone());
                    spec.updated_at = now;
                    spec
                }),
            });
            parent_run_id = Some(run.id.clone());
            next_run_number = next_run_number.saturating_add(1);
            runs.push(run);
        }
        events.push(TaskEventPayload::TaskRescheduled {
            task_id: task_response.task.id.clone(),
            trigger: updated_trigger,
            rescheduled_at: now,
            reason: if runs.is_empty() {
                TaskRescheduleReason::MissedFireSkipped
            } else {
                TaskRescheduleReason::TriggerFired
            },
        });
        let Some(expected_next_fire_at) = trigger.next_fire_at else {
            return Ok(0);
        };
        let reserve_executions = runs
            .iter()
            .map(|run| (run.id.clone(), run.executor_kind))
            .collect::<Vec<_>>();
        let appended = self
            .store
            .append_due_trigger_task_events(
                trigger.id.as_str(),
                expected_next_fire_at,
                now,
                events,
                reserve_executions,
            )
            .await?;
        if appended.is_empty() {
            return Ok(0);
        }
        self.event_bus.publish_many(appended).await;
        let created_count = runs.len();
        for run in runs {
            let _ = self
                .dispatch_run(task_response.task.workspace_id.clone(), run)
                .await?;
        }
        Ok(created_count)
    }

    async fn skip_recurring_fire_for_active_run(
        &self,
        task_id: String,
        trigger: TaskTrigger,
        now: i64,
    ) -> TaskRuntimeResult<()> {
        let mut updated_trigger = trigger;
        updated_trigger.next_fire_at =
            TaskTriggerCalculator::next_after_fire(&updated_trigger, now)?;
        updated_trigger.status = TaskTriggerStatus::Active;
        updated_trigger.updated_at = now;
        let appended = self
            .projector
            .append_events(
                vec![TaskEventPayload::TaskRescheduled {
                    task_id: task_id.clone(),
                    trigger: updated_trigger.clone(),
                    rescheduled_at: now,
                    reason: TaskRescheduleReason::MissedFireSkipped,
                }],
                now,
            )
            .await?;
        self.event_bus.publish_many(appended).await;
        debug!(
            task_id = %task_id,
            trigger_id = %updated_trigger.id,
            next_fire_at = ?updated_trigger.next_fire_at,
            "skipped recurring task fire because a prior run is still active"
        );
        Ok(())
    }

    async fn process_retry_run(&self, run: TaskRun, now: i64) -> TaskRuntimeResult<bool> {
        let Some(task_response) = self.store.get_task(run.task_id.as_str()).await? else {
            return Ok(false);
        };
        if is_terminal_task(task_response.task.status)
            || crate::service::is_terminal_run(run.status)
            || run.retry_of_run_id.is_none()
            || run.ready_at.is_some_and(|ready_at| ready_at > now)
        {
            return Ok(false);
        }
        self.dispatch_run(task_response.task.workspace_id, run)
            .await
    }

    async fn process_queued_run(&self, run: TaskRun) -> TaskRuntimeResult<bool> {
        let Some(task_response) = self.store.get_task(run.task_id.as_str()).await? else {
            return Ok(false);
        };
        if is_terminal_task(task_response.task.status)
            || crate::service::is_terminal_run(run.status)
        {
            return Ok(false);
        }
        if self.executors.get(run.executor_kind).await.is_none() {
            return Ok(false);
        }
        self.dispatch_run(task_response.task.workspace_id, run)
            .await
    }

    async fn dispatch_run(&self, workspace_id: String, run: TaskRun) -> TaskRuntimeResult<bool> {
        let Some(executor) = self.executors.get(run.executor_kind).await else {
            return Ok(false);
        };
        let claimed_at = now_timestamp_secs();
        let Some(run) = self
            .store
            .claim_task_run_for_dispatch(run.id.as_str(), claimed_at)
            .await?
        else {
            debug!(
                task_id = %run.task_id,
                run_id = %run.id,
                status = ?run.status,
                "task run dispatch skipped because it was already claimed"
            );
            return Ok(false);
        };
        let execution = self
            .store
            .reserve_execution_for_run(run.id.as_str(), run.executor_kind, claimed_at)
            .await?;
        let worker_id = format!("task-worker-{}", generate_id(ID_LEN));
        let lease_until = claimed_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS);
        let Some(execution) = self
            .store
            .claim_execution_at(
                execution.id.as_str(),
                worker_id.as_str(),
                claimed_at,
                lease_until,
            )
            .await?
        else {
            debug!(
                task_id = %run.task_id,
                run_id = %run.id,
                execution_id = %execution.id,
                "task run dispatch skipped because execution is terminal"
            );
            return Ok(false);
        };
        let context = TaskExecutionContext {
            workspace_id,
            task_id: run.task_id.clone(),
            execution_id: Some(execution.id),
            worker_id,
        };
        let handle = TaskExecutionHandle::new(
            self.store.clone(),
            self.event_bus.clone(),
            run.task_id.clone(),
            run.id.clone(),
        );

        if run.executor_kind == TaskExecutorKind::Agent {
            tokio::spawn(async move {
                if let Err(error) = dispatch_run_to_executor(executor, context, run, handle).await {
                    error!(error = %format!("{error:#}"), "agent task dispatch failed");
                }
            });
            return Ok(true);
        }

        dispatch_run_to_executor(executor, context, run, handle).await?;
        Ok(true)
    }
}

async fn dispatch_run_to_executor(
    executor: Arc<dyn crate::executor::TaskExecutor>,
    context: TaskExecutionContext,
    run: TaskRun,
    handle: TaskExecutionHandle,
) -> TaskRuntimeResult<()> {
    match executor
        .start_run(context, run.clone(), handle.clone())
        .await
    {
        Ok(TaskExecutorStartOutcome::Started) => {}
        Ok(TaskExecutorStartOutcome::Queued) => {}
        Ok(TaskExecutorStartOutcome::Rejected) => {
            let now = now_timestamp_secs();
            let error = task_error(
                "task_executor_rejected",
                "task executor rejected the run".to_owned(),
                TaskErrorClass::Internal,
                Some(run.id.clone()),
            );
            handle.fail_run(Some(error), now).await?;
        }
        Err(error) => {
            let now = now_timestamp_secs();
            let task_error = task_error(
                "task_executor_start_failed",
                format!("{error:#}"),
                TaskErrorClass::Internal,
                Some(run.id.clone()),
            );
            handle.fail_run(Some(task_error), now).await?;
        }
    }
    Ok(())
}

fn is_recurring(trigger: &TaskTrigger) -> bool {
    matches!(
        trigger.kind(),
        TaskTriggerKind::Interval | TaskTriggerKind::Cron
    )
}
