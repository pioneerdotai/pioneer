use crate::TaskRuntimeResult;
use crate::actor_contract::build_occurrence_contract;
use crate::event_bus::{TaskEventBus, TaskEventSubscription, TaskEventWakeDelivery};
use crate::executor::{
    TaskExecutionContext, TaskExecutionHandle, TaskExecutorRegistry, TaskExecutorStartOutcome,
};
use crate::service::{
    LIVE_RECONCILIATION_INTERVAL, is_terminal_task, now_timestamp_secs, task_error,
};
use crate::trigger::{TaskTriggerCalculator, TaskTriggerCatchUpPlan};
use anyhow::Context;
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    TaskErrorClass, TaskEventPayload, TaskExecutorKind, TaskRescheduleReason, TaskRun,
    TaskRunStatus, TaskTrigger, TaskTriggerKind, TaskTriggerStatus, generate_id,
};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS,
    is_anyhow_sqlite_transient_access, retry_with_backoff,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, sleep};
use tracing::{debug, error, warn};

const ID_LEN: usize = 21;
pub const TASK_EXECUTION_LEASE_SECONDS: i64 = 300;
const TASK_SCHEDULER_MAX_SLEEP_SECONDS: u64 = 60;
const TASK_FINISH_QUEUE_TASK_LIMIT: u64 = 64;

/// Only schedule calculation failures can quarantine a Task. Storage and
/// dispatch failures must retain their existing retry/recovery behavior.
#[derive(Debug)]
struct TriggerScheduleError(anyhow::Error);

/// Discovery completed, but these addressed locators remain retryable.
#[derive(Debug)]
pub(crate) struct PartialDiscoveryError(Vec<pioneer_crud::TaskSchedulerRowFailure>);
impl std::fmt::Display for PartialDiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "incomplete scheduler rows: {:?}", self.0)
    }
}
impl std::error::Error for PartialDiscoveryError {}

impl std::fmt::Display for TriggerScheduleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for TriggerScheduleError {}

#[derive(Clone)]
pub struct TaskSchedulerHandle {
    notify: Arc<Notify>,
    dirty_tasks: Arc<Mutex<HashMap<String, u64>>>,
    needs_rebuild: Arc<AtomicBool>,
}

impl TaskSchedulerHandle {
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// Mark the exact inputs before polling a fresh mutation future. Wake only
    /// after it returns and releases DB owners; cancellation/unknown outcome
    /// keeps SchedulerPass's rebuild flag without sending under DB capacity.
    pub(crate) async fn track_task_mutation<T>(
        &self,
        task_id: &str,
        operation: impl std::future::Future<Output = TaskRuntimeResult<T>>,
    ) -> TaskRuntimeResult<T> {
        {
            let mut dirty = self.dirty_tasks.lock().await;
            let revision = dirty.entry(task_id.to_owned()).or_default();
            *revision = revision.wrapping_add(1);
        }
        let mut pass = SchedulerPass {
            needs_rebuild: &self.needs_rebuild,
            complete: false,
        };
        let outcome = operation.await;
        // A concurrent refresh may have consumed the pre-write revision. Keep
        // a fresh addressed observation even when no new event was appended.
        {
            let mut dirty = self.dirty_tasks.lock().await;
            let revision = dirty.entry(task_id.to_owned()).or_default();
            *revision = revision.wrapping_add(1);
        }
        pass.complete = outcome.is_ok();
        drop(pass);
        self.notify.notify_one();
        outcome
    }
}

pub struct TaskScheduler {
    store: Arc<CrudStore>,
    event_bus: Arc<TaskEventBus>,
    executors: Arc<TaskExecutorRegistry>,
    notify: Arc<Notify>,
    due_queue: Mutex<Vec<TaskTriggerQueueItem>>,
    process_lock: Mutex<()>,
    events: Mutex<TaskEventSubscription>,
    needs_rebuild: Arc<AtomicBool>,
    dirty_tasks: Arc<Mutex<HashMap<String, u64>>>,
    due_run_cursor: Mutex<Option<String>>,
    due_run_upper: Mutex<Option<String>>,
    due_trigger_upper: Mutex<Option<(chrono::DateTime<chrono::FixedOffset>, String)>>,
    expired_task_cursor: Mutex<Option<(String, String)>>,
    due_trigger_cursor: Mutex<Option<(chrono::DateTime<chrono::FixedOffset>, String)>>,
}

#[derive(Debug, Clone, PartialEq)]
struct TaskTriggerQueueItem {
    task_id: String,
    wake_id: String,
    next_fire_at: i64,
    // Only a future observation arms an occurrence. Keep the schedule snapshot
    // so a replacement spec at the same timestamp cannot inherit it.
    armed_trigger: Option<TaskTrigger>,
}

/// Cancellation or an error after possible mutation makes the next pass
/// reconstruct durable state; only a completed pass may disarm this guard.
struct SchedulerPass<'a> {
    needs_rebuild: &'a AtomicBool,
    complete: bool,
}
impl Drop for SchedulerPass<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.needs_rebuild.store(true, Ordering::Release);
        }
    }
}

impl TaskScheduler {
    pub fn new(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        executors: Arc<TaskExecutorRegistry>,
    ) -> Self {
        let events = event_bus.subscribe(Default::default());
        let notify = event_bus.scheduler_notify();
        Self {
            store: Arc::new(store.with_maintenance_access()),
            event_bus,
            executors,
            notify,
            due_queue: Mutex::new(Vec::new()),
            process_lock: Mutex::new(()),
            events: Mutex::new(events),
            needs_rebuild: Arc::new(AtomicBool::new(true)),
            dirty_tasks: Arc::new(Mutex::new(HashMap::new())),
            due_run_cursor: Mutex::new(None),
            due_run_upper: Mutex::new(None),
            due_trigger_upper: Mutex::new(None),
            expired_task_cursor: Mutex::new(None),
            due_trigger_cursor: Mutex::new(None),
        }
    }

    pub fn handle(&self) -> TaskSchedulerHandle {
        TaskSchedulerHandle {
            notify: self.notify.clone(),
            dirty_tasks: self.dirty_tasks.clone(),
            needs_rebuild: self.needs_rebuild.clone(),
        }
    }

    pub fn wake(&self) {
        self.notify.notify_one();
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            let now = now_timestamp_secs();
            let processing = self
                .process_due_once_with_transient_storage_retry(now)
                .await;
            let sleep_duration = match processing {
                Ok(_) => self.next_sleep_duration(now).await,
                Err(error) => {
                    // SchedulerPass already records unknown outcomes. Partial row
                    // errors preserve discovery progress and addressed retries.
                    let is_transient_storage = is_transient_scheduler_storage_error(&error);
                    if is_transient_storage {
                        warn!(
                            failure_class = "task_scheduler_transient_storage_failure",
                            "task scheduler due processing deferred by transient storage access failure"
                        );
                        Duration::from_secs(TASK_SCHEDULER_MAX_SLEEP_SECONDS)
                    } else {
                        error!(
                            failure_class = "task_scheduler_due_processing_failed",
                            "task scheduler due processing failed"
                        );
                        self.next_sleep_duration(now).await
                    }
                }
            };
            tokio::select! {
                _ = self.notify.notified() => {}
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
            is_transient_scheduler_storage_error,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn process_due_once(&self, now: i64) -> TaskRuntimeResult<usize> {
        let _guard = self.process_lock.lock().await;
        let mut pass = SchedulerPass {
            needs_rebuild: &self.needs_rebuild,
            complete: false,
        };
        // Recover a stop committed before queue disposal, without waiting for
        // a retry's old ready_at. One bounded disposal quantum per Task keeps
        // transactions small; any remaining queue is rediscovered next pass.
        for task_id in self
            .store
            .list_task_finish_queue_candidates(TASK_FINISH_QUEUE_TASK_LIMIT)
            .await?
        {
            self.mark_task_dirty(&task_id).await;
            let events = self
                .store
                .discard_finished_task_queue(&task_id, now)
                .await?;
            self.event_bus.publish_many(events).await;
        }
        let mut row_failures = self.refresh_inputs(now).await?;
        let after = self.due_trigger_cursor.lock().await.clone();
        let upper = self.due_trigger_upper.lock().await.clone();
        let (due, upper) = self
            .store
            .list_due_active_task_triggers_page_finite(
                now,
                after.as_ref().map(|(at, id)| (*at, id.as_str())),
                upper.as_ref().map(|(at, id)| (*at, id.as_str())),
                1024,
            )
            .await?;
        // Advance from discovery, including no-op and failed rows. A short
        // page wraps next pass; neither poison nor an unchanged head can pin it.
        // SQL already bounds and orders the page. Equality is sufficient;
        // remote database collation need not match Rust byte ordering.
        let more = due.scanned == 1024 && due.last.as_ref() != upper.as_ref();
        // Acquire both advisory guards before publishing the pair: cancellation
        // cannot leave an advanced cursor with an uncaptured/replaced upper.
        {
            let mut cursor = self.due_trigger_cursor.lock().await;
            let mut upper_state = self.due_trigger_upper.lock().await;
            *cursor = if more { due.last.clone() } else { None };
            *upper_state = if more { upper } else { None };
        }
        for failure in &due.failures {
            self.mark_task_dirty(&failure.task_id).await;
        }
        row_failures.extend(due.failures);
        let armed = self
            .due_queue
            .lock()
            .await
            .iter()
            .filter_map(|item| item.armed_trigger.as_ref())
            .map(|trigger| (trigger.id.clone(), trigger.clone()))
            .collect::<HashMap<_, _>>();
        let mut created = 0usize;
        let mut failure = None;
        for trigger in due.records {
            // Mark before attempting a write: a failed/unknown commit is not empty.
            self.mark_task_dirty(&trigger.task_id).await;
            let is_armed = armed
                .get(&trigger.id)
                .is_some_and(|previous| same_scheduled_occurrence(previous, &trigger));
            match self.process_trigger(trigger.clone(), now, is_armed).await {
                Ok(count) => created = created.saturating_add(count),
                Err(error) if error.downcast_ref::<TriggerScheduleError>().is_some() => {
                    if let Err(error) = self
                        .block_task_for_trigger_failure(
                            &trigger,
                            now,
                            "task_trigger_schedule_invalid",
                            &error,
                        )
                        .await
                    {
                        failure = Some(error);
                    }
                }
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        let after_id = self.due_run_cursor.lock().await.clone();
        let upper = self.due_run_upper.lock().await.clone();
        let (runs, upper) = self
            .store
            .list_scheduler_due_task_runs_finite(now, after_id.as_deref(), upper.as_deref(), 1024)
            .await?;
        let more = runs.scanned == 1024 && runs.last.as_ref() != upper.as_ref();
        // Acquire both advisory guards before publishing the pair: cancellation
        // cannot leave an advanced cursor with an uncaptured/replaced upper.
        {
            let mut cursor = self.due_run_cursor.lock().await;
            let mut upper_state = self.due_run_upper.lock().await;
            *cursor = if more { runs.last.clone() } else { None };
            *upper_state = if more { upper } else { None };
        }
        for failure in &runs.failures {
            self.mark_task_dirty(&failure.task_id).await;
        }
        row_failures.extend(runs.failures);
        for run in runs.records {
            self.mark_task_dirty(&run.task_id).await;
            let outcome = if run.retry_of_run_id.is_some() {
                self.process_retry_run(run, now).await
            } else {
                self.process_queued_run(&self.store, run).await
            };
            match outcome {
                Ok(true) => created = created.saturating_add(1),
                Ok(false) => {}
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        row_failures.extend(self.refresh_inputs(now).await?);
        if let Some(error) = failure {
            return Err(error.context(format!("discovery row failures: {:?}", row_failures)));
        }
        if self.needs_rebuild.load(Ordering::Acquire) {
            return Err(anyhow::anyhow!(
                "unknown addressed scheduler discovery: {:?}",
                row_failures
            ));
        }
        pass.complete = true;
        if !row_failures.is_empty() {
            return Err(PartialDiscoveryError(row_failures).into());
        }
        Ok(created)
    }

    pub(crate) async fn process_queued_run_once(
        &self,
        store: &Arc<CrudStore>,
        run: TaskRun,
    ) -> TaskRuntimeResult<bool> {
        // Inherit the request handle for this freshly committed immediate run.
        // Scheduled/background dispatch continues to use the scheduler store.
        // Do not queue the caller behind an unrelated global due sweep. A
        // concurrent scheduler wake is safe: claim_task_run_for_dispatch is
        // the per-run CAS and exactly one contender can own the dispatch.
        let mut pass = SchedulerPass {
            needs_rebuild: &self.needs_rebuild,
            complete: false,
        };
        let task_id = run.task_id.clone();
        self.mark_task_dirty(&task_id).await;
        self.notify.notify_one();
        let outcome = self.process_queued_run(store, run).await?;
        self.mark_task_dirty(&task_id).await;
        self.notify.notify_one();
        pass.complete = true;
        Ok(outcome)
    }

    async fn load_due_task_actor_contract(
        &self,
        trigger: &TaskTrigger,
        now: i64,
    ) -> TaskRuntimeResult<Option<pioneer_protocol::TaskActorContract>> {
        match self
            .store
            .get_task_actor_contract(trigger.task_id.as_str())
            .await
        {
            Ok(Some(contract)) => Ok(Some(contract)),
            Ok(None) => {
                let error =
                    anyhow::anyhow!("Task `{}` has no durable actor contract", trigger.task_id);
                self.block_task_for_trigger_failure(
                    trigger,
                    now,
                    "task_actor_contract_invalid",
                    &error,
                )
                .await?;
                Ok(None)
            }
            Err(error) if is_transient_scheduler_storage_error(&error) => Err(error),
            Err(error) => {
                // Contract decoding, fingerprint verification and version
                // migration all happen before a Run is materialized. A bad
                // immutable row therefore belongs to this Task and can be
                // isolated without changing the retry semantics of later
                // storage or dispatch failures.
                self.block_task_for_trigger_failure(
                    trigger,
                    now,
                    "task_actor_contract_invalid",
                    &error,
                )
                .await?;
                Ok(None)
            }
        }
    }

    async fn block_task_for_trigger_failure(
        &self,
        trigger: &TaskTrigger,
        now: i64,
        error_code: &str,
        error: &anyhow::Error,
    ) -> TaskRuntimeResult<()> {
        let message = format!("{error:#}");
        let Some(expected_next_fire_at) = trigger.next_fire_at else {
            return Ok(());
        };
        let appended = match self
            .store
            .block_task_for_due_trigger_failure(
                trigger,
                task_error(
                    error_code,
                    message.clone(),
                    TaskErrorClass::Validation,
                    None,
                ),
                now,
            )
            .await
        {
            Ok(appended) => appended,
            Err(append_error) if is_transient_scheduler_storage_error(&append_error) => {
                return Err(append_error);
            }
            Err(append_error) => {
                // A terminal transition can win after process_trigger read the
                // Task but before this isolation transaction starts. The
                // terminal event deliberately owns the same idempotency slot,
                // so reconcile that newer state instead of turning a harmless
                // race into a scheduler-wide failure.
                if let Some(reconciliation) = self
                    .store
                    .reconcile_due_trigger_for_nonexecuting_task(
                        trigger.id.as_str(),
                        expected_next_fire_at,
                        now,
                    )
                    .await?
                {
                    self.event_bus
                        .publish_many(reconciliation.appended_events)
                        .await;
                    debug!(
                        task_id = %trigger.task_id,
                        trigger_id = %trigger.id,
                        error = %message,
                        task_status = ?reconciliation.task_status,
                        trigger_status = ?reconciliation.trigger_status,
                        failure_class = "task_scheduler_trigger_failure_terminal_race",
                        "a concurrent terminal Task transition superseded trigger isolation"
                    );
                    return Ok(());
                }
                return Err(append_error);
            }
        };
        if appended.is_empty() {
            // The same transaction that appends TaskBlocked also verifies the
            // original due timestamp and schedule. If another actor already
            // repaired or replaced the trigger, that newer state wins and
            // must not be overwritten by this stale failure.
            debug!(
                task_id = %trigger.task_id,
                trigger_id = %trigger.id,
                error = %message,
                failure_class = "task_scheduler_trigger_failure_superseded",
                "did not block a Task because its due trigger changed concurrently"
            );
            return Ok(());
        }
        self.event_bus.publish_many(appended).await;
        warn!(
            task_id = %trigger.task_id,
            trigger_id = %trigger.id,
            error = %message,
            failure_class = "task_scheduler_trigger_isolated",
            "blocked a Task whose durable trigger could not be processed"
        );
        Ok(())
    }

    async fn mark_task_dirty(&self, task_id: &str) {
        let mut dirty = self.dirty_tasks.lock().await;
        let revision = dirty.entry(task_id.to_owned()).or_default();
        *revision = revision.wrapping_add(1);
    }

    /// The subscription is created with the scheduler, before startup work. A
    /// bounded broadcast retains exact task hints; overflow requests durable
    /// reconstruction. Notifications only wake the loop and never imply empty.
    async fn refresh_inputs(
        &self,
        now: i64,
    ) -> TaskRuntimeResult<Vec<pioneer_crud::TaskSchedulerRowFailure>> {
        let mut failures = Vec::new();
        {
            let mut events = self.events.lock().await;
            let mut dirty = self.dirty_tasks.lock().await;
            // One channel quantum. A saturated channel reports lag on the next
            // pass rather than keeping the scheduler in an unbounded drain.
            for _ in 0..1024 {
                match events.try_recv() {
                    Some(TaskEventWakeDelivery::Wake(wake)) => {
                        let version = dirty.entry(wake.task_id).or_default();
                        *version = version.wrapping_add(1);
                    }
                    Some(TaskEventWakeDelivery::Lagged(_)) => {
                        self.needs_rebuild.store(true, Ordering::Release);
                    }
                    Some(TaskEventWakeDelivery::Closed) | None => break,
                }
            }
        }
        if self.needs_rebuild.load(Ordering::Acquire) {
            failures.extend(self.rebuild_due_queue(now).await?);
            self.needs_rebuild.store(false, Ordering::Release);
        }
        // Positive due SQL cannot observe foreign pause/delete/move-later. Rotate
        // addressed expired cache tasks, bounded independently of dirty hints.
        let mut expired = self
            .due_queue
            .lock()
            .await
            .iter()
            .filter(|item| item.next_fire_at <= now)
            .map(|item| item.task_id.clone())
            .collect::<Vec<_>>();
        expired.sort();
        expired.dedup();
        let mut cursor = self.expired_task_cursor.lock().await;
        // Capture a finite cache pass; newly appended suffix tasks cannot defer
        // revisiting an older expired locator indefinitely.
        let upper = cursor
            .as_ref()
            .map(|(_, upper)| upper.clone())
            .or_else(|| expired.last().cloned());
        let selected = expired
            .iter()
            .filter(|id| {
                cursor.as_ref().is_none_or(|(after, _)| *id > after)
                    && upper.as_ref().is_some_and(|upper| *id <= upper)
            })
            .take(64)
            .cloned()
            .collect::<Vec<_>>();
        *cursor = match (selected.last(), upper) {
            (Some(last), Some(upper)) if selected.len() == 64 && last < &upper => {
                Some((last.clone(), upper))
            }
            _ => None,
        };
        drop(cursor);
        for id in selected {
            self.mark_task_dirty(&id).await;
        }
        // Keep each locator until its reload succeeds, including cancellation.
        let tasks = self.dirty_tasks.lock().await.clone();
        for (task_id, revision) in tasks {
            let (triggers, runs) = match self.store.task_scheduler_inputs_partial(&task_id).await {
                Ok(inputs) => inputs,
                Err(error) => {
                    self.needs_rebuild.store(true, Ordering::Release);
                    failures.push(pioneer_crud::TaskSchedulerRowFailure {
                        id: task_id.clone(),
                        task_id: task_id.clone(),
                        error,
                    });
                    continue;
                }
            };
            let incomplete = !triggers.failures.is_empty() || !runs.failures.is_empty();
            let bad_ids = triggers
                .failures
                .iter()
                .chain(&runs.failures)
                .map(|row| row.id.clone())
                .collect::<std::collections::HashSet<_>>();
            failures.extend(triggers.failures);
            failures.extend(runs.failures);
            let mut queue = self.due_queue.lock().await;
            let armed = queue
                .iter()
                .filter_map(|item| item.armed_trigger.clone())
                .map(|trigger| (trigger.id.clone(), trigger))
                .collect::<HashMap<_, _>>();
            queue.retain(|item| item.task_id != task_id || bad_ids.contains(&item.wake_id));
            {
                for trigger in triggers.records {
                    if trigger.status == TaskTriggerStatus::Active {
                        if let Some(next_fire_at) = trigger.next_fire_at {
                            let is_armed = next_fire_at > now
                                || armed
                                    .get(&trigger.id)
                                    .is_some_and(|old| same_scheduled_occurrence(old, &trigger));
                            queue.push(TaskTriggerQueueItem {
                                task_id: task_id.clone(),
                                wake_id: trigger.id.clone(),
                                next_fire_at,
                                armed_trigger: is_armed.then_some(trigger),
                            });
                        }
                    }
                }
                for run in runs.records {
                    if run.status == TaskRunStatus::Queued {
                        queue.push(TaskTriggerQueueItem {
                            task_id: task_id.clone(),
                            wake_id: run.id,
                            next_fire_at: run.ready_at.unwrap_or(now),
                            armed_trigger: None,
                        });
                    }
                }
            }
            queue.sort_by_key(|item| {
                (
                    item.next_fire_at,
                    item.task_id.clone(),
                    item.wake_id.clone(),
                )
            });
            drop(queue);
            let mut dirty = self.dirty_tasks.lock().await;
            if !incomplete && dirty.get(&task_id) == Some(&revision) {
                dirty.remove(&task_id);
            }
        }
        Ok(failures)
    }

    async fn rebuild_due_queue(
        &self,
        now: i64,
    ) -> TaskRuntimeResult<Vec<pioneer_crud::TaskSchedulerRowFailure>> {
        let triggers = self.store.list_active_task_triggers_partial().await?;
        let runs = self.store.list_queued_task_schedule_partial(1024).await?;
        let mut failures = triggers.failures;
        failures.extend(runs.failures);
        for row in &failures {
            self.mark_task_dirty(&row.task_id).await;
        }
        let armed = self
            .due_queue
            .lock()
            .await
            .iter()
            .filter_map(|item| item.armed_trigger.as_ref())
            .map(|trigger| (trigger.id.clone(), trigger.clone()))
            .collect::<HashMap<_, _>>();
        let mut items = triggers
            .records
            .into_iter()
            .filter_map(|trigger| {
                let next_fire_at = trigger.next_fire_at?;
                let is_armed = next_fire_at > now
                    || armed
                        .get(&trigger.id)
                        .is_some_and(|previous| same_scheduled_occurrence(previous, &trigger));
                Some(TaskTriggerQueueItem {
                    task_id: trigger.task_id.clone(),
                    wake_id: trigger.id.clone(),
                    next_fire_at,
                    armed_trigger: is_armed.then_some(trigger),
                })
            })
            .collect::<Vec<_>>();
        items.extend(runs.records.into_iter().filter_map(|run| {
            let ready_at = run.ready_at.unwrap_or(now);
            Some(TaskTriggerQueueItem {
                task_id: run.task_id,
                wake_id: run.id,
                next_fire_at: ready_at,
                armed_trigger: None,
            })
        }));
        items.sort_by(|left, right| {
            left.next_fire_at
                .cmp(&right.next_fire_at)
                .then_with(|| left.task_id.cmp(&right.task_id))
                .then_with(|| left.wake_id.cmp(&right.wake_id))
        });
        let bad_ids = failures
            .iter()
            .map(|row| row.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut queue = self.due_queue.lock().await;
        items.extend(
            queue
                .iter()
                .filter(|item| bad_ids.contains(item.wake_id.as_str()))
                .cloned(),
        );
        items.sort_by_key(|item| {
            (
                item.next_fire_at,
                item.task_id.clone(),
                item.wake_id.clone(),
            )
        });
        *queue = items;
        Ok(failures)
    }

    pub(crate) async fn next_sleep_duration(&self, now: i64) -> Duration {
        self.due_queue
            .lock()
            .await
            .first()
            .map(|item| {
                if item.next_fire_at <= now {
                    Duration::ZERO
                } else {
                    Duration::from_secs(
                        u64::try_from(item.next_fire_at.saturating_sub(now)).unwrap_or(u64::MAX),
                    )
                }
            })
            .map(|duration| duration.min(Duration::from_secs(TASK_SCHEDULER_MAX_SLEEP_SECONDS)))
            .unwrap_or_else(|| Duration::from_secs(TASK_SCHEDULER_MAX_SLEEP_SECONDS))
            // The former unconditional live-reconcile pulse supplied this
            // foreign-writer discovery ceiling. Keep it without empty wakes.
            // Transient storage backoff in run() retains its separate 60s path.
            .min(LIVE_RECONCILIATION_INTERVAL)
    }

    async fn process_trigger(
        &self,
        trigger: TaskTrigger,
        now: i64,
        armed: bool,
    ) -> TaskRuntimeResult<usize> {
        if trigger.status != TaskTriggerStatus::Active {
            return Ok(0);
        }
        let Some(expected_next_fire_at) = trigger.next_fire_at else {
            return Ok(0);
        };
        let task_response = match self.store.get_task(trigger.task_id.as_str()).await? {
            Some(response) if !is_terminal_task(response.task.status) => response,
            _ => {
                if let Some(reconciliation) = self
                    .store
                    .reconcile_due_trigger_for_nonexecuting_task(
                        trigger.id.as_str(),
                        expected_next_fire_at,
                        now,
                    )
                    .await?
                {
                    self.event_bus
                        .publish_many(reconciliation.appended_events)
                        .await;
                    warn!(
                        task_id = %reconciliation.task_id,
                        trigger_id = %reconciliation.trigger_id,
                        task_status = ?reconciliation.task_status,
                        trigger_status = ?reconciliation.trigger_status,
                        "reconciled due trigger for a non-executing Task"
                    );
                }
                return Ok(0);
            }
        };
        if task_response.runs.iter().any(|run| {
            run.trigger_id.as_deref() == Some(trigger.id.as_str()) && !is_recurring(&trigger)
        }) {
            return Ok(0);
        }
        // Authenticate and upcast the immutable Task-owned contract before
        // calculating or materializing any new occurrence. The completed
        // one-shot guard above deliberately runs first: a stale trigger that
        // cannot create another occurrence must not reclassify its Task only
        // because an otherwise-unused historical contract is unreadable.
        // Everything after this boundary keeps the scheduler's existing
        // storage/dispatch retry semantics.
        let Some(actor_contract) = self.load_due_task_actor_contract(&trigger, now).await? else {
            return Ok(0);
        };
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
        let catch_up =
            TaskTriggerCalculator::due_plan(&trigger, now, armed).map_err(TriggerScheduleError)?;
        if active_run_count >= max_parallel_runs && !catch_up.fire_times.is_empty() {
            // Capacity is a scheduling condition, not a terminal result.  Keep
            // the occurrence durable and queued; the normal queued-run pass
            // will dispatch it once a permit is available.  This prevents a
            // recurring fire from disappearing merely because a sibling is
            // still running.
            return self
                .enqueue_saturated_occurrence(
                    task_response,
                    trigger,
                    &actor_contract,
                    catch_up,
                    now,
                )
                .await;
        }

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
                Some(fire_at) => TaskTriggerCalculator::next_after_fire(&trigger, fire_at)
                    .map_err(TriggerScheduleError)?,
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
        let mut next_generation = self
            .store
            .next_task_occurrence_execution_generation(task_response.task.id.as_str())
            .await?;
        let mut next_run_number = task_response
            .runs
            .last()
            .map(|run| {
                run.run_number
                    .checked_add(1)
                    .context("Task run number overflow")
            })
            .transpose()?
            .unwrap_or(1);
        let mut parent_run_id = task_response.runs.last().map(|run| run.id.clone());
        for _fire_at in fire_times {
            let run = TaskRun {
                trigger_input: None,
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
                agent_spec: task_response
                    .agent_specs
                    .iter()
                    .rev()
                    .find(|spec| spec.run_id.is_none())
                    .or_else(|| task_response.agent_specs.last())
                    .cloned()
                    .map(|mut spec| {
                        spec.id = generate_id(ID_LEN);
                        spec.run_id = Some(run.id.clone());
                        spec.updated_at = now;
                        spec
                    }),
            });
            parent_run_id = Some(run.id.clone());
            next_run_number = next_run_number
                .checked_add(1)
                .context("Task run number overflow")?;
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
        let reserve_executions = runs
            .iter()
            .map(|run| (run.id.clone(), run.executor_kind))
            .collect::<Vec<_>>();
        let mut occurrence_contracts = Vec::with_capacity(runs.len());
        for run in &runs {
            occurrence_contracts.push(build_occurrence_contract(
                &task_response.task,
                &actor_contract,
                run.id.as_str(),
                Some(trigger.id.as_str()),
                format!("{}:{}", trigger.id, run.run_number),
                next_generation,
                now,
            )?);
            next_generation = next_generation
                .checked_add(1)
                .context("Task occurrence execution generation overflow")?;
        }
        let appended = self
            .store
            .append_due_trigger_task_events(
                &trigger,
                now,
                events,
                occurrence_contracts,
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
                .dispatch_run(
                    &self.store,
                    task_response.task.workspace_id.clone(),
                    run,
                    None,
                )
                .await?;
        }
        Ok(created_count)
    }

    async fn enqueue_saturated_occurrence(
        &self,
        task_response: pioneer_protocol::TaskGetResponse,
        trigger: TaskTrigger,
        actor_contract: &pioneer_protocol::TaskActorContract,
        catch_up: TaskTriggerCatchUpPlan,
        now: i64,
    ) -> TaskRuntimeResult<usize> {
        if task_response.runs.iter().any(|run| {
            run.trigger_id.as_deref() == Some(trigger.id.as_str())
                && run.status == TaskRunStatus::Queued
                && run.ready_at.is_none_or(|ready_at| ready_at <= now)
        }) {
            // The occurrence already represented by the queued run must not
            // be duplicated.  Still consume this due fire, otherwise every
            // scheduler pass observes the same timestamp forever and the
            // trigger can never move past an occupied serial slot.
            let mut updated_trigger = trigger.clone();
            updated_trigger.last_fire_at = catch_up.last_fire_at;
            updated_trigger.updated_at = now;
            updated_trigger.next_fire_at = catch_up.next_fire_at;
            updated_trigger.status = if catch_up.exhausted {
                TaskTriggerStatus::Exhausted
            } else {
                TaskTriggerStatus::Active
            };
            let appended = self
                .store
                .append_due_trigger_task_events(
                    &trigger,
                    now,
                    vec![TaskEventPayload::TaskRescheduled {
                        task_id: task_response.task.id.clone(),
                        trigger: updated_trigger,
                        rescheduled_at: now,
                        reason: TaskRescheduleReason::MissedFireSkipped,
                    }],
                    Vec::new(),
                    Vec::new(),
                )
                .await?;
            if !appended.is_empty() {
                self.event_bus.publish_many(appended).await;
            }
            return Ok(0);
        }
        let run_number = task_response
            .runs
            .last()
            .map(|run| {
                run.run_number
                    .checked_add(1)
                    .context("Task run number overflow")
            })
            .transpose()?
            .unwrap_or(1);
        let run = TaskRun {
            trigger_input: None,
            id: generate_id(ID_LEN),
            task_id: task_response.task.id.clone(),
            trigger_id: Some(trigger.id.clone()),
            parent_run_id: task_response.runs.last().map(|run| run.id.clone()),
            run_group_id: generate_id(ID_LEN),
            attempt_number: 1,
            retry_of_run_id: None,
            ready_at: Some(now),
            run_number,
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
        let mut updated_trigger = trigger.clone();
        let fire_at = catch_up.fire_times[0];
        let truncated = catch_up.fire_times.len() > 1;
        updated_trigger.last_fire_at = if truncated {
            Some(fire_at)
        } else {
            catch_up.last_fire_at
        };
        updated_trigger.updated_at = now;
        if truncated {
            updated_trigger.next_fire_at =
                TaskTriggerCalculator::next_after_fire(&trigger, fire_at)
                    .map_err(TriggerScheduleError)?;
            updated_trigger.status = TaskTriggerStatus::Active;
        } else if !catch_up.exhausted {
            updated_trigger.next_fire_at = catch_up.next_fire_at;
            updated_trigger.status = TaskTriggerStatus::Active;
        } else {
            updated_trigger.next_fire_at = None;
            updated_trigger.status = TaskTriggerStatus::Exhausted;
        }
        let events = vec![
            TaskEventPayload::TaskQueued {
                task_id: task_response.task.id.clone(),
                run_id: Some(run.id.clone()),
            },
            TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: task_response
                    .agent_specs
                    .iter()
                    .rev()
                    .find(|spec| spec.run_id.is_none())
                    .or_else(|| task_response.agent_specs.last())
                    .cloned()
                    .map(|mut spec| {
                        spec.id = generate_id(ID_LEN);
                        spec.run_id = Some(run.id.clone());
                        spec.updated_at = now;
                        spec
                    }),
            },
            TaskEventPayload::TaskRescheduled {
                task_id: task_response.task.id.clone(),
                trigger: updated_trigger,
                rescheduled_at: now,
                reason: TaskRescheduleReason::TriggerFired,
            },
        ];
        let generation = self
            .store
            .next_task_occurrence_execution_generation(task_response.task.id.as_str())
            .await?;
        let occurrence = build_occurrence_contract(
            &task_response.task,
            actor_contract,
            run.id.as_str(),
            Some(trigger.id.as_str()),
            format!("{}:{}", trigger.id, run.run_number),
            generation,
            now,
        )?;
        let appended = self
            .store
            .append_due_trigger_task_events(&trigger, now, events, vec![occurrence], Vec::new())
            .await?;
        if appended.is_empty() {
            return Ok(0);
        }
        self.event_bus.publish_many(appended).await;
        Ok(1)
    }

    async fn process_retry_run(&self, run: TaskRun, now: i64) -> TaskRuntimeResult<bool> {
        let Some(task_response) = self.store.get_task(run.task_id.as_str()).await? else {
            return Ok(false);
        };
        if task_response.task.finish_request().is_some() {
            let events = self
                .store
                .discard_finished_task_queue(&run.task_id, now)
                .await?;
            self.event_bus.publish_many(events).await;
            return Ok(false);
        }
        if is_terminal_task(task_response.task.status)
            || crate::service::is_terminal_run(run.status)
            || run.retry_of_run_id.is_none()
            || run.ready_at.is_some_and(|ready_at| ready_at > now)
        {
            return Ok(false);
        }
        if !self.has_capacity_for_queued_run(&task_response, run.id.as_str()) {
            return Ok(false);
        }
        let previous = self
            .store
            .get_task_occurrence_contract_by_run(run.retry_of_run_id.as_deref().unwrap_or_default())
            .await?
            .with_context(|| {
                format!(
                    "Task retry `{}` has no durable source occurrence contract",
                    run.id
                )
            })?;
        let mut occurrence = previous.retry(run.attempt_number).map_err(|error| {
            anyhow::anyhow!("invalid task occurrence retry transition: {error:?}")
        })?;
        occurrence.run_id = run.id.clone();
        self.dispatch_run(
            &self.store,
            task_response.task.workspace_id,
            run,
            Some(occurrence),
        )
        .await
    }

    async fn process_queued_run(
        &self,
        store: &Arc<CrudStore>,
        run: TaskRun,
    ) -> TaskRuntimeResult<bool> {
        let Some(task_response) = store.get_task(run.task_id.as_str()).await? else {
            return Ok(false);
        };
        if task_response.task.finish_request().is_some() {
            let events = store
                .discard_finished_task_queue(&run.task_id, now_timestamp_secs())
                .await?;
            self.event_bus.publish_many(events).await;
            return Ok(false);
        }
        if is_terminal_task(task_response.task.status)
            || crate::service::is_terminal_run(run.status)
        {
            return Ok(false);
        }
        if !self.has_capacity_for_queued_run(&task_response, run.id.as_str()) {
            return Ok(false);
        }
        if self.executors.get(run.executor_kind).await.is_none() {
            return Ok(false);
        }
        store
            .get_task_occurrence_contract_by_run(run.id.as_str())
            .await?
            .with_context(|| {
                format!(
                    "queued Task run `{}` has no durable occurrence contract",
                    run.id
                )
            })?;
        self.dispatch_run(store, task_response.task.workspace_id, run, None)
            .await
    }

    fn has_capacity_for_queued_run(
        &self,
        task_response: &pioneer_protocol::TaskGetResponse,
        queued_run_id: &str,
    ) -> bool {
        let max_parallel_runs = task_response
            .task
            .concurrency_policy
            .as_ref()
            .map(|policy| usize::try_from(policy.max_parallel_runs.max(1)).unwrap_or(1))
            .unwrap_or(1);
        let Some(candidate) = task_response
            .runs
            .iter()
            .find(|run| run.id == queued_run_id)
        else {
            return false;
        };
        let now = now_timestamp_secs();
        let occupied = task_response
            .runs
            .iter()
            .filter(|run| {
                !crate::service::is_terminal_run(run.status) && run.status != TaskRunStatus::Queued
            })
            .count();
        let earlier_ready = task_response
            .runs
            .iter()
            .filter(|run| {
                run.status == TaskRunStatus::Queued && run.run_number < candidate.run_number
            })
            .filter(|run| run.ready_at.is_none_or(|ready| ready <= now))
            .count();
        occupied.saturating_add(earlier_ready) < max_parallel_runs
    }

    async fn dispatch_run(
        &self,
        store: &Arc<CrudStore>,
        workspace_id: String,
        run: TaskRun,
        retry_occurrence: Option<pioneer_protocol::TaskOccurrenceContract>,
    ) -> TaskRuntimeResult<bool> {
        let Some(executor) = self.executors.get(run.executor_kind).await else {
            return Ok(false);
        };
        let claimed_at = now_timestamp_secs();
        let claim = if let Some(occurrence) = retry_occurrence {
            store
                .claim_task_retry_for_dispatch_with_event(&run.id, claimed_at, occurrence)
                .await?
        } else {
            store
                .claim_task_run_for_dispatch_with_event(&run.id, claimed_at)
                .await?
        };
        let Some((run, claimed_event)) = claim else {
            debug!(
                task_id = %run.task_id,
                run_id = %run.id,
                status = ?run.status,
                "task run dispatch skipped because it was already claimed"
            );
            return Ok(false);
        };
        self.event_bus.publish(claimed_event).await;
        let worker_id = format!("task-worker-{}", generate_id(ID_LEN));
        let lease_until = claimed_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS);
        let Some(execution) = store
            .claim_task_run_execution_for_dispatch(
                run.id.as_str(),
                run.executor_kind,
                worker_id.as_str(),
                claimed_at,
                lease_until,
            )
            .await?
        else {
            debug!(
                task_id = %run.task_id,
                run_id = %run.id,
                "task run dispatch skipped because execution or occurrence is no longer claimable"
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
            store.clone(),
            self.event_bus.clone(),
            run.task_id.clone(),
            run.id.clone(),
        );

        if run.executor_kind == TaskExecutorKind::Agent {
            tokio::spawn(async move {
                if let Err(error) = dispatch_run_to_executor(executor, context, run, handle).await {
                    report_dispatch_terminal_failure(&error);
                }
            });
            return Ok(true);
        }

        dispatch_run_to_executor(executor, context, run, handle).await?;
        Ok(true)
    }
}

#[derive(Debug)]
struct DispatchStartDiagnostic(Option<crate::TaskStartFailureDescriptor>);
impl std::fmt::Display for DispatchStartDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("terminalization after executor start failure/rejection failed")
    }
}

fn report_dispatch_terminal_failure(error: &anyhow::Error) {
    let failure =
        crate::TaskStartFailure::from_borrowed_error(crate::TaskStartStage::ExecutorStart, error);
    let diagnostic = failure.descriptor();
    let original = error
        .downcast_ref::<DispatchStartDiagnostic>()
        .and_then(|value| value.0.as_ref());
    error!(
        failure_class = "agent_task_dispatch_failed",
        failure_phase = error
            .downcast_ref::<crate::executor::TerminalCommitPhase>()
            .map(|phase| phase.label()),
        cause = if error
            .downcast_ref::<pioneer_crud::TaskTerminalConflict>()
            .is_some()
        {
            "domain_conflict"
        } else {
            diagnostic.cause.label()
        },
        sqlite_primary_code = diagnostic.sqlite_primary_code,
        sqlite_extended_code = diagnostic.sqlite_extended_code,
        original_start_stage = original.map(|value| value.stage.label()),
        original_start_cause = original.map(|value| value.cause.label()),
        correlation_id = original.and_then(|value| value.correlation_id.as_deref()),
        "agent task dispatch failed"
    );
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
            handle
                .fail_run(Some(error), now)
                .await
                .context(DispatchStartDiagnostic(None))?;
        }
        Err(mut error) => {
            let descriptor = crate::TaskStartFailure::report_for_scheduler(&mut error);
            let now = now_timestamp_secs();
            let task_error = descriptor.task_error(Some(run.id.clone()));
            handle
                .fail_run(Some(task_error), now)
                .await
                .context(DispatchStartDiagnostic(Some(descriptor)))?;
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

fn same_scheduled_occurrence(left: &TaskTrigger, right: &TaskTrigger) -> bool {
    // RunTerminalStatusRefresh changes updated_at without changing the schedule.
    // It must not turn an armed deadline into recovery when a sibling finishes.
    left.id == right.id
        && left.task_id == right.task_id
        && left.status == right.status
        && left.spec == right.spec
        && left.next_fire_at == right.next_fire_at
        && left.last_fire_at == right.last_fire_at
        && left.created_at == right.created_at
}

fn is_transient_scheduler_storage_error(error: &anyhow::Error) -> bool {
    is_anyhow_sqlite_transient_access(error)
}

#[cfg(test)]
mod tests {
    use super::is_transient_scheduler_storage_error;
    use anyhow::anyhow;

    #[test]
    fn scheduler_treats_pool_acquire_timeout_as_transient_storage_access() {
        let error = anyhow!(
            "failed to list active task triggers: Failed to acquire connection from pool: \
             Connection pool timed out: Connection pool timed out"
        );

        assert!(is_transient_scheduler_storage_error(&error));
    }
    #[tokio::test]
    async fn scheduler_wake_survives_before_wait_registration() {
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let handle = super::TaskSchedulerHandle {
            notify: notify.clone(),
            dirty_tasks: Default::default(),
            needs_rebuild: Default::default(),
        };
        handle.wake();
        tokio::time::timeout(std::time::Duration::from_millis(100), notify.notified())
            .await
            .expect("post-commit wake must retain a permit before a wait exists");
    }
}
