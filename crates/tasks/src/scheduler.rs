use crate::TaskRuntimeResult;
use crate::actor_contract::build_occurrence_contract;
use crate::event_bus::TaskEventBus;
use crate::executor::{
    TaskExecutionContext, TaskExecutionHandle, TaskExecutorRegistry, TaskExecutorStartOutcome,
};
use crate::service::{is_terminal_task, now_timestamp_secs, task_error};
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
use std::sync::Arc;
use tokio::sync::Mutex;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::time::Duration;
use tracing::{debug, error, warn};

const ID_LEN: usize = 21;
pub const TASK_EXECUTION_LEASE_SECONDS: i64 = 300;
const TASK_SCHEDULER_STORAGE_BACKOFF_SECONDS: u64 = 60;
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
    changes: Arc<pioneer_crud::TaskSchedulerChanges>,
    work: Arc<pioneer_sqlite::background::Wake>,
}

impl TaskSchedulerHandle {
    pub fn wake(&self) {
        self.work.invalidate();
    }

    /// Mark the exact inputs before polling a fresh mutation future. Wake only
    /// after it returns and releases DB owners; cancellation/unknown outcome
    /// keeps SchedulerPass's rebuild flag without sending under DB capacity.
    pub(crate) async fn track_task_mutation<T>(
        &self,
        task_id: &str,
        operation: impl std::future::Future<Output = TaskRuntimeResult<T>>,
    ) -> TaskRuntimeResult<T> {
        let change = self.work.changing();
        self.changes.record(task_id);
        let mut pass = SchedulerPass {
            changes: &self.changes,
            due_runs: None,
            complete: false,
        };
        let outcome = operation.await;
        // A concurrent refresh may have consumed the pre-write revision. Keep
        // a fresh addressed observation even when no new event was appended.
        self.changes.record(task_id);
        pass.complete = outcome.is_ok();
        drop(pass);
        if outcome.is_ok() {
            change.finish(true);
        }
        // Error/cancellation drops the guard as Unknown, including outcomes
        // whose COMMIT may still be settling on the serialized connection.
        outcome
    }
}

#[derive(Default)]
pub(crate) struct SchedulerState {
    #[cfg(test)]
    before_plan: std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    local_rebuild: std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>,
    rebuild: Mutex<Option<TriggerRebuild>>,
    continuation: std::sync::atomic::AtomicBool,
    due_runs: std::sync::Mutex<DueRunPass>,
    // Earliest clock used by this complete Runs + Triggers traversal. A
    // deadline crossing does not advance producer generation.
    due_cutoff: std::sync::Mutex<Option<i64>>,
    due_trigger_upper: Mutex<Option<(chrono::DateTime<chrono::FixedOffset>, String)>>,
    due_trigger_cursor: Mutex<Option<(chrono::DateTime<chrono::FixedOffset>, String)>>,
}

// The drain serializes foreground and owner quanta. Keep the input fence with
// the retained frontier, rather than with the long-lived owner's stack.
#[derive(Default)]
struct DueRunPass {
    after: Option<String>,
    upper: Option<String>,
    observation: Option<pioneer_sqlite::background::Observation>,
    rescan_requested: bool,
}

struct TriggerRebuild {
    after: Option<String>,
    upper: Option<String>,
    generation: u64,
    observed_at: i64,
    seen: std::collections::HashSet<String>,
}

pub struct TaskScheduler {
    store: Arc<CrudStore>,
    event_bus: Arc<TaskEventBus>,
    executors: Arc<TaskExecutorRegistry>,
    work: Arc<pioneer_sqlite::background::Wake>,
    state: Arc<SchedulerState>,
    changes: Arc<pioneer_crud::TaskSchedulerChanges>,
    #[cfg(test)]
    pub(super) parked: Notify,
    #[cfg(test)]
    pub(super) future_observed: Notify,
}

/// Cancellation or an error after possible mutation makes the next pass
/// reconstruct durable state; only a completed pass may disarm this guard.
// A foreground quantum may leave a quiet finite reconstruction tail. Return
// that obligation only after its admission and state restoration unwind.
struct ForegroundSchedulerHandoff<'a>(&'a pioneer_sqlite::background::Wake);
impl Drop for ForegroundSchedulerHandoff<'_> {
    fn drop(&mut self) {
        self.0.invalidate();
    }
}

struct SchedulerPass<'a> {
    changes: &'a pioneer_crud::TaskSchedulerChanges,
    due_runs: Option<&'a std::sync::Mutex<DueRunPass>>,
    complete: bool,
}
impl Drop for SchedulerPass<'_> {
    fn drop(&mut self) {
        if !self.complete {
            if let Some(runs) = self.due_runs {
                runs.lock().unwrap().rescan_requested = true;
            }
            self.changes.reconstruct();
        }
    }
}

impl TaskScheduler {
    pub fn new(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        executors: Arc<TaskExecutorRegistry>,
    ) -> Self {
        let (work, state, changes) = event_bus.bind_scheduler(&store);
        changes.reconstruct();
        work.invalidate();
        Self {
            store: Arc::new(store.with_maintenance_access()),
            event_bus,
            executors,
            work,
            state,
            changes,
            #[cfg(test)]
            parked: Default::default(),
            #[cfg(test)]
            future_observed: Default::default(),
        }
    }

    pub fn handle(&self) -> TaskSchedulerHandle {
        TaskSchedulerHandle {
            changes: self.changes.clone(),
            work: self.work.clone(),
        }
    }

    pub fn wake(&self) {
        self.work.invalidate();
    }

    pub async fn run(self: Arc<Self>) {
        use pioneer_sqlite::background::{Plan, Ready};
        let Some(mut owner) = self.work.acquire() else {
            return;
        };
        let mut after_sweep = None;
        loop {
            let ready = owner.ready().await;
            if ready == Ready::Closed {
                return;
            }
            let observation = owner.observe();
            let Some(_drain) = self.work.drain().await else {
                return;
            };
            if ready == Ready::SettlementRequired {
                let settle_observation = owner.observe();
                after_sweep = None;
                self.changes.reconstruct();
                if self
                    .store
                    .database_connection()
                    .settle_background_change(&owner)
                    .await
                    .is_err()
                {
                    warn!(
                        failure_class = "task_scheduler_settlement_failure",
                        "task scheduler change settlement deferred"
                    );
                    owner.retry_after(
                        settle_observation,
                        Duration::from_secs(TASK_SCHEDULER_STORAGE_BACKOFF_SECONDS),
                    );
                }
                continue;
            }
            if let Some((observed, plan)) = after_sweep {
                let deadline_reached =
                    matches!(plan, Plan::WallDeadline(at) if at <= self.work.wall_now());
                if owner.observe() == observed && !self.changes.pending() && !deadline_reached {
                    self.changes.sweep_trigger_observations();
                    if !self.changes.sweeping() {
                        after_sweep = None;
                        let settled = owner.publish(observed, plan);
                        #[cfg(test)]
                        if settled {
                            match plan {
                                Plan::Dormant => self.parked.notify_one(),
                                Plan::WallDeadline(_) => self.future_observed.notify_one(),
                                _ => {}
                            }
                        }
                        let _ = settled;
                    }
                    tokio::task::yield_now().await;
                    continue;
                }
                after_sweep = None;
            }
            let now = self
                .work
                .wall_now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .min(i64::MAX as u64) as i64;
            let processing = owner
                .scope(self.process_due_once_with_transient_storage_retry(now))
                .await;
            #[cfg(test)]
            {
                let barrier = self.state.before_plan.lock().unwrap().take();
                if let Some(barrier) = barrier {
                    barrier.wait().await;
                    barrier.wait().await;
                }
            }
            match processing {
                Ok(_) if !owner.requires_settlement() => match self.next_work_plan(now).await {
                    Ok(plan) => {
                        // Cleanup of old arming observations is bounded memory
                        // work. Reuse this observed wait plan while sweeping;
                        // do not repeat domain SQL for each memory page. A new
                        // generation or a reached deadline interrupts the sweep.
                        let sweeping = self.changes.sweeping() && !matches!(plan, Plan::Runnable);
                        let settled = owner
                            .publish(observation, if sweeping { Plan::Runnable } else { plan });
                        if settled && sweeping {
                            after_sweep = Some((observation, plan));
                        }
                        #[cfg(test)]
                        if settled && !sweeping {
                            match plan {
                                Plan::Dormant => self.parked.notify_one(),
                                Plan::WallDeadline(_) => self.future_observed.notify_one(),
                                _ => {}
                            }
                        }
                        let _ = settled;
                    }
                    Err(error) => {
                        warn!(
                            failure_class = "task_scheduler_deadline_failed",
                            "task scheduler deadline observation deferred"
                        );
                        owner.retry_after(
                            observation,
                            if is_transient_scheduler_storage_error(&error) {
                                Duration::from_secs(TASK_SCHEDULER_STORAGE_BACKOFF_SECONDS)
                            } else {
                                Duration::from_secs(5)
                            },
                        );
                    }
                },
                Ok(_) => {
                    owner.retry_after(
                        observation,
                        Duration::from_secs(TASK_SCHEDULER_STORAGE_BACKOFF_SECONDS),
                    );
                }
                Err(error) => {
                    let delay = if is_transient_scheduler_storage_error(&error) {
                        warn!(
                            failure_class = "task_scheduler_transient_storage_failure",
                            "task scheduler due processing deferred by transient storage access failure"
                        );
                        Duration::from_secs(TASK_SCHEDULER_STORAGE_BACKOFF_SECONDS)
                    } else {
                        error!(
                            failure_class = "task_scheduler_due_processing_failed",
                            "task scheduler due processing failed"
                        );
                        // A malformed due row can retain a past cache clock.
                        // That is retry debt, not a zero-duration continuation.
                        match self.store.next_task_scheduler_deadline(now).await {
                            Ok(Some(at)) => std::time::SystemTime::from(at)
                                .duration_since(std::time::SystemTime::now())
                                .unwrap_or(Duration::from_secs(5))
                                .min(Duration::from_secs(5)),
                            _ => Duration::from_secs(5),
                        }
                    };
                    owner.retry_after(observation, delay);
                }
            }
        }
    }

    async fn process_due_once_with_transient_storage_retry(
        &self,
        now: i64,
    ) -> TaskRuntimeResult<usize> {
        retry_with_backoff(
            || self.process_due_admitted(now),
            is_transient_scheduler_storage_error,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn process_due_once(&self, now: i64) -> TaskRuntimeResult<usize> {
        let _handoff = ForegroundSchedulerHandoff(&self.work);
        let Some(_drain) = self.work.drain().await else {
            return Ok(0);
        };
        self.process_due_admitted(now).await
    }

    // Owner retains admission through next_work_plan/publication; foreground
    // retains it through restoration and then hands work back without SQL.
    async fn process_due_admitted(&self, now: i64) -> TaskRuntimeResult<usize> {
        // Capture before consuming semantic hints/release bits or doing any
        // awaited work. Refreshing triggers does not revalidate queued runs.
        let observed = self.work.observe();
        {
            let mut cutoff = self.state.due_cutoff.lock().unwrap();
            // A rollback can exclude an obligation from a later page too.
            // Keep the earliest comparison boundary across both finite lanes.
            *cutoff = Some(cutoff.map_or(now, |old| old.min(now)));
        }
        let released = self.changes.take_releases();
        {
            let mut runs = self.state.due_runs.lock().unwrap();
            if runs.after.is_some()
                && (released
                    || runs
                        .observation
                        .is_some_and(|prior| self.work.producer_changed_since(prior)))
            {
                runs.rescan_requested = true;
            }
            runs.observation = Some(observed);
        }
        self.state
            .continuation
            .store(false, std::sync::atomic::Ordering::Release);
        let mut pass = SchedulerPass {
            changes: &self.changes,
            due_runs: Some(&self.state.due_runs),
            complete: false,
        };
        // Recover a stop committed before queue disposal, without waiting for
        // a retry's old ready_at. One bounded disposal quantum per Task keeps
        // transactions small; any remaining queue is rediscovered next pass.
        let finish = self
            .store
            .list_task_finish_queue_candidates(TASK_FINISH_QUEUE_TASK_LIMIT)
            .await?;
        if finish.len() == TASK_FINISH_QUEUE_TASK_LIMIT as usize {
            self.state
                .continuation
                .store(true, std::sync::atomic::Ordering::Release);
        }
        for task_id in finish {
            self.mark_task_dirty(&task_id).await;
            let events = self
                .store
                .discard_finished_task_queue(&task_id, now)
                .await?;
            if events.len() >= 16 {
                self.state
                    .continuation
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            self.event_bus.publish_many(events).await;
        }
        let mut row_failures = self.refresh_inputs(now).await?;
        let after = self.state.due_trigger_cursor.lock().await.clone();
        let upper = self.state.due_trigger_upper.lock().await.clone();
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
            let mut cursor = self.state.due_trigger_cursor.lock().await;
            let mut upper_state = self.state.due_trigger_upper.lock().await;
            *cursor = if more { due.last.clone() } else { None };
            *upper_state = if more { upper } else { None };
        }
        for failure in &due.failures {
            self.mark_task_dirty(&failure.task_id).await;
        }
        row_failures.extend(due.failures);
        let mut created = 0usize;
        let mut failure = None;
        for trigger in due.records {
            // Mark before attempting a write: a failed/unknown commit is not empty.
            self.mark_task_dirty(&trigger.task_id).await;
            let is_armed = self.changes.armed(&trigger);
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
        let (after_id, upper) = {
            let runs = self.state.due_runs.lock().unwrap();
            (runs.after.clone(), runs.upper.clone())
        };
        let (runs, upper) = self
            .store
            .list_scheduler_due_task_runs_finite(now, after_id.as_deref(), upper.as_deref(), 1024)
            .await?;
        let more = runs.scanned == 1024 && runs.last.as_ref() != upper.as_ref();
        // Publish the captured pair together before row processing, preserving
        // healthy-tail progress when a point fails or this quantum is dropped.
        {
            let mut pass = self.state.due_runs.lock().unwrap();
            pass.after = if more { runs.last.clone() } else { None };
            pass.upper = if more { upper } else { None };
        }
        for failure in &runs.failures {
            self.mark_task_dirty(&failure.task_id).await;
        }
        row_failures.extend(runs.failures);
        for run in runs.records {
            // Visiting an unchanged blocked run is not a committed input.
            // Lower mutation guards retain revisions/Unknown; SchedulerPass
            // reconstructs on error or cancellation. Do not replenish the
            // semantic locator set merely because this due row was observed.
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
        let released = self.changes.take_releases();
        {
            let mut pass = self.state.due_runs.lock().unwrap();
            // A producer may arrive during this page, including the final
            // refresh which ACKs its task locator. Keep that debt across EOF.
            // A committed own completion can release a previously visited run
            // without advancing the external generation. Consume the semantic
            // release into this same debt; blocked/no-op visits set neither.
            pass.rescan_requested |= released || self.work.producer_changed_since(observed);
            if !more && std::mem::take(&mut pass.rescan_requested) {
                self.state
                    .continuation
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        if let Some(error) = failure {
            return Err(error.context(format!("discovery row failures: {:?}", row_failures)));
        }
        pass.complete = true;
        if !row_failures.is_empty() {
            self.state.due_runs.lock().unwrap().rescan_requested = true;
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
            changes: &self.changes,
            due_runs: None,
            complete: false,
        };
        let task_id = run.task_id.clone();
        self.mark_task_dirty(&task_id).await;
        self.wake();
        let outcome = self.process_queued_run(store, run).await?;
        self.mark_task_dirty(&task_id).await;
        self.wake();
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
        self.changes.record(task_id);
    }

    /// Committed producers retain a bounded coalesced set before publication.
    /// Overflow requests durable reconstruction; business event subscriptions
    /// remain independent of this readiness path.
    async fn refresh_inputs(
        &self,
        now: i64,
    ) -> TaskRuntimeResult<Vec<pioneer_crud::TaskSchedulerRowFailure>> {
        let mut failures = Vec::new();
        self.changes.sweep_trigger_observations();
        // Publication coalesces task locators before reaching this owner; the
        // business broadcast is independent and is not replayed for readiness.
        // Consume the reconstruction flag BEFORE await: a concurrent producer
        // can set it again and cannot be erased by this older rebuild.
        let reconstruct = self.changes.take_reconstruction();
        let rebuilding = self.state.rebuild.lock().await.is_some();
        if reconstruct {
            *self.state.rebuild.lock().await = None;
        }
        if reconstruct || rebuilding {
            failures.extend(self.rebuild_trigger_observations(now).await?);
        }
        // Keep each locator until its reload succeeds, including cancellation.
        let tasks = self.changes.page();
        for (task_id, revision) in tasks {
            let snapshot = self.changes.generation();
            let triggers = match self.store.task_scheduler_triggers_for_task(&task_id).await {
                Ok(inputs) => inputs,
                Err(error) => {
                    self.changes.reconstruct();
                    failures.push(pioneer_crud::TaskSchedulerRowFailure {
                        id: task_id.clone(),
                        task_id: task_id.clone(),
                        error,
                    });
                    continue;
                }
            };
            let incomplete = !triggers.failures.is_empty();
            let complete_page = triggers.scanned < 64;
            let bad_ids = triggers
                .failures
                .iter()
                .map(|row| row.id.clone())
                .collect::<std::collections::HashSet<_>>();
            failures.extend(triggers.failures);
            let mut seen = bad_ids;
            for trigger in triggers.records {
                seen.insert(trigger.id.clone());
                self.changes.observe_trigger(&trigger, snapshot, now);
            }
            if complete_page {
                self.changes
                    .finish_task_trigger_observation(&task_id, &seen, snapshot);
            }
            if !incomplete {
                self.changes.acknowledge(&task_id, revision);
            }
        }
        Ok(failures)
    }

    async fn rebuild_trigger_observations(
        &self,
        now: i64,
    ) -> TaskRuntimeResult<Vec<pioneer_crud::TaskSchedulerRowFailure>> {
        let mut scan = self
            .state
            .rebuild
            .lock()
            .await
            .take()
            .unwrap_or_else(|| TriggerRebuild {
                after: None,
                upper: None,
                generation: self.changes.begin_trigger_reconstruction(now),
                observed_at: now,
                seen: Default::default(),
            });
        #[cfg(test)]
        {
            let barrier = self.state.local_rebuild.lock().unwrap().take();
            if let Some(barrier) = barrier {
                barrier.wait().await;
                barrier.wait().await;
            }
        }
        let (page, upper) = self
            .store
            .task_scheduler_trigger_page(scan.after.as_deref(), scan.upper.as_deref(), 64)
            .await?;
        let more = page.scanned == 64 && page.last.as_ref() != upper.as_ref();
        scan.seen
            .extend(page.records.iter().map(|trigger| trigger.id.clone()));
        scan.seen
            .extend(page.failures.iter().map(|row| row.id.clone()));
        for row in &page.failures {
            self.mark_task_dirty(&row.task_id).await;
        }
        for trigger in page.records {
            self.changes
                .observe_trigger(&trigger, scan.generation, scan.observed_at);
        }
        if more {
            scan.after = page.last;
            scan.upper = upper;
            *self.state.rebuild.lock().await = Some(scan);
        } else {
            self.changes
                .finish_trigger_reconstruction(scan.seen, scan.generation);
        }
        Ok(page.failures)
    }

    pub(crate) async fn next_work_plan(
        &self,
        now: i64,
    ) -> TaskRuntimeResult<pioneer_sqlite::background::Plan> {
        use pioneer_sqlite::background::Plan;
        let rebuilding = self.state.rebuild.lock().await.is_some();
        let runs = {
            let pass = self.state.due_runs.lock().unwrap();
            pass.after.is_some() || pass.rescan_requested
        };
        let triggers = self.state.due_trigger_cursor.lock().await.is_some();
        if self
            .state
            .continuation
            .load(std::sync::atomic::Ordering::Acquire)
            || self.changes.pending()
            || rebuilding
            || runs
            || triggers
        {
            return Ok(Plan::Runnable);
        }
        let cutoff = self
            .state
            .due_cutoff
            .lock()
            .unwrap()
            .unwrap_or(now)
            .min(now);
        let next = self.store.next_task_scheduler_deadline(cutoff).await?;
        // Only a complete successful observation consumes the captured clock.
        // A deadline that crossed behind either frontier remains a wall wait
        // (already due => one fresh finite pass), whose next cutoff is current.
        // Errors/cancellation above retain it; shared drain covers publication.
        *self.state.due_cutoff.lock().unwrap() = None;
        Ok(next.map_or(Plan::Dormant, |at| Plan::WallDeadline(at.into())))
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
        // Sample the same wall source used for scheduler eligibility. The
        // transactional claim still revalidates ReadyAt at this fresh instant.
        let claimed_at = self
            .work
            .wall_now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .min(i64::MAX as u64) as i64;
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

fn is_transient_scheduler_storage_error(error: &anyhow::Error) -> bool {
    is_anyhow_sqlite_transient_access(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn scheduler_treats_pool_acquire_timeout_as_transient_storage_access() {
        let error = anyhow!(
            "failed to list active task triggers: Failed to acquire connection from pool: \
             Connection pool timed out: Connection pool timed out"
        );

        assert!(is_transient_scheduler_storage_error(&error));
    }
    async fn scheduler_fixture() -> (
        tempfile::TempDir,
        Arc<CrudStore>,
        crate::TaskRuntime,
        Arc<crate::tests::DispatchDatabaseObserver>,
    ) {
        use crate::tests::{DispatchDatabaseObserver, seed_task_test_workspace};
        use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
        use sea_orm::{ConnectOptions, Database};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("scheduler.sqlite");
        let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
        options.max_connections(1);
        let writer = Database::connect(options).await.unwrap();
        let bootstrap = SqliteWriteExecutor::new(writer.clone());
        bootstrap
            .apply_pragmas(SqliteWriteClass::Maintenance)
            .await
            .unwrap();
        bootstrap
            .run_migrations::<migration::Migrator>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        seed_task_test_workspace(&writer).await;
        drop(bootstrap);
        let mut options =
            ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
        options.map_sqlx_sqlite_opts(|options| options.read_only(true).pragma("query_only", "ON"));
        let reader = Database::connect(options).await.unwrap();
        let observer = Arc::new(DispatchDatabaseObserver::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        let store = Arc::new(CrudStore::new(database.maintenance()));
        let runtime = crate::TaskRuntime::new(store.clone());
        (directory, store, runtime, observer)
    }
    #[tokio::test]
    async fn real_scheduler_parks_and_processes_a_registered_deadline_without_periodic_discovery() {
        use super::*;
        use crate::tests::{CompletingSystemExecutor, create_params};
        use pioneer_sqlite::SqliteReadClass;
        let (_directory, store, runtime, observer) = scheduler_fixture().await;
        let executors = Arc::new(TaskExecutorRegistry::new());
        executors.register(Arc::new(CompletingSystemExecutor)).await;
        let scheduler = Arc::new(TaskScheduler::new(
            store.clone(),
            runtime.event_bus(),
            executors,
        ));
        let worker = tokio::spawn(scheduler.clone().run());
        tokio::time::timeout(Duration::from_secs(10), scheduler.parked.notified())
            .await
            .unwrap();
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(observer.reads.lock().unwrap().is_empty());
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        let task =
            runtime
                .service()
                .create_task(
                    crate::TaskCreateContext::default(),
                    create_params(pioneer_protocol::TaskTriggerSpec::ScheduledAt {
                        scheduled_at: now_timestamp_secs() + 3,
                        timezone: None,
                        catch_up_policy: Some(
                            pioneer_protocol::TaskTriggerCatchUpPolicy::skip_missed(),
                        ),
                    }),
                )
                .await
                .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            scheduler.future_observed.notified(),
        )
        .await
        .unwrap();
        // No client, bus replay, manual process_due_once or fallback tick.
        tokio::time::timeout(Duration::from_secs(10), scheduler.parked.notified())
            .await
            .unwrap();
        let result = store.get_task(&task.task.id).await.unwrap().unwrap();
        assert_eq!(result.runs.len(), 1);
        assert_eq!(result.runs[0].status, TaskRunStatus::Succeeded);
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|class| *class == SqliteReadClass::Maintenance)
        );
        // Replaying an already committed business event still delivers it to
        // business subscribers; it is not another lower durable mutation.
        let event = store
            .get_task_events(&task.task.id, None)
            .await
            .unwrap()
            .events
            .into_iter()
            .next()
            .unwrap();
        let replay = pioneer_crud::AppendedTaskEvent {
            id: event.id.clone(),
            task_id: event.task_id,
            run_id: event.run_id,
            thread_id: event.thread_id,
            turn_id: event.turn_id,
            sequence: event.sequence,
            event_type: event.event_type,
            idempotency_key: event.idempotency_key,
            payload: event.payload,
            workspace_id: None,
            root_task_id: None,
            parent_task_id: None,
            created_at: chrono::DateTime::from_timestamp(event.created_at, 0)
                .unwrap()
                .fixed_offset(),
            append_status: pioneer_crud::TaskEventAppendStatus::Inserted,
        };
        let bus = runtime.event_bus();
        let mut subscriber = bus.subscribe(Default::default());
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        for _ in 0..1000 {
            bus.publish(replay.clone()).await;
        }
        for _ in 0..1000 {
            let crate::event_bus::TaskEventWakeDelivery::Wake(wake) = subscriber.recv().await
            else {
                panic!("a replay remains a business notification");
            };
            assert_eq!(wake.event_id, event.id);
        }
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            observer.reads.lock().unwrap().is_empty(),
            "bus replay is not a new durable scheduler input"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        for _ in 0..1000 {
            scheduler.wake();
        }
        tokio::time::timeout(Duration::from_secs(10), scheduler.parked.notified())
            .await
            .unwrap();
        assert!(
            observer.writes.lock().unwrap().is_empty(),
            "coalesced hints do not manufacture claims"
        );
        observer.reads.lock().unwrap().clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            observer.reads.lock().unwrap().is_empty(),
            "completed history stays dormant"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        worker.abort();
        let _ = worker.await;
    }
    #[tokio::test]
    async fn raw_trigger_history_advances_empty_match_pages_and_old_key_schedule_fires() {
        use crate::tests::{CompletingSystemExecutor, create_params};
        use pioneer_protocol::{TaskTriggerCatchUpPolicy, TaskTriggerSpec, TaskTriggerStatus};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
        for all_excluded in [false, true] {
            let (_directory, store, runtime, observer) = scheduler_fixture().await;
            let now = now_timestamp_secs();
            let mut tasks = Vec::new();
            for _ in 0..129 {
                tasks.push(
                    runtime
                        .service()
                        .create_task(
                            crate::TaskCreateContext::default(),
                            create_params(TaskTriggerSpec::ScheduledAt {
                                scheduled_at: now + 7200,
                                timezone: None,
                                catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
                            }),
                        )
                        .await
                        .unwrap(),
                );
            }
            tasks.sort_by(|a, b| a.trigger.id.cmp(&b.trigger.id));
            // Restored history fixture, before worker admission. Its first two
            // raw pages have zero active matches, followed by a future tail.
            let db = store.database_connection();
            for (index, task) in tasks.iter().enumerate() {
                if index < 128 || all_excluded {
                    pioneer_entity::task_trigger::Entity::update_many()
                        .filter(
                            pioneer_entity::task_trigger::Column::Id.eq(task.trigger.id.clone()),
                        )
                        .col_expr(
                            pioneer_entity::task_trigger::Column::Status,
                            sea_orm::sea_query::Expr::value(if index % 2 == 0 {
                                "exhausted"
                            } else {
                                "paused"
                            }),
                        )
                        .col_expr(
                            pioneer_entity::task_trigger::Column::NextFireAt,
                            sea_orm::sea_query::Expr::value(
                                None::<chrono::DateTime<chrono::FixedOffset>>,
                            ),
                        )
                        .exec(&db)
                        .await
                        .unwrap();
                }
            }
            let (first, upper) = store
                .task_scheduler_trigger_page(None, None, 64)
                .await
                .unwrap();
            assert_eq!(first.scanned, 64);
            assert!(first.records.is_empty() && first.failures.is_empty());
            assert_eq!(first.last.as_deref(), Some(tasks[63].trigger.id.as_str()));
            let (second, _) = store
                .task_scheduler_trigger_page(first.last.as_deref(), upper.as_deref(), 64)
                .await
                .unwrap();
            assert_eq!(second.scanned, 64);
            assert!(second.records.is_empty());
            let (tail, _) = store
                .task_scheduler_trigger_page(second.last.as_deref(), upper.as_deref(), 64)
                .await
                .unwrap();
            assert_eq!(tail.scanned, 1);
            assert_eq!(tail.records.len(), usize::from(!all_excluded));
            assert_eq!(tail.last, upper);
            let executors = Arc::new(TaskExecutorRegistry::new());
            executors.register(Arc::new(CompletingSystemExecutor)).await;
            let scheduler = Arc::new(TaskScheduler::new(
                store.clone(),
                runtime.event_bus(),
                executors,
            ));
            scheduler
                .work
                .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
            let worker = tokio::spawn(scheduler.clone().run());
            if all_excluded {
                tokio::time::timeout(Duration::from_secs(10), scheduler.parked.notified())
                    .await
                    .unwrap();
            } else {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    scheduler.future_observed.notified(),
                )
                .await
                .unwrap();
            }
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            tokio::time::pause();
            let hold = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert!(observer.reads.lock().unwrap().is_empty());
            assert!(observer.writes.lock().unwrap().is_empty());
            // A supported old-key reschedule must replace the retained EOF and
            // preserve its committed arming marker, without a manual owner kick.
            let mut trigger = tasks[0].trigger.clone();
            trigger.status = TaskTriggerStatus::Active;
            trigger.next_fire_at = Some(now + 30);
            trigger.spec = TaskTriggerSpec::ScheduledAt {
                scheduled_at: now + 30,
                timezone: None,
                catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
            };
            runtime
                .service()
                .append_event(
                    TaskEventPayload::TaskRescheduled {
                        task_id: tasks[0].task.id.clone(),
                        trigger,
                        rescheduled_at: now,
                        reason: TaskRescheduleReason::UserRequested,
                    },
                    now,
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if matches!(scheduler.next_work_plan(now).await.unwrap(),
                        pioneer_sqlite::background::Plan::WallDeadline(at)
                        if at == std::time::UNIX_EPOCH + Duration::from_secs((now + 30) as u64))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("committed old-key deadline must replace the former arm");
            scheduler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH + Duration::from_secs((now + 31) as u64),
            );
            tokio::time::advance(Duration::from_secs(30)).await;
            for _ in 0..512 {
                if store
                    .get_task(&tasks[0].task.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .runs
                    .iter()
                    .any(|run| run.status == TaskRunStatus::Succeeded)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            let saved = store.get_task(&tasks[0].task.id).await.unwrap().unwrap();
            assert!(
                saved
                    .runs
                    .iter()
                    .any(|run| run.status == TaskRunStatus::Succeeded),
                "rescheduled task must complete: {saved:?}; plan={:?}",
                scheduler.next_work_plan(now + 31).await
            );
            worker.abort();
            let _ = worker.await;
            hold.abort();
            tokio::time::resume();
        }
    }
    #[tokio::test]
    async fn foreground_reconstruction_handoff_finishes_unseen_future_trigger_pages() {
        use super::*;
        use crate::tests::{CompletingSystemExecutor, create_params, runtime};
        use pioneer_protocol::{TaskTriggerCatchUpPolicy, TaskTriggerSpec};
        for cancel_foreground in [false, true] {
            let runtime = runtime().await;
            let store = runtime.service().store().clone();
            let now = now_timestamp_secs();
            let due = now + 3600;
            let mut tasks = Vec::new();
            for _ in 0..257 {
                tasks.push(
                    runtime
                        .service()
                        .create_task(
                            crate::TaskCreateContext::default(),
                            create_params(TaskTriggerSpec::ScheduledAt {
                                scheduled_at: due + 3600,
                                timezone: None,
                                catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
                            }),
                        )
                        .await
                        .unwrap(),
                );
            }
            tasks.sort_by(|a, b| a.trigger.id.cmp(&b.trigger.id));
            let mut target = tasks.pop().unwrap();
            target.trigger.spec = TaskTriggerSpec::ScheduledAt {
                scheduled_at: due,
                timezone: None,
                catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
            };
            target.trigger.next_fire_at = Some(due);
            runtime
                .service()
                .append_event(
                    TaskEventPayload::TaskRescheduled {
                        task_id: target.task.id.clone(),
                        trigger: target.trigger,
                        rescheduled_at: now,
                        reason: TaskRescheduleReason::UserRequested,
                    },
                    now,
                )
                .await
                .unwrap();
            let executors = Arc::new(TaskExecutorRegistry::new());
            executors.register(Arc::new(CompletingSystemExecutor)).await;
            let scheduler = Arc::new(TaskScheduler::new(
                store.clone(),
                runtime.event_bus(),
                executors,
            ));
            // Model restored durable inputs without process-local producer locators.
            // These ACKs grant no database claim/dispatch authority.
            loop {
                let page = scheduler.changes.page();
                if page.is_empty() {
                    break;
                }
                for (task, revision) in page {
                    scheduler.changes.acknowledge(&task, revision);
                }
            }
            let generation = scheduler.changes.begin_trigger_reconstruction(now);
            scheduler
                .changes
                .finish_trigger_reconstruction(Default::default(), generation);
            while scheduler.changes.sweeping() {
                scheduler.changes.sweep_trigger_observations();
            }
            let durable = store.get_task(&target.task.id).await.unwrap().unwrap();
            assert!(
                !scheduler.changes.armed(&durable.triggers[0]),
                "restored input has no process-local arming"
            );
            scheduler
                .work
                .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
            let page = Arc::new(tokio::sync::Barrier::new(2));
            *scheduler.state.before_plan.lock().unwrap() = Some(page.clone());
            let worker = tokio::spawn(scheduler.clone().run());
            page.wait().await;
            let local = Arc::new(tokio::sync::Barrier::new(2));
            *scheduler.state.local_rebuild.lock().unwrap() = Some(local.clone());
            let foreground_scheduler = scheduler.clone();
            let mut foreground =
                Box::pin(async move { foreground_scheduler.process_due_once(now).await });
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(foreground.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            assert!(
                scheduler.state.rebuild.lock().await.is_some(),
                "foreground queues before owner plan"
            );
            page.wait().await;
            let foreground = tokio::spawn(foreground);
            local.wait().await;
            assert!(
                scheduler.state.rebuild.lock().await.is_none(),
                "real scan temporarily local"
            );
            if cancel_foreground {
                foreground.abort();
                let _ = foreground.await;
            } else {
                local.wait().await;
                foreground.await.unwrap().unwrap();
            }
            tokio::time::timeout(
                Duration::from_secs(10),
                scheduler.future_observed.notified(),
            )
            .await
            .unwrap();
            assert!(
                scheduler.state.rebuild.lock().await.is_none(),
                "all finite pages reconstructed before wait"
            );
            tokio::time::pause();
            let hold = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            // A normal late wake must retain initial arming, not skip the unseen fire.
            scheduler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH + Duration::from_secs((due + 1) as u64),
            );
            tokio::time::advance(Duration::from_secs(30)).await;
            for _ in 0..256 {
                if !store
                    .get_task(&target.task.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .runs
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            let result = store.get_task(&target.task.id).await.unwrap().unwrap();
            assert_eq!(result.runs.len(), 1);
            // Complete the actual dispatch before observing outcome.
            for _ in 0..256 {
                if store.get_task(&target.task.id).await.unwrap().unwrap().runs[0].status
                    == TaskRunStatus::Succeeded
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                store.get_task(&target.task.id).await.unwrap().unwrap().runs[0].status,
                TaskRunStatus::Succeeded
            );
            worker.abort();
            let _ = worker.await;
            hold.abort();
            tokio::time::resume();
        }
    }
    // Extend the real scheduler owner matrix at the ORM (rather than JSON)
    // boundary, with healthy witnesses in and beyond the same1024-key page.
    #[tokio::test]
    async fn typed_finite_pages_preserve_dispatch_tail_and_retry_without_producer() {
        use crate::tests::{CompletingSystemExecutor, create_params};
        use pioneer_entity::{task_run as run_row, task_trigger as trigger_row};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
        for trigger_fault in [false, true] {
            let (_directory, store, runtime, observer) = scheduler_fixture().await;
            let at = now_timestamp_secs() + 3600;
            let mut inputs = Vec::new();
            for _ in 0..1025 {
                let spec = if trigger_fault {
                    pioneer_protocol::TaskTriggerSpec::ScheduledAt {
                        scheduled_at: at,
                        timezone: None,
                        catch_up_policy: None,
                    }
                } else {
                    pioneer_protocol::TaskTriggerSpec::Immediate
                };
                let response = runtime
                    .service()
                    .create_task(crate::TaskCreateContext::default(), create_params(spec))
                    .await
                    .unwrap();
                let key = if trigger_fault {
                    response.trigger.id
                } else {
                    response.run.unwrap().id
                };
                inputs.push((response.task.id, key));
            }
            inputs.sort_by(|a, b| a.1.cmp(&b.1));
            let db = store.database_connection();
            let original = if trigger_fault {
                let row = trigger_row::Entity::find_by_id(&inputs[0].1)
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
                trigger_row::Entity::update_many()
                    .filter(trigger_row::Column::Id.eq(&row.id))
                    .col_expr(
                        trigger_row::Column::UpdatedAt,
                        Expr::value("malformed-date"),
                    )
                    .exec(&db)
                    .await
                    .unwrap();
                row.updated_at
            } else {
                let row = run_row::Entity::find_by_id(&inputs[0].1)
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
                run_row::Entity::update_many()
                    .filter(run_row::Column::Id.eq(&row.id))
                    .col_expr(run_row::Column::UpdatedAt, Expr::value("malformed-date"))
                    .exec(&db)
                    .await
                    .unwrap();
                row.updated_at
            };
            runtime
                .register_executor(Arc::new(CompletingSystemExecutor))
                .await;
            let scheduler = runtime.scheduler_for_test();
            scheduler
                .work
                .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(at as u64));
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            *scheduler.state.before_plan.lock().unwrap() = Some(barrier.clone());
            let worker = tokio::spawn(scheduler.clone().run());
            let ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            let mut reached = false;
            for _ in 0..40 {
                tokio::time::timeout(Duration::from_secs(60), barrier.wait())
                    .await
                    .unwrap();
                let tail = store
                    .get_task(&inputs.last().unwrap().0)
                    .await
                    .unwrap()
                    .unwrap();
                if tail
                    .runs
                    .iter()
                    .any(|r| r.status == TaskRunStatus::Succeeded)
                {
                    reached = true;
                    break;
                }
                *scheduler.state.before_plan.lock().unwrap() = Some(barrier.clone());
                barrier.wait().await;
                {
                    let admission = scheduler.work.drain().await.unwrap();
                    drop(admission);
                }
                tokio::time::pause();
                tokio::time::advance(Duration::from_secs(60)).await;
                tokio::time::resume();
            }
            assert!(
                reached,
                "healthy tail beyond1024 reaches dispatch before poison repair"
            );
            assert!(
                store
                    .get_task(&inputs[1].0)
                    .await
                    .unwrap()
                    .unwrap()
                    .runs
                    .iter()
                    .any(|r| r.status == TaskRunStatus::Succeeded),
                "same-page healthy dispatch"
            );
            assert!(
                scheduler.state.due_runs.lock().unwrap().rescan_requested
                    || scheduler
                        .state
                        .continuation
                        .load(std::sync::atomic::Ordering::Acquire),
                "failed row retains rescan/retry debt at EOF"
            );
            let bad = if trigger_fault {
                store
                    .task_scheduler_trigger_partial(&inputs[0].1)
                    .await
                    .map(|_| ())
            } else {
                store
                    .task_scheduler_run_partial(&inputs[0].1)
                    .await
                    .map(|_| ())
            };
            assert!(
                bad.is_err(),
                "typed ORM failure remains, not a JSON conversion"
            );
            // A further real quantum with unchanged inputs proves the retained
            // timed retry before repairing anything. No producer supplies it.
            *scheduler.state.before_plan.lock().unwrap() = Some(barrier.clone());
            barrier.wait().await;
            {
                let admission = scheduler.work.drain().await.unwrap();
                drop(admission);
            }
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::time::resume();
            tokio::time::timeout(Duration::from_secs(60), barrier.wait())
                .await
                .unwrap();
            let before = scheduler.work.observe();
            if trigger_fault {
                trigger_row::Entity::update_many()
                    .filter(trigger_row::Column::Id.eq(&inputs[0].1))
                    .col_expr(trigger_row::Column::UpdatedAt, Expr::value(original))
                    .exec(&db)
                    .await
                    .unwrap();
            } else {
                run_row::Entity::update_many()
                    .filter(run_row::Column::Id.eq(&inputs[0].1))
                    .col_expr(run_row::Column::UpdatedAt, Expr::value(original))
                    .exec(&db)
                    .await
                    .unwrap();
            }
            assert_eq!(
                scheduler.work.observe(),
                before,
                "repair emits no producer hint"
            );
            barrier.wait().await;
            {
                let admission = scheduler.work.drain().await.unwrap();
                drop(admission);
            }
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::time::resume();
            tokio::time::timeout(Duration::from_secs(60), scheduler.parked.notified())
                .await
                .unwrap();
            assert!(
                store
                    .get_task(&inputs[0].0)
                    .await
                    .unwrap()
                    .unwrap()
                    .runs
                    .iter()
                    .any(|r| r.status == TaskRunStatus::Succeeded),
                "repaired input reaches durable dispatch without wake"
            );
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(observer.reads.lock().unwrap().is_empty());
            assert!(observer.writes.lock().unwrap().is_empty());
            tokio::time::resume();
            ticker.abort();
            worker.abort();
            let _ = worker.await;
            runtime.shutdown().await;
        }
    }

    #[tokio::test]
    async fn due_blocked_population_settles_then_registration_drains_all_tasks() {
        use crate::tests::{CompletingSystemExecutor, create_params};
        // A clock crossing behind a retained PK frontier is an obligation
        // even when no producer commits and no sibling live owner exists.
        for jump_seconds in [1_i64, 3600] {
            use pioneer_entity::task_run as run_row;
            use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
            let (_directory, store, runtime, observer) = scheduler_fixture().await;
            // Readiness and writer-owned claim revalidation share the same
            // wall source. A is genuinely future to its first sample.
            let now = now_timestamp_secs() - 7200;
            let due = now + 1;
            let mut tail = Vec::new();
            for _ in 0..1025 {
                let mut params = create_params(pioneer_protocol::TaskTriggerSpec::Immediate);
                params.executor_kind = TaskExecutorKind::Tool;
                let task = runtime
                    .service()
                    .create_task(crate::TaskCreateContext::default(), params)
                    .await
                    .unwrap();
                tail.push(task.run.unwrap().id);
            }
            tail.sort();
            let mut selected = None;
            for _ in 0..128 {
                let task = runtime
                    .service()
                    .create_task(
                        crate::TaskCreateContext::default(),
                        create_params(pioneer_protocol::TaskTriggerSpec::Immediate),
                    )
                    .await
                    .unwrap();
                if task.run.as_ref().unwrap().id < tail[1023] {
                    selected = Some(task);
                    break;
                }
                runtime
                    .service()
                    .cancel_task(
                        crate::TaskMutationContext::default(),
                        pioneer_protocol::TaskCancelParams {
                            task_id: task.task.id,
                            reason: Some("select generated early clock fixture".into()),
                            scope: pioneer_protocol::TaskCancelScope::TaskOnly,
                        },
                    )
                    .await
                    .unwrap();
            }
            let target = selected.expect("generated early clock fixture within 128 tries");
            let a = target.run.unwrap().id;
            assert!(
                store
                    .get_task_occurrence_contract_by_run(&a)
                    .await
                    .unwrap()
                    .is_some()
            );
            let db = store.database_connection();
            // Restore eligibility clocks only, before any worker acquisition.
            // No IDs, event/contract identities, or occurrence generations change.
            run_row::Entity::update_many()
                .filter(run_row::Column::Id.is_in(tail.clone()))
                .col_expr(
                    run_row::Column::ReadyAt,
                    Expr::value(Some(pioneer_crud::unix_to_datetime(now))),
                )
                .exec(&db)
                .await
                .unwrap();
            run_row::Entity::update_many()
                .filter(run_row::Column::Id.eq(&a))
                .col_expr(
                    run_row::Column::ReadyAt,
                    Expr::value(Some(pioneer_crud::unix_to_datetime(due))),
                )
                .exec(&db)
                .await
                .unwrap();
            runtime
                .register_executor(Arc::new(CompletingSystemExecutor))
                .await;
            let scheduler = runtime.scheduler_for_test();
            scheduler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH + Duration::from_secs((now - 60) as u64),
            );
            // Settle the REAL owner before either existing clock is due. A
            // foreground warmup handoff would itself invalidate generation.
            let worker = tokio::spawn(scheduler.clone().run());
            tokio::time::timeout(
                Duration::from_secs(60),
                scheduler.future_observed.notified(),
            )
            .await
            .unwrap();
            assert!(!scheduler.changes.pending());
            let (first, upper) = store
                .list_scheduler_due_task_runs_finite(now, None, None, 1024)
                .await
                .unwrap();
            assert_eq!(first.scanned, 1024);
            assert!(first.failures.is_empty());
            assert!(!first.records.iter().any(|run| run.id == a));
            assert!(first.last.as_ref().unwrap() > &a);
            assert_ne!(first.last, upper);
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            *scheduler.state.before_plan.lock().unwrap() = Some(barrier.clone());
            // Only the already retained Tool ReadyAt opens this pass. Give
            // the SQL-free wall sampler one interval; no setup kick/producer.
            let before_start = scheduler.work.observe();
            let sampler_ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::pause();
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            scheduler
                .work
                .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
            tokio::time::advance(Duration::from_secs(30)).await;
            tokio::time::resume();
            sampler_ticker.abort();
            assert_eq!(scheduler.work.observe(), before_start);
            tokio::time::timeout(Duration::from_secs(60), barrier.wait())
                .await
                .unwrap();
            {
                let pass = scheduler.state.due_runs.lock().unwrap();
                assert!(pass.after.as_ref().is_some_and(|after| after > &a));
                assert!(
                    pass.upper.is_some(),
                    "real retained frontier, not an EOF test"
                );
            }
            assert_eq!(
                store.get_task_run(&a).await.unwrap().unwrap().status,
                TaskRunStatus::Queued
            );
            let observed = scheduler.work.observe();
            scheduler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH + Duration::from_secs((now + jump_seconds) as u64),
            );
            assert_eq!(
                scheduler.work.observe(),
                observed,
                "clock movement is no producer"
            );
            barrier.wait().await;
            // Only the existing captured tail and clock obligation may proceed.
            // No kick, write, executor registration, foreground or live recovery.
            tokio::time::timeout(Duration::from_secs(60), scheduler.parked.notified())
                .await
                .unwrap();
            assert_eq!(
                store.get_task_run(&a).await.unwrap().unwrap().status,
                TaskRunStatus::Succeeded,
                "clock jump {jump_seconds}"
            );
            for id in &tail {
                assert_eq!(
                    store.get_task_run(id).await.unwrap().unwrap().status,
                    TaskRunStatus::Queued
                );
            }
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            let ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "unchanged blocked tail parks"
            );
            assert!(observer.writes.lock().unwrap().is_empty());
            tokio::time::resume();
            ticker.abort();
            worker.abort();
            let _ = worker.await;
            runtime.shutdown().await;
        }
        // Own-origin release: A(run2) is visited before B(run1), which the
        // same scheduler quantum completes. No sibling live owner can rescue A.
        {
            use pioneer_entity::task_run as run_row;
            use pioneer_protocol::{
                TaskConcurrencyConflictPolicy, TaskConcurrencyPolicy, TaskUpdateParams,
            };
            use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
            let (_directory, store, runtime, observer) = scheduler_fixture().await;
            let now = now_timestamp_secs();
            let mut tail = Vec::new();
            for _ in 0..1025 {
                let mut params = create_params(pioneer_protocol::TaskTriggerSpec::Immediate);
                params.executor_kind = TaskExecutorKind::Tool;
                let task = runtime
                    .service()
                    .create_task(crate::TaskCreateContext::default(), params)
                    .await
                    .unwrap();
                tail.push(task.run.unwrap().id);
            }
            tail.sort();
            let anchor = now + 3600;
            let mut selected = None;
            for _ in 0..128 {
                let mut params = create_params(pioneer_protocol::TaskTriggerSpec::Interval {
                    interval_seconds: 10,
                    interval_anchor_at: Some(anchor),
                    catch_up_policy: None,
                });
                // Two genuine ordinary scheduled occurrences, without an
                // executor. Saturated max1 would deliberately deduplicate run2.
                params.concurrency_policy = Some(TaskConcurrencyPolicy {
                    key: None,
                    max_parallel_runs: 2,
                    on_conflict: TaskConcurrencyConflictPolicy::Queue,
                });
                let task = runtime
                    .service()
                    .create_task(crate::TaskCreateContext::default(), params)
                    .await
                    .unwrap();
                runtime.process_due_once(anchor).await.unwrap();
                runtime.process_due_once(anchor + 10).await.unwrap();
                let response = store.get_task(&task.task.id).await.unwrap().unwrap();
                assert_eq!(response.runs.len(), 2);
                let b = response
                    .runs
                    .iter()
                    .find(|run| run.run_number == 1)
                    .unwrap()
                    .clone();
                let a = response
                    .runs
                    .iter()
                    .find(|run| run.run_number == 2)
                    .unwrap()
                    .clone();
                assert_eq!(a.status, TaskRunStatus::Queued);
                assert_eq!(b.status, TaskRunStatus::Queued);
                assert!(
                    store
                        .get_task_occurrence_contract_by_run(&a.id)
                        .await
                        .unwrap()
                        .is_some()
                );
                assert!(
                    store
                        .get_task_occurrence_contract_by_run(&b.id)
                        .await
                        .unwrap()
                        .is_some()
                );
                if a.id < b.id && b.id < tail[1022] {
                    runtime
                        .service()
                        .update_task(
                            crate::TaskMutationContext::default(),
                            TaskUpdateParams {
                                task_id: task.task.id.clone(),
                                concurrency_policy: Some(TaskConcurrencyPolicy {
                                    key: None,
                                    max_parallel_runs: 1,
                                    on_conflict: TaskConcurrencyConflictPolicy::Queue,
                                }),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    // Only restored eligibility clocks change; service-created
                    // IDs, events, occurrence contracts and generations stay real.
                    run_row::Entity::update_many()
                        .filter(run_row::Column::Id.is_in([a.id.clone(), b.id.clone()]))
                        .col_expr(
                            run_row::Column::ReadyAt,
                            Expr::value(Some(pioneer_crud::unix_to_datetime(now - 1))),
                        )
                        .exec(&store.database_connection())
                        .await
                        .unwrap();
                    selected = Some((task.task.id, a.id, b.id));
                    break;
                }
                runtime
                    .service()
                    .cancel_task(
                        crate::TaskMutationContext::default(),
                        pioneer_protocol::TaskCancelParams {
                            task_id: task.task.id,
                            reason: Some("select generated serial fixture keys".into()),
                            scope: pioneer_protocol::TaskCancelScope::TaskOnly,
                        },
                    )
                    .await
                    .unwrap();
            }
            let (task_id, a, b) = selected.expect("generated A<B in first1024 page");
            let scheduler = runtime.scheduler_for_test();
            let worker = tokio::spawn(scheduler.clone().run());
            tokio::time::timeout(
                Duration::from_secs(60),
                scheduler.future_observed.notified(),
            )
            .await
            .unwrap();
            // Settle all setup generation/hints before registering the executor.
            // There is no scheduler due prefix yet on which setup could leave debt.
            let (first, upper) = store
                .list_scheduler_due_task_runs_finite(now_timestamp_secs(), None, None, 1024)
                .await
                .unwrap();
            assert_eq!(first.scanned, 1024);
            assert!(first.records.iter().any(|run| run.id == a));
            assert!(first.records.iter().any(|run| run.id == b));
            assert_ne!(first.last, upper);
            assert!(a < b);
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            *scheduler.state.before_plan.lock().unwrap() = Some(barrier.clone());
            runtime
                .register_executor(Arc::new(CompletingSystemExecutor))
                .await;
            let registered = scheduler.work.observe();
            tokio::time::timeout(Duration::from_secs(60), barrier.wait())
                .await
                .unwrap();
            assert_eq!(
                store.get_task_run(&b).await.unwrap().unwrap().status,
                TaskRunStatus::Succeeded
            );
            assert_eq!(
                store.get_task_run(&a).await.unwrap().unwrap().status,
                TaskRunStatus::Queued
            );
            assert!(
                !scheduler.work.producer_changed_since(registered),
                "B completion is own-origin, not another external rescue"
            );
            barrier.wait().await;
            // Only the captured tail, own release debt and real dispatch now run.
            // No clock movement, foreground processing, sibling live or new input.
            tokio::time::timeout(
                Duration::from_secs(60),
                scheduler.future_observed.notified(),
            )
            .await
            .unwrap();
            assert_eq!(
                store.get_task_run(&a).await.unwrap().unwrap().status,
                TaskRunStatus::Succeeded
            );
            let task = store.get_task(&task_id).await.unwrap().unwrap();
            assert_eq!(
                task.runs.len(),
                2,
                "the distant interval has not fired again"
            );
            assert!(task.triggers.iter().any(|trigger| {
                trigger
                    .next_fire_at
                    .is_some_and(|at| at > now_timestamp_secs() + 300)
            }));
            for id in tail {
                assert_eq!(
                    store.get_task_run(&id).await.unwrap().unwrap().status,
                    TaskRunStatus::Queued
                );
            }
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            let ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "own release verification settles before the distant trigger"
            );
            assert!(observer.writes.lock().unwrap().is_empty());
            tokio::time::resume();
            ticker.abort();
            worker.abort();
            let _ = worker.await;
            runtime.shutdown().await;
        }
        // Own scheduler boundary: the sibling live owner is never started.
        // Settle trigger reconstruction/hints BEFORE opening a new due pass;
        // otherwise that unrelated continuation could accidentally rescue A.
        {
            let (_directory, store, runtime, observer) = scheduler_fixture().await;
            let mut tail = Vec::new();
            let mut tail_run_ids = Vec::new();
            for _ in 0..1025 {
                let mut params = create_params(pioneer_protocol::TaskTriggerSpec::Immediate);
                params.executor_kind = TaskExecutorKind::Tool;
                let task = runtime
                    .service()
                    .create_task(crate::TaskCreateContext::default(), params)
                    .await
                    .unwrap();
                let run = task.run.unwrap();
                assert_eq!(run.status, TaskRunStatus::Queued);
                tail_run_ids.push(run.id);
                tail.push(task.task.id);
            }
            tail_run_ids.sort();
            // IDs remain generated by the real service. Choose a System A in
            // the first page without rewriting PK/occurrence/event authority.
            // Discard rejected setup candidates through the real cancel API.
            let mut target = None;
            for _ in 0..128 {
                let task = runtime
                    .service()
                    .create_task(
                        crate::TaskCreateContext::default(),
                        create_params(pioneer_protocol::TaskTriggerSpec::Immediate),
                    )
                    .await
                    .unwrap();
                if task.run.as_ref().unwrap().id < tail_run_ids[1023] {
                    target = Some(task);
                    break;
                }
                runtime
                    .service()
                    .cancel_task(
                        crate::TaskMutationContext::default(),
                        pioneer_protocol::TaskCancelParams {
                            task_id: task.task.id,
                            reason: Some("choose an early generated fixture key".into()),
                            scope: pioneer_protocol::TaskCancelScope::TaskOnly,
                        },
                    )
                    .await
                    .unwrap();
            }
            let target = target.expect("generated early System run for first page");
            let target_run = target.run.as_ref().unwrap().id.clone();
            let scheduler = runtime.scheduler_for_test();
            let worker = tokio::spawn(scheduler.clone().run());
            tokio::time::timeout(Duration::from_secs(30), scheduler.parked.notified())
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_task_run(&target_run)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Queued
            );
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            let ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "unchanged >1024 blocked runs park"
            );
            assert!(observer.writes.lock().unwrap().is_empty());
            tokio::time::resume();
            ticker.abort();

            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            *scheduler.state.before_plan.lock().unwrap() = Some(barrier.clone());
            // Setup wake only: the tested registration below is the final input.
            scheduler.wake();
            tokio::time::timeout(Duration::from_secs(30), barrier.wait())
                .await
                .unwrap();
            // The first page visited A while blocked; the captured Tool tail
            // still has no executor, mutations or future deadline.
            let (page, upper) = store
                .list_scheduler_due_task_runs_finite(now_timestamp_secs(), None, None, 1024)
                .await
                .unwrap();
            assert_eq!(page.scanned, 1024);
            assert!(page.records.iter().any(|run| run.id == target_run));
            assert_ne!(page.last, upper, "a captured healthy tail remains");
            assert_eq!(
                store
                    .get_task_run(&target_run)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Queued
            );
            runtime
                .register_executor(Arc::new(CompletingSystemExecutor))
                .await;
            barrier.wait().await;
            // No later kick/producer/clock, and no live reconciliation task.
            tokio::time::timeout(Duration::from_secs(30), scheduler.parked.notified())
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_task_run(&target_run)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Succeeded
            );
            for id in tail {
                let task = store.get_task(&id).await.unwrap().unwrap();
                assert_eq!(task.runs.len(), 1);
                assert_eq!(task.runs[0].status, TaskRunStatus::Queued);
            }
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            let ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "verified tail parks after useful rescan"
            );
            assert!(observer.writes.lock().unwrap().is_empty());
            tokio::time::resume();
            ticker.abort();
            worker.abort();
            let _ = worker.await;
            runtime.shutdown().await;
        }
        for saturated in [false, true] {
            let (_directory, store, runtime, observer) = scheduler_fixture().await;
            // Separate capacity no-op branch, using real saturated scheduling to
            // create the second occurrence before starting the worker.
            let capacity = if saturated {
                let capacity = runtime
                    .service()
                    .create_task(
                        crate::TaskCreateContext::default(),
                        create_params(pioneer_protocol::TaskTriggerSpec::Interval {
                            interval_seconds: 10,
                            interval_anchor_at: Some(4_000_000_000),
                            catch_up_policy: None,
                        }),
                    )
                    .await
                    .unwrap();
                runtime.process_due_once(4_000_000_000).await.unwrap();
                let first = store
                    .get_task(&capacity.task.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .runs[0]
                    .clone();
                let capacity_handle = crate::TaskExecutionHandle::new(
                    store.clone(),
                    runtime.event_bus(),
                    capacity.task.id.clone(),
                    first.id.clone(),
                );
                capacity_handle.mark_started(4_000_000_000).await.unwrap();
                runtime.process_due_once(4_000_000_010).await.unwrap();
                let blocked = store.get_task(&capacity.task.id).await.unwrap().unwrap();
                assert_eq!(blocked.runs.len(), 2);
                assert_eq!(blocked.runs[1].status, TaskRunStatus::Queued);
                // Setup scheduling above creates genuine occurrence authority at
                // a synthetic future clock. Move only its queued eligibility
                // clock into the past BEFORE owner acquisition so this branch
                // actually exercises a due capacity no-op on production UTC.
                use pioneer_entity::task_run as run;
                use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
                run::Entity::update_many()
                    .col_expr(
                        run::Column::ReadyAt,
                        Expr::value(Some(pioneer_crud::unix_to_datetime(
                            now_timestamp_secs() - 1,
                        ))),
                    )
                    .filter(run::Column::Id.eq(blocked.runs[1].id.clone()))
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
                let due = store
                    .get_task_run(&blocked.runs[1].id)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(due.ready_at.is_some_and(|at| at <= now_timestamp_secs()));
                Some((capacity.task.id, capacity_handle))
            } else {
                None
            };
            let mut ids = Vec::new();
            for _ in 0..129 {
                let task = runtime
                    .service()
                    .create_task(
                        crate::TaskCreateContext::default(),
                        create_params(pioneer_protocol::TaskTriggerSpec::Immediate),
                    )
                    .await
                    .unwrap();
                assert_eq!(task.run.as_ref().unwrap().status, TaskRunStatus::Queued);
                ids.push(task.task.id);
            }
            let scheduler = runtime.scheduler_for_test();
            let worker = tokio::spawn(scheduler.clone().run());
            tokio::time::timeout(Duration::from_secs(10), async {
                if saturated {
                    scheduler.future_observed.notified().await;
                } else {
                    scheduler.parked.notified().await;
                }
            })
            .await
            .unwrap();
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            tokio::time::pause();
            let hold = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "blocked due rows do not replenish semantic debt"
            );
            assert!(observer.writes.lock().unwrap().is_empty());
            hold.abort();
            tokio::time::resume();
            // Only the supported producer registration; no manual processing/kick.
            runtime
                .register_executor(Arc::new(CompletingSystemExecutor))
                .await;
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let mut complete = true;
                    for id in &ids {
                        let response = store.get_task(id).await.unwrap().unwrap();
                        complete &= response.runs.len() == 1
                            && response.runs[0].status == TaskRunStatus::Succeeded;
                    }
                    if complete {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // Capacity release is its own lower committed producer, independent
            // of registration. The queued second occurrence must now complete.
            if let Some((capacity_id, capacity_handle)) = capacity {
                capacity_handle
                    .complete_run(None, 4_000_000_011)
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let task = store.get_task(&capacity_id).await.unwrap().unwrap();
                        if task
                            .runs
                            .iter()
                            .all(|run| run.status == TaskRunStatus::Succeeded)
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            worker.abort();
            let _ = worker.await;
            runtime.shutdown().await;
        }
    }
}
