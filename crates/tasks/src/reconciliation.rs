use crate::TaskRuntimeResult;
use crate::event_bus::TaskEventBus;
use crate::executor::{TaskExecutionContext, TaskExecutionHandle, TaskExecutorRegistry};
use crate::projector::TaskProjector;
use crate::scheduler::{TASK_EXECUTION_LEASE_SECONDS, TaskSchedulerHandle};
use crate::task_boundary::task_fresh_task;
use pioneer_crud::{
    CrudStore, TaskOccurrenceClaimDeferral, TaskOccurrenceClaimFailure,
    TaskOccurrenceTerminalRepairOutcome,
};
use pioneer_protocol::{TaskEventPayload, TaskRun, TaskRunStatus, TaskTriggerStatus, generate_id};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tracing::warn;

const DORMANT_TRIGGER_RECOVERY_MESSAGE: &str =
    "active trigger has no next_fire_at; scheduler will keep it dormant";
const DISPATCHABLE_RUN_RECOVERY_MESSAGE: &str = "queued run is dispatchable after startup";
const ID_LEN: usize = 21;
const OCCURRENCE_STORAGE_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconciliationReport {
    pub active_triggers: usize,
    pub queued_runs: usize,
    pub starting_runs: usize,
    pub running_runs: usize,
    pub waiting_review_runs: usize,
    pub recovered_events: usize,
    pub terminal_occurrence_mismatches: usize,
    pub repaired_terminal_occurrences: usize,
}

/// A completed quantum with visible row-local failures; startup may continue
/// independent dispatch before returning this error to its caller.
#[derive(Debug)]
pub(crate) struct PartialRecoveryError(Vec<anyhow::Error>);
impl std::fmt::Display for PartialRecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "incomplete recovery: {:?}", self.0)
    }
}
impl std::error::Error for PartialRecoveryError {}

pub struct TaskStartupReconciler {
    store: Arc<CrudStore>,
    projector: TaskProjector,
    event_bus: Arc<TaskEventBus>,
    executors: Arc<TaskExecutorRegistry>,
    scheduler: TaskSchedulerHandle,
    work: Arc<pioneer_sqlite::background::Wake>,
    state: Arc<ReconciliationState>,
}

/// Finite domain cursors only. The common Wake owns waiting and drain admission.
#[derive(Default)]
struct ReconciliationState {
    discovery: Mutex<SchedulerRecovery>,
    occurrence_retry: Mutex<Option<Instant>>,
    #[cfg(test)]
    parked: tokio::sync::Notify,
    #[cfg(test)]
    future_observed: tokio::sync::Notify,
    #[cfg(test)]
    page_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    pass_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    drain_waiting: tokio::sync::Notify,
    #[cfg(test)]
    clock: std::sync::atomic::AtomicI64,
}
#[derive(Default)]
struct SchedulerRecovery {
    cycle_open: bool,
    rescan_requested: bool,
    dormant_done: bool,
    dormant_cursor: Option<String>,
    dormant_upper: Option<String>,
    run_done: [bool; 4],
    run_cursors: [Option<String>; 4],
    run_uppers: [Option<String>; 4],
    retry_at: HashMap<String, Instant>,
    trigger_retry: HashMap<String, Instant>,
    // Earliest clock used by this finite pass. Observe future obligations from
    // this cutoff, not the EOF clock: both skipped leases and leases claimed by
    // recovery itself may have expired meanwhile. The query still filters to
    // currently live runs, so completed work leaves no stale timer.
    lease_cutoff: i64,
    lease_done: bool,
    lease_after: Option<String>,
    lease_upper: Option<String>,
    lease_nearest: Option<(String, i64)>,
    lease_retry: Option<Instant>,
}
struct RecoveryPass<'a> {
    shared: &'a ReconciliationState,
    work: &'a pioneer_sqlite::background::Wake,
    state: SchedulerRecovery,
    complete: bool,
}
impl Drop for RecoveryPass<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.state.cycle_open = false;
        }
        // Return the finite cursor state even on cancellation. Never hold this
        // mutex over a database operation, executor, or notification.
        let old = std::mem::replace(
            &mut *self.shared.discovery.lock().unwrap(),
            std::mem::take(&mut self.state),
        );
        drop(old);
        // Explicit foreground reconciliation can leave a finite tail without
        // writing anything. Hand it back after restoring the real state, also
        // on cancellation. The live owner's own quantum suppresses this hint.
        self.work.invalidate();
    }
}
impl TaskStartupReconciler {
    #[cfg(test)]
    pub(super) fn parked_for_test(&self) -> &tokio::sync::Notify {
        &self.state.parked
    }

    #[cfg(test)]
    pub(super) fn pause_pass_for_test(&self, barrier: Arc<tokio::sync::Barrier>) {
        *self.state.pass_barrier.lock().unwrap() = Some(barrier);
    }

    pub fn new(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        executors: Arc<TaskExecutorRegistry>,
        scheduler: TaskSchedulerHandle,
    ) -> Self {
        let store = Arc::new(store.with_maintenance_access());
        let projector = TaskProjector::new(store.clone());
        let work = store.background_work("tasks.live");
        let state = store
            .database_connection()
            .shared_resource::<ReconciliationState>();
        Self {
            store,
            projector,
            event_bus,
            executors,
            scheduler,
            work,
            state,
        }
    }

    pub async fn run(self: Arc<Self>) {
        use pioneer_sqlite::background::Ready;
        let Some(mut owner) = self.work.acquire() else {
            return;
        };
        let mut previous_observation = None;
        loop {
            match owner.ready().await {
                Ready::Closed => return,
                Ready::SettlementRequired => {
                    let settle_observation = owner.observe();
                    let Some(_drain) = self.work.drain().await else {
                        return;
                    };
                    {
                        let mut state = self.state.discovery.lock().unwrap();
                        state.cycle_open = false;
                    }
                    if self
                        .store
                        .database_connection()
                        .settle_background_change(&owner)
                        .await
                        .is_err()
                    {
                        warn!(
                            failure_class = "task_live_settlement_failed",
                            "task recovery settlement deferred"
                        );
                        owner.retry_after(settle_observation, Duration::from_secs(60));
                    }
                    continue;
                }
                Ready::Work => {}
            }
            // A wake received between pages must survive until EOF, even when
            // it came from a lower execution producer without semantic hints.
            // Finish the current tail first; coalesce all changes into one rescan.
            let observed = owner.observe();
            // Observation precedes admission so a producer while waiting still
            // fences publication. All access to retained traversal state,
            // including planning/publication, shares this same admission with
            // foreground callers: mem::take's placeholder is never observed.
            let admission = self.work.drain();
            #[cfg(test)]
            let admission = {
                tokio::pin!(admission);
                std::future::poll_fn(|cx| {
                    let poll = std::future::Future::poll(admission.as_mut(), cx);
                    if poll.is_pending() {
                        self.state.drain_waiting.notify_one();
                    }
                    poll
                })
                .await
            };
            #[cfg(not(test))]
            let admission = admission.await;
            let Some(_drain) = admission else {
                return;
            };
            if previous_observation.is_none_or(|prior| owner.producer_changed_since(prior)) {
                let mut state = self.state.discovery.lock().unwrap();
                if state.cycle_open {
                    state.rescan_requested = true;
                }
            }
            previous_observation = Some(observed);
            let result = owner
                .scope(async {
                    self.reconcile_admitted(self.now()).await?;
                    #[cfg(test)]
                    {
                        let barrier = self.state.page_barrier.lock().unwrap().take();
                        if let Some(barrier) = barrier {
                            barrier.wait().await;
                            barrier.wait().await;
                        }
                    }
                    self.next_plan(self.now()).await
                })
                .await;
            match result {
                Ok(plan) => {
                    let published = owner.publish(observed, plan);
                    #[cfg(test)]
                    if published {
                        match plan {
                            pioneer_sqlite::background::Plan::Dormant => {
                                self.state.parked.notify_one()
                            }
                            pioneer_sqlite::background::Plan::WallDeadline(_)
                            | pioneer_sqlite::background::Plan::Deadlines { .. }
                            | pioneer_sqlite::background::Plan::MonotonicDeadline(_) => {
                                self.state.future_observed.notify_one()
                            }
                            _ => {}
                        }
                    }
                    #[cfg(not(test))]
                    let _ = published;
                }
                Err(error) => {
                    warn!(
                        failure_class = "task_live_reconciliation_failed",
                        "live task terminal reconciliation pass failed"
                    );
                    let delay = if pioneer_sqlite::is_anyhow_sqlite_transient_access(&error) {
                        60
                    } else {
                        5
                    };
                    owner.retry_after(observed, Duration::from_secs(delay));
                }
            }
        }
    }

    fn now(&self) -> i64 {
        #[cfg(test)]
        {
            let now = self.state.clock.load(std::sync::atomic::Ordering::Acquire);
            if now != 0 {
                return now;
            }
        }
        self.wall_seconds()
    }

    fn wall_seconds(&self) -> i64 {
        self.work
            .wall_now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .min(i64::MAX as u64) as i64
    }

    async fn next_plan(&self, now: i64) -> TaskRuntimeResult<pioneer_sqlite::background::Plan> {
        use pioneer_sqlite::background::Plan;
        let (continuation, retry) = {
            let recovery = self.state.discovery.lock().unwrap();
            (
                recovery.cycle_open || recovery.rescan_requested,
                recovery
                    .retry_at
                    .values()
                    .chain(recovery.trigger_retry.values())
                    .copied()
                    .min(),
            )
        };
        if continuation || self.store.task_live_changes().pending() {
            return Ok(Plan::Runnable);
        }
        let (cutoff, nearest, lease_retry) = {
            let state = self.state.discovery.lock().unwrap();
            (
                state.lease_cutoff,
                state.lease_nearest.clone(),
                state.lease_retry,
            )
        };
        if now < cutoff {
            self.state.discovery.lock().unwrap().rescan_requested = true;
            return Ok(Plan::Runnable);
        }
        // Own completion/claim may have changed the sampled minimum without
        // external invalidation. Revalidate this witness before parking. A
        // changed witness requests another finite pass, not a global MIN.
        if let Some((id, at)) = nearest.as_ref() {
            if self.store.task_live_lease_point(id, cutoff).await? != Some(*at) {
                self.state.discovery.lock().unwrap().rescan_requested = true;
                return Ok(Plan::Runnable);
            }
        }
        let queued = self
            .store
            .next_task_live_queued_deadline(cutoff.min(now))
            .await?;
        let lease = nearest.map(|(_, at)| at).into_iter().chain(queued).min();
        let cooldown = *self.state.occurrence_retry.lock().unwrap();
        let cooldown = cooldown.filter(|at| *at > Instant::now());
        let occurrence = if cooldown.is_none() {
            self.store
                .next_task_occurrence_reconcile_at(self.wall_seconds())
                .await?
        } else {
            None
        };
        let deadline = lease.into_iter().chain(occurrence).min();
        let wall = deadline.map(|at| std::time::UNIX_EPOCH + Duration::from_secs(at.max(0) as u64));
        let plan = wall.map_or(Plan::Dormant, |at| {
            if at <= self.work.wall_now() {
                Plan::Runnable
            } else {
                Plan::WallDeadline(at)
            }
        });
        let retry = retry.into_iter().chain(cooldown).chain(lease_retry).min();
        Ok(retry.map_or(plan, |at| plan.with_monotonic_deadline(at)))
    }

    pub async fn reconcile(&self, now: i64) -> TaskRuntimeResult<ReconciliationReport> {
        let Some(_drain) = self.work.drain().await else {
            return Ok(ReconciliationReport::default());
        };
        self.reconcile_admitted(now).await
    }

    // Caller retains the tasks.live drain until state is restored. The live
    // owner also retains it through next_plan and publication, without a nested
    // semaphore acquisition or any mutex/database resource across domain awaits.
    async fn reconcile_admitted(&self, now: i64) -> TaskRuntimeResult<ReconciliationReport> {
        let changed = self.store.task_live_changes().take();
        let state = std::mem::take(&mut *self.state.discovery.lock().unwrap());
        let mut pass = RecoveryPass {
            shared: &self.state,
            work: &self.work,
            state,
            complete: false,
        };
        let recovery = &mut pass.state;
        if changed && recovery.cycle_open {
            recovery.rescan_requested = true;
        }
        if !recovery.cycle_open {
            recovery.rescan_requested = false;
            recovery.cycle_open = true;
            recovery.dormant_done = false;
            recovery.dormant_cursor = None;
            recovery.dormant_upper = None;
            recovery.run_done = [false; 4];
            recovery.run_cursors = Default::default();
            recovery.run_uppers = Default::default();
            recovery.lease_cutoff = now;
            recovery.lease_done = false;
            recovery.lease_after = None;
            recovery.lease_upper = None;
            recovery.lease_nearest = None;
            recovery.lease_retry = None;
        }
        if now < recovery.lease_cutoff {
            recovery.rescan_requested = true;
        }
        let mut discovery_errors = Vec::new();
        let mut unknown_discovery = false;
        let active_triggers = if recovery.dormant_done {
            None
        } else {
            // Only NULL-next-fire active triggers require this owner's
            // diagnostic recovery. Future arming belongs to the scheduler's
            // independent bounded reconstruction, including at startup.
            let (page, upper) = self
                .store
                .list_dormant_active_task_triggers_finite(
                    recovery.dormant_cursor.as_deref(),
                    recovery.dormant_upper.as_deref(),
                    64,
                )
                .await?;
            let more = page.scanned == 64 && page.last.as_ref() != upper.as_ref();
            recovery.dormant_done = !more;
            recovery.dormant_cursor = if more { page.last.clone() } else { None };
            recovery.dormant_upper = if more { upper } else { None };
            Some(page)
        };
        let active_triggers = active_triggers.unwrap_or(pioneer_crud::TaskSchedulerPage {
            records: Vec::new(),
            failures: Vec::new(),
            scanned: 0,
            last: None,
        });
        for row in &active_triggers.failures {
            recovery
                .trigger_retry
                .insert(row.id.clone(), Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
        }
        discovery_errors.extend(active_triggers.failures.into_iter().map(|row| {
            row.error
                .context(format!("scheduler row {} (task {})", row.id, row.task_id))
        }));
        let mut active_triggers = active_triggers.records;
        let mut trigger_retries = recovery
            .trigger_retry
            .iter()
            .filter(|(_, at)| **at <= Instant::now())
            .map(|(id, at)| (*at, id.clone()))
            .collect::<Vec<_>>();
        trigger_retries.sort();
        for (_, id) in trigger_retries.into_iter().take(64) {
            recovery
                .trigger_retry
                .insert(id.clone(), Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
            let page = match self.store.task_scheduler_trigger_partial(&id).await {
                Ok(page) => page,
                Err(error) => {
                    unknown_discovery = true;
                    discovery_errors.push(error);
                    continue;
                }
            };
            // Hydration is not completion: history/diagnostic processing below
            // can still fail before it acquires a mutation guard.
            if page.failures.is_empty() && page.records.is_empty() {
                recovery.trigger_retry.remove(&id);
            }
            discovery_errors.extend(page.failures.into_iter().map(|row| {
                row.error
                    .context(format!("scheduler row {} (task {})", row.id, row.task_id))
            }));
            for trigger in page.records {
                if !active_triggers.iter().any(|row| row.id == trigger.id) {
                    active_triggers.push(trigger);
                }
            }
        }
        let statuses = [
            TaskRunStatus::Queued,
            TaskRunStatus::Starting,
            TaskRunStatus::Running,
            TaskRunStatus::WaitingReview,
        ];
        let mut candidates = HashMap::new();
        for (index, status) in statuses.into_iter().enumerate() {
            if recovery.run_done[index] {
                continue;
            }
            let limit = 64;
            let after = recovery.run_cursors[index].as_deref();
            let upper = recovery.run_uppers[index].as_deref();
            let (page, upper) = if status == TaskRunStatus::Queued {
                self.store
                    .list_scheduler_due_task_runs_finite(now, after, upper, limit)
                    .await?
            } else {
                self.store
                    .list_task_recovery_runs_finite(status, after, upper, limit)
                    .await?
            };
            let more = page.scanned == limit as usize && page.last.as_ref() != upper.as_ref();
            recovery.run_done[index] = !more;
            recovery.run_cursors[index] = if more { page.last.clone() } else { None };
            recovery.run_uppers[index] = if more { upper } else { None };
            for row in &page.failures {
                recovery
                    .retry_at
                    .insert(row.id.clone(), Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
            }
            discovery_errors.extend(page.failures.into_iter().map(|row| {
                row.error
                    .context(format!("scheduler row {} (task {})", row.id, row.task_id))
            }));
            for run in page.records {
                candidates.insert(run.id.clone(), run);
            }
        }
        let mut local = recovery
            .retry_at
            .iter()
            .filter(|(_, retry_at)| **retry_at <= Instant::now())
            .map(|(id, retry_at)| (*retry_at, id.clone()))
            .collect::<Vec<_>>();
        local.sort();
        for (_, run_id) in local.into_iter().take(64) {
            let page = match self.store.task_scheduler_run_partial(&run_id).await {
                Ok(page) => page,
                Err(error) => {
                    unknown_discovery = true;
                    discovery_errors.push(error);
                    recovery
                        .retry_at
                        .insert(run_id.clone(), Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
                    continue;
                }
            };
            if !page.failures.is_empty() {
                recovery
                    .retry_at
                    .insert(run_id.clone(), Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
                discovery_errors.extend(page.failures.into_iter().map(|row| {
                    row.error
                        .context(format!("scheduler row {} (task {})", row.id, row.task_id))
                }));
                continue;
            }
            if let Some(run) = page.records.into_iter().next() {
                if !crate::service::is_terminal_run(run.status) {
                    candidates.insert(run.id.clone(), run);
                } else {
                    recovery.retry_at.remove(&run_id);
                }
            } else {
                recovery.retry_at.remove(&run_id);
            }
        }
        // Completed row failures preserve finite progress. Cancellation resets
        // the pass so every selected-but-unprocessed row is reconstructed.
        recovery.cycle_open =
            !(recovery.dormant_done && recovery.run_done.iter().all(|done| *done));
        candidates.retain(|id, run| {
            if run.status == TaskRunStatus::Queued && run.ready_at.is_some_and(|ready| ready > now)
            {
                // The scheduler owns future queued dispatch; live recovery
                // also retains its concrete ready deadline below.
                recovery.retry_at.remove(id);
                false
            } else {
                true
            }
        });
        let runs = candidates
            .into_values()
            .filter(|run| {
                recovery
                    .retry_at
                    .get(&run.id)
                    .is_none_or(|at| *at <= Instant::now())
            })
            .collect::<Vec<_>>();
        let queued_runs = runs
            .iter()
            .filter(|run| run.status == TaskRunStatus::Queued)
            .collect::<Vec<_>>();
        let starting_runs = runs
            .iter()
            .filter(|run| run.status == TaskRunStatus::Starting)
            .collect::<Vec<_>>();
        let running_runs = runs
            .iter()
            .filter(|run| run.status == TaskRunStatus::Running)
            .collect::<Vec<_>>();
        let waiting_review_runs = runs
            .iter()
            .filter(|run| run.status == TaskRunStatus::WaitingReview)
            .collect::<Vec<_>>();
        let mut recovered_events = 0usize;
        let mut partial_only = !unknown_discovery;

        for trigger in &active_triggers {
            if trigger.status != TaskTriggerStatus::Active || trigger.next_fire_at.is_some() {
                recovery.trigger_retry.remove(&trigger.id);
                continue;
            }
            // A task-local history/append failure must not suppress independent
            // runs or terminal occurrence work selected in this same quantum.
            let outcome: TaskRuntimeResult<bool> = async {
                if self
                    .recovery_event_exists(
                        trigger.task_id.as_str(),
                        None,
                        DORMANT_TRIGGER_RECOVERY_MESSAGE,
                    )
                    .await?
                {
                    return Ok(false);
                }
                let appended = self
                    .scheduler
                    .track_task_mutation(
                        &trigger.task_id,
                        self.projector.append_event(
                            TaskEventPayload::TaskRecovered {
                                task_id: trigger.task_id.clone(),
                                run_id: None,
                                message: DORMANT_TRIGGER_RECOVERY_MESSAGE.to_owned(),
                                recovered_at: now,
                            },
                            now,
                        ),
                    )
                    .await?;
                self.event_bus.publish(appended).await;
                Ok(true)
            }
            .await;
            match outcome {
                Ok(changed) => {
                    recovery.trigger_retry.remove(&trigger.id);
                    if changed {
                        recovered_events = recovered_events.saturating_add(1);
                    }
                }
                Err(error) => {
                    recovery.trigger_retry.insert(
                        trigger.id.clone(),
                        Instant::now() + OCCURRENCE_STORAGE_BACKOFF,
                    );
                    partial_only = false;
                    discovery_errors.push(error);
                }
            }
        }

        for run in queued_runs
            .iter()
            .chain(starting_runs.iter())
            .chain(running_runs.iter())
            .chain(waiting_review_runs.iter())
        {
            match self.recover_run(run, now).await {
                Ok(changed) => {
                    recovery.retry_at.remove(&run.id);
                    if changed {
                        recovered_events = recovered_events.saturating_add(1);
                    }
                }
                Err(error) => {
                    recovery
                        .retry_at
                        .insert(run.id.clone(), Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
                    partial_only = false;
                    discovery_errors.push(error);
                }
            }
        }

        let (terminal_occurrence_mismatch_count, repaired_terminal_occurrences) = match self
            .reconcile_terminal_occurrences(self.wall_seconds())
            .await
        {
            Ok(counts) => counts,
            Err(error) => {
                partial_only = false;
                discovery_errors.push(error);
                (0, 0)
            }
        };

        if !recovery.lease_done {
            let (keys, upper) = self
                .store
                .task_live_lease_keys(
                    recovery.lease_after.as_deref(),
                    recovery.lease_upper.as_deref(),
                    64,
                )
                .await?;
            let more = keys.len() == 64 && keys.last() != upper.as_ref();
            for id in &keys {
                match self
                    .store
                    .task_live_lease_point(id, recovery.lease_cutoff)
                    .await
                {
                    Ok(Some(at))
                        if recovery
                            .lease_nearest
                            .as_ref()
                            .is_none_or(|(_, old)| at < *old) =>
                    {
                        recovery.lease_nearest = Some((id.clone(), at));
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let delay = if pioneer_sqlite::is_anyhow_sqlite_transient_access(&error) {
                            Duration::from_secs(60)
                        } else {
                            OCCURRENCE_STORAGE_BACKOFF
                        };
                        let retry = Instant::now() + delay;
                        recovery.lease_retry =
                            Some(recovery.lease_retry.map_or(retry, |old| old.min(retry)));
                        partial_only = false;
                        discovery_errors.push(error);
                    }
                }
            }
            recovery.lease_done = !more;
            recovery.lease_after = keys.last().cloned();
            recovery.lease_upper = upper;
        }
        recovery.cycle_open |= !recovery.lease_done;

        #[cfg(test)]
        {
            // No DB permit or state mutex is held here. Unlike page_barrier,
            // the real cursor still lives in RecoveryPass at this boundary.
            let barrier = self.state.pass_barrier.lock().unwrap().take();
            if let Some(barrier) = barrier {
                barrier.wait().await;
                barrier.wait().await;
            }
        }
        pass.complete = true;
        if !discovery_errors.is_empty() {
            if !partial_only {
                return Err(anyhow::anyhow!(
                    "unknown recovery discovery: {:?}",
                    discovery_errors
                ));
            }
            return Err(PartialRecoveryError(discovery_errors).into());
        }
        Ok(ReconciliationReport {
            active_triggers: active_triggers.len(),
            queued_runs: queued_runs.len(),
            starting_runs: starting_runs.len(),
            running_runs: running_runs.len(),
            waiting_review_runs: waiting_review_runs.len(),
            recovered_events,
            terminal_occurrence_mismatches: terminal_occurrence_mismatch_count,
            repaired_terminal_occurrences,
        })
    }

    async fn reconcile_terminal_occurrences(&self, now: i64) -> TaskRuntimeResult<(usize, usize)> {
        // Drain admission serializes this phase. Only copy the cooldown under
        // the process-local mutex; DB and executor awaits retain no state lock.
        let retry_at = *self.state.occurrence_retry.lock().unwrap();
        if retry_at.is_some_and(|deadline| deadline > Instant::now()) {
            return Ok((0, 0));
        }
        let clock = || self.wall_seconds();
        // Fixed fair quotas: scope <=16 inputs, seed <=16, due runs <=32.
        // Bookkeeping failures don't prevent this pass's healthy run checks.
        let mut storage_error = false;
        if self
            .store
            .expand_task_occurrence_scope(&clock)
            .await
            .is_err()
        {
            storage_error = true;
            warn!(
                failure_class = "task_occurrence_scope_storage_failed",
                "Task occurrence scope expansion failed"
            );
        }
        if self
            .store
            .seed_unfinished_task_occurrences(&clock)
            .await
            .is_err()
        {
            storage_error = true;
            warn!(
                failure_class = "task_occurrence_seed_storage_failed",
                "Task occurrence seed failed"
            );
        }
        let discovery = match self
            .store
            .discover_task_occurrence_reconcile_with_audit(clock())
            .await
        {
            Ok(discovery) => discovery,
            Err(error) => {
                *self.state.occurrence_retry.lock().unwrap() =
                    Some(Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
                return Err(error);
            }
        };
        let audit_error = discovery.audit_error;
        storage_error |= audit_error.is_some();
        let mut changed = 0;
        for candidate in discovery.candidates {
            let store = self.store.clone();
            let work = self.work.clone();
            // Reuse the owned/abort-on-drop task boundary. A candidate panic or
            // storage failure cannot abort the rest of the selected quantum.
            let outcome = task_fresh_task(
                async move {
                    let Some(claim) = store
                        .claim_task_occurrence_reconcile(&candidate, &|| {
                            work.wall_now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                .min(i64::MAX as u64) as i64
                        })
                        .await?
                    else {
                        return Ok(TaskOccurrenceTerminalRepairOutcome::StaleClaim);
                    };
                    store.reconcile_claimed_task_occurrence(&claim, now).await
                },
                "Task occurrence repair task did not finish",
            )
            .await;
            match outcome {
                Ok(TaskOccurrenceTerminalRepairOutcome::Changed) => {
                    changed += 1;
                    warn!(
                        failure_class = "task_terminal_occurrence_mismatch_repaired",
                        "repaired terminal Task occurrence projection mismatch"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    // Claim errors already attempted an exact-snapshot durable
                    // deferral. If storage prevented it, also delay the worker.
                    storage_error |= error
                        .downcast_ref::<TaskOccurrenceClaimFailure>()
                        .is_none_or(|failure| {
                            matches!(
                                failure.deferral,
                                TaskOccurrenceClaimDeferral::NoSnapshot
                                    | TaskOccurrenceClaimDeferral::Failed
                            )
                        });
                    warn!(
                        failure_class = "task_terminal_occurrence_repair_failed",
                        "failed to repair terminal Task occurrence projection mismatch"
                    );
                }
            }
        }
        *self.state.occurrence_retry.lock().unwrap() =
            storage_error.then(|| Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
        if let Some(error) = audit_error {
            return Err(error);
        }
        // Count confirmed mismatches, not dirty candidates (which may agree).
        Ok((changed, changed))
    }

    async fn recover_run(&self, run: &TaskRun, now: i64) -> TaskRuntimeResult<bool> {
        let Some(executor) = self.executors.get(run.executor_kind).await else {
            return Ok(false);
        };
        if matches!(
            run.status,
            TaskRunStatus::Starting | TaskRunStatus::Running | TaskRunStatus::WaitingReview
        ) && self
            .store
            .load_execution_for_run(&run.id)
            .await?
            .is_some_and(|execution| {
                execution.worker_id.is_some()
                    && execution.lease_until.is_some_and(|until| until > now)
            })
        {
            return Ok(false);
        }
        let Some(task_record) = self.store.get_task_record(run.task_id.as_str()).await? else {
            return Ok(false);
        };
        let recovery_message = match run.status {
            TaskRunStatus::Queued => DISPATCHABLE_RUN_RECOVERY_MESSAGE,
            TaskRunStatus::Starting | TaskRunStatus::Running => {
                "in-flight run is recoverable after startup"
            }
            TaskRunStatus::WaitingReview => "waiting-review run is recoverable after startup",
            _ => return Ok(false),
        };
        if run.status == TaskRunStatus::Queued {
            if !self
                .scheduler
                .track_task_mutation(
                    &run.task_id,
                    self.store.revalidate_and_sync_active_task_occurrence(
                        run.id.as_str(),
                        TaskRunStatus::Queued,
                        now,
                    ),
                )
                .await?
            {
                return Ok(false);
            }
            return self
                .emit_recovery_event_if_missing(run, recovery_message, now)
                .await;
        }
        if run.status == TaskRunStatus::WaitingReview {
            if !self
                .scheduler
                .track_task_mutation(
                    &run.task_id,
                    self.store.revalidate_and_sync_active_task_occurrence(
                        run.id.as_str(),
                        TaskRunStatus::WaitingReview,
                        now,
                    ),
                )
                .await?
            {
                return Ok(false);
            }
            let emitted_recovery_event = self
                .emit_recovery_event_if_missing(run, recovery_message, now)
                .await?;
            let handle = TaskExecutionHandle::new(
                self.store.clone(),
                self.event_bus.clone(),
                run.task_id.clone(),
                run.id.clone(),
            );
            if let Err(error) = executor
                .recover_run(
                    TaskExecutionContext {
                        workspace_id: task_record.workspace_id,
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
                return Err(error);
            }
            return Ok(emitted_recovery_event);
        }

        let worker_id = format!("task-recovery-{}", generate_id(ID_LEN));
        let lease_until = now.saturating_add(TASK_EXECUTION_LEASE_SECONDS);
        let Some(execution) = self
            .scheduler
            .track_task_mutation(
                &run.task_id,
                self.store.claim_task_run_execution_for_recovery(
                    run.id.as_str(),
                    run.executor_kind,
                    worker_id.as_str(),
                    now,
                    lease_until,
                ),
            )
            .await?
        else {
            // A valid execution lease or a terminal authority prevents both
            // recovery ownership and the occurrence transition atomically.
            return Ok(false);
        };
        // A successful own claim may create/renew a lease behind the finite
        // execution cursor (or above its captured upper). Own ACK suppression
        // is correct for bookkeeping, but this new obligation needs one rescan.
        // A blocked/no-op claim never reaches this point.
        self.store.task_live_changes().record();
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
            workspace_id: task_record.workspace_id,
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
            return Err(error);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskScheduler;
    use crate::executor::{TaskExecutor, TaskExecutorRecoveryOutcome, TaskExecutorStartOutcome};
    use crate::tests::{
        CompletingSystemExecutor, DispatchDatabaseObserver, create_params, seed_task_test_workspace,
    };
    use pioneer_protocol::{TaskExecutorKind, TaskTriggerSpec};
    use pioneer_sqlite::{SqliteDatabase, SqliteReadClass, SqliteWriteClass, SqliteWriteExecutor};
    use sea_orm::{ConnectOptions, Database};

    struct CompletingRecovery;
    struct LeaveFirstClaim(std::sync::atomic::AtomicBool);
    #[async_trait::async_trait]
    impl TaskExecutor for LeaveFirstClaim {
        fn kind(&self) -> TaskExecutorKind {
            TaskExecutorKind::System
        }
        async fn start_run(
            &self,
            context: TaskExecutionContext,
            run: TaskRun,
            handle: TaskExecutionHandle,
        ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
            CompletingSystemExecutor
                .start_run(context, run, handle)
                .await
        }
        async fn cancel_run(
            &self,
            _: TaskExecutionContext,
            _: &str,
            _: &str,
            _: TaskExecutionHandle,
        ) -> TaskRuntimeResult<()> {
            Ok(())
        }
        async fn recover_run(
            &self,
            _: TaskExecutionContext,
            _: TaskRun,
            handle: TaskExecutionHandle,
        ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
            if self.0.swap(false, std::sync::atomic::Ordering::AcqRel) {
                Ok(TaskExecutorRecoveryOutcome::LeftUnchanged)
            } else {
                handle
                    .complete_run(None, crate::service::now_timestamp_secs())
                    .await?;
                Ok(TaskExecutorRecoveryOutcome::Recovered)
            }
        }
    }
    #[async_trait::async_trait]
    impl TaskExecutor for CompletingRecovery {
        fn kind(&self) -> TaskExecutorKind {
            TaskExecutorKind::System
        }
        async fn start_run(
            &self,
            context: TaskExecutionContext,
            run: TaskRun,
            handle: TaskExecutionHandle,
        ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
            CompletingSystemExecutor
                .start_run(context, run, handle)
                .await
        }
        async fn cancel_run(
            &self,
            _: TaskExecutionContext,
            _: &str,
            _: &str,
            _: TaskExecutionHandle,
        ) -> TaskRuntimeResult<()> {
            Ok(())
        }
        async fn recover_run(
            &self,
            _: TaskExecutionContext,
            _: TaskRun,
            handle: TaskExecutionHandle,
        ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
            handle
                .complete_run(None, crate::service::now_timestamp_secs())
                .await?;
            Ok(TaskExecutorRecoveryOutcome::Recovered)
        }
    }

    async fn live_fixture() -> (
        tempfile::TempDir,
        Arc<CrudStore>,
        crate::TaskRuntime,
        Arc<DispatchDatabaseObserver>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("live.sqlite");
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

    // Typed durable events give the Runs stable tuple order without rewriting
    // models/IDs behind the projector. No scheduler or other worker is started.
    async fn ordered_live_runs(
        store: &Arc<CrudStore>,
        runtime: &crate::TaskRuntime,
        count: usize,
        system_index: usize,
        now: i64,
    ) -> Vec<TaskRun> {
        let projector = TaskProjector::new(store.clone());
        let mut runs = Vec::new();
        for index in 0..count {
            let mut params = create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            });
            params.executor_kind = if index == system_index {
                TaskExecutorKind::System
            } else {
                TaskExecutorKind::Workflow
            };
            let response = runtime
                .maintenance_service()
                .create_task(crate::TaskCreateContext::default(), params)
                .await
                .unwrap();
            let run = TaskRun {
                id: format!("run_{index:017}"),
                task_id: response.task.id.clone(),
                trigger_id: Some(response.trigger.id),
                trigger_input: None,
                parent_run_id: None,
                run_group_id: generate_id(ID_LEN),
                attempt_number: 1,
                retry_of_run_id: None,
                ready_at: Some(now),
                run_number: 1,
                status: TaskRunStatus::Queued,
                executor_kind: response.task.executor_kind,
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
            projector
                .append_events(
                    vec![
                        TaskEventPayload::TaskQueued {
                            task_id: run.task_id.clone(),
                            run_id: Some(run.id.clone()),
                        },
                        TaskEventPayload::RunCreated {
                            run: run.clone(),
                            agent_spec: None,
                        },
                    ],
                    now,
                )
                .await
                .unwrap();
            // Recovery claims require the same typed occurrence authority as
            // service-created Runs. Raw RunCreated alone is not sufficient and
            // would leave compatibility backfill writes to rescue the race.
            let actor = store
                .get_task_actor_contract(&run.task_id)
                .await
                .unwrap()
                .unwrap();
            let occurrence = crate::actor_contract::build_occurrence_contract(
                &response.task,
                &actor,
                &run.id,
                run.trigger_id.as_deref(),
                format!("manual:{}", run.id),
                1,
                now,
            )
            .unwrap();
            store
                .upsert_task_occurrence_contract(&occurrence, now)
                .await
                .unwrap();
            TaskExecutionHandle::new(
                store.clone(),
                runtime.event_bus(),
                run.task_id.clone(),
                run.id.clone(),
            )
            .mark_started(now)
            .await
            .unwrap();
            runs.push(run);
        }
        // Prime the manual-trigger diagnostic history while every executor is
        // absent. Later foreground pages must be genuinely read-only: otherwise
        // a recovery-event commit could accidentally rescue a lost handoff.
        let reconciler = runtime.live_owner_for_test();
        let reconstruction_now = crate::service::now_timestamp_secs();
        // Occurrence reconstruction consumes one Task scope per quantum,
        // independently of the 64-Run recovery pages used by the race below.
        for _ in 0..count + 32 {
            reconciler
                .reconcile_terminal_occurrences(reconstruction_now)
                .await
                .unwrap();
            if store
                .next_task_occurrence_reconcile_at(reconstruction_now)
                .await
                .unwrap()
                .is_none_or(|at| at > reconstruction_now)
            {
                break;
            }
        }
        let mut settled = false;
        for _ in 0..64 {
            reconciler.reconcile(reconstruction_now).await.unwrap();
            let admission = reconciler.work.drain().await.unwrap();
            let plan = reconciler.next_plan(reconstruction_now).await.unwrap();
            drop(admission);
            if matches!(plan, pioneer_sqlite::background::Plan::Dormant) {
                settled = true;
                break;
            }
        }
        if !settled {
            let state = reconciler.state.discovery.lock().unwrap();
            eprintln!(
                "live fixture cycle={} rescan={} leases={:?} cutoff={} runs={:?} pending={} retry={:?}",
                state.cycle_open,
                state.rescan_requested,
                state.lease_nearest,
                state.lease_cutoff,
                state.run_done,
                store.task_live_changes().pending(),
                state.retry_at
            );
            drop(state);
            eprintln!(
                "occurrence next={:?}, now={}",
                store
                    .next_task_occurrence_reconcile_at(reconstruction_now)
                    .await,
                reconstruction_now
            );
        }
        assert!(
            settled,
            "fixture must finish reconstruction before the race: {:?}",
            reconciler.next_plan(reconstruction_now).await,
        );
        store.task_live_changes().take();
        runs
    }

    // Independent owner/foreground tasks are explicitly polled at the lifetime
    // boundaries, so both interleavings are deterministic without OS timing.
    #[tokio::test]
    async fn live_foreground_state_handoff_retains_rescan_and_unvisited_tail() {
        for window in ["registration_during_pass", "foreground_before_plan"] {
            let (_directory, store, runtime, observer) = live_fixture().await;
            let now = crate::service::now_timestamp_secs();
            let registration = window == "registration_during_pass";
            let (count, target) = if registration { (65, 0) } else { (129, 128) };
            let runs = ordered_live_runs(&store, &runtime, count, target, now).await;
            let reconciler = runtime.live_owner_for_test();
            let page = Arc::new(tokio::sync::Barrier::new(2));
            if !registration {
                runtime
                    .register_executor(Arc::new(CompletingRecovery))
                    .await;
                *reconciler.state.page_barrier.lock().unwrap() = Some(page.clone());
            }
            let worker = tokio::spawn(reconciler.clone().run());
            if registration {
                tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
                    .await
                    .unwrap();
            } else {
                page.wait().await;
            }

            let pass = Arc::new(tokio::sync::Barrier::new(2));
            *reconciler.state.pass_barrier.lock().unwrap() = Some(pass.clone());
            let foreground_reconciler = reconciler.clone();
            let mut foreground =
                Box::pin(async move { foreground_reconciler.reconcile(now).await });
            if !registration {
                // Poll the real foreground entry point while the owner is
                // between page and plan. It must queue for the SAME admission,
                // leaving the actual unfinished cursor available for planning.
                std::future::poll_fn(|cx| {
                    assert!(std::future::Future::poll(foreground.as_mut(), cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                assert!(reconciler.state.discovery.lock().unwrap().cycle_open);
                page.wait().await;
            }
            let foreground = tokio::spawn(foreground);
            pass.wait().await;
            // The foreground owns the real state, not a finished/default pass.
            assert!(!reconciler.state.discovery.lock().unwrap().cycle_open);
            assert_eq!(
                store
                    .get_task_run(&runs[target].id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Running
            );
            if registration {
                runtime
                    .register_executor(Arc::new(CompletingRecovery))
                    .await;
            }
            // This notification comes from polling the real semaphore to
            // Pending, after the owner's pre-admission observation was taken.
            tokio::time::timeout(
                Duration::from_secs(10),
                reconciler.state.drain_waiting.notified(),
            )
            .await
            .unwrap();
            pass.wait().await;
            let report = foreground.await.unwrap().unwrap();
            assert_eq!(report.recovered_events, 0, "quiet foreground page");
            assert_eq!(report.repaired_terminal_occurrences, 0);
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if store
                        .get_task_run(&runs[target].id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        == TaskRunStatus::Succeeded
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("handoff must recover without a later producer");
            assert_eq!(
                store
                    .get_task_run(&runs[target].id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Succeeded,
                "{window}: useful recovery before any later write or kick"
            );
            // Retire unavailable Workflow history only AFTER the useful outcome.
            let admission = reconciler.work.drain().await.unwrap();
            for (index, run) in runs.iter().enumerate() {
                if index != target {
                    TaskExecutionHandle::new(
                        store.clone(),
                        runtime.event_bus(),
                        run.task_id.clone(),
                        run.id.clone(),
                    )
                    .cancel_run(Some("retire handoff fixture".into()), now)
                    .await
                    .unwrap();
                }
            }
            drop(admission);
            tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
                .await
                .unwrap();
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert!(observer.reads.lock().unwrap().is_empty(), "{window}");
            assert!(observer.writes.lock().unwrap().is_empty(), "{window}");
            tokio::time::resume();
            worker.abort();
            let _ = worker.await;
        }
    }

    // Each case crosses a real finite-page handoff; old tests used one Run and
    // only a future wait, so neither an expired observation nor old-key wake
    // could be lost behind an active cursor.
    #[tokio::test]
    async fn live_pages_retain_crossed_leases_and_external_rescans_without_polling() {
        use std::sync::atomic::Ordering;
        for cause in [
            "lease_crossing",
            "own_lease_crossing",
            "executor_registration",
            "lower_heartbeat",
        ] {
            let (_directory, store, runtime, observer) = live_fixture().await;
            let now = crate::service::now_timestamp_secs();
            let mut runs = Vec::new();
            for _ in 0..130 {
                let response = runtime
                    .maintenance_service()
                    .create_task(
                        crate::TaskCreateContext::default(),
                        create_params(TaskTriggerSpec::Immediate),
                    )
                    .await
                    .unwrap();
                let run = response.run.unwrap();
                TaskExecutionHandle::new(
                    store.clone(),
                    runtime.event_bus(),
                    response.task.id,
                    run.id.clone(),
                )
                .mark_started(now - 1)
                .await
                .unwrap();
                runs.push(run);
            }
            runs.sort_by(|a, b| a.id.cmp(&b.id));
            let first = runs.remove(0);
            // Same supported System executor, with live foreign leases for
            // the quiet tail. Registration must revisit the early unleased
            // target BEFORE those unrelated far-future leases expire.
            let db = store.database_connection();
            for run in &runs {
                let row = store
                    .reserve_execution_for_run(&run.id, TaskExecutorKind::System, now - 1)
                    .await
                    .unwrap();
                store
                    .claim_execution_at(&row.id, "other-tail", now - 1, now + 3600)
                    .await
                    .unwrap()
                    .unwrap();
            }
            let fractional = store
                .load_execution_for_run(&runs[0].id)
                .await
                .unwrap()
                .unwrap();
            {
                use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
                let deadline = chrono::DateTime::from_timestamp(now + 120, 1)
                    .unwrap()
                    .fixed_offset();
                pioneer_entity::task_run_execution::Entity::update_many()
                    .filter(
                        pioneer_entity::task_run_execution::Column::Id.eq(fractional.id.clone()),
                    )
                    .col_expr(
                        pioneer_entity::task_run_execution::Column::LeaseUntil,
                        sea_orm::sea_query::Expr::value(deadline),
                    )
                    .exec(&db)
                    .await
                    .unwrap();
            }
            assert_eq!(
                store
                    .task_live_lease_point(&fractional.id, now - 2)
                    .await
                    .unwrap(),
                Some(now + 121)
            );
            // Restored terminal history must consume raw inputs but contribute
            // no clock even with a future execution lease. The real owner below
            // must traverse it before publishing its finite minimum.
            let mut terminal_execution_ids = Vec::new();
            for _ in 0..65 {
                let response = runtime
                    .maintenance_service()
                    .create_task(
                        crate::TaskCreateContext::default(),
                        create_params(TaskTriggerSpec::Immediate),
                    )
                    .await
                    .unwrap();
                let run = response.run.unwrap();
                let handle = TaskExecutionHandle::new(
                    store.clone(),
                    runtime.event_bus(),
                    run.task_id.clone(),
                    run.id.clone(),
                );
                handle.mark_started(now - 1).await.unwrap();
                let execution = store
                    .reserve_execution_for_run(&run.id, TaskExecutorKind::System, now - 1)
                    .await
                    .unwrap();
                store
                    .claim_execution_at(&execution.id, "terminal-history", now - 1, now + 7200)
                    .await
                    .unwrap()
                    .unwrap();
                handle
                    .cancel_run(Some("restored terminal history".into()), now)
                    .await
                    .unwrap();
                // Restore a stale execution receipt after canonical Run
                // terminalization, to exercise the independent live-Run guard.
                {
                    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
                    pioneer_entity::task_run_execution::Entity::update_many()
                        .filter(
                            pioneer_entity::task_run_execution::Column::Id.eq(execution.id.clone()),
                        )
                        .col_expr(
                            pioneer_entity::task_run_execution::Column::WorkerId,
                            sea_orm::sea_query::Expr::value("terminal-history"),
                        )
                        .col_expr(
                            pioneer_entity::task_run_execution::Column::LeaseUntil,
                            sea_orm::sea_query::Expr::value(
                                chrono::DateTime::from_timestamp(now + 7200, 0)
                                    .unwrap()
                                    .fixed_offset(),
                            ),
                        )
                        .exec(&db)
                        .await
                        .unwrap();
                }
                terminal_execution_ids.push(execution.id);
            }
            let (keys, upper) = store.task_live_lease_keys(None, None, 64).await.unwrap();
            assert_eq!(keys.len(), 64);
            assert!(keys.last() < upper.as_ref());
            for id in terminal_execution_ids {
                assert_eq!(
                    store.task_live_lease_point(&id, now - 2).await.unwrap(),
                    None
                );
            }
            let reconciler = runtime.live_owner_for_test();
            reconciler.state.clock.store(
                if cause == "own_lease_crossing" {
                    now - TASK_EXECUTION_LEASE_SECONDS - 1
                } else if cause == "lease_crossing" {
                    now - 1
                } else {
                    now
                },
                Ordering::Release,
            );
            let execution = if cause == "own_lease_crossing" {
                runtime
                    .register_executor(Arc::new(LeaveFirstClaim(
                        std::sync::atomic::AtomicBool::new(true),
                    )))
                    .await;
                None
            } else if cause != "executor_registration" {
                runtime
                    .register_executor(Arc::new(CompletingRecovery))
                    .await;
                let row = store
                    .reserve_execution_for_run(&first.id, TaskExecutorKind::System, now - 1)
                    .await
                    .unwrap();
                let lease = if cause == "lease_crossing" {
                    now
                } else {
                    now + 3600
                };
                store
                    .claim_execution_at(&row.id, "other", now - 1, lease)
                    .await
                    .unwrap()
                    .unwrap();
                Some(row)
            } else {
                None
            };
            // Consume setup semantic hints before discovery, as a settled
            // producer batch does. Subsequent signals must come from real paths.
            store.task_live_changes().take();
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            *reconciler.state.page_barrier.lock().unwrap() = Some(barrier.clone());
            let worker = tokio::spawn(reconciler.clone().run());
            barrier.wait().await;
            assert!(reconciler.state.discovery.lock().unwrap().cycle_open);
            assert_eq!(
                store.get_task_run(&first.id).await.unwrap().unwrap().status,
                TaskRunStatus::Running
            );
            match cause {
                "lease_crossing" | "own_lease_crossing" => {
                    reconciler.state.clock.store(now, Ordering::Release)
                }
                "executor_registration" => {
                    runtime
                        .register_executor(Arc::new(CompletingRecovery))
                        .await
                }
                _ => {
                    CrudStore::new(db.clone())
                        .heartbeat_execution(&execution.unwrap().id, now, Some(now - 1))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(
                        !store.task_live_changes().pending(),
                        "lower producer has only a generation hint"
                    );
                }
            }
            barrier.wait().await;
            tokio::time::timeout(
                Duration::from_secs(10),
                reconciler.state.future_observed.notified(),
            )
            .await
            .unwrap();
            assert_eq!(
                store.get_task_run(&first.id).await.unwrap().unwrap().status,
                TaskRunStatus::Succeeded,
                "{cause}"
            );
            assert_eq!(
                reconciler
                    .state
                    .discovery
                    .lock()
                    .unwrap()
                    .lease_nearest
                    .as_ref()
                    .map(|(_, at)| *at),
                Some(now + 121),
                "full-resolution minimum survived the finite raw pass"
            );
            // The useful target outcome above preceded ANY later writes/kicks.
            // Now retire the independently leased fixture tail through the real
            // terminal producer, then prove settled Empty has no SQL timer.
            let admission = reconciler.work.drain().await.unwrap();
            for run in &runs {
                TaskExecutionHandle::new(
                    store.clone(),
                    runtime.event_bus(),
                    run.task_id.clone(),
                    run.id.clone(),
                )
                .cancel_run(Some("retire fixture tail".into()), now)
                .await
                .unwrap();
            }
            drop(admission);
            tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
                .await
                .unwrap();
            observer.reads.lock().unwrap().clear();
            observer.writes.lock().unwrap().clear();
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(300)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert!(
                observer.reads.lock().unwrap().is_empty(),
                "{cause}: settled history is dormant"
            );
            assert!(observer.writes.lock().unwrap().is_empty());
            tokio::time::resume();
            worker.abort();
            let _ = worker.await;
        }
    }

    #[tokio::test]
    async fn real_live_owner_parks_and_recovers_at_the_retained_execution_lease() {
        let (_directory, store, runtime, observer) = live_fixture().await;
        let executors = Arc::new(TaskExecutorRegistry::new());
        executors.register(Arc::new(CompletingRecovery)).await;
        let scheduler = TaskScheduler::new(store.clone(), runtime.event_bus(), executors.clone());
        let reconciler = Arc::new(TaskStartupReconciler::new(
            store.clone(),
            runtime.event_bus(),
            executors,
            scheduler.handle(),
        ));
        let worker = tokio::spawn(reconciler.clone().run());
        tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
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
        // Hold the real independent quantum admission while a supported producer
        // commits birth, status and lease. No payload-bearing bus replay or kick.
        let admission = reconciler.work.drain().await.unwrap();
        let task = runtime
            .service()
            .create_task(
                crate::TaskCreateContext::default(),
                create_params(TaskTriggerSpec::Immediate),
            )
            .await
            .unwrap();
        let run = task.run.unwrap();
        let now = crate::service::now_timestamp_secs();
        TaskExecutionHandle::new(
            store.clone(),
            runtime.event_bus(),
            task.task.id,
            run.id.clone(),
        )
        .mark_started(now)
        .await
        .unwrap();
        let execution = store
            .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::System, now)
            .await
            .unwrap();
        store
            .claim_execution_at(&execution.id, "prior-owner", now, now + 3)
            .await
            .unwrap()
            .unwrap();
        drop(admission);
        tokio::time::timeout(
            Duration::from_secs(10),
            reconciler.state.future_observed.notified(),
        )
        .await
        .unwrap();
        assert_eq!(
            store.get_task_run(&run.id).await.unwrap().unwrap().status,
            TaskRunStatus::Running
        );
        tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
            .await
            .unwrap();
        assert_eq!(
            store.get_task_run(&run.id).await.unwrap().unwrap().status,
            TaskRunStatus::Succeeded
        );
        assert!(
            observer
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|class| *class == SqliteReadClass::Maintenance)
        );
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            observer.reads.lock().unwrap().is_empty(),
            "settled history stays dormant"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        worker.abort();
        let _ = worker.await;
    }

    #[tokio::test]
    async fn live_owner_real_claim_failure_retains_retry_across_wall_rollback() {
        use pioneer_protocol::TaskOccurrenceStatus;
        use sea_orm::ConnectionTrait;
        let (_directory, store, runtime, _observer) = live_fixture().await;
        let setup_now = crate::service::now_timestamp_secs();
        let now = setup_now + 7200;
        // Independent lease which would win the old one-clock minimum.
        let response = runtime
            .maintenance_service()
            .create_task(
                crate::TaskCreateContext::default(),
                create_params(TaskTriggerSpec::Immediate),
            )
            .await
            .unwrap();
        let leased_run = response.run.unwrap();
        TaskExecutionHandle::new(
            store.clone(),
            runtime.event_bus(),
            response.task.id,
            leased_run.id.clone(),
        )
        .mark_started(now - 1)
        .await
        .unwrap();
        let execution = store
            .reserve_execution_for_run(&leased_run.id, TaskExecutorKind::System, now - 1)
            .await
            .unwrap();
        store
            .claim_execution_at(&execution.id, "mixed-clock-other", now - 1, now + 2)
            .await
            .unwrap()
            .unwrap();
        runtime
            .register_executor(Arc::new(CompletingRecovery))
            .await;
        let response = runtime
            .maintenance_service()
            .create_task(
                crate::TaskCreateContext::default(),
                create_params(TaskTriggerSpec::ScheduledAt {
                    scheduled_at: now - 7200,
                    timezone: Some("UTC".into()),
                    catch_up_policy: None,
                }),
            )
            .await
            .unwrap();
        runtime.process_due_once(now - 7200).await.unwrap();
        let terminal = runtime
            .maintenance_service()
            .get_task(pioneer_protocol::TaskGetParams {
                task_id: response.task.id,
            })
            .await
            .unwrap();
        let repaired_run = terminal.runs[0].id.clone();
        assert_eq!(terminal.runs[0].status, TaskRunStatus::Succeeded);
        let db = store.database_connection();
        // A real historical mismatch plus a supported typed queue producer.
        db.execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "UPDATE task_occurrence_contract SET status='running' WHERE run_id=?",
            [repaired_run.clone().into()],
        ))
        .await
        .unwrap();
        let mut occurrence = store
            .get_task_occurrence_contract_by_run(&repaired_run)
            .await
            .unwrap()
            .unwrap();
        occurrence.status = TaskOccurrenceStatus::Recovering;
        store
            .upsert_task_occurrence_contract(&occurrence, now - 7200)
            .await
            .unwrap();
        let reconciler = runtime.live_owner_for_test();
        reconciler
            .work
            .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        // Reject BOTH the claim and its exact-snapshot deferral. Error is real,
        // row-local and swallowed by the successful reconciliation quantum.
        db.execute_unprepared("CREATE TRIGGER clock_claim_failure BEFORE UPDATE ON task_occurrence_reconcile_pending BEGIN SELECT RAISE(ABORT,'clock claim storage fault'); END").await.unwrap();
        tokio::time::pause();
        let hold_clock = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        let report = reconciler.reconcile(now).await.unwrap();
        assert_eq!(report.repaired_terminal_occurrences, 0);
        assert!(reconciler.state.occurrence_retry.lock().unwrap().is_some());
        // Recover storage BEFORE the final successful cooling-down quantum.
        db.execute_unprepared("DROP TRIGGER clock_claim_failure")
            .await
            .unwrap();
        let worker = tokio::spawn(reconciler.clone().run());
        tokio::time::timeout(
            Duration::from_secs(10),
            reconciler.state.future_observed.notified(),
        )
        .await
        .unwrap();
        assert!(reconciler.state.occurrence_retry.lock().unwrap().is_some());
        // The producer dates its ticket using real UTC, independently of the
        // contract timestamp. Prove that rollback retains actual eligibility.
        use sea_orm::EntityTrait;
        let ticket =
            pioneer_entity::task_occurrence_reconcile_pending::Entity::find_by_id(&repaired_run)
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert!(ticket.next_attempt_at <= now - 3600);
        // No more writes/kicks: Wake's eligibility and the reconciler use this
        // same wall clock; the occurrence ticket predates the rollback.
        reconciler.work.set_wall_clock_for_test(
            std::time::UNIX_EPOCH + Duration::from_secs((now - 3600) as u64),
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..128 {
            let row = store
                .get_task_occurrence_contract_by_run(&repaired_run)
                .await
                .unwrap()
                .unwrap();
            if row.status == TaskOccurrenceStatus::Delivered {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            store
                .get_task_occurrence_contract_by_run(&repaired_run)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskOccurrenceStatus::Delivered
        );
        assert_eq!(
            store
                .get_task_run(&leased_run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunStatus::Running,
            "useful occurrence retry is independent of the rolled-back lease clock"
        );
        worker.abort();
        let _ = worker.await;
        hold_clock.abort();
        tokio::time::resume();
    }
    #[tokio::test]
    async fn live_owner_point_decode_retry_uses_monotonic_budget_with_independent_wall_lease() {
        use pioneer_entity::{task_run as run_row, task_trigger as trigger_row};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
        for forward in [false, true] {
            let (_directory, store, runtime, observer) = live_fixture().await;
            let now = crate::service::now_timestamp_secs() + 7200;
            let runs = ordered_live_runs(&store, &runtime, 65, 0, now - 7200).await;
            let response = runtime
                .maintenance_service()
                .create_task(
                    crate::TaskCreateContext::default(),
                    create_params(TaskTriggerSpec::Immediate),
                )
                .await
                .unwrap();
            let leased = response.run.unwrap();
            TaskExecutionHandle::new(
                store.clone(),
                runtime.event_bus(),
                response.task.id,
                leased.id.clone(),
            )
            .mark_started(now - 1)
            .await
            .unwrap();
            let execution = store
                .reserve_execution_for_run(&leased.id, TaskExecutorKind::System, now - 1)
                .await
                .unwrap();
            store
                .claim_execution_at(&execution.id, "point-clock-foreign", now - 1, now + 2)
                .await
                .unwrap()
                .unwrap();
            runtime
                .register_executor(Arc::new(CompletingRecovery))
                .await;
            let db = store.database_connection();
            run_row::Entity::update_many()
                .col_expr(run_row::Column::ResultJson, Expr::value("{"))
                .filter(run_row::Column::Id.eq(&runs[0].id))
                .exec(&db)
                .await
                .unwrap();
            let trigger_id = runs[0].trigger_id.as_ref().unwrap();
            let trigger = trigger_row::Entity::find_by_id(trigger_id)
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            trigger_row::Entity::update_many()
                .col_expr(trigger_row::Column::SpecJson, Expr::value("{"))
                .filter(trigger_row::Column::Id.eq(trigger_id))
                .exec(&db)
                .await
                .unwrap();
            let reconciler = runtime.live_owner_for_test();
            reconciler
                .work
                .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
            let ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            tokio::time::pause();
            // Actual selected model decodes fail; successful raw traversal retains the healthy tail.
            // Visit both raw pages: arbitrary trigger IDs need not put the
            // trigger fault on the same page as the ordered Run fault.
            for _ in 0..4 {
                let _ = reconciler.reconcile(now).await;
                let state = reconciler.state.discovery.lock().unwrap();
                if state.retry_at.contains_key(&runs[0].id)
                    && state.trigger_retry.contains_key(trigger_id)
                {
                    break;
                }
            }
            assert!(
                reconciler
                    .state
                    .discovery
                    .lock()
                    .unwrap()
                    .trigger_retry
                    .contains_key(trigger_id)
            );
            assert!(
                reconciler
                    .state
                    .discovery
                    .lock()
                    .unwrap()
                    .retry_at
                    .contains_key(&runs[0].id)
            );
            assert!(
                reconciler.state.occurrence_retry.lock().unwrap().is_none(),
                "not the occurrence claim-error branch"
            );
            let before_repair = reconciler.work.observe();
            // Direct fixture repair intentionally emits no supported producer hint.
            run_row::Entity::update_many()
                .col_expr(run_row::Column::ResultJson, Expr::value(None::<String>))
                .filter(run_row::Column::Id.eq(&runs[0].id))
                .exec(&db)
                .await
                .unwrap();
            trigger_row::Entity::update_many()
                .col_expr(
                    trigger_row::Column::SpecJson,
                    Expr::value(trigger.spec_json),
                )
                .filter(trigger_row::Column::Id.eq(trigger_id))
                .exec(&db)
                .await
                .unwrap();
            assert_eq!(reconciler.work.observe(), before_repair);
            tokio::time::resume();
            let worker = tokio::spawn(reconciler.clone().run());
            tokio::time::timeout(
                Duration::from_secs(10),
                reconciler.state.future_observed.notified(),
            )
            .await
            .unwrap();
            assert_eq!(
                store
                    .get_task_run(&runs[0].id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Running
            );
            assert_eq!(
                store
                    .get_task_run(&leased.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                TaskRunStatus::Running
            );
            tokio::time::pause();
            // Same source drives Wake waiting AND reconciler eligibility. Do not set state.clock.
            reconciler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH
                    + Duration::from_secs((if forward { now + 200 } else { now - 3600 }) as u64),
            );
            tokio::time::advance(Duration::from_secs(5)).await;
            tokio::time::resume();
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if store
                        .get_task_run(&runs[0].id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        == TaskRunStatus::Succeeded
                        && (!forward
                            || store
                                .get_task_run(&leased.id)
                                .await
                                .unwrap()
                                .unwrap()
                                .status
                                == TaskRunStatus::Succeeded)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                store
                    .get_task_run(&leased.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                if forward {
                    TaskRunStatus::Succeeded
                } else {
                    TaskRunStatus::Running
                },
                "real lease stays a wall deadline independent of point retry"
            );
            assert!(
                !reconciler
                    .state
                    .discovery
                    .lock()
                    .unwrap()
                    .retry_at
                    .contains_key(&runs[0].id)
            );
            // All genuine clocks are retired only AFTER useful recovery.
            // A stale trigger error budget would prevent Dormant after rollback.
            if !forward {
                TaskExecutionHandle::new(
                    store.clone(),
                    runtime.event_bus(),
                    leased.task_id.clone(),
                    leased.id.clone(),
                )
                .cancel_run(Some("retire independent lease".into()), now)
                .await
                .unwrap();
            }
            // Recovery above is proved under the rolled-back clock. Its
            // completion/cancellation then creates real occurrence obligations
            // at commit time; restore business time before requiring EOF.
            reconciler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH
                    + Duration::from_secs(crate::service::now_timestamp_secs() as u64),
            );
            tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
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
            worker.abort();
            let _ = worker.await;
            ticker.abort();
        }
    }

    // The two typed finite-reader boundaries share this real live-owner proof;
    // history/JSON poison retain their independent existing regressions below.
    #[tokio::test]
    async fn typed_finite_pages_preserve_live_tail_and_monotonic_retry() {
        use pioneer_entity::{task_run as run_row, task_trigger as trigger_row};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
        for trigger_fault in [false, true] {
            let (_directory, store, runtime, observer) = live_fixture().await;
            let now = crate::service::now_timestamp_secs();
            let db = store.database_connection();
            let mut inputs = Vec::new();
            for _ in 0..65 {
                let spec = if trigger_fault {
                    TaskTriggerSpec::ScheduledAt {
                        scheduled_at: now + 86400,
                        timezone: None,
                        catch_up_policy: None,
                    }
                } else {
                    TaskTriggerSpec::Immediate
                };
                let response = runtime
                    .maintenance_service()
                    .create_task(crate::TaskCreateContext::default(), create_params(spec))
                    .await
                    .unwrap();
                let key = if trigger_fault {
                    trigger_row::Entity::update_many()
                        .filter(trigger_row::Column::Id.eq(&response.trigger.id))
                        .col_expr(
                            trigger_row::Column::NextFireAt,
                            Expr::value(None::<chrono::DateTime<chrono::FixedOffset>>),
                        )
                        .exec(&db)
                        .await
                        .unwrap();
                    response.trigger.id
                } else {
                    let run = response.run.unwrap();
                    TaskExecutionHandle::new(
                        store.clone(),
                        runtime.event_bus(),
                        response.task.id.clone(),
                        run.id.clone(),
                    )
                    .mark_started(now - 8001)
                    .await
                    .unwrap();
                    let execution = store
                        .reserve_execution_for_run(&run.id, TaskExecutorKind::System, now - 8001)
                        .await
                        .unwrap();
                    store
                        .claim_execution_at(
                            &execution.id,
                            "expired-fixture",
                            now - 8001,
                            now - 8000,
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    run.id
                };
                inputs.push((response.task.id, key));
            }
            inputs.sort_by(|a, b| a.1.cmp(&b.1));
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
                .register_executor(Arc::new(CompletingRecovery))
                .await;
            let reconciler = runtime.live_owner_for_test();
            reconciler
                .work
                .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            *reconciler.state.pass_barrier.lock().unwrap() = Some(barrier.clone());
            let worker = tokio::spawn(reconciler.clone().run());
            let ticker = tokio::spawn(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
            let mut reached = false;
            for _ in 0..12 {
                tokio::time::timeout(Duration::from_secs(30), barrier.wait())
                    .await
                    .unwrap();
                let healthy = if trigger_fault {
                    store
                        .get_task_events(&inputs.last().unwrap().0, None)
                        .await
                        .unwrap()
                        .events
                        .iter()
                        .any(|event| {
                            matches!(
                                &event.payload,
                                TaskEventPayload::TaskRecovered { run_id: None, .. }
                            )
                        })
                } else {
                    store
                        .get_task_run(&inputs.last().unwrap().1)
                        .await
                        .unwrap()
                        .unwrap()
                        .status
                        == TaskRunStatus::Succeeded
                };
                if healthy {
                    reached = true;
                    break;
                }
                *reconciler.state.pass_barrier.lock().unwrap() = Some(barrier.clone());
                barrier.wait().await;
                {
                    let admission = reconciler.work.drain().await.unwrap();
                    drop(admission);
                }
                tokio::time::pause();
                tokio::time::advance(OCCURRENCE_STORAGE_BACKOFF).await;
                tokio::time::resume();
            }
            assert!(
                reached,
                "healthy later page recovers before repairing poison"
            );
            if trigger_fault {
                assert!(
                    store
                        .get_task_events(&inputs[1].0, None)
                        .await
                        .unwrap()
                        .events
                        .iter()
                        .any(|event| matches!(
                            &event.payload,
                            TaskEventPayload::TaskRecovered { run_id: None, .. }
                        ))
                );
            } else {
                assert_eq!(
                    store
                        .get_task_run(&inputs[1].1)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    TaskRunStatus::Succeeded
                );
            }
            tokio::time::pause();
            barrier.wait().await;
            let admission = reconciler.work.drain().await.unwrap();
            {
                let state = reconciler.state.discovery.lock().unwrap();
                let retries = if trigger_fault {
                    &state.trigger_retry
                } else {
                    &state.retry_at
                };
                assert!(
                    retries
                        .get(&inputs[0].1)
                        .is_some_and(|at| *at > Instant::now()),
                    "explicit retained monotonic row retry"
                );
            }
            let before = reconciler.work.observe();
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
                reconciler.work.observe(),
                before,
                "repair emits no producer hint"
            );
            reconciler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH + Duration::from_secs((now - 3600) as u64),
            );
            drop(admission);
            tokio::time::advance(OCCURRENCE_STORAGE_BACKOFF).await;
            tokio::time::resume();
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let recovered = if trigger_fault {
                        store
                            .get_task_events(&inputs[0].0, None)
                            .await
                            .unwrap()
                            .events
                            .iter()
                            .any(|event| {
                                matches!(
                                    &event.payload,
                                    TaskEventPayload::TaskRecovered { run_id: None, .. }
                                )
                            })
                    } else {
                        store
                            .get_task_run(&inputs[0].1)
                            .await
                            .unwrap()
                            .unwrap()
                            .status
                            == TaskRunStatus::Succeeded
                    };
                    if recovered {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if trigger_fault {
                let events = store.get_task_events(&inputs[0].0, None).await.unwrap();
                assert_eq!(
                    events
                        .events
                        .iter()
                        .filter(|event| matches!(
                            &event.payload,
                            TaskEventPayload::TaskRecovered { run_id: None, .. }
                        ))
                        .count(),
                    1
                );
            } else {
                assert_eq!(
                    store
                        .get_task_run(&inputs[0].1)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    TaskRunStatus::Succeeded
                );
            }
            // Completion creates real occurrence reconciliation clocks at its
            // commit time. After proving repair under rollback, let those
            // independent wall obligations settle before asserting Dormant.
            reconciler.work.set_wall_clock_for_test(
                std::time::UNIX_EPOCH
                    + Duration::from_secs(crate::service::now_timestamp_secs() as u64),
            );
            tokio::time::timeout(Duration::from_secs(30), reconciler.state.parked.notified())
                .await
                .unwrap();
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
    async fn live_owner_dormant_history_fault_retains_retry_until_diagnostic_then_parks() {
        use pioneer_entity::{task_event as event_row, task_trigger as trigger_row};
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
        let (_directory, store, runtime, observer) = live_fixture().await;
        let now = crate::service::now_timestamp_secs();
        let mut tasks = Vec::new();
        let projector = TaskProjector::new(store.clone());
        let db = store.database_connection();
        for index in 0..65 {
            let response = runtime
                .maintenance_service()
                .create_task(
                    crate::TaskCreateContext::default(),
                    create_params(TaskTriggerSpec::ScheduledAt {
                        scheduled_at: now + 86_400,
                        timezone: None,
                        catch_up_policy: None,
                    }),
                )
                .await
                .unwrap();
            let trigger_id = format!("{index:021}");
            trigger_row::Entity::update_many()
                .filter(trigger_row::Column::Id.eq(&response.trigger.id))
                .col_expr(trigger_row::Column::Id, Expr::value(trigger_id.clone()))
                .col_expr(
                    trigger_row::Column::NextFireAt,
                    Expr::value(None::<chrono::DateTime<chrono::FixedOffset>>),
                )
                .exec(&db)
                .await
                .unwrap();
            // Every healthy row, including the second page, is already audited.
            // Its successful history check emits no mutation/rescan hint.
            if index != 0 {
                projector
                    .append_event(
                        TaskEventPayload::TaskRecovered {
                            task_id: response.task.id.clone(),
                            run_id: None,
                            message: DORMANT_TRIGGER_RECOVERY_MESSAGE.to_owned(),
                            recovered_at: now,
                        },
                        now,
                    )
                    .await
                    .unwrap();
            }
            tasks.push((response.task.id, trigger_id));
        }
        // Settle the independent occurrence bootstrap without auditing the
        // dormant trigger. Its missing diagnostic is the fault's real target.
        let reconciler = runtime.live_owner_for_test();
        reconciler
            .work
            .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        for _ in 0..tasks.len() + 32 {
            reconciler
                .reconcile_terminal_occurrences(now)
                .await
                .unwrap();
            if store
                .next_task_occurrence_reconcile_at(now)
                .await
                .unwrap()
                .is_none_or(|at| at > now)
            {
                break;
            }
        }
        let bad_event = event_row::Entity::find()
            .filter(event_row::Column::TaskId.eq(&tasks[0].0))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        // Corrupt only historical event hydration/decode, not Trigger::SpecJson.
        // Use the existing scoped fixture mutation seam, never a second writer.
        event_row::Entity::update_many()
            .filter(event_row::Column::Id.eq(&bad_event.id))
            .col_expr(event_row::Column::PayloadJson, Expr::value("{"))
            .col_expr(
                event_row::Column::PayloadJsonPayloadId,
                Expr::value(None::<String>),
            )
            .exec(&db)
            .await
            .unwrap();
        assert!(store.get_task_events(&tasks[0].0, None).await.is_err());
        let hydrated = store
            .task_scheduler_trigger_partial(&tasks[0].1)
            .await
            .unwrap();
        assert!(hydrated.failures.is_empty());
        assert_eq!(hydrated.records.len(), 1, "valid dormant trigger Model");
        let reconciler = runtime.live_owner_for_test();
        reconciler
            .work
            .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        let ticker = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        tokio::time::pause();
        assert!(reconciler.reconcile(now).await.is_err());
        let retry = reconciler
            .state
            .discovery
            .lock()
            .unwrap()
            .trigger_retry
            .get(&tasks[0].1)
            .copied()
            .expect("history error retains ID");
        let tail = reconciler.reconcile(now).await.unwrap();
        assert_eq!(
            tail.active_triggers, 1,
            "healthy second raw page progressed"
        );
        assert_eq!(
            tail.recovered_events, 0,
            "no tail mutation can hide lost retry"
        );
        assert!(!reconciler.state.discovery.lock().unwrap().cycle_open);
        assert!(matches!(reconciler.next_plan(now).await.unwrap(),
            pioneer_sqlite::background::Plan::MonotonicDeadline(at) if at == retry));
        // Retry successfully hydrates the same Model but history still fails.
        tokio::time::advance(OCCURRENCE_STORAGE_BACKOFF).await;
        assert!(reconciler.reconcile(now).await.is_err());
        assert!(
            reconciler
                .state
                .discovery
                .lock()
                .unwrap()
                .trigger_retry
                .contains_key(&tasks[0].1)
        );
        // Complete this retained healthy tail before launching the physical owner.
        reconciler.reconcile(now).await.unwrap();
        tokio::time::resume();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        *reconciler.state.pass_barrier.lock().unwrap() = Some(barrier.clone());
        let worker = tokio::spawn(reconciler.clone().run());
        tokio::time::timeout(Duration::from_secs(10), barrier.wait())
            .await
            .unwrap();
        // The real owner's first selected page has failed BEFORE a mutation guard.
        // Repair at its existing rendezvous, after releasing all DB capacity.
        let before_repair = reconciler.work.observe();
        event_row::Entity::update_many()
            .filter(event_row::Column::Id.eq(&bad_event.id))
            .col_expr(
                event_row::Column::PayloadJson,
                Expr::value(bad_event.payload_json),
            )
            .col_expr(
                event_row::Column::PayloadJsonPayloadId,
                Expr::value(bad_event.payload_json_payload_id),
            )
            .exec(&db)
            .await
            .unwrap();
        assert_eq!(
            reconciler.work.observe(),
            before_repair,
            "history repair deliberately emits no producer hint"
        );
        tokio::time::pause();
        reconciler.work.set_wall_clock_for_test(
            std::time::UNIX_EPOCH + Duration::from_secs((now - 3600) as u64),
        );
        barrier.wait().await;
        // Let the failed quantum publish its outer monotonic retry before advancing.
        {
            let admission = reconciler.work.drain().await.unwrap();
            drop(admission);
        }
        tokio::time::advance(OCCURRENCE_STORAGE_BACKOFF).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
            .await
            .unwrap();
        for (task_id, _) in &tasks {
            let history = store.get_task_events(task_id, None).await.unwrap();
            assert_eq!(
                history
                    .events
                    .iter()
                    .filter(|event| matches!(
                        &event.payload, TaskEventPayload::TaskRecovered {
                            run_id: None, message, ..
                        } if message == DORMANT_TRIGGER_RECOVERY_MESSAGE
                    ))
                    .count(),
                1,
                "diagnostic exactly once after repair, including faulty early key"
            );
        }
        assert!(
            reconciler
                .state
                .discovery
                .lock()
                .unwrap()
                .trigger_retry
                .is_empty()
        );
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(300)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            observer.reads.lock().unwrap().is_empty(),
            "settled SQL quiet"
        );
        assert!(observer.writes.lock().unwrap().is_empty());
        tokio::time::resume();
        worker.abort();
        let _ = worker.await;
        ticker.abort();
    }

    #[tokio::test]
    async fn late_own_claim_reopens_completed_lease_frontier_then_recovers_without_a_producer() {
        let (_directory, store, runtime, observer) = live_fixture().await;
        let now = crate::service::now_timestamp_secs();
        let runs = ordered_live_runs(&store, &runtime, 129, 128, now).await;
        // More than one execution page, all terminal and clock-ineligible.
        // The final System Run is on the THIRD Run page, after lease EOF.
        for _ in 0..65 {
            let response = runtime
                .maintenance_service()
                .create_task(
                    crate::TaskCreateContext::default(),
                    create_params(TaskTriggerSpec::Immediate),
                )
                .await
                .unwrap();
            let run = response.run.unwrap();
            let handle = TaskExecutionHandle::new(
                store.clone(),
                runtime.event_bus(),
                run.task_id.clone(),
                run.id.clone(),
            );
            handle.mark_started(now).await.unwrap();
            let execution = store
                .reserve_execution_for_run(&run.id, TaskExecutorKind::System, now)
                .await
                .unwrap();
            store
                .claim_execution_at(&execution.id, "terminal-history", now, now + 7200)
                .await
                .unwrap()
                .unwrap();
            handle
                .cancel_run(Some("terminal fixture history".into()), now)
                .await
                .unwrap();
        }
        let reconciler = runtime.live_owner_for_test();
        reconciler
            .work
            .set_wall_clock_for_test(std::time::UNIX_EPOCH + Duration::from_secs(now as u64));
        runtime
            .register_executor(Arc::new(LeaveFirstClaim(
                std::sync::atomic::AtomicBool::new(true),
            )))
            .await;
        let worker = tokio::spawn(reconciler.clone().run());
        tokio::time::timeout(
            Duration::from_secs(10),
            reconciler.state.future_observed.notified(),
        )
        .await
        .unwrap();
        assert_eq!(
            store
                .get_task_run(&runs[128].id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunStatus::Running
        );
        let execution = store
            .load_execution_for_run(&runs[128].id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .task_live_lease_point(&execution.id, now)
                .await
                .unwrap(),
            Some(now + TASK_EXECUTION_LEASE_SECONDS)
        );
        assert_eq!(
            reconciler
                .state
                .discovery
                .lock()
                .unwrap()
                .lease_nearest
                .as_ref()
                .map(|(_, at)| *at),
            Some(now + TASK_EXECUTION_LEASE_SECONDS),
            "late own claim survives the earlier lease EOF"
        );
        let ticker = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        observer.reads.lock().unwrap().clear();
        observer.writes.lock().unwrap().clear();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(30)).await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(observer.reads.lock().unwrap().is_empty());
        assert!(observer.writes.lock().unwrap().is_empty());
        // Only the genuine retained lease expires; no heartbeat/kick/write.
        reconciler.work.set_wall_clock_for_test(
            std::time::UNIX_EPOCH
                + Duration::from_secs((now + TASK_EXECUTION_LEASE_SECONDS + 1) as u64),
        );
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::time::resume();
        tokio::time::timeout(Duration::from_secs(10), reconciler.state.parked.notified())
            .await
            .unwrap();
        assert_eq!(
            store
                .get_task_run(&runs[128].id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunStatus::Succeeded
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
        worker.abort();
        let _ = worker.await;
        ticker.abort();
    }
}
