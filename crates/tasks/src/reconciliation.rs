use crate::TaskRuntimeResult;
use crate::event_bus::{TaskEventBus, TaskEventSubscription, TaskEventWakeDelivery};
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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
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
    terminal_occurrence_retry_at: Mutex<Option<Instant>>,
    scheduler_recovery: Mutex<SchedulerRecovery>,
}

/// Private startup/live discovery state. Never doubles as domain authority.
struct SchedulerRecovery {
    startup_complete: bool,
    events: TaskEventSubscription,
    dormant_cursor: Option<String>,
    dormant_upper: Option<String>,
    run_cursors: [Option<String>; 4],
    run_uppers: [Option<String>; 4],
    retry_at: HashMap<String, i64>,
    trigger_retry: HashMap<String, i64>,
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
        let events = event_bus.subscribe(Default::default());
        Self {
            store,
            projector,
            event_bus,
            executors,
            scheduler,
            terminal_occurrence_retry_at: Mutex::new(None),
            scheduler_recovery: Mutex::new(SchedulerRecovery {
                startup_complete: false,
                events,
                dormant_cursor: None,
                dormant_upper: None,
                run_cursors: Default::default(),
                run_uppers: Default::default(),
                retry_at: HashMap::new(),
                trigger_retry: Default::default(),
            }),
        }
    }

    pub async fn reconcile(&self, now: i64) -> TaskRuntimeResult<ReconciliationReport> {
        // Serializes discovery only; holds no DB capacity between calls. Keep
        // discovery incomplete on error/cancellation so an unknown outcome rebuilds.
        let mut recovery = self.scheduler_recovery.lock().await;
        let mut task_hints = std::collections::HashSet::new();
        let mut run_hints = std::collections::HashSet::new();
        for _ in 0..1024 {
            match recovery.events.try_recv() {
                Some(TaskEventWakeDelivery::Wake(wake)) => {
                    if let Some(run_id) = wake.run_id {
                        run_hints.insert(run_id);
                    } else {
                        task_hints.insert(wake.task_id);
                    }
                }
                Some(TaskEventWakeDelivery::Lagged(_)) => recovery.startup_complete = false,
                Some(TaskEventWakeDelivery::Closed) | None => break,
            }
        }
        let startup = !recovery.startup_complete;
        recovery.startup_complete = false;
        let mut discovery_errors = Vec::new();
        let mut unknown_discovery = false;
        let active_triggers = if startup {
            self.store.list_active_task_triggers_partial().await?
        } else {
            let (page, upper) = self
                .store
                .list_dormant_active_task_triggers_finite(
                    recovery.dormant_cursor.as_deref(),
                    recovery.dormant_upper.as_deref(),
                    64,
                )
                .await?;
            // SQL already bounds and orders the page. Equality is sufficient;
            // remote database collation need not match Rust byte ordering.
            let more = page.scanned == 64 && page.last.as_ref() != upper.as_ref();
            recovery.dormant_cursor = if more { page.last.clone() } else { None };
            recovery.dormant_upper = if more { upper } else { None };
            page
        };
        for row in &active_triggers.failures {
            recovery
                .trigger_retry
                .insert(row.id.clone(), now.saturating_add(5));
        }
        discovery_errors.extend(active_triggers.failures.into_iter().map(|row| {
            row.error
                .context(format!("scheduler row {} (task {})", row.id, row.task_id))
        }));
        let mut active_triggers = active_triggers.records;
        let mut trigger_retries = recovery
            .trigger_retry
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(id, at)| (*at, id.clone()))
            .collect::<Vec<_>>();
        trigger_retries.sort();
        for (_, id) in trigger_retries.into_iter().take(64) {
            recovery
                .trigger_retry
                .insert(id.clone(), now.saturating_add(5));
            let page = match self.store.task_scheduler_trigger_partial(&id).await {
                Ok(page) => page,
                Err(error) => {
                    unknown_discovery = true;
                    discovery_errors.push(error);
                    continue;
                }
            };
            if page.failures.is_empty() {
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
            let limit = if startup { 1024 } else { 64 };
            let after = if startup {
                None
            } else {
                recovery.run_cursors[index].as_deref()
            };
            let upper = if startup {
                None
            } else {
                recovery.run_uppers[index].as_deref()
            };
            let (page, upper) = if !startup && status == TaskRunStatus::Queued {
                // Future queue entries are durable deadlines, not unfinished
                // local dependencies. Filter ready_at in SQL before LIMIT.
                self.store
                    .list_scheduler_due_task_runs_finite(now, after, upper, limit)
                    .await?
            } else {
                self.store
                    .list_task_recovery_runs_finite(status, after, upper, limit)
                    .await?
            };
            let more = page.scanned == limit as usize && page.last.as_ref() != upper.as_ref();
            recovery.run_cursors[index] = if more { page.last.clone() } else { None };
            recovery.run_uppers[index] = if more { upper } else { None };
            for row in &page.failures {
                recovery
                    .retry_at
                    .insert(row.id.clone(), now.saturating_add(5));
            }
            discovery_errors.extend(page.failures.into_iter().map(|row| {
                row.error
                    .context(format!("scheduler row {} (task {})", row.id, row.task_id))
            }));
            for run in page.records {
                candidates.insert(run.id.clone(), run);
            }
        }
        // Hints address the owning task (a terminal sibling may free capacity).
        // Recovery runs are bounded; scheduler dispatch has its separate due set.
        for task_id in task_hints {
            for status in statuses {
                let page = match self
                    .store
                    .list_task_recovery_runs_for_task_partial(&task_id, status, 64)
                    .await
                {
                    Ok(page) => page,
                    Err(error) => {
                        unknown_discovery = true;
                        discovery_errors.push(error);
                        continue;
                    }
                };
                for row in &page.failures {
                    recovery
                        .retry_at
                        .insert(row.id.clone(), now.saturating_add(5));
                }
                discovery_errors.extend(page.failures.into_iter().map(|row| {
                    row.error
                        .context(format!("scheduler row {} (task {})", row.id, row.task_id))
                }));
                for run in page.records {
                    candidates.insert(run.id.clone(), run);
                }
            }
        }
        for run_id in run_hints {
            let page = match self.store.task_scheduler_run_partial(&run_id).await {
                Ok(page) => page,
                Err(error) => {
                    unknown_discovery = true;
                    discovery_errors.push(error);
                    recovery
                        .retry_at
                        .insert(run_id.clone(), now.saturating_add(5));
                    continue;
                }
            };
            if !page.failures.is_empty() {
                recovery
                    .retry_at
                    .insert(run_id.clone(), now.saturating_add(5));
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
        let mut local = recovery
            .retry_at
            .iter()
            .filter(|(_, retry_at)| **retry_at <= now)
            .map(|(id, retry_at)| (*retry_at, id.clone()))
            .collect::<Vec<_>>();
        local.sort();
        for (_, run_id) in local.into_iter().take(1024) {
            let page = match self.store.task_scheduler_run_partial(&run_id).await {
                Ok(page) => page,
                Err(error) => {
                    unknown_discovery = true;
                    discovery_errors.push(error);
                    recovery
                        .retry_at
                        .insert(run_id.clone(), now.saturating_add(5));
                    continue;
                }
            };
            if !page.failures.is_empty() {
                recovery
                    .retry_at
                    .insert(run_id.clone(), now.saturating_add(5));
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
        // Discovery is complete independently of row recovery and occurrence
        // repair. Preserve the keysets even if those later operations fail or
        // are cancelled. Retain every selected ID before the first side effect.
        recovery.startup_complete = !unknown_discovery;
        candidates.retain(|id, run| {
            if run.status == TaskRunStatus::Queued && run.ready_at.is_some_and(|ready| ready > now)
            {
                // The existing indexed due fallback rediscovers this row when
                // eligible, including an unpublished foreign deadline change.
                recovery.retry_at.remove(id);
                false
            } else {
                true
            }
        });
        let runs = candidates
            .into_values()
            .filter(|run| recovery.retry_at.get(&run.id).is_none_or(|at| *at <= now))
            .collect::<Vec<_>>();
        for run in &runs {
            recovery
                .retry_at
                .insert(run.id.clone(), now.saturating_add(5));
        }
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
                Ok(true) => recovered_events = recovered_events.saturating_add(1),
                Ok(false) => {}
                Err(error) => {
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
            // Retry local dependencies independently of whether a recovery event
            // was newly inserted. Existing events are not an empty-state proof.
            recovery
                .retry_at
                .insert(run.id.clone(), now.saturating_add(5));
            match self.recover_run(run, now).await {
                Ok(true) => recovered_events = recovered_events.saturating_add(1),
                Ok(false) => {}
                Err(error) => {
                    partial_only = false;
                    discovery_errors.push(error);
                }
            }
        }

        let (terminal_occurrence_mismatch_count, repaired_terminal_occurrences) =
            match self.reconcile_terminal_occurrences(now).await {
                Ok(counts) => counts,
                Err(error) => {
                    partial_only = false;
                    discovery_errors.push(error);
                    (0, 0)
                }
            };

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
        // This mutex only serializes quanta/backoff; it retains no DB capacity.
        // Durable locators, claims and checkpoints survive cancellation/restart.
        let mut retry_at = self.terminal_occurrence_retry_at.lock().await;
        if retry_at.is_some_and(|deadline| deadline > Instant::now()) {
            return Ok((0, 0));
        }
        let clock = || chrono::Utc::now().timestamp();
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
                *retry_at = Some(Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
                return Err(error);
            }
        };
        let audit_error = discovery.audit_error;
        storage_error |= audit_error.is_some();
        let mut changed = 0;
        for candidate in discovery.candidates {
            let store = self.store.clone();
            // Reuse the owned/abort-on-drop task boundary. A candidate panic or
            // storage failure cannot abort the rest of the selected quantum.
            let outcome = task_fresh_task(
                async move {
                    let Some(claim) = store
                        .claim_task_occurrence_reconcile(&candidate, &|| {
                            chrono::Utc::now().timestamp()
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
        *retry_at = storage_error.then(|| Instant::now() + OCCURRENCE_STORAGE_BACKOFF);
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
            if let Err(_error) = executor
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
