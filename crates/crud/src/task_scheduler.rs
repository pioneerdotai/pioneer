//! Scheduler reads inherit the scoped CrudStore reader handle. No writer or
//! transaction is entered for discovery, including empty fallback polls.
use crate::repositories::{task_run, task_trigger};
use crate::util::unix_to_datetime;
use crate::{CrudStore, task_run_from_db_model, task_trigger_from_db_model};
use anyhow::Result;
use pioneer_protocol::{TaskRun, TaskRunStatus, TaskTrigger};
use sea_orm::prelude::DateTimeWithTimeZone;

/// Coalesced continuation for actual Task transitions made by live recovery
/// itself. Its own ACK origin may suppress a wake, never a newly inserted Run.
#[derive(Default)]
pub struct TaskLiveChanges(std::sync::atomic::AtomicBool);
impl TaskLiveChanges {
    pub fn record(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn take(&self) -> bool {
        self.0.swap(false, std::sync::atomic::Ordering::AcqRel)
    }
    pub fn pending(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Bounded process-local scheduler hints shared by every store over the same
/// physical runtime. These locators convey no dispatch/claim authority.
#[derive(Default)]
pub struct TaskSchedulerChanges(std::sync::Mutex<SchedulerChanges>);
#[derive(Default)]
struct SchedulerChanges {
    revision: u64,
    reconstruct: bool,
    tasks: std::collections::BTreeMap<String, u64>,
    page: Option<(String, String)>,
    // The scheduler's existing arming observations live with the physical
    // runtime so committed producers can register a future schedule before a
    // delayed drain observes it. This is active schedule state, not event mail.
    arms: std::collections::BTreeMap<String, TriggerArm>,
    arms_by_task: std::collections::HashMap<String, std::collections::BTreeSet<String>>,
    arm_sweep: Option<ArmSweep>,
    released: bool,
    reconstruction: Option<(u64, i64)>,
}
struct ArmSweep {
    seen: std::collections::HashSet<String>,
    snapshot: u64,
    after: Option<String>,
    upper: Option<String>,
}
impl SchedulerChanges {
    fn set_arm(&mut self, id: String, arm: TriggerArm) {
        let task = arm.task_id.clone();
        self.remove_arm(&id);
        self.arms_by_task
            .entry(task)
            .or_default()
            .insert(id.clone());
        self.arms.insert(id, arm);
    }
    fn remove_arm(&mut self, id: &str) {
        if let Some(arm) = self.arms.remove(id) {
            if let Some(keys) = self.arms_by_task.get_mut(&arm.task_id) {
                keys.remove(id);
                if keys.is_empty() {
                    self.arms_by_task.remove(&arm.task_id);
                }
            }
        }
    }
}
struct TriggerArm {
    task_id: String,
    schedule: Option<TaskTrigger>,
    changed: u64,
}
fn same_schedule(left: &TaskTrigger, right: &TaskTrigger) -> bool {
    left.id == right.id
        && left.task_id == right.task_id
        && left.status == right.status
        && left.spec == right.spec
        && left.next_fire_at == right.next_fire_at
        && left.last_fire_at == right.last_fire_at
        && left.created_at == right.created_at
}
impl TaskSchedulerChanges {
    fn scheduled(&self, trigger_id: &str, task_id: &str, at: Option<i64>) {
        let now = crate::utc_now().timestamp();
        let mut state = self.0.lock().unwrap();
        state.revision = state
            .revision
            .checked_add(1)
            .expect("scheduler generation exhausted");
        let changed = state.revision;
        let schedule = state
            .arms
            .get(trigger_id)
            .and_then(|arm| arm.schedule.as_ref())
            .and_then(|old| {
                let mut updated = old.clone();
                updated.next_fire_at = at;
                updated.last_fire_at = None;
                updated.status = pioneer_protocol::TaskTriggerStatus::Active;
                (at.is_some_and(|at| at > now) || same_schedule(old, &updated)).then_some(updated)
            });
        // If no complete schedule has been observed, retain its version marker:
        // an older bootstrap page must not backdate this newly committed input.
        state.set_arm(
            trigger_id.to_owned(),
            TriggerArm {
                task_id: task_id.to_owned(),
                schedule,
                changed,
            },
        );
    }
    pub fn record_committed_events(&self, events: &[crate::AppendedTaskEvent]) -> bool {
        let mut changed = false;
        for event in events {
            if event.append_status.is_inserted() && task_event_affects_scheduler(&event.payload) {
                self.record(&event.task_id);
                register_trigger_event(self, &event.payload);
                use pioneer_protocol::TaskEventPayload::*;
                if matches!(
                    &event.payload,
                    RunCompleted { .. }
                        | RunFailed { .. }
                        | RunCancelled { .. }
                        | RunBlocked { .. }
                        | TaskCompleted { .. }
                        | TaskFailed { .. }
                        | TaskCancelled { .. }
                        | WriteLockReleased { .. }
                        | WriteLockExpired { .. }
                ) {
                    self.0.lock().unwrap().released = true;
                }
                changed = true;
            }
        }
        changed
    }
    pub fn register_trigger(&self, trigger: &TaskTrigger) {
        self.committed_trigger(trigger, crate::utc_now().timestamp());
    }
    pub fn armed(&self, trigger: &TaskTrigger) -> bool {
        self.0
            .lock()
            .unwrap()
            .arms
            .get(&trigger.id)
            .and_then(|arm| arm.schedule.as_ref())
            .is_some_and(|old| same_schedule(old, trigger))
    }
    fn committed_trigger(&self, trigger: &TaskTrigger, now: i64) {
        self.committed_trigger_at(trigger, now, false);
    }
    fn committed_trigger_at(&self, trigger: &TaskTrigger, now: i64, refresh: bool) {
        let mut state = self.0.lock().unwrap();
        state.revision = state
            .revision
            .checked_add(1)
            .expect("scheduler generation exhausted");
        // A canonical terminal refresh changes only updated_at. If no actual
        // schedule producer superseded the bootstrap snapshot, its old future
        // observation remains valid even when this page has not been read yet.
        let observed_at = if refresh {
            state
                .reconstruction
                .filter(|(floor, _)| {
                    state
                        .arms
                        .get(&trigger.id)
                        .is_none_or(|arm| arm.changed <= *floor)
                })
                .map_or(now, |(_, at)| at)
        } else {
            now
        };
        let changed = state.revision;
        if trigger.status != pioneer_protocol::TaskTriggerStatus::Active {
            state.set_arm(
                trigger.id.clone(),
                TriggerArm {
                    task_id: trigger.task_id.clone(),
                    schedule: None,
                    changed,
                },
            );
            return;
        }
        let armed = trigger.next_fire_at.is_some_and(|at| at > observed_at)
            || state
                .arms
                .get(&trigger.id)
                .and_then(|arm| arm.schedule.as_ref())
                .is_some_and(|old| same_schedule(old, trigger));
        state.set_arm(
            trigger.id.clone(),
            TriggerArm {
                task_id: trigger.task_id.clone(),
                schedule: armed.then(|| trigger.clone()),
                changed,
            },
        );
    }
    pub fn observe_trigger(&self, trigger: &TaskTrigger, snapshot: u64, observed_at: i64) {
        let mut state = self.0.lock().unwrap();
        if state
            .arms
            .get(&trigger.id)
            .is_some_and(|arm| arm.changed > snapshot)
        {
            return;
        }
        let changed = state.arms.get(&trigger.id).map_or(0, |arm| arm.changed);
        let armed = trigger.next_fire_at.is_some_and(|at| at > observed_at)
            || state
                .arms
                .get(&trigger.id)
                .and_then(|arm| arm.schedule.as_ref())
                .is_some_and(|old| same_schedule(old, trigger));
        state.set_arm(
            trigger.id.clone(),
            TriggerArm {
                task_id: trigger.task_id.clone(),
                schedule: armed.then(|| trigger.clone()),
                changed,
            },
        );
    }
    pub fn finish_task_trigger_observation(
        &self,
        task: &str,
        seen: &std::collections::HashSet<String>,
        snapshot: u64,
    ) {
        let mut state = self.0.lock().unwrap();
        let Some(keys) = state.arms_by_task.get(task) else {
            return;
        };
        if keys.len() > 64 {
            return;
        }
        let keys = keys.iter().cloned().collect::<Vec<_>>();
        for id in keys {
            let arm = &state.arms[&id];
            let newer = arm.changed > snapshot;
            let marker = state
                .reconstruction
                .is_some_and(|(floor, _)| arm.changed > floor);
            if !newer && (!seen.contains(&id) || (arm.schedule.is_none() && !marker)) {
                state.remove_arm(&id);
            }
        }
    }
    pub fn finish_trigger_reconstruction(
        &self,
        seen: std::collections::HashSet<String>,
        snapshot: u64,
    ) {
        let mut state = self.0.lock().unwrap();
        if state
            .reconstruction
            .is_some_and(|(floor, _)| floor == snapshot)
        {
            state.reconstruction = None;
        }
        let upper = state.arms.keys().next_back().cloned();
        let obsolete = state.arm_sweep.replace(ArmSweep {
            seen,
            snapshot,
            after: None,
            upper,
        });
        drop(state);
        drop(obsolete);
    }
    pub fn sweep_trigger_observations(&self) {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let mut state = self.0.lock().unwrap();
        let Some(mut sweep) = state.arm_sweep.take() else {
            return;
        };
        let Some(upper) = sweep.upper.as_ref() else {
            drop(state);
            drop(sweep);
            return;
        };
        let keys = state
            .arms
            .range::<String, _>((
                sweep.after.as_ref().map_or(Unbounded, Excluded),
                Included(upper),
            ))
            .take(64)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in &keys {
            let arm = &state.arms[id];
            if arm.changed <= sweep.snapshot && (!sweep.seen.contains(id) || arm.schedule.is_none())
            {
                state.remove_arm(id);
            }
        }
        let more = keys.len() == 64 && keys.last().is_some_and(|last| last < upper);
        if more {
            sweep.after = keys.last().cloned();
            state.arm_sweep = Some(sweep);
        } else {
            drop(state);
            drop(sweep);
        }
        // Any completed seen-set is freed after releasing the state lock.
    }
    pub fn begin_trigger_reconstruction(&self, now: i64) -> u64 {
        let mut state = self.0.lock().unwrap();
        let revision = state.revision;
        state.reconstruction = Some((revision, now));
        let obsolete = state.arm_sweep.take();
        drop(state);
        drop(obsolete);
        revision
    }
    pub fn record(&self, task_id: &str) {
        let mut state = self.0.lock().unwrap();
        state.revision = state
            .revision
            .checked_add(1)
            .expect("scheduler generation exhausted");
        if state.tasks.len() >= 1024 && !state.tasks.contains_key(task_id) {
            state.reconstruct = true;
            return;
        }
        let revision = state.revision;
        state.tasks.insert(task_id.to_owned(), revision);
    }
    pub fn reconstruct(&self) {
        self.0.lock().unwrap().reconstruct = true;
    }
    pub fn take_reconstruction(&self) -> bool {
        std::mem::take(&mut self.0.lock().unwrap().reconstruct)
    }
    pub fn page(&self) -> Vec<(String, u64)> {
        let mut state = self.0.lock().unwrap();
        let upper = state
            .page
            .as_ref()
            .map(|(_, upper)| upper.clone())
            .or_else(|| state.tasks.keys().next_back().cloned());
        let rows = state
            .tasks
            .iter()
            .filter(|(id, _)| {
                state.page.as_ref().is_none_or(|(after, _)| *id > after)
                    && upper.as_ref().is_some_and(|upper| *id <= upper)
            })
            .take(64)
            .map(|(id, revision)| (id.clone(), *revision))
            .collect::<Vec<_>>();
        state.page = match (rows.last(), upper) {
            (Some((last, _)), Some(upper)) if rows.len() == 64 && last < &upper => {
                Some((last.clone(), upper))
            }
            _ => None,
        };
        rows
    }
    pub fn acknowledge(&self, task_id: &str, revision: u64) {
        let mut state = self.0.lock().unwrap();
        if state.tasks.get(task_id) == Some(&revision) {
            state.tasks.remove(task_id);
        }
    }
    pub fn pending(&self) -> bool {
        let state = self.0.lock().unwrap();
        state.reconstruct || state.released || !state.tasks.is_empty()
    }
    pub fn sweeping(&self) -> bool {
        self.0.lock().unwrap().arm_sweep.is_some()
    }
    pub fn take_releases(&self) -> bool {
        std::mem::take(&mut self.0.lock().unwrap().released)
    }
    pub fn generation(&self) -> u64 {
        self.0.lock().unwrap().revision
    }
}

/// Finite readers retain raw membership across hydration/domain failures.
/// Locator-query failures remain outer errors rather than proving Empty.
#[derive(Debug)]
pub struct TaskSchedulerPage<T, K> {
    pub records: Vec<T>,
    pub failures: Vec<TaskSchedulerRowFailure>,
    pub scanned: usize,
    pub last: Option<K>,
}
#[derive(Debug)]
pub struct TaskSchedulerRowFailure {
    pub id: String,
    pub task_id: String,
    pub error: anyhow::Error,
}
impl<T, K> TaskSchedulerPage<T, K> {
    fn complete(self) -> Result<Vec<T>> {
        if self.failures.is_empty() {
            Ok(self.records)
        } else {
            anyhow::bail!("scheduler discovery incomplete: {:?}", self.failures)
        }
    }
}
fn decode<M, T, K>(
    rows: Vec<M>,
    key: impl Fn(&M) -> K,
    locator: impl Fn(&M) -> (String, String),
    convert: impl Fn(M) -> Result<T>,
) -> TaskSchedulerPage<T, K> {
    let scanned = rows.len();
    let last = rows.last().map(key);
    let mut page = TaskSchedulerPage {
        records: Vec::new(),
        failures: Vec::new(),
        scanned,
        last,
    };
    for row in rows {
        let (id, task_id) = locator(&row);
        match convert(row) {
            Ok(record) => page.records.push(record),
            Err(error) => page
                .failures
                .push(TaskSchedulerRowFailure { id, task_id, error }),
        }
    }
    page
}

fn task_payload_affects_review(payload: &pioneer_protocol::TaskEventPayload) -> bool {
    use pioneer_protocol::TaskEventPayload::*;
    matches!(
        payload,
        TaskCreated { .. }
            | TaskUpdated { .. }
            | TaskDetached { .. }
            | TaskPaused { .. }
            | TaskResumed { .. }
            | TaskQueued { .. }
            | TaskCompleted { .. }
            | TaskFailed { .. }
            | TaskBlocked { .. }
            | TaskCancelled { .. }
            | RunCreated { .. }
            | RunStarting { .. }
            | RunStarted { .. }
            | RunCompleted { .. }
            | RunFailed { .. }
            | RunBlocked { .. }
            | RunCancelled { .. }
            | RunRetryScheduled { .. }
            | RunRetryExhausted { .. }
            | TaskRunEnteredReview { .. }
            | TaskResultCandidateCreated { .. }
            | TaskResultCandidateAccepted { .. }
            | TaskResultCandidateRejected { .. }
            | TaskResultCandidateCancelled { .. }
            | TaskResultReviewEventRecorded { .. }
            | TaskRevisionRequested { .. }
    )
}
pub(crate) fn task_review_changed(events: &[crate::AppendedTaskEvent]) -> bool {
    events.iter().any(|event| {
        event.append_status.is_inserted() && task_payload_affects_review(&event.payload)
    })
}

impl CrudStore {
    pub async fn task_scheduler_trigger_page(
        &self,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskTrigger, String>, Option<String>)> {
        let (keys, upper) =
            task_trigger::raw_page(&self.connection, after, upper, limit.min(64)).await?;
        let mut page = TaskSchedulerPage {
            records: Vec::new(),
            failures: Vec::new(),
            scanned: keys.len(),
            last: keys.last().map(|(id, _)| id.clone()),
        };
        for (id, task_id) in keys {
            let row = async {
                use sea_orm::EntityTrait;
                let Some(row) = pioneer_entity::task_trigger::Entity::find_by_id(id.clone())
                    .one(&self.connection)
                    .await?
                else {
                    return Ok(None);
                };
                if row.status != "active" {
                    return Ok(None);
                }
                task_trigger_from_db_model(row).map(Some)
            }
            .await;
            match row {
                Ok(Some(row)) => page.records.push(row),
                Ok(None) => {}
                Err(error) => page
                    .failures
                    .push(TaskSchedulerRowFailure { id, task_id, error }),
            }
        }
        Ok((page, upper))
    }
    pub async fn task_scheduler_triggers_for_task(
        &self,
        task_id: &str,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        let rows = task_trigger::scheduler_task_page(&self.connection, task_id).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }
    /// Due pages have already visited past/NULL clocks in this quantum. A
    /// parked dependency retains no expired timer; its producer reopens work.
    /// Future full-resolution clocks are rounded UP to the authoritative
    /// integer-second comparison boundary, never down through protocol DTOs.
    pub async fn next_task_scheduler_deadline(
        &self,
        now: i64,
    ) -> Result<Option<DateTimeWithTimeZone>> {
        let now = unix_to_datetime(now);
        let trigger = task_trigger::next_future_deadline(&self.connection, now).await?;
        let run = task_run::next_future_ready(&self.connection, now).await?;
        let deadline = trigger.into_iter().chain(run).min();
        Ok(deadline.map(|at| {
            unix_to_datetime(
                at.timestamp()
                    .saturating_add(i64::from(at.timestamp_subsec_nanos() > 0)),
            )
        }))
    }
    pub async fn task_live_lease_keys(
        &self,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(Vec<String>, Option<String>)> {
        crate::repositories::task_run_execution::recovery_lease_keys(
            &self.connection,
            after,
            upper,
            limit.min(64),
        )
        .await
    }
    pub async fn task_live_lease_point(&self, id: &str, cutoff: i64) -> Result<Option<i64>> {
        Ok(
            crate::repositories::task_run_execution::recovery_lease_point(
                &self.connection,
                id,
                unix_to_datetime(cutoff),
            )
            .await?
            .map(|at| {
                at.timestamp()
                    .saturating_add(i64::from(at.timestamp_subsec_nanos() > 0))
            }),
        )
    }
    pub async fn next_task_live_queued_deadline(&self, now: i64) -> Result<Option<i64>> {
        Ok(
            task_run::next_future_ready(&self.connection, unix_to_datetime(now))
                .await?
                .map(|at| {
                    at.timestamp()
                        .saturating_add(i64::from(at.timestamp_subsec_nanos() > 0))
                }),
        )
    }
    pub async fn next_task_occurrence_reconcile_at(&self, now: i64) -> Result<Option<i64>> {
        crate::repositories::task_occurrence_reconcile::next_work_at(&self.connection, now).await
    }
    pub async fn next_task_review_timeout(&self, timeout_seconds: i64) -> Result<Option<i64>> {
        crate::repositories::task_result_candidate::next_review_timeout(
            &self.connection,
            timeout_seconds,
        )
        .await
    }
    pub async fn due_task_review_runs_page(
        &self,
        now: i64,
        timeout_seconds: i64,
        limit: u64,
        after: Option<&str>,
        upper: Option<&str>,
    ) -> Result<(TaskSchedulerPage<TaskRun, String>, Option<(String, String)>)> {
        let (rows, continuation) = crate::repositories::task_result_candidate::due_review_runs(
            &self.connection,
            now,
            timeout_seconds,
            limit,
            after,
            upper,
        )
        .await?;
        Ok((
            decode(
                rows,
                |row| row.id.clone(),
                |row| (row.id.clone(), row.task_id.clone()),
                task_run_from_db_model,
            ),
            continuation,
        ))
    }
    pub async fn task_review_observation_page(
        &self,
        now: i64,
        timeout_seconds: i64,
        limit: u64,
        after: Option<&str>,
        upper: Option<&str>,
    ) -> Result<(
        TaskSchedulerPage<TaskRun, String>,
        Option<(String, String)>,
        Option<i64>,
        bool,
    )> {
        let (rows, next, nearest, failed) =
            crate::repositories::task_result_candidate::review_runs_page(
                &self.connection,
                now,
                timeout_seconds,
                limit,
                after,
                upper,
            )
            .await?;
        Ok((
            decode(
                rows,
                |row| row.id.clone(),
                |row| (row.id.clone(), row.task_id.clone()),
                task_run_from_db_model,
            ),
            next,
            nearest,
            failed,
        ))
    }
    pub fn task_live_changes(&self) -> std::sync::Arc<TaskLiveChanges> {
        self.connection.shared_resource::<TaskLiveChanges>()
    }
    pub fn task_scheduler_changes(&self) -> std::sync::Arc<TaskSchedulerChanges> {
        self.connection.shared_resource::<TaskSchedulerChanges>()
    }

    pub(crate) async fn task_scheduler_event_change(
        &self,
        inputs: Option<&[pioneer_protocol::TaskEventPayload]>,
        operation: impl std::future::Future<Output = Result<Vec<crate::AppendedTaskEvent>>>,
    ) -> Result<Vec<crate::AppendedTaskEvent>> {
        let wait_change = self.task_wait_mutation(inputs);
        let scheduler = inputs.is_none_or(|events| events.iter().any(task_event_affects_scheduler));
        let review = inputs.is_none_or(|events| events.iter().any(task_payload_affects_review));
        let fanout = inputs.is_none_or(|events| !events.is_empty());
        let lifecycle = inputs.is_none_or(|events| events.iter().any(task_event_affects_lifecycle));
        let lifecycle_change =
            lifecycle.then(|| self.background_work("tasks.lifecycle").changing());
        let scheduler_change =
            scheduler.then(|| self.background_work("tasks.scheduler").changing());
        let live_change = scheduler.then(|| self.background_work("tasks.live").changing());
        let history_change =
            scheduler.then(|| self.background_work("compaction.history").changing());
        let frozen_change = scheduler.then(|| self.background_work("storage.frozen").changing());
        let retention_change =
            scheduler.then(|| self.background_work("storage.html.retention").changing());
        let html_history_change =
            scheduler.then(|| self.background_work("storage.html.history").changing());
        let review_change = review.then(|| self.background_work("tasks.review").changing());
        let effect_change = review.then(|| self.background_work("recovery.effects").changing());
        let fanout_change = fanout.then(|| self.background_work("tasks.fanout").changing());
        let portable_change = fanout.then(|| self.background_work("storage.portable").changing());
        let events = operation.await?;
        let scheduler_changed = scheduler
            && self
                .task_scheduler_changes()
                .record_committed_events(&events);
        if let Some(change) = scheduler_change {
            change.finish(scheduler_changed);
        }
        if scheduler_changed {
            self.task_live_changes().record();
        }
        if let Some(change) = live_change {
            change.finish(scheduler_changed);
        }
        if let Some(change) = frozen_change {
            change.finish(scheduler_changed);
        }
        if let Some(change) = retention_change {
            change.finish(scheduler_changed);
        }
        if let Some(change) = html_history_change {
            change.finish(scheduler_changed);
        }
        if let Some(change) = history_change {
            if scheduler_changed {
                self.compaction_history_changes().executor_released();
                self.background_work("compaction.history")
                    .dependency_released();
            }
            change.finish(scheduler_changed);
        }
        if let Some(change) = review_change {
            change.finish(task_review_changed(&events));
        }
        if let Some(change) = effect_change {
            change.finish(task_review_changed(&events));
        }
        if let Some(change) = lifecycle_change {
            change.finish(events.iter().any(|event| {
                event.append_status.is_inserted() && task_event_affects_lifecycle(&event.payload)
            }));
        }
        let inserted = events.iter().any(|event| event.append_status.is_inserted());
        if let Some(change) = fanout_change {
            change.finish(inserted);
        }
        if let Some(change) = portable_change {
            change.finish(inserted);
        }
        wait_change.finish_events(&events);
        Ok(events)
    }

    pub(crate) async fn track_task_scheduler<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T>>,
        mark: impl FnOnce(&T, &TaskSchedulerChanges) -> bool,
        wait_outcome: impl FnOnce(&T, crate::task_wait::TaskWaitMutation),
    ) -> Result<T> {
        let wait_change = self.task_wait_mutation(None);
        let change = self.background_work("tasks.scheduler").changing();
        let live_change = self.background_work("tasks.live").changing();
        let lifecycle_change = self.background_work("tasks.lifecycle").changing();
        let history_change = self.background_work("compaction.history").changing();
        let frozen_change = self.background_work("storage.frozen").changing();
        let portable_change = self.background_work("storage.portable").changing();
        let retention_change = self.background_work("storage.html.retention").changing();
        let html_history_change = self.background_work("storage.html.history").changing();
        let result = operation.await?;
        let changed = mark(&result, &self.task_scheduler_changes());
        if changed {
            self.task_live_changes().record();
        }
        change.finish(changed);
        live_change.finish(changed);
        lifecycle_change.finish(changed);
        if changed {
            self.compaction_history_changes().executor_released();
            self.background_work("compaction.history")
                .dependency_released();
        }
        history_change.finish(changed);
        frozen_change.finish(changed);
        portable_change.finish(changed);
        retention_change.finish(changed);
        html_history_change.finish(changed);
        wait_outcome(&result, wait_change);
        Ok(result)
    }
    pub async fn list_due_active_task_triggers_page(
        &self,
        now: i64,
        after: Option<(i64, &str)>,
        limit: u64,
    ) -> Result<Vec<TaskTrigger>> {
        self.list_due_active_task_triggers_page_partial(now, after, limit)
            .await?
            .complete()
    }
    pub async fn list_due_active_task_triggers_page_partial(
        &self,
        now: i64,
        after: Option<(i64, &str)>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskTrigger, (i64, String)>> {
        let rows = task_trigger::list_due_active_triggers_bounded(
            &self.connection,
            unix_to_datetime(now),
            after.map(|(at, id)| (unix_to_datetime(at), id)),
            limit,
        )
        .await?;
        Ok(decode(
            rows,
            |row| {
                (
                    row.next_fire_at
                        .as_ref()
                        .expect("due predicate")
                        .timestamp(),
                    row.id.clone(),
                )
            },
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }

    pub async fn list_dormant_active_task_triggers(
        &self,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskTrigger>> {
        self.list_dormant_active_task_triggers_partial(after_id, limit)
            .await?
            .complete()
    }
    pub async fn list_dormant_active_task_triggers_partial(
        &self,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        let rows =
            task_trigger::list_dormant_active_triggers(&self.connection, after_id, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }

    pub async fn list_task_recovery_runs(
        &self,
        status: TaskRunStatus,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        self.list_task_recovery_runs_partial(status, after_id, limit)
            .await?
            .complete()
    }
    pub async fn list_task_recovery_runs_partial(
        &self,
        status: TaskRunStatus,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows = task_run::list_recovery_runs(&self.connection, status, after_id, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    pub async fn list_task_recovery_runs_for_task(
        &self,
        task_id: &str,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        self.list_task_recovery_runs_for_task_partial(task_id, status, limit)
            .await?
            .complete()
    }
    pub async fn list_task_recovery_runs_for_task_partial(
        &self,
        task_id: &str,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows =
            task_run::list_recovery_runs_for_task(&self.connection, task_id, status, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    pub async fn list_queued_task_schedule(&self, limit: u64) -> Result<Vec<TaskRun>> {
        self.list_queued_task_schedule_partial(limit)
            .await?
            .complete()
    }
    pub async fn list_queued_task_schedule_partial(
        &self,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows = task_run::list_queued_schedule(&self.connection, limit).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    pub async fn list_scheduler_due_task_runs(
        &self,
        now: i64,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        self.list_scheduler_due_task_runs_partial(now, after_id, limit)
            .await?
            .complete()
    }
    pub async fn list_scheduler_due_task_runs_partial(
        &self,
        now: i64,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let rows = task_run::list_scheduler_due_runs(
            &self.connection,
            unix_to_datetime(now),
            after_id,
            limit,
        )
        .await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }

    /// Only active triggers are scheduling inputs; malformed inactive history is irrelevant.
    pub async fn task_scheduler_inputs(
        &self,
        task_id: &str,
    ) -> Result<(Vec<TaskTrigger>, Vec<TaskRun>)> {
        let (triggers, runs) = self.task_scheduler_inputs_partial(task_id).await?;
        Ok((triggers.complete()?, runs.complete()?))
    }
    pub async fn task_scheduler_inputs_partial(
        &self,
        task_id: &str,
    ) -> Result<(
        TaskSchedulerPage<TaskTrigger, String>,
        TaskSchedulerPage<TaskRun, String>,
    )> {
        let triggers =
            task_trigger::list_scheduler_active_triggers(&self.connection, task_id).await?;
        let runs = task_run::list_scheduler_queued_runs(&self.connection, task_id).await?;
        Ok((
            decode(
                triggers,
                |row| row.id.clone(),
                |row| (row.id.clone(), row.task_id.clone()),
                task_trigger_from_db_model,
            ),
            decode(
                runs,
                |row| row.id.clone(),
                |row| (row.id.clone(), row.task_id.clone()),
                task_run_from_db_model,
            ),
        ))
    }
    pub async fn list_active_task_triggers_partial(
        &self,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        let rows = task_trigger::list_active_triggers(&self.connection).await?;
        Ok(decode(
            rows,
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }
    pub async fn task_scheduler_run_partial(
        &self,
        id: &str,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        let row = task_run::find_run_by_id(&self.connection, id).await?;
        Ok(decode(
            row.into_iter().collect(),
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_run_from_db_model,
        ))
    }
    pub async fn task_scheduler_trigger_partial(
        &self,
        id: &str,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        let row = task_trigger::find_trigger_by_id(&self.connection, id)
            .await?
            .filter(|row| row.status == "active");
        Ok(decode(
            row.into_iter().collect(),
            |row| row.id.clone(),
            |row| (row.id.clone(), row.task_id.clone()),
            task_trigger_from_db_model,
        ))
    }
    /// Keep the exact stored timestamp in finite keys; foreign/legacy rows may
    /// have subsecond deadlines even though protocol TaskTrigger uses seconds.
    pub async fn list_due_active_task_triggers_page_finite(
        &self,
        now: i64,
        after: Option<(DateTimeWithTimeZone, &str)>,
        upper: Option<(DateTimeWithTimeZone, &str)>,
        limit: u64,
    ) -> Result<(
        TaskSchedulerPage<TaskTrigger, (DateTimeWithTimeZone, String)>,
        Option<(DateTimeWithTimeZone, String)>,
    )> {
        let bound = match upper.map(|(at, id)| (at, id.to_owned())) {
            Some(bound) => Some(bound),
            None => {
                task_trigger::scheduler_due_upper(&self.connection, unix_to_datetime(now)).await?
            }
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let keys = task_trigger::list_due_active_trigger_keys_through(
            &self.connection,
            unix_to_datetime(now),
            after,
            Some((bound.0, bound.1.as_str())),
            limit,
        )
        .await?;
        let mut page = TaskSchedulerPage {
            records: Vec::new(),
            failures: Vec::new(),
            scanned: keys.len(),
            last: keys.last().map(|(at, id, _)| (*at, id.clone())),
        };
        for (_, id, task_id) in keys {
            let row = async {
                use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
                let Some(row) = pioneer_entity::task_trigger::Entity::find_by_id(id.clone())
                    .filter(pioneer_entity::task_trigger::Column::TaskId.eq(task_id.clone()))
                    .filter(pioneer_entity::task_trigger::Column::Status.eq("active"))
                    .filter(
                        pioneer_entity::task_trigger::Column::NextFireAt.lte(unix_to_datetime(now)),
                    )
                    .one(&self.connection)
                    .await?
                else {
                    return Ok(None);
                };
                task_trigger_from_db_model(row).map(Some)
            }
            .await;
            match row {
                Ok(Some(record)) => page.records.push(record),
                Ok(None) => {}
                Err(error) => page
                    .failures
                    .push(TaskSchedulerRowFailure { id, task_id, error }),
            }
        }
        Ok((page, Some(bound_key.clone())))
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.
    pub async fn list_dormant_active_task_triggers_finite(
        &self,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskTrigger, String>, Option<String>)> {
        let bound = match upper.map(str::to_owned) {
            Some(bound) => Some(bound),
            None => task_trigger::scheduler_dormant_upper(&self.connection).await?,
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let keys = task_trigger::list_dormant_active_trigger_keys_through(
            &self.connection,
            after,
            Some(bound.as_str()),
            limit,
        )
        .await?;
        let mut page = TaskSchedulerPage {
            records: Vec::new(),
            failures: Vec::new(),
            scanned: keys.len(),
            last: keys.last().map(|(id, _)| id.clone()),
        };
        for (id, task_id) in keys {
            let row = async {
                use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
                let Some(row) = pioneer_entity::task_trigger::Entity::find_by_id(id.clone())
                    .filter(pioneer_entity::task_trigger::Column::TaskId.eq(task_id.clone()))
                    .filter(pioneer_entity::task_trigger::Column::Status.eq("active"))
                    .filter(pioneer_entity::task_trigger::Column::NextFireAt.is_null())
                    .one(&self.connection)
                    .await?
                else {
                    return Ok(None);
                };
                task_trigger_from_db_model(row).map(Some)
            }
            .await;
            match row {
                Ok(Some(record)) => page.records.push(record),
                Ok(None) => {}
                Err(error) => page
                    .failures
                    .push(TaskSchedulerRowFailure { id, task_id, error }),
            }
        }
        Ok((page, Some(bound_key.clone())))
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.
    pub async fn list_task_recovery_runs_finite(
        &self,
        status: TaskRunStatus,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskRun, String>, Option<String>)> {
        let bound = match upper.map(str::to_owned) {
            Some(bound) => Some(bound),
            None => task_run::scheduler_recovery_upper(&self.connection, status).await?,
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let keys = task_run::list_recovery_run_keys_through(
            &self.connection,
            status,
            after,
            Some(bound.as_str()),
            limit,
        )
        .await?;
        let mut page = TaskSchedulerPage {
            records: Vec::new(),
            failures: Vec::new(),
            scanned: keys.len(),
            last: keys.last().map(|(id, _)| id.clone()),
        };
        for (id, task_id) in keys {
            let row = async {
                use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
                let Some(row) = pioneer_entity::task_run::Entity::find_by_id(id.clone())
                    .filter(pioneer_entity::task_run::Column::TaskId.eq(task_id.clone()))
                    .filter(
                        pioneer_entity::task_run::Column::Status
                            .eq(crate::convention::task_run_status_to_db(status)),
                    )
                    .one(&self.connection)
                    .await?
                else {
                    return Ok(None);
                };
                task_run_from_db_model(row).map(Some)
            }
            .await;
            match row {
                Ok(Some(record)) => page.records.push(record),
                Ok(None) => {}
                Err(error) => page
                    .failures
                    .push(TaskSchedulerRowFailure { id, task_id, error }),
            }
        }
        Ok((page, Some(bound_key.clone())))
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.
    pub async fn list_scheduler_due_task_runs_finite(
        &self,
        now: i64,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskRun, String>, Option<String>)> {
        let bound = match upper.map(str::to_owned) {
            Some(bound) => Some(bound),
            None => task_run::scheduler_due_upper(&self.connection, unix_to_datetime(now)).await?,
        };
        let Some(ref bound_key) = bound else {
            return Ok((
                TaskSchedulerPage {
                    records: Vec::new(),
                    failures: Vec::new(),
                    scanned: 0,
                    last: None,
                },
                None,
            ));
        };
        let bound = bound_key;
        let keys = task_run::list_scheduler_due_run_keys_through(
            &self.connection,
            unix_to_datetime(now),
            after,
            Some(bound.as_str()),
            limit,
        )
        .await?;
        let mut page = TaskSchedulerPage {
            records: Vec::new(),
            failures: Vec::new(),
            scanned: keys.len(),
            last: keys.last().map(|(id, _)| id.clone()),
        };
        for (id, task_id) in keys {
            let row = async {
                use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
                let Some(row) =
                    pioneer_entity::task_run::Entity::find_by_id(id.clone())
                        .filter(pioneer_entity::task_run::Column::TaskId.eq(task_id.clone()))
                        .filter(pioneer_entity::task_run::Column::Status.eq("queued"))
                        .filter(pioneer_entity::task_run::Column::ReadyAt.is_null().or(
                            pioneer_entity::task_run::Column::ReadyAt.lte(unix_to_datetime(now)),
                        ))
                        .one(&self.connection)
                        .await?
                else {
                    return Ok(None);
                };
                task_run_from_db_model(row).map(Some)
            }
            .await;
            match row {
                Ok(Some(record)) => page.records.push(record),
                Ok(None) => {}
                Err(error) => page
                    .failures
                    .push(TaskSchedulerRowFailure { id, task_id, error }),
            }
        }
        Ok((page, Some(bound_key.clone())))
    }
}

pub fn task_event_affects_scheduler(event: &pioneer_protocol::TaskEventPayload) -> bool {
    use pioneer_protocol::TaskEventPayload::*;
    matches!(
        event,
        TaskCreated { .. }
            | TriggerCreated { .. }
            | DependencyCreated { .. }
            | TaskScheduled { .. }
            | TaskQueued { .. }
            | RunCreated { .. }
            | RunStarting { .. }
            | RunStarted { .. }
            | RunCompleted { .. }
            | RunFailed { .. }
            | RunBlocked { .. }
            | RunCancelled { .. }
            | RunRetryScheduled { .. }
            | RunRetryExhausted { .. }
            | TaskCompleted { .. }
            | TaskFailed { .. }
            | TaskBlocked { .. }
            | TaskCancelled { .. }
            | TaskDetached { .. }
            | TaskUpdated { .. }
            | TaskRescheduled { .. }
            | TaskPaused { .. }
            | TaskResumed { .. }
            | TaskRunEnteredReview { .. }
            | TaskRevisionRequested { .. }
            | WriteLockReleased { .. }
            | WriteLockExpired { .. }
    )
}
fn register_trigger_event(
    changes: &TaskSchedulerChanges,
    event: &pioneer_protocol::TaskEventPayload,
) {
    use pioneer_protocol::TaskEventPayload::*;
    let now = crate::utc_now().timestamp();
    match event {
        TaskScheduled {
            task_id,
            trigger_id,
            next_fire_at,
        } => changes.scheduled(trigger_id, task_id, *next_fire_at),
        TaskRescheduled {
            trigger,
            reason: pioneer_protocol::TaskRescheduleReason::RunTerminalStatusRefresh,
            ..
        } => changes.committed_trigger_at(trigger, now, true),
        TriggerCreated { trigger } | TaskRescheduled { trigger, .. } => {
            changes.committed_trigger(trigger, now)
        }
        TaskUpdated {
            trigger: Some(trigger),
            ..
        } => changes.committed_trigger(trigger, now),
        TaskPaused { triggers, .. } | TaskResumed { triggers, .. } => {
            for trigger in triggers {
                changes.committed_trigger(trigger, now);
            }
        }
        _ => {}
    }
}

fn task_event_affects_lifecycle(payload: &pioneer_protocol::TaskEventPayload) -> bool {
    // Progress and recovery diagnostics do not change a lifecycle obligation.
    !matches!(
        payload,
        pioneer_protocol::TaskEventPayload::Progress { .. }
            | pioneer_protocol::TaskEventPayload::TaskRecovered { .. }
    )
}
