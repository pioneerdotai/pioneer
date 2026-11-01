//! One bounded live registry and prehistory, owned by telemetry consent epochs.
//! Fixed typed hot-path records: no FS, payload serialization or object walks.
use crate::{
    identity::monotonic_ms,
    work::{Outcome, WorkContext},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Mutex, OnceLock},
};
pub const MAX_ACTIVE: usize = 512;
pub const MAX_QUEUES: usize = 128;
const MAX_RECORDS: usize = 4096;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const HISTORY_MS: u64 = 120_000;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    Unknown,
    DatabaseAdmission,
    DatabasePool,
    DatabaseBlocking,
    ChannelCapacity,
    QueueItem,
    Network,
    Reply,
    Child,
    Timer,
    UserOrProvider,
    Lock,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Queued,
    Executing,
    Admission,
    Database,
    Send,
    Receive,
    Decode,
    Publication,
    Callback,
    Retry,
    Shutdown,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", content = "detail", rename_all = "snake_case")]
pub enum WorkState {
    Queued,
    Running,
    Waiting(WaitKind),
    SleepingUntil(u64),
    Terminal(Outcome),
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ActiveWork {
    /// Fixed portable payload operation detail; absent for other work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portable_payload: Option<crate::portable_payload::LiveOperation>,
    /// Last pre-scope hold refusal on this caller; not a completed payload read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub portable_hold_rejection: Option<crate::portable_payload::Family>,
    #[serde(skip)]
    pending_outcome: Option<Outcome>,
    #[serde(skip)]
    wait_stack: [Option<(WorkState, Stage)>; 4],
    pub wait_overflow: u64,
    pub context: WorkContext,
    pub state: WorkState,
    pub stage: Stage,
    pub started_ms: u64,
    pub last_progress_ms: u64,
    pub state_since_ms: u64,
    pub deadline_ms: Option<u64>,
    pub retry_backoff_ms: Option<u64>,
    pub worker_lifetime: bool,
    pub lease_count: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "record", content = "value", rename_all = "snake_case")]
pub enum JournalPayload {
    Work(ActiveWork),
    Queue(crate::queues::QueueSnapshot),
    Resources(Box<crate::resources::ResourceSnapshot>),
    Health(crate::telemetry::ExportHealth),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JournalRecord {
    pub sequence: u64,
    pub monotonic_ms: u64,
    pub retained_bytes: usize,
    pub payload: JournalPayload,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolSnapshot {
    pub role: String,
    pub sampled_ms: u64,
    pub size_connections: u64,
    pub idle_connections: u64,
    pub capacity_connections: u64,
    pub source: String,
    pub waiters: Option<u64>,
    pub wait_coverage: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveSnapshot {
    pub database_pools: Vec<PoolSnapshot>,
    pub schema_version: u32,
    pub consent_generation: u64,
    pub snapshot_ms: u64,
    pub active: Vec<ActiveWork>,
    pub queues: Vec<crate::queues::QueueSnapshot>,
    pub journal: Vec<JournalRecord>,
    pub first_sequence: Option<u64>,
    pub last_sequence: u64,
    pub overwritten: u64,
    pub lost: u64,
    pub active_overflow: u64,
    pub abandoned: u64,
    pub queue_overflow: u64,
    pub metric_counter_overflow: bool,
    pub journal_bytes: usize,
    pub consistency: String,
}
pub(crate) struct LiveStore {
    pools: [Option<PoolSnapshot>; 2],
    pub work_counts: [u64; 210],
    pub context_loss_counts: [u64; 3],
    pub queue_counts: [u64; 240],
    pub metric_overflow: bool,
    pub epoch: u64,
    pub active: BTreeMap<u64, ActiveWork>,
    pub queues: BTreeMap<u64, crate::queues::QueueState>,
    pub next_queue: u64,
    journal: VecDeque<JournalRecord>,
    bytes: usize,
    sequence: u64,
    overwritten: u64,
    lost: u64,
    overflow: u64,
    abandoned: u64,
    pub queue_overflow: u64,
}
impl LiveStore {
    fn new(epoch: u64) -> Self {
        Self {
            epoch,
            pools: [None, None],
            work_counts: [0; 210],
            context_loss_counts: [0; 3],
            queue_counts: [0; 240],
            metric_overflow: false,
            active: BTreeMap::new(),
            queues: BTreeMap::new(),
            next_queue: 1,
            journal: VecDeque::new(),
            bytes: 0,
            sequence: 0,
            overwritten: 0,
            lost: 0,
            overflow: 0,
            abandoned: 0,
            queue_overflow: 0,
        }
    }
    pub(crate) fn append(&mut self, payload: JournalPayload, bytes: usize, now: u64) {
        if self.sequence == u64::MAX {
            self.lost = self.lost.saturating_add(1);
            return;
        }
        self.sequence += 1;
        if bytes > MAX_BYTES {
            self.lost = self.lost.saturating_add(1);
            return;
        }
        self.trim(now);
        while self.journal.len() >= MAX_RECORDS || self.bytes.saturating_add(bytes) > MAX_BYTES {
            self.pop();
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.journal.push_back(JournalRecord {
            sequence: self.sequence,
            monotonic_ms: now,
            retained_bytes: bytes,
            payload,
        });
    }
    fn pop(&mut self) {
        if let Some(record) = self.journal.pop_front() {
            self.bytes = self.bytes.saturating_sub(record.retained_bytes);
            self.overwritten = self.overwritten.saturating_add(1);
        }
    }
    fn trim(&mut self, now: u64) {
        while self
            .journal
            .front()
            .is_some_and(|r| now.saturating_sub(r.monotonic_ms) > HISTORY_MS)
        {
            self.pop();
        }
    }
    fn sweep(&mut self, now: u64) {
        self.trim(now);
        // Active entries are owned by WorkGuard or QueuedWork. Age is evidence
        // of long work / lack of progress, never evidence that its owner died.
        // MAX_ACTIVE bounds retention until the real terminal/drop/epoch change.
    }
}
static STORE: OnceLock<Mutex<LiveStore>> = OnceLock::new();
pub(crate) fn with_store<T>(epoch: u64, callback: impl FnOnce(&mut LiveStore) -> T) -> Option<T> {
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        return None;
    }
    let mut state = STORE
        .get_or_init(|| Mutex::new(LiveStore::new(epoch)))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        return None;
    }
    if state.epoch != epoch {
        *state = LiveStore::new(epoch);
    }
    Some(callback(&mut state))
}
pub(crate) fn discard(epoch: u64) {
    if let Some(store) = STORE.get() {
        *store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = LiveStore::new(epoch);
    }
}
fn record_work(state: &mut LiveStore, entry: ActiveWork, now: u64) {
    state.append(
        JournalPayload::Work(entry),
        std::mem::size_of::<JournalRecord>(),
        now,
    );
}
/// A scope lease, not a task registry/scheduler. Only first lease creates start;
/// nested carriers referring to one token cannot create duplicate terminals.
#[derive(Debug)]
pub struct WorkGuard {
    context: Option<WorkContext>,
    finished: bool,
}
impl WorkGuard {
    pub fn start(context: Option<WorkContext>, worker_lifetime: bool) -> Self {
        let mut guard = Self {
            context: None,
            finished: false,
        };
        let Some(context) = context else { return guard };
        let now = monotonic_ms();
        let registered = with_store(context.consent_generation, |state| {
            if let Some(entry) = state.active.get_mut(&context.token) {
                if entry.lease_count == u16::MAX {
                    state.overflow = state.overflow.saturating_add(1);
                    return false;
                }
                entry.lease_count += 1;
                if entry.state == WorkState::Queued {
                    entry.state = WorkState::Running;
                    entry.stage = Stage::Executing;
                    entry.state_since_ms = now;
                    entry.last_progress_ms = now;
                    let entry = *entry;
                    record_work(state, entry, now);
                }
                return true;
            }
            if state.active.len() >= MAX_ACTIVE {
                state.overflow = state.overflow.saturating_add(1);
                return false;
            }
            let entry = ActiveWork {
                portable_payload: None,
                portable_hold_rejection: None,
                pending_outcome: None,
                wait_stack: [None; 4],
                wait_overflow: 0,
                context,
                state: WorkState::Running,
                stage: Stage::Executing,
                started_ms: now,
                last_progress_ms: now,
                state_since_ms: now,
                deadline_ms: None,
                retry_backoff_ms: None,
                worker_lifetime,
                lease_count: 1,
            };
            state.active.insert(context.token, entry);
            record_work(state, entry, now);
            true
        })
        .unwrap_or(false);
        if registered {
            guard.context = Some(context);
        }
        guard
    }
    /// An execution window was actually dropped/aborted. Retain the logical
    /// owner's lease through cleanup, but never let a later normal finish turn
    /// this into success. Does not create/revive an entry or end another lease.
    pub fn cancel_on_finish(&self) {
        let Some(context) = self.context.filter(|_| !self.finished) else {
            return;
        };
        with_store(context.consent_generation, |state| {
            if let Some(entry) = state.active.get_mut(&context.token) {
                if !matches!(
                    entry.pending_outcome,
                    Some(Outcome::Panicked | Outcome::Abandoned)
                ) {
                    entry.pending_outcome = Some(Outcome::Cancelled);
                }
            }
        });
    }
    pub fn finish(&mut self, outcome: Outcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        let Some(context) = self.context.take() else {
            return;
        };
        let now = monotonic_ms();
        with_store(context.consent_generation, |state| {
            let Some(mut entry) = state.active.remove(&context.token) else {
                return;
            };
            let severity = |o| match o {
                Outcome::Completed => 0,
                Outcome::Failed => 1,
                Outcome::Cancelled => 2,
                Outcome::Panicked => 3,
                Outcome::Abandoned => 4,
            };
            let outcome = entry
                .pending_outcome
                .filter(|old| severity(*old) > severity(outcome))
                .unwrap_or(outcome);
            entry.pending_outcome = Some(outcome);
            if entry.lease_count > 1 {
                entry.lease_count -= 1;
                state.active.insert(context.token, entry);
                return;
            }
            entry.state = WorkState::Terminal(outcome);
            entry.state_since_ms = now;
            if outcome == Outcome::Abandoned {
                state.abandoned = state.abandoned.saturating_add(1);
            }
            record_work(state, entry, now);
        });
    }
}
impl Drop for WorkGuard {
    fn drop(&mut self) {
        self.finish(if std::thread::panicking() {
            Outcome::Panicked
        } else {
            Outcome::Cancelled
        });
    }
}
pub fn progress(stage: Stage) {
    let Some(context) = crate::work::current() else {
        return;
    };
    let now = monotonic_ms();
    with_store(context.consent_generation, |state| {
        if let Some(entry) = state.active.get_mut(&context.token) {
            let transition =
                entry.stage != stage || now.saturating_sub(entry.last_progress_ms) >= 1000;
            entry.stage = stage;
            entry.last_progress_ms = now;
            if transition {
                let entry = *entry;
                record_work(state, entry, now);
            }
        }
    });
}
/// Uses the existing bounded work registry/journal. Callers aggregate hot-loop
/// phases locally and throttle these updates rather than recording each block.
pub(crate) fn portable_progress(
    context: Option<WorkContext>,
    detail: crate::portable_payload::LiveOperation,
) {
    let Some(context) = context else { return };
    let now = monotonic_ms();
    with_store(context.consent_generation, |state| {
        if let Some(entry) = state.active.get_mut(&context.token) {
            entry.portable_payload = Some(detail);
            entry.last_progress_ms = now;
            let entry = *entry;
            record_work(state, entry, now);
        }
    });
}
pub(crate) fn portable_hold_rejected(
    context: Option<WorkContext>,
    family: crate::portable_payload::Family,
) {
    let Some(context) = context else { return };
    let now = monotonic_ms();
    with_store(context.consent_generation, |state| {
        if let Some(entry) = state.active.get_mut(&context.token) {
            entry.portable_hold_rejection = Some(family);
            let entry = *entry;
            record_work(state, entry, now);
        }
    });
}
pub fn retry(backoff_ms: u64) {
    let Some(context) = crate::work::current() else {
        return;
    };
    let now = monotonic_ms();
    with_store(context.consent_generation, |state| {
        if let Some(entry) = state.active.get_mut(&context.token) {
            if entry.context.attempt == u16::MAX {
                let count = &mut state.context_loss_counts[0];
                if *count == u64::MAX {
                    state.metric_overflow = true;
                }
                *count = count.saturating_add(1);
            }
            entry.context = entry.context.retry();
            entry.retry_backoff_ms = Some(backoff_ms);
            let entry = *entry;
            record_work(state, entry, now);
        }
    });
}
pub struct WaitGuard {
    context: Option<WorkContext>,
    previous: Option<(WorkState, Stage)>,
    previous_deadline: Option<Option<u64>>,
}
impl WaitGuard {
    pub fn new(kind: WaitKind) -> Self {
        Self::state(WorkState::Waiting(kind))
    }
    /// The owner supplies the remaining timeout; expired deadlines map to now.
    pub fn with_timeout(kind: WaitKind, duration: std::time::Duration) -> Self {
        let mut guard = Self::new(kind);
        if let Some(context) = guard.context {
            guard.previous_deadline = with_store(context.consent_generation, |state| {
                state.active.get_mut(&context.token).map(|entry| {
                    let previous = entry.deadline_ms;
                    entry.deadline_ms = Some(
                        monotonic_ms()
                            .saturating_add(duration.as_millis().min(u64::MAX as u128) as u64),
                    );
                    previous
                })
            })
            .flatten();
        }
        guard
    }
    pub fn sleeping_until(at: u64) -> Self {
        Self::state(WorkState::SleepingUntil(at))
    }
    fn state(next: WorkState) -> Self {
        let context = crate::work::current();
        let now = monotonic_ms();
        let previous = context.and_then(|context| {
            with_store(context.consent_generation, |state| {
                if let Some(entry) = state.active.get_mut(&context.token) {
                    let previous = (entry.state, entry.stage);
                    entry.state = next;
                    entry.state_since_ms = now;
                    let entry = *entry;
                    record_work(state, entry, now);
                    Some(previous)
                } else {
                    None
                }
            })
            .flatten()
        });
        Self {
            context,
            previous,
            previous_deadline: None,
        }
    }
}
impl Drop for WaitGuard {
    fn drop(&mut self) {
        let Some((context, previous)) = self.context.zip(self.previous) else {
            return;
        };
        let now = monotonic_ms();
        with_store(context.consent_generation, |state| {
            if let Some(entry) = state.active.get_mut(&context.token) {
                entry.state = previous.0;
                entry.stage = previous.1;
                if let Some(deadline) = self.previous_deadline {
                    entry.deadline_ms = deadline;
                }
                entry.state_since_ms = now;
                let entry = *entry;
                record_work(state, entry, now);
            }
        });
    }
}
pub async fn waiting<F: std::future::Future>(kind: WaitKind, future: F) -> F::Output {
    let _wait = WaitGuard::new(kind);
    future.await
}
pub async fn sleeping<F: std::future::Future>(
    duration: std::time::Duration,
    future: F,
) -> F::Output {
    let _wait = WaitGuard::sleeping_until(
        monotonic_ms().saturating_add(duration.as_millis().min(u64::MAX as u128) as u64),
    );
    future.await
}
pub(crate) fn sample_resources(resources: crate::resources::ResourceSnapshot) {
    let epoch = resources.consent_generation;
    let now = monotonic_ms();
    // Sampler-side serialization is outside all hot paths/DB/UI and used only to
    // account the bounded snapshot's retained cost (including owned strings).
    let cost = serde_json::to_vec(&resources)
        .map(|b| b.len().saturating_mul(2))
        .unwrap_or(MAX_BYTES + 1)
        .saturating_add(std::mem::size_of::<JournalRecord>());
    with_store(epoch, |state| {
        state.sweep(now);
        state.append(JournalPayload::Resources(Box::new(resources)), cost, now);
        let queues: Vec<_> = state.queues.values().map(|q| q.snapshot(now)).collect();
        for queue in queues {
            state.append(
                JournalPayload::Queue(queue),
                std::mem::size_of::<JournalRecord>() + 1024,
                now,
            );
        }
        state.append(
            JournalPayload::Health(crate::telemetry::export_health()),
            std::mem::size_of::<JournalRecord>() + 1024,
            now,
        );
    });
}
pub(crate) fn record_pool_snapshot(
    epoch: u64,
    role: crate::DatabaseRole,
    capacity: u64,
    snapshot: crate::DatabasePoolSnapshot,
) {
    with_store(epoch, |state| {
        let (index, role) = match role {
            crate::DatabaseRole::Reader => (0, "reader"),
            crate::DatabaseRole::Writer => (1, "writer"),
        };
        let size = snapshot.size.min(capacity);
        state.pools[index]=Some(PoolSnapshot{role:role.into(),sampled_ms:monotonic_ms(),size_connections:size,idle_connections:snapshot.idle.min(size),capacity_connections:capacity,source:"existing SQLx size/num_idle exporter callback; no acquire/query/probe; freshness relative to SDK collection, not resource sampler cadence".into(),waiters:None,wait_coverage:"SQLx pool waiter count and foreign SQLite-worker wait unavailable from pinned public owner API; no inferred zero".into()});
    });
}
pub fn snapshot() -> Option<LiveSnapshot> {
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return None;
    }
    let now = monotonic_ms();
    with_store(epoch, |state| {
        state.sweep(now);
        LiveSnapshot{database_pools:state.pools.iter().flatten().cloned().collect(),schema_version:1,consent_generation:epoch,snapshot_ms:now,active:state.active.values().copied().collect(),queues:state.queues.values().map(|q|q.snapshot(now)).collect(),journal:state.journal.iter().cloned().collect(),first_sequence:state.journal.front().map(|r|r.sequence),last_sequence:state.sequence,overwritten:state.overwritten,lost:state.lost,active_overflow:state.overflow,abandoned:state.abandoned,queue_overflow:state.queue_overflow,metric_counter_overflow:state.metric_overflow,journal_bytes:state.bytes,consistency:"one registry-lock instant; OS/resources/runtimes independently timestamped; no global atomic application snapshot".into()}
    })
}

struct LiveMetrics {
    active: opentelemetry::metrics::Gauge<u64>,
    age: opentelemetry::metrics::Gauge<f64>,
    queue: opentelemetry::metrics::Gauge<u64>,
    queue_age: opentelemetry::metrics::Gauge<f64>,
    loss: opentelemetry::metrics::Gauge<u64>,
    runtime: opentelemetry::metrics::Gauge<u64>,
}
static METRICS: OnceLock<LiveMetrics> = OnceLock::new();
pub(crate) fn init(meter: &opentelemetry::metrics::Meter) {
    let _=METRICS.set(LiveMetrics{active:meter.u64_gauge("pioneer.work.active").with_unit("{work}").build(),age:meter.f64_gauge("pioneer.work.oldest.age").with_unit("s").build(),queue:meter.u64_gauge("pioneer.queue.current").with_description("Current aggregate per closed lane; field specifies elements/capacity/bytes/coverage, never admission histograms").build(),queue_age:meter.f64_gauge("pioneer.queue.oldest.age").with_unit("s").build(),loss:meter.u64_gauge("pioneer.observation.loss.snapshot").with_description("Local epoch lifetime snapshot, gauge NOT a cumulative sum submitted under Delta").build(),runtime:meter.u64_gauge("pioneer.runtime.current").with_description("Stable aggregate counters by closed runtime role; no per-task/thread labels").build()});
}
pub(crate) fn emit_current() {
    crate::work::emit_pending();
    crate::queues::emit_pending();
    use opentelemetry::KeyValue;
    let Some(metrics) = METRICS.get() else { return };
    let Some(snapshot) = snapshot() else { return };
    let mut active: BTreeMap<(&'static str, &'static str), (u64, u64)> = BTreeMap::new();
    // Emit closed catalog combinations including observed zero, so stale gauges
    // cannot survive a finished work unit. Oldest is meaningful with active >0.
    for kind in [
        crate::work::WorkKind::WorkerLifecycle,
        crate::work::WorkKind::Database,
        crate::work::WorkKind::RpcRead,
        crate::work::WorkKind::RpcMutation,
        crate::work::WorkKind::RpcTurn,
        crate::work::WorkKind::RpcControl,
        crate::work::WorkKind::RpcUnknown,
        crate::work::WorkKind::Message,
        crate::work::WorkKind::Task,
        crate::work::WorkKind::PostStartup,
        crate::work::WorkKind::Event,
        crate::work::WorkKind::ClientCommand,
        crate::work::WorkKind::ClientRpc,
        crate::work::WorkKind::ClientPublication,
        crate::work::WorkKind::Maintenance,
        crate::work::WorkKind::Blocking,
        crate::work::WorkKind::Child,
    ] {
        for state in ["queued", "running", "waiting", "sleeping"] {
            active.insert((kind.label(), state), (0, 0));
        }
    }
    for entry in &snapshot.active {
        let state = match entry.state {
            WorkState::Queued => "queued",
            WorkState::Running => "running",
            WorkState::Waiting(_) => "waiting",
            WorkState::SleepingUntil(_) => "sleeping",
            WorkState::Terminal(_) => continue,
        };
        let value = active
            .entry((entry.context.kind.label(), state))
            .or_default();
        value.0 += 1;
        value.1 = value
            .1
            .max(snapshot.snapshot_ms.saturating_sub(entry.last_progress_ms));
    }
    for ((kind, state), (count, age)) in active {
        let attrs = [KeyValue::new("kind", kind), KeyValue::new("state", state)];
        metrics.active.record(count, &attrs);
        if count > 0 {
            metrics.age.record(age as f64 / 1000.0, &attrs);
        }
    }
    for (reason, value) in [
        ("active_overflow", snapshot.active_overflow),
        ("abandoned", snapshot.abandoned),
        ("journal_overwritten", snapshot.overwritten),
        ("journal_lost", snapshot.lost),
        ("queue_overflow", snapshot.queue_overflow),
    ] {
        metrics
            .loss
            .record(value, &[KeyValue::new("reason", reason)]);
    }
    let mut queues: BTreeMap<&str, (u64, u64, u64, u64, Option<u64>, bool)> = BTreeMap::new();
    for kind in crate::queues::QueueKind::ALL {
        queues.insert(kind.label(), (0, 0, 0, 0, None, false));
    }
    for queue in &snapshot.queues {
        let value = queues
            .entry(queue.kind.label())
            .or_insert((0, 0, 0, 0, None, true));
        if value.0 == 0 {
            value.5 = true;
        }
        value.0 += 1;
        value.1 = value.1.saturating_add(queue.depth);
        value.2 = value.2.saturating_add(queue.capacity.unwrap_or(0) as u64);
        value.3 = value.3.saturating_add(queue.bytes);
        value.4 = value
            .4
            .zip(queue.oldest_age_ms)
            .map(|(a, b)| a.max(b))
            .or(value.4)
            .or(queue.oldest_age_ms);
        value.5 &= queue.initial_depth_known;
    }
    for (kind, (count, depth, capacity, bytes, oldest, known)) in queues {
        // A maximum over only the known subset is not the oldest of this lane.
        // Preserve native per-owner detail locally; omit the aggregate on gaps.
        let complete_heads = known
            && snapshot
                .queues
                .iter()
                .filter(|q| q.kind.label() == kind)
                .all(|q| q.depth == 0 || q.oldest_age_ms.is_some());
        let oldest = oldest.filter(|_| complete_heads);
        for (field, value) in [
            ("registered_queues", count),
            ("depth_elements", depth),
            ("known_capacity_elements", capacity),
            ("accounted_payload_bytes", bytes),
            ("depth_known", u64::from(known)),
            ("oldest_available", u64::from(oldest.is_some())),
            (
                "bytes_known",
                u64::from(
                    count > 0
                        && snapshot
                            .queues
                            .iter()
                            .filter(|q| q.kind.label() == kind)
                            .all(|q| q.byte_quality == "known_payload_bytes"),
                ),
            ),
        ] {
            if (field == "depth_elements" || field == "accounted_payload_bytes")
                && (!known || count == 0)
            {
                continue;
            }
            metrics.queue.record(
                value,
                &[
                    KeyValue::new("queue", kind.to_owned()),
                    KeyValue::new("field", field),
                ],
            );
        }
        if let Some(age) = oldest {
            metrics.queue_age.record(
                age as f64 / 1000.0,
                &[KeyValue::new("queue", kind.to_owned())],
            );
        }
    }
    if let Some(runtime) = crate::runtime::snapshot() {
        let mut roles: BTreeMap<&str, (u64, u64, u64, u64)> = BTreeMap::new();
        for role in crate::runtime::RuntimeRole::ALL {
            roles.insert(role.label(), (0, 0, 0, 0));
        }
        for entry in runtime.runtimes {
            let value = roles.entry(entry.role.label()).or_default();
            value.0 += entry.workers as u64;
            value.1 += entry.alive_tasks as u64;
            value.2 += entry.global_queue_depth as u64;
            value.3 += 1;
        }
        for (role, (workers, tasks, depth, count)) in roles {
            for (field, value) in [
                ("registered_runtimes", count),
                ("workers", workers),
                ("alive_tasks", tasks),
                ("global_queue_depth", depth),
            ] {
                metrics.runtime.record(
                    value,
                    &[KeyValue::new("role", role), KeyValue::new("field", field)],
                );
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct QueuedWork {
    context: WorkContext,
    armed: bool,
}
impl Clone for QueuedWork {
    fn clone(&self) -> Self {
        Self {
            context: self.context,
            armed: false,
        }
    }
}
impl QueuedWork {
    pub(crate) fn new(context: Option<WorkContext>) -> Option<Self> {
        let context = context?;
        let now = monotonic_ms();
        with_store(context.consent_generation, |state| {
            if state.active.contains_key(&context.token) {
                return None;
            }
            if state.active.len() >= MAX_ACTIVE {
                state.overflow = state.overflow.saturating_add(1);
                return None;
            }
            let entry = ActiveWork {
                portable_payload: None,
                portable_hold_rejection: None,
                pending_outcome: None,
                wait_stack: [None; 4],
                wait_overflow: 0,
                context,
                state: WorkState::Queued,
                stage: Stage::Queued,
                started_ms: now,
                last_progress_ms: now,
                state_since_ms: now,
                deadline_ms: None,
                retry_backoff_ms: None,
                worker_lifetime: false,
                lease_count: 0,
            };
            state.active.insert(context.token, entry);
            record_work(state, entry, now);
            Some(Self {
                context,
                armed: true,
            })
        })
        .flatten()
    }
}
impl Drop for QueuedWork {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let now = monotonic_ms();
        with_store(self.context.consent_generation, |state| {
            if state
                .active
                .get(&self.context.token)
                .is_some_and(|e| e.state == WorkState::Queued)
            {
                let mut entry = state.active.remove(&self.context.token).unwrap();
                entry.state = WorkState::Terminal(Outcome::Cancelled);
                entry.state_since_ms = now;
                record_work(state, entry, now);
            }
        });
    }
}

/// Paired owner events (admission enqueued/acquired/cancelled). The callback
/// has no await; its wait stack lives in the ONE active registry, bounded at 4.
pub fn admission_wait(enter: bool) {
    let Some(context) = crate::work::current() else {
        return;
    };
    let now = monotonic_ms();
    with_store(context.consent_generation, |state| {
        if let Some(entry) = state.active.get_mut(&context.token) {
            if enter {
                if let Some(slot) = entry.wait_stack.iter_mut().find(|s| s.is_none()) {
                    *slot = Some((entry.state, entry.stage));
                    entry.state = WorkState::Waiting(WaitKind::DatabaseAdmission);
                    entry.stage = Stage::Admission;
                } else {
                    entry.wait_overflow = entry.wait_overflow.saturating_add(1);
                }
            } else if entry.wait_overflow > 0 {
                entry.wait_overflow -= 1;
            } else if let Some(slot) = entry.wait_stack.iter_mut().rev().find(|s| s.is_some()) {
                let (previous, stage) = slot.take().unwrap();
                entry.state = previous;
                entry.stage = stage;
            }
            entry.state_since_ms = now;
            entry.last_progress_ms = now;
            let entry = *entry;
            record_work(state, entry, now);
        }
    });
}

#[cfg(test)]
mod model_tests {
    use super::*;
    use crate::work::{Boundary, ExecutorRole, Trigger, WorkCarrier, WorkKind};
    fn context() -> WorkCarrier {
        WorkCarrier::independent(
            Boundary::Timer,
            WorkKind::Maintenance,
            Trigger::Timer,
            ExecutorRole::Gateway,
        )
    }
    #[test]
    fn owned_work_survives_long_age_with_and_without_progress() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let progressing = context();
        let stalled = context();
        let mut first = WorkGuard::start(progressing.context(), false);
        let second = WorkGuard::start(stalled.context(), false);
        let epoch = progressing.context().unwrap().consent_generation;
        with_store(epoch, |store| {
            for e in store.active.values_mut() {
                e.started_ms = 0;
                e.last_progress_ms = 0;
            }
            store
                .active
                .get_mut(&progressing.context().unwrap().token)
                .unwrap()
                .last_progress_ms = 1_000_000;
            store.sweep(1_000_001);
            assert_eq!(store.active.len(), 2);
            assert_eq!(store.abandoned, 0);
            assert!(store.journal.is_empty()); // old history expired, active ownership retained
        });
        first.finish(Outcome::Completed);
        first.finish(Outcome::Failed);
        drop(second);
        let snap = snapshot().unwrap();
        assert!(snap.active.is_empty());
        let terminal: Vec<_> = snap
            .journal
            .iter()
            .filter_map(|r| match r.payload {
                JournalPayload::Work(e) if matches!(e.state, WorkState::Terminal(_)) => {
                    Some(e.state)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            terminal,
            [
                WorkState::Terminal(Outcome::Completed),
                WorkState::Terminal(Outcome::Cancelled)
            ]
        );
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn retry_metadata_and_scoped_backoff_restore_running_and_outer_wait() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let carrier = context();
        let _scope = carrier.enter();
        let active = WorkGuard::start(carrier.context(), false);
        for outer in [None, Some(WaitKind::Reply)] {
            let wait = outer.map(WaitGuard::new);
            let previous = snapshot().unwrap().active[0].state;
            retry(200);
            assert_eq!(snapshot().unwrap().active[0].state, previous);
            let sleep = WaitGuard::sleeping_until(500);
            assert_eq!(
                snapshot().unwrap().active[0].state,
                WorkState::SleepingUntil(500)
            );
            drop(sleep);
            progress(Stage::Executing);
            assert_eq!(snapshot().unwrap().active[0].state, previous);
            assert_eq!(snapshot().unwrap().active[0].retry_backoff_ms, Some(200));
            drop(wait);
        }
        assert_eq!(
            snapshot().unwrap().active[0].context.attempt,
            carrier.context().unwrap().attempt + 2
        );
        drop(active);
        assert!(snapshot().unwrap().active.is_empty());
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn lease_overflow_does_not_claim_or_release_an_existing_lease() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let carrier = context();
        let context = carrier.context().unwrap();
        let mut owner = WorkGuard::start(Some(context), false);
        with_store(context.consent_generation, |state| {
            state.active.get_mut(&context.token).unwrap().lease_count = u16::MAX;
        });
        drop(WorkGuard::start(Some(context), false));
        let snapshot = snapshot().unwrap();
        assert_eq!(snapshot.active.len(), 1);
        assert_eq!(snapshot.active[0].lease_count, u16::MAX);
        assert_eq!(snapshot.active_overflow, 1);
        with_store(context.consent_generation, |state| {
            state.active.get_mut(&context.token).unwrap().lease_count = 1;
        });
        owner.finish(Outcome::Completed);
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn nested_failure_and_cancellation_are_terminal_not_success() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let carrier = context();
        let token = carrier.context().unwrap().token;
        let mut outer = WorkGuard::start(carrier.context(), false);
        let mut nested = WorkGuard::start(carrier.context(), false);
        nested.finish(Outcome::Failed);
        assert_eq!(snapshot().unwrap().active.len(), 1);
        outer.finish(Outcome::Completed);
        let snap = snapshot().unwrap();
        assert!(snap.active.is_empty());
        assert!(snap.journal.iter().any(|r| matches!(&r.payload, JournalPayload::Work(e) if e.context.token == token && e.state == WorkState::Terminal(Outcome::Failed))));
        let carrier = context();
        let token = carrier.context().unwrap().token;
        drop(WorkGuard::start(carrier.context(), false));
        assert!(snapshot().unwrap().journal.iter().any(|r| matches!(&r.payload, JournalPayload::Work(e) if e.context.token == token && e.state == WorkState::Terminal(Outcome::Cancelled))));
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn known_sleep_nested_wait_and_epoch_are_distinct() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let carrier = context();
        let _scope = carrier.enter();
        let mut active = WorkGuard::start(carrier.context(), false);
        let sleep = WaitGuard::sleeping_until(50_000);
        {
            let _wait = WaitGuard::new(WaitKind::Reply);
            assert_eq!(
                snapshot().unwrap().active[0].state,
                WorkState::Waiting(WaitKind::Reply)
            );
        }
        assert_eq!(
            snapshot().unwrap().active[0].state,
            WorkState::SleepingUntil(50_000)
        );
        drop(sleep);
        assert_eq!(snapshot().unwrap().active[0].state, WorkState::Running);
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        active.finish(Outcome::Completed);
        let snap = snapshot().unwrap();
        assert!(snap.active.is_empty());
        assert!(snap.journal.is_empty());
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn ring_count_bytes_age_and_sequence_gaps_have_independent_expected_values() {
        let mut store = LiveStore::new(1);
        for at in 0..4100 {
            store.append(
                JournalPayload::Health(crate::telemetry::export_health()),
                64,
                at,
            );
        }
        assert_eq!(store.journal.len(), 4096);
        assert_eq!(store.overwritten, 4);
        assert_eq!(store.journal.front().unwrap().sequence, 5);
        assert_eq!(store.bytes, 4096 * 64);
        store.append(
            JournalPayload::Health(crate::telemetry::export_health()),
            MAX_BYTES + 1,
            4101,
        );
        assert_eq!(store.lost, 1);
        assert_eq!(store.sequence, 4101);
        store.trim(124_101);
        assert!(store.journal.is_empty());
        assert_eq!(store.bytes, 0);
        store.append(
            JournalPayload::Health(crate::telemetry::export_health()),
            MAX_BYTES,
            124_102,
        );
        store.append(
            JournalPayload::Health(crate::telemetry::export_health()),
            1,
            124_103,
        );
        assert_eq!(store.journal.len(), 1);
        assert_eq!(store.bytes, 1);
        assert_eq!(store.journal.front().unwrap().sequence, 4103);
    }
    #[test]
    fn active_overflow_never_evicts_running_work_as_success() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let guards: Vec<_> = (0..MAX_ACTIVE + 3)
            .map(|_| WorkGuard::start(context().context(), false))
            .collect();
        let snap = snapshot().unwrap();
        assert_eq!(snap.active.len(), 512);
        assert_eq!(snap.active_overflow, 3);
        assert!(!snap.journal.iter().any(|r|matches!(&r.payload,JournalPayload::Work(e) if matches!(e.state,WorkState::Terminal(_)))));
        drop(guards);
        assert!(snapshot().unwrap().active.is_empty());
        crate::set_telemetry_enabled(old);
    }
}
