//! Observation at existing queue owners. This does not wrap/replace channels,
//! alter capacity, order, backpressure, ACK or delivery semantics.
use crate::{
    identity::monotonic_ms,
    live::{self, JournalPayload, LiveStore},
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueueKind {
    GatewayOutbound,
    ClientCommands,
    ClientRpc,
    ClientSubscriptions,
    ClientPublications,
    ClientController,
    EventDurable,
    EventProgress,
    EventProgressBroadcast,
    EventSnapshot,
    OrderedDurable,
    OrderedCoalesced,
    DatabaseAdmission,
    DatabaseReadAdmission,
    DatabasePool,
    DatabaseBlocking,
}
impl QueueKind {
    pub(crate) const ALL: [Self; 16] = [
        Self::GatewayOutbound,
        Self::ClientCommands,
        Self::ClientRpc,
        Self::ClientSubscriptions,
        Self::ClientPublications,
        Self::ClientController,
        Self::EventDurable,
        Self::EventProgress,
        Self::EventProgressBroadcast,
        Self::EventSnapshot,
        Self::OrderedDurable,
        Self::OrderedCoalesced,
        Self::DatabaseAdmission,
        Self::DatabaseReadAdmission,
        Self::DatabasePool,
        Self::DatabaseBlocking,
    ];
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::GatewayOutbound => "gateway_outbound",
            Self::ClientCommands => "client_commands",
            Self::ClientRpc => "client_rpc",
            Self::ClientSubscriptions => "client_subscriptions",
            Self::ClientPublications => "client_publications",
            Self::ClientController => "client_controller",
            Self::EventDurable => "event_durable",
            Self::EventProgress => "event_progress",
            Self::EventProgressBroadcast => "event_progress_broadcast",
            Self::EventSnapshot => "event_snapshot",
            Self::OrderedDurable => "ordered_durable",
            Self::OrderedCoalesced => "ordered_coalesced",
            Self::DatabaseAdmission => "database_admission",
            Self::DatabaseReadAdmission => "database_read_admission",
            Self::DatabasePool => "database_pool",
            Self::DatabaseBlocking => "database_blocking",
        }
    }
}
static EVENTS: std::sync::OnceLock<opentelemetry::metrics::Counter<u64>> =
    std::sync::OnceLock::new();
pub(crate) fn init(meter: &opentelemetry::metrics::Meter) {
    let _=EVENTS.set(meter.u64_counter("pioneer.queue.events").with_unit("{event}").with_description("Closed queue/event labels; Delta sums; enqueue-observed-or-prepared minus rejected-send-after-prepare gives accepted enqueue; dequeue/processed/commit are separate milestones").build());
}
const EVENTS_CATALOG: [&str; 15] = [
    "enqueue_observed_or_prepared",
    "dequeued",
    "processed",
    "delivery_dropped",
    "cancelled",
    "dropped",
    "rejected",
    "coalesced_input",
    "stale",
    "resnapshot",
    "replay",
    "committed",
    "commit_rejected",
    "rejected_send_after_prepare",
    "send_capacity_cancelled",
];
fn record(epoch: u64, kind: QueueKind, event: &'static str, count: u64) {
    let Some(index) = EVENTS_CATALOG.iter().position(|s| *s == event) else {
        return;
    };
    live::with_store(epoch, |state| {
        let dest = &mut state.queue_counts[kind as usize * 15 + index];
        if dest.checked_add(count).is_none() {
            state.metric_overflow = true;
        }
        *dest = dest.saturating_add(count);
    });
}
pub(crate) fn emit_pending() {
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return;
    }
    let Some(counts) = live::with_store(epoch, |state| {
        let counts = state.queue_counts;
        state.queue_counts = [0; 240];
        counts
    }) else {
        return;
    };
    if let Some(counter) = EVENTS.get() {
        for kind in QueueKind::ALL {
            for (index, event) in EVENTS_CATALOG.into_iter().enumerate() {
                let n = counts[kind as usize * 15 + index];
                if n > 0 {
                    counter.add(
                        n,
                        &[
                            opentelemetry::KeyValue::new("queue", kind.label()),
                            opentelemetry::KeyValue::new("event", event),
                        ],
                    );
                }
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(tag = "quality", content = "bytes", rename_all = "snake_case")]
pub enum QueueBytes {
    Known(u64),
    Estimate(u64),
    Unsupported,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueueOutcome {
    Processed,
    DeliveryDropped,
    Cancelled,
    Dropped,
    Rejected,
    Coalesced,
    Stale,
    Resnapshot,
    Replay,
    Committed,
    CommitRejected,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueSnapshot {
    pub in_processing: u64,
    pub abandoned_processing: u64,
    pub delivery_dropped: u64,
    pub send_capacity_cancelled: u64,
    pub initial_depth_known: bool,
    pub balance_scope: String,
    pub observation_started_ms: u64,
    pub id: u64,
    pub kind: QueueKind,
    pub capacity: Option<usize>,
    pub snapshot_ms: u64,
    /// Last producer observation, distinct from the time of a diagnostic read.
    pub last_observed_ms: u64,
    pub depth: u64,
    pub bytes: u64,
    pub byte_quality: String,
    pub byte_definition: String,
    pub oldest_age_ms: Option<u64>,
    pub age_coverage: String,
    pub offered: u64,
    pub enqueued: u64,
    pub dequeued: u64,
    pub processed: u64,
    pub rejected: u64,
    pub cancelled: u64,
    pub dropped: u64,
    pub coalesced: u64,
    pub stale: u64,
    pub resnapshot: u64,
    pub replay: u64,
    pub committed: u64,
    pub commit_rejected: u64,
    pub send_waiters: u64,
    pub metadata_overflow: u64,
    pub accounting_errors: u64,
}
pub(crate) struct QueueState {
    snapshot: QueueSnapshot,
    native_oldest: Option<(u64, u64)>, // source time, age at source (ms)
    native_revision: Option<u64>,
    native_oldest_recorded: bool,
}
impl QueueState {
    fn is_native_admission(&self) -> bool {
        matches!(
            self.snapshot.kind,
            QueueKind::DatabaseAdmission | QueueKind::DatabaseReadAdmission
        )
    }
    fn has_owner_current(&self) -> bool {
        self.is_native_admission()
            || matches!(
                self.snapshot.kind,
                QueueKind::EventDurable
                    | QueueKind::EventSnapshot
                    | QueueKind::EventProgressBroadcast
            )
    }
    fn fifo_age_supported(&self) -> bool {
        !matches!(
            self.snapshot.kind,
            QueueKind::EventProgress
                | QueueKind::EventProgressBroadcast
                | QueueKind::EventSnapshot
                | QueueKind::ClientPublications
                | QueueKind::DatabaseAdmission
                | QueueKind::DatabaseReadAdmission
        )
    }
    fn new(id: u64, kind: QueueKind, capacity: Option<usize>, definition: &'static str) -> Self {
        Self {
            snapshot: QueueSnapshot {
                in_processing: 0,
                abandoned_processing: 0,
                delivery_dropped: 0,
                send_capacity_cancelled: 0,
                initial_depth_known: true,
                balance_scope: "events since observation_started_ms; mid-epoch registration may omit earlier items; current owner depth is authoritative when available; replay/commit are separate milestones".into(),
                observation_started_ms: monotonic_ms(),
                id,
                kind,
                capacity,
                snapshot_ms: 0,
                last_observed_ms: monotonic_ms(),
                depth: 0,
                bytes: 0,
                byte_quality: "unsupported".into(),
                byte_definition: definition.into(),
                oldest_age_ms: None,
                age_coverage: "observed_empty".into(),
                offered: 0,
                enqueued: 0,
                dequeued: 0,
                processed: 0,
                rejected: 0,
                cancelled: 0,
                dropped: 0,
                coalesced: 0,
                stale: 0,
                resnapshot: 0,
                replay: 0,
                committed: 0,
                commit_rejected: 0,
                send_waiters: 0,
                metadata_overflow: 0,
                accounting_errors: 0,
            },
            native_oldest: None,
            native_revision: None,
            native_oldest_recorded: false,
        }
    }
    pub(crate) fn snapshot(&self, now: u64) -> QueueSnapshot {
        let mut value = self.snapshot.clone();
        value.snapshot_ms = now;
        value.oldest_age_ms = None;
        if self.native_oldest_recorded {
            value.oldest_age_ms = self
                .native_oldest
                .map(|(at, age)| now.saturating_sub(at).saturating_add(age));
        }
        if !value.initial_depth_known {
            value.oldest_age_ms = None;
        }
        value.age_coverage = if !value.initial_depth_known {
            "initial_depth_not_recorded"
        } else if value.depth == 0 {
            "observed_empty"
        } else if self.native_oldest_recorded {
            "observed_native_owner_head"
        } else if matches!(
            value.kind,
            QueueKind::DatabaseAdmission | QueueKind::DatabaseReadAdmission
        ) {
            "unavailable_from_current_admission_observer"
        } else if !self.fifo_age_supported() {
            "not_recorded_non_fifo_or_retained_journal"
        } else {
            "unavailable_without_native_item_order"
        }
        .into();
        value
    }
    fn enqueue(&mut self, bytes: QueueBytes, _now: u64) {
        self.snapshot.offered = self.snapshot.offered.saturating_add(1);
        self.snapshot.enqueued = self.snapshot.enqueued.saturating_add(1);
        if self.has_owner_current() {
            return;
        }
        self.native_oldest_recorded = false;
        self.native_oldest = None;
        self.snapshot.depth = self.snapshot.depth.saturating_add(1);
        match bytes {
            QueueBytes::Known(n) => {
                self.snapshot.bytes = self.snapshot.bytes.saturating_add(n);
                if self.snapshot.byte_quality == "unsupported" && self.snapshot.depth == 1 {
                    self.snapshot.byte_quality = "known_payload_bytes".into();
                }
            }
            QueueBytes::Estimate(n) => {
                self.snapshot.bytes = self.snapshot.bytes.saturating_add(n);
                self.snapshot.byte_quality = "estimate_payload_bytes".into();
            }
            QueueBytes::Unsupported => {
                self.snapshot.byte_quality = "unsupported_or_partial".into();
            }
        }
        // Multiple producers can record before send in a different order from
        // delivery; depth (even a native depth) cannot attest the head's time.
    }
    fn dequeue(&mut self, bytes: QueueBytes) {
        self.snapshot.in_processing = self.snapshot.in_processing.saturating_add(1);
        self.snapshot.dequeued = self.snapshot.dequeued.saturating_add(1);
        if self.has_owner_current() {
            return;
        }
        self.native_oldest_recorded = false;
        self.native_oldest = None;
        if self.snapshot.depth == 0 {
            if self.snapshot.initial_depth_known {
                self.snapshot.accounting_errors = self.snapshot.accounting_errors.saturating_add(1);
            }
        } else {
            self.snapshot.depth -= 1;
        }
        match bytes {
            QueueBytes::Known(n) | QueueBytes::Estimate(n) => {
                self.snapshot.bytes = self.snapshot.bytes.saturating_sub(n)
            }
            QueueBytes::Unsupported => {
                self.snapshot.byte_quality = "unsupported_or_partial".into();
            }
        }
        if self.snapshot.depth == 0 {
            self.snapshot.bytes = 0;
        }
    }
    fn outcome(&mut self, outcome: QueueOutcome, count: u64) {
        let dest = match outcome {
            QueueOutcome::Processed => &mut self.snapshot.processed,
            QueueOutcome::DeliveryDropped => &mut self.snapshot.delivery_dropped,
            QueueOutcome::Cancelled => &mut self.snapshot.cancelled,
            QueueOutcome::Dropped => &mut self.snapshot.dropped,
            QueueOutcome::Rejected => &mut self.snapshot.rejected,
            QueueOutcome::Coalesced => &mut self.snapshot.coalesced,
            QueueOutcome::Stale => &mut self.snapshot.stale,
            QueueOutcome::Resnapshot => &mut self.snapshot.resnapshot,
            QueueOutcome::Replay => &mut self.snapshot.replay,
            QueueOutcome::Committed => &mut self.snapshot.committed,
            QueueOutcome::CommitRejected => &mut self.snapshot.commit_rejected,
        };
        *dest = dest.saturating_add(count);
        if matches!(outcome, QueueOutcome::Processed | QueueOutcome::Cancelled) {
            self.snapshot.in_processing = self.snapshot.in_processing.saturating_sub(count);
        }
        if matches!(outcome, QueueOutcome::Rejected | QueueOutcome::Coalesced) {
            self.snapshot.offered = self.snapshot.offered.saturating_add(count);
        }
    }
}
struct QueueLease {
    id: u64,
    epoch: u64,
}
impl Drop for QueueLease {
    fn drop(&mut self) {
        live::with_store(self.epoch, |state| {
            if let Some(mut queue) = state.queues.remove(&self.id) {
                queue.snapshot.dropped =
                    queue.snapshot.dropped.saturating_add(queue.snapshot.depth);
                queue.snapshot.depth = 0;
                queue.snapshot.bytes = 0;
                queue.snapshot.abandoned_processing = queue
                    .snapshot
                    .abandoned_processing
                    .saturating_add(queue.snapshot.in_processing);
                queue.snapshot.in_processing = 0;
                let now = monotonic_ms();
                state.append(
                    JournalPayload::Queue(queue.snapshot(now)),
                    std::mem::size_of::<crate::live::JournalRecord>() + 1024,
                    now,
                );
            }
        });
    }
}
struct Observer {
    lease: Mutex<Option<QueueLease>>,
    kind: QueueKind,
    capacity: Option<usize>,
    definition: &'static str,
}
#[derive(Clone)]
pub struct QueueObserver {
    inner: Arc<Observer>,
    expected_epoch: Option<u64>,
}
impl std::fmt::Debug for QueueObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueObserver")
            .field("kind", &self.inner.kind)
            .finish()
    }
}
impl QueueObserver {
    pub fn new(kind: QueueKind, capacity: Option<usize>, definition: &'static str) -> Self {
        let result = Self {
            expected_epoch: None,
            inner: Arc::new(Observer {
                lease: Mutex::new(None),
                kind,
                capacity,
                definition,
            }),
        };
        result.update(|q| q.snapshot.initial_depth_known = true);
        result
    }
    /// Pin one send/item observation across native send/error/drop paths.
    /// The long-lived owner remains unpinned and can observe future epochs.
    pub fn in_current_epoch(&self) -> Self {
        let mut observer = self.clone();
        observer
            .expected_epoch
            .get_or_insert_with(|| crate::telemetry_consent_snapshot().1);
        observer
    }
    fn observation_epoch(&self) -> Option<u64> {
        let (enabled, current) = crate::telemetry_consent_snapshot();
        enabled.then_some(self.expected_epoch.unwrap_or(current))
    }
    fn update(&self, callback: impl FnOnce(&mut QueueState)) {
        if let Some(epoch) = self.observation_epoch() {
            self.update_at(epoch, callback);
        }
    }
    fn update_at(&self, epoch: u64, callback: impl FnOnce(&mut QueueState)) {
        self.update_source(epoch, None, |queue| {
            callback(queue);
            true
        });
    }
    fn update_source(
        &self,
        epoch: u64,
        sampled_ms: Option<u64>,
        callback: impl FnOnce(&mut QueueState) -> bool,
    ) {
        if !crate::telemetry_sample_allowed(Some(epoch)) {
            return;
        }
        #[cfg(test)]
        MUTATION_HOOK.with(|hook| {
            let callback = hook.borrow_mut().take();
            if let Some(callback) = callback {
                callback();
            }
        });
        let mut lease = self
            .inner
            .lease
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !crate::telemetry_sample_allowed(Some(epoch)) {
            return;
        }
        if lease.as_ref().is_none_or(|l| l.epoch != epoch) {
            let id = live::with_store(epoch, |state: &mut LiveStore| {
                if state.queues.len() >= live::MAX_QUEUES {
                    state.queue_overflow = state.queue_overflow.saturating_add(1);
                    return None;
                }
                if state.next_queue == u64::MAX {
                    state.queue_overflow = state.queue_overflow.saturating_add(1);
                    return None;
                }
                let id = state.next_queue;
                state.next_queue += 1;
                let mut queue = QueueState::new(
                    id,
                    self.inner.kind,
                    self.inner.capacity,
                    self.inner.definition,
                );
                queue.snapshot.initial_depth_known = false;
                state.queues.insert(id, queue);
                Some(id)
            })
            .flatten();
            *lease = id.map(|id| QueueLease { id, epoch });
        }
        let Some(lease) = lease.as_ref() else { return };
        live::with_store(lease.epoch, |state| {
            if let Some(queue) = state.queues.get_mut(&lease.id) {
                if callback(queue) {
                    if let Some(at) = sampled_ms {
                        queue.snapshot.last_observed_ms = at;
                    } else if !queue.has_owner_current() {
                        queue.snapshot.last_observed_ms = monotonic_ms();
                    }
                }
            }
        });
    }
    pub fn enqueued(&self, bytes: QueueBytes) {
        let Some(epoch) = self.observation_epoch() else {
            return;
        };
        self.update_at(epoch, |q| {
            q.enqueue(bytes, monotonic_ms());
        });
        record(epoch, self.inner.kind, "enqueue_observed_or_prepared", 1);
    }
    pub fn dequeued(&self, bytes: QueueBytes) {
        let Some(epoch) = self.observation_epoch() else {
            return;
        };
        self.update_at(epoch, |q| q.dequeue(bytes));
        record(epoch, self.inner.kind, "dequeued", 1);
    }
    pub fn outcome(&self, outcome: QueueOutcome, count: u64) {
        if let Some(epoch) = self.observation_epoch() {
            self.outcome_at(epoch, outcome, count);
        }
    }
    fn outcome_at(&self, epoch: u64, outcome: QueueOutcome, count: u64) {
        self.update_at(epoch, |q| q.outcome(outcome, count));
        record(
            epoch,
            self.inner.kind,
            match outcome {
                QueueOutcome::Processed => "processed",
                QueueOutcome::DeliveryDropped => "delivery_dropped",
                QueueOutcome::Cancelled => "cancelled",
                QueueOutcome::Dropped => "dropped",
                QueueOutcome::Rejected => "rejected",
                QueueOutcome::Coalesced => "coalesced_input",
                QueueOutcome::Stale => "stale",
                QueueOutcome::Resnapshot => "resnapshot",
                QueueOutcome::Replay => "replay",
                QueueOutcome::Committed => "committed",
                QueueOutcome::CommitRejected => "commit_rejected",
            },
            count,
        );
    }
    pub fn processing(&self) -> ProcessingGuard {
        ProcessingGuard {
            observer: self.clone(),
            epoch: self
                .expected_epoch
                .unwrap_or_else(|| crate::telemetry_consent_snapshot().1),
            finished: false,
        }
    }
    pub fn send_wait(&self) -> SendWait {
        let epoch = self
            .expected_epoch
            .unwrap_or_else(|| crate::telemetry_consent_snapshot().1);
        self.update_at(epoch, |q| {
            q.snapshot.send_waiters = q.snapshot.send_waiters.saturating_add(1)
        });
        SendWait {
            observer: self.clone(),
            epoch,
            finished: false,
        }
    }
    pub fn rejected_send(&self, bytes: QueueBytes) {
        let Some(epoch) = self.observation_epoch() else {
            return;
        };
        self.update_at(epoch, |q| {
            q.native_oldest_recorded = false;
            q.native_oldest = None;
            q.snapshot.enqueued = q.snapshot.enqueued.saturating_sub(1);
            q.snapshot.offered = q.snapshot.offered.saturating_sub(1);
            q.snapshot.depth = q.snapshot.depth.saturating_sub(1);
            match bytes {
                QueueBytes::Known(n) | QueueBytes::Estimate(n) => {
                    q.snapshot.bytes = q.snapshot.bytes.saturating_sub(n)
                }
                QueueBytes::Unsupported => {}
            }
            q.outcome(QueueOutcome::Rejected, 1);
        });
        record(epoch, self.inner.kind, "rejected_send_after_prepare", 1);
    }
    /// A prepared wake was merged into the existing capacity-one slot.
    pub fn coalesced_send(&self, bytes: QueueBytes) {
        let Some(epoch) = self.observation_epoch() else {
            return;
        };
        self.update_at(epoch, |q| {
            q.native_oldest_recorded = false;
            q.native_oldest = None;
            q.snapshot.enqueued = q.snapshot.enqueued.saturating_sub(1);
            q.snapshot.offered = q.snapshot.offered.saturating_sub(1);
            q.snapshot.depth = q.snapshot.depth.saturating_sub(1);
            if let QueueBytes::Known(n) | QueueBytes::Estimate(n) = bytes {
                q.snapshot.bytes = q.snapshot.bytes.saturating_sub(n);
            }
            q.outcome(QueueOutcome::Coalesced, 1);
        });
        // The capacity-one send was rejected after preparation, then merged.
        // This compensates the enqueue milestone without calling it data loss.
        record(epoch, self.inner.kind, "rejected_send_after_prepare", 1);
        record(epoch, self.inner.kind, "coalesced_input", 1);
    }
    /// Source revision/time are captured together under the native admission
    /// lock. Delayed callbacks cannot renew freshness or overwrite newer state.
    pub fn native_admission_snapshot(
        &self,
        expected_epoch: u64,
        revision: u64,
        sampled_ms: u64,
        depth: usize,
        oldest_waited_ms: Option<u64>,
    ) {
        if self
            .expected_epoch
            .is_some_and(|epoch| epoch != expected_epoch)
        {
            return;
        }
        self.update_source(expected_epoch, Some(sampled_ms), |q| {
            if revision == u64::MAX {
                q.snapshot.metadata_overflow = q.snapshot.metadata_overflow.saturating_add(1);
                return false;
            }
            if q.native_revision.is_some_and(|current| revision <= current) {
                return false;
            }
            q.native_revision = Some(revision);
            q.snapshot.initial_depth_known = true;
            q.snapshot.depth = depth as u64;
            q.native_oldest_recorded = depth == 0 || oldest_waited_ms.is_some();
            q.native_oldest = oldest_waited_ms.map(|age| (sampled_ms, age));
            true
        });
    }
    /// The owner holds its native queue lock and supplies its actual front.
    /// Depth alone does not establish age. Old-epoch fronts remain unavailable.
    pub fn native_item_head<T>(&self, depth: usize, head: Option<&crate::work::WorkItem<T>>) {
        let Some(epoch) = self.observation_epoch() else {
            return;
        };
        let at = head.and_then(|item| {
            item.carrier
                .context()
                .filter(|c| c.consent_generation == epoch)
                .and(item.enqueued_monotonic_ms)
        });
        self.update_at(epoch, |q| {
            q.snapshot.initial_depth_known = true;
            q.snapshot.depth = depth as u64;
            q.native_oldest_recorded = depth == 0 || at.is_some();
            q.native_oldest = at.map(|at| (at, 0));
        });
    }
    pub fn current(&self, depth: usize, bytes: QueueBytes) {
        let Some(epoch) = self.observation_epoch() else {
            return;
        };
        self.update_source(epoch, Some(monotonic_ms()), |q| {
            q.native_oldest_recorded = depth == 0;
            q.native_oldest = None;
            q.snapshot.initial_depth_known = true;
            q.snapshot.depth = depth as u64;
            match bytes {
                QueueBytes::Known(n) => {
                    q.snapshot.bytes = n;
                    q.snapshot.byte_quality = "known_payload_bytes".into();
                }
                QueueBytes::Estimate(n) => {
                    q.snapshot.bytes = n;
                    q.snapshot.byte_quality = "estimate_payload_bytes".into();
                }
                QueueBytes::Unsupported => {
                    q.snapshot.byte_quality = "unsupported".into();
                }
            }
            true
        });
    }
}
pub struct SendWait {
    observer: QueueObserver,
    epoch: u64,
    finished: bool,
}
impl SendWait {
    pub fn complete(&mut self) {
        self.finished = true;
    }
}
impl Drop for SendWait {
    fn drop(&mut self) {
        if !crate::telemetry_sample_allowed(Some(self.epoch)) {
            return;
        }
        self.observer.update_at(self.epoch, |q| {
            q.snapshot.send_waiters = q.snapshot.send_waiters.saturating_sub(1);
            if !self.finished {
                q.snapshot.send_capacity_cancelled =
                    q.snapshot.send_capacity_cancelled.saturating_add(1);
            }
        });
        if !self.finished {
            record(
                self.epoch,
                self.observer.inner.kind,
                "send_capacity_cancelled",
                1,
            );
        }
    }
}

pub struct ProcessingGuard {
    observer: QueueObserver,
    epoch: u64,
    finished: bool,
}
impl ProcessingGuard {
    pub fn processed(&mut self) {
        if !self.finished {
            self.finished = true;
            if crate::telemetry_sample_allowed(Some(self.epoch)) {
                self.observer
                    .outcome_at(self.epoch, QueueOutcome::Processed, 1);
            }
        }
    }
}
impl Drop for ProcessingGuard {
    fn drop(&mut self) {
        if !self.finished && crate::telemetry_sample_allowed(Some(self.epoch)) {
            self.observer
                .outcome_at(self.epoch, QueueOutcome::Cancelled, 1);
        }
    }
}

/// Observe an existing nonblocking owner send without replacing its channel.
/// Size is supplied by the owner; no payload serialization is performed here.
pub fn observe_try_send<T, E>(
    observer: Option<&QueueObserver>,
    bytes: QueueBytes,
    send: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let observer = observer.map(QueueObserver::in_current_epoch);
    if let Some(queue) = &observer {
        queue.enqueued(bytes);
    }
    let result = send();
    if result.is_err() {
        if let Some(queue) = &observer {
            queue.rejected_send(bytes);
        }
    }
    result
}

#[cfg(test)]
thread_local! { static MUTATION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None); }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owner_current_event_milestones_preserve_depth_head_and_source_freshness() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let epoch = crate::telemetry_consent_snapshot().1;
        for kind in [
            QueueKind::EventDurable,
            QueueKind::EventSnapshot,
            QueueKind::EventProgressBroadcast,
        ] {
            let queue = QueueObserver::new(kind, Some(2), "fixture source authority");
            queue.current(1, QueueBytes::Unsupported);
            // Controlled source-clock sentinel: a later callback must not replace
            // source time with its own wall/monotonic delivery instant.
            let source_time = u64::MAX - 1;
            live::with_store(epoch, |store| {
                store
                    .queues
                    .values_mut()
                    .find(|q| q.snapshot.kind == kind)
                    .unwrap()
                    .snapshot
                    .last_observed_ms = source_time;
            });
            queue.enqueued(QueueBytes::Unsupported);
            queue.dequeued(QueueBytes::Unsupported);
            queue.outcome(QueueOutcome::Processed, 1);
            queue.outcome(QueueOutcome::Committed, 1);
            let snapshot = crate::live::snapshot().unwrap();
            let q = snapshot.queues.iter().find(|q| q.kind == kind).unwrap();
            assert_eq!(
                (q.depth, q.last_observed_ms, q.oldest_age_ms),
                (1, source_time, None)
            );
            assert_eq!(
                (q.enqueued, q.dequeued, q.processed, q.committed),
                (1, 1, 1, 1)
            );
        }
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn native_admission_delivery_order_milestones_and_source_age_are_independent() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let epoch = crate::telemetry_consent_snapshot().1;
        for kind in [
            QueueKind::DatabaseAdmission,
            QueueKind::DatabaseReadAdmission,
        ] {
            let queue = QueueObserver::new(kind, None, "fixture source order");
            // Actual source head10; sample100 measures90. Delivery/read130
            // adds30, retaining120ms of age rather than replacing it with90.
            let age = (kind == QueueKind::DatabaseAdmission).then_some(90);
            queue.native_admission_snapshot(epoch, 1, 100, 1, age);
            let read = || {
                live::with_store(epoch, |store| {
                    store
                        .queues
                        .values()
                        .find(|q| q.snapshot.kind == kind)
                        .unwrap()
                        .snapshot(130)
                })
                .unwrap()
            };
            assert_eq!(read().oldest_age_ms, age.map(|_| 120));
            assert_eq!(read().last_observed_ms, 100);
            queue.native_admission_snapshot(epoch, 2, 110, 0, None);
            // Delayed milestone has its own counter but no authority over current.
            queue.enqueued(QueueBytes::Unsupported);
            queue.dequeued(QueueBytes::Unsupported);
            queue.native_admission_snapshot(epoch, 1, 100, 1, age);
            let after = read();
            assert_eq!(
                (after.depth, after.oldest_age_ms, after.last_observed_ms),
                (0, None, 110)
            );
            assert_eq!((after.offered, after.enqueued, after.dequeued), (1, 1, 1));
            queue.native_admission_snapshot(epoch, 3, 120, 2, age.map(|_| 110));
            assert_eq!((read().depth, read().last_observed_ms), (2, 120));
            assert_eq!(read().oldest_age_ms, age.map(|_| 120));
            queue.native_admission_snapshot(epoch, u64::MAX, 125, 0, None);
            assert_eq!(
                (
                    read().depth,
                    read().last_observed_ms,
                    read().metadata_overflow
                ),
                (2, 120, 1)
            );
        }
        let queue = QueueObserver::new(QueueKind::DatabaseAdmission, None, "fixture epoch");
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        queue.native_admission_snapshot(epoch, 9, 125, 99, Some(5));
        assert!(crate::live::snapshot().unwrap().queues.is_empty());
        queue.native_admission_snapshot(crate::telemetry_consent_snapshot().1, 10, 130, 0, None);
        assert_eq!(crate::live::snapshot().unwrap().queues[0].depth, 0);
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn send_rejection_keeps_the_preparation_epoch_across_consent_change() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let queue = QueueObserver::new(QueueKind::ClientController, Some(1), "fixture");
        let result: Result<(), ()> = observe_try_send(Some(&queue), QueueBytes::Known(0), || {
            crate::set_telemetry_enabled(false);
            crate::set_telemetry_enabled(true);
            Err(())
        });
        assert!(result.is_err());
        let snapshot = crate::live::snapshot().unwrap();
        assert!(snapshot.queues.is_empty());
        live::with_store(snapshot.consent_generation, |store| {
            assert!(store.queue_counts.iter().all(|n| *n == 0))
        });
        queue.current(0, QueueBytes::Unsupported);
        assert_eq!(crate::live::snapshot().unwrap().queues[0].rejected, 0);
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn actual_native_front_is_the_only_source_of_item_head_age() {
        use crate::work::{Boundary, ExecutorRole, WorkItem, WorkKind};
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let queue = QueueObserver::new(QueueKind::ClientSubscriptions, Some(4), "fixture");
        let mut first = WorkItem::new(
            (),
            Boundary::StdThread,
            WorkKind::Event,
            ExecutorRole::StdThread,
        );
        first.enqueued_monotonic_ms = Some(10);
        queue.enqueued(QueueBytes::Unsupported);
        queue.native_item_head(1, Some(&first));
        let epoch = crate::telemetry_consent_snapshot().1;
        live::with_store(epoch, |store| {
            assert_eq!(
                store
                    .queues
                    .values()
                    .next()
                    .unwrap()
                    .snapshot(100)
                    .oldest_age_ms,
                Some(90)
            )
        });
        queue.dequeued(QueueBytes::Unsupported);
        queue.current(1, QueueBytes::Unsupported); // new depth does not establish new front
        assert_eq!(
            crate::live::snapshot().unwrap().queues[0].oldest_age_ms,
            None
        );
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        queue.native_item_head(1, Some(&first)); // old backlog
        assert_eq!(
            crate::live::snapshot().unwrap().queues[0].oldest_age_ms,
            None
        );
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn epoch_change_between_guard_check_and_mutation_does_not_leak() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let queue = QueueObserver::new(QueueKind::ClientCommands, Some(4), "fixture");
        for processing in [true, false] {
            let mut guard = queue.processing();
            let wait = queue.send_wait();
            MUTATION_HOOK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(|| {
                    crate::set_telemetry_enabled(false);
                    crate::set_telemetry_enabled(true);
                }))
            });
            if processing {
                guard.processed();
            } else {
                drop(wait);
            }
            let snapshot = crate::live::snapshot().unwrap();
            assert!(snapshot.queues.is_empty());
            live::with_store(snapshot.consent_generation, |state| {
                assert!(state.queue_counts.iter().all(|n| *n == 0))
            });
            queue.enqueued(QueueBytes::Known(1));
            queue.dequeued(QueueBytes::Known(1));
            let mut fresh = queue.processing();
            fresh.processed();
            assert_eq!(crate::live::snapshot().unwrap().queues[0].processed, 1);
        }
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn depth_does_not_prove_head_order_after_backlog_reorder_or_empty() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        let queue = QueueObserver::new(QueueKind::EventDurable, Some(8), "fixture");
        crate::set_telemetry_enabled(true);
        queue.enqueued(QueueBytes::Unsupported); // new A, old backlog has no timestamp
        queue.dequeued(QueueBytes::Unsupported); // old item, NOT A
        queue.current(3, QueueBytes::Unsupported);
        queue.enqueued(QueueBytes::Unsupported); // new B
        assert_eq!(
            crate::live::snapshot().unwrap().queues[0].oldest_age_ms,
            None
        );
        queue.current(0, QueueBytes::Unsupported);
        queue.enqueued(QueueBytes::Unsupported); // producer A records first, sends second
        queue.enqueued(QueueBytes::Unsupported); // producer B sends first
        queue.dequeued(QueueBytes::Unsupported);
        assert_eq!(
            crate::live::snapshot().unwrap().queues[0].oldest_age_ms,
            None
        );
        queue.rejected_send(QueueBytes::Unsupported);
        queue.current(0, QueueBytes::Unsupported);
        assert_eq!(
            crate::live::snapshot().unwrap().queues[0].age_coverage,
            "observed_empty"
        );
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn coalesced_and_retained_lanes_do_not_invent_a_fifo_head_age() {
        for kind in [
            QueueKind::EventProgress,
            QueueKind::EventProgressBroadcast,
            QueueKind::EventSnapshot,
            QueueKind::ClientPublications,
        ] {
            let mut queue = QueueState::new(1, kind, Some(2), "fixture");
            queue.enqueue(QueueBytes::Unsupported, 5);
            queue.enqueue(QueueBytes::Unsupported, 10);
            queue.dequeue(QueueBytes::Unsupported); // may remove any key/subscriber output
            if queue.has_owner_current() {
                queue.snapshot.depth = 1;
            } // actual native owner state
            let snapshot = queue.snapshot(100);
            assert_eq!(snapshot.depth, 1);
            assert_eq!(snapshot.oldest_age_ms, None);
            assert_eq!(
                snapshot.age_coverage,
                "not_recorded_non_fifo_or_retained_journal"
            );
        }
    }
    #[test]
    fn reading_queue_does_not_refresh_producer_observation() {
        let mut state = QueueState::new(1, QueueKind::ClientCommands, None, "fixture");
        state.snapshot.last_observed_ms = 15;
        let first = state.snapshot(100);
        let later = state.snapshot(500);
        assert_eq!(first.snapshot_ms, 100);
        assert_eq!(later.snapshot_ms, 500);
        assert_eq!(later.last_observed_ms, 15);
    }
    #[test]
    fn lossless_milestones_and_commit_are_separate() {
        let mut q = QueueState::new(1, QueueKind::EventDurable, Some(4), "fixture bytes");
        q.enqueue(QueueBytes::Known(10), 100);
        q.enqueue(QueueBytes::Known(30), 200);
        assert_eq!(q.snapshot.depth, 0); // milestones have no native-current authority
        q.snapshot.depth = 2; // native owner publishes after its actual sends
        q.snapshot.bytes = 40;
        assert_eq!(q.snapshot(500).oldest_age_ms, None);
        q.dequeue(QueueBytes::Known(10));
        assert_eq!(q.snapshot.depth, 2); // late receipt cannot alter a newer owner snapshot
        q.snapshot.depth = 1;
        q.snapshot.bytes = 30;
        let s = q.snapshot(500);
        assert_eq!(
            (s.depth, s.bytes, s.in_processing, s.processed, s.committed),
            (1, 30, 1, 0, 0)
        );
        q.outcome(QueueOutcome::Processed, 1);
        q.outcome(QueueOutcome::Committed, 1);
        let s = q.snapshot(500);
        assert_eq!(
            (s.enqueued, s.dequeued, s.processed, s.committed),
            (2, 1, 1, 1)
        );
        assert_eq!(s.oldest_age_ms, None);
    }
    #[test]
    fn coalescing_replay_and_send_capacity_cancel_do_not_create_output_items() {
        let mut q = QueueState::new(1, QueueKind::OrderedCoalesced, Some(2), "fixture");
        q.enqueue(QueueBytes::Estimate(8), 100);
        q.outcome(QueueOutcome::Coalesced, 4);
        q.outcome(QueueOutcome::Replay, 2);
        q.outcome(QueueOutcome::Resnapshot, 1);
        let s = q.snapshot(100);
        assert_eq!(
            (s.offered, s.enqueued, s.depth, s.coalesced, s.replay),
            (5, 1, 1, 4, 2)
        );
        assert_eq!(s.byte_quality, "estimate_payload_bytes");
        q.dequeue(QueueBytes::Estimate(8));
        q.outcome(QueueOutcome::Cancelled, 1);
        let s = q.snapshot(100);
        assert_eq!(
            (s.depth, s.in_processing, s.processed, s.cancelled),
            (0, 0, 0, 1)
        );
    }
    #[test]
    fn age_overflow_is_explicit_and_native_admission_head_is_not_fifo_guess() {
        let mut q = QueueState::new(1, QueueKind::ClientCommands, None, "fixture");
        for at in 0..2050 {
            q.enqueue(QueueBytes::Unsupported, at);
        }
        assert_eq!(q.snapshot.metadata_overflow, 0);
        for _ in 0..2048 {
            q.dequeue(QueueBytes::Unsupported);
            q.outcome(QueueOutcome::Processed, 1);
        }
        let s = q.snapshot(3000);
        assert_eq!(s.depth, 2);
        assert_eq!(s.oldest_age_ms, None);
        let mut admission = QueueState::new(2, QueueKind::DatabaseAdmission, None, "fixture");
        admission.enqueue(QueueBytes::Unsupported, 1);
        assert_eq!(admission.snapshot(100).oldest_age_ms, None);
        admission.native_oldest_recorded = true;
        admission.native_oldest = Some((40, 0));
        assert_eq!(admission.snapshot(100).oldest_age_ms, Some(60));
    }
    #[test]
    fn old_epoch_guards_cannot_register_or_cancel_new_epoch_queues() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let queue = QueueObserver::new(QueueKind::ClientCommands, Some(4), "fixture");
        let waiting = queue.send_wait();
        queue.enqueued(QueueBytes::Known(1));
        queue.dequeued(QueueBytes::Known(1));
        let processing = queue.processing();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        drop(waiting);
        drop(processing);
        assert!(crate::live::snapshot().unwrap().queues.is_empty());
        queue.current(2, QueueBytes::Unsupported);
        let s = crate::live::snapshot().unwrap();
        assert_eq!(s.queues[0].depth, 2);
        assert_eq!(s.queues[0].cancelled, 0);
        crate::set_telemetry_enabled(old);
    }
}
