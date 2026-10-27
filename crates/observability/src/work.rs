//! Typed local causality. Capture occurs BEFORE executor/message ownership
//! changes. TLS exists for a single poll or synchronous callback, never await.
use crate::{DatabaseWorkloadContext, identity::run_identity};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Meter},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    future::Future,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkKind {
    WorkerLifecycle,
    Database,
    RpcRead,
    RpcMutation,
    RpcTurn,
    RpcControl,
    RpcUnknown,
    Message,
    Task,
    PostStartup,
    Event,
    ClientCommand,
    ClientRpc,
    ClientPublication,
    Maintenance,
    Blocking,
    Child,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    Rpc,
    Event,
    Timer,
    Retry,
    Reconcile,
    Lifecycle,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorRole {
    Gateway,
    Task,
    PostStartup,
    Event,
    ClientWs,
    ClientController,
    Blocking,
    StdThread,
    Child,
    DesktopForeground,
    Foreign,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    Rpc,
    MessageFreshTask,
    TaskFreshTask,
    PostStartup,
    OrderedIngress,
    EventLane,
    ClientCommand,
    ClientRpc,
    ClientPublication,
    StdThread,
    Blocking,
    Child,
    Timer,
    Database,
}
impl Boundary {
    fn label(self) -> &'static str {
        match self {
            Self::Rpc => "rpc",
            Self::MessageFreshTask => "message_fresh_task",
            Self::TaskFreshTask => "task_fresh_task",
            Self::PostStartup => "post_startup",
            Self::OrderedIngress => "ordered_ingress",
            Self::EventLane => "event_lane",
            Self::ClientCommand => "client_command",
            Self::ClientRpc => "client_rpc",
            Self::ClientPublication => "client_publication",
            Self::StdThread => "std_thread",
            Self::Blocking => "blocking",
            Self::Child => "child",
            Self::Timer => "timer",
            Self::Database => "database",
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextOrigin {
    Inherited,
    MissingLocalParent,
    ForeignBoundary,
    IntentionallyIndependent,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Completed,
    Failed,
    Cancelled,
    Panicked,
    Abandoned,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkContext {
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub token: u64,
    pub parent: Option<u64>,
    pub links: [Option<u64>; 4],
    pub kind: WorkKind,
    pub trigger: Trigger,
    pub executor: ExecutorRole,
    pub attempt: u16,
    pub consent_generation: u64,
    pub origin: ContextOrigin,
}
#[derive(Clone, Debug, Default)]
pub struct WorkCarrier {
    context: Option<WorkContext>,
    database: Option<DatabaseWorkloadContext>,
    queued: Option<crate::live::QueuedWork>,
}
thread_local! {static CURRENT:RefCell<WorkCarrier>=RefCell::new(WorkCarrier::default());}
static NEXT: AtomicU64 = AtomicU64::new(1);
static COVERAGE: OnceLock<Counter<u64>> = OnceLock::new();
static LOSS: OnceLock<Counter<u64>> = OnceLock::new();
pub(crate) fn init(meter: &Meter) {
    let _=COVERAGE.set(meter.u64_counter("pioneer.work.boundaries").with_description("Denominator per boundary AND phase is sum across mutually exclusive coverage outcomes; capture and receive are separate observations; task IDs never labels").with_unit("{boundary}").build());
    let _ = LOSS.set(
        meter
            .u64_counter("pioneer.work.context.loss")
            .with_unit("{context}")
            .build(),
    );
}
#[cfg(test)]
static TEST_COVERAGE: [AtomicU64; 225] = [const { AtomicU64::new(0) }; 225];
fn coverage_index(boundary: Boundary, phase: &str, outcome: &str) -> usize {
    let phase = match phase {
        "capture" => 0,
        "receive" => 1,
        "root" => 2,
        _ => panic!("open phase"),
    };
    let outcome = match outcome {
        "accepted_with_context" => 0,
        "missing" => 1,
        "invalid" => 2,
        "locally_rooted" => 3,
        "intentionally_independent" => 4,
        _ => panic!("open coverage"),
    };
    boundary as usize * 15 + phase * 5 + outcome
}
fn coverage(boundary: Boundary, phase: &'static str, outcome: &'static str) {
    if !crate::telemetry_enabled() {
        return;
    }
    #[cfg(test)]
    TEST_COVERAGE[coverage_index(boundary, phase, outcome)].fetch_add(1, Ordering::Relaxed);
    let epoch = crate::telemetry_consent_snapshot().1;
    crate::live::with_store(epoch, |state| {
        let count = &mut state.work_counts[coverage_index(boundary, phase, outcome)];
        if *count == u64::MAX {
            state.metric_overflow = true;
        }
        *count = count.saturating_add(1);
    });
}
const LOSS_REASONS: [&str; 3] = ["attempt_overflow", "link_overflow", "token_overflow"];
fn loss(reason: &'static str) {
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return;
    }
    let Some(index) = LOSS_REASONS.iter().position(|r| *r == reason) else {
        return;
    };
    crate::live::with_store(epoch, |state| {
        let count = &mut state.context_loss_counts[index];
        if *count == u64::MAX {
            state.metric_overflow = true;
        }
        *count = count.saturating_add(1);
    });
}
pub(crate) fn emit_pending() {
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return;
    }
    let Some((counts, losses)) = crate::live::with_store(epoch, |state| {
        let counts = state.work_counts;
        let losses = state.context_loss_counts;
        state.work_counts = [0; 210];
        state.context_loss_counts = [0; 3];
        (counts, losses)
    }) else {
        return;
    };
    const BOUNDARIES: [Boundary; 14] = [
        Boundary::Rpc,
        Boundary::MessageFreshTask,
        Boundary::TaskFreshTask,
        Boundary::PostStartup,
        Boundary::OrderedIngress,
        Boundary::EventLane,
        Boundary::ClientCommand,
        Boundary::ClientRpc,
        Boundary::ClientPublication,
        Boundary::StdThread,
        Boundary::Blocking,
        Boundary::Child,
        Boundary::Timer,
        Boundary::Database,
    ];
    if let Some(counter) = COVERAGE.get() {
        for boundary in BOUNDARIES {
            for phase in ["capture", "receive", "root"] {
                for outcome in [
                    "accepted_with_context",
                    "missing",
                    "invalid",
                    "locally_rooted",
                    "intentionally_independent",
                ] {
                    let n = counts[coverage_index(boundary, phase, outcome)];
                    if n > 0 {
                        counter.add(
                            n,
                            &[
                                KeyValue::new("boundary", boundary.label()),
                                KeyValue::new("phase", phase),
                                KeyValue::new("coverage", outcome),
                            ],
                        );
                    }
                }
            }
        }
    }
    if let Some(counter) = LOSS.get() {
        for (index, reason) in LOSS_REASONS.into_iter().enumerate() {
            if losses[index] > 0 {
                counter.add(losses[index], &[KeyValue::new("reason", reason)]);
            }
        }
    }
}
impl WorkContext {
    fn valid(&self) -> bool {
        self.token != 0 && crate::telemetry_sample_allowed(Some(self.consent_generation))
    }
    pub(crate) fn retry(mut self) -> Self {
        self.trigger = Trigger::Retry;
        self.attempt = self.attempt.saturating_add(1);
        self
    }
    pub fn linked(mut self, token: u64) -> Self {
        if self.token == token || self.parent == Some(token) || self.links.contains(&Some(token)) {
            return self;
        }
        if let Some(slot) = self.links.iter_mut().find(|s| s.is_none()) {
            *slot = Some(token);
        } else {
            loss("link_overflow");
        }
        self
    }
}
pub fn current() -> Option<WorkContext> {
    if !crate::telemetry_enabled() {
        return None;
    }
    CURRENT.with(|s| s.borrow().context.filter(WorkContext::valid))
}
pub fn capture() -> WorkCarrier {
    if !crate::telemetry_enabled() {
        return WorkCarrier::default();
    }
    CURRENT.with(|s| {
        let c = s.borrow();
        if c.context.is_some_and(|c| c.valid()) {
            c.clone()
        } else {
            WorkCarrier::default()
        }
    })
}
pub fn current_database() -> Option<DatabaseWorkloadContext> {
    if current().is_none() {
        return None;
    }
    CURRENT.with(|s| s.borrow().database.clone())
}
fn make(
    kind: WorkKind,
    trigger: Trigger,
    executor: ExecutorRole,
    parent: Option<WorkContext>,
    origin: ContextOrigin,
) -> Option<WorkContext> {
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return None;
    }
    // Parent capture and construction can straddle consent off/on. Do not
    // place an old token/DB carrier under a newly generated context.
    if parent.is_some_and(|parent| parent.consent_generation != epoch) {
        return None;
    }
    let Some(token) = NEXT
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        .ok()
    else {
        loss("token_overflow");
        return None;
    };
    let _ = run_identity();
    use opentelemetry::trace::TraceContextExt;
    let otel = opentelemetry::Context::current();
    let span = otel.span();
    let span_context = span.span_context();
    let trace_id = if span_context.is_valid() {
        Some(span_context.trace_id().to_bytes())
    } else {
        parent.and_then(|p| p.trace_id)
    };
    let span_id = if span_context.is_valid() {
        Some(span_context.span_id().to_bytes())
    } else {
        parent.and_then(|p| p.span_id)
    };
    Some(WorkContext {
        trace_id,
        span_id,
        token,
        parent: parent.map(|c| c.token),
        links: [None; 4],
        kind,
        trigger,
        executor,
        attempt: 1,
        consent_generation: epoch,
        origin,
    })
}
impl WorkCarrier {
    pub fn queued(mut self) -> Self {
        self.queued = crate::live::QueuedWork::new(self.context());
        self
    }
    pub fn context(&self) -> Option<WorkContext> {
        self.context.filter(WorkContext::valid)
    }
    pub fn child(
        boundary: Boundary,
        kind: WorkKind,
        trigger: Trigger,
        executor: ExecutorRole,
    ) -> Self {
        if !crate::telemetry_enabled() {
            return Self::default();
        }
        let mut carrier = capture();
        let parent = carrier.context();
        let origin = if parent.is_some() {
            ContextOrigin::Inherited
        } else {
            ContextOrigin::MissingLocalParent
        };
        coverage(
            boundary,
            "capture",
            if parent.is_some() {
                "accepted_with_context"
            } else {
                "locally_rooted"
            },
        );
        carrier.context = make(kind, trigger, executor, parent, origin);
        carrier.queued = None;
        carrier
    }
    pub fn independent(
        boundary: Boundary,
        kind: WorkKind,
        trigger: Trigger,
        executor: ExecutorRole,
    ) -> Self {
        if !crate::telemetry_enabled() {
            return Self::default();
        }
        coverage(boundary, "root", "intentionally_independent");
        Self {
            context: make(
                kind,
                trigger,
                executor,
                None,
                ContextOrigin::IntentionallyIndependent,
            ),
            database: None,
            queued: None,
        }
    }
    pub fn received(self, boundary: Boundary, kind: WorkKind, executor: ExecutorRole) -> Self {
        if !crate::telemetry_enabled() {
            return Self::default();
        }
        match self.context {
            Some(c) if c.valid() => {
                coverage(boundary, "receive", "accepted_with_context");
                self
            }
            Some(_) => {
                coverage(boundary, "receive", "invalid");
                Self {
                    context: make(
                        kind,
                        Trigger::Event,
                        executor,
                        None,
                        ContextOrigin::ForeignBoundary,
                    ),
                    database: None,
                    queued: None,
                }
            }
            None => {
                coverage(boundary, "receive", "missing");
                Self {
                    context: make(
                        kind,
                        Trigger::Event,
                        executor,
                        None,
                        ContextOrigin::MissingLocalParent,
                    ),
                    database: None,
                    queued: None,
                }
            }
        }
    }
    pub fn with_database(mut self, context: DatabaseWorkloadContext) -> Self {
        let parent = self.context();
        coverage(
            Boundary::Database,
            "capture",
            if parent.is_some() {
                "accepted_with_context"
            } else {
                "locally_rooted"
            },
        );
        let kind = match context.workload() {
            crate::DatabaseWorkload::FrozenStorageMaintenance
            | crate::DatabaseWorkload::HtmlPreviewMaintenance => WorkKind::Maintenance,
            _ => WorkKind::Database,
        };
        self.context = make(
            kind,
            parent.map_or(Trigger::Reconcile, |p| p.trigger),
            parent.map_or(ExecutorRole::Gateway, |p| p.executor),
            parent,
            if parent.is_some() {
                ContextOrigin::Inherited
            } else {
                ContextOrigin::MissingLocalParent
            },
        );
        self.queued = None;
        self.database = Some(context);
        self
    }
    /// A fanout/retry execution is a child of the carried local producer, not
    /// another receipt. It does not add a second boundary denominator.
    pub fn execution_child(&self, kind: WorkKind, executor: ExecutorRole) -> Self {
        let parent = self.context();
        let mut result = self.clone();
        result.queued = None;
        result.context = make(
            kind,
            parent.map_or(Trigger::Event, |p| p.trigger),
            executor,
            parent,
            if parent.is_some() {
                ContextOrigin::Inherited
            } else {
                ContextOrigin::MissingLocalParent
            },
        );
        if parent.is_none() {
            result.database = None;
        }
        result
    }
    /// The receiver has delivered this queued handoff. Keep causal metadata for
    /// fanout executions, but retire the original queue lease exactly once.
    pub fn complete_handoff(mut self) -> Self {
        let mut guard = crate::live::WorkGuard::start(self.context(), false);
        guard.finish(Outcome::Completed);
        self.queued = None;
        self
    }
    pub fn with_attempt(mut self, attempt: u16) -> Self {
        if let Some(context) = self.context.as_mut() {
            context.attempt = attempt.max(1);
            if attempt > 1 {
                context.trigger = Trigger::Retry;
            }
        }
        self
    }
    pub fn merge_link(&mut self, other: &Self) {
        if self.context().is_none() {
            if other.context().is_some() {
                *self = other.clone();
            }
            return;
        }
        if let Some(other) = other.context() {
            if let Some(context) = &mut self.context {
                *context = context.linked(other.token);
                for token in other.links.into_iter().flatten() {
                    *context = context.linked(token);
                }
            }
        }
    }

    pub fn enter(&self) -> WorkScope {
        let next = if self.context().is_some() {
            self.clone()
        } else {
            Self::default()
        };
        let previous = CURRENT.with(|slot| slot.replace(next));
        WorkScope {
            previous: Some(previous),
            not_send: std::marker::PhantomData,
        }
    }
    pub fn run<T>(&self, callback: impl FnOnce() -> T) -> T {
        let _scope = self.enter();
        let mut live = crate::live::WorkGuard::start(self.context(), false);
        let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback));
        match output {
            Ok(value) => {
                live.finish(Outcome::Completed);
                value
            }
            Err(error) => {
                live.finish(Outcome::Panicked);
                std::panic::resume_unwind(error)
            }
        }
    }
    pub fn run_result<T, E>(&self, callback: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
        let _scope = self.enter();
        let mut live = crate::live::WorkGuard::start(self.context(), false);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
            Ok(value) => {
                live.finish(if value.is_ok() {
                    Outcome::Completed
                } else {
                    Outcome::Failed
                });
                value
            }
            Err(error) => {
                live.finish(Outcome::Panicked);
                std::panic::resume_unwind(error)
            }
        }
    }
    pub fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        scope_classified(self, future, |_| Outcome::Completed)
    }
    /// A native turn owns an explicit terminal outcome (including cooperative
    /// cancellation and a panic already caught by its domain boundary).
    pub fn scope_outcome<F>(self, future: F) -> impl Future<Output = Outcome>
    where
        F: Future<Output = Outcome>,
    {
        scope_classified(self, future, |outcome| *outcome)
    }
    pub fn scope_result<F, T, E>(self, future: F) -> impl Future<Output = Result<T, E>>
    where
        F: Future<Output = Result<T, E>>,
    {
        scope_classified(self, future, |output| {
            if output.is_ok() {
                Outcome::Completed
            } else {
                Outcome::Failed
            }
        })
    }
}
/// Not Send: restoring TLS on another OS thread would corrupt the carrier.
pub struct WorkScope {
    previous: Option<WorkCarrier>,
    not_send: std::marker::PhantomData<*mut ()>,
}
impl Drop for WorkScope {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            CURRENT.with(|s| s.replace(previous));
        }
    }
}
pub fn inherit<F: Future>(
    boundary: Boundary,
    kind: WorkKind,
    executor: ExecutorRole,
    future: F,
) -> impl Future<Output = F::Output> {
    WorkCarrier::child(boundary, kind, Trigger::Lifecycle, executor).scope(future)
}
pub fn blocking<F, T>(work: F) -> impl FnOnce() -> T + Send + 'static
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let carrier = WorkCarrier::child(
        Boundary::Blocking,
        WorkKind::Blocking,
        Trigger::Lifecycle,
        ExecutorRole::Blocking,
    );
    move || carrier.run(work)
}
/// Same Tokio ownership/JoinHandle/cancellation behavior as spawn_blocking.
pub fn spawn_blocking<F, T>(work: F) -> tokio::task::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(blocking(work))
}

/// Local queue carrier. No auth/domain payload enters context or labels.
#[derive(Clone, Debug)]
pub struct WorkItem<T> {
    pub queue: Option<(crate::queues::QueueObserver, crate::queues::QueueBytes)>,
    pub value: T,
    pub carrier: WorkCarrier,
    pub enqueued_monotonic_ms: Option<u64>,
}
impl<T> WorkItem<T> {
    pub fn new(value: T, boundary: Boundary, kind: WorkKind, executor: ExecutorRole) -> Self {
        Self {
            queue: None,
            value,
            carrier: WorkCarrier::child(boundary, kind, Trigger::Event, executor),
            enqueued_monotonic_ms: crate::telemetry_enabled().then(crate::identity::monotonic_ms),
        }
    }
    pub fn from_carrier(
        value: T,
        carrier: WorkCarrier,
        enqueued_monotonic_ms: Option<u64>,
    ) -> Self {
        Self {
            queue: None,
            value,
            carrier,
            enqueued_monotonic_ms,
        }
    }
    pub fn observed(
        mut self,
        queue: crate::queues::QueueObserver,
        bytes: crate::queues::QueueBytes,
    ) -> Self {
        let queue = queue.in_current_epoch();
        self.carrier = self.carrier.queued();
        queue.enqueued(bytes);
        self.queue = Some((queue, bytes));
        self
    }
    pub fn received(
        self,
        boundary: Boundary,
        kind: WorkKind,
        executor: ExecutorRole,
    ) -> (T, WorkCarrier) {
        if let Some((queue, bytes)) = &self.queue {
            queue.dequeued(*bytes);
        }
        (self.value, self.carrier.received(boundary, kind, executor))
    }
}
impl<T> std::ops::Deref for WorkItem<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

pub fn clock_ms() -> u64 {
    crate::identity::monotonic_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::poll_fn, pin::pin};
    use std::{
        sync::{Arc, Mutex},
        task::{Context, Poll, Waker},
    };
    fn root(kind: WorkKind) -> WorkCarrier {
        WorkCarrier::independent(Boundary::Timer, kind, Trigger::Timer, ExecutorRole::Gateway)
    }
    #[test]
    fn captured_parent_cannot_be_rebuilt_under_another_consent_generation() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let parent = WorkCarrier::independent(
            Boundary::StdThread,
            WorkKind::Event,
            Trigger::Event,
            ExecutorRole::StdThread,
        )
        .context()
        .unwrap();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        assert!(
            make(
                WorkKind::Event,
                Trigger::Event,
                ExecutorRole::StdThread,
                Some(parent),
                ContextOrigin::Inherited
            )
            .is_none()
        );
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn nested_and_panicking_callbacks_restore_outer() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let a = root(WorkKind::Task);
        let b = root(WorkKind::Event);
        a.run(|| {
            assert_eq!(current(), a.context());
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                b.run(|| {
                    assert_eq!(current(), b.context());
                    panic!("fixture")
                })
            }));
            assert!(caught.is_err());
            assert_eq!(current(), a.context());
        });
        assert!(current().is_none());
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn execution_hop_retains_lineage_but_records_actual_executor() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let producer = root(WorkKind::Event);
        let delivered =
            producer.execution_child(WorkKind::ClientPublication, ExecutorRole::DesktopForeground);
        let parent = producer.context().unwrap();
        let child = delivered.context().unwrap();
        assert_eq!(parent.executor, ExecutorRole::Gateway);
        assert_eq!(child.executor, ExecutorRole::DesktopForeground);
        assert_eq!(child.parent, Some(parent.token));
        assert_eq!(child.trigger, parent.trigger);
        assert_ne!(child.token, parent.token);
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn interleaved_future_polls_never_leave_thread_context() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let a = root(WorkKind::Task);
        let b = root(WorkKind::Event);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let future = |kind, seen: Arc<Mutex<Vec<WorkKind>>>| {
            let mut count = 0;
            poll_fn(move |_| {
                seen.lock().unwrap().push(current().unwrap().kind);
                assert_eq!(current().unwrap().kind, kind);
                count += 1;
                if count == 2 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
        };
        let mut first = pin!(a.scope(future(WorkKind::Task, seen.clone())));
        let mut second = pin!(b.scope(future(WorkKind::Event, seen.clone())));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(current().is_none());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert!(current().is_none());
        assert!(first.as_mut().poll(&mut cx).is_ready());
        assert!(second.as_mut().poll(&mut cx).is_ready());
        assert_eq!(
            *seen.lock().unwrap(),
            [
                WorkKind::Task,
                WorkKind::Event,
                WorkKind::Task,
                WorkKind::Event
            ]
        );
        crate::set_telemetry_enabled(old);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_and_blocking_capture_before_hop() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let parent = root(WorkKind::Task);
        let token = parent.context().unwrap().token;
        let future = parent.run(|| {
            inherit(
                Boundary::TaskFreshTask,
                WorkKind::Task,
                ExecutorRole::Task,
                async move {
                    let child = current().unwrap();
                    assert_eq!(child.parent, Some(token));
                    tokio::task::yield_now().await;
                    assert_eq!(current().unwrap().token, child.token);
                    let parent = child.token;
                    let blocking = spawn_blocking(move || {
                        let context = current().unwrap();
                        assert_eq!(context.parent, Some(parent));
                        context.token
                    });
                    blocking.await.unwrap()
                },
            )
        });
        tokio::spawn(future).await.unwrap();
        assert!(current().is_none());
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn cancelled_future_drops_owner_under_its_carrier_and_restores_outer() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        struct Owner(Arc<Mutex<Option<u64>>>);
        impl Drop for Owner {
            fn drop(&mut self) {
                *self.0.lock().unwrap() = current().map(|c| c.token);
            }
        }
        let seen = Arc::new(Mutex::new(None));
        let carrier = root(WorkKind::Task);
        let token = carrier.context().unwrap().token;
        let owner = Owner(seen.clone());
        let mut future = Box::pin(carrier.scope(async move {
            let _owner = owner;
            std::future::pending::<()>().await;
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        let outer = root(WorkKind::Event);
        outer.run(|| {
            drop(future);
            assert_eq!(current(), outer.context());
        });
        assert_eq!(*seen.lock().unwrap(), Some(token));
        assert!(current().is_none());
        let snapshot = crate::live::snapshot().unwrap();
        assert!(!snapshot.active.iter().any(|w| w.context.token == token));
        assert!(snapshot.journal.iter().any(|r|matches!(&r.payload,crate::live::JournalPayload::Work(w) if w.context.token==token && w.state==crate::live::WorkState::Terminal(Outcome::Cancelled))));
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn receipt_and_capture_denominators_are_separate_and_conserved() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let root = root(WorkKind::Task);
        let read = || {
            TEST_COVERAGE
                .iter()
                .map(|n| n.load(Ordering::Relaxed))
                .collect::<Vec<_>>()
        };
        let before = read();
        let item = root
            .run(|| WorkItem::new(7, Boundary::EventLane, WorkKind::Event, ExecutorRole::Event));
        let (value, received) =
            item.received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Event);
        assert_eq!(value, 7);
        assert_eq!(
            received.context().unwrap().parent,
            root.context().map(|c| c.token)
        );
        let after = read();
        assert_eq!(
            after.iter().zip(&before).map(|(a, b)| a - b).sum::<u64>(),
            2
        );
        for phase in ["capture", "receive"] {
            let index = coverage_index(Boundary::EventLane, phase, "accepted_with_context");
            assert_eq!(after[index] - before[index], 1);
        }
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn epoch_invalidates_queued_context_and_links_are_bounded() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let carrier = root(WorkKind::Event);
        let mut context = carrier.context().unwrap();
        for token in 100..110 {
            context = context.linked(token);
        }
        assert_eq!(context.links, [Some(100), Some(101), Some(102), Some(103)]);
        crate::set_telemetry_enabled(false);
        assert!(carrier.context().is_none());
        crate::set_telemetry_enabled(true);
        let new_epoch = root(WorkKind::Event);
        let mut merged = carrier.clone();
        merged.merge_link(&new_epoch);
        assert_eq!(merged.context(), new_epoch.context());
        assert_eq!(merged.context().unwrap().links, [None; 4]);
        merged.merge_link(&carrier);
        assert_eq!(merged.context(), new_epoch.context());
        let received = carrier.received(Boundary::EventLane, WorkKind::Event, ExecutorRole::Event);
        assert_eq!(
            received.context().unwrap().origin,
            ContextOrigin::ForeignBoundary
        );
        crate::set_telemetry_enabled(old);
    }
}

struct ContextFuture<F: Future, C> {
    future: Option<F>,
    carrier: WorkCarrier,
    classify: C,
    live: Option<crate::live::WorkGuard>,
}
impl<F: Future, C: Fn(&F::Output) -> Outcome> Future for ContextFuture<F, C> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // `future` is structurally pinned: it is only polled in place or replaced
        // with None (which drops in place, like Pin::set). It is never taken/moved.
        let this = unsafe { self.get_unchecked_mut() };
        if !crate::telemetry_enabled() {
            let future = this
                .future
                .as_mut()
                .expect("context future polled after completion");
            return unsafe { std::pin::Pin::new_unchecked(future) }.poll(cx);
        }
        let _scope = this.carrier.enter();
        if this.live.is_none() {
            let worker = this.carrier.context().is_some_and(|c| {
                matches!(c.kind, WorkKind::WorkerLifecycle | WorkKind::PostStartup)
            });
            this.live = Some(crate::live::WorkGuard::start(
                this.carrier.context(),
                worker,
            ));
            this.carrier.queued.take();
        }
        let future = this
            .future
            .as_mut()
            .expect("context future polled after completion");
        let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unsafe { std::pin::Pin::new_unchecked(future) }.poll(cx)
        }));
        match output {
            Ok(std::task::Poll::Ready(output)) => {
                this.future = None;
                if let Some(live) = this.live.as_mut() {
                    live.finish((this.classify)(&output));
                }
                std::task::Poll::Ready(output)
            }
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(error) => {
                if let Some(live) = this.live.as_mut() {
                    live.finish(Outcome::Panicked);
                }
                std::panic::resume_unwind(error)
            }
        }
    }
}
impl<F: Future, C> Drop for ContextFuture<F, C> {
    fn drop(&mut self) {
        let _scope = self.carrier.enter();
        self.future = None; /* cancellation drops DB/child owners under the same carrier */
    }
}
fn scope_classified<F: Future>(
    mut carrier: WorkCarrier,
    future: F,
    classify: impl Fn(&F::Output) -> Outcome,
) -> impl Future<Output = F::Output> {
    if carrier.queued.is_none() {
        carrier.queued = crate::live::QueuedWork::new(carrier.context());
    }
    ContextFuture {
        future: Some(future),
        carrier,
        classify,
        live: None,
    }
}

pub fn inherit_result<F, T, E>(
    boundary: Boundary,
    kind: WorkKind,
    executor: ExecutorRole,
    future: F,
) -> impl Future<Output = Result<T, E>>
where
    F: Future<Output = Result<T, E>>,
{
    WorkCarrier::child(boundary, kind, Trigger::Lifecycle, executor).scope_result(future)
}

impl WorkKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::WorkerLifecycle => "worker_lifetime",
            Self::Database => "database",
            Self::RpcRead => "rpc_read",
            Self::RpcMutation => "rpc_mutation",
            Self::RpcTurn => "rpc_turn",
            Self::RpcControl => "rpc_control",
            Self::RpcUnknown => "rpc_unknown",
            Self::Message => "message",
            Self::Task => "task",
            Self::PostStartup => "post_startup",
            Self::Event => "event",
            Self::ClientCommand => "client_command",
            Self::ClientRpc => "client_rpc",
            Self::ClientPublication => "client_publication",
            Self::Maintenance => "maintenance",
            Self::Blocking => "blocking",
            Self::Child => "child",
        }
    }
}
