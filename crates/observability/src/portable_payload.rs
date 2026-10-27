//! Bounded portable-payload observations. Durations are elapsed wall time, not
//! CPU. Async read/publish/cleanup phases include existing admission/pool waits;
//! their durations overlap those signals and must not be added to them.
//!
//! Each logical full/range read or background quantum owns fixed atomic arrays.
//! Nested same-family reads contribute to their enclosing operation. No block
//! emits an OTEL record: aggregate metrics are emitted once at completion/drop,
//! independently of the rate-limited anomaly summary trace. Native blob reads
//! are separate read observations, never synthetic ordinary SQL callbacks.
use crate::{live, work};
use opentelemetry::{
    Context, KeyValue,
    metrics::{Counter, Histogram, Meter},
    trace::{
        Span as _, SpanBuilder, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags,
        TraceId, TraceState, Tracer as _,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant, SystemTime},
};

macro_rules! labels {
    ($name:ident { $($variant:ident => $label:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[repr(usize)]
        pub enum $name { $(#[serde(rename = $label)] $variant),+ }
        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $label),+ }
            }
        }
    }
}
labels!(Family {
    ToolText => "tool.text", ToolMetadata => "tool.metadata",
    CliMapping => "cli.mapping", TaskHistory => "task.history",
    TaskEvent => "task.event", Mixed => "mixed",
});
labels!(Mode { Full => "full", Range => "range", Background => "background" });
labels!(Stage {
    SourceRead => "source_read", BlockRead => "block_read",
    DictionaryLoad => "dictionary_load", DictionaryTrain => "dictionary_train",
    DictionaryPrepare => "dictionary_prepare", DictionaryLookup => "dictionary_lookup",
    Compress => "compress", Decompress => "decompress", VerifyAssemble => "verify_assemble",
    Publish => "publish", Cleanup => "cleanup", Idle => "idle", Backoff => "backoff",
});
labels!(Outcome {
    Success => "success", NoWork => "no_work", Raw => "raw", Published => "published",
    Stale => "stale", Error => "error", Cleanup => "cleanup", Cancelled => "cancelled",
});
labels!(ByteKind {
    ReadOriginal => "read_original", ReadEncoded => "read_encoded",
    EncodedOriginal => "encoded_original", Encoded => "encoded",
    PublishedOriginal => "published_original", PublishedEncoded => "published_encoded",
});
labels!(BlockKind { Read => "read", Encoded => "encoded", Published => "published", Cleaned => "cleaned" });
labels!(DictionaryEvent {
    Hit => "hit", Miss => "miss", Train => "train", Prepare => "prepare",
    TrainFallback => "train_fallback",
});
labels!(NativeOutcome { Ok => "ok", Missing => "missing", Error => "error", Cancelled => "cancelled" });

/// `Mixed` means unknown or multiple families (discovery/cleanup); never infer
/// a family from an owner ID, table text, dictionary identity or payload bytes.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct LiveOperation {
    pub family: Family,
    pub mode: Mode,
    pub stage: Option<Stage>,
    pub outcome: Option<Outcome>,
}

const FAMILIES: usize = Family::ALL.len();
const STAGES: usize = Stage::ALL.len();
const BYTE_KINDS: usize = ByteKind::ALL.len();
const BLOCK_KINDS: usize = BlockKind::ALL.len();
const DICTIONARY_EVENTS: usize = DictionaryEvent::ALL.len();
const LIVE_INTERVAL_MS: u64 = 250;
// Instrument label combinations per resource, without an outcome on phases,
// bytes, blocks or dictionary events. Resource identity is the existing SDK's.
pub const MAX_METRIC_SERIES: usize = Family::ALL.len()
    * Mode::ALL.len()
    * (2 * Outcome::ALL.len() + STAGES + BYTE_KINDS + BLOCK_KINDS + DICTIONARY_EVENTS)
    + Family::ALL.len() * (NativeOutcome::ALL.len() + 2 + 1);
const _: () = assert!(MAX_METRIC_SERIES < 2_000);

pub(crate) struct Metrics {
    operations: Counter<u64>,
    duration: Histogram<f64>,
    phase: Histogram<f64>,
    bytes: Counter<u64>,
    blocks: Counter<u64>,
    dictionary: Counter<u64>,
    native_reads: Counter<u64>,
    native_duration: Histogram<f64>,
    native_bytes: Counter<u64>,
}
impl Metrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            operations: meter.u64_counter("pioneer.gateway.portable.operations")
                .with_description("Logical portable full/range reads and background quanta, including cancelled drops; not SQL query count")
                .with_unit("{operation}").build(),
            duration: meter.f64_histogram("pioneer.gateway.portable.duration")
                .with_description("Logical operation elapsed wall duration; includes waits and nested phases, not CPU time")
                .with_unit("ms").build(),
            phase: meter.f64_histogram("pioneer.gateway.portable.phase.duration")
                .with_description("Sum of phase elapsed wall durations per logical operation; inclusive nested phases must not be added together")
                .with_unit("ms").build(),
            bytes: meter.u64_counter("pioneer.gateway.portable.bytes")
                .with_description("Actual read/encoded/published bytes by kind; PPAY envelope included in encoded sizes, SQLite/dictionary/storage overhead excluded; published only after commit")
                .with_unit("By").build(),
            blocks: meter.u64_counter("pioneer.gateway.portable.blocks")
                .with_description("Actual read/encoded/published/cleaned blocks by kind; staged blocks are not published")
                .with_unit("{block}").build(),
            dictionary: meter.u64_counter("pioneer.gateway.portable.dictionary.events")
                .with_description("Prepared dictionary cache lookup hit/miss, attempted train/prepare and native training failure handled by normal dictionary fallback; aggregated per logical operation")
                .with_unit("{event}").build(),
            native_reads: meter.u64_counter("pioneer.gateway.portable.native.reads")
                .with_description("Native guard/blob read operations; outside ordinary SQL callback/query-count and named SQLite worker-thread CPU coverage")
                .with_unit("{read}").build(),
            native_duration: meter.f64_histogram("pioneer.gateway.portable.native.duration")
                .with_description("Native handle wait or native guard/blob execution elapsed wall time; excludes pool/admission waits; not CPU")
                .with_unit("ms").build(),
            native_bytes: meter.u64_counter("pioneer.gateway.portable.native.bytes")
                .with_description("Bytes returned by successful native guard/blob reads; not ordinary query bytes")
                .with_unit("By").build(),
        }
    }
}

fn add(slot: &AtomicU64, value: u64) {
    let _ = slot.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}
fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}
fn ms(value: u64) -> f64 {
    Duration::from_nanos(value).as_secs_f64() * 1_000.0
}

struct Aggregate {
    family: AtomicUsize,
    mode: Mode,
    outcome: AtomicUsize,
    phases: [[AtomicU64; STAGES]; FAMILIES],
    phase_calls: [[AtomicU64; STAGES]; FAMILIES],
    bytes: [[AtomicU64; BYTE_KINDS]; FAMILIES],
    blocks: [[AtomicU64; BLOCK_KINDS]; FAMILIES],
    dictionary: [[AtomicU64; DICTIONARY_EVENTS]; FAMILIES],
    epoch: u64,
    work: Option<work::WorkContext>,
    last_live_ms: AtomicU64,
    live_stages_seen: AtomicU64,
}
impl Aggregate {
    fn new(family: Family, mode: Mode, epoch: u64, work: Option<work::WorkContext>) -> Self {
        Self {
            family: AtomicUsize::new(family as usize),
            mode,
            outcome: AtomicUsize::new(Outcome::Success as usize),
            phases: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            phase_calls: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            bytes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            blocks: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            dictionary: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            epoch,
            work,
            last_live_ms: AtomicU64::new(0),
            live_stages_seen: AtomicU64::new(0),
        }
    }
    fn family(&self) -> Family {
        Family::ALL[self.family.load(Ordering::Relaxed)]
    }
    fn outcome(&self) -> Outcome {
        Outcome::ALL[self.outcome.load(Ordering::Relaxed)]
    }
    fn record_outcome(&self, next: Outcome) {
        // Failure/revalidation rejection cannot disappear behind an outer Ok.
        let priority = |o| match o {
            Outcome::Success => 0,
            Outcome::NoWork => 1,
            Outcome::Cleanup => 2,
            Outcome::Raw => 3,
            Outcome::Published => 4,
            Outcome::Stale => 5,
            Outcome::Error => 6,
            Outcome::Cancelled => 7,
        };
        let _ = self
            .outcome
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                (priority(next) > priority(Outcome::ALL[old])).then_some(next as usize)
            });
    }
    fn live(&self, stage: Option<Stage>, terminal: Option<Outcome>, force: bool) {
        if !crate::telemetry_sample_allowed(Some(self.epoch)) {
            return;
        }
        let now = crate::identity::monotonic_ms();
        // Short CPU phases are aggregate-only until the live interval elapses.
        // In particular a one-block background quantum must not add one live
        // journal event for each codec/cache phase. First I/O stages remain
        // immediately visible; repeated transitions are coalesced to 4/second.
        let first_stage = stage.is_some_and(|stage| {
            let bit = 1_u64 << stage as usize;
            let first = self.live_stages_seen.fetch_or(bit, Ordering::Relaxed) & bit == 0;
            first
                && !matches!(
                    stage,
                    Stage::DictionaryTrain
                        | Stage::DictionaryPrepare
                        | Stage::DictionaryLookup
                        | Stage::Compress
                        | Stage::Decompress
                        | Stage::VerifyAssemble
                )
        });
        if !force && !first_stage {
            if self
                .last_live_ms
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                    (old == 0 || now.saturating_sub(old) >= LIVE_INTERVAL_MS).then_some(now.max(1))
                })
                .is_err()
            {
                return;
            }
        } else {
            self.last_live_ms.store(now, Ordering::Relaxed);
        }
        live::portable_progress(
            self.work,
            LiveOperation {
                family: self.family(),
                mode: self.mode,
                stage,
                outcome: terminal,
            },
        );
    }
    fn phase(&self, family: Family, stage: Stage, elapsed: Duration) {
        add(
            &self.phases[family as usize][stage as usize],
            nanos(elapsed),
        );
        add(&self.phase_calls[family as usize][stage as usize], 1);
    }
}
thread_local! { static CURRENT: RefCell<Option<Arc<Aggregate>>> = const { RefCell::new(None) }; }
fn current() -> Option<Arc<Aggregate>> {
    CURRENT.with(|slot| {
        slot.borrow()
            .as_ref()
            .filter(|s| crate::telemetry_sample_allowed(Some(s.epoch)))
            .cloned()
    })
}
struct CurrentScope(Option<Arc<Aggregate>>);
impl CurrentScope {
    fn enter(state: Arc<Aggregate>) -> Self {
        Self(CURRENT.with(|slot| slot.replace(Some(state))))
    }
}
impl Drop for CurrentScope {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.replace(self.0.take()));
    }
}

pub fn set_family(family: Family) {
    if let Some(state) = current() {
        if state.family.swap(family as usize, Ordering::Relaxed) != family as usize {
            state.live(None, None, true);
        }
    }
}
pub fn outcome(outcome: Outcome) {
    if let Some(s) = current() {
        s.record_outcome(outcome);
    }
}
pub fn bytes(kind: ByteKind, count: u64) {
    if let Some(s) = current() {
        add(&s.bytes[s.family() as usize][kind as usize], count);
    }
}
pub fn blocks(kind: BlockKind, count: u64) {
    if let Some(s) = current() {
        add(&s.blocks[s.family() as usize][kind as usize], count);
    }
}
pub fn dictionary_event(event: DictionaryEvent) {
    if let Some(s) = current() {
        add(&s.dictionary[s.family() as usize][event as usize], 1);
    }
}
pub fn phase(stage: Stage, elapsed: Duration) {
    if let Some(s) = current() {
        s.phase(s.family(), stage, elapsed);
    }
}
pub fn live_stage(stage: Stage) {
    if let Some(s) = current() {
        s.live(Some(stage), None, false);
    }
}

#[must_use = "keep the phase timer alive until its measured work ends"]
pub struct StageGuard {
    state: Option<Arc<Aggregate>>,
    family: Family,
    stage: Stage,
    started: Instant,
}
pub fn stage(stage: Stage) -> StageGuard {
    let state = current();
    if let Some(s) = &state {
        s.live(Some(stage), None, false);
    }
    StageGuard {
        family: state.as_ref().map_or(Family::Mixed, |s| s.family()),
        state,
        stage,
        started: Instant::now(),
    }
}
impl Drop for StageGuard {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.phase(self.family, self.stage, self.started.elapsed());
        }
    }
}

struct Operation {
    state: Arc<Aggregate>,
    carrier: work::WorkCarrier,
    live: live::WorkGuard,
    parent: Context,
    started: Instant,
    started_at: SystemTime,
}
/// A work boundary carries IDs but has no OTEL attachment and does not retain
/// sampling flags or trace state. Reuse a valid active context when available;
/// otherwise restore only the known parent IDs. Unknown sampling defaults to
/// unsampled and trace state to empty, so a parent-based sampler may omit that
/// summary. Aggregate metrics remain unsampled and independent of that choice.
fn trace_parent(parent: Option<work::WorkContext>) -> Context {
    let current = Context::current();
    if current.span().span_context().is_valid() {
        return current;
    }
    let Some((trace_id, span_id)) = parent.and_then(|p| p.trace_id.zip(p.span_id)) else {
        return current;
    };
    let span = SpanContext::new(
        TraceId::from_bytes(trace_id),
        SpanId::from_bytes(span_id),
        TraceFlags::default(),
        true,
        TraceState::default(),
    );
    if span.is_valid() {
        current.with_remote_span_context(span)
    } else {
        current
    }
}
impl Operation {
    fn new(family: Family, mode: Mode, epoch: u64) -> Self {
        let parent_carrier = work::capture();
        let parent = trace_parent(parent_carrier.context());
        let executor = parent_carrier
            .context()
            .map_or(work::ExecutorRole::Gateway, |c| c.executor);
        let carrier = parent_carrier.execution_child(work::WorkKind::Database, executor);
        let state = Arc::new(Aggregate::new(family, mode, epoch, carrier.context()));
        let live = live::WorkGuard::start(carrier.context(), false);
        state.live(None, None, true);
        Self {
            state,
            carrier,
            live,
            parent,
            started: Instant::now(),
            started_at: SystemTime::now(),
        }
    }
    fn finish(mut self, cancelled: bool) {
        if cancelled {
            self.state.record_outcome(Outcome::Cancelled);
        }
        let outcome = self.state.outcome();
        self.state.live(None, Some(outcome), true);
        self.live.finish(match outcome {
            Outcome::Error | Outcome::Stale => work::Outcome::Failed,
            Outcome::Cancelled => work::Outcome::Cancelled,
            _ => work::Outcome::Completed,
        });
        emit(&self, self.started.elapsed());
    }
}

struct Scoped<F> {
    future: Option<Pin<Box<F>>>,
    family: Family,
    mode: Mode,
    initialized: bool,
    operation: Option<Operation>,
    shared: Option<Arc<Aggregate>>,
}
impl<F, T, E> Future for Scoped<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;
    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !this.initialized {
            this.initialized = true;
            let (enabled, epoch) = crate::telemetry_consent_snapshot();
            if enabled {
                if let Some(parent) = current().filter(|s| s.family() == this.family) {
                    this.shared = Some(parent);
                } else {
                    this.operation = Some(Operation::new(this.family, this.mode, epoch));
                }
            }
        }
        let result = {
            let _work = this.operation.as_ref().map(|op| op.carrier.enter());
            let _portable = this
                .operation
                .as_ref()
                .map(|op| &op.state)
                .or(this.shared.as_ref())
                .map(|state| CurrentScope::enter(state.clone()));
            // TLS and OTEL attachment live for one poll only, never across await.
            let _otel = this.operation.as_ref().map(|op| op.parent.clone().attach());
            this.future
                .as_mut()
                .expect("portable future polled after completion")
                .as_mut()
                .poll(cx)
        };
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                // Drop all future-owned timers/resources before summary export.
                {
                    let _work = this.operation.as_ref().map(|op| op.carrier.enter());
                    let _portable = this
                        .operation
                        .as_ref()
                        .map(|op| CurrentScope::enter(op.state.clone()));
                    this.future = None;
                }
                if let Some(op) = this.operation.take() {
                    if result.is_err() && op.state.outcome() != Outcome::Stale {
                        op.state.record_outcome(Outcome::Error);
                    }
                    op.finish(false);
                } else if result.is_err() {
                    if let Some(state) = &this.shared {
                        if state.outcome() != Outcome::Stale {
                            state.record_outcome(Outcome::Error);
                        }
                    }
                }
                Poll::Ready(result)
            }
        }
    }
}
impl<F> Drop for Scoped<F> {
    fn drop(&mut self) {
        let cancelled = self.future.is_some();
        {
            let _work = self.operation.as_ref().map(|op| op.carrier.enter());
            let _portable = self
                .operation
                .as_ref()
                .map(|op| CurrentScope::enter(op.state.clone()));
            self.future = None;
        }
        if let Some(op) = self.operation.take() {
            op.finish(true);
        } else if cancelled {
            if let Some(state) = &self.shared {
                state.record_outcome(Outcome::Cancelled);
            }
        }
    }
}
/// Retains the caller's database recorder, scheduling class, trace and parent
/// owner. Same-family nested hydration/range helpers reuse the current aggregate.
pub fn scope_result<F, T, E>(
    family: Family,
    mode: Mode,
    future: F,
) -> impl Future<Output = Result<T, E>>
where
    F: Future<Output = Result<T, E>>,
{
    Scoped {
        future: Some(Box::pin(future)),
        family,
        mode,
        initialized: false,
        operation: None,
        shared: None,
    }
}

/// Separate bounded sleeping work after the completed quantum has been
/// retired. This records only existing local work/wait diagnostics, with no
/// portable operation metrics or summary span. The caller owns the duration,
/// cancellation future and policy; the inherited database workload is retained.
pub async fn pause<F: Future>(duration: Duration, backoff: bool, future: F) -> F::Output {
    // Keep 1-ms progress pacing out of the live journal. Long idle and retry
    // sleeps retain explicit ownership; neither path creates operation metrics.
    if duration < Duration::from_millis(LIVE_INTERVAL_MS) || !crate::telemetry_enabled() {
        return future.await;
    }
    let parent = work::capture();
    let executor = parent
        .context()
        .map_or(work::ExecutorRole::Gateway, |p| p.executor);
    let child = parent.execution_child(work::WorkKind::Maintenance, executor);
    let context = child.context();
    child
        .scope(async {
            live::portable_progress(
                context,
                LiveOperation {
                    family: Family::Mixed,
                    mode: Mode::Background,
                    stage: Some(if backoff { Stage::Backoff } else { Stage::Idle }),
                    outcome: None,
                },
            );
            if backoff {
                live::retry(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
            }
            live::sleeping(duration, future).await
        })
        .await
}

fn emit(op: &Operation, elapsed: Duration) {
    let s = &op.state;
    if !crate::telemetry_sample_allowed(Some(s.epoch)) {
        return;
    }
    let Some(state) = crate::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref().map(|m| &m.portable_payload) else {
        return;
    };
    let base = [
        KeyValue::new("family", s.family().as_str()),
        KeyValue::new("mode", s.mode.as_str()),
    ];
    let terminal = [
        base[0].clone(),
        base[1].clone(),
        KeyValue::new("outcome", s.outcome().as_str()),
    ];
    metrics.operations.add(1, &terminal);
    metrics
        .duration
        .record(elapsed.as_secs_f64() * 1_000.0, &terminal);
    for family in Family::ALL {
        let family_index = *family as usize;
        let dimensions = [KeyValue::new("family", family.as_str()), base[1].clone()];
        for stage in Stage::ALL {
            if s.phase_calls[family_index][*stage as usize].load(Ordering::Relaxed) > 0 {
                metrics.phase.record(
                    ms(s.phases[family_index][*stage as usize].load(Ordering::Relaxed)),
                    &[
                        dimensions[0].clone(),
                        dimensions[1].clone(),
                        KeyValue::new("stage", stage.as_str()),
                    ],
                );
            }
        }
        for kind in ByteKind::ALL {
            let count = s.bytes[family_index][*kind as usize].load(Ordering::Relaxed);
            if count > 0 {
                metrics.bytes.add(
                    count,
                    &[
                        dimensions[0].clone(),
                        dimensions[1].clone(),
                        KeyValue::new("kind", kind.as_str()),
                    ],
                );
            }
        }
        for kind in BlockKind::ALL {
            let count = s.blocks[family_index][*kind as usize].load(Ordering::Relaxed);
            if count > 0 {
                metrics.blocks.add(
                    count,
                    &[
                        dimensions[0].clone(),
                        dimensions[1].clone(),
                        KeyValue::new("kind", kind.as_str()),
                    ],
                );
            }
        }
        for event in DictionaryEvent::ALL {
            let count = s.dictionary[family_index][*event as usize].load(Ordering::Relaxed);
            if count > 0 {
                metrics.dictionary.add(
                    count,
                    &[
                        dimensions[0].clone(),
                        dimensions[1].clone(),
                        KeyValue::new("event", event.as_str()),
                    ],
                );
            }
        }
    }
    // Reuse the existing 30-second workload anomaly limiter. All portable
    // summaries share its one slot; main metrics above are never sampled.
    if elapsed < Duration::from_millis(500)
        && !matches!(s.outcome(), Outcome::Error | Outcome::Stale)
    {
        return;
    }
    if !crate::database_workload::reserve_anomaly_trace(
        crate::DatabaseWorkload::PortablePayloadMaintenance,
    ) {
        return;
    }
    let mut attributes = terminal.to_vec();
    attributes.push(KeyValue::new(
        "coverage",
        "elapsed_only;ordinary_queries_partial;native_sql_cpu_unmeasured",
    ));
    // Trace totals include all families visited by this quantum. Metrics above
    // retain each sample's family, so discovery/cleanup remains mixed even when
    // the quantum subsequently selects a particular field.
    let total = |rows: &[[AtomicU64; STAGES]; FAMILIES], stage: usize| {
        rows.iter().fold(0_u64, |sum, row| {
            sum.saturating_add(row[stage].load(Ordering::Relaxed))
        })
    };
    for stage in Stage::ALL {
        if total(&s.phase_calls, *stage as usize) > 0 {
            attributes.push(KeyValue::new(
                format!("phase.all_families.{}.elapsed_ms", stage.as_str()),
                ms(total(&s.phases, *stage as usize)),
            ));
        }
    }
    for kind in ByteKind::ALL {
        let count = s.bytes.iter().fold(0_u64, |sum, row| {
            sum.saturating_add(row[*kind as usize].load(Ordering::Relaxed))
        });
        attributes.push(KeyValue::new(
            format!("bytes.all_families.{}", kind.as_str()),
            i64::try_from(count).unwrap_or(i64::MAX),
        ));
    }
    for kind in BlockKind::ALL {
        let count = s.blocks.iter().fold(0_u64, |sum, row| {
            sum.saturating_add(row[*kind as usize].load(Ordering::Relaxed))
        });
        attributes.push(KeyValue::new(
            format!("blocks.all_families.{}", kind.as_str()),
            i64::try_from(count).unwrap_or(i64::MAX),
        ));
    }
    let builder = SpanBuilder::from_name("gateway.portable_payload.summary")
        .with_kind(SpanKind::Internal)
        .with_start_time(op.started_at)
        .with_attributes(attributes);
    let mut span = state.tracer.build_with_context(builder, &op.parent);
    span.set_status(match s.outcome() {
        Outcome::Error => Status::Error {
            description: "portable operation failed".into(),
        },
        Outcome::Cancelled | Outcome::Stale => Status::Unset,
        _ => Status::Ok,
    });
    span.end_with_timestamp(
        op.started_at
            .checked_add(elapsed)
            .unwrap_or_else(SystemTime::now),
    );
}

/// Separate native read accounting. Construct before resource acquisition,
/// start the timer immediately before lock_handle; call handle_pending only
/// after that future returns Poll::Pending, then executing after lock succeeds,
/// and finish only after handle/reader/permit release. No query callbacks, SQL
/// fingerprints or CPU samples are fabricated.
pub struct NativeRead {
    family: Family,
    epoch: Option<u64>,
    waiting: Option<Instant>,
    executing: Option<Instant>,
    wait_elapsed: Option<Duration>,
    execute_elapsed: Option<Duration>,
    live_wait: Option<live::WaitGuard>,
    finished: bool,
}
impl NativeRead {
    pub fn new(family: Family) -> Self {
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        Self {
            family,
            epoch: enabled.then_some(epoch),
            waiting: None,
            executing: None,
            wait_elapsed: None,
            execute_elapsed: None,
            live_wait: None,
            finished: false,
        }
    }
    pub fn waiting_for_handle(&mut self) {
        if self.epoch.is_none() {
            return;
        }
        self.waiting = Some(Instant::now());
    }
    /// Lazy local wait registration avoids journal churn for immediately-ready
    /// locks. Repeated Pending polls retain the same single wait guard.
    pub fn handle_pending(&mut self) {
        if self.waiting.is_some()
            && self.live_wait.is_none()
            && crate::telemetry_sample_allowed(self.epoch)
        {
            self.live_wait = Some(live::WaitGuard::new(live::WaitKind::Lock));
        }
    }
    pub fn executing(&mut self) {
        if let Some(start) = self.waiting.take() {
            self.wait_elapsed = Some(start.elapsed());
        }
        self.live_wait = None;
        if self.epoch.is_some() {
            self.executing = Some(Instant::now());
        }
    }
    pub fn finish(mut self, outcome: NativeOutcome, read_bytes: u64) {
        self.emit(outcome, read_bytes);
    }
    fn emit(&mut self, outcome: NativeOutcome, read_bytes: u64) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(start) = self.waiting.take() {
            self.wait_elapsed = Some(start.elapsed());
        }
        if let Some(start) = self.executing.take() {
            self.execute_elapsed = Some(start.elapsed());
        }
        self.live_wait = None;
        if !crate::telemetry_sample_allowed(self.epoch) {
            return;
        }
        let Some(state) = crate::telemetry::state() else {
            return;
        };
        let Some(metrics) = state.gateway_metrics.as_ref().map(|m| &m.portable_payload) else {
            return;
        };
        let family = KeyValue::new("family", self.family.as_str());
        metrics.native_reads.add(
            1,
            &[family.clone(), KeyValue::new("outcome", outcome.as_str())],
        );
        for (phase, elapsed) in [
            ("handle_wait", self.wait_elapsed),
            ("execute", self.execute_elapsed),
        ] {
            if let Some(elapsed) = elapsed {
                metrics.native_duration.record(
                    elapsed.as_secs_f64() * 1_000.0,
                    &[family.clone(), KeyValue::new("stage", phase)],
                );
            }
        }
        if outcome == NativeOutcome::Ok && read_bytes > 0 {
            metrics.native_bytes.add(read_bytes, &[family]);
        }
    }
}
impl Drop for NativeRead {
    fn drop(&mut self) {
        self.emit(NativeOutcome::Cancelled, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseOperationSite, DatabaseWorkload, DatabaseWorkloadTrace};
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use std::task::Waker;

    struct Consent {
        previous: bool,
        _lock: std::sync::MutexGuard<'static, ()>,
    }
    impl Consent {
        fn enabled() -> Self {
            let lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
            let previous = crate::telemetry_enabled();
            crate::set_telemetry_enabled(false);
            crate::set_telemetry_enabled(true);
            Self {
                previous,
                _lock: lock,
            }
        }
    }
    impl Drop for Consent {
        fn drop(&mut self) {
            crate::set_telemetry_enabled(self.previous);
        }
    }
    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut TaskContext::from_waker(Waker::noop()))
    }
    fn terminals() -> Vec<live::ActiveWork> {
        live::snapshot()
            .unwrap()
            .journal
            .into_iter()
            .filter_map(|record| match record.payload {
                live::JournalPayload::Work(work)
                    if work.portable_payload.is_some()
                        && matches!(work.state, live::WorkState::Terminal(_)) =>
                {
                    Some(work)
                }
                _ => None,
            })
            .collect()
    }

    // One primary owner for context and nested operation accounting. No DB,
    // storage fixture or fake SQL callback is needed for this contract.
    #[test]
    fn nested_range_keeps_caller_database_trace_and_failure_under_outer_ok() {
        // Both an attached context and a WorkCarrier-only boundary must retain
        // known trace identity. The latter cannot recover missing sample flags.
        for attached in [true, false] {
            let _consent = Consent::enabled();
            let _empty = Context::new().attach();
            let trace = Context::new().with_remote_span_context(SpanContext::new(
                TraceId::from_bytes([7; 16]),
                SpanId::from_bytes([9; 8]),
                TraceFlags::SAMPLED,
                true,
                TraceState::default(),
            ));
            let mut trace_guard = Some(trace.attach());
            let database = DatabaseWorkloadTrace::start_at(
                DatabaseWorkload::TimelinePage,
                DatabaseOperationSite::TimelinePage,
            );
            let caller = work::WorkCarrier::independent(
                work::Boundary::Rpc,
                work::WorkKind::RpcRead,
                work::Trigger::Rpc,
                work::ExecutorRole::Gateway,
            )
            .with_database(database.context());
            let parent = caller.context().unwrap();
            if !attached {
                trace_guard.take();
            }
            let mut future = Box::pin(scope_result(Family::ToolText, Mode::Full, async {
                let child = work::current().unwrap();
                assert_eq!(child.parent, Some(parent.token));
                assert_eq!(child.trace_id, Some([7; 16]));
                assert_eq!(child.executor, parent.executor);
                assert_eq!(child.trigger, parent.trigger);
                let otel = Context::current();
                let span = otel.span();
                let context = span.span_context();
                assert!(context.is_valid());
                assert_eq!(context.trace_id(), TraceId::from_bytes([7; 16]));
                assert_eq!(context.span_id(), SpanId::from_bytes([9; 8]));
                assert_eq!(
                    context.trace_flags(),
                    if attached {
                        TraceFlags::SAMPLED
                    } else {
                        TraceFlags::default()
                    }
                );
                let recorder = work::current_database().unwrap();
                assert_eq!(recorder.workload(), DatabaseWorkload::TimelinePage);
                assert_eq!(recorder.site(), DatabaseOperationSite::TimelinePage);
                let nested = scope_result(Family::ToolText, Mode::Range, async {
                    assert_eq!(work::current().unwrap().token, child.token);
                    phase(Stage::Decompress, Duration::from_millis(3));
                    Err::<(), ()>(())
                })
                .await;
                assert!(nested.is_err());
                Ok::<_, ()>(())
            }));
            assert_eq!(caller.run(|| poll(future.as_mut())), Poll::Ready(Ok(())));
            assert!(work::current().is_none());
            assert!(work::current_database().is_none());
            let terminal = terminals();
            assert_eq!(terminal.len(), 1); // full + internal range counts once
            assert!(matches!(
                terminal[0].state,
                live::WorkState::Terminal(work::Outcome::Failed)
            ));
            assert_eq!(
                terminal[0].portable_payload.unwrap().outcome,
                Some(Outcome::Error)
            );
            assert_eq!(terminal[0].portable_payload.unwrap().mode, Mode::Full);
        }
    }

    #[test]
    fn family_selection_preserves_mixed_cleanup_and_known_rejection() {
        let _consent = Consent::enabled();
        let mut future = Box::pin(scope_result(Family::Mixed, Mode::Background, async {
            phase(Stage::Cleanup, Duration::from_millis(5));
            set_family(Family::TaskEvent);
            phase(Stage::Compress, Duration::from_millis(2));
            phase(Stage::Compress, Duration::from_millis(3));
            bytes(ByteKind::Encoded, u64::MAX);
            bytes(ByteKind::Encoded, 1);
            dictionary_event(DictionaryEvent::TrainFallback);
            let state = current().unwrap();
            assert_eq!(state.outcome(), Outcome::Success); // normal fallback is not an error
            assert_eq!(
                state.dictionary[Family::TaskEvent as usize]
                    [DictionaryEvent::TrainFallback as usize]
                    .load(Ordering::Relaxed),
                1
            );
            assert_eq!(
                state.phases[Family::Mixed as usize][Stage::Cleanup as usize]
                    .load(Ordering::Relaxed),
                5_000_000
            );
            assert_eq!(
                state.phases[Family::TaskEvent as usize][Stage::Cleanup as usize]
                    .load(Ordering::Relaxed),
                0
            );
            assert_eq!(
                state.phases[Family::TaskEvent as usize][Stage::Compress as usize]
                    .load(Ordering::Relaxed),
                5_000_000
            );
            assert_eq!(
                state.bytes[Family::TaskEvent as usize][ByteKind::Encoded as usize]
                    .load(Ordering::Relaxed),
                u64::MAX
            );
            outcome(Outcome::Stale);
            Err::<(), ()>(())
        }));
        assert_eq!(poll(future.as_mut()), Poll::Ready(Err(())));
        let terminal = terminals();
        assert_eq!(terminal.len(), 1);
        assert_eq!(
            terminal[0].portable_payload.unwrap().family,
            Family::TaskEvent
        );
        assert_eq!(
            terminal[0].portable_payload.unwrap().outcome,
            Some(Outcome::Stale)
        );
    }

    #[test]
    fn cancellation_drops_phase_before_terminal_and_retires_live_work() {
        let _consent = Consent::enabled();
        let captured = std::cell::RefCell::new(None);
        let mut future = Box::pin(scope_result(Family::ToolMetadata, Mode::Range, async {
            let state = current().unwrap();
            captured.replace(Some(state));
            let _phase = stage(Stage::BlockRead);
            bytes(ByteKind::ReadEncoded, 17);
            blocks(BlockKind::Read, 1);
            std::future::pending::<Result<(), ()>>().await
        }));
        assert!(poll(future.as_mut()).is_pending());
        assert!(work::current().is_none()); // TLS never leaks between polls
        drop(future);
        let state = captured.borrow_mut().take().unwrap();
        assert_eq!(state.outcome(), Outcome::Cancelled);
        assert_eq!(
            state.phase_calls[Family::ToolMetadata as usize][Stage::BlockRead as usize]
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            state.bytes[Family::ToolMetadata as usize][ByteKind::ReadEncoded as usize]
                .load(Ordering::Relaxed),
            17
        );
        assert!(live::snapshot().unwrap().active.is_empty());
        let terminal = terminals();
        assert_eq!(terminal.len(), 1);
        assert!(matches!(
            terminal[0].state,
            live::WorkState::Terminal(work::Outcome::Cancelled)
        ));
        assert_eq!(
            terminal[0].portable_payload.unwrap().outcome,
            Some(Outcome::Cancelled)
        );
    }

    #[test]
    fn local_operation_detail_preserves_diagnostic_wire_contract() {
        let detail = LiveOperation {
            family: Family::Mixed,
            mode: Mode::Background,
            stage: Some(Stage::Cleanup),
            outcome: Some(Outcome::NoWork),
        };
        assert_eq!(
            serde_json::to_value(detail).unwrap(),
            serde_json::json!({
                "family": "mixed", "mode": "background", "stage": "cleanup", "outcome": "no_work"
            })
        );
    }

    #[test]
    fn native_lock_journals_only_actual_pending_and_releases_on_drop() {
        for pending in [false, true] {
            let _consent = Consent::enabled();
            let caller = work::WorkCarrier::independent(
                work::Boundary::Timer,
                work::WorkKind::Maintenance,
                work::Trigger::Timer,
                work::ExecutorRole::Gateway,
            );
            let token = caller.context().unwrap().token;
            caller.run(|| {
                let mut native = NativeRead::new(Family::ToolText);
                native.waiting_for_handle();
                let waits = || live::snapshot().unwrap().journal.iter().filter(|r| {
                    matches!(&r.payload, live::JournalPayload::Work(w)
                        if w.context.token == token && w.state == live::WorkState::Waiting(live::WaitKind::Lock))
                }).count();
                assert_eq!(waits(), 0); // timer alone is not a live wait
                if pending {
                    native.handle_pending();
                    native.handle_pending(); // same guard across multiple pending polls
                    assert_eq!(waits(), 1);
                    drop(native); // cancellation also retires the live wait
                } else {
                    native.executing();
                    native.finish(NativeOutcome::Ok, 17);
                    assert_eq!(waits(), 0);
                }
                assert_eq!(live::snapshot().unwrap().active.iter().find(|w| w.context.token == token).unwrap().state, live::WorkState::Running);
            });
            assert!(live::snapshot().unwrap().active.is_empty());
        }
    }

    #[test]
    fn pause_owns_separate_sleep_work_preserves_database_and_cancels_cleanly() {
        for (duration, backoff) in [
            (Duration::from_secs(60), false),
            (Duration::from_secs(60), true),
            (Duration::from_millis(1), false),
        ] {
            let observed = duration >= Duration::from_millis(250);
            let _consent = Consent::enabled();
            let database = DatabaseWorkloadTrace::start_at(
                DatabaseWorkload::TimelinePage,
                DatabaseOperationSite::TimelinePage,
            );
            let caller = work::WorkCarrier::independent(
                work::Boundary::Timer,
                work::WorkKind::Maintenance,
                work::Trigger::Timer,
                work::ExecutorRole::Gateway,
            )
            .with_database(database.context());
            let parent = caller.context().unwrap();
            let mut future = Box::pin(caller.scope(async {
                pause(duration, backoff, async {
                    let child = work::current().unwrap();
                    if observed {
                        assert_ne!(child.token, parent.token);
                        assert_eq!(child.parent, Some(parent.token));
                    } else {
                        assert_eq!(child.token, parent.token);
                    }
                    assert_eq!(
                        work::current_database().unwrap().workload(),
                        DatabaseWorkload::TimelinePage
                    );
                    assert!(current().is_none()); // pause is not a portable logical operation
                    std::future::pending::<()>().await
                })
                .await
            }));
            assert!(poll(future.as_mut()).is_pending());
            let active = live::snapshot().unwrap().active;
            if !observed {
                assert!(active.iter().all(|w| w.portable_payload.is_none()));
                drop(future);
                assert!(live::snapshot().unwrap().active.is_empty());
                assert!(terminals().is_empty());
                continue;
            }
            let sleeping = active
                .iter()
                .find(|w| w.portable_payload.is_some())
                .unwrap();
            assert!(matches!(sleeping.state, live::WorkState::SleepingUntil(_)));
            assert_eq!(sleeping.portable_payload.unwrap().family, Family::Mixed);
            assert_eq!(
                sleeping.portable_payload.unwrap().stage,
                Some(if backoff { Stage::Backoff } else { Stage::Idle })
            );
            assert_eq!(
                sleeping.retry_backoff_ms,
                if backoff { Some(60_000) } else { None }
            );
            drop(future);
            assert!(live::snapshot().unwrap().active.is_empty());
            let terminal = terminals();
            assert_eq!(terminal.len(), 1);
            assert!(matches!(
                terminal[0].state,
                live::WorkState::Terminal(work::Outcome::Cancelled)
            ));
        }
    }
}
