//! Capture-only attribution of SQLx command CPU on named Gateway SQLite workers.
//! No SQL, identifiers, errors, or per-query records leave this bounded collector.
use serde::Serialize;
use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tracing::{
    Span, Subscriber,
    span::{Attributes, Id},
};
use tracing_subscriber::{Layer, layer::Context as LayerContext, registry::LookupSpan};

pub(crate) const TARGET: &str = "pioneer_sqlite::query_cpu";
const MAX_SCOPES: usize = 128;
const MAX_SEGMENTS: u64 = 20_000;
// Independent first-admission reservations: unmeasured sequential scopes cannot
// consume the capacity for every measured segment to have its own full key.
const MAX_MEASURED_GROUPS: usize = MAX_SEGMENTS as usize;
const MAX_UNMEASURED_GROUPS: usize = 8192 + MAX_SCOPES;
pub(crate) const MAX_GROUPS: usize = MAX_MEASURED_GROUPS + MAX_UNMEASURED_GROUPS;
// Cumulative text allowance per admission class, not truncation of a key.
// Current fixed labels need <=95 bytes/full key (including fingerprint).
const MAX_MEASURED_KEY_BYTES: usize = MAX_MEASURED_GROUPS * 128;
const MAX_UNMEASURED_KEY_BYTES: usize = MAX_UNMEASURED_GROUPS * 128;
pub(crate) const MAX_GROUP_KEY_BYTES: usize = MAX_MEASURED_KEY_BYTES + MAX_UNMEASURED_KEY_BYTES;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static LIVE_SPANS: AtomicUsize = AtomicUsize::new(0);
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
static SESSION: OnceLock<Mutex<Option<Weak<SessionState>>>> = OnceLock::new();
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn slot() -> &'static Mutex<Option<Weak<SessionState>>> {
    SESSION.get_or_init(|| Mutex::new(None))
}

#[derive(Clone)]
struct Key {
    session: Arc<SessionState>,
    id: u64,
}
impl Key {
    fn same(&self, other: &Self) -> bool {
        self.id == other.id && Arc::ptr_eq(&self.session, &other.session)
    }
}
thread_local! {
    static CREATING: RefCell<Option<Key>> = const { RefCell::new(None) };
    static CALLER: RefCell<Option<Key>> = const { RefCell::new(None) };
    static WORKER: RefCell<Option<Segment>> = const { RefCell::new(None) };
}
struct Segment {
    key: Key,
    depth: u32,
    start: Option<u64>,
    conflicted: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize)]
struct GroupKey {
    fingerprint: Option<String>,
    workload: &'static str,
    site: &'static str,
    control: Option<&'static str>,
}
#[derive(Clone, Default, Debug, Serialize)]
pub struct Totals {
    pub completed_calls: u64,
    pub errors: u64,
    pub cancelled: u64,
    pub incomplete: u64,
    /// None means no successfully measured segment, Some(0) is a measured zero.
    pub cpu_ns: Option<u64>,
    pub measured_segments: u64,
}
impl Totals {
    fn add(&mut self, other: &Self) {
        self.completed_calls += other.completed_calls;
        self.errors += other.errors;
        self.cancelled += other.cancelled;
        self.incomplete += other.incomplete;
        self.measured_segments += other.measured_segments;
        if let Some(cpu) = other.cpu_ns {
            *self.cpu_ns.get_or_insert(0) += cpu;
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct Group {
    #[serde(flatten)]
    key: GroupKey,
    #[serde(flatten)]
    pub totals: Totals,
}
#[derive(Clone, Default, Debug, Serialize)]
pub struct Coverage {
    pub registered_scopes: u64,
    pub incomplete_scopes: u64,
    pub scope_reservation_overflow: u64,
    pub group_overflow_scopes: u64,
    pub segment_reservation_overflow: u64,
    pub reserved_segments: u64,
    pub measured_segments: u64,
    pub cpu_clock_reads: u64,
    pub clock_api_errors: u64,
    pub clock_invalid_or_overflow: u64,
    pub clock_regressions: u64,
    pub clock_zero_deltas: u64,
    pub unsupported_platform: u64,
    pub unfinished_segments: u64,
    pub nested_scope_conflicts: u64,
    pub scopes_without_callback: u64,
    pub fingerprint_unknown_callbacks: u64,
    pub conflicting_fingerprints: u64,
    pub missing_local_layer: u64,
    pub missing_work_context: u64,
    pub missing_database_context: u64,
}
#[derive(Clone, Debug, Serialize)]
pub struct Aggregation {
    /// Completeness of retained groups only, not instrumentation/process coverage.
    pub complete: bool,
    pub incomplete_reason: Option<&'static str>,
    pub retained_groups: usize,
    pub retained_key_bytes: usize,
    pub max_key_bytes: usize,
    /// Charges stay in the class of the first admitted scope, even if a null
    /// group is measured later. Updates to retained keys never need admission.
    pub measured_admission_groups: usize,
    pub unmeasured_admission_groups: usize,
    pub measured_admission_key_bytes: usize,
    pub unmeasured_admission_key_bytes: usize,
    pub max_measured_admission_groups: usize,
    pub max_unmeasured_admission_groups: usize,
    pub max_measured_admission_key_bytes: usize,
    pub max_unmeasured_admission_key_bytes: usize,
    /// Each aggregated scope exactly once, including the disjoint residual.
    pub accounted: Totals,
}
#[derive(Clone, Debug, Serialize)]
pub struct Snapshot {
    pub schema_version: u32,
    pub generation: u64,
    pub consent_generation: u64,
    pub started_unix_ms: u64,
    pub started_monotonic_ms: u64,
    pub ended_monotonic_ms: u64,
    pub elapsed_ns: u64,
    pub requested_seconds: u64,
    pub closed_reason: &'static str,
    pub max_scopes: usize,
    pub max_groups: usize,
    pub max_segments: u64,
    pub groups: Vec<Group>,
    pub aggregation: Aggregation,
    pub measured_unknown: Totals,
    pub group_overflow: Totals,
    pub coverage: Coverage,
    pub measurement: &'static str,
    pub limitations: &'static [&'static str],
}
struct QueryData {
    group: GroupKey,
    fingerprint: Option<u64>,
    callback: bool,
    fingerprint_conflict: bool,
    outcome: Outcome,
    totals: Totals,
    open_segments: u64,
    // Existing causality retained internally only; never exported as dimensions.
    _work: Option<crate::work::WorkContext>,
}
#[derive(Clone, Copy)]
enum Outcome {
    Pending,
    Ok,
    Error,
    Cancelled,
}
struct Data {
    sealed: Option<Arc<Snapshot>>,
    next_id: u64,
    queries: HashMap<u64, QueryData>,
    groups: Vec<Group>,
    group_index: HashMap<GroupKey, usize>,
    group_key_bytes: usize,
    measured_admission_groups: usize,
    unmeasured_admission_groups: usize,
    measured_admission_key_bytes: usize,
    unmeasured_admission_key_bytes: usize,
    aggregation_limit: Option<&'static str>,
    accounted: Totals,
    unknown: Totals,
    overflow: Totals,
    coverage: Coverage,
}
struct SessionState {
    generation: u64,
    epoch: u64,
    start: Instant,
    deadline: Instant,
    started_wall: u64,
    started_mono: u64,
    seconds: u64,
    data: Mutex<Data>,
}
impl SessionState {
    fn writable(&self, data: &Data) -> bool {
        data.sealed.is_none()
            && Instant::now() < self.deadline
            && crate::telemetry_sample_allowed(Some(self.epoch))
    }
    fn aggregate(data: &mut Data, mut query: QueryData, incomplete: bool) {
        if !query.callback {
            data.coverage.scopes_without_callback += 1;
        }
        query.group.fingerprint = query
            .fingerprint
            .filter(|_| !query.fingerprint_conflict)
            .map(|v| format!("{v:016x}"));
        if incomplete {
            data.coverage.incomplete_scopes += 1;
            query.totals.incomplete += 1;
            match query.outcome {
                Outcome::Error => query.totals.errors += 1,
                Outcome::Cancelled => query.totals.cancelled += 1,
                _ => {}
            }
        } else {
            match query.outcome {
                Outcome::Ok => query.totals.completed_calls += 1,
                Outcome::Error => {
                    query.totals.completed_calls += 1;
                    query.totals.errors += 1;
                }
                Outcome::Pending | Outcome::Cancelled => query.totals.cancelled += 1,
            }
        }
        data.coverage.unfinished_segments += query.open_segments;
        if query.group.fingerprint.is_none() {
            data.unknown.add(&query.totals);
        }
        data.accounted.add(&query.totals);
        if let Some(&index) = data.group_index.get(&query.group) {
            data.groups[index].totals.add(&query.totals);
            return;
        }
        let key_bytes = query.group.fingerprint.as_ref().map_or(0, String::len)
            + query.group.workload.len()
            + query.group.site.len()
            + query.group.control.map_or(0, str::len);
        let measured = query.totals.cpu_ns.is_some(); // Some(0) is measured.
        let reason = if measured {
            if data.measured_admission_groups >= MAX_MEASURED_GROUPS {
                Some("measured_group_count_budget")
            } else if key_bytes > MAX_MEASURED_KEY_BYTES - data.measured_admission_key_bytes {
                Some("measured_group_key_bytes_budget")
            } else {
                None
            }
        } else if data.unmeasured_admission_groups >= MAX_UNMEASURED_GROUPS {
            Some("unmeasured_group_count_budget")
        } else if key_bytes > MAX_UNMEASURED_KEY_BYTES - data.unmeasured_admission_key_bytes {
            Some("unmeasured_group_key_bytes_budget")
        } else {
            None
        };
        if let Some(reason) = reason {
            data.aggregation_limit.get_or_insert(reason);
            data.coverage.group_overflow_scopes += 1;
            data.overflow.add(&query.totals);
        } else {
            data.group_key_bytes += key_bytes;
            if measured {
                data.measured_admission_groups += 1;
                data.measured_admission_key_bytes += key_bytes;
            } else {
                data.unmeasured_admission_groups += 1;
                data.unmeasured_admission_key_bytes += key_bytes;
            }
            data.group_index
                .insert(query.group.clone(), data.groups.len());
            data.groups.push(Group {
                key: query.group,
                totals: query.totals,
            });
        }
    }
    fn seal(&self) -> Arc<Snapshot> {
        let mut data = lock(&self.data);
        if let Some(snapshot) = &data.sealed {
            return snapshot.clone();
        }
        let now = Instant::now();
        let end = now.min(self.deadline);
        let elapsed_ns = end
            .duration_since(self.start)
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let queries = std::mem::take(&mut data.queries);
        for (_, query) in queries {
            Self::aggregate(&mut data, query, true);
        }
        let snapshot = Snapshot {
            schema_version: 2,
            generation: self.generation,
            consent_generation: self.epoch,
            started_unix_ms: self.started_wall,
            started_monotonic_ms: self.started_mono,
            ended_monotonic_ms: self.started_mono + elapsed_ns / 1_000_000,
            elapsed_ns,
            requested_seconds: self.seconds,
            closed_reason: if !crate::telemetry_sample_allowed(Some(self.epoch)) {
                "consent_changed"
            } else if now >= self.deadline {
                "deadline"
            } else {
                "capture_finished_or_dropped"
            },
            max_scopes: MAX_SCOPES,
            max_groups: MAX_GROUPS,
            max_segments: MAX_SEGMENTS,
            aggregation: Aggregation {
                complete: data.aggregation_limit.is_none(),
                incomplete_reason: data.aggregation_limit,
                retained_groups: data.groups.len(),
                retained_key_bytes: data.group_key_bytes,
                max_key_bytes: MAX_GROUP_KEY_BYTES,
                measured_admission_groups: data.measured_admission_groups,
                unmeasured_admission_groups: data.unmeasured_admission_groups,
                measured_admission_key_bytes: data.measured_admission_key_bytes,
                unmeasured_admission_key_bytes: data.unmeasured_admission_key_bytes,
                max_measured_admission_groups: MAX_MEASURED_GROUPS,
                max_unmeasured_admission_groups: MAX_UNMEASURED_GROUPS,
                max_measured_admission_key_bytes: MAX_MEASURED_KEY_BYTES,
                max_unmeasured_admission_key_bytes: MAX_UNMEASURED_KEY_BYTES,
                accounted: data.accounted.clone(),
            },
            groups: std::mem::take(&mut data.groups),
            measured_unknown: data.unknown.clone(),
            group_overflow: data.overflow.clone(),
            coverage: data.coverage.clone(),
            measurement: "CPU of SQLx commands in query scopes on Gateway SQLite workers",
            limitations: &[
                "Includes preparation, SQLite execution, row processing/transfer and cleanup; not sqlite3_step alone.",
                "Private SQLx command kinds, including pool service commands under a scope, cannot be separated.",
                "Scopes begun before capture, uninstrumented work and worker bootstrap are not reconstructed.",
                "Open segments at window end have no endpoint CPU measurement; their unmeasured cost is unknown.",
                "Clock representation is nanoseconds, resolution is OS dependent; zero deltas may be below resolution.",
                "Consent changes can invalidate existing WorkCarrier owners; missing context remains explicit unknown.",
                "Only instrumented workload/site causality is available; this is not a complete async stack or SQL registry.",
                "SQL fingerprint/source mapping uses the exact build outside Gateway; ambiguity remains unresolved.",
                "Native profile and sidecar have independent windows; no process CPU or sample-share allocation is performed.",
                "aggregation.complete concerns retained observed groups only, not all SQL or process CPU.",
                "Measured and unmeasured first-admission reservations are independent; later updates retain their original charge.",
                "groups plus disjoint group_overflow equal aggregation.accounted; unmeasured CPU remains null/unknown.",
                "measured_unknown overlaps null-fingerprint groups and possibly group_overflow; never add it to accounted.",
            ],
        };
        let snapshot = Arc::new(snapshot);
        data.sealed = Some(snapshot.clone());
        snapshot
    }
}

/// One capture owner. Old scopes retain cleanup state, never authorization to
/// write into a later generation. Dropping the owner closes the window.
pub struct Session {
    state: Arc<SessionState>,
}
impl Session {
    pub fn start(seconds: u64, consent_epoch: u64) -> Option<Self> {
        if !(1..=60).contains(&seconds) || !crate::telemetry_sample_allowed(Some(consent_epoch)) {
            return None;
        }
        let mut current = lock(slot());
        if current.as_ref().and_then(Weak::upgrade).is_some()
            || !crate::telemetry_sample_allowed(Some(consent_epoch))
        {
            return None;
        }
        let start = Instant::now();
        let state = Arc::new(SessionState {
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            epoch: consent_epoch,
            start,
            deadline: start + Duration::from_secs(seconds),
            seconds,
            started_wall: crate::identity::unix_ms(),
            started_mono: crate::identity::monotonic_ms(),
            data: Mutex::new(Data {
                sealed: None,
                next_id: 0,
                queries: HashMap::new(),
                groups: Vec::with_capacity(MAX_GROUPS),
                group_index: HashMap::with_capacity(MAX_GROUPS),
                group_key_bytes: 0,
                measured_admission_groups: 0,
                unmeasured_admission_groups: 0,
                measured_admission_key_bytes: 0,
                unmeasured_admission_key_bytes: 0,
                aggregation_limit: None,
                accounted: Totals::default(),
                unknown: Totals::default(),
                overflow: Totals::default(),
                coverage: Coverage::default(),
            }),
        });
        *current = Some(Arc::downgrade(&state));
        ACTIVE.store(true, Ordering::Release);
        Some(Self { state })
    }
    /// The capture owner uses the collector's original deadline, never a second window.
    pub(crate) fn deadline(&self) -> Instant {
        self.state.deadline
    }
    pub fn finish(self) -> Snapshot {
        // seal is also called by opt-out/Drop: it only freezes the table. Copy
        // and rank here, on the existing diagnostic owner, outside its mutex.
        let mut snapshot = (*self.state.seal()).clone();
        snapshot.groups.sort_unstable_by(|a, b| {
            b.totals
                .cpu_ns
                .cmp(&a.totals.cpu_ns)
                .then_with(|| a.key.site.cmp(b.key.site))
                .then_with(|| a.key.fingerprint.cmp(&b.key.fingerprint))
                .then_with(|| a.key.workload.cmp(b.key.workload))
                .then_with(|| a.key.control.cmp(&b.key.control))
        });
        snapshot
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        let mut current = lock(slot());
        self.state.seal();
        if current
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|s| Arc::ptr_eq(&s, &self.state))
        {
            ACTIVE.store(false, Ordering::Release);
            *current = None;
        }
    }
}

// Consent transition is a cold control path. Seal here so a long native
// finalization cannot stretch the recorded authorized interval after opt-out.
pub(crate) fn close_on_opt_out() {
    if !ACTIVE.load(Ordering::Acquire) {
        return;
    }
    let current = lock(slot());
    if let Some(state) = current.as_ref().and_then(Weak::upgrade) {
        state.seal();
        ACTIVE.store(false, Ordering::Release);
    }
}

/// A caller reference to the same diagnostic state carried by SQLx's span.
/// The state does not own this span, avoiding a reference cycle.
#[derive(Default)]
pub struct QueryScope {
    span: Option<Span>,
    key: Option<Key>,
}
impl QueryScope {
    pub fn new() -> Self {
        Self::create(None)
    }
    /// Callers use only fixed source labels, never SQL or runtime values.
    pub fn control(label: &'static str) -> Self {
        Self::create(Some(label))
    }
    fn create(control: Option<&'static str>) -> Self {
        if !ACTIVE.load(Ordering::Acquire) {
            return Self::default();
        }
        let Some(session) = lock(slot()).as_ref().and_then(Weak::upgrade) else {
            return Self::default();
        };
        let mut data = lock(&session.data);
        if !session.writable(&data) {
            return Self::default();
        }
        if data.queries.len() >= MAX_SCOPES {
            data.coverage.scope_reservation_overflow += 1;
            return Self::default();
        }
        let db = crate::work::current_database();
        let work = crate::work::current();
        if work.is_none() {
            data.coverage.missing_work_context += 1;
        }
        if db.is_none() {
            data.coverage.missing_database_context += 1;
        }
        let group = GroupKey {
            fingerprint: None,
            workload: db
                .as_ref()
                .map_or("unclassified", |v| v.workload().as_str()),
            site: db.as_ref().map_or("unknown", |v| v.site().as_str()),
            control,
        };
        data.next_id += 1;
        let id = data.next_id;
        data.queries.insert(
            id,
            QueryData {
                group,
                fingerprint: None,
                callback: false,
                fingerprint_conflict: false,
                outcome: Outcome::Pending,
                totals: Totals::default(),
                open_segments: 0,
                _work: work,
            },
        );
        data.coverage.registered_scopes += 1;
        drop(data);
        let key = Key { session, id };
        let previous = CREATING.with(|s| s.replace(Some(key.clone())));
        let span = tracing::info_span!(target: "pioneer_sqlite::query_cpu", "sqlite.query_cpu");
        CREATING.with(|s| {
            s.replace(previous);
        });
        if span.is_disabled() {
            let mut data = lock(&key.session.data);
            if key.session.writable(&data) {
                data.coverage.missing_local_layer += 1;
                data.queries.remove(&id);
            }
            return Self::default();
        }
        Self {
            span: Some(span),
            key: Some(key),
        }
    }
    pub fn enter(&self) -> QueryGuard<'_> {
        let restore = self.key.is_some() || ACTIVE.load(Ordering::Acquire);
        let previous = if restore {
            CALLER.with(|s| s.replace(self.key.clone()))
        } else {
            None
        };
        QueryGuard {
            _entered: self.span.as_ref().map(Span::enter),
            previous,
            restore,
            _not_send: PhantomData,
        }
    }
    pub fn finish(&self, success: bool) {
        let Some(key) = &self.key else {
            return;
        };
        let mut data = lock(&key.session.data);
        if key.session.writable(&data) {
            if let Some(query) = data.queries.get_mut(&key.id) {
                // A stream error remains an error even if a later poll returns None.
                if !matches!(query.outcome, Outcome::Error) {
                    query.outcome = if success { Outcome::Ok } else { Outcome::Error };
                }
            }
        }
    }
}
impl Drop for QueryScope {
    fn drop(&mut self) {
        if let Some(key) = &self.key {
            let mut data = lock(&key.session.data);
            if key.session.writable(&data) {
                if let Some(query) = data.queries.get_mut(&key.id) {
                    if matches!(query.outcome, Outcome::Pending) {
                        query.outcome = Outcome::Cancelled;
                    }
                }
            }
        }
        // Span drops only after outcome is set; worker refs may close it later.
    }
}
pub struct QueryGuard<'a> {
    _entered: Option<tracing::span::Entered<'a>>,
    previous: Option<Key>,
    restore: bool,
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for QueryGuard<'_> {
    fn drop(&mut self) {
        if self.restore {
            CALLER.with(|s| {
                s.replace(self.previous.take());
            });
        }
    }
}

/// Attach the existing callback's canonical result, without hashing SQL again.
pub fn record_fingerprint(fingerprint: Option<u64>) {
    if !ACTIVE.load(Ordering::Acquire) {
        return;
    }
    CALLER.with(|s| {
        let Some(key) = s.borrow().clone() else {
            return;
        };
        let mut data = lock(&key.session.data);
        if !key.session.writable(&data) {
            return;
        }
        let fingerprint = fingerprint.filter(|v| *v != 0);
        if fingerprint.is_none() {
            data.coverage.fingerprint_unknown_callbacks += 1;
        }
        let mut conflict = false;
        if let Some(query) = data.queries.get_mut(&key.id) {
            // A control scope can cover several setup/service commands. Its
            // CPU stays explicitly unknown even if a callback supplies a hash.
            if query.group.control.is_some() {
                query.callback = true;
                return;
            }
            if query.callback && query.fingerprint != fingerprint {
                query.fingerprint_conflict = true;
                conflict = true;
            } else {
                query.fingerprint = fingerprint;
            }
            query.callback = true;
        }
        if conflict {
            data.coverage.conflicting_fingerprints += 1;
        }
    });
}

/// The pinned inner future is explicitly destroyed under its query context,
/// including cancellation. No guard survives an await/poll boundary.
pub struct ObservedFuture<F> {
    inner: std::mem::ManuallyDrop<F>,
    live: bool,
    scope: QueryScope,
}
impl<F> ObservedFuture<F> {
    pub fn new(future: F, scope: QueryScope) -> Self {
        Self {
            inner: std::mem::ManuallyDrop::new(future),
            live: true,
            scope,
        }
    }
}
impl<F: Future<Output = Result<T, E>>, T, E> Future for ObservedFuture<F> {
    type Output = Result<T, E>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: inner is structurally pinned, never moved, and destroyed in
        // place before this wrapper is released. No API exposes a mutable F.
        let this = unsafe { self.get_unchecked_mut() };
        assert!(this.live, "polled completed database future");
        let _guard = this.scope.enter();
        let result = unsafe { Pin::new_unchecked(&mut *this.inner) }.poll(cx);
        if let Poll::Ready(value) = &result {
            this.scope.finish(value.is_ok());
            this.live = false;
            unsafe {
                std::mem::ManuallyDrop::drop(&mut this.inner);
            }
        }
        result
    }
}
impl<F> Drop for ObservedFuture<F> {
    fn drop(&mut self) {
        let _guard = self.scope.enter();
        if self.live {
            self.live = false;
            // SAFETY: drop in place exactly once; pinning is maintained.
            unsafe {
                std::mem::ManuallyDrop::drop(&mut self.inner);
            }
        }
    }
}
pub async fn observe<F: Future<Output = Result<T, E>>, T, E>(future: F) -> Result<T, E> {
    ObservedFuture::new(future, QueryScope::new()).await
}

pub(crate) struct QueryCpuLayer;
pub(crate) fn target_enabled(target: &str) -> bool {
    // Keep lifecycle callbacks enabled after capture closes. Never enable
    // verbose SQLx/SeaORM spans or events as a side effect of capture.
    target == TARGET || (target == "pioneer_sqlite::pool" && crate::telemetry_enabled())
}
fn key_for<S>(id: &Id, ctx: &LayerContext<'_, S>) -> Option<Key>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    ctx.span(id)?
        .scope()
        .find_map(|span| span.extensions().get::<Key>().cloned())
}
impl<S> Layer<S> for QueryCpuLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        if attrs.metadata().target() != TARGET {
            return;
        }
        if let Some(key) = CREATING.with(|s| s.borrow().clone()) {
            if let Some(span) = ctx.span(id) {
                span.extensions_mut().insert(key);
                LIVE_SPANS.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    fn on_enter(&self, id: &Id, ctx: LayerContext<'_, S>) {
        if !ACTIVE.load(Ordering::Acquire) && LIVE_SPANS.load(Ordering::Relaxed) == 0 {
            return;
        }
        // Thread names are application prefixes, not operating system TIDs.
        let thread = std::thread::current();
        if !thread.name().is_some_and(|n| {
            n.starts_with("pioneer-sqlite-reader-") || n.starts_with("pioneer-sqlite-writer-")
        }) {
            return;
        }
        let Some(key) = key_for(id, &ctx) else {
            return;
        };
        WORKER.with(|cell| {
            let mut slot = cell.borrow_mut();
            if let Some(segment) = slot.as_mut() {
                segment.depth += 1;
                if !segment.key.same(&key) {
                    segment.conflicted = true;
                    let mut data = lock(&key.session.data);
                    if key.session.writable(&data) {
                        data.coverage.nested_scope_conflicts += 1;
                    }
                }
                return;
            }
            let mut data = lock(&key.session.data);
            if !key.session.writable(&data) {
                return;
            }
            if data.coverage.reserved_segments >= MAX_SEGMENTS {
                data.coverage.segment_reservation_overflow += 1;
                // Retain nesting balance even when no clock budget remains.
                *slot = Some(Segment {
                    key: key.clone(),
                    depth: 1,
                    start: None,
                    conflicted: false,
                });
                return;
            }
            if !data.queries.contains_key(&key.id) {
                return;
            }
            data.coverage.reserved_segments += 1;
            let start = read_clock(&mut data.coverage);
            if start.is_some() {
                data.queries.get_mut(&key.id).unwrap().open_segments += 1;
            }
            drop(data);
            *slot = Some(Segment {
                key,
                depth: 1,
                start,
                conflicted: false,
            });
        });
    }
    fn on_exit(&self, id: &Id, ctx: LayerContext<'_, S>) {
        if LIVE_SPANS.load(Ordering::Relaxed) == 0 {
            return;
        }
        if key_for(id, &ctx).is_none() {
            return;
        }
        WORKER.with(|cell| {
            let mut slot = cell.borrow_mut();
            let Some(segment) = slot.as_mut() else {
                return;
            };
            segment.depth -= 1;
            if segment.depth != 0 {
                return;
            }
            let segment = slot.take().unwrap();
            let Some(start) = segment.start else {
                return;
            };
            let key = segment.key;
            let mut data = lock(&key.session.data);
            if !key.session.writable(&data) {
                return;
            }
            let end = read_clock(&mut data.coverage);
            if let Some(query) = data.queries.get_mut(&key.id) {
                query.open_segments -= 1;
                // Different diagnostic owners nested on one worker cannot be
                // separated by this clock segment. Retain CPU as unknown.
                query.fingerprint_conflict |= segment.conflicted;
            }
            let Some(end) = end else {
                return;
            };
            let Some(delta) = end.checked_sub(start) else {
                data.coverage.clock_regressions += 1;
                return;
            };
            if delta == 0 {
                data.coverage.clock_zero_deltas += 1;
            }
            data.coverage.measured_segments += 1;
            if let Some(query) = data.queries.get_mut(&key.id) {
                query.totals.measured_segments += 1;
                *query.totals.cpu_ns.get_or_insert(0) += delta;
            }
        });
    }
    fn on_close(&self, id: Id, ctx: LayerContext<'_, S>) {
        if LIVE_SPANS.load(Ordering::Relaxed) == 0 {
            return;
        }
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(key) = span.extensions().get::<Key>().cloned() else {
            return;
        };
        LIVE_SPANS.fetch_sub(1, Ordering::Relaxed);
        let mut data = lock(&key.session.data);
        // At deadline leave it registered for the sealed incomplete snapshot.
        // Registry extensions still drop normally even when recording is shut.
        if !key.session.writable(&data) {
            return;
        }
        if let Some(query) = data.queries.remove(&key.id) {
            SessionState::aggregate(&mut data, query, false);
        }
    }
}

#[derive(Clone, Copy)]
enum ClockError {
    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    Api,
    #[cfg(any(target_os = "macos", target_os = "linux", windows))]
    Invalid,
    #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
    Unsupported,
}
fn read_clock(coverage: &mut Coverage) -> Option<u64> {
    coverage.cpu_clock_reads += 1;
    match thread_cpu_ns() {
        Ok(cpu) => Some(cpu),
        Err(error) => {
            match error {
                #[cfg(any(target_os = "macos", target_os = "linux", windows))]
                ClockError::Api => coverage.clock_api_errors += 1,
                #[cfg(any(target_os = "macos", target_os = "linux", windows))]
                ClockError::Invalid => coverage.clock_invalid_or_overflow += 1,
                #[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
                ClockError::Unsupported => coverage.unsupported_platform += 1,
            }
            None
        }
    }
}
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn thread_cpu_ns() -> Result<u64, ClockError> {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: OS writes one timespec; only inspected after a successful call.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, value.as_mut_ptr()) } != 0 {
        return Err(ClockError::Api);
    }
    let value = unsafe { value.assume_init() };
    let sec = u64::try_from(value.tv_sec).map_err(|_| ClockError::Invalid)?;
    let ns = u64::try_from(value.tv_nsec).map_err(|_| ClockError::Invalid)?;
    if ns >= 1_000_000_000 {
        return Err(ClockError::Invalid);
    }
    sec.checked_mul(1_000_000_000)
        .and_then(|s| s.checked_add(ns))
        .ok_or(ClockError::Invalid)
}
#[cfg(windows)]
fn thread_cpu_ns() -> Result<u64, ClockError> {
    use windows_sys::Win32::{
        Foundation::FILETIME,
        System::Threading::{GetCurrentThread, GetThreadTimes},
    };
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: pseudo handle is valid for this thread; all output pointers are live.
    if unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    } == 0
    {
        return Err(ClockError::Api);
    }
    let ticks = |v: FILETIME| (u64::from(v.dwHighDateTime) << 32) | u64::from(v.dwLowDateTime);
    ticks(kernel)
        .checked_add(ticks(user))
        .and_then(|v| v.checked_mul(100))
        .ok_or(ClockError::Invalid)
}
#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
fn thread_cpu_ns() -> Result<u64, ClockError> {
    Err(ClockError::Unsupported)
}

// Deterministic, test-only input at the collector's completed-scope boundary.
// Does not replace the production CPU clock or export a production test hook.
#[cfg(test)]
pub(crate) mod aggregation_fixtures {
    use super::*;
    pub(super) fn session() -> Session {
        let start = Instant::now();
        Session {
            state: Arc::new(SessionState {
                generation: 0,
                epoch: crate::telemetry_consent_snapshot().1,
                start,
                deadline: start + Duration::from_secs(60),
                started_wall: 0,
                started_mono: 0,
                seconds: 60,
                data: Mutex::new(Data {
                    sealed: None,
                    next_id: 0,
                    queries: HashMap::new(),
                    groups: Vec::with_capacity(MAX_GROUPS),
                    group_index: HashMap::with_capacity(MAX_GROUPS),
                    group_key_bytes: 0,
                    measured_admission_groups: 0,
                    unmeasured_admission_groups: 0,
                    measured_admission_key_bytes: 0,
                    unmeasured_admission_key_bytes: 0,
                    aggregation_limit: None,
                    accounted: Totals::default(),
                    unknown: Totals::default(),
                    overflow: Totals::default(),
                    coverage: Coverage::default(),
                }),
            }),
        }
    }
    pub(super) fn record(
        session: &Session,
        fingerprint: Option<u64>,
        workload: &'static str,
        site: &'static str,
        control: Option<&'static str>,
        cpu_ns: Option<u64>,
        outcome: Outcome,
        incomplete: bool,
    ) {
        let mut data = lock(&session.state.data);
        data.coverage.registered_scopes += 1;
        data.coverage.measured_segments += u64::from(cpu_ns.is_some());
        data.coverage.reserved_segments += u64::from(cpu_ns.is_some());
        SessionState::aggregate(
            &mut data,
            QueryData {
                group: GroupKey {
                    fingerprint: None,
                    workload,
                    site,
                    control,
                },
                fingerprint,
                callback: true,
                fingerprint_conflict: false,
                outcome,
                totals: Totals {
                    cpu_ns,
                    measured_segments: u64::from(cpu_ns.is_some()),
                    ..Totals::default()
                },
                open_segments: 0,
                _work: None,
            },
            incomplete,
        );
    }
    pub(crate) fn full_budget_snapshot() -> Snapshot {
        let session = session();
        const WORKLOAD: &str = "cli_runtime.native_event_cleanup";
        const SITE: &str = "projection_receipt_cleanup_quantum";
        // Generated fixed source labels only in this fixture. Each null-control
        // full key is 32+34+46+16=128 UTF-8 bytes, including valid JSON escapes.
        // No SQL, parameters, domain identity or decorative envelope padding.
        for i in 0..8192 {
            let control = Box::leak(format!("{}{:016x}", "\u{1}".repeat(46), i).into_boxed_str());
            record(
                &session,
                None,
                WORKLOAD,
                SITE,
                Some(control),
                None,
                Outcome::Ok,
                false,
            );
        }
        for i in 1..=20_000 {
            record(
                &session,
                Some(i),
                WORKLOAD,
                SITE,
                None,
                Some(if i == 20_000 {
                    1_000_000
                } else if i == 1 {
                    0
                } else {
                    1
                }),
                Outcome::Ok,
                false,
            );
        }
        {
            let mut data = lock(&session.state.data);
            for i in 8192..8320 {
                let control =
                    Box::leak(format!("{}{:016x}", "\u{1}".repeat(46), i).into_boxed_str());
                // Leave all 128 active records for the real seal path after
                // the completed measured/unmeasured tables have filled.
                data.queries.insert(
                    i,
                    QueryData {
                        group: GroupKey {
                            fingerprint: None,
                            workload: WORKLOAD,
                            site: SITE,
                            control: Some(control),
                        },
                        fingerprint: None,
                        callback: true,
                        fingerprint_conflict: false,
                        outcome: Outcome::Pending,
                        totals: Totals::default(),
                        open_segments: 0,
                        _work: None,
                    },
                );
                data.coverage.registered_scopes += 1;
            }
        }
        session.finish()
    }
    pub(crate) fn large_snapshot() -> Snapshot {
        let session = session();
        for i in 1..=1000 {
            record(
                &session,
                Some(i),
                "interactive",
                "site_a",
                None,
                Some(if i == 1000 {
                    20_000
                } else if i == 999 {
                    10_000
                } else {
                    1
                }),
                Outcome::Ok,
                false,
            );
        }
        session.finish()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Barrier;
    use tracing_subscriber::{filter::filter_fn, layer::SubscriberExt};

    // Own the global capture slot only for these collector boundary tests.
    pub(crate) static SERIAL: Mutex<()> = Mutex::new(());
    fn dispatch() -> tracing::Dispatch {
        tracing::Dispatch::new(tracing_subscriber::registry().with(
            QueryCpuLayer.with_filter(filter_fn(|m| m.is_span() && target_enabled(m.target()))),
        ))
    }

    // Shared with diagnostics owner-path regressions. Real scope/callback and
    // named SQLite-worker span boundaries; no SQL connections or fabricated totals.
    pub(crate) fn capture_command(fingerprint: u64, measured: bool) {
        capture_scope(Some(fingerprint), measured, None);
    }
    pub(crate) fn capture_control() {
        capture_scope(None, false, Some("begin"));
    }
    fn capture_scope(fingerprint: Option<u64>, measured: bool, control: Option<&'static str>) {
        let dispatch = dispatch();
        tracing::dispatcher::with_default(&dispatch, || {
            let trace = crate::DatabaseWorkloadTrace::start_at(
                crate::DatabaseWorkload::TimelinePage,
                crate::DatabaseOperationSite::TimelinePage,
            );
            let carrier = crate::work::capture().with_database(trace.context());
            let _work = carrier.enter();
            let scope = control.map_or_else(QueryScope::new, QueryScope::control);
            if measured {
                if let Some(span) = scope.span.clone() {
                    let worker_dispatch = dispatch.clone();
                    std::thread::Builder::new()
                        .name("pioneer-sqlite-reader-capture-test".into())
                        .spawn(move || {
                            tracing::dispatcher::with_default(&worker_dispatch, || {
                                let _entered = span.enter();
                                std::hint::black_box(17_u64.wrapping_mul(23));
                            })
                        })
                        .unwrap()
                        .join()
                        .unwrap();
                }
            }
            {
                let _entered = scope.enter();
                record_fingerprint(fingerprint);
            }
            scope.finish(true);
            drop(scope);
            trace.finish_success();
        });
    }
    pub(crate) fn wait_for_capture() -> Instant {
        let timeout = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(session) = lock(slot()).as_ref().and_then(Weak::upgrade) {
                return session.deadline;
            }
            assert!(Instant::now() < timeout, "capture Session did not start");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn assert_balance(snapshot: &Snapshot) {
        // Partition check only; fixed totals/ranks below are the independent
        // oracle. Unknown is deliberately absent from this disjoint sum.
        macro_rules! counter {
            ($field:ident) => {
                assert_eq!(
                    snapshot.groups.iter().map(|g| g.totals.$field).sum::<u64>()
                        + snapshot.group_overflow.$field,
                    snapshot.aggregation.accounted.$field
                );
            };
        }
        counter!(completed_calls);
        counter!(errors);
        counter!(cancelled);
        counter!(incomplete);
        counter!(measured_segments);
        let cpu: Vec<_> = snapshot
            .groups
            .iter()
            .map(|g| g.totals.cpu_ns)
            .chain(std::iter::once(snapshot.group_overflow.cpu_ns))
            .flatten()
            .collect();
        assert_eq!(
            if cpu.is_empty() {
                None
            } else {
                Some(cpu.iter().sum::<u64>())
            },
            snapshot.aggregation.accounted.cpu_ns
        );
        assert_eq!(
            snapshot.aggregation.accounted.measured_segments,
            snapshot.coverage.measured_segments
        );
    }

    #[test]
    fn full_table_retains_late_expensive_groups_and_is_completion_order_independent() {
        use aggregation_fixtures::{record, session};
        let collect = |reverse: bool| {
            let session = session();
            let mut inputs: Vec<_> = (1..=1000)
                .map(|i| {
                    (
                        Some(i),
                        "interactive",
                        "site_a",
                        None,
                        Some(if i == 1000 {
                            20_000
                        } else if i == 999 {
                            10_000
                        } else {
                            1
                        }),
                    )
                })
                .collect();
            inputs.extend([
                (Some(1000), "interactive", "site_a", None, Some(7)),
                (Some(1000), "interactive", "site_b", None, Some(5000)),
                (Some(1000), "maintenance", "site_a", None, Some(20_007)),
                (None, "interactive", "site_a", Some("begin"), Some(0)),
                (None, "interactive", "site_a", Some("end"), Some(0)),
                (None, "interactive", "site_a", Some("ping"), None),
            ]);
            if reverse {
                inputs.reverse();
            }
            for (fp, workload, site, control, cpu) in inputs {
                record(
                    &session,
                    fp,
                    workload,
                    site,
                    control,
                    cpu,
                    Outcome::Ok,
                    false,
                );
            }
            // An idempotent seal must not recount scopes or perform ranking.
            let first = session.state.seal();
            assert!(Arc::ptr_eq(&first, &session.state.seal()));
            session.finish()
        };
        let forward = collect(false);
        let reverse = collect(true);
        assert_eq!(forward.groups.len(), 1005);
        assert!(forward.aggregation.complete);
        assert_eq!(forward.coverage.group_overflow_scopes, 0);
        assert_eq!(forward.aggregation.accounted.completed_calls, 1006);
        assert_eq!(forward.aggregation.accounted.cpu_ns, Some(56_012));
        assert_eq!(forward.aggregation.accounted.measured_segments, 1005);
        assert_eq!(
            forward.groups[0].key.fingerprint.as_deref(),
            Some("00000000000003e8")
        );
        assert_eq!(forward.groups[0].totals.cpu_ns, Some(20_007));
        assert_eq!(forward.groups[0].totals.completed_calls, 2);
        assert_eq!(forward.groups[0].key.workload, "interactive");
        assert_eq!(forward.groups[1].key.workload, "maintenance");
        assert_eq!(forward.groups[1].totals.cpu_ns, Some(20_007));
        assert_eq!(forward.groups[2].totals.cpu_ns, Some(10_000));
        assert_eq!(forward.groups[3].key.site, "site_b");
        assert_eq!(forward.groups[1002].key.control, Some("begin"));
        assert_eq!(forward.groups[1003].key.control, Some("end"));
        assert_eq!(forward.groups[1002].totals.cpu_ns, Some(0));
        assert_eq!(forward.groups[1003].totals.cpu_ns, Some(0));
        assert_eq!(forward.groups[1004].totals.cpu_ns, None);
        assert_eq!(forward.measured_unknown.completed_calls, 3);
        assert_eq!(forward.measured_unknown.cpu_ns, Some(0));
        assert_eq!(
            serde_json::to_value(&forward.groups).unwrap(),
            serde_json::to_value(&reverse.groups).unwrap()
        );
        assert_balance(&forward);
        assert_balance(&reverse);
    }

    #[test]
    fn full_segment_capacity_and_extra_null_incomplete_allowance_retain_every_identity() {
        let snapshot = aggregation_fixtures::full_budget_snapshot();
        assert!(snapshot.aggregation.complete);
        assert_eq!(snapshot.groups.len(), 28_320);
        assert_eq!(snapshot.aggregation.measured_admission_groups, 20_000);
        assert_eq!(snapshot.aggregation.unmeasured_admission_groups, 8320);
        assert_eq!(snapshot.coverage.registered_scopes, 28_320);
        assert_eq!(snapshot.coverage.measured_segments, 20_000);
        assert_eq!(snapshot.coverage.reserved_segments, MAX_SEGMENTS);
        assert_eq!(snapshot.aggregation.accounted.cpu_ns, Some(1_019_998));
        assert_eq!(snapshot.aggregation.accounted.completed_calls, 28_192);
        assert_eq!(snapshot.aggregation.accounted.incomplete, 128);
        assert_eq!(
            snapshot.groups[0].key.fingerprint.as_deref(),
            Some("0000000000004e20")
        );
        assert_eq!(snapshot.groups[0].totals.cpu_ns, Some(1_000_000));
        assert_eq!(snapshot.groups[19_999].totals.cpu_ns, Some(0));
        // Check the entire identity set, not just count/sum/top.
        let fingerprints: std::collections::BTreeSet<_> = snapshot
            .groups
            .iter()
            .filter_map(|g| g.key.fingerprint.as_ref())
            .map(|v| u64::from_str_radix(v, 16).unwrap())
            .collect();
        assert_eq!(
            fingerprints,
            (1..=20_000).collect::<std::collections::BTreeSet<_>>()
        );
        let controls: std::collections::BTreeSet<_> = snapshot
            .groups
            .iter()
            .filter_map(|g| g.key.control)
            .map(|control| {
                assert_eq!(control.len(), 62);
                assert!(control.as_bytes()[..46].iter().all(|b| *b == 1));
                u64::from_str_radix(&control[46..], 16).unwrap()
            })
            .collect();
        assert_eq!(
            controls,
            (0..8320).collect::<std::collections::BTreeSet<_>>()
        );
        assert_eq!(snapshot.aggregation.measured_admission_key_bytes, 1_640_000);
        assert_eq!(
            snapshot.aggregation.unmeasured_admission_key_bytes,
            1_064_960
        );
        assert_eq!(snapshot.aggregation.retained_key_bytes, 2_704_960);
        assert_eq!(snapshot.measured_unknown.cpu_ns, None);
        assert_eq!(snapshot.group_overflow.completed_calls, 0);
        assert_eq!(snapshot.coverage.group_overflow_scopes, 0);
        assert_balance(&snapshot);
    }

    #[test]
    fn unmeasured_allowance_exhaustion_does_not_displace_measured_groups() {
        use aggregation_fixtures::{record, session};
        let session = session();
        for i in 1..=MAX_UNMEASURED_GROUPS {
            record(
                &session,
                Some(i as u64),
                "interactive",
                "a",
                None,
                None,
                Outcome::Ok,
                false,
            );
        }
        // First overflow is truly unmeasured; measured reservation stays free.
        record(
            &session,
            None,
            "interactive",
            "a",
            None,
            None,
            Outcome::Error,
            true,
        );
        record(
            &session,
            None,
            "interactive",
            "a",
            Some("begin"),
            None,
            Outcome::Cancelled,
            true,
        );
        // A retained null key can be measured later without another admission.
        record(
            &session,
            Some(1),
            "interactive",
            "a",
            None,
            Some(0),
            Outcome::Error,
            false,
        );
        record(
            &session,
            Some(99_999),
            "interactive",
            "a",
            None,
            Some(100),
            Outcome::Ok,
            false,
        );
        let snapshot = session.finish();
        assert!(!snapshot.aggregation.complete);
        assert_eq!(
            snapshot.aggregation.incomplete_reason,
            Some("unmeasured_group_count_budget")
        );
        assert_eq!(snapshot.aggregation.unmeasured_admission_groups, 8320);
        assert_eq!(snapshot.aggregation.measured_admission_groups, 1);
        assert_eq!(snapshot.coverage.group_overflow_scopes, 2);
        assert_eq!(snapshot.group_overflow.cpu_ns, None);
        assert_eq!(snapshot.group_overflow.measured_segments, 0);
        assert_eq!(snapshot.group_overflow.incomplete, 2);
        assert_eq!(snapshot.group_overflow.errors, 1);
        assert_eq!(snapshot.group_overflow.cancelled, 1);
        assert_eq!(snapshot.measured_unknown.cpu_ns, None);
        assert_eq!(
            snapshot.groups[0].key.fingerprint.as_deref(),
            Some("000000000001869f")
        );
        assert_eq!(snapshot.groups[0].totals.cpu_ns, Some(100));
        assert_eq!(snapshot.groups[1].totals.cpu_ns, Some(0));
        assert_eq!(snapshot.aggregation.accounted.cpu_ns, Some(100));
        assert_eq!(
            snapshot.aggregation.accounted.completed_calls,
            MAX_UNMEASURED_GROUPS as u64 + 2
        );
        assert_eq!(snapshot.aggregation.accounted.errors, 2);
        assert_balance(&snapshot);
    }

    #[test]
    fn key_bytes_budgets_preserve_measured_residual_and_unknown_overlap() {
        use aggregation_fixtures::{record, session};
        // Long fixed labels exercise cumulative text bounds independently of
        // group count. These are collector inputs, never domain/SQL text.
        const LONG_SITE: &str = concat!(
            "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
            "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
        );
        for measured in [false, true] {
            let session = session();
            for i in 1..=16_000 {
                record(
                    &session,
                    Some(i),
                    "interactive",
                    LONG_SITE,
                    None,
                    measured.then_some(1),
                    Outcome::Ok,
                    false,
                );
            }
            record(
                &session,
                Some(1),
                "interactive",
                LONG_SITE,
                None,
                Some(0),
                Outcome::Error,
                false,
            );
            record(
                &session,
                Some(99_999),
                "interactive",
                LONG_SITE,
                None,
                measured.then_some(100),
                Outcome::Ok,
                false,
            );
            record(
                &session,
                None,
                "interactive",
                LONG_SITE,
                None,
                measured.then_some(25),
                Outcome::Error,
                true,
            );
            let snapshot = session.finish();
            assert!(!snapshot.aggregation.complete);
            assert_eq!(
                snapshot.aggregation.incomplete_reason,
                Some(if measured {
                    "measured_group_key_bytes_budget"
                } else {
                    "unmeasured_group_key_bytes_budget"
                })
            );
            assert!(
                snapshot.groups.len()
                    < if measured {
                        MAX_MEASURED_GROUPS
                    } else {
                        MAX_UNMEASURED_GROUPS
                    }
            );
            assert!(snapshot.aggregation.retained_key_bytes <= MAX_GROUP_KEY_BYTES);
            assert!(snapshot.group_overflow.completed_calls > 0);
            assert_eq!(snapshot.group_overflow.errors, 1);
            assert_eq!(snapshot.group_overflow.incomplete, 1);
            assert_eq!(
                snapshot.aggregation.accounted.cpu_ns,
                Some(if measured { 16_125 } else { 0 })
            );
            assert_eq!(snapshot.aggregation.accounted.completed_calls, 16_002);
            assert_eq!(snapshot.measured_unknown.cpu_ns, measured.then_some(25));
            assert_eq!(snapshot.group_overflow.cpu_ns.is_some(), measured);
            // Unknown is a subset of residual here, never another partition.
            assert_balance(&snapshot);
        }
    }

    #[test]
    fn callback_and_worker_close_order_preserve_one_segment_and_one_completion() {
        let _serial = lock(&SERIAL);
        let dispatch = dispatch();
        tracing::dispatcher::with_default(&dispatch, || {
            let session = Session::start(60, crate::telemetry_consent_snapshot().1).unwrap();
            for callback_first in [true, false] {
                let scope = QueryScope::new();
                let worker_span = scope.span.as_ref().unwrap().clone();
                let ready = Arc::new(Barrier::new(2));
                let release = Arc::new(Barrier::new(2));
                let thread_dispatch = dispatch.clone();
                let worker = {
                    let ready = ready.clone();
                    let release = release.clone();
                    std::thread::Builder::new()
                        .name("pioneer-sqlite-reader-test".into())
                        .spawn(move || {
                            tracing::dispatcher::with_default(&thread_dispatch, || {
                                let _outer = worker_span.enter();
                                // A nested pool span sees the same ancestor. It must
                                // not create a second CPU segment.
                                let nested =
                                    tracing::trace_span!(target: "pioneer_sqlite::pool", "nested");
                                let _nested = nested.enter();
                                ready.wait();
                                release.wait();
                            });
                        })
                        .unwrap()
                };
                ready.wait();
                if callback_first {
                    let _guard = scope.enter();
                    record_fingerprint(Some(17));
                }
                scope.finish(true);
                if callback_first {
                    drop(scope);
                    release.wait();
                    worker.join().unwrap();
                } else {
                    release.wait();
                    worker.join().unwrap();
                    {
                        let _guard = scope.enter();
                        record_fingerprint(Some(17));
                    }
                    drop(scope);
                }
            }
            let snapshot = session.finish();
            assert_eq!(snapshot.groups.len(), 1);
            assert_eq!(snapshot.groups[0].totals.completed_calls, 2);
            assert_eq!(snapshot.groups[0].totals.cancelled, 0);
            assert_eq!(snapshot.coverage.scopes_without_callback, 0);
            assert_eq!(snapshot.coverage.reserved_segments, 2);
            #[cfg(any(target_os = "macos", target_os = "linux", windows))]
            {
                assert_eq!(snapshot.coverage.cpu_clock_reads, 4);
                assert_eq!(snapshot.coverage.measured_segments, 2);
                assert!(snapshot.groups[0].totals.cpu_ns.is_some());
            }
        });
    }

    #[test]
    fn sealing_live_worker_then_new_generation_cannot_transfer_cpu_or_late_fingerprint() {
        let _serial = lock(&SERIAL);
        let dispatch = dispatch();
        tracing::dispatcher::with_default(&dispatch, || {
            let epoch = crate::telemetry_consent_snapshot().1;
            let session = Session::start(60, epoch).unwrap();
            assert!(Session::start(60, epoch).is_none(), "overlap is rejected");
            let scope = QueryScope::new();
            let span = scope.span.as_ref().unwrap().clone();
            let ready = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            let thread_dispatch = dispatch.clone();
            let worker = {
                let ready = ready.clone();
                let release = release.clone();
                std::thread::Builder::new()
                    .name("pioneer-sqlite-writer-test".into())
                    .spawn(move || {
                        tracing::dispatcher::with_default(&thread_dispatch, || {
                            let _outer = span.enter();
                            ready.wait();
                            release.wait();
                            // Enter/exit after sealing must still balance cleanup.
                            let nested =
                                tracing::trace_span!(target: "pioneer_sqlite::pool", "late_nested");
                            let _nested = nested.enter();
                        });
                    })
                    .unwrap()
            };
            ready.wait();
            let closed = session.finish();
            assert_eq!(closed.measured_unknown.incomplete, 1);
            assert_eq!(closed.coverage.cpu_clock_reads, 1);
            assert_eq!(closed.coverage.unfinished_segments, 1);
            assert_eq!(closed.measured_unknown.cpu_ns, None);
            let next = Session::start(60, epoch).unwrap();
            {
                let _guard = scope.enter();
                record_fingerprint(Some(99)); // old generation, ignored
            }
            release.wait();
            worker.join().unwrap();
            drop(scope);
            let fresh = QueryScope::new();
            {
                let _guard = fresh.enter();
                record_fingerprint(Some(23));
            }
            fresh.finish(true);
            drop(fresh);
            let current = next.finish();
            assert_ne!(closed.generation, current.generation);
            assert_eq!(current.groups.len(), 1);
            assert_eq!(
                current.groups[0].key.fingerprint.as_deref(),
                Some("0000000000000017")
            );
            assert_eq!(current.groups[0].totals.completed_calls, 1);
            assert_eq!(current.groups[0].totals.cpu_ns, None);
            assert_eq!(current.coverage.cpu_clock_reads, 0);
            assert!(WORKER.with(|s| s.borrow().is_none()));

            // A different owner nested on one worker has no independent
            // endpoint interval. Combined CPU must lose the outer SQL identity.
            let session = Session::start(60, epoch).unwrap();
            let outer = QueryScope::new();
            {
                let _entered = outer.enter();
                record_fingerprint(Some(17));
            }
            let span = outer.span.as_ref().unwrap().clone();
            let worker_dispatch = dispatch.clone();
            std::thread::Builder::new()
                .name("pioneer-sqlite-reader-mixed".into())
                .spawn(move || {
                    tracing::dispatcher::with_default(&worker_dispatch, || {
                        let _entered = span.enter();
                        let inner = QueryScope::new();
                        {
                            let _inner = inner.enter();
                            record_fingerprint(Some(19));
                        }
                        inner.finish(true);
                        drop(inner);
                    });
                })
                .unwrap()
                .join()
                .unwrap();
            outer.finish(true);
            drop(outer);
            let mixed = session.finish();
            assert_eq!(mixed.coverage.nested_scope_conflicts, 1);
            assert_eq!(mixed.coverage.reserved_segments, 1);
            assert!(
                !mixed
                    .groups
                    .iter()
                    .any(|g| g.key.fingerprint.as_deref() == Some("0000000000000011"))
            );
            assert_eq!(mixed.measured_unknown.completed_calls, 1);
            #[cfg(any(target_os = "macos", target_os = "linux", windows))]
            assert!(mixed.measured_unknown.cpu_ns.is_some());
        });
    }

    #[test]
    fn scope_and_segment_limits_report_loss_without_aggregation_loss() {
        let _serial = lock(&SERIAL);
        let dispatch = dispatch();
        tracing::dispatcher::with_default(&dispatch, || {
            let session = Session::start(60, crate::telemetry_consent_snapshot().1).unwrap();
            let scopes: Vec<_> = (0..MAX_SCOPES).map(|_| QueryScope::new()).collect();
            drop(QueryScope::new()); // one rejected reservation
            for (i, scope) in scopes.into_iter().enumerate() {
                {
                    let _guard = scope.enter();
                    if i == 0 {
                        record_fingerprint(None);
                        record_fingerprint(Some(7)); // mixed callback identities stay unknown
                    } else {
                        record_fingerprint(Some(i as u64));
                    }
                }
                scope.finish(true);
                drop(scope);
            }
            let measured = QueryScope::new();
            let span = measured.span.as_ref().unwrap().clone();
            let thread_dispatch = dispatch.clone();
            std::thread::Builder::new()
                .name("pioneer-sqlite-reader-budget".into())
                .spawn(move || {
                    tracing::dispatcher::with_default(&thread_dispatch, || {
                        for _ in 0..=MAX_SEGMENTS {
                            let _entered = span.enter();
                        }
                    });
                })
                .unwrap()
                .join()
                .unwrap();
            // No callback: any measured CPU belongs to unknown, not hash zero.
            drop(measured);
            let snapshot = session.finish();
            assert_eq!(snapshot.groups.len(), MAX_SCOPES);
            assert_eq!(snapshot.coverage.scope_reservation_overflow, 1);
            assert_eq!(snapshot.coverage.group_overflow_scopes, 0);
            assert!(snapshot.aggregation.complete);
            assert_eq!(snapshot.coverage.reserved_segments, MAX_SEGMENTS);
            assert_eq!(snapshot.coverage.segment_reservation_overflow, 1);
            assert!(snapshot.coverage.cpu_clock_reads <= 40_000);
            assert_eq!(snapshot.coverage.conflicting_fingerprints, 1);
            assert_eq!(snapshot.coverage.scopes_without_callback, 1);
            assert_eq!(snapshot.measured_unknown.completed_calls, 1);
            assert_eq!(snapshot.measured_unknown.cancelled, 1);
            #[cfg(any(target_os = "macos", target_os = "linux", windows))]
            assert!(snapshot.measured_unknown.cpu_ns.is_some());
        });
    }

    #[test]
    fn deadline_and_consent_stop_clock_reads_without_stopping_span_cleanup() {
        let _serial = lock(&SERIAL);
        let dispatch = dispatch();
        tracing::dispatcher::with_default(&dispatch, || {
            let epoch = crate::telemetry_consent_snapshot().1;
            let session = Session::start(1, epoch).unwrap();
            let scope = QueryScope::new();
            // Deadline is intentionally exercised through the real clock,
            // without a production fake-clock hook.
            std::thread::sleep(Duration::from_millis(1_050));
            {
                let _guard = scope.enter();
                record_fingerprint(Some(17));
            }
            scope.finish(true);
            drop(scope);
            let snapshot = session.finish();
            assert_eq!(snapshot.closed_reason, "deadline");
            assert_eq!(snapshot.elapsed_ns, 1_000_000_000);
            assert_eq!(snapshot.measured_unknown.incomplete, 1);
            assert_eq!(snapshot.coverage.cpu_clock_reads, 0);

            let session = Session::start(60, epoch).unwrap();
            let scope = QueryScope::new();
            // Use the existing opt-out gate; restore it even if assertions fail.
            struct Restore;
            impl Drop for Restore {
                fn drop(&mut self) {
                    crate::set_telemetry_gate(true);
                }
            }
            let _restore = Restore;
            crate::set_telemetry_gate(false);
            {
                let _guard = scope.enter();
                record_fingerprint(Some(33));
            }
            drop(scope);
            let snapshot = session.finish();
            assert_eq!(snapshot.closed_reason, "consent_changed");
            assert_eq!(snapshot.coverage.cpu_clock_reads, 0);
            assert_eq!(snapshot.measured_unknown.incomplete, 1);
        });
    }
}
