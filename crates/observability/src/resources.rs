//! Telemetry-owned selected-process sampling. Field values are snapshots or OS
//! lifetime counters, never cumulative counters submitted to the Delta reader.
mod platform;
use crate::identity::{monotonic_ms, unix_ms};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Meter},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex, OnceLock},
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub const MAX_CHILDREN: usize = 64;
const MAX_EXITED: usize = 64;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum Field {
    RssBytes,
    FootprintBytes,
    Threads,
    Descriptors,
    Handles,
    Faults,
    MinorFaults,
    MajorFaults,
    Wakeups,
    StorageReadBytes,
    StorageWriteBytes,
    LogicalReadBytes,
    LogicalWriteBytes,
    HostMemoryBytes,
    HostAvailableBytes,
    HostSwapUsedBytes,
    HostPressure,
    LogicalCpus,
    QuotaCores,
}
impl Field {
    const PROCESS: [Self; 13] = [
        Self::RssBytes,
        Self::FootprintBytes,
        Self::Threads,
        Self::Descriptors,
        Self::Handles,
        Self::Faults,
        Self::MinorFaults,
        Self::MajorFaults,
        Self::Wakeups,
        Self::StorageReadBytes,
        Self::StorageWriteBytes,
        Self::LogicalReadBytes,
        Self::LogicalWriteBytes,
    ];
    fn label(self) -> &'static str {
        match self {
            Self::RssBytes => "rss",
            Self::FootprintBytes => "physical_footprint",
            Self::Threads => "threads",
            Self::Descriptors => "descriptors",
            Self::Handles => "handles",
            Self::Faults => "faults",
            Self::MinorFaults => "minor_faults",
            Self::MajorFaults => "pageins_or_major_faults",
            Self::Wakeups => "interrupt_wakeups",
            Self::StorageReadBytes => "storage_read",
            Self::StorageWriteBytes => "storage_write",
            Self::LogicalReadBytes => "logical_read",
            Self::LogicalWriteBytes => "logical_write",
            Self::HostMemoryBytes => "host_memory",
            Self::HostAvailableBytes => "host_available",
            Self::HostSwapUsedBytes => "host_swap_used",
            Self::HostPressure => "host_pressure",
            Self::LogicalCpus => "logical_cpus",
            Self::QuotaCores => "quota_cores",
        }
    }
    fn unit(self) -> &'static str {
        match self {
            Self::RssBytes
            | Self::FootprintBytes
            | Self::StorageReadBytes
            | Self::StorageWriteBytes
            | Self::LogicalReadBytes
            | Self::LogicalWriteBytes
            | Self::HostMemoryBytes
            | Self::HostAvailableBytes
            | Self::HostSwapUsedBytes => "By",
            Self::HostPressure => "source_defined",
            Self::QuotaCores => "{core}",
            _ => "{count}",
        }
    }
    fn temporality(self) -> &'static str {
        match self {
            Self::Faults
            | Self::MinorFaults
            | Self::MajorFaults
            | Self::Wakeups
            | Self::StorageReadBytes
            | Self::StorageWriteBytes
            | Self::LogicalReadBytes
            | Self::LogicalWriteBytes => "os_lifetime_snapshot",
            _ => "instantaneous",
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    Observed,
    Unsupported,
    Unavailable,
    Overflow,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Value {
    pub field: Field,
    pub value: Option<f64>,
    pub status: SourceStatus,
    pub source: String,
    pub unit: String,
    pub scope: String,
    pub temporality: String,
}
impl Value {
    fn missing(field: Field, status: SourceStatus, source: &str) -> Self {
        Self {
            field,
            value: None,
            status,
            source: source.into(),
            unit: field.unit().into(),
            scope: if field as usize >= 13 {
                "host_or_container"
            } else {
                "process"
            }
            .into(),
            temporality: field.temporality().into(),
        }
    }
    fn observed(field: Field, value: f64, source: &str) -> Self {
        Self {
            value: Some(value),
            ..Self::missing(field, SourceStatus::Observed, source)
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawUsage {
    pub main_module_id: Option<String>,
    pub start_identity: String,
    pub user_ns: u64,
    pub system_ns: u64,
    pub cpu_source: String,
    pub values: Vec<Value>,
}
impl RawUsage {
    fn new(start_identity: String, user_ns: u64, system_ns: u64, source: &str) -> Self {
        Self {
            main_module_id: None,
            start_identity,
            user_ns,
            system_ns,
            cpu_source: source.into(),
            values: Field::PROCESS
                .into_iter()
                .map(|f| Value::missing(f, SourceStatus::Unsupported, "none"))
                .collect(),
        }
    }
    fn set(&mut self, field: Field, value: f64, source: &str) {
        self.values[field as usize] = Value::observed(field, value, source);
    }
    fn unavailable(&mut self, field: Field, source: &str) {
        self.values[field as usize] = Value::missing(field, SourceStatus::Unavailable, source);
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CpuStatus {
    First,
    Observed,
    Reset,
    Gap,
    SuspendOrClockDiscontinuity,
    ExitedOrReused,
    Unavailable,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessSample {
    pub pid: u32,
    pub sampled_unix_ms: u64,
    pub sampled_monotonic_ms: u64,
    pub boot_ns: Option<u64>,
    pub raw: Option<RawUsage>,
    pub cpu_status: CpuStatus,
    pub elapsed_ns: Option<u64>,
    pub delta_user_ns: Option<u64>,
    pub delta_system_ns: Option<u64>,
    pub cpu_cores: Option<f64>,
    pub machine_fraction: Option<f64>,
    pub quota_fraction: Option<f64>,
}
fn derive_cpu(previous: Option<&ProcessSample>, current: &mut ProcessSample, period: Duration) {
    let Some(raw) = current.raw.as_ref() else {
        return;
    };
    let Some(previous) = previous else { return };
    let Some(old) = previous.raw.as_ref() else {
        return;
    };
    if raw.start_identity != old.start_identity {
        current.cpu_status = CpuStatus::ExitedOrReused;
        return;
    }
    let elapsed = current
        .sampled_monotonic_ms
        .saturating_sub(previous.sampled_monotonic_ms)
        .saturating_mul(1_000_000);
    current.elapsed_ns = Some(elapsed);
    let boot = current
        .boot_ns
        .zip(previous.boot_ns)
        .and_then(|(a, b)| a.checked_sub(b));
    if current
        .boot_ns
        .zip(previous.boot_ns)
        .is_some_and(|(a, b)| a < b)
        || boot.is_some_and(|b| b.abs_diff(elapsed) > 1_000_000_000)
    {
        current.cpu_status = CpuStatus::SuspendOrClockDiscontinuity;
        return;
    }
    let Some((user, system)) = raw
        .user_ns
        .checked_sub(old.user_ns)
        .zip(raw.system_ns.checked_sub(old.system_ns))
    else {
        current.cpu_status = CpuStatus::Reset;
        return;
    };
    current.delta_user_ns = Some(user);
    current.delta_system_ns = Some(system);
    if elapsed == 0 || elapsed > period.as_nanos().min(u64::MAX as u128) as u64 * 3 {
        current.cpu_status = CpuStatus::Gap;
        return;
    }
    current.cpu_status = CpuStatus::Observed;
    current.cpu_cores = Some((user as f64 + system as f64) / elapsed as f64);
}
pub fn process_start_identity(pid: u32) -> std::io::Result<String> {
    platform::start(pid).map_err(normalize_identity_error)
}
fn normalize_identity_error(error: std::io::Error) -> std::io::Error {
    #[cfg(unix)]
    if error.raw_os_error() == Some(libc::ESRCH) {
        return std::io::Error::new(std::io::ErrorKind::NotFound, "process_gone");
    }
    error
}
#[cfg(all(test, unix))]
#[test]
fn identity_error_keeps_unknown_and_permission_but_normalizes_process_gone() {
    use std::io::{Error, ErrorKind};
    assert_eq!(
        normalize_identity_error(Error::from_raw_os_error(libc::ESRCH)).kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        normalize_identity_error(Error::from_raw_os_error(libc::EACCES)).kind(),
        ErrorKind::PermissionDenied
    );
    assert_eq!(
        normalize_identity_error(Error::other("unknown")).kind(),
        ErrorKind::Other
    );
}
pub fn read_process(pid: u32) -> std::io::Result<RawUsage> {
    platform::sample(pid)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChildRole {
    CliAgent,
    Mcp,
    Shell,
    Browser,
    DesktopHelper,
    Profiler,
    PlatformHelper,
}
impl ChildRole {
    fn label(self) -> &'static str {
        match self {
            Self::CliAgent => "cli_agent",
            Self::Mcp => "mcp",
            Self::Shell => "shell",
            Self::Browser => "browser",
            Self::DesktopHelper => "desktop_helper",
            Self::Profiler => "profiler",
            Self::PlatformHelper => "platform_helper",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildSnapshot {
    pub registered_in_generation: u64,
    pub work: Option<crate::work::WorkContext>,
    pub generation: u64,
    pub owner_generation: Option<u64>,
    pub parent_run: String,
    pub role: ChildRole,
    pub pid: u32,
    pub start_identity: Option<String>,
    pub spawned_monotonic_ms: u64,
    #[serde(default)]
    pub ownership_ended_monotonic_ms: Option<u64>,
    /// Time of the owner's wait/try_wait observation; None for unknown Drop.
    pub exited_monotonic_ms: Option<u64>,
    pub outcome: String,
    pub final_usage: String,
    pub sample: Option<ProcessSample>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub child_coverage: String,
    pub schema_version: u32,
    pub consent_generation: u64,
    pub snapshot_monotonic_ms: u64,
    pub last_success_monotonic_ms: Option<u64>,
    pub last_success_unix_ms: Option<u64>,
    pub stale: bool,
    pub period_ms: u64,
    pub failed_samples: u64,
    pub skipped_samples: u64,
    pub child_overflow: u64,
    pub child_history_overwritten: u64,
    pub self_process: Option<ProcessSample>,
    pub host: Vec<Value>,
    pub children: Vec<ChildSnapshot>,
}
struct RegisteredChild {
    snapshot: ChildSnapshot,
    live: Option<crate::live::WorkGuard>,
}
struct Store {
    epoch: u64,
    period: Duration,
    last_success: Option<u64>,
    last_success_unix_ms: Option<u64>,
    self_process: Option<ProcessSample>,
    host: Vec<Value>,
    children: BTreeMap<u64, RegisteredChild>,
    exited: std::collections::VecDeque<ChildSnapshot>,
    next: u64,
    failed: u64,
    skipped: u64,
    overflow: u64,
    overwritten: u64,
}
impl Store {
    fn new() -> Self {
        Self {
            epoch: 0,
            period: Duration::from_secs(5),
            last_success: None,
            last_success_unix_ms: None,
            self_process: None,
            host: Vec::new(),
            children: BTreeMap::new(),
            exited: Default::default(),
            next: 1,
            failed: 0,
            skipped: 0,
            overflow: 0,
            overwritten: 0,
        }
    }
    fn discard(&mut self, epoch: u64) {
        self.epoch = epoch;
        self.last_success = None;
        self.last_success_unix_ms = None;
        self.self_process = None;
        self.host.clear();
        // Keep bounded ownership identity, never prior-epoch observations.
        // Disabled registration still creates no new OS reads/records.
        for child in self.children.values_mut() {
            child.snapshot.sample = None;
            child.snapshot.work = None;
            child.live = None;
        }
        self.exited.clear();
        self.failed = 0;
        self.skipped = 0;
        self.overflow = 0;
        self.overwritten = 0;
    }
}
static STORE: OnceLock<Mutex<Store>> = OnceLock::new();
fn store() -> &'static Mutex<Store> {
    STORE.get_or_init(|| Mutex::new(Store::new()))
}
fn lock() -> std::sync::MutexGuard<'static, Store> {
    store()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
pub(crate) fn discard(epoch: u64) {
    if STORE.get().is_some() {
        lock().discard(epoch);
    }
}
pub fn resource_snapshot() -> Option<ResourceSnapshot> {
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return None;
    }
    let state = lock();
    if state.epoch != epoch {
        return None;
    }
    let now = monotonic_ms();
    Some(ResourceSnapshot {
        child_coverage: "known active ownership identities survive epoch changes; samples/work lineage do not; births while disabled and unregistered foreign launches are not a census; final wait-rusage unavailable".into(),
        schema_version: 1,
        consent_generation: epoch,
        snapshot_monotonic_ms: now,
        last_success_monotonic_ms: state.last_success,
        last_success_unix_ms: state.last_success_unix_ms,
        stale: state
            .last_success
            .is_none_or(|last| now.saturating_sub(last) > state.period.as_millis() as u64 * 3),
        period_ms: state.period.as_millis() as u64,
        failed_samples: state.failed,
        skipped_samples: state.skipped,
        child_overflow: state.overflow,
        child_history_overwritten: state.overwritten,
        self_process: state.self_process.clone(),
        host: state.host.clone(),
        children: state
            .children
            .values().map(|c| &c.snapshot)
            .chain(state.exited.iter())
            .cloned()
            .collect(),
    })
}
/// Registration observes ownership only: it never waits, signals, or reaps.
/// Drop records incomplete owner coverage; only explicit owner exit is terminal.
#[derive(Debug)]
pub struct ChildRegistration {
    generation: Option<u64>,
}
impl ChildRegistration {
    pub fn register(pid: Option<u32>, role: ChildRole, owner_generation: Option<u64>) -> Self {
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        let result = Self { generation: None };
        if !enabled {
            return result;
        }
        let Some(pid) = pid else { return result };
        let start = platform::start(pid).ok();
        Self::register_known(pid, role, owner_generation, start, epoch)
    }
    fn register_known(
        pid: u32,
        role: ChildRole,
        owner_generation: Option<u64>,
        start: Option<String>,
        epoch: u64,
    ) -> Self {
        let mut result = Self { generation: None };
        let mut state = lock();
        if !crate::telemetry_sample_allowed(Some(epoch)) {
            return result;
        }
        if state.epoch != epoch {
            state.discard(epoch);
        }
        if state.children.len() >= MAX_CHILDREN || state.next == u64::MAX {
            state.overflow = state.overflow.saturating_add(1);
            return result;
        }
        let work = crate::work::WorkCarrier::child(
            crate::work::Boundary::Child,
            crate::work::WorkKind::Child,
            crate::work::Trigger::Lifecycle,
            crate::work::ExecutorRole::Child,
        )
        .context();
        let generation = state.next;
        state.next = state.next.saturating_add(1);
        state.children.insert(
            generation,
            RegisteredChild {
                snapshot: ChildSnapshot {
                    registered_in_generation: epoch,
                    work,
                    generation,
                    owner_generation,
                    parent_run: crate::identity::run_identity().run_id.clone(),
                    role,
                    pid,
                    start_identity: start,
                    spawned_monotonic_ms: monotonic_ms(),
                    ownership_ended_monotonic_ms: None,
                    exited_monotonic_ms: None,
                    outcome: "active".into(),
                    final_usage: "not_provided_by_owner".into(),
                    sample: None,
                },
                live: Some(crate::live::WorkGuard::start(work, true)),
            },
        );
        result.generation = Some(generation);
        drop(state);
        result
    }
    pub fn exited(&mut self, success: bool) {
        self.finish(if success {
            "exited_success"
        } else {
            "exited_failure"
        });
    }
    fn finish(&mut self, outcome: &str) {
        let Some(generation) = self.generation.take() else {
            return;
        };
        #[cfg(test)]
        FINISH_HOOK.with(|hook| {
            let callback = hook.borrow_mut().take();
            if let Some(callback) = callback {
                callback();
            }
        });
        let mut state = lock();
        // Consent may have changed while this owner waited for the store.
        // Reconcile only the generation observed UNDER this lock; never roll a
        // newer store back to a pre-lock snapshot. Own removal also runs off.
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        if state.epoch != epoch {
            state.discard(epoch);
        }
        let Some(mut owned) = state.children.remove(&generation) else {
            return;
        };
        if !enabled || !crate::telemetry_sample_allowed(Some(epoch)) {
            return;
        }
        if let Some(mut live) = owned.live.take() {
            live.finish(match outcome {
                "exited_success" => crate::work::Outcome::Completed,
                "exited_failure" => crate::work::Outcome::Failed,
                _ => crate::work::Outcome::Abandoned,
            });
        }
        let mut child = owned.snapshot;
        let ended = monotonic_ms();
        child.ownership_ended_monotonic_ms = Some(ended);
        child.exited_monotonic_ms =
            matches!(outcome, "exited_success" | "exited_failure").then_some(ended);
        child.outcome = outcome.into();
        child.final_usage = "incomplete_no_wait_rusage".into();
        if state.exited.len() == MAX_EXITED {
            state.exited.pop_front();
            state.overwritten = state.overwritten.saturating_add(1);
        }
        state.exited.push_back(child);
    }
}
#[cfg(test)]
thread_local! { static FINISH_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None); }

impl Drop for ChildRegistration {
    fn drop(&mut self) {
        self.finish("owner_dropped_exit_unknown");
    }
}

struct Metrics {
    freshness: Gauge<f64>,
    status: Gauge<u64>,
    cpu_available: Gauge<u64>,
    cpu_fraction: Gauge<f64>,
    value: Gauge<f64>,
    cpu: Gauge<f64>,
    cpu_time: Gauge<f64>,
    health: Counter<u64>,
    age: Gauge<f64>,
    capability: Gauge<u64>,
}
impl Metrics {
    fn new(meter: &Meter) -> Self {
        Self{freshness:meter.f64_gauge("pioneer.process.sample.timestamp").with_unit("s").with_description("Unix wall-clock timestamp of last attempt/success, scope=source process; backend can detect stopped sampler even if SDK repeats cached gauges; wall clock may step").build(),status:meter.u64_gauge("pioneer.process.resource.status").with_description("Closed numeric capability: 0 observed, 1 unsupported, 2 unavailable, 3 overflow; freshness separate").build(),cpu_available:meter.u64_gauge("pioneer.process.cpu.available").with_description("1 only for a fresh valid CPU delta; 0 means not recorded, NOT zero CPU").build(),cpu_fraction:meter.f64_gauge("pioneer.process.cpu.fraction").with_description("CPU cores divided by known machine logical cores or visible quota, named separately").with_unit("1").build(),value:meter.f64_gauge("pioneer.process.resource.snapshot").with_description("Selected OS values; field/unit/source/temporality identify meaning; missing values are not recorded").build(),cpu:meter.f64_gauge("pioneer.process.cpu.cores").with_unit("{core}").with_description("Delta CPU time / actual elapsed; one unit is one fully occupied logical core").build(),cpu_time:meter.f64_gauge("pioneer.process.cpu.time.snapshot").with_unit("s").with_description("OS lifetime user/system CPU time, snapshot gauge, not a Delta sum").build(),health:meter.u64_counter("pioneer.process.samples").with_unit("{sample}").build(),age:meter.f64_gauge("pioneer.process.sample.age").with_unit("s").build(),capability:meter.u64_gauge("pioneer.process.resource.available").with_description("Observed=1; unavailable/unsupported/overflow=0 with explicit status").build()}
    }
    fn emit(&self, s: &ResourceSnapshot) {
        if let Some(last) = s.last_success_unix_ms {
            self.freshness.record(
                last as f64 / 1000.0,
                &[KeyValue::new("milestone", "last_success")],
            );
        }
        if let Some(sample) = &s.self_process {
            self.freshness.record(
                sample.sampled_unix_ms as f64 / 1000.0,
                &[KeyValue::new("milestone", "last_attempt")],
            );
        }
        if let Some(last) = s.last_success_monotonic_ms {
            self.age.record(
                s.snapshot_monotonic_ms.saturating_sub(last) as f64 / 1000.0,
                &[],
            );
        }
        self.cpu_available.record(
            u64::from(
                !s.stale
                    && s.self_process
                        .as_ref()
                        .is_some_and(|sample| sample.cpu_status == CpuStatus::Observed),
            ),
            &[KeyValue::new("scope", "self")],
        );
        if let Some(sample) = &s.self_process {
            if let Some(value) = sample.machine_fraction {
                self.cpu_fraction.record(
                    value,
                    &[KeyValue::new("denominator", "machine_logical_cpus")],
                );
            }
            if let Some(value) = sample.quota_fraction {
                self.cpu_fraction
                    .record(value, &[KeyValue::new("denominator", "visible_cpu_quota")]);
            }
            if let Some(raw) = &sample.raw {
                for (mode, ns) in [("user", raw.user_ns), ("system", raw.system_ns)] {
                    self.cpu_time
                        .record(ns as f64 / 1e9, &[KeyValue::new("mode", mode)]);
                }
                self.values(&raw.values, "self");
            } else {
                for field in Field::PROCESS {
                    self.status.record(
                        2,
                        &[
                            KeyValue::new("field", field.label()),
                            KeyValue::new("scope", "self"),
                        ],
                    );
                    self.capability.record(
                        0,
                        &[
                            KeyValue::new("field", field.label()),
                            KeyValue::new("scope", "self"),
                        ],
                    );
                }
            }
            if let Some(cores) = sample.cpu_cores {
                self.cpu.record(cores, &[KeyValue::new("scope", "self")]);
            }
        }
        self.values(&s.host, "host_or_container");
        // Do not create per-PID metric series. Each child remains individually visible
        // in bounded local detail; aggregate means sampled children only, not tree RSS.
        for role in [
            ChildRole::CliAgent,
            ChildRole::Mcp,
            ChildRole::Shell,
            ChildRole::Browser,
            ChildRole::DesktopHelper,
            ChildRole::Profiler,
            ChildRole::PlatformHelper,
        ] {
            let mut count = 0;
            let mut cpu = 0.0;
            for c in &s.children {
                if c.role == role
                    && c.ownership_ended_monotonic_ms.is_none()
                    && c.exited_monotonic_ms.is_none()
                {
                    if let Some(v) = c.sample.as_ref().and_then(|s| s.cpu_cores) {
                        count += 1;
                        cpu += v;
                    }
                }
            }
            self.cpu_available.record(
                u64::from(count > 0),
                &[
                    KeyValue::new("scope", "registered_children"),
                    KeyValue::new("role", role.label()),
                ],
            );
            if count > 0 {
                self.cpu.record(
                    cpu,
                    &[
                        KeyValue::new("scope", "registered_children"),
                        KeyValue::new("role", role.label()),
                    ],
                );
            }
        }
    }
    fn values(&self, values: &[Value], scope: &'static str) {
        for v in values {
            let attrs = [
                KeyValue::new("field", v.field.label()),
                KeyValue::new("source", v.source.clone()),
                KeyValue::new("unit", v.unit.clone()),
                KeyValue::new("scope", scope),
                KeyValue::new("temporality", v.temporality.clone()),
            ];
            if let Some(n) = v.value {
                self.value.record(n, &attrs);
            }
            self.status.record(
                match v.status {
                    SourceStatus::Observed => 0,
                    SourceStatus::Unsupported => 1,
                    SourceStatus::Unavailable => 2,
                    SourceStatus::Overflow => 3,
                },
                &[
                    KeyValue::new("field", v.field.label()),
                    KeyValue::new("scope", scope),
                ],
            );
            self.capability.record(
                u64::from(v.status == SourceStatus::Observed),
                &[
                    KeyValue::new("field", v.field.label()),
                    KeyValue::new("scope", scope),
                ],
            );
        }
    }
}
struct Stop {
    stopped: Mutex<bool>,
    wake: Condvar,
}
pub(crate) struct ResourceSampler {
    stop: Arc<Stop>,
    thread: Mutex<Option<JoinHandle<()>>>,
}
impl ResourceSampler {
    pub(crate) fn start(meter: &Meter) -> std::io::Result<Self> {
        let stop = Arc::new(Stop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let child_stop = stop.clone();
        let metrics = Metrics::new(meter);
        // Bounded config without expanding application settings. Absent/invalid is
        // five seconds; documented clamp protects accidental rapid polling.
        let secs = std::env::var("PIONEER_RESOURCE_SAMPLE_SECONDS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5)
            .clamp(1, 60);
        let period = Duration::from_secs(secs);
        lock().period = period;
        let thread = std::thread::Builder::new()
            .name("pioneer-observability".into())
            .spawn(move || {
                let mut expected = Instant::now();
                loop {
                    if *child_stop
                        .stopped
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                    {
                        break;
                    }
                    let (enabled, epoch) = crate::telemetry_consent_snapshot();
                    if enabled {
                        let missed = Instant::now()
                            .saturating_duration_since(expected)
                            .as_millis()
                            > period.as_millis();
                        crate::runtime::tick(epoch, period);
                        tick(epoch, period, missed, &metrics);
                    }
                    expected = Instant::now() + period;
                    let guard = child_stop
                        .stopped
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if *guard {
                        break;
                    }
                    let (guard, _) = child_stop
                        .wake
                        .wait_timeout(guard, period)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if *guard {
                        break;
                    }
                }
            })?;
        Ok(Self {
            stop,
            thread: Mutex::new(Some(thread)),
        })
    }
    pub(crate) fn shutdown(&self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = thread.join();
        }
    }
}
impl Drop for ResourceSampler {
    fn drop(&mut self) {
        self.shutdown();
    }
}
fn tick(epoch: u64, period: Duration, missed: bool, metrics: &Metrics) {
    let now = monotonic_ms();
    let wall = unix_ms();
    let boot = platform::boot_ns();
    let raw = platform::sample(std::process::id());
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        return;
    }
    let host = platform::host();
    let children: Vec<_> = {
        let state = lock();
        state
            .children
            .values()
            .map(|c| {
                (
                    c.snapshot.generation,
                    c.snapshot.pid,
                    c.snapshot.start_identity.clone(),
                )
            })
            .collect()
    };
    let samples: Vec<_> = children
        .into_iter()
        .take_while(|_| crate::telemetry_sample_allowed(Some(epoch)))
        .map(|(g, pid, start)| {
            let at = monotonic_ms();
            let wall = unix_ms();
            let boot = platform::boot_ns();
            (g, pid, start, at, wall, boot, platform::sample(pid))
        })
        .collect();
    {
        let mut state = lock();
        if !crate::telemetry_sample_allowed(Some(epoch)) {
            return;
        }
        if state.epoch != epoch {
            state.discard(epoch);
        }
        if missed {
            state.skipped = state.skipped.saturating_add(1);
        }
        let succeeded = raw.is_ok();
        if !succeeded {
            state.failed = state.failed.saturating_add(1);
        }
        let mut current = ProcessSample {
            pid: std::process::id(),
            sampled_unix_ms: wall,
            sampled_monotonic_ms: now,
            boot_ns: boot,
            raw: raw.ok(),
            cpu_status: if succeeded {
                CpuStatus::First
            } else {
                CpuStatus::Unavailable
            },
            elapsed_ns: None,
            delta_user_ns: None,
            delta_system_ns: None,
            cpu_cores: None,
            machine_fraction: None,
            quota_fraction: None,
        };
        derive_cpu(state.self_process.as_ref(), &mut current, period);
        for (field, dest) in [
            (Field::LogicalCpus, &mut current.machine_fraction),
            (Field::QuotaCores, &mut current.quota_fraction),
        ] {
            *dest = current
                .cpu_cores
                .zip(host.iter().find(|v| v.field == field).and_then(|v| v.value))
                .filter(|(_, v)| *v > 0.0)
                .map(|(c, v)| c / v);
        }
        state.self_process = Some(current);
        state.host = host;
        if succeeded {
            state.last_success = Some(now);
            state.last_success_unix_ms = Some(wall);
        }
        for (generation, pid, _start, child_at, child_wall, child_boot, result) in samples {
            if let Some(owned) = state.children.get_mut(&generation) {
                let status = match &result {
                    Ok(_) => CpuStatus::First,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        CpuStatus::ExitedOrReused
                    }
                    Err(_) => CpuStatus::Unavailable,
                };
                let sample = ProcessSample {
                    pid,
                    sampled_unix_ms: child_wall,
                    sampled_monotonic_ms: child_at,
                    boot_ns: child_boot,
                    raw: result.ok(),
                    cpu_status: status,
                    elapsed_ns: None,
                    delta_user_ns: None,
                    delta_system_ns: None,
                    cpu_cores: None,
                    machine_fraction: None,
                    quota_fraction: None,
                };
                observe_child_sample(owned, epoch, sample, period);
            }
        }
    }
    // Serialize/export never runs while retaining registry lock or DB capacity.
    if let Some(snapshot) = resource_snapshot() {
        if snapshot.consent_generation == epoch && crate::telemetry_sample_allowed(Some(epoch)) {
            crate::record_in_epoch(epoch, || {
                metrics.health.add(
                    1,
                    &[KeyValue::new(
                        "outcome",
                        if snapshot
                            .self_process
                            .as_ref()
                            .is_some_and(|s| s.raw.is_some())
                        {
                            "sampled"
                        } else {
                            "failed"
                        },
                    )],
                );
                metrics.emit(&snapshot);
                emit_child_detail(&snapshot);
            });
            crate::live::sample_resources(snapshot);
            crate::record_in_epoch(epoch, crate::live::emit_current);
        }
    }
}

fn observe_child_sample(
    owned: &mut RegisteredChild,
    epoch: u64,
    mut sample: ProcessSample,
    period: Duration,
) {
    if !crate::telemetry_sample_allowed(Some(epoch)) {
        return;
    }
    if owned
        .snapshot
        .work
        .as_ref()
        .is_none_or(|c| c.consent_generation != epoch)
    {
        let work = crate::work::WorkCarrier::default()
            .received(
                crate::work::Boundary::Child,
                crate::work::WorkKind::Child,
                crate::work::ExecutorRole::Child,
            )
            .context();
        owned.snapshot.work = work;
        owned.live = Some(crate::live::WorkGuard::start(work, true));
    }
    if let Some(raw) = &sample.raw {
        if owned.snapshot.start_identity.as_ref() != Some(&raw.start_identity) {
            sample.raw = None;
            sample.cpu_status = if owned.snapshot.start_identity.is_some() {
                CpuStatus::ExitedOrReused
            } else {
                CpuStatus::Unavailable
            };
        }
    }
    derive_cpu(owned.snapshot.sample.as_ref(), &mut sample, period);
    owned.snapshot.sample = Some(sample);
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) fn sample(at: u64, user: u64, system: u64) -> ProcessSample {
        ProcessSample {
            pid: 42,
            sampled_unix_ms: 1000 + at,
            sampled_monotonic_ms: at,
            boot_ns: Some(at * 1_000_000),
            raw: Some(RawUsage::new("start-1".into(), user, system, "fixture")),
            cpu_status: CpuStatus::First,
            elapsed_ns: None,
            delta_user_ns: None,
            delta_system_ns: None,
            cpu_cores: None,
            machine_fraction: None,
            quota_fraction: None,
        }
    }
    #[test]
    fn cpu_contract_and_discontinuities() {
        let previous = sample(1000, 1_000_000_000, 500_000_000);
        let mut next = sample(3000, 2_500_000_000, 1_000_000_000);
        derive_cpu(Some(&previous), &mut next, Duration::from_secs(5));
        assert_eq!(next.cpu_cores, Some(1.0));
        assert_eq!(next.elapsed_ns, Some(2_000_000_000));
        assert_eq!(next.delta_user_ns, Some(1_500_000_000));
        let mut reset = sample(4000, 0, 0);
        derive_cpu(Some(&next), &mut reset, Duration::from_secs(5));
        assert_eq!(reset.cpu_status, CpuStatus::Reset);
        assert_eq!(reset.cpu_cores, None);
        let mut gap = sample(100000, 3_000_000_000, 1_000_000_000);
        derive_cpu(Some(&next), &mut gap, Duration::from_secs(5));
        assert_eq!(gap.cpu_status, CpuStatus::Gap);
        let mut suspend = sample(4000, 3_000_000_000, 1_000_000_000);
        suspend.boot_ns = Some(100_000_000_000);
        derive_cpu(Some(&next), &mut suspend, Duration::from_secs(5));
        assert_eq!(suspend.cpu_status, CpuStatus::SuspendOrClockDiscontinuity);
    }
    #[test]
    fn pid_reuse_is_not_a_delta() {
        let old = sample(1000, 1, 1);
        let mut new = sample(2000, 10, 10);
        new.raw.as_mut().unwrap().start_identity = "start-2".into();
        derive_cpu(Some(&old), &mut new, Duration::from_secs(5));
        assert_eq!(new.cpu_status, CpuStatus::ExitedOrReused);
        assert!(new.cpu_cores.is_none());
    }
    #[test]
    fn missing_is_not_observed_zero() {
        let missing = Value::missing(Field::FootprintBytes, SourceStatus::Unsupported, "none");
        let zero = Value::observed(Field::FootprintBytes, 0.0, "fixture");
        assert!(missing.value.is_none());
        assert_eq!(zero.value, Some(0.0));
        assert_ne!(missing.status, zero.status);
    }
    #[test]
    fn active_owner_survives_consent_but_usage_and_lineage_restart() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let epoch = crate::telemetry_consent_snapshot().1;
        let mut owner = ChildRegistration::register_known(
            42,
            ChildRole::Mcp,
            None,
            Some("start-1".into()),
            epoch,
        );
        let generation = owner.generation.unwrap();
        lock()
            .children
            .get_mut(&generation)
            .unwrap()
            .snapshot
            .sample = Some(sample(1000, 1_000_000_000, 500_000_000));
        crate::set_telemetry_enabled(false);
        assert!(resource_snapshot().is_none());
        {
            let state = lock();
            let child = &state.children[&generation];
            assert!(child.snapshot.sample.is_none());
            assert!(child.snapshot.work.is_none());
            assert!(child.live.is_none());
        }
        crate::set_telemetry_enabled(true);
        let next = crate::telemetry_consent_snapshot().1;
        {
            let mut state = lock();
            state.discard(next);
            let child = state.children.get_mut(&generation).unwrap();
            observe_child_sample(
                child,
                next,
                sample(3000, 2_500_000_000, 1_000_000_000),
                Duration::from_secs(5),
            );
            let first = child.snapshot.sample.as_ref().unwrap();
            assert_eq!(first.cpu_status, CpuStatus::First);
            assert_eq!(first.cpu_cores, None);
            assert_eq!(first.elapsed_ns, None);
            let context = child.snapshot.work.unwrap();
            assert_eq!(context.consent_generation, next);
            assert_eq!(context.parent, None);
            assert_eq!(context.links, [None; 4]);
            observe_child_sample(
                child,
                next,
                sample(5000, 4_000_000_000, 1_500_000_000),
                Duration::from_secs(5),
            );
            assert_eq!(child.snapshot.sample.as_ref().unwrap().cpu_cores, Some(1.0));
        }
        owner.exited(true);
        let snapshot = resource_snapshot().unwrap();
        assert_eq!(snapshot.children[0].registered_in_generation, epoch);
        assert_eq!(snapshot.consent_generation, next);
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn epoch_discards_active_and_terminal_detail() {
        let mut s = Store::new();
        s.self_process = Some(sample(1000, 0, 0));
        s.last_success = Some(1000);
        s.failed = 12;
        s.discard(7);
        assert_eq!(s.epoch, 7);
        assert!(s.self_process.is_none());
        assert_eq!(s.last_success, None);
        assert_eq!(s.failed, 0);
    }
}

// At most eight child records per sample, round-robin. Parent run/build are
// inherited from the SAME OTLP Resource. No raw command/paths/payloads.
fn emit_child_detail(snapshot: &ResourceSnapshot) {
    use opentelemetry::trace::{Span, Tracer};
    static CURSOR: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let Some(owner) = crate::telemetry::state() else {
        return;
    };
    let count = snapshot.children.len();
    if count == 0 {
        return;
    }
    let start = CURSOR.fetch_add(8, std::sync::atomic::Ordering::Relaxed) % count;
    for child in snapshot
        .children
        .iter()
        .cycle()
        .skip(start)
        .take(count.min(8))
    {
        if !crate::telemetry_sample_allowed(Some(snapshot.consent_generation)) {
            return;
        }
        let mut span = owner.tracer.start("process.child.sample");
        span.set_attribute(KeyValue::new(
            "pioneer.consent.generation",
            snapshot.consent_generation.to_string(),
        ));
        span.set_attributes([
            KeyValue::new("pioneer.child.generation", child.generation as i64),
            KeyValue::new("process.pid", child.pid as i64),
            KeyValue::new("pioneer.child.role", child.role.label()),
            KeyValue::new(
                "pioneer.child.start_identity",
                child
                    .start_identity
                    .clone()
                    .unwrap_or_else(|| "unavailable".into()),
            ),
            KeyValue::new("pioneer.child.outcome", child.outcome.clone()),
            KeyValue::new("pioneer.child.final_usage", child.final_usage.clone()),
            KeyValue::new(
                "pioneer.child.spawned_monotonic_ms",
                child.spawned_monotonic_ms as f64,
            ),
            KeyValue::new(
                "pioneer.sample.monotonic_ms",
                snapshot.snapshot_monotonic_ms as i64,
            ),
        ]);
        if let Some(ended) = child.ownership_ended_monotonic_ms {
            span.set_attribute(KeyValue::new(
                "pioneer.child.ownership_ended_monotonic_ms",
                ended as f64,
            ));
        }
        if let Some(exited) = child.exited_monotonic_ms {
            span.set_attribute(KeyValue::new(
                "pioneer.child.exited_monotonic_ms",
                exited as f64,
            ));
        }
        if let Some(sample) = &child.sample {
            span.set_attribute(KeyValue::new(
                "process.cpu.status",
                format!("{:?}", sample.cpu_status),
            ));
            for (field, value) in [
                ("process.cpu.interval_ns", sample.elapsed_ns),
                ("process.cpu.delta_user_ns", sample.delta_user_ns),
                ("process.cpu.delta_system_ns", sample.delta_system_ns),
            ] {
                if let Some(value) = value {
                    span.set_attribute(KeyValue::new(field, value as f64));
                }
            }
            span.set_attribute(KeyValue::new(
                "pioneer.sample.unix_ms",
                sample.sampled_unix_ms as i64,
            ));
            if let Some(cpu) = sample.cpu_cores {
                span.set_attribute(KeyValue::new("process.cpu.cores", cpu));
            }
            if let Some(raw) = &sample.raw {
                span.set_attributes([
                    KeyValue::new("process.cpu.user_ns", raw.user_ns as f64),
                    KeyValue::new("process.cpu.system_ns", raw.system_ns as f64),
                ]);
                // Thirteen closed fields × at most five attributes + identity/CPU
                // stays below the SDK's128-attribute span budget. All sources and
                // units come from this adapter; no argv/path/payload is recorded.
                for value in raw.values.iter().take(Field::PROCESS.len()) {
                    let field = value.field.label();
                    span.set_attributes([
                        KeyValue::new(
                            format!("process.resource.{field}.status"),
                            match value.status {
                                SourceStatus::Observed => "observed",
                                SourceStatus::Unsupported => "unsupported",
                                SourceStatus::Unavailable => "unavailable",
                                SourceStatus::Overflow => "overflow",
                            },
                        ),
                        KeyValue::new(
                            format!("process.resource.{field}.source"),
                            value.source.clone(),
                        ),
                        KeyValue::new(format!("process.resource.{field}.unit"), value.unit.clone()),
                        KeyValue::new(
                            format!("process.resource.{field}.temporality"),
                            value.temporality.clone(),
                        ),
                    ]);
                    if let Some(number) = value.value {
                        span.set_attribute(KeyValue::new(
                            format!("process.resource.{field}.value"),
                            number,
                        ));
                    }
                }
            }
        }
        span.end();
    }
}

#[cfg(test)]
mod child_owner_tests {
    use super::*;
    #[test]
    fn finish_waiting_for_store_never_discards_a_new_consent_generation() {
        use std::{cell::RefCell, rc::Rc};
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let mut a = ChildRegistration::register_known(
            42,
            ChildRole::Shell,
            None,
            Some("old".into()),
            crate::telemetry_consent_snapshot().1,
        );
        let a_id = a.generation.unwrap();
        let retained = Rc::new(RefCell::new(None));
        let child = retained.clone();
        FINISH_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let before_lock = crate::telemetry_consent_snapshot();
                crate::set_telemetry_enabled(false);
                crate::set_telemetry_enabled(true);
                let epoch = crate::telemetry_consent_snapshot().1;
                assert_ne!(epoch, before_lock.1);
                let b = ChildRegistration::register_known(
                    42,
                    ChildRole::Shell,
                    None,
                    Some("new".into()),
                    epoch,
                );
                let mut history = ChildRegistration::register_known(
                    43,
                    ChildRole::Shell,
                    None,
                    Some("history".into()),
                    epoch,
                );
                history.exited(false);
                let mut state = lock();
                state.last_success = Some(123);
                state.last_success_unix_ms = Some(456);
                let owned = state.children.get_mut(&b.generation.unwrap()).unwrap();
                owned.snapshot.sample = Some(super::tests::sample(1000, 7, 9));
                drop(state);
                *child.borrow_mut() = Some(b);
            }))
        });
        a.exited(true);
        let mut b = retained.borrow_mut().take().unwrap();
        let (epoch, token) = {
            let state = lock();
            let owned = &state.children[&b.generation.unwrap()];
            assert_eq!(state.epoch, crate::telemetry_consent_snapshot().1);
            assert_eq!(
                (state.last_success, state.last_success_unix_ms),
                (Some(123), Some(456))
            );
            assert!(!state.children.contains_key(&a_id));
            assert_eq!(owned.snapshot.start_identity.as_deref(), Some("new"));
            assert_eq!(
                owned
                    .snapshot
                    .sample
                    .as_ref()
                    .unwrap()
                    .raw
                    .as_ref()
                    .unwrap()
                    .user_ns,
                7
            );
            assert!(owned.live.is_some());
            assert!(
                state
                    .exited
                    .iter()
                    .any(|c| c.pid == 43 && c.outcome == "exited_failure")
            );
            (state.epoch, owned.snapshot.work.unwrap().token)
        };
        let live = crate::live::snapshot().unwrap();
        assert_eq!(live.consent_generation, epoch);
        assert!(live.active.iter().any(|w| w.context.token == token));
        assert!(!live.journal.iter().any(|r| matches!(&r.payload, crate::live::JournalPayload::Work(w) if w.context.token == token && matches!(w.state, crate::live::WorkState::Terminal(_)))));
        b.exited(false);
        let history = resource_snapshot().unwrap();
        let b = history
            .children
            .iter()
            .find(|c| c.start_identity.as_deref() == Some("new"))
            .unwrap();
        assert_eq!(b.outcome, "exited_failure");
        assert!(b.exited_monotonic_ms.is_some());
        let mut disabled =
            ChildRegistration::register_known(44, ChildRole::Shell, None, None, epoch);
        let id = disabled.generation.unwrap();
        crate::set_telemetry_enabled(false);
        disabled.exited(true);
        assert!(!lock().children.contains_key(&id));
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn known_probe_exit_is_preserved_even_when_protocol_parsing_later_fails() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        for success in [true, false] {
            let mut owner = ChildRegistration::register_known(
                42,
                ChildRole::CliAgent,
                None,
                Some("fixture".into()),
                crate::telemetry_consent_snapshot().1,
            );
            let generation = owner.generation;
            owner.exited(success); // wait_with_output supplied this before protocol parsing
            let _protocol_result: Result<(), &str> = Err("malformed response");
            drop(owner);
            let snapshot = resource_snapshot().unwrap();
            let child = snapshot
                .children
                .iter()
                .find(|c| Some(c.generation) == generation)
                .unwrap();
            assert_eq!(
                child.outcome,
                if success {
                    "exited_success"
                } else {
                    "exited_failure"
                }
            );
            assert!(child.exited_monotonic_ms.is_some());
            assert_eq!(child.final_usage, "incomplete_no_wait_rusage");
        }
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn late_unregister_and_short_lived_children_preserve_each_generation() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let epoch = crate::telemetry_consent_snapshot().1;
        let mut first = ChildRegistration::register_known(
            42,
            ChildRole::Mcp,
            Some(1),
            Some("start-a".into()),
            epoch,
        );
        let mut second = ChildRegistration::register_known(
            42,
            ChildRole::Mcp,
            Some(2),
            Some("start-b".into()),
            epoch,
        );
        assert_ne!(first.generation, second.generation);
        first.exited(false);
        drop(first);
        let snap = resource_snapshot().unwrap();
        assert_eq!(snap.children.len(), 2);
        assert_eq!(
            snap.children
                .iter()
                .filter(|c| c.exited_monotonic_ms.is_none())
                .count(),
            1
        );
        let active = snap
            .children
            .iter()
            .find(|c| c.exited_monotonic_ms.is_none())
            .unwrap();
        assert_eq!(active.start_identity.as_deref(), Some("start-b"));
        assert!(active.sample.is_none());
        second.exited(true);
        let snap = resource_snapshot().unwrap();
        assert!(
            snap.children
                .iter()
                .all(|c| c.final_usage == "incomplete_no_wait_rusage")
        );
        assert!(snap.children.iter().all(|c| c.sample.is_none()));
        crate::set_telemetry_enabled(old);
    }
    #[test]
    fn child_limit_and_revoked_owner_do_not_touch_new_epoch() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        let epoch = crate::telemetry_consent_snapshot().1;
        let owners: Vec<_> = (0..67)
            .map(|pid| ChildRegistration::register_known(pid, ChildRole::Shell, None, None, epoch))
            .collect();
        let snap = resource_snapshot().unwrap();
        assert_eq!(snap.children.len(), 64);
        assert_eq!(snap.child_overflow, 3);
        crate::set_telemetry_enabled(false);
        crate::set_telemetry_enabled(true);
        drop(owners);
        let snapshot = resource_snapshot().unwrap();
        assert_eq!(snapshot.children.len(), 64);
        assert!(snapshot.children.iter().all(|c| c.sample.is_none()
            && c.work.is_none()
            && c.ownership_ended_monotonic_ms.is_some()
            && c.exited_monotonic_ms.is_none()
            && c.outcome == "owner_dropped_exit_unknown"));
        crate::set_telemetry_enabled(old);
    }
}

/// Existing owners with the default `Command::output` stdio contract use this
/// seam to retain PID/start identity while waiting. No command/args/output are
/// copied into observation. Callers with custom stdio keep their own spawn owner.
pub fn captured_output(
    command: &mut std::process::Command,
    role: ChildRole,
) -> std::io::Result<std::process::Output> {
    use std::process::Stdio;
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut owner = ChildRegistration::register(Some(child.id()), role, None);
    let output = child.wait_with_output()?;
    owner.exited(output.status.success());
    Ok(output)
}
