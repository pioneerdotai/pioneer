use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context as TracingContext, Layer};
use tracing_subscriber::registry::LookupSpan;

static NATIVE_LIFECYCLE_DEPTH_VALUES: [AtomicU64; 5] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static GATEWAY_ACTIVE_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static GATEWAY_DATABASE_SIZE_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum DatabaseRole {
    Reader,
    Writer,
}

impl DatabaseRole {
    pub const CARDINALITY: usize = 2;

    const fn as_str(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Writer => "writer",
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum DatabaseOperation {
    Select,
    Insert,
    Update,
    Delete,
    Replace,
    Transaction,
    Schema,
    Pragma,
    Other,
}

#[derive(Clone, Copy, Debug)]
pub struct DatabaseOperationMetric {
    pub role: DatabaseRole,
    pub operation: DatabaseOperation,
    pub workload: crate::DatabaseWorkload,
    /// Stable, value-free SQL-shape fingerprint. Zero is the bounded overflow
    /// bucket and never represents a raw query or application identifier.
    pub query_fingerprint: u64,
    pub elapsed: Duration,
    pub failed: bool,
}

impl DatabaseOperation {
    pub const CARDINALITY: usize = 9;

    const fn as_str(self) -> &'static str {
        match self {
            Self::Select => "select",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Replace => "replace",
            Self::Transaction => "transaction",
            Self::Schema => "schema",
            Self::Pragma => "pragma",
            Self::Other => "other",
        }
    }

    const fn kind(self) -> &'static str {
        match self {
            Self::Select | Self::Pragma => "read",
            Self::Insert
            | Self::Update
            | Self::Delete
            | Self::Replace
            | Self::Transaction
            | Self::Schema => "write",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DatabasePoolSnapshot {
    pub size: u64,
    pub idle: u64,
}

/// One low-cardinality observation emitted by the Gateway's SQLite writer
/// executor. Values are selected from finite runtime enums; no
/// SQL, IDs, paths, or error text are accepted here.
#[derive(Clone, Copy, Debug)]
pub struct DatabaseAdmissionMetric {
    pub event: &'static str,
    pub class: &'static str,
    pub reason: &'static str,
    pub critical_queue: u64,
    pub interactive_queue: u64,
    pub maintenance_queue: u64,
    pub waited: Option<Duration>,
    pub held: Option<Duration>,
}

/// End-to-end reader observation, including maintenance admission and SQLx
/// pool acquisition. The class and outcome are selected from fixed enums in
/// pioneer-sqlite; no query or request data is accepted here.
#[derive(Clone, Copy, Debug)]
pub struct DatabaseReadMetric {
    pub class: &'static str,
    pub outcome: &'static str,
    pub elapsed: Duration,
}

/// Lifecycle of the one-permit maintenance-reader limiter.
#[derive(Clone, Copy, Debug)]
pub struct DatabaseReadAdmissionMetric {
    pub event: &'static str,
    pub class: &'static str,
    pub queue_depth: u64,
    pub active: u64,
    pub waited: Option<Duration>,
    pub held: Option<Duration>,
}

pub(crate) struct GatewayMetrics {
    pub(crate) meter: Meter,
    pub(crate) database_operations: Counter<u64>,
    pub(crate) database_operation_duration: Histogram<f64>,
    pub(crate) database_pool_acquire_duration: Histogram<f64>,
    pub(crate) database_read_operations: Counter<u64>,
    pub(crate) database_read_duration: Histogram<f64>,
    pub(crate) database_read_admission_events: Counter<u64>,
    pub(crate) database_read_admission_wait_duration: Histogram<f64>,
    pub(crate) database_read_admission_hold_duration: Histogram<f64>,
    pub(crate) database_read_admission_queue_depth: Histogram<u64>,
    pub(crate) database_read_admission_active: Histogram<u64>,
    pub(crate) database_admission_events: Counter<u64>,
    pub(crate) database_admission_wait_duration: Histogram<f64>,
    pub(crate) database_admission_quantum_duration: Histogram<f64>,
    pub(crate) database_admission_queue_depth: Histogram<u64>,
    pub(crate) database_workload_operations: Counter<u64>,
    pub(crate) database_workload_duration: Histogram<f64>,
    pub(crate) database_workload_query_count: Histogram<u64>,
    pub(crate) database_workload_query_duration: Histogram<f64>,
    pub(crate) database_workload_anomalies: Counter<u64>,
    pub(crate) provider_warmup_duration: Histogram<f64>,
    pub(crate) provider_warmup_stage_duration: Histogram<f64>,
    pub(crate) provider_warmup_failures: Counter<u64>,
    pub(crate) provider_readiness_checks: Counter<u64>,
    pub(crate) native_lifecycle_events: Counter<u64>,
    pub(crate) native_lifecycle_readiness_checks: Counter<u64>,
    pub(crate) native_lifecycle_duration: Histogram<f64>,
    pub(crate) gateway_operation_duration: Histogram<f64>,
    pub(crate) gateway_operation_stage_duration: Histogram<f64>,
    pub(crate) gateway_operation_items: Histogram<u64>,
    pub(crate) gateway_operation_failures: Counter<u64>,
    pub(crate) patch_operations: Counter<u64>,
    pub(crate) patch_operation_duration: Histogram<f64>,
    pub(crate) patch_committed_files: Counter<u64>,
    pub(crate) patch_committed_hunks: Counter<u64>,
    pub(crate) patch_committed_bytes: Counter<u64>,
    pub(crate) patch_fallbacks: Counter<u64>,
}

impl GatewayMetrics {
    pub(crate) fn new(meter: Meter) -> Self {
        let database_operations = meter
            .u64_counter("pioneer.gateway.db.operations")
            .with_description("Number of SQLite operations executed by the gateway")
            .with_unit("{operation}")
            .build();
        let database_operation_duration = meter
            .f64_histogram("pioneer.gateway.db.operation.duration")
            .with_description("SQLite operation execution duration, excluding pool wait time")
            .with_unit("ms")
            .with_boundaries(vec![
                0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0,
                2_500.0, 5_000.0,
            ])
            .build();
        let database_pool_acquire_duration = meter
            .f64_histogram("pioneer.gateway.db.pool.acquire.duration")
            .with_description("Time to acquire a SQLite connection from the gateway pool")
            .with_unit("ms")
            .with_boundaries(vec![
                0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
                1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
            ])
            .build();
        let database_read_operations = meter
            .u64_counter("pioneer.gateway.db.read.operations")
            .with_description("Number of typed SQLite reader operations by scheduling class")
            .with_unit("{operation}")
            .build();
        let database_read_duration = meter
            .f64_histogram("pioneer.gateway.db.read.duration")
            .with_description(
                "End-to-end typed SQLite reader duration including admission and pool wait",
            )
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let database_read_admission_events = meter
            .u64_counter("pioneer.gateway.db.read.admission.events")
            .with_description("Maintenance-reader limiter lifecycle events")
            .with_unit("{event}")
            .build();
        let database_read_admission_wait_duration = meter
            .f64_histogram("pioneer.gateway.db.read.admission.wait.duration")
            .with_description("Time a maintenance read waits for the reader limiter")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let database_read_admission_hold_duration = meter
            .f64_histogram("pioneer.gateway.db.read.admission.hold.duration")
            .with_description("Time a maintenance read retains the reader limiter permit")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let database_read_admission_queue_depth = meter
            .u64_histogram("pioneer.gateway.db.read.admission.queue.depth")
            .with_description("Maintenance-reader queue depth at limiter lifecycle events")
            .with_unit("{request}")
            .with_boundaries(vec![
                0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1_024.0,
            ])
            .build();
        let database_read_admission_active = meter
            .u64_histogram("pioneer.gateway.db.read.admission.active")
            .with_description("Active maintenance-reader permits at limiter lifecycle events")
            .with_unit("{request}")
            .with_boundaries(vec![0.0, 1.0])
            .build();
        let database_admission_events = meter
            .u64_counter("pioneer.gateway.db.admission.events")
            .with_description(
                "SQLite writer-executor admission events by class, event, and grant reason",
            )
            .with_unit("{event}")
            .build();
        let database_admission_wait_duration = meter
            .f64_histogram("pioneer.gateway.db.admission.wait.duration")
            .with_description("Time a SQLite write waits for writer-executor admission")
            .with_unit("ms")
            .with_boundaries(vec![
                0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
                1_000.0, 2_000.0, 5_000.0, 10_000.0, 30_000.0,
            ])
            .build();
        let database_admission_quantum_duration = meter
            .f64_histogram("pioneer.gateway.db.admission.quantum.duration")
            .with_description("Time one write retains the SQLite writer executor")
            .with_unit("ms")
            .with_boundaries(vec![
                0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
                1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
            ])
            .build();
        let database_admission_queue_depth = meter
            .u64_histogram("pioneer.gateway.db.admission.queue.depth")
            .with_description("Writer-executor queue depth at SQLite admission events")
            .with_unit("{request}")
            .with_boundaries(vec![
                0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1_024.0,
            ])
            .build();
        let database_workload_operations = meter
            .u64_counter("pioneer.gateway.db.workload.operations")
            .with_description("Logical Gateway database-capable work units by bounded owner")
            .with_unit("{operation}")
            .build();
        let database_workload_duration = meter
            .f64_histogram("pioneer.gateway.db.workload.duration")
            .with_description("End-to-end duration of a bounded Gateway database work unit")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let database_workload_query_count = meter
            .u64_histogram("pioneer.gateway.db.workload.query_count")
            .with_description("SQLite statement count within one bounded Gateway work unit")
            .with_unit("{query}")
            .with_boundaries(operation_item_boundaries())
            .build();
        let database_workload_query_duration = meter
            .f64_histogram("pioneer.gateway.db.workload.query_duration")
            .with_description("Cumulative SQLite execution time within one Gateway work unit")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let database_workload_anomalies = meter
            .u64_counter("pioneer.gateway.db.workload.anomalies")
            .with_description(
                "Database work units exceeding bounded amplification, latency or failure thresholds",
            )
            .with_unit("{anomaly}")
            .build();
        let provider_warmup_duration = meter
            .f64_histogram("pioneer.gateway.providers.warmup.duration")
            .with_description("End-to-end duration of Gateway-owned provider warm-up")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let provider_warmup_stage_duration = meter
            .f64_histogram("pioneer.gateway.providers.warmup.stage.duration")
            .with_description("Duration of a stable Gateway provider warm-up stage")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let provider_warmup_failures = meter
            .u64_counter("pioneer.gateway.providers.warmup.failures")
            .with_description("Number of failed Gateway provider warm-ups")
            .with_unit("{failure}")
            .build();
        let provider_readiness_checks = meter
            .u64_counter("pioneer.gateway.providers.readiness.checks")
            .with_description(
                "Number of Gateway-owned provider readiness checks by bounded result state",
            )
            .with_unit("{check}")
            .build();
        let native_lifecycle_events = meter
            .u64_counter("pioneer.gateway.native.lifecycle.events")
            .with_description(
                "Native-agent lifecycle events by bounded stage and outcome without resource IDs",
            )
            .with_unit("{event}")
            .build();
        let native_lifecycle_readiness_checks = meter
            .u64_counter("pioneer.gateway.native.readiness.checks")
            .with_description(
                "Native lifecycle readiness component checks by bounded component and state",
            )
            .with_unit("{check}")
            .build();
        let native_lifecycle_duration = meter
            .f64_histogram("pioneer.gateway.native.lifecycle.duration")
            .with_description(
                "Native lifecycle latency for bounded provider, tool, durable and terminal stages",
            )
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        register_native_lifecycle_depth_observable(&meter);
        let gateway_operation_duration = meter
            .f64_histogram("pioneer.gateway.operation.duration")
            .with_description("End-to-end duration of a bounded Gateway operation")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let gateway_operation_stage_duration = meter
            .f64_histogram("pioneer.gateway.operation.stage.duration")
            .with_description("Duration of a stable stage within a bounded Gateway operation")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let gateway_operation_items = meter
            .u64_histogram("pioneer.gateway.operation.items")
            .with_description("Bounded item counts observed by a Gateway operation")
            .with_unit("{item}")
            .with_boundaries(operation_item_boundaries())
            .build();
        let gateway_operation_failures = meter
            .u64_counter("pioneer.gateway.operation.failures")
            .with_description("Number of failed bounded Gateway operations")
            .with_unit("{failure}")
            .build();
        let patch_operations = meter
            .u64_counter("pioneer.patch.operations")
            .with_description(
                "Apply Patch calls by bounded runtime, profile, authority, outcome and exactness",
            )
            .with_unit("{operation}")
            .build();
        let patch_operation_duration = meter
            .f64_histogram("pioneer.patch.operation.duration")
            .with_description("End-to-end Apply Patch operation duration")
            .with_unit("ms")
            .with_boundaries(operation_duration_boundaries())
            .build();
        let patch_committed_files = meter
            .u64_counter("pioneer.patch.committed.files")
            .with_description("Files committed by Apply Patch")
            .with_unit("{file}")
            .build();
        let patch_committed_hunks = meter
            .u64_counter("pioneer.patch.committed.hunks")
            .with_description("Committed source hunks from Apply Patch")
            .with_unit("{hunk}")
            .build();
        let patch_committed_bytes = meter
            .u64_counter("pioneer.patch.committed.bytes")
            .with_description("Snapshot bytes represented by committed Apply Patch changes")
            .with_unit("By")
            .build();
        let patch_fallbacks = meter
            .u64_counter("pioneer.patch.mutation_fallbacks")
            .with_description(
                "Shell or Python command fallback after an Apply Patch failure, without command text",
            )
            .with_unit("{fallback}")
            .build();
        register_patch_telemetry_observables(&meter);
        Self {
            meter,
            database_operations,
            database_operation_duration,
            database_pool_acquire_duration,
            database_read_operations,
            database_read_duration,
            database_read_admission_events,
            database_read_admission_wait_duration,
            database_read_admission_hold_duration,
            database_read_admission_queue_depth,
            database_read_admission_active,
            database_admission_events,
            database_admission_wait_duration,
            database_admission_quantum_duration,
            database_admission_queue_depth,
            database_workload_operations,
            database_workload_duration,
            database_workload_query_count,
            database_workload_query_duration,
            database_workload_anomalies,
            provider_warmup_duration,
            provider_warmup_stage_duration,
            provider_warmup_failures,
            provider_readiness_checks,
            native_lifecycle_events,
            native_lifecycle_readiness_checks,
            native_lifecycle_duration,
            gateway_operation_duration,
            gateway_operation_stage_duration,
            gateway_operation_items,
            gateway_operation_failures,
            patch_operations,
            patch_operation_duration,
            patch_committed_files,
            patch_committed_hunks,
            patch_committed_bytes,
            patch_fallbacks,
        }
    }
}

pub(crate) struct StartupMetrics {
    pub(crate) duration: Histogram<f64>,
    pub(crate) stage_duration: Histogram<f64>,
    pub(crate) failures: Counter<u64>,
}

pub(crate) struct DesktopUpdateMetrics {
    pub(crate) apply_duration: Histogram<f64>,
    pub(crate) relaunch_duration: Histogram<f64>,
    pub(crate) failures: Counter<u64>,
}

pub(crate) struct DesktopGatewayLifecycleMetrics {
    pub(crate) duration: Histogram<f64>,
    pub(crate) stage_duration: Histogram<f64>,
    pub(crate) failures: Counter<u64>,
}

impl DesktopGatewayLifecycleMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        let duration = meter
            .f64_histogram("pioneer.desktop.gateway.lifecycle.duration")
            .with_description("Duration of a Desktop-managed Gateway start or update operation")
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let stage_duration = meter
            .f64_histogram("pioneer.desktop.gateway.lifecycle.stage.duration")
            .with_description(
                "Duration of a stable, low-cardinality Desktop-managed Gateway lifecycle stage",
            )
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let failures = meter
            .u64_counter("pioneer.desktop.gateway.lifecycle.failures")
            .with_description("Number of Desktop-managed Gateway lifecycle operations that failed")
            .with_unit("{failure}")
            .build();

        Self {
            duration,
            stage_duration,
            failures,
        }
    }
}

impl DesktopUpdateMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        let apply_duration = meter
            .f64_histogram("pioneer.desktop.update.apply.duration")
            .with_description("Time spent replacing the installed Desktop application")
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let relaunch_duration = meter
            .f64_histogram("pioneer.desktop.update.relaunch.duration")
            .with_description(
                "Time from a durable applied receipt until the new Desktop process claims it",
            )
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let failures = meter
            .u64_counter("pioneer.desktop.update.failures")
            .with_description("Number of confirmed Desktop update startups that failed or stalled")
            .with_unit("{failure}")
            .build();

        Self {
            apply_duration,
            relaunch_duration,
            failures,
        }
    }
}

impl StartupMetrics {
    pub(crate) fn new(
        meter: &Meter,
        duration_name: &'static str,
        stage_duration_name: &'static str,
        failures_name: &'static str,
        target_label: &'static str,
    ) -> Self {
        let duration = meter
            .f64_histogram(duration_name)
            .with_description(format!(
                "Time from {target_label} process entry until the app is operational"
            ))
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let stage_duration = meter
            .f64_histogram(stage_duration_name)
            .with_description(format!(
                "Duration of a stable, low-cardinality {target_label} startup stage"
            ))
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let failures = meter
            .u64_counter(failures_name)
            .with_description(format!(
                "Number of {target_label} startups that failed before an operational state"
            ))
            .with_unit("{failure}")
            .build();

        Self {
            duration,
            stage_duration,
            failures,
        }
    }
}

fn startup_duration_boundaries() -> Vec<f64> {
    vec![
        1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
        20_000.0, 30_000.0, 60_000.0, 120_000.0,
    ]
}

fn operation_duration_boundaries() -> Vec<f64> {
    vec![
        0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0,
        5_000.0, 10_000.0, 30_000.0,
    ]
}

fn operation_item_boundaries() -> Vec<f64> {
    vec![
        1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 500.0, 1_000.0, 2_500.0, 5_000.0,
        10_000.0, 25_000.0, 100_000.0,
    ]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeLifecycleStage {
    Turn,
    FirstEvent,
    DurableCommit,
    ProviderRound,
    ToolAttempt,
    ToolRetry,
    Recovery,
    TerminalCommit,
    Terminalization,
    Readiness,
}

impl NativeLifecycleStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Turn => "turn",
            Self::FirstEvent => "first_event",
            Self::DurableCommit => "durable_commit",
            Self::ProviderRound => "provider_round",
            Self::ToolAttempt => "tool_attempt",
            Self::ToolRetry => "tool_retry",
            Self::Recovery => "recovery",
            Self::TerminalCommit => "terminal_commit",
            Self::Terminalization => "terminalization",
            Self::Readiness => "readiness",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeLifecycleOutcome {
    Started,
    Succeeded,
    Failed,
    Blocked,
    Interrupted,
    TimedOut,
    Rejected,
    Exhausted,
    Saturated,
    Closed,
    Recovered,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeProviderClass {
    Api,
    Cli,
    Unknown,
}

impl NativeProviderClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Cli => "cli",
            Self::Unknown => "unknown",
        }
    }
}

impl NativeLifecycleOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Interrupted => "interrupted",
            Self::TimedOut => "timed_out",
            Self::Rejected => "rejected",
            Self::Exhausted => "exhausted",
            Self::Saturated => "saturated",
            Self::Closed => "closed",
            Self::Recovered => "recovered",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NativeLifecycleEventMetric {
    pub stage: NativeLifecycleStage,
    pub outcome: NativeLifecycleOutcome,
    pub provider_class: NativeProviderClass,
    pub elapsed: Option<Duration>,
}

pub fn record_native_lifecycle_event(metric: NativeLifecycleEventMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("native.stage", metric.stage.as_str()),
        KeyValue::new("outcome", metric.outcome.as_str()),
        KeyValue::new("provider.class", metric.provider_class.as_str()),
    ];
    metrics.native_lifecycle_events.add(1, &attributes);
    if let Some(elapsed) = metric.elapsed {
        metrics
            .native_lifecycle_duration
            .record(elapsed.as_secs_f64() * 1_000.0, &attributes);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeReadinessComponent {
    Database,
    NativeAgentManager,
    DurableListeners,
    RecoveryCoordinator,
    Terminalization,
    ProviderRegistry,
}

impl NativeReadinessComponent {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Database => "database",
            Self::NativeAgentManager => "native_agent_manager",
            Self::DurableListeners => "durable_listeners",
            Self::RecoveryCoordinator => "recovery_coordinator",
            Self::Terminalization => "terminalization",
            Self::ProviderRegistry => "provider_registry",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeReadinessState {
    Starting,
    Healthy,
    Degraded,
    Unhealthy,
}

impl NativeReadinessState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }
}

pub fn record_native_readiness_component(
    component: NativeReadinessComponent,
    state_value: NativeReadinessState,
) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    metrics.native_lifecycle_readiness_checks.add(
        1,
        &[
            KeyValue::new("native.component", component.as_str()),
            KeyValue::new("native.state", state_value.as_str()),
        ],
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeLifecycleDepthKind {
    ActiveTurns,
    StaleRunningTurns,
    RecoveryBacklog,
    TerminalBacklog,
    UnresolvedTerminalEffects,
}

impl NativeLifecycleDepthKind {
    const ALL: [Self; 5] = [
        Self::ActiveTurns,
        Self::StaleRunningTurns,
        Self::RecoveryBacklog,
        Self::TerminalBacklog,
        Self::UnresolvedTerminalEffects,
    ];

    const fn index(self) -> usize {
        match self {
            Self::ActiveTurns => 0,
            Self::StaleRunningTurns => 1,
            Self::RecoveryBacklog => 2,
            Self::TerminalBacklog => 3,
            Self::UnresolvedTerminalEffects => 4,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::ActiveTurns => "active_turns",
            Self::StaleRunningTurns => "stale_running_turns",
            Self::RecoveryBacklog => "recovery_backlog",
            Self::TerminalBacklog => "terminal_backlog",
            Self::UnresolvedTerminalEffects => "unresolved_terminal_effects",
        }
    }
}

pub fn record_native_lifecycle_depth(kind: NativeLifecycleDepthKind, value: u64) {
    NATIVE_LIFECYCLE_DEPTH_VALUES[kind.index()].store(value, Ordering::Release);
}

fn register_native_lifecycle_depth_observable(meter: &Meter) {
    meter
        .u64_observable_gauge("pioneer.gateway.native.lifecycle.depth")
        .with_description(
            "Current bounded native lifecycle active, stale, recovery and terminal backlog depth",
        )
        .with_unit("{item}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            for kind in NativeLifecycleDepthKind::ALL {
                observer.observe(
                    NATIVE_LIFECYCLE_DEPTH_VALUES[kind.index()].load(Ordering::Acquire),
                    &[KeyValue::new("native.depth.kind", kind.as_str())],
                );
            }
        })
        .build();
}

/// One bounded, source-free Apply Patch observation. All string values are
/// selected by trusted runtime code from finite enums; patch text, file text,
/// paths, IDs and unrestricted diagnostics are intentionally absent.
#[derive(Clone, Copy, Debug)]
pub struct PatchOperationMetric {
    pub runtime: &'static str,
    pub profile: &'static str,
    pub authority: &'static str,
    pub outcome: &'static str,
    pub failed_stage: Option<&'static str>,
    pub error_code: Option<&'static str>,
    pub tracking: &'static str,
    pub exact: bool,
    pub committed_files: u64,
    pub committed_hunks: u64,
    pub committed_bytes: u64,
    pub elapsed: Duration,
}

pub fn record_patch_operation(metric: PatchOperationMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("patch.runtime", metric.runtime),
        KeyValue::new("patch.profile", metric.profile),
        KeyValue::new("patch.authority", metric.authority),
        KeyValue::new("patch.stage", metric.failed_stage.unwrap_or("complete")),
        KeyValue::new("error.type", metric.error_code.unwrap_or("none")),
        KeyValue::new(
            "patch.exactness",
            if metric.exact { "exact" } else { "inexact" },
        ),
        KeyValue::new("outcome", metric.outcome),
        KeyValue::new("patch.tracking", metric.tracking),
    ];
    metrics.patch_operations.add(1, &attributes);
    metrics
        .patch_operation_duration
        .record(metric.elapsed.as_secs_f64() * 1_000.0, &attributes);
    metrics
        .patch_committed_files
        .add(metric.committed_files, &attributes);
    metrics
        .patch_committed_hunks
        .add(metric.committed_hunks, &attributes);
    metrics
        .patch_committed_bytes
        .add(metric.committed_bytes, &attributes);
}

pub fn record_patch_mutation_fallback(
    runtime: &'static str,
    profile: &'static str,
    fallback: &'static str,
) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    metrics.patch_fallbacks.add(
        1,
        &[
            KeyValue::new("patch.runtime", runtime),
            KeyValue::new("patch.profile", profile),
            KeyValue::new("fallback.type", fallback),
        ],
    );
}

fn register_patch_telemetry_observables(meter: &Meter) {
    meter
        .u64_observable_counter("pioneer.patch.internal.operations.total")
        .with_description("Process-wide Apply Patch outcomes used for reconciliation and alerts")
        .with_unit("{operation}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            for (outcome, value) in [
                ("applied", snapshot.applied),
                ("partial", snapshot.partial),
                ("rejected", snapshot.rejected),
                ("failed", snapshot.failed),
                ("commit_state_uncertain", snapshot.uncertain),
            ] {
                observer.observe(value, &[KeyValue::new("outcome", outcome)]);
            }
        })
        .build();
    meter
        .u64_observable_counter("pioneer.patch.internal.stage.duration.total")
        .with_description("Cumulative Apply Patch stage latency")
        .with_unit("ns")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            for (stage, value) in [
                ("parse", snapshot.parse_latency_ns),
                ("plan", snapshot.plan_latency_ns),
                ("lock", snapshot.lock_latency_ns),
                ("commit", snapshot.commit_latency_ns),
                ("persist", snapshot.persist_latency_ns),
                ("total", snapshot.total_latency_ns),
            ] {
                observer.observe(value, &[KeyValue::new("patch.stage", stage)]);
            }
        })
        .build();
    meter
        .u64_observable_counter("pioneer.patch.internal.tracking.events.total")
        .with_description("Patch history publication, replay and reconciliation signals")
        .with_unit("{event}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            for (event, value) in [
                ("record_append", snapshot.applied_record_appends),
                ("publication_failure", snapshot.tracker_publication_failures),
                ("projection_lag", snapshot.projection_lag),
                ("pending_ordinal", snapshot.pending_ordinals),
                ("duplicate_suppression", snapshot.duplicate_suppressions),
                ("pending_tracking", snapshot.pending_tracking),
            ] {
                observer.observe(value, &[KeyValue::new("patch.tracking.event", event)]);
            }
        })
        .build();
    meter
        .u64_observable_counter("pioneer.patch.internal.stale.total")
        .with_description("Apply Patch stale failures by bounded decision point")
        .with_unit("{failure}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            for (reason, value) in [
                ("observed_guard", snapshot.observed_guard_stale),
                ("context", snapshot.context_stale),
                (
                    "prepared_revalidation",
                    snapshot.prepared_revalidation_stale,
                ),
                ("commit_cas", snapshot.commit_cas_stale),
            ] {
                observer.observe(value, &[KeyValue::new("patch.stale.reason", reason)]);
            }
        })
        .build();
    meter
        .u64_observable_counter("pioneer.patch.internal.authority.calls.total")
        .with_description("Apply Patch calls by trusted mutation/history authority")
        .with_unit("{operation}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            for (authority, value) in [
                ("native_patch_engine", snapshot.native_calls),
                ("managed_claude_patch_engine", snapshot.managed_calls),
                ("untracked", snapshot.untracked_calls),
            ] {
                observer.observe(value, &[KeyValue::new("patch.authority", authority)]);
            }
        })
        .build();
    meter
        .u64_observable_counter("pioneer.patch.internal.task.results.total")
        .with_description("Complete agent-task outcomes, separate from patch tool outcomes")
        .with_unit("{task}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            observer.observe(
                snapshot.task_successes,
                &[KeyValue::new("outcome", "success")],
            );
            observer.observe(
                snapshot.task_failures,
                &[KeyValue::new("outcome", "failure")],
            );
        })
        .build();
    meter
        .u64_observable_counter("pioneer.patch.internal.tool.results.total")
        .with_description("Apply Patch tool outcomes, separate from complete agent-task outcomes")
        .with_unit("{tool_call}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            observer.observe(
                snapshot.tool_successes,
                &[KeyValue::new("outcome", "success")],
            );
            observer.observe(
                snapshot.tool_failures,
                &[KeyValue::new("outcome", "failure")],
            );
        })
        .build();
    meter
        .u64_observable_counter("pioneer.patch.internal.record.append.duration.total")
        .with_description("Cumulative immutable applied-record append latency")
        .with_unit("ns")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            observer.observe(
                crate::patch_telemetry::snapshot().applied_record_append_latency_ns,
                &[],
            );
        })
        .build();
    meter
        .u64_observable_gauge("pioneer.patch.snapshot.storage.bytes")
        .with_description("Logical, physical and referenced snapshot storage bytes")
        .with_unit("By")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            for (kind, value) in [
                ("logical", snapshot.snapshot_logical_bytes),
                ("physical", snapshot.snapshot_physical_bytes),
                (
                    "referenced_logical",
                    snapshot.snapshot_referenced_logical_bytes,
                ),
                ("gc_reclaimed", snapshot.snapshot_gc_bytes),
            ] {
                observer.observe(value, &[KeyValue::new("patch.snapshot.bytes.kind", kind)]);
            }
        })
        .build();
    meter
        .u64_observable_gauge("pioneer.patch.snapshot.storage.ratio")
        .with_description("Snapshot deduplication and compression ratio in parts per million")
        .with_unit("ppm")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            observer.observe(
                snapshot.snapshot_dedup_ratio_ppm,
                &[KeyValue::new("patch.snapshot.ratio.kind", "dedup")],
            );
            observer.observe(
                snapshot.snapshot_compression_ratio_ppm,
                &[KeyValue::new("patch.snapshot.ratio.kind", "compression")],
            );
        })
        .build();
    meter
        .u64_observable_gauge("pioneer.patch.snapshot.storage.objects")
        .with_description("Snapshot references and garbage-collected blob count")
        .with_unit("{object}")
        .with_callback(|observer| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = crate::patch_telemetry::snapshot();
            observer.observe(
                snapshot.snapshot_references,
                &[KeyValue::new("patch.snapshot.object.kind", "reference")],
            );
            observer.observe(
                snapshot.snapshot_gc_blobs,
                &[KeyValue::new(
                    "patch.snapshot.object.kind",
                    "gc_reclaimed_blob",
                )],
            );
        })
        .build();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DatabasePoolContext {
    role: DatabaseRole,
    class: &'static str,
}

impl DatabasePoolContext {
    fn from_span_name(name: &str) -> Option<Self> {
        Some(match name {
            "pioneer.sqlite.reader.interactive" => Self {
                role: DatabaseRole::Reader,
                class: "interactive",
            },
            "pioneer.sqlite.reader.maintenance" => Self {
                role: DatabaseRole::Reader,
                class: "maintenance",
            },
            "pioneer.sqlite.writer.critical" => Self {
                role: DatabaseRole::Writer,
                class: "critical",
            },
            "pioneer.sqlite.writer.interactive" => Self {
                role: DatabaseRole::Writer,
                class: "interactive",
            },
            "pioneer.sqlite.writer.maintenance" => Self {
                role: DatabaseRole::Writer,
                class: "maintenance",
            },
            _ => return None,
        })
    }
}

fn record_database_pool_acquire(context: DatabasePoolContext, elapsed: Duration) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    metrics.database_pool_acquire_duration.record(
        elapsed.as_secs_f64() * 1_000.0,
        &[
            KeyValue::new("db.system.name", "sqlite"),
            KeyValue::new("db.pool.role", context.role.as_str()),
            KeyValue::new("db.scheduling.class", context.class),
        ],
    );
}

/// Consumes SQLx's pool-acquisition timing event without exposing it in normal
/// logs. Unscoped SQLx pools are deliberately ignored rather than mislabeled
/// as the Gateway reader or writer.
pub(crate) struct DatabasePoolAcquireMetricsLayer;

impl<S> Layer<S> for DatabasePoolAcquireMetricsLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, ctx: TracingContext<'_, S>) {
        let mut visitor = PoolAcquireEventVisitor::default();
        event.record(&mut visitor);
        let Some(seconds) = visitor
            .acquired_after_seconds
            .filter(|value| value.is_finite())
        else {
            return;
        };
        let Some(context) = database_pool_context_from_event(event, &ctx) else {
            return;
        };
        record_database_pool_acquire(context, Duration::from_secs_f64(seconds.max(0.0)));
    }
}

fn database_pool_context_from_event<S>(
    event: &Event<'_>,
    ctx: &TracingContext<'_, S>,
) -> Option<DatabasePoolContext>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    ctx.event_scope(event).and_then(|scope| {
        scope
            .into_iter()
            .find_map(|span| DatabasePoolContext::from_span_name(span.metadata().name()))
    })
}

#[derive(Default)]
struct PoolAcquireEventVisitor {
    acquired_after_seconds: Option<f64>,
}

impl Visit for PoolAcquireEventVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "acquired_after_secs" {
            self.acquired_after_seconds = Some(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

pub fn record_database_operation(metric: DatabaseOperationMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let attributes = [
        KeyValue::new("db.system.name", "sqlite"),
        KeyValue::new("db.pool.role", metric.role.as_str()),
        KeyValue::new("db.operation.name", metric.operation.as_str()),
        KeyValue::new("db.operation.type", metric.operation.kind()),
        KeyValue::new("db.workload.name", metric.workload.as_str()),
        KeyValue::new(
            "db.query.fingerprint",
            crate::database_workload::fingerprint_attribute_value(metric.query_fingerprint),
        ),
        KeyValue::new("outcome", if metric.failed { "error" } else { "ok" }),
    ];
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    metrics.database_operations.add(1, &attributes);
    metrics
        .database_operation_duration
        .record(metric.elapsed.as_secs_f64() * 1_000.0, &attributes);
}

pub fn record_gateway_active_connections(value: u64) {
    GATEWAY_ACTIVE_CONNECTIONS.store(value, Ordering::Release);
}

pub fn record_gateway_database_size_bytes(value: u64) {
    GATEWAY_DATABASE_SIZE_BYTES.store(value, Ordering::Release);
}

fn database_runtime_context() -> (u64, u64, u64, u64) {
    let active_turns = NATIVE_LIFECYCLE_DEPTH_VALUES[NativeLifecycleDepthKind::ActiveTurns.index()]
        .load(Ordering::Acquire);
    let recovery_backlog = NATIVE_LIFECYCLE_DEPTH_VALUES
        [NativeLifecycleDepthKind::RecoveryBacklog.index()]
    .load(Ordering::Acquire);
    let active_connections = GATEWAY_ACTIVE_CONNECTIONS.load(Ordering::Acquire);
    let database_size_bytes = GATEWAY_DATABASE_SIZE_BYTES.load(Ordering::Acquire);
    (
        active_turns,
        active_connections,
        recovery_backlog,
        database_size_bytes,
    )
}

/// Aggregate metrics deliberately combine the three live-load dimensions into
/// one finite cohort. Keeping them as independent labels would multiply every
/// workload/fingerprint series and eventually hit the SDK cardinality limit.
pub(crate) fn database_runtime_metric_attributes() -> [KeyValue; 2] {
    let (active_turns, active_connections, recovery_backlog, database_size_bytes) =
        database_runtime_context();
    [
        KeyValue::new(
            "gateway.load.bucket",
            gateway_load_bucket(active_turns, active_connections, recovery_backlog),
        ),
        KeyValue::new(
            "gateway.database_size.bucket",
            database_size_bucket(database_size_bytes),
        ),
    ]
}

pub(crate) fn database_runtime_load_metric_attribute() -> KeyValue {
    let (active_turns, active_connections, recovery_backlog, _) = database_runtime_context();
    KeyValue::new(
        "gateway.load.bucket",
        gateway_load_bucket(active_turns, active_connections, recovery_backlog),
    )
}

/// Rate-limited anomaly traces can retain the individual buckets without
/// creating persistent metric series for their Cartesian product.
pub(crate) fn database_runtime_trace_attributes() -> [KeyValue; 3] {
    let (active_turns, active_connections, recovery_backlog, _) = database_runtime_context();
    [
        KeyValue::new(
            "gateway.active_turns.bucket",
            activity_count_bucket(active_turns),
        ),
        KeyValue::new(
            "gateway.active_connections.bucket",
            connection_count_bucket(active_connections),
        ),
        KeyValue::new(
            "gateway.recovery_backlog.bucket",
            backlog_count_bucket(recovery_backlog),
        ),
    ]
}

const fn gateway_load_bucket(
    active_turns: u64,
    active_connections: u64,
    recovery_backlog: u64,
) -> &'static str {
    match (
        active_turns > 0,
        active_connections > 0,
        recovery_backlog > 0,
    ) {
        (false, false, false) => "idle",
        (false, true, false) => "client_connected",
        (true, _, false) => "agent_active",
        (false, _, true) => "recovery",
        (true, _, true) => "mixed",
    }
}

const fn activity_count_bucket(value: u64) -> &'static str {
    match value {
        0 => "0",
        1 => "1",
        2..=4 => "2_4",
        5..=16 => "5_16",
        _ => "17_plus",
    }
}

const fn connection_count_bucket(value: u64) -> &'static str {
    match value {
        0 => "0",
        1 => "1",
        2..=4 => "2_4",
        _ => "5_plus",
    }
}

const fn backlog_count_bucket(value: u64) -> &'static str {
    match value {
        0 => "0",
        1..=16 => "1_16",
        17..=256 => "17_256",
        257..=4_096 => "257_4096",
        _ => "4097_plus",
    }
}

const fn database_size_bucket(value: u64) -> &'static str {
    match value {
        0 => "unknown",
        1..=268_435_455 => "under_256_mib",
        268_435_456..=1_073_741_823 => "256_mib_1_gib",
        1_073_741_824..=4_294_967_295 => "1_4_gib",
        4_294_967_296..=17_179_869_183 => "4_16_gib",
        _ => "16_gib_plus",
    }
}

pub fn record_database_read(metric: DatabaseReadMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("db.system.name", "sqlite"),
        KeyValue::new("db.pool.role", DatabaseRole::Reader.as_str()),
        KeyValue::new("db.scheduling.class", metric.class),
        KeyValue::new("outcome", metric.outcome),
    ];
    metrics.database_read_operations.add(1, &attributes);
    metrics
        .database_read_duration
        .record(metric.elapsed.as_secs_f64() * 1_000.0, &attributes);
}

pub fn record_database_read_admission(metric: DatabaseReadAdmissionMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("db.system.name", "sqlite"),
        KeyValue::new("db.pool.role", DatabaseRole::Reader.as_str()),
        KeyValue::new("db.admission.event", metric.event),
        KeyValue::new("db.admission.class", metric.class),
    ];
    metrics.database_read_admission_events.add(1, &attributes);
    metrics
        .database_read_admission_queue_depth
        .record(metric.queue_depth, &attributes);
    metrics
        .database_read_admission_active
        .record(metric.active, &attributes);
    if let Some(waited) = metric.waited {
        metrics
            .database_read_admission_wait_duration
            .record(waited.as_secs_f64() * 1_000.0, &attributes);
    }
    if let Some(held) = metric.held {
        metrics
            .database_read_admission_hold_duration
            .record(held.as_secs_f64() * 1_000.0, &attributes);
    }
}

pub fn record_database_admission(metric: DatabaseAdmissionMetric) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    let attributes = [
        KeyValue::new("db.system.name", "sqlite"),
        KeyValue::new("db.pool.role", DatabaseRole::Writer.as_str()),
        KeyValue::new("db.admission.event", metric.event),
        KeyValue::new("db.admission.class", metric.class),
        KeyValue::new("db.admission.reason", metric.reason),
    ];
    metrics.database_admission_events.add(1, &attributes);
    metrics.database_admission_queue_depth.record(
        metric.critical_queue,
        &[
            attributes[0].clone(),
            attributes[1].clone(),
            attributes[2].clone(),
            KeyValue::new("db.admission.queue.class", "critical"),
        ],
    );
    metrics.database_admission_queue_depth.record(
        metric.interactive_queue,
        &[
            attributes[0].clone(),
            attributes[1].clone(),
            attributes[2].clone(),
            KeyValue::new("db.admission.queue.class", "interactive"),
        ],
    );
    metrics.database_admission_queue_depth.record(
        metric.maintenance_queue,
        &[
            attributes[0].clone(),
            attributes[1].clone(),
            attributes[2].clone(),
            KeyValue::new("db.admission.queue.class", "maintenance"),
        ],
    );
    if let Some(waited) = metric.waited {
        metrics
            .database_admission_wait_duration
            .record(waited.as_secs_f64() * 1_000.0, &attributes);
    }
    if let Some(held) = metric.held {
        metrics
            .database_admission_quantum_duration
            .record(held.as_secs_f64() * 1_000.0, &attributes);
    }
}

pub fn register_database_pool_observer<F>(
    role: DatabaseRole,
    max_connections: u64,
    observer: F,
) -> bool
where
    F: Fn() -> DatabasePoolSnapshot + Send + Sync + 'static,
{
    let Some(state) = super::telemetry::state() else {
        return false;
    };
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return false;
    };
    metrics
        .meter
        .u64_observable_gauge("pioneer.gateway.db.pool.connections")
        .with_description("Current and configured SQLite connection pool size")
        .with_unit("{connection}")
        .with_callback(move |gauge| {
            if !super::telemetry_enabled() {
                return;
            }
            let snapshot = observer();
            let size = snapshot.size.min(max_connections);
            let idle = snapshot.idle.min(size);
            let in_use = size.saturating_sub(idle);
            let common = [
                KeyValue::new("db.system.name", "sqlite"),
                KeyValue::new("db.pool.role", role.as_str()),
            ];
            gauge.observe(
                idle,
                &[
                    common[0].clone(),
                    common[1].clone(),
                    KeyValue::new("state", "idle"),
                ],
            );
            gauge.observe(
                in_use,
                &[
                    common[0].clone(),
                    common[1].clone(),
                    KeyValue::new("state", "in_use"),
                ],
            );
            gauge.observe(
                max_connections,
                &[
                    common[0].clone(),
                    common[1].clone(),
                    KeyValue::new("state", "max"),
                ],
            );
        })
        .build();
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::filter::{LevelFilter, Targets};
    use tracing_subscriber::layer::{Context as TracingContext, Layer};
    use tracing_subscriber::prelude::*;

    struct PoolContextCaptureLayer(Arc<Mutex<Vec<Option<DatabasePoolContext>>>>);

    #[test]
    fn database_runtime_context_buckets_have_stable_boundaries() {
        assert_eq!(gateway_load_bucket(0, 0, 0), "idle");
        assert_eq!(gateway_load_bucket(0, 1, 0), "client_connected");
        assert_eq!(gateway_load_bucket(1, 0, 0), "agent_active");
        assert_eq!(gateway_load_bucket(0, 1, 1), "recovery");
        assert_eq!(gateway_load_bucket(1, 1, 1), "mixed");

        assert_eq!(activity_count_bucket(0), "0");
        assert_eq!(activity_count_bucket(1), "1");
        assert_eq!(activity_count_bucket(4), "2_4");
        assert_eq!(activity_count_bucket(16), "5_16");
        assert_eq!(activity_count_bucket(17), "17_plus");

        assert_eq!(connection_count_bucket(0), "0");
        assert_eq!(connection_count_bucket(4), "2_4");
        assert_eq!(connection_count_bucket(5), "5_plus");

        assert_eq!(backlog_count_bucket(0), "0");
        assert_eq!(backlog_count_bucket(256), "17_256");
        assert_eq!(backlog_count_bucket(4_097), "4097_plus");

        assert_eq!(database_size_bucket(0), "unknown");
        assert_eq!(database_size_bucket(268_435_455), "under_256_mib");
        assert_eq!(database_size_bucket(268_435_456), "256_mib_1_gib");
        assert_eq!(database_size_bucket(4_294_967_296), "4_16_gib");
    }

    impl<S> Layer<S> for PoolContextCaptureLayer
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_event(&self, event: &Event<'_>, ctx: TracingContext<'_, S>) {
            if event.metadata().target() == "sqlx::pool::acquire" {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(database_pool_context_from_event(event, &ctx));
            }
        }
    }

    #[test]
    fn pool_acquire_context_is_exact_and_unscoped_events_are_ignored() {
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let database_metrics_filter = Targets::new()
            .with_default(LevelFilter::OFF)
            .with_target("sqlx::pool::acquire", LevelFilter::TRACE)
            .with_target("pioneer_sqlite::pool", LevelFilter::TRACE);
        let subscriber = tracing_subscriber::registry()
            .with(PoolContextCaptureLayer(contexts.clone()).with_filter(database_metrics_filter));

        tracing::subscriber::with_default(subscriber, || {
            record_test_pool_acquire(tracing::trace_span!(
                target: "pioneer_sqlite::pool",
                "pioneer.sqlite.reader.interactive"
            ));
            record_test_pool_acquire(tracing::trace_span!(
                target: "pioneer_sqlite::pool",
                "pioneer.sqlite.reader.maintenance"
            ));
            record_test_pool_acquire(tracing::trace_span!(
                target: "pioneer_sqlite::pool",
                "pioneer.sqlite.writer.critical"
            ));
            record_test_pool_acquire(tracing::trace_span!(
                target: "pioneer_sqlite::pool",
                "pioneer.sqlite.writer.interactive"
            ));
            record_test_pool_acquire(tracing::trace_span!(
                target: "pioneer_sqlite::pool",
                "pioneer.sqlite.writer.maintenance"
            ));
            tracing::trace!(
                target: "sqlx::pool::acquire",
                acquired_after_secs = 0.5_f64,
                "unscoped connection"
            );
        });

        assert_eq!(
            *contexts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![
                Some(DatabasePoolContext {
                    role: DatabaseRole::Reader,
                    class: "interactive",
                }),
                Some(DatabasePoolContext {
                    role: DatabaseRole::Reader,
                    class: "maintenance",
                }),
                Some(DatabasePoolContext {
                    role: DatabaseRole::Writer,
                    class: "critical",
                }),
                Some(DatabasePoolContext {
                    role: DatabaseRole::Writer,
                    class: "interactive",
                }),
                Some(DatabasePoolContext {
                    role: DatabaseRole::Writer,
                    class: "maintenance",
                }),
                None,
            ]
        );
    }

    fn record_test_pool_acquire(span: tracing::Span) {
        let _guard = span.enter();
        tracing::trace!(
            target: "sqlx::pool::acquire",
            acquired_after_secs = 0.25_f64,
            "acquired connection"
        );
    }

    #[test]
    fn native_lifecycle_depth_is_a_bounded_latest_value_snapshot() {
        for (index, kind) in NativeLifecycleDepthKind::ALL.into_iter().enumerate() {
            record_native_lifecycle_depth(kind, u64::try_from(index + 1).unwrap());
        }
        record_native_lifecycle_depth(NativeLifecycleDepthKind::ActiveTurns, 99);

        let values = NativeLifecycleDepthKind::ALL
            .map(|kind| NATIVE_LIFECYCLE_DEPTH_VALUES[kind.index()].load(Ordering::Acquire));
        assert_eq!(values, [99, 2, 3, 4, 5]);
        assert_eq!(
            NativeLifecycleDepthKind::ALL.map(NativeLifecycleDepthKind::as_str),
            [
                "active_turns",
                "stale_running_turns",
                "recovery_backlog",
                "terminal_backlog",
                "unresolved_terminal_effects",
            ]
        );
    }
}
