use opentelemetry::KeyValue;
use opentelemetry::metrics::{BoundCounter, BoundHistogram, Counter, Histogram, Meter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context as TracingContext, Layer};

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

// Drains in-flight DB records before a closed-consent discard. The exclusive
// barrier is released before SDK collection/export. Recorders check their source
// epoch both before taking the read side (fast off/stale skip) and under it.
pub(crate) static DB_RECORD_GATE: RwLock<()> = RwLock::new(());

fn finite_series_index(attributes: &[KeyValue]) -> Option<usize> {
    let mut index = 0usize;
    for attribute in attributes {
        let values: &[&str] = match attribute.key.as_str() {
            "db.system.name" => &["sqlite"],
            "db.pool.role" => &["reader", "writer"],
            "db.scheduling.class" | "db.admission.class" | "db.admission.queue.class" => {
                &["critical", "interactive", "maintenance"]
            }
            "db.admission.event" => &["enqueued", "acquired", "released", "cancelled"],
            "db.admission.reason" => &[
                "none",
                "uncontended",
                "only_class",
                "priority",
                "interactive_fairness",
                "maintenance_fairness",
            ],
            "outcome" => &["ok", "error", "cancelled"],
            "gateway.load.bucket" => &[
                "idle",
                "client_connected",
                "agent_active",
                "recovery",
                "mixed",
            ],
            "gateway.database_size.bucket" => &[
                "unknown",
                "under_256_mib",
                "256_mib_1_gib",
                "1_4_gib",
                "4_16_gib",
                "16_gib_plus",
            ],
            "db.workload.anomaly.reason" => {
                &["error", "query_amplification", "database_time", "latency"]
            }
            "db.workload.name" => {
                let opentelemetry::Value::String(value) = &attribute.value else {
                    return None;
                };
                index = index * crate::DatabaseWorkload::CARDINALITY
                    + crate::database_workload::workload_metric_index(value.as_str())?;
                continue;
            }
            _ => return None,
        };
        let opentelemetry::Value::String(value) = &attribute.value else {
            return None;
        };
        index = index * values.len()
            + values
                .iter()
                .position(|candidate| *candidate == value.as_str())?;
    }
    Some(index)
}

// These are instrument handles, not aggregators. All observations still update
// the SDK tracker individually. Slot tables are lazy and bounded by the finite
// label catalog; no SDK registration occurs for unobserved combinations.
macro_rules! finite_db_instrument {
    ($name:ident, $plain:ident, $bound:ident, $method:ident) => {
        pub(crate) struct $name<T> {
            instrument: $plain<T>,
            slots: Box<[OnceLock<$bound<T>>]>,
        }
        impl<T> $name<T> {
            fn new(instrument: $plain<T>, capacity: usize) -> Self {
                Self {
                    instrument,
                    slots: (0..capacity).map(|_| OnceLock::new()).collect(),
                }
            }
            pub(crate) fn $method(&self, value: T, attributes: &[KeyValue]) {
                if let Some(slot) =
                    finite_series_index(attributes).and_then(|index| self.slots.get(index))
                {
                    slot.get_or_init(|| self.instrument.bind(attributes))
                        .$method(value);
                } else {
                    // Preserve the public metric API for non-catalog callers,
                    // without retaining arbitrary strings in a new cache.
                    self.instrument.$method(value, attributes);
                }
            }
        }
    };
}
finite_db_instrument!(DbCounter, Counter, BoundCounter, add);
finite_db_instrument!(DbHistogram, Histogram, BoundHistogram, record);

/// Owned by one admitted SQL metric-series identity in the Gateway limiter.
/// Binding runs after that limiter's mutex is released. The process provider is
/// immutable; handles live no longer than this bounded owner and its callbacks.
#[derive(Debug, Default)]
pub struct DatabaseOperationSeries {
    handles: OnceLock<(BoundCounter<u64>, BoundHistogram<f64>)>,
}

impl DatabaseOperationSeries {
    pub fn record(&self, metric: DatabaseOperationMetric) {
        let (enabled, generation) = super::telemetry_consent_snapshot();
        self.record_for_consent(metric, enabled.then_some(generation));
    }

    /// Carries the callback's consent epoch across fingerprint admission.
    pub fn record_for_consent(&self, metric: DatabaseOperationMetric, generation: Option<u64>) {
        if !super::telemetry_sample_allowed(generation) {
            return;
        }
        let _write = DB_RECORD_GATE
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !super::telemetry_sample_allowed(generation) {
            return;
        }
        let Some(metrics) =
            super::telemetry::state().and_then(|state| state.gateway_metrics.as_ref())
        else {
            return;
        };
        let (count, duration) = self.handles.get_or_init(|| {
            let attributes = database_operation_attributes(metric);
            (
                metrics.database_operations.bind(&attributes),
                metrics.database_operation_duration.bind(&attributes),
            )
        });
        count.add(1);
        duration.record(metric.elapsed.as_secs_f64() * 1_000.0);
    }
}

pub(crate) struct GatewayMetrics {
    pub(crate) portable_payload: crate::portable_payload::Metrics,
    pub(crate) meter: Meter,
    pub(crate) database_operations: Counter<u64>,
    pub(crate) database_operation_duration: Histogram<f64>,
    pub(crate) database_pool_acquire_duration: DbHistogram<f64>,
    pub(crate) database_read_operations: DbCounter<u64>,
    pub(crate) database_read_duration: DbHistogram<f64>,
    pub(crate) database_read_admission_events: DbCounter<u64>,
    pub(crate) database_read_admission_wait_duration: DbHistogram<f64>,
    pub(crate) database_read_admission_hold_duration: DbHistogram<f64>,
    pub(crate) database_read_admission_queue_depth: DbHistogram<u64>,
    pub(crate) database_read_admission_active: DbHistogram<u64>,
    pub(crate) database_admission_events: DbCounter<u64>,
    pub(crate) database_admission_wait_duration: DbHistogram<f64>,
    pub(crate) database_admission_quantum_duration: DbHistogram<f64>,
    pub(crate) database_admission_queue_depth: DbHistogram<u64>,
    pub(crate) database_workload_operations: DbCounter<u64>,
    pub(crate) database_workload_duration: DbHistogram<f64>,
    pub(crate) database_workload_query_count: DbHistogram<u64>,
    pub(crate) database_workload_query_duration: DbHistogram<f64>,
    pub(crate) database_workload_anomalies: DbCounter<u64>,
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
            portable_payload: crate::portable_payload::Metrics::new(&meter),
            meter,
            database_operations,
            database_operation_duration,
            database_pool_acquire_duration: DbHistogram::new(database_pool_acquire_duration, 6),
            database_read_operations: DbCounter::new(database_read_operations, 18),
            database_read_duration: DbHistogram::new(database_read_duration, 18),
            database_read_admission_events: DbCounter::new(database_read_admission_events, 24),
            database_read_admission_wait_duration: DbHistogram::new(
                database_read_admission_wait_duration,
                24,
            ),
            database_read_admission_hold_duration: DbHistogram::new(
                database_read_admission_hold_duration,
                24,
            ),
            database_read_admission_queue_depth: DbHistogram::new(
                database_read_admission_queue_depth,
                24,
            ),
            database_read_admission_active: DbHistogram::new(database_read_admission_active, 24),
            database_admission_events: DbCounter::new(database_admission_events, 144),
            database_admission_wait_duration: DbHistogram::new(
                database_admission_wait_duration,
                144,
            ),
            database_admission_quantum_duration: DbHistogram::new(
                database_admission_quantum_duration,
                144,
            ),
            database_admission_queue_depth: DbHistogram::new(database_admission_queue_depth, 24),
            database_workload_operations: DbCounter::new(database_workload_operations, 1980),
            database_workload_duration: DbHistogram::new(database_workload_duration, 1980),
            database_workload_query_count: DbHistogram::new(database_workload_query_count, 1980),
            database_workload_query_duration: DbHistogram::new(
                database_workload_query_duration,
                1980,
            ),
            database_workload_anomalies: DbCounter::new(database_workload_anomalies, 440),
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

use crate::pool_context::PoolContext as DatabasePoolContext;

fn record_database_pool_acquire(context: DatabasePoolContext, elapsed: Duration) {
    crate::turn_startup::record_current_duration(crate::turn_startup::Stage::DbAcquire, elapsed);
    let (enabled, generation) = super::telemetry_consent_snapshot();
    if !super::telemetry_sample_allowed(enabled.then_some(generation)) {
        return;
    }
    let _write = DB_RECORD_GATE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !super::telemetry_sample_allowed(enabled.then_some(generation)) {
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
            KeyValue::new("db.pool.role", context.role().as_str()),
            KeyValue::new("db.scheduling.class", context.class()),
        ],
    );
}

/// Consumes SQLx's pool-acquisition timing event without exposing it in normal
/// logs. Unscoped SQLx pools are deliberately ignored rather than mislabeled
/// as the Gateway reader or writer.
pub(crate) struct DatabasePoolAcquireMetricsLayer;

impl<S> Layer<S> for DatabasePoolAcquireMetricsLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: TracingContext<'_, S>) {
        let mut visitor = PoolAcquireEventVisitor::default();
        event.record(&mut visitor);
        let Some(seconds) = visitor
            .acquired_after_seconds
            .filter(|value| value.is_finite())
        else {
            return;
        };
        let Some(context) = crate::pool_context::current() else {
            return;
        };
        record_database_pool_acquire(context, Duration::from_secs_f64(seconds.max(0.0)));
    }
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

fn database_operation_attributes(metric: DatabaseOperationMetric) -> [KeyValue; 7] {
    [
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
    ]
}

pub fn record_database_operation(metric: DatabaseOperationMetric) {
    let (enabled, generation) = super::telemetry_consent_snapshot();
    if !super::telemetry_sample_allowed(enabled.then_some(generation)) {
        return;
    }
    let _write = DB_RECORD_GATE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !super::telemetry_sample_allowed(enabled.then_some(generation)) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let attributes = database_operation_attributes(metric);
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
    let (enabled, generation) = super::telemetry_consent_snapshot();
    record_database_read_for_consent(metric, enabled.then_some(generation));
}

/// Carries the reader observer's entry epoch through delayed callback delivery.
pub fn record_database_read_for_consent(metric: DatabaseReadMetric, generation: Option<u64>) {
    if !super::telemetry_sample_allowed(generation) {
        return;
    }
    let _write = DB_RECORD_GATE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !super::telemetry_sample_allowed(generation) {
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
    let (enabled, generation) = super::telemetry_consent_snapshot();
    record_database_read_admission_for_consent(metric, enabled.then_some(generation));
}

/// Preserves the native transition's epoch through delayed observer delivery.
pub fn record_database_read_admission_for_consent(
    metric: DatabaseReadAdmissionMetric,
    generation: Option<u64>,
) {
    if !super::telemetry_sample_allowed(generation) {
        return;
    }
    let _write = DB_RECORD_GATE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !super::telemetry_sample_allowed(generation) {
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
    let (enabled, generation) = super::telemetry_consent_snapshot();
    record_database_admission_for_consent(metric, enabled.then_some(generation));
}

/// Preserves the native transition's epoch through delayed observer delivery.
pub fn record_database_admission_for_consent(
    metric: DatabaseAdmissionMetric,
    generation: Option<u64>,
) {
    if !super::telemetry_sample_allowed(generation) {
        return;
    }
    let _write = DB_RECORD_GATE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !super::telemetry_sample_allowed(generation) {
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
            let (enabled, epoch) = super::telemetry_consent_snapshot();
            if !enabled {
                return;
            }
            let snapshot = observer();
            crate::live::record_pool_snapshot(epoch, role, max_connections, snapshot);
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
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: TracingContext<'_, S>) {
            if event.metadata().target() == "sqlx::pool::acquire" {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(crate::pool_context::current());
            }
        }
    }

    #[test]
    fn pool_acquire_context_is_exact_and_unscoped_events_are_ignored() {
        let contexts = Arc::new(Mutex::new(Vec::new()));
        let database_metrics_filter = Targets::new()
            .with_default(LevelFilter::OFF)
            .with_target("sqlx::pool::acquire", LevelFilter::TRACE);
        let subscriber = tracing_subscriber::registry()
            .with(PoolContextCaptureLayer(contexts.clone()).with_filter(database_metrics_filter));

        tracing::subscriber::with_default(subscriber, || {
            for context in [
                DatabasePoolContext::ReaderInteractive,
                DatabasePoolContext::ReaderMaintenance,
                DatabasePoolContext::WriterCritical,
                DatabasePoolContext::WriterInteractive,
                DatabasePoolContext::WriterMaintenance,
            ] {
                record_test_pool_acquire(context);
            }
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
                Some(DatabasePoolContext::ReaderInteractive),
                Some(DatabasePoolContext::ReaderMaintenance),
                Some(DatabasePoolContext::WriterCritical),
                Some(DatabasePoolContext::WriterInteractive),
                Some(DatabasePoolContext::WriterMaintenance),
                None,
            ]
        );
    }

    fn record_test_pool_acquire(context: DatabasePoolContext) {
        let _guard = context.enter();
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

#[cfg(test)]
mod bound_db_regressions {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    struct ConsentReset(bool);
    impl Drop for ConsentReset {
        fn drop(&mut self) {
            crate::set_telemetry_enabled(self.0);
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    enum Observation {
        F64Histogram {
            bounds: Vec<f64>,
            buckets: Vec<u64>,
            count: u64,
            sum: f64,
            min: Option<f64>,
            max: Option<f64>,
        },
        U64Histogram {
            bounds: Vec<f64>,
            buckets: Vec<u64>,
            count: u64,
            sum: u64,
            min: Option<u64>,
            max: Option<u64>,
        },
        Counter(u64),
    }
    #[derive(Clone, Debug, PartialEq)]
    struct Sample {
        unit: String,
        temporality: Temporality,
        attributes: Vec<(String, opentelemetry::Value)>,
        observation: Observation,
    }
    fn sorted_attributes<'a>(
        values: impl Iterator<Item = &'a KeyValue>,
    ) -> Vec<(String, opentelemetry::Value)> {
        let mut values: Vec<_> = values
            .map(|kv| (kv.key.to_string(), kv.value.clone()))
            .collect();
        values.sort_by(|a, b| a.0.cmp(&b.0));
        values
    }
    // Keep collection rounds separate: order within a collection is unspecified,
    // while order across Delta collections is meaningful.
    type Samples = BTreeMap<String, Vec<Vec<Sample>>>;
    #[derive(Clone, Debug)]
    struct Sink(Arc<Mutex<Samples>>, &'static [&'static str]);
    impl Sink {
        fn new(names: &'static [&'static str]) -> Self {
            Self(Arc::new(Mutex::new(BTreeMap::new())), names)
        }
    }
    impl PushMetricExporter for Sink {
        async fn export(
            &self,
            metrics: &ResourceMetrics,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            let mut samples = self.0.lock().unwrap();
            for metric in metrics.scope_metrics().flat_map(|scope| scope.metrics()) {
                // GatewayMetrics also registers observable gauges. Only collect
                // this scenario's instruments; keep type checks strict for them.
                if !self.1.contains(&metric.name()) {
                    continue;
                }
                let mut rows = Vec::new();
                match metric.data() {
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            rows.push(Sample {
                                unit: metric.unit().to_owned(),
                                temporality: histogram.temporality(),
                                attributes: sorted_attributes(point.attributes()),
                                observation: Observation::F64Histogram {
                                    bounds: point.bounds().collect(),
                                    buckets: point.bucket_counts().collect(),
                                    count: point.count(),
                                    sum: point.sum(),
                                    min: point.min(),
                                    max: point.max(),
                                },
                            });
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            rows.push(Sample {
                                unit: metric.unit().to_owned(),
                                temporality: histogram.temporality(),
                                attributes: sorted_attributes(point.attributes()),
                                observation: Observation::U64Histogram {
                                    bounds: point.bounds().collect(),
                                    buckets: point.bucket_counts().collect(),
                                    count: point.count(),
                                    sum: point.sum(),
                                    min: point.min(),
                                    max: point.max(),
                                },
                            });
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            rows.push(Sample {
                                unit: metric.unit().to_owned(),
                                temporality: sum.temporality(),
                                attributes: sorted_attributes(point.attributes()),
                                observation: Observation::Counter(point.value()),
                            });
                        }
                    }
                    _ => panic!("unexpected aggregation"),
                }
                rows.sort_by_key(|row| format!("{:?}", row.attributes));
                if !rows.is_empty() {
                    samples
                        .entry(metric.name().to_owned())
                        .or_default()
                        .push(rows);
                }
            }
            Ok(())
        }
        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(&self, _: Duration) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
        fn temporality(&self) -> Temporality {
            Temporality::Delta
        }
    }

    // Protects the actual SDK wire observations, including bound tracker reuse
    // after empty Delta collections and owner drop/rebind. Concurrent cold
    // registration must neither split a series nor lose a measurement.
    #[test]
    fn bound_db_series_preserve_sdk_outputs_across_delta_and_rebind() {
        let sink = Sink::new(&[
            "plain.duration",
            "bound.duration",
            "plain.count",
            "bound.count",
        ]);
        let reader = PeriodicReader::builder(sink.clone())
            .with_interval(Duration::from_secs(86400))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("bound-db-regression");
        let plain_hist = meter
            .f64_histogram("plain.duration")
            .with_unit("ms")
            .with_boundaries(vec![0.5, 1.5, 3.0])
            .build();
        let bound_hist = meter
            .f64_histogram("bound.duration")
            .with_unit("ms")
            .with_boundaries(vec![0.5, 1.5, 3.0])
            .build();
        let plain_count = meter
            .u64_counter("plain.count")
            .with_unit("{operation}")
            .build();
        let bound_count = meter
            .u64_counter("bound.count")
            .with_unit("{operation}")
            .build();
        let attributes = [
            KeyValue::new("db.system.name", "sqlite"),
            KeyValue::new("db.pool.role", "reader"),
            KeyValue::new("db.scheduling.class", "maintenance"),
        ];
        for round in 0..2 {
            let hist = Arc::new(DbHistogram::new(bound_hist.clone(), 6));
            let count = Arc::new(DbCounter::new(bound_count.clone(), 6));
            std::thread::scope(|scope| {
                for value in [0.25, 1.0, 2.0, 4.0, 0.25, 1.0, 2.0, 4.0] {
                    let hist = hist.clone();
                    let count = count.clone();
                    let attributes = &attributes;
                    let plain_hist = &plain_hist;
                    let plain_count = &plain_count;
                    scope.spawn(move || {
                        plain_hist.record(value, attributes);
                        plain_count.add(1, attributes);
                        hist.record(value, attributes);
                        count.add(1, attributes);
                    });
                }
            });
            provider.force_flush().unwrap();
            {
                let samples = sink.0.lock().unwrap();
                assert_eq!(samples["plain.duration"], samples["bound.duration"]);
                assert_eq!(samples["plain.count"], samples["bound.count"]);
                assert_eq!(samples["bound.duration"].len(), round + 1);
                assert_eq!(
                    samples["bound.duration"][round],
                    vec![Sample {
                        unit: "ms".into(),
                        temporality: Temporality::Delta,
                        attributes: sorted_attributes(attributes.iter()),
                        observation: Observation::F64Histogram {
                            bounds: vec![0.5, 1.5, 3.0],
                            buckets: vec![2, 2, 2, 2],
                            count: 8,
                            sum: 14.5,
                            min: Some(0.25),
                            max: Some(4.0),
                        },
                    }]
                );
                assert_eq!(
                    samples["bound.count"][round],
                    vec![Sample {
                        unit: "{operation}".into(),
                        temporality: Temporality::Delta,
                        attributes: sorted_attributes(attributes.iter()),
                        observation: Observation::Counter(8),
                    }]
                );
            }
            // Empty collection must emit no synthetic zero for a bound series.
            provider.force_flush().unwrap();
            assert_eq!(sink.0.lock().unwrap()["bound.duration"].len(), round + 1);
            drop(hist);
            drop(count);
            provider.force_flush().unwrap();
        }
        provider.shutdown().unwrap();
    }
    // Exercises actual Gateway instrument construction, U64 aggregation and
    // independent finite slots, rather than an artificial F64-only instrument.
    #[test]
    fn admission_u64_cohorts_keep_representation_across_warm_delta_rounds() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let _reset = ConsentReset(crate::telemetry_enabled());
        crate::set_telemetry_enabled(true);
        let sink = Sink::new(&[
            "pioneer.gateway.db.read.admission.queue.depth",
            "pioneer.gateway.db.read.admission.active",
            "pioneer.gateway.db.read.admission.events",
        ]);
        let reader = PeriodicReader::builder(sink.clone())
            .with_interval(Duration::from_secs(86400))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let state = crate::telemetry::tests::gateway_test_state(provider.clone());
        let labels = |event| {
            [
                KeyValue::new("db.system.name", "sqlite"),
                KeyValue::new("db.pool.role", "reader"),
                KeyValue::new("db.admission.event", event),
                KeyValue::new("db.admission.class", "maintenance"),
            ]
        };
        for round in 0..2 {
            // Two different event cohorts on each instrument, cold then warm.
            for (event, depths, active) in
                [("enqueued", [1, 4], [0, 1]), ("acquired", [0, 2], [1, 1])]
            {
                for (depth, active) in depths.into_iter().zip(active) {
                    crate::telemetry::tests::with_test_state(state, || {
                        record_database_read_admission(DatabaseReadAdmissionMetric {
                            event,
                            class: "maintenance",
                            queue_depth: depth,
                            active,
                            waited: None,
                            held: None,
                        })
                    });
                }
            }
            provider.force_flush().unwrap();
            let samples = sink.0.lock().unwrap();
            let expected = |event, bounds, buckets, sum, min, max| Sample {
                unit: "{request}".into(),
                temporality: Temporality::Delta,
                attributes: sorted_attributes(labels(event).iter()),
                observation: Observation::U64Histogram {
                    bounds,
                    buckets,
                    count: 2,
                    sum,
                    min: Some(min),
                    max: Some(max),
                },
            };
            let depth = &samples["pioneer.gateway.db.read.admission.queue.depth"];
            let active = &samples["pioneer.gateway.db.read.admission.active"];
            assert_eq!(depth.len(), round + 1);
            assert_eq!(active.len(), round + 1);
            let bounds = vec![
                0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
            ];
            assert_eq!(
                depth[round],
                vec![
                    expected(
                        "acquired",
                        bounds.clone(),
                        vec![1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        2,
                        0,
                        2
                    ),
                    expected(
                        "enqueued",
                        bounds,
                        vec![0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                        5,
                        1,
                        4
                    ),
                ]
            );
            assert_eq!(
                active[round],
                vec![
                    expected("acquired", vec![0.0, 1.0], vec![0, 2, 0], 2, 1, 1),
                    expected("enqueued", vec![0.0, 1.0], vec![1, 1, 0], 1, 0, 1),
                ]
            );
            assert_eq!(
                samples["pioneer.gateway.db.read.admission.events"][round],
                vec![
                    Sample {
                        unit: "{event}".into(),
                        temporality: Temporality::Delta,
                        attributes: sorted_attributes(labels("acquired").iter()),
                        observation: Observation::Counter(2)
                    },
                    Sample {
                        unit: "{event}".into(),
                        temporality: Temporality::Delta,
                        attributes: sorted_attributes(labels("enqueued").iter()),
                        observation: Observation::Counter(2)
                    },
                ]
            );
            drop(samples);
            provider.force_flush().unwrap();
            assert_eq!(
                sink.0.lock().unwrap()["pioneer.gateway.db.read.admission.active"].len(),
                round + 1
            );
        }
        provider.force_flush().unwrap();
        provider.shutdown().unwrap();
    }

    #[test]
    fn sdk_exhausted_bound_owner_stays_overflow_until_drop_and_rebind() {
        use opentelemetry_sdk::metrics::Stream;
        let sink = Sink::new(&["bounded.count"]);
        let reader = PeriodicReader::builder(sink.clone())
            .with_interval(Duration::from_secs(86400))
            .build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_view(|_: &opentelemetry_sdk::metrics::Instrument| {
                Stream::builder().with_cardinality_limit(1).build().ok()
            })
            .build();
        let instrument = provider
            .meter("bound-lifetime")
            .u64_counter("bounded.count")
            .with_unit("{operation}")
            .build();
        let a = [KeyValue::new("cohort", "a")];
        let b = [KeyValue::new("cohort", "b")];
        let first = instrument.bind(&a);
        let rejected = instrument.bind(&b);
        first.add(1);
        rejected.add(2);
        provider.force_flush().unwrap();
        let expected = |attributes, value| Sample {
            unit: "{operation}".into(),
            temporality: Temporality::Delta,
            attributes,
            observation: Observation::Counter(value),
        };
        let overflow = vec![(
            "otel.metric.overflow".into(),
            opentelemetry::Value::Bool(true),
        )];
        assert_eq!(
            sink.0.lock().unwrap()["bounded.count"][0],
            vec![
                expected(sorted_attributes(a.iter()), 1),
                expected(overflow.clone(), 2),
            ]
        );
        drop(first);
        // Evict the now-unbound stale A tracker. B's owner still points to overflow.
        provider.force_flush().unwrap();
        rejected.add(3);
        provider.force_flush().unwrap();
        assert_eq!(
            sink.0.lock().unwrap()["bounded.count"][1],
            vec![expected(overflow, 3)]
        );
        drop(rejected);
        provider.force_flush().unwrap();
        let rebound = instrument.bind(&b);
        rebound.add(4);
        provider.force_flush().unwrap();
        assert_eq!(
            sink.0.lock().unwrap()["bounded.count"][2],
            vec![expected(sorted_attributes(b.iter()), 4)]
        );
        drop(rebound);
        provider.force_flush().unwrap();
        assert_eq!(sink.0.lock().unwrap()["bounded.count"].len(), 3);
        provider.shutdown().unwrap();
    }

    #[test]
    fn sql_limiter_bound_owners_export_admitted_and_metric_overflow_identities() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let _reset = ConsentReset(crate::telemetry_enabled());
        crate::set_telemetry_enabled(true);
        let sink = Sink::new(&[
            "pioneer.gateway.db.operations",
            "pioneer.gateway.db.operation.duration",
        ]);
        let reader = PeriodicReader::builder(sink.clone())
            .with_interval(Duration::from_secs(86400))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let state = crate::telemetry::tests::gateway_test_state(provider.clone());
        use crate::query_fingerprint_fixture::{QueryFingerprintLimiter, QueryFingerprintState};
        let limiter = QueryFingerprintLimiter {
            max_raw_query_cache_entries: 4,
            max_fingerprints: 4,
            max_metric_series: 1,
            state: Mutex::new(QueryFingerprintState::default()),
        };
        let lookup = |sql| {
            limiter.metric_fingerprint(
                sql,
                DatabaseRole::Reader,
                DatabaseOperation::Select,
                crate::DatabaseWorkload::TimelinePage,
                false,
            )
        };
        let admitted = lookup("SELECT admitted FROM fixture");
        let overflow = lookup("SELECT overflow FROM fixture");
        assert!(
            admitted
                .canonical
                .is_some_and(|fingerprint| fingerprint != 0)
        );
        assert_eq!(Some(admitted.metric), admitted.canonical);
        assert!(
            overflow
                .canonical
                .is_some_and(|fingerprint| fingerprint != 0)
        );
        assert_ne!(overflow.canonical, admitted.canonical);
        assert_eq!(
            overflow.metric, 0,
            "metric budget overflow must not erase canonical CPU identity"
        );
        let admitted_label = admitted.metric.to_string();
        let admitted_owner = Arc::downgrade(&admitted.series);
        let overflow_owner = Arc::downgrade(&overflow.series);
        for round in 0..2 {
            crate::telemetry::tests::with_test_state(state, || {
                for sql in [
                    "SELECT admitted FROM fixture",
                    "SELECT overflow FROM fixture",
                ] {
                    // The same production limiter lookup supplies the metric identity
                    // and the bound owner; SDK bind runs after its lock is released.
                    let identity = lookup(sql);
                    for elapsed in [Duration::from_micros(250), Duration::from_millis(2)] {
                        identity.series.record(DatabaseOperationMetric {
                            role: DatabaseRole::Reader,
                            operation: DatabaseOperation::Select,
                            workload: crate::DatabaseWorkload::TimelinePage,
                            query_fingerprint: identity.metric,
                            elapsed,
                            failed: false,
                        });
                    }
                }
            });
            provider.force_flush().unwrap();
            let samples = sink.0.lock().unwrap();
            for (name, unit) in [
                ("pioneer.gateway.db.operations", "{operation}"),
                ("pioneer.gateway.db.operation.duration", "ms"),
            ] {
                let rows = &samples[name];
                assert_eq!(rows.len(), round + 1);
                assert_eq!(rows[round].len(), 2);
                for (sample, fingerprint) in rows[round].iter().zip(["0", admitted_label.as_str()])
                {
                    assert_eq!(sample.unit, unit);
                    assert_eq!(sample.temporality, Temporality::Delta);
                    let mut labels = vec![
                        ("db.system.name", "sqlite"),
                        ("db.pool.role", "reader"),
                        ("db.operation.name", "select"),
                        ("db.operation.type", "read"),
                        ("db.workload.name", "timeline.page"),
                        ("db.query.fingerprint", fingerprint),
                        ("outcome", "ok"),
                    ];
                    labels.sort();
                    assert_eq!(
                        sample.attributes,
                        labels
                            .into_iter()
                            .map(|(k, v)| (
                                k.to_owned(),
                                if k == "db.query.fingerprint" {
                                    opentelemetry::Value::I64(v.parse().unwrap())
                                } else {
                                    opentelemetry::Value::String(v.to_owned().into())
                                }
                            ))
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(
                        sample.observation,
                        if name.ends_with("operations") {
                            Observation::Counter(2)
                        } else {
                            Observation::F64Histogram {
                                bounds: vec![
                                    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0,
                                    500.0, 1000.0, 2500.0, 5000.0,
                                ],
                                buckets: vec![0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                                count: 2,
                                sum: 2.25,
                                min: Some(0.25),
                                max: Some(2.0),
                            }
                        }
                    );
                }
            }
            drop(samples);
            provider.force_flush().unwrap();
            assert_eq!(
                sink.0.lock().unwrap()["pioneer.gateway.db.operations"].len(),
                round + 1
            );
        }
        drop(limiter);
        drop(admitted);
        drop(overflow);
        assert!(admitted_owner.upgrade().is_none());
        assert!(overflow_owner.upgrade().is_none());
        provider.force_flush().unwrap();
        provider.shutdown().unwrap();
    }
}
