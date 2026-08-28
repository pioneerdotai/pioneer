use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context as TracingContext, Layer};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseRole {
    Shared,
    Reader,
    Writer,
}

impl DatabaseRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Reader => "reader",
            Self::Writer => "writer",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl DatabaseOperation {
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

/// One low-cardinality observation emitted by the Gateway's fair SQLite
/// admission controller. Values are selected from finite runtime enums; no
/// SQL, IDs, paths, or error text are accepted here.
#[derive(Clone, Copy, Debug)]
pub struct DatabaseAdmissionMetric {
    pub event: &'static str,
    pub class: &'static str,
    pub reason: &'static str,
    pub foreground_queue: u64,
    pub background_queue: u64,
    pub waited: Option<Duration>,
    pub held: Option<Duration>,
}

pub(crate) struct GatewayMetrics {
    pub(crate) meter: Meter,
    pub(crate) database_operations: Counter<u64>,
    pub(crate) database_operation_duration: Histogram<f64>,
    pub(crate) database_pool_acquire_duration: Histogram<f64>,
    pub(crate) database_admission_events: Counter<u64>,
    pub(crate) database_admission_wait_duration: Histogram<f64>,
    pub(crate) database_admission_quantum_duration: Histogram<f64>,
    pub(crate) database_admission_queue_depth: Histogram<u64>,
    pub(crate) provider_warmup_duration: Histogram<f64>,
    pub(crate) provider_warmup_stage_duration: Histogram<f64>,
    pub(crate) provider_warmup_failures: Counter<u64>,
    pub(crate) provider_readiness_checks: Counter<u64>,
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
        let database_admission_events = meter
            .u64_counter("pioneer.gateway.db.admission.events")
            .with_description(
                "Fair SQLite admission events by bounded class, event, and grant reason",
            )
            .with_unit("{event}")
            .build();
        let database_admission_wait_duration = meter
            .f64_histogram("pioneer.gateway.db.admission.wait.duration")
            .with_description(
                "Time a foreground or background database quantum waits for fair admission",
            )
            .with_unit("ms")
            .with_boundaries(vec![
                0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
                1_000.0, 2_000.0, 5_000.0, 10_000.0, 30_000.0,
            ])
            .build();
        let database_admission_quantum_duration = meter
            .f64_histogram("pioneer.gateway.db.admission.quantum.duration")
            .with_description("Time SQLite admission is retained by one bounded database quantum")
            .with_unit("ms")
            .with_boundaries(vec![
                0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
                1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
            ])
            .build();
        let database_admission_queue_depth = meter
            .u64_histogram("pioneer.gateway.db.admission.queue.depth")
            .with_description("Foreground and background queue depth at SQLite admission events")
            .with_unit("{request}")
            .with_boundaries(vec![
                0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1_024.0,
            ])
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
            database_admission_events,
            database_admission_wait_duration,
            database_admission_quantum_duration,
            database_admission_queue_depth,
            provider_warmup_duration,
            provider_warmup_stage_duration,
            provider_warmup_failures,
            provider_readiness_checks,
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

fn record_database_pool_acquire(role: DatabaseRole, elapsed: Duration) {
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
            KeyValue::new("db.pool.role", role.as_str()),
        ],
    );
}

/// Consumes SQLx's pool-acquisition timing event without exposing it in normal logs.
pub(crate) struct DatabasePoolAcquireMetricsLayer;

impl<S> Layer<S> for DatabasePoolAcquireMetricsLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: TracingContext<'_, S>) {
        let mut visitor = PoolAcquireEventVisitor::default();
        event.record(&mut visitor);
        if let Some(seconds) = visitor
            .acquired_after_seconds
            .filter(|value| value.is_finite())
        {
            record_database_pool_acquire(
                DatabaseRole::Shared,
                Duration::from_secs_f64(seconds.max(0.0)),
            );
        }
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

pub fn record_database_operation(
    role: DatabaseRole,
    operation: DatabaseOperation,
    elapsed: Duration,
    failed: bool,
) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let attributes = [
        KeyValue::new("db.system.name", "sqlite"),
        KeyValue::new("db.pool.role", role.as_str()),
        KeyValue::new("db.operation.name", operation.as_str()),
        KeyValue::new("db.operation.type", operation.kind()),
        KeyValue::new("outcome", if failed { "error" } else { "ok" }),
    ];
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };
    metrics.database_operations.add(1, &attributes);
    metrics
        .database_operation_duration
        .record(elapsed.as_secs_f64() * 1_000.0, &attributes);
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
        KeyValue::new("db.admission.event", metric.event),
        KeyValue::new("db.admission.class", metric.class),
        KeyValue::new("db.admission.reason", metric.reason),
    ];
    metrics.database_admission_events.add(1, &attributes);
    metrics.database_admission_queue_depth.record(
        metric.foreground_queue,
        &[
            attributes[0].clone(),
            attributes[1].clone(),
            KeyValue::new("db.admission.queue.class", "foreground"),
        ],
    );
    metrics.database_admission_queue_depth.record(
        metric.background_queue,
        &[
            attributes[0].clone(),
            attributes[1].clone(),
            KeyValue::new("db.admission.queue.class", "background"),
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
