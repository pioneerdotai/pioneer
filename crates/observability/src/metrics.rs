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

pub(crate) struct GatewayMetrics {
    pub(crate) meter: Meter,
    pub(crate) database_operations: Counter<u64>,
    pub(crate) database_operation_duration: Histogram<f64>,
    pub(crate) database_pool_acquire_duration: Histogram<f64>,
    pub(crate) startup_duration: Histogram<f64>,
    pub(crate) startup_stage_duration: Histogram<f64>,
    pub(crate) startup_failures: Counter<u64>,
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
        let startup_duration = meter
            .f64_histogram("pioneer.gateway.startup.duration")
            .with_description("Time from Gateway process entry until the Gateway is ready")
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let startup_stage_duration = meter
            .f64_histogram("pioneer.gateway.startup.stage.duration")
            .with_description("Duration of a stable, low-cardinality Gateway startup stage")
            .with_unit("ms")
            .with_boundaries(startup_duration_boundaries())
            .build();
        let startup_failures = meter
            .u64_counter("pioneer.gateway.startup.failures")
            .with_description("Number of Gateway startups that failed before readiness")
            .with_unit("{failure}")
            .build();

        Self {
            meter,
            database_operations,
            database_operation_duration,
            database_pool_acquire_duration,
            startup_duration,
            startup_stage_duration,
            startup_failures,
        }
    }
}

fn startup_duration_boundaries() -> Vec<f64> {
    vec![
        1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
        20_000.0, 30_000.0, 60_000.0, 120_000.0,
    ]
}

fn record_database_pool_acquire(role: DatabaseRole, elapsed: Duration) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    state.metrics.database_pool_acquire_duration.record(
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
    state.metrics.database_operations.add(1, &attributes);
    state
        .metrics
        .database_operation_duration
        .record(elapsed.as_secs_f64() * 1_000.0, &attributes);
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
    state
        .metrics
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
