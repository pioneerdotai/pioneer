//! Actual SQLx event -> production adapter -> actual pool layer -> SDK output.
//! SqliteDatabase routing itself remains owned by SQLite's existing tests.
use crate::metrics::DatabasePoolAcquireMetricsLayer;
use crate::pool_context::PoolContext;
use crate::sqlite_query_scope::{PoolFutureExt, observe_control, open_stream};
use crate::telemetry::tests::{gateway_test_state, with_test_state};
use futures_util::StreamExt;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
    StreamTrait, TransactionTrait,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

const BOUNDS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0,
    5_000.0, 10_000.0, 30_000.0,
];
#[derive(Debug, PartialEq)]
struct Point {
    unit: String,
    attributes: BTreeMap<String, String>,
    temporality: Temporality,
    bounds: Vec<f64>,
    buckets: Vec<u64>,
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}
#[derive(Clone, Default, Debug)]
struct Output(Arc<Mutex<Vec<Point>>>);
impl PushMetricExporter for Output {
    async fn export(&self, metrics: &ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
        for metric in metrics.scope_metrics().flat_map(|scope| scope.metrics()) {
            if metric.name() != "pioneer.gateway.db.pool.acquire.duration" {
                continue;
            }
            let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
                panic!("pool acquisition must remain an F64 histogram");
            };
            for point in histogram.data_points() {
                self.0.lock().unwrap().push(Point {
                    unit: metric.unit().into(),
                    attributes: point
                        .attributes()
                        .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                        .collect(),
                    temporality: histogram.temporality(),
                    bounds: point.bounds().collect(),
                    buckets: point.bucket_counts().collect(),
                    count: point.count(),
                    sum: point.sum(),
                    min: point.min(),
                    max: point.max(),
                });
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

// Independent oracle: capture SQLx's actual field, not the outer query time or
// the adapter's TLS value. Only the production layer decides class/role.
#[derive(Clone, Default)]
struct Acquisitions(Arc<Mutex<Vec<f64>>>);
impl<S: Subscriber> Layer<S> for Acquisitions {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        struct Seconds(Option<f64>);
        impl Visit for Seconds {
            fn record_f64(&mut self, field: &Field, value: f64) {
                if field.name() == "acquired_after_secs" {
                    self.0 = Some(value);
                }
            }
            fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
        }
        let mut seconds = Seconds(None);
        event.record(&mut seconds);
        if let Some(seconds) = seconds.0 {
            self.0.lock().unwrap().push(seconds);
        }
    }
}
fn statement() -> Statement {
    Statement::from_string(
        DbBackend::Sqlite,
        "SELECT 1 AS value UNION ALL SELECT 2".to_owned(),
    )
}
async fn connection(url: String) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).min_connections(1);
    // Same SQLx timing configuration as Gateway, with type inferred to avoid
    // adding a direct log dependency just for LevelFilter.
    options.map_sqlx_sqlite_pool_opts(|pool| pool.acquire_time_level("trace".parse().unwrap()));
    Database::connect(options).await.unwrap()
}
fn expect_acquisition(
    provider: &SdkMeterProvider,
    output: &Output,
    events: &Acquisitions,
    role: &str,
    class: &str,
) {
    let seconds = std::mem::take(&mut *events.0.lock().unwrap());
    assert_eq!(seconds.len(), 1, "one real SQLx acquisition per boundary");
    assert!(seconds[0].is_finite() && seconds[0] >= 0.0);
    let ms = Duration::from_secs_f64(seconds[0]).as_secs_f64() * 1_000.0;
    let mut buckets = vec![0; BOUNDS.len() + 1];
    buckets[BOUNDS
        .iter()
        .position(|bound| ms <= *bound)
        .unwrap_or(BOUNDS.len())] = 1;
    provider.force_flush().unwrap();
    assert_eq!(
        std::mem::take(&mut *output.0.lock().unwrap()),
        vec![Point {
            unit: "ms".into(),
            attributes: BTreeMap::from([
                ("db.system.name".into(), "sqlite".into()),
                ("db.pool.role".into(), role.into()),
                ("db.scheduling.class".into(), class.into()),
            ]),
            temporality: Temporality::Delta,
            bounds: BOUNDS.to_vec(),
            buckets,
            count: 1,
            sum: ms,
            min: Some(ms),
            max: Some(ms),
        }]
    );
}
fn expect_no_acquisition(provider: &SdkMeterProvider, output: &Output, events: &Acquisitions) {
    assert!(
        events.0.lock().unwrap().is_empty(),
        "held connection must not reacquire"
    );
    provider.force_flush().unwrap();
    assert!(
        output.0.lock().unwrap().is_empty(),
        "empty Delta must remain empty"
    );
}

#[test]
fn sqlx_pool_adapter_exports_actual_acquisitions_for_scalar_stream_and_control() {
    let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            crate::set_telemetry_gate(self.0);
        }
    }
    let _restore = Restore(crate::telemetry_enabled());
    crate::set_telemetry_gate(true);
    let output = Output::default();
    let reader = PeriodicReader::builder(output.clone())
        .with_interval(Duration::from_secs(86400))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let state = gateway_test_state(provider.clone());
    let events = Acquisitions::default();
    let subscriber = tracing_subscriber::registry()
        .with(
            DatabasePoolAcquireMetricsLayer
                .with_filter(filter_fn(|m| m.target() == "sqlx::pool::acquire")),
        )
        .with(
            events
                .clone()
                .with_filter(filter_fn(|m| m.target() == "sqlx::pool::acquire")),
        );
    // A current-thread runtime keeps the private provider fixture on every
    // caller poll. SQLx worker activity is real, but emits no pool acquisitions.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    with_test_state(state, || {
        tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("pool-observer.sqlite");
                let writer = connection(format!("sqlite://{}?mode=rwc", path.display())).await;
                writer
                    .execute_unprepared("PRAGMA journal_mode=WAL")
                    .await
                    .unwrap();
                let reader = connection(format!("sqlite://{}?mode=ro", path.display())).await;
                reader
                    .execute_unprepared("PRAGMA query_only=ON")
                    .await
                    .unwrap();
                // Bootstrap is deliberately unscoped, so none of its actual events
                // acquire a fabricated class. Remove only the independent event log.
                events.0.lock().unwrap().clear();
                provider.force_flush().unwrap();
                assert!(output.0.lock().unwrap().is_empty());

                use PoolContext::*;
                for (context, pool, role, class) in [
                    (ReaderInteractive, &reader, "reader", "interactive"),
                    (ReaderMaintenance, &reader, "reader", "maintenance"),
                    (WriterCritical, &writer, "writer", "critical"),
                    (WriterInteractive, &writer, "writer", "interactive"),
                    (WriterMaintenance, &writer, "writer", "maintenance"),
                ] {
                    // Repeat after Delta reset to exercise the warm bound handle.
                    for _ in 0..2 {
                        crate::query_cpu::observe(pool.query_all_raw(statement()))
                            .in_pool(context)
                            .await
                            .unwrap();
                        expect_acquisition(&provider, &output, &events, role, class);
                        expect_no_acquisition(&provider, &output, &events);
                    }
                    observe_control(pool.ping(), "ping")
                        .in_pool(context)
                        .await
                        .unwrap();
                    expect_acquisition(&provider, &output, &events, role, class);
                    for commit in [false, true] {
                        let transaction = observe_control(pool.begin(), "begin")
                            .in_pool(context)
                            .await
                            .unwrap();
                        assert_eq!(events.0.lock().unwrap().len(), 1);
                        crate::query_cpu::observe(transaction.query_all_raw(statement()))
                            .in_pool(context)
                            .await
                            .unwrap();
                        if commit {
                            observe_control(transaction.commit(), "commit")
                                .in_pool(context)
                                .await
                                .unwrap();
                        } else {
                            observe_control(transaction.rollback(), "rollback")
                                .in_pool(context)
                                .await
                                .unwrap();
                        }
                        // Inspect/export only after releasing the held connection.
                        // Statements and commit/rollback must add no acquisition.
                        expect_acquisition(&provider, &output, &events, role, class);
                        expect_no_acquisition(&provider, &output, &events);
                    }

                    for eof in [false, true] {
                        let mut stream = open_stream(pool.stream_raw(statement()))
                            .in_pool(context)
                            .await
                            .unwrap();
                        assert!(stream.next().await.unwrap().is_ok());
                        if eof {
                            while let Some(row) = stream.next().await {
                                row.unwrap();
                            }
                        }
                        drop(stream);
                        expect_acquisition(&provider, &output, &events, role, class);
                        expect_no_acquisition(&provider, &output, &events);
                    }
                }
                assert!(crate::pool_context::current().is_none());
                // Actual foreign/unscoped SQLx event after all scoped operations:
                // it must not inherit the final writer-maintenance attribution.
                reader.query_all_raw(statement()).await.unwrap();
                assert_eq!(events.0.lock().unwrap().len(), 1);
                events.0.lock().unwrap().clear();
                expect_no_acquisition(&provider, &output, &events);
                reader.close().await.unwrap();
                writer.close().await.unwrap();
            })
        })
    });
    provider.shutdown().unwrap();
}
