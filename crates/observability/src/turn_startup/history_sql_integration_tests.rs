//! R1: real startup stages -> exact SQLite poll/drop adapter -> real SeaORM
//! callback installer + limiter -> completed SDK history.capture attributes.
use super::{Input, Outcome, Runtime, Stage};
use crate::DatabaseRole;
use crate::gateway_query_observer_fixture::install_query_observer;
use crate::pool_context::PoolContext::ReaderInteractive;
use crate::query_fingerprint_fixture::{QueryFingerprintLimiter, QueryFingerprintState};
use crate::sqlite_query_scope::{PoolFutureExt, open_stream};
use crate::telemetry::tests::{gateway_test_state_with_tracer, with_test_state};
use futures_util::StreamExt;
use opentelemetry::trace::{SpanId, TraceContextExt};
use opentelemetry::{Array, Context, KeyValue, Value};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
    StreamTrait,
};
use std::sync::{Arc, Mutex};

const FIRST: &str = "SELECT 1 AS owner_a UNION ALL SELECT 2";
const SECOND: &str = "SELECT 3 AS owner_b";

#[derive(Clone, Debug, Default)]
struct Spans(Arc<Mutex<Vec<SpanData>>>);
impl SpanExporter for Spans {
    async fn export(&self, spans: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().unwrap().extend(spans);
        Ok(())
    }
}
impl Spans {
    fn capture(&self, id: SpanId) -> SpanData {
        let matching: Vec<_> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|span| span.span_context.span_id() == id)
            .cloned()
            .collect();
        assert_eq!(matching.len(), 1, "one completed SDK span for this capture");
        assert_eq!(matching[0].name.as_ref(), "history.capture");
        matching[0].clone()
    }
}

// Only private, in-memory providers. No OTLP, metric readers, resource samplers,
// global provider installation, or query_cpu session. Drop restores on panic too.
fn with_sdk(work: impl FnOnce(&Spans)) {
    let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
    struct Reset {
        enabled: bool,
        meter: SdkMeterProvider,
        tracer: SdkTracerProvider,
    }
    impl Drop for Reset {
        fn drop(&mut self) {
            let _ = self.tracer.shutdown();
            let _ = self.meter.shutdown();
            crate::set_telemetry_gate(self.enabled);
        }
    }
    let spans = Spans::default();
    let reset = Reset {
        enabled: crate::telemetry_enabled(),
        meter: SdkMeterProvider::builder().build(),
        tracer: SdkTracerProvider::builder()
            .with_simple_exporter(spans.clone())
            .build(),
    };
    crate::set_telemetry_gate(true);
    let state = gateway_test_state_with_tracer(reset.meter.clone(), reset.tracer.clone());
    with_test_state(state, || work(&spans));
}

struct Startup(&'static str);
impl Startup {
    fn begin(key: &'static str) -> Self {
        super::begin(key, Input::Text, Runtime::Native);
        Self(key)
    }
}
impl Drop for Startup {
    fn drop(&mut self) {
        super::finish(self.0, Outcome::CompletedWithoutOutput);
        // begin's registered ACTIVE lease is retired by finish. Remove only our
        // own registry key so later tests cannot inherit a closed fixture root.
        super::registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(self.0);
    }
}
fn current_span_id() -> SpanId {
    Context::current().span().span_context().span_id()
}
fn statement(sql: &str) -> Statement {
    Statement::from_string(DbBackend::Sqlite, sql.to_owned())
}
fn limiter() -> Arc<QueryFingerprintLimiter> {
    Arc::new(QueryFingerprintLimiter {
        max_raw_query_cache_entries: 4,
        max_fingerprints: 4,
        // Every actual callback must bridge canonical identity despite metric=0.
        max_metric_series: 0,
        state: Mutex::new(QueryFingerprintState::default()),
    })
}
fn identity(limiter: &QueryFingerprintLimiter, sql: &str) -> String {
    let identity = limiter.metric_fingerprint(
        sql,
        DatabaseRole::Reader,
        crate::DatabaseOperation::Select,
        crate::DatabaseWorkload::Unclassified,
        false,
    );
    assert_eq!(identity.metric, 0);
    let canonical = identity.canonical.expect("canonical budget has room");
    assert_ne!(canonical, 0);
    format!("{canonical:016x}")
}
async fn connection(limiter: Arc<QueryFingerprintLimiter>) -> DatabaseConnection {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1).min_connections(1);
    let mut connection = Database::connect(options).await.unwrap();
    connection
        .execute_unprepared("PRAGMA query_only=ON")
        .await
        .unwrap();
    // Exact production set_metric_callback, not an alternate test callback.
    // Deliberately do not register the returned weak resource observer.
    let _observer = install_query_observer(&mut connection, DatabaseRole::Reader, limiter);
    connection
}
fn expect_summary(span: &SpanData, fingerprint: Option<&str>, count: i64) {
    let attrs = &span.attributes;
    let fingerprints = fingerprint.into_iter().map(Into::into).collect();
    assert!(attrs.contains(&KeyValue::new(
        "history.sql.top.fingerprints",
        Array::String(fingerprints)
    )));
    assert!(attrs.contains(&KeyValue::new(
        "history.sql.top.counts",
        Array::I64(if count == 0 { vec![] } else { vec![count] })
    )));
    assert!(attrs.contains(&KeyValue::new("history.sql.observed.count", count)));
    assert!(attrs.contains(&KeyValue::new("history.sql.exported.count", count)));
    for category in ["unknown", "overflow", "omitted"] {
        assert!(attrs.contains(&KeyValue::new(
            format!("history.sql.{category}.count"),
            0_i64
        )));
    }
    assert!(attrs.contains(&KeyValue::new("history.sql.identity_complete", true)));
    assert!(attrs.contains(&KeyValue::new("stage.completion", "returned")));
    let elapsed = |key: &str| match &attrs
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .unwrap()
        .value
    {
        Value::F64(ms) => *ms,
        other => panic!("expected elapsed_ms, got {other:?}"),
    };
    let observed = elapsed("history.sql.observed.elapsed_ms");
    assert!(observed.is_finite() && observed >= 0.);
    assert_eq!(observed, elapsed("history.sql.exported.elapsed_ms"));
    let top = attrs
        .iter()
        .find(|kv| kv.key.as_str() == "history.sql.top.elapsed_ms")
        .unwrap();
    assert_eq!(
        top.value,
        Value::Array(Array::F64(if count == 0 { vec![] } else { vec![observed] }))
    );
}

// Detects removal of stage-owned recorder creation, callback installation/bridge,
// canonical-vs-metric confusion, nested double counting, or PoolFuture's saved
// owner. Recorder-only tests bypass all four production boundaries.
#[test]
fn real_sql_callback_uses_startup_capture_and_canonical_identity_across_foreign_poll() {
    with_sdk(|spans| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let limiter = limiter();
                let first_fp = identity(&limiter, FIRST);
                let second_fp = identity(&limiter, SECOND);
                assert_ne!(first_fp, second_fp);
                let db = connection(limiter).await;
                let first = Startup::begin("sql-attribution-scalar-first");
                let second = Startup::begin("sql-attribution-scalar-second");
                let (first_id, second_id) =
                    super::scope_stage(Some(first.0.into()), Stage::HistoryCapture, async {
                        let first_id = current_span_id();
                        super::scope_current_stage(Stage::HistoryLoad, async {
                            let rows =
                                crate::query_cpu::observe(db.query_all_raw(statement(FIRST)))
                                    .in_pool(ReaderInteractive)
                                    .await
                                    .unwrap();
                            assert_eq!(rows.len(), 2);
                        })
                        .await;
                        // Construct under A, poll under B: the production PoolFuture
                        // must keep A even though B and its ordinary nested phase run.
                        let owned_by_first =
                            crate::query_cpu::observe(db.query_all_raw(statement(FIRST)))
                                .in_pool(ReaderInteractive);
                        let second_id = super::scope_stage(
                            Some(second.0.into()),
                            Stage::HistoryCapture,
                            async {
                                let second_id = current_span_id();
                                super::scope_current_stage(Stage::HistoryLoad, async {
                                    assert_eq!(owned_by_first.await.unwrap().len(), 2);
                                    let row = crate::query_cpu::observe(
                                        db.query_one_raw(statement(SECOND)),
                                    )
                                    .in_pool(ReaderInteractive)
                                    .await
                                    .unwrap()
                                    .unwrap();
                                    assert_eq!(row.try_get::<i64>("", "owner_b").unwrap(), 3);
                                })
                                .await;
                                second_id
                            },
                        )
                        .await;
                        (first_id, second_id)
                    })
                    .await;
                expect_summary(&spans.capture(first_id), Some(&first_fp), 2);
                expect_summary(&spans.capture(second_id), Some(&second_fp), 1);
                db.close().await.unwrap();
            });
    });
}

// EOF destroys the real MetricStream inside DiagnosticStream::poll_next; early
// drop uses DiagnosticStream::drop instead. An absent creator exercises shadowing
// of foreign ambient context. Removing either guard must misattribute a callback.
#[test]
fn real_metric_stream_keeps_creator_or_absence_at_eof_and_early_drop() {
    with_sdk(|spans| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let limiter = limiter();
                let first_fp = identity(&limiter, FIRST);
                let second_fp = identity(&limiter, SECOND);
                let db = connection(limiter).await;
                for owned in [true, false] {
                    for eof in [true, false] {
                        let first = Startup::begin("sql-attribution-stream-first");
                        let second = Startup::begin("sql-attribution-stream-second");
                        let (first_id, second_id) = super::scope_stage(
                            Some(first.0.into()),
                            Stage::HistoryCapture,
                            async {
                                let first_id = current_span_id();
                                let open = || async {
                                    open_stream(db.stream_raw(statement(FIRST)))
                                        .in_pool(ReaderInteractive)
                                        .await
                                        .unwrap()
                                };
                                let mut stream = if owned {
                                    open().await
                                } else {
                                    // Construct the adapter itself outside startup, so it
                                    // saves None rather than A before context is cleared.
                                    super::scope(None, async { open().await }).await
                                };
                                assert_eq!(
                                    stream
                                        .next()
                                        .await
                                        .unwrap()
                                        .unwrap()
                                        .try_get::<i64>("", "owner_a")
                                        .unwrap(),
                                    1
                                );
                                let second_id = super::scope_stage(
                                    Some(second.0.into()),
                                    Stage::HistoryCapture,
                                    async {
                                        let second_id = current_span_id();
                                        super::scope_current_stage(Stage::HistoryLoad, async {
                                            if eof {
                                                assert_eq!(
                                                    stream
                                                        .next()
                                                        .await
                                                        .unwrap()
                                                        .unwrap()
                                                        .try_get::<i64>("", "owner_a")
                                                        .unwrap(),
                                                    2
                                                );
                                                assert!(stream.next().await.is_none());
                                            }
                                            drop(stream);
                                            // Also proves adapter scope restoration: this is
                                            // B's callback, not A's or the saved absent owner.
                                            crate::query_cpu::observe(
                                                db.query_one_raw(statement(SECOND)),
                                            )
                                            .in_pool(ReaderInteractive)
                                            .await
                                            .unwrap()
                                            .unwrap();
                                        })
                                        .await;
                                        second_id
                                    },
                                )
                                .await;
                                (first_id, second_id)
                            },
                        )
                        .await;
                        expect_summary(
                            &spans.capture(first_id),
                            owned.then_some(first_fp.as_str()),
                            i64::from(owned),
                        );
                        expect_summary(&spans.capture(second_id), Some(&second_fp), 1);
                    }
                }
                db.close().await.unwrap();
            });
    });
}
