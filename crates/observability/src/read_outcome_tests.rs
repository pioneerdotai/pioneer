//! Exported observations for the reader callback-entry consent epoch.
use crate::metrics::{DatabaseReadMetric, record_database_read_for_consent};
use crate::telemetry::{
    self,
    tests::{gateway_test_state, with_test_state},
};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, PartialEq)]
struct Point {
    name: String,
    unit: String,
    attributes: BTreeMap<String, String>,
    temporality: Temporality,
    count: u64,
    duration_sum: Option<f64>,
}
#[derive(Clone, Debug, Default)]
struct Output(Arc<Mutex<Vec<Point>>>);
impl PushMetricExporter for Output {
    async fn export(&self, metrics: &ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
        let mut output = self.0.lock().unwrap();
        for metric in metrics.scope_metrics().flat_map(|scope| scope.metrics()) {
            if !matches!(
                metric.name(),
                "pioneer.gateway.db.read.operations" | "pioneer.gateway.db.read.duration"
            ) {
                continue;
            }
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    for point in sum.data_points() {
                        output.push(Point {
                            name: metric.name().into(),
                            unit: metric.unit().into(),
                            attributes: point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                .collect(),
                            temporality: sum.temporality(),
                            count: point.value(),
                            duration_sum: None,
                        });
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    for point in histogram.data_points() {
                        output.push(Point {
                            name: metric.name().into(),
                            unit: metric.unit().into(),
                            attributes: point
                                .attributes()
                                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                                .collect(),
                            temporality: histogram.temporality(),
                            count: point.count(),
                            duration_sum: Some(point.sum()),
                        });
                    }
                }
                _ => panic!("unexpected read metric representation"),
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

#[test]
fn delayed_read_completion_never_promotes_stale_or_disabled_callback_epoch() {
    let _lock = telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            crate::set_telemetry_gate(self.0);
        }
    }
    let _restore = Restore(crate::telemetry_enabled());
    crate::set_telemetry_gate(true);
    let original_epoch = crate::telemetry_consent_snapshot().1;
    let output = Output::default();
    let reader = PeriodicReader::builder(output.clone())
        .with_interval(Duration::from_secs(86400))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let state = gateway_test_state(provider.clone());
    with_test_state(state, || {
        crate::set_telemetry_gate(false);
        telemetry::force_flush_observability().unwrap();
        // Model callbacks paused after entry with G or None. Re-enable only
        // after the real closed-gate discard completed, as production does.
        crate::set_telemetry_gate(true);
        let fresh_epoch = crate::telemetry_consent_snapshot().1;
        assert_ne!(fresh_epoch, original_epoch);
        for outcome in ["ok", "error", "cancelled"] {
            let metric = DatabaseReadMetric {
                class: "maintenance",
                outcome,
                elapsed: Duration::from_millis(2),
            };
            record_database_read_for_consent(metric, Some(original_epoch));
            record_database_read_for_consent(metric, None);
            provider.force_flush().unwrap();
            assert!(
                output.0.lock().unwrap().is_empty(),
                "stale and off-entry callbacks must publish no observations"
            );
            record_database_read_for_consent(metric, Some(fresh_epoch));
            provider.force_flush().unwrap();
            let mut points = std::mem::take(&mut *output.0.lock().unwrap());
            points.sort_by(|a, b| a.name.cmp(&b.name));
            let attributes = BTreeMap::from([
                ("db.system.name".into(), "sqlite".into()),
                ("db.pool.role".into(), "reader".into()),
                ("db.scheduling.class".into(), "maintenance".into()),
                ("outcome".into(), outcome.into()),
            ]);
            assert_eq!(
                points,
                vec![
                    Point {
                        name: "pioneer.gateway.db.read.duration".into(),
                        unit: "ms".into(),
                        attributes: attributes.clone(),
                        temporality: Temporality::Delta,
                        count: 1,
                        duration_sum: Some(2.0)
                    },
                    Point {
                        name: "pioneer.gateway.db.read.operations".into(),
                        unit: "{operation}".into(),
                        attributes,
                        temporality: Temporality::Delta,
                        count: 1,
                        duration_sum: None
                    },
                ]
            );
            provider.force_flush().unwrap();
            assert!(
                output.0.lock().unwrap().is_empty(),
                "empty Delta must not replay reader observations"
            );
        }
    });
    provider.shutdown().unwrap();
}
