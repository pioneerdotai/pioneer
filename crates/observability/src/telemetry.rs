use crate::metrics::GatewayMetrics;
use anyhow::{Context, Result, bail};
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::trace::{
    SdkTracer, SdkTracerProvider, SpanData, SpanExporter as SdkSpanExporter,
};
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;
use url::{Host, Url};

const SERVICE_NAME: &str = "pioneer-gateway";
const METER_NAME: &str = "pioneer.gateway";
const TRACER_NAME: &str = "pioneer.gateway";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtlpTelemetryConfig {
    pub metrics_endpoint: String,
    pub traces_endpoint: String,
    pub export_interval: Duration,
    pub export_timeout: Duration,
}

pub(crate) struct ObservabilityState {
    meter_provider: SdkMeterProvider,
    tracer_provider: SdkTracerProvider,
    pub(crate) tracer: SdkTracer,
    pub(crate) metrics: GatewayMetrics,
}

static OBSERVABILITY: OnceLock<ObservabilityState> = OnceLock::new();

pub fn init_otlp_observability(config: OtlpTelemetryConfig) -> Result<()> {
    validate_config(&config)?;
    if OBSERVABILITY.get().is_some() {
        return Ok(());
    }

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(config.metrics_endpoint.trim())
        .with_timeout(config.export_timeout)
        .with_temporality(Temporality::Delta)
        .build()
        .context("failed to build OTLP/HTTP metrics exporter")?;
    let metric_reader = PeriodicReader::builder(ConsentGatedMetricExporter {
        inner: metric_exporter,
    })
    .with_interval(config.export_interval)
    .build();

    let trace_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(config.traces_endpoint.trim())
        .with_timeout(config.export_timeout)
        .build()
        .context("failed to build OTLP/HTTP traces exporter")?;

    let resource = gateway_resource();
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(metric_reader)
        .build();
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(ConsentGatedSpanExporter {
            inner: trace_exporter,
        })
        .build();
    let metrics = GatewayMetrics::new(meter_provider.meter(METER_NAME));
    let tracer = tracer_provider.tracer(TRACER_NAME);

    OBSERVABILITY
        .set(ObservabilityState {
            meter_provider,
            tracer_provider,
            tracer,
            metrics,
        })
        .map_err(|_| anyhow::anyhow!("OTLP observability pipeline was initialized concurrently"))
}

pub fn shutdown_observability(timeout: Duration) -> Result<()> {
    let Some(state) = OBSERVABILITY.get() else {
        return Ok(());
    };

    let trace_result = state
        .tracer_provider
        .shutdown_with_timeout(timeout)
        .context("failed to shut down OTLP traces pipeline");
    let metrics_result = state
        .meter_provider
        .shutdown_with_timeout(timeout)
        .context("failed to shut down OTLP metrics pipeline");

    match (trace_result, metrics_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(trace_error), Ok(())) => Err(trace_error),
        (Ok(()), Err(metrics_error)) => Err(metrics_error),
        (Err(trace_error), Err(metrics_error)) => Err(anyhow::anyhow!(
            "{trace_error:#}; additionally, {metrics_error:#}"
        )),
    }
}

pub(crate) fn state() -> Option<&'static ObservabilityState> {
    OBSERVABILITY.get()
}

fn gateway_resource() -> Resource {
    let deployment_environment = if cfg!(debug_assertions) {
        "development"
    } else {
        "production"
    };
    Resource::builder_empty()
        .with_service_name(SERVICE_NAME)
        .with_attributes([
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment.name", deployment_environment),
        ])
        .build()
}

fn validate_config(config: &OtlpTelemetryConfig) -> Result<()> {
    validate_endpoint(config.metrics_endpoint.as_str(), "metrics")?;
    validate_endpoint(config.traces_endpoint.as_str(), "traces")?;
    if !(Duration::from_secs(5)..=Duration::from_secs(15 * 60)).contains(&config.export_interval) {
        bail!("OTLP metrics export interval must be between 5 seconds and 15 minutes");
    }
    if !(Duration::from_millis(100)..=Duration::from_secs(30)).contains(&config.export_timeout) {
        bail!("OTLP export timeout must be between 100 milliseconds and 30 seconds");
    }
    Ok(())
}

fn validate_endpoint(endpoint: &str, signal: &str) -> Result<()> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        bail!("OTLP {signal} endpoint must not be empty");
    }
    if endpoint.len() > 2_048 {
        bail!("OTLP {signal} endpoint must not exceed 2048 bytes");
    }
    let parsed = Url::parse(endpoint)
        .with_context(|| format!("OTLP {signal} endpoint must be a valid URL"))?;
    let host = parsed
        .host()
        .with_context(|| format!("OTLP {signal} endpoint must include a host"))?;
    let secure = parsed.scheme() == "https";
    let loopback_host = match host {
        Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        Host::Ipv4(address) => address.is_loopback(),
        Host::Ipv6(address) => address.is_loopback(),
    };
    let loopback = parsed.scheme() == "http" && loopback_host;
    if !secure && !loopback {
        bail!("OTLP {signal} endpoint must use HTTPS (HTTP is allowed only for loopback)");
    }
    Ok(())
}

struct ConsentGatedMetricExporter<E> {
    inner: E,
}

impl<E> PushMetricExporter for ConsentGatedMetricExporter<E>
where
    E: PushMetricExporter,
{
    fn export(&self, metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
        async move {
            if !super::telemetry_enabled() {
                return Ok(());
            }
            self.inner.export(metrics).await
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        if super::telemetry_enabled() {
            self.inner.force_flush()
        } else {
            Ok(())
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        Temporality::Delta
    }
}

#[derive(Debug)]
struct ConsentGatedSpanExporter<E> {
    inner: E,
}

impl<E> SdkSpanExporter for ConsentGatedSpanExporter<E>
where
    E: SdkSpanExporter,
{
    fn export(&self, batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
        async move {
            if !super::telemetry_enabled() {
                return Ok(());
            }
            self.inner.export(batch).await
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        if super::telemetry_enabled() {
            self.inner.force_flush()
        } else {
            Ok(())
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConsentGatedMetricExporter, ConsentGatedSpanExporter, OtlpTelemetryConfig, validate_config,
    };
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::Temporality;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::trace::{SpanData, SpanExporter};
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    static TELEMETRY_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct TelemetryEnabledReset;

    impl Drop for TelemetryEnabledReset {
        fn drop(&mut self) {
            super::super::set_telemetry_enabled(true);
        }
    }

    struct CountingMetricExporter {
        exports: Arc<AtomicUsize>,
    }

    impl PushMetricExporter for CountingMetricExporter {
        fn export(&self, _metrics: &ResourceMetrics) -> impl Future<Output = OTelSdkResult> + Send {
            let exports = self.exports.clone();
            async move {
                exports.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Delta
        }
    }

    #[derive(Debug)]
    struct CountingSpanExporter {
        exports: Arc<AtomicUsize>,
    }

    impl SpanExporter for CountingSpanExporter {
        fn export(&self, _batch: Vec<SpanData>) -> impl Future<Output = OTelSdkResult> + Send {
            let exports = self.exports.clone();
            async move {
                exports.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        fn set_resource(&mut self, _resource: &Resource) {}
    }

    fn await_ready<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test exporter future must complete immediately"),
        }
    }

    fn config(metrics_endpoint: &str, traces_endpoint: &str) -> OtlpTelemetryConfig {
        OtlpTelemetryConfig {
            metrics_endpoint: metrics_endpoint.to_owned(),
            traces_endpoint: traces_endpoint.to_owned(),
            export_interval: Duration::from_secs(30),
            export_timeout: Duration::from_secs(3),
        }
    }

    #[test]
    fn production_endpoints_require_https() {
        assert!(
            validate_config(&config(
                "https://telemetry.example/v1/metrics",
                "https://telemetry.example/v1/traces"
            ))
            .is_ok()
        );
        assert!(
            validate_config(&config(
                "http://telemetry.example/v1/metrics",
                "https://telemetry.example/v1/traces"
            ))
            .is_err()
        );
        assert!(
            validate_config(&config(
                "https://telemetry.example/v1/metrics",
                "http://telemetry.example/v1/traces"
            ))
            .is_err()
        );
    }

    #[test]
    fn loopback_http_endpoints_are_available_for_development() {
        assert!(
            validate_config(&config(
                "http://127.0.0.1:4318/v1/metrics",
                "http://localhost:4318/v1/traces"
            ))
            .is_ok()
        );
        assert!(
            validate_config(&config(
                "http://localhost.example/v1/metrics",
                "http://127.0.0.1:4318/v1/traces"
            ))
            .is_err()
        );
    }

    #[test]
    fn export_timing_is_bounded() {
        let mut candidate = config(
            "https://telemetry.example/v1/metrics",
            "https://telemetry.example/v1/traces",
        );
        candidate.export_interval = Duration::from_secs(1);
        assert!(validate_config(&candidate).is_err());
        candidate.export_interval = Duration::from_secs(30);
        candidate.export_timeout = Duration::from_secs(31);
        assert!(validate_config(&candidate).is_err());
    }

    #[test]
    fn consent_gate_covers_metric_and_trace_exporters() {
        let _guard = TELEMETRY_TEST_LOCK.lock().expect("telemetry test lock");
        let _reset = TelemetryEnabledReset;
        let metric_exports = Arc::new(AtomicUsize::new(0));
        let trace_exports = Arc::new(AtomicUsize::new(0));
        let metric_exporter = ConsentGatedMetricExporter {
            inner: CountingMetricExporter {
                exports: metric_exports.clone(),
            },
        };
        let trace_exporter = ConsentGatedSpanExporter {
            inner: CountingSpanExporter {
                exports: trace_exports.clone(),
            },
        };
        let metrics = ResourceMetrics::default();

        super::super::set_telemetry_enabled(false);
        await_ready(metric_exporter.export(&metrics)).expect("disabled metric export is a no-op");
        await_ready(trace_exporter.export(Vec::new())).expect("disabled trace export is a no-op");
        assert_eq!(metric_exports.load(Ordering::Relaxed), 0);
        assert_eq!(trace_exports.load(Ordering::Relaxed), 0);

        super::super::set_telemetry_enabled(true);
        await_ready(metric_exporter.export(&metrics)).expect("enabled metric export succeeds");
        await_ready(trace_exporter.export(Vec::new())).expect("enabled trace export succeeds");
        assert_eq!(metric_exports.load(Ordering::Relaxed), 1);
        assert_eq!(trace_exports.load(Ordering::Relaxed), 1);
    }
}
