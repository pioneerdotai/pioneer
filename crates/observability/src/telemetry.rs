use crate::metrics::{
    DesktopGatewayLifecycleMetrics, DesktopUpdateMetrics, GatewayMetrics, StartupMetrics,
};
use crate::performance::{DesktopTimelineMetrics, GatewayMarkdownMetrics};
use anyhow::{Context, Result, bail};
use opentelemetry::KeyValue;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{
    MetricExporter, RetryPolicy, SpanExporter, WithExportConfig, WithHttpConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::trace::{
    SdkTracer, SdkTracerProvider, SpanData, SpanExporter as SdkSpanExporter,
};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use url::{Host, Url};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TelemetryTarget {
    Gateway,
    Desktop,
    Mobile,
}

impl TelemetryTarget {
    const fn service_name(self) -> &'static str {
        match self {
            Self::Gateway => "pioneer-gateway",
            Self::Desktop => "pioneer-desktop",
            Self::Mobile => "pioneer-mobile",
        }
    }

    const fn instrumentation_name(self) -> &'static str {
        match self {
            Self::Gateway => "pioneer.gateway",
            Self::Desktop => "pioneer.desktop",
            Self::Mobile => "pioneer.mobile",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Gateway => "Gateway",
            Self::Desktop => "Desktop",
            Self::Mobile => "Mobile",
        }
    }

    const fn startup_duration_name(self) -> &'static str {
        match self {
            Self::Gateway => "pioneer.gateway.startup.duration",
            Self::Desktop => "pioneer.desktop.startup.duration",
            Self::Mobile => "pioneer.mobile.startup.duration",
        }
    }

    const fn startup_stage_duration_name(self) -> &'static str {
        match self {
            Self::Gateway => "pioneer.gateway.startup.stage.duration",
            Self::Desktop => "pioneer.desktop.startup.stage.duration",
            Self::Mobile => "pioneer.mobile.startup.stage.duration",
        }
    }

    const fn startup_failures_name(self) -> &'static str {
        match self {
            Self::Gateway => "pioneer.gateway.startup.failures",
            Self::Desktop => "pioneer.desktop.startup.failures",
            Self::Mobile => "pioneer.mobile.startup.failures",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtlpTelemetryConfig {
    pub metrics_endpoint: String,
    pub traces_endpoint: String,
    pub export_interval: Duration,
    pub export_timeout: Duration,
    pub deployment_environment: Option<String>,
    /// Version of the executable application that owns this telemetry
    /// pipeline. Mobile embeds observability through `pioneer-client-ffi`, so
    /// its application version cannot be inferred from this crate's package
    /// version.
    pub service_version: Option<String>,
}

pub(crate) struct ObservabilityState {
    pub(crate) target: TelemetryTarget,
    meter_provider: SdkMeterProvider,
    tracer_provider: SdkTracerProvider,
    pub(crate) tracer: SdkTracer,
    pub(crate) startup_metrics: StartupMetrics,
    pub(crate) desktop_update_metrics: Option<DesktopUpdateMetrics>,
    pub(crate) desktop_gateway_lifecycle_metrics: Option<DesktopGatewayLifecycleMetrics>,
    pub(crate) gateway_metrics: Option<GatewayMetrics>,
    pub(crate) gateway_markdown_metrics: Option<GatewayMarkdownMetrics>,
    pub(crate) desktop_timeline_metrics: Option<DesktopTimelineMetrics>,
}

static OBSERVABILITY: OnceLock<ObservabilityState> = OnceLock::new();
static OBSERVABILITY_FLUSH_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

fn otlp_retry_policy() -> RetryPolicy {
    RetryPolicy {
        // Four total attempts: the initial request and three retries.
        max_retries: 3,
        initial_delay_ms: 250,
        max_delay_ms: 2_000,
        jitter_ms: 100,
    }
}

pub fn init_otlp_observability(config: OtlpTelemetryConfig) -> Result<()> {
    init_otlp_observability_for(TelemetryTarget::Gateway, config)
}

pub fn init_otlp_observability_for(
    target: TelemetryTarget,
    config: OtlpTelemetryConfig,
) -> Result<()> {
    validate_config(&config)?;
    if let Some(state) = OBSERVABILITY.get() {
        if state.target == target {
            return Ok(());
        }
        bail!(
            "OTLP observability pipeline is already initialized for {}",
            state.target.service_name()
        );
    }

    let availability = Arc::new(ExportAvailability::default());
    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(config.metrics_endpoint.trim())
        .with_timeout(config.export_timeout)
        .with_retry_policy(otlp_retry_policy())
        .with_temporality(Temporality::Delta)
        .build()
        .context("failed to build OTLP/HTTP metrics exporter")?;
    let metric_reader = PeriodicReader::builder(ConsentGatedMetricExporter {
        inner: metric_exporter,
        availability: availability.clone(),
    })
    .with_interval(config.export_interval)
    .build();

    let trace_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(config.traces_endpoint.trim())
        .with_timeout(config.export_timeout)
        .with_retry_policy(otlp_retry_policy())
        .build()
        .context("failed to build OTLP/HTTP traces exporter")?;

    let resource = resource(
        target,
        config.deployment_environment.as_deref(),
        config.service_version.as_deref(),
    );
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(metric_reader)
        .build();
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(ConsentGatedSpanExporter {
            inner: trace_exporter,
            availability,
        })
        .build();
    let meter = meter_provider.meter(target.instrumentation_name());
    crate::turn_startup::init(&meter);
    let startup_metrics = StartupMetrics::new(
        &meter,
        target.startup_duration_name(),
        target.startup_stage_duration_name(),
        target.startup_failures_name(),
        target.label(),
    );
    let gateway_markdown_metrics = crate::performance::target_supports_gateway_markdown(target)
        .then(|| GatewayMarkdownMetrics::new(&meter));
    let desktop_timeline_metrics = crate::performance::target_supports_desktop_timeline(target)
        .then(|| DesktopTimelineMetrics::new(&meter));
    let desktop_update_metrics =
        (target == TelemetryTarget::Desktop).then(|| DesktopUpdateMetrics::new(&meter));
    let desktop_gateway_lifecycle_metrics =
        (target == TelemetryTarget::Desktop).then(|| DesktopGatewayLifecycleMetrics::new(&meter));
    let gateway_metrics = (target == TelemetryTarget::Gateway).then(|| GatewayMetrics::new(meter));
    let tracer = tracer_provider.tracer(target.instrumentation_name());

    OBSERVABILITY
        .set(ObservabilityState {
            target,
            meter_provider,
            tracer_provider,
            tracer,
            startup_metrics,
            desktop_update_metrics,
            desktop_gateway_lifecycle_metrics,
            gateway_metrics,
            gateway_markdown_metrics,
            desktop_timeline_metrics,
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

/// Flushes all currently recorded signals without shutting the pipeline down.
///
/// This is primarily used by short-lived/mobile lifecycle boundaries where
/// waiting for the periodic metrics reader would risk losing the only startup
/// sample. Callers that run on a UI thread must execute it in the background.
pub fn force_flush_observability() -> Result<()> {
    let Some(state) = OBSERVABILITY.get() else {
        return Ok(());
    };

    let trace_result = state
        .tracer_provider
        .force_flush()
        .context("failed to flush OTLP traces pipeline");
    let metrics_result = state
        .meter_provider
        .force_flush()
        .context("failed to flush OTLP metrics pipeline");

    match (trace_result, metrics_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(trace_error), Ok(())) => Err(trace_error),
        (Ok(()), Err(metrics_error)) => Err(metrics_error),
        (Err(trace_error), Err(metrics_error)) => Err(anyhow::anyhow!(
            "{trace_error:#}; additionally, {metrics_error:#}"
        )),
    }
}

/// Schedules a best-effort flush without blocking an application/UI thread.
///
/// Startup is recorded only once per process and can otherwise remain in the
/// periodic metrics buffer for tens of seconds. A shared singleflight guard
/// keeps Desktop and Mobile lifecycle boundaries from creating redundant
/// exporter threads.
pub fn schedule_observability_flush() {
    if OBSERVABILITY.get().is_none()
        || !super::telemetry_enabled()
        || OBSERVABILITY_FLUSH_IN_FLIGHT
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
    {
        return;
    }

    if std::thread::Builder::new()
        .name("pioneer-telemetry-flush".to_owned())
        .spawn(|| {
            if let Err(error) = force_flush_observability() {
                tracing::error!(
                    error = %format!("{error:#}"),
                    "failed to flush observability pipeline"
                );
            }
            OBSERVABILITY_FLUSH_IN_FLIGHT.store(false, Ordering::Release);
        })
        .is_err()
    {
        OBSERVABILITY_FLUSH_IN_FLIGHT.store(false, Ordering::Release);
    }
}

pub(crate) fn state() -> Option<&'static ObservabilityState> {
    OBSERVABILITY.get()
}

fn resource(
    target: TelemetryTarget,
    deployment_environment: Option<&str>,
    service_version: Option<&str>,
) -> Resource {
    let deployment_environment = deployment_environment.unwrap_or(if cfg!(debug_assertions) {
        "development"
    } else {
        "production"
    });
    let service_version = service_version
        .map(str::trim)
        .unwrap_or(env!("CARGO_PKG_VERSION"));
    Resource::builder_empty()
        .with_service_name(target.service_name())
        .with_attributes([
            KeyValue::new("service.version", service_version.to_owned()),
            KeyValue::new(
                "deployment.environment.name",
                deployment_environment.to_owned(),
            ),
            KeyValue::new("os.type", std::env::consts::OS),
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
    if let Some(environment) = config.deployment_environment.as_deref()
        && !matches!(environment, "development" | "production")
    {
        bail!("OTLP deployment environment must be development or production");
    }
    if let Some(version) = config.service_version.as_deref()
        && (version.trim().is_empty() || version.len() > 128)
    {
        bail!("OTLP service version must contain between 1 and 128 bytes");
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

const EXPORT_OUTAGE_REPORT_COOLDOWN: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Default)]
struct ExportAvailability {
    state: Mutex<ExportAvailabilityState>,
}

#[derive(Debug, Default)]
struct ExportAvailabilityState {
    unavailable_signals: u8,
    episode_started_at: Option<Instant>,
    last_report_at: Option<Instant>,
    episode_failed_exports: u64,
    // Lifetime totals for this shared availability instance. Recovery and new
    // reports never reset them. One final Err after SDK retries counts once,
    // regardless of the number of HTTP attempts. All counters saturate.
    total_failed_exports: u64,
    total_suppressed_episodes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExportOutageSummary {
    total_suppressed_episodes: u64,
    total_failed_exports: u64,
    episode_failed_exports: u64,
    // Time from the first observed final Err to this observation, not a
    // measurement of server availability. On recovery this is the full episode.
    observed_episode_duration_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExportDiagnostic {
    None,
    Unavailable(ExportOutageSummary),
    Suppressed(ExportOutageSummary),
    Recovered(ExportOutageSummary),
}

impl ExportAvailabilityState {
    fn summary(&self, now: Instant) -> ExportOutageSummary {
        ExportOutageSummary {
            total_suppressed_episodes: self.total_suppressed_episodes,
            total_failed_exports: self.total_failed_exports,
            episode_failed_exports: self.episode_failed_exports,
            observed_episode_duration_ms: self
                .episode_started_at
                .map(|start| now.saturating_duration_since(start).as_millis())
                .unwrap_or_default()
                .min(u64::MAX as u128) as u64,
        }
    }

    fn observe(&mut self, signal_bit: u8, failed: bool, now: Instant) -> ExportDiagnostic {
        let previous = self.unavailable_signals;
        if failed {
            self.unavailable_signals |= signal_bit;
            self.total_failed_exports = self.total_failed_exports.saturating_add(1);
            if previous != 0 {
                self.episode_failed_exports = self.episode_failed_exports.saturating_add(1);
                return ExportDiagnostic::None;
            }
            self.episode_started_at = Some(now);
            self.episode_failed_exports = 1;
            if self.last_report_at.is_none_or(|last| {
                now.saturating_duration_since(last) >= EXPORT_OUTAGE_REPORT_COOLDOWN
            }) {
                self.last_report_at = Some(now);
                ExportDiagnostic::Unavailable(self.summary(now))
            } else {
                self.total_suppressed_episodes = self.total_suppressed_episodes.saturating_add(1);
                ExportDiagnostic::Suppressed(self.summary(now))
            }
        } else {
            self.unavailable_signals &= !signal_bit;
            if previous != 0 && self.unavailable_signals == 0 {
                let summary = self.summary(now);
                self.episode_started_at = None;
                // Keep the completed episode count until the next episode;
                // neither recovery nor reporting clears lifetime totals.
                ExportDiagnostic::Recovered(summary)
            } else {
                ExportDiagnostic::None
            }
        }
    }
}

impl ExportAvailability {
    fn observe(&self, signal: &'static str, signal_bit: u8, result: &OTelSdkResult) {
        let diagnostic = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Sample monotonic time under the same lock as the transition, so
            // concurrent exporters cannot apply timestamps in reverse order.
            state.observe(signal_bit, result.is_err(), Instant::now())
        };
        Self::emit(signal, result, diagnostic);
    }

    fn emit(signal: &'static str, result: &OTelSdkResult, diagnostic: ExportDiagnostic) {
        // No tracing/Sentry or other external work runs with the state locked.
        match diagnostic {
            ExportDiagnostic::Unavailable(summary) => {
                if let Err(error) = result {
                    tracing::error!(
                        target: "pioneer_observability::otlp",
                        signal,
                        error = %error,
                        total_suppressed_episodes = summary.total_suppressed_episodes,
                        total_failed_exports = summary.total_failed_exports,
                        episode_failed_exports = summary.episode_failed_exports,
                        "OTLP exporter became unavailable after retries"
                    );
                }
            }
            ExportDiagnostic::Suppressed(summary) => {
                tracing::info!(
                    target: "pioneer_observability::otlp",
                    signal,
                    total_suppressed_episodes = summary.total_suppressed_episodes,
                    total_failed_exports = summary.total_failed_exports,
                    "OTLP exporter outage report suppressed"
                );
            }
            ExportDiagnostic::Recovered(summary) => {
                tracing::info!(
                    target: "pioneer_observability::otlp",
                    signal,
                    total_suppressed_episodes = summary.total_suppressed_episodes,
                    total_failed_exports = summary.total_failed_exports,
                    episode_failed_exports = summary.episode_failed_exports,
                    observed_episode_duration_ms = summary.observed_episode_duration_ms,
                    "OTLP exporter recovered"
                );
            }
            ExportDiagnostic::None => {}
        }
    }

    #[cfg(test)]
    fn observe_at(
        &self,
        signal: &'static str,
        signal_bit: u8,
        result: &OTelSdkResult,
        now: Instant,
    ) -> ExportDiagnostic {
        let diagnostic = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .observe(signal_bit, result.is_err(), now);
        Self::emit(signal, result, diagnostic);
        diagnostic
    }
}

struct ConsentGatedMetricExporter<E> {
    inner: E,
    availability: Arc<ExportAvailability>,
}

impl<E> PushMetricExporter for ConsentGatedMetricExporter<E>
where
    E: PushMetricExporter,
{
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        if !super::telemetry_enabled() {
            return Ok(());
        }
        let result = self.inner.export(metrics).await;
        self.availability.observe("metrics", 0b01, &result);
        result
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
    availability: Arc<ExportAvailability>,
}

impl<E> SdkSpanExporter for ConsentGatedSpanExporter<E>
where
    E: SdkSpanExporter,
{
    async fn export(&self, mut batch: Vec<SpanData>) -> OTelSdkResult {
        if !super::telemetry_enabled() {
            return Ok(());
        }
        let epoch = super::telemetry_consent_snapshot().1 as i64;
        batch.retain_mut(|span| startup_span_attributes_allowed(&mut span.attributes, epoch));
        let result = self.inner.export(batch).await;
        self.availability.observe("traces", 0b10, &result);
        result
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
pub(crate) mod tests {
    use super::{
        ConsentGatedMetricExporter, ConsentGatedSpanExporter, EXPORT_OUTAGE_REPORT_COOLDOWN,
        ExportAvailability, ExportDiagnostic, ExportOutageSummary, OtlpTelemetryConfig,
        otlp_retry_policy, validate_config,
    };
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
    use opentelemetry_sdk::metrics::Temporality;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::trace::{SpanData, SpanExporter};
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::{Duration, Instant};

    pub(crate) static TELEMETRY_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct TelemetryEnabledReset(bool);

    impl Drop for TelemetryEnabledReset {
        fn drop(&mut self) {
            super::super::set_telemetry_enabled(self.0);
        }
    }

    struct CountingMetricExporter {
        exports: Arc<AtomicUsize>,
        fail: Arc<AtomicBool>,
    }

    impl PushMetricExporter for CountingMetricExporter {
        async fn export(&self, _metrics: &ResourceMetrics) -> OTelSdkResult {
            let exports = self.exports.clone();
            exports.fetch_add(1, Ordering::Relaxed);
            if self.fail.load(Ordering::Relaxed) {
                failure()
            } else {
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
        fail: Arc<AtomicBool>,
    }

    impl SpanExporter for CountingSpanExporter {
        async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
            let exports = self.exports.clone();
            exports.fetch_add(1, Ordering::Relaxed);
            if self.fail.load(Ordering::Relaxed) {
                failure()
            } else {
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
            deployment_environment: None,
            service_version: None,
        }
    }

    #[test]
    fn retry_policy_is_bounded() {
        let policy = otlp_retry_policy();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_delay_ms, 250);
        assert_eq!(policy.max_delay_ms, 2_000);
        assert_eq!(policy.jitter_ms, 100);
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
    fn service_version_is_bounded_and_non_empty_when_overridden() {
        let mut valid = config(
            "https://telemetry.example/v1/metrics",
            "https://telemetry.example/v1/traces",
        );
        valid.service_version = Some("1.2.3+456".to_owned());
        assert!(validate_config(&valid).is_ok());

        valid.service_version = Some("   ".to_owned());
        assert!(validate_config(&valid).is_err());

        valid.service_version = Some("v".repeat(129));
        assert!(validate_config(&valid).is_err());
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
    fn deployment_environment_is_bounded() {
        let mut candidate = config(
            "https://telemetry.example/v1/metrics",
            "https://telemetry.example/v1/traces",
        );
        candidate.deployment_environment = Some("development".to_owned());
        assert!(validate_config(&candidate).is_ok());
        candidate.deployment_environment = Some("customer-provided".to_owned());
        assert!(validate_config(&candidate).is_err());
    }

    #[test]
    fn consent_gate_covers_metric_and_trace_exporters() {
        let _guard = TELEMETRY_TEST_LOCK.lock().expect("telemetry test lock");
        let _reset = TelemetryEnabledReset(super::super::telemetry_enabled());
        let metric_exports = Arc::new(AtomicUsize::new(0));
        let trace_exports = Arc::new(AtomicUsize::new(0));
        let availability = Arc::new(ExportAvailability::default());
        let metric_exporter = ConsentGatedMetricExporter {
            inner: CountingMetricExporter {
                exports: metric_exports.clone(),
                fail: Arc::new(AtomicBool::new(false)),
            },
            availability: availability.clone(),
        };
        let trace_exporter = ConsentGatedSpanExporter {
            inner: CountingSpanExporter {
                exports: trace_exports.clone(),
                fail: Arc::new(AtomicBool::new(false)),
            },
            availability: availability.clone(),
        };
        let metrics = ResourceMetrics::default();
        availability.observe("metrics", 0b01, &failure());
        availability.observe("traces", 0b10, &failure());

        super::super::set_telemetry_enabled(false);
        await_ready(metric_exporter.export(&metrics)).expect("disabled metric export is a no-op");
        await_ready(trace_exporter.export(Vec::new())).expect("disabled trace export is a no-op");
        assert_eq!(metric_exports.load(Ordering::Relaxed), 0);
        assert_eq!(trace_exports.load(Ordering::Relaxed), 0);
        assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0b11);

        super::super::set_telemetry_enabled(true);
        await_ready(metric_exporter.export(&metrics)).expect("enabled metric export succeeds");
        assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0b10);
        await_ready(trace_exporter.export(Vec::new())).expect("enabled trace export succeeds");
        assert_eq!(metric_exports.load(Ordering::Relaxed), 1);
        assert_eq!(trace_exports.load(Ordering::Relaxed), 1);
        assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0);
    }

    fn failure() -> OTelSdkResult {
        Err(OTelSdkError::InternalFailure(
            "fixture final export failure".to_owned(),
        ))
    }

    fn summary(diagnostic: ExportDiagnostic) -> ExportOutageSummary {
        match diagnostic {
            ExportDiagnostic::Unavailable(summary)
            | ExportDiagnostic::Suppressed(summary)
            | ExportDiagnostic::Recovered(summary) => summary,
            ExportDiagnostic::None => panic!("expected a diagnostic"),
        }
    }

    #[test]
    fn exporter_availability_first_failure_and_continuous_outage() {
        let availability = ExportAvailability::default();
        let now = Instant::now();
        let first = availability.observe_at("traces", 0b10, &failure(), now);
        assert!(matches!(first, ExportDiagnostic::Unavailable(_)));
        assert_eq!(summary(first).total_failed_exports, 1);
        assert_eq!(summary(first).total_suppressed_episodes, 0);
        for (signal, bit, elapsed) in [
            ("traces", 0b10, Duration::from_secs(1)),
            ("metrics", 0b01, EXPORT_OUTAGE_REPORT_COOLDOWN),
            ("traces", 0b10, EXPORT_OUTAGE_REPORT_COOLDOWN * 2),
        ] {
            assert_eq!(
                availability.observe_at(signal, bit, &failure(), now + elapsed),
                ExportDiagnostic::None
            );
        }
        let state = availability.state.lock().unwrap();
        assert_eq!(state.unavailable_signals, 0b11);
        assert_eq!(state.total_failed_exports, 4);
        assert_eq!(state.episode_failed_exports, 4);
        assert_eq!(state.total_suppressed_episodes, 0);
        assert_eq!(state.last_report_at, Some(now));
    }

    #[test]
    fn exporter_availability_cooldown_boundary_is_inclusive() {
        for (elapsed, allowed) in [
            (
                EXPORT_OUTAGE_REPORT_COOLDOWN - Duration::from_nanos(1),
                false,
            ),
            (EXPORT_OUTAGE_REPORT_COOLDOWN, true),
            (
                EXPORT_OUTAGE_REPORT_COOLDOWN + Duration::from_nanos(1),
                true,
            ),
        ] {
            let availability = ExportAvailability::default();
            let now = Instant::now();
            availability.observe_at("metrics", 0b01, &failure(), now);
            let recovered =
                availability.observe_at("metrics", 0b01, &Ok(()), now + Duration::from_secs(1));
            assert!(matches!(recovered, ExportDiagnostic::Recovered(_)));
            assert_eq!(summary(recovered).observed_episode_duration_ms, 1_000);
            let next = availability.observe_at("traces", 0b10, &failure(), now + elapsed);
            assert_eq!(matches!(next, ExportDiagnostic::Unavailable(_)), allowed);
            assert_eq!(matches!(next, ExportDiagnostic::Suppressed(_)), !allowed);
            let state = availability.state.lock().unwrap();
            assert_eq!(state.unavailable_signals, 0b10);
            assert_eq!(
                state.last_report_at,
                Some(if allowed { now + elapsed } else { now })
            );
            assert_eq!(state.total_suppressed_episodes, u64::from(!allowed));
        }
    }

    #[test]
    fn exporter_availability_recovery_preserves_cooldown_and_lifetime_totals() {
        let availability = ExportAvailability::default();
        let now = Instant::now();
        availability.observe_at("metrics", 0b01, &failure(), now);
        availability.observe_at("metrics", 0b01, &Ok(()), now + Duration::from_secs(2));
        for seconds in [10, 20] {
            let start = now + Duration::from_secs(seconds);
            let suppressed = availability.observe_at("traces", 0b10, &failure(), start);
            assert!(matches!(suppressed, ExportDiagnostic::Suppressed(_)));
            assert_eq!(
                availability.observe_at("metrics", 0b01, &failure(), start),
                ExportDiagnostic::None
            );
            assert_eq!(
                availability.observe_at("traces", 0b10, &Ok(()), start + Duration::from_secs(1)),
                ExportDiagnostic::None
            );
            let recovered =
                availability.observe_at("metrics", 0b01, &Ok(()), start + Duration::from_secs(3));
            assert!(matches!(recovered, ExportDiagnostic::Recovered(_)));
            let recovered = summary(recovered);
            assert_eq!(recovered.episode_failed_exports, 2);
            assert_eq!(recovered.observed_episode_duration_ms, 3_000);
            assert_eq!(recovered.total_suppressed_episodes, seconds / 10);
            assert_eq!(recovered.total_failed_exports, 1 + seconds / 5);
            assert_eq!(availability.state.lock().unwrap().last_report_at, Some(now));
        }
        let allowed = availability.observe_at(
            "metrics",
            0b01,
            &failure(),
            now + EXPORT_OUTAGE_REPORT_COOLDOWN,
        );
        assert!(matches!(allowed, ExportDiagnostic::Unavailable(_)));
        assert_eq!(summary(allowed).total_suppressed_episodes, 2);
        assert_eq!(summary(allowed).total_failed_exports, 6);
        assert_eq!(summary(allowed).episode_failed_exports, 1);
        availability.observe_at(
            "metrics",
            0b01,
            &Ok(()),
            now + EXPORT_OUTAGE_REPORT_COOLDOWN,
        );
        let suppressed = availability.observe_at(
            "metrics",
            0b01,
            &failure(),
            now + EXPORT_OUTAGE_REPORT_COOLDOWN + Duration::from_secs(1),
        );
        assert!(matches!(suppressed, ExportDiagnostic::Suppressed(_)));
        assert_eq!(summary(suppressed).total_suppressed_episodes, 3);
    }

    #[test]
    fn exporter_availability_both_signal_orders_and_partial_recovery() {
        for failures in [
            [("metrics", 0b01), ("traces", 0b10)],
            [("traces", 0b10), ("metrics", 0b01)],
        ] {
            for recoveries in [failures, [failures[1], failures[0]]] {
                let availability = ExportAvailability::default();
                let now = Instant::now();
                for episode in 0..2 {
                    let start = now + Duration::from_secs(episode * 10);
                    let first =
                        availability.observe_at(failures[0].0, failures[0].1, &failure(), start);
                    assert!(if episode == 0 {
                        matches!(first, ExportDiagnostic::Unavailable(_))
                    } else {
                        matches!(first, ExportDiagnostic::Suppressed(_))
                    });
                    // Success of an unaffected signal cannot recover the episode.
                    assert_eq!(
                        availability.observe_at(failures[1].0, failures[1].1, &Ok(()), start),
                        ExportDiagnostic::None
                    );
                    assert_eq!(
                        availability.observe_at(failures[1].0, failures[1].1, &failure(), start),
                        ExportDiagnostic::None
                    );
                    assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0b11);
                    assert_eq!(
                        availability.observe_at(recoveries[0].0, recoveries[0].1, &Ok(()), start),
                        ExportDiagnostic::None
                    );
                    assert_eq!(
                        availability.state.lock().unwrap().unavailable_signals,
                        recoveries[1].1
                    );
                    assert_eq!(
                        availability.observe_at(recoveries[0].0, recoveries[0].1, &Ok(()), start),
                        ExportDiagnostic::None
                    );
                    assert!(matches!(
                        availability.observe_at(recoveries[1].0, recoveries[1].1, &Ok(()), start),
                        ExportDiagnostic::Recovered(_)
                    ));
                    assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0);
                }
            }
        }
    }

    #[test]
    fn exporter_availability_concurrent_observations_are_coalesced() {
        let availability = Arc::new(ExportAvailability::default());
        let now = Instant::now();
        for (elapsed, allowed) in [
            (Duration::ZERO, true),
            (Duration::from_secs(1), false),
            (EXPORT_OUTAGE_REPORT_COOLDOWN, true),
        ] {
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let results: Vec<_> = std::thread::scope(|scope| {
                let handles: Vec<_> = [("metrics", 0b01), ("traces", 0b10)]
                    .into_iter()
                    .map(|(signal, bit)| {
                        let availability = availability.clone();
                        let barrier = barrier.clone();
                        scope.spawn(move || {
                            barrier.wait();
                            availability.observe_at(signal, bit, &failure(), now + elapsed)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect()
            });
            assert_eq!(
                results
                    .iter()
                    .filter(|result| matches!(result, ExportDiagnostic::Unavailable(_)))
                    .count(),
                usize::from(allowed)
            );
            assert_eq!(
                results
                    .iter()
                    .filter(|result| matches!(result, ExportDiagnostic::Suppressed(_)))
                    .count(),
                usize::from(!allowed)
            );
            assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0b11);
            let recovered: Vec<_> = std::thread::scope(|scope| {
                let handles: Vec<_> = [("metrics", 0b01), ("traces", 0b10)]
                    .into_iter()
                    .map(|(signal, bit)| {
                        let availability = availability.clone();
                        let barrier = barrier.clone();
                        scope.spawn(move || {
                            barrier.wait();
                            availability.observe_at(signal, bit, &Ok(()), now + elapsed)
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|handle| handle.join().unwrap())
                    .collect()
            });
            assert_eq!(
                recovered
                    .iter()
                    .filter(|result| matches!(result, ExportDiagnostic::Recovered(_)))
                    .count(),
                1
            );
            assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0);
        }
        let state = availability.state.lock().unwrap();
        assert_eq!(state.total_failed_exports, 6);
        assert_eq!(state.total_suppressed_episodes, 1);
    }

    #[test]
    fn exporter_availability_concurrent_failure_and_recovery_preserve_mask() {
        let availability = Arc::new(ExportAvailability::default());
        let now = Instant::now();
        availability.observe_at("traces", 0b10, &failure(), now);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let results = std::thread::scope(|scope| {
            let recovering = {
                let availability = availability.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    availability.observe_at("traces", 0b10, &Ok(()), now + Duration::from_secs(1))
                })
            };
            let failing = {
                let availability = availability.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    availability.observe_at(
                        "metrics",
                        0b01,
                        &failure(),
                        now + Duration::from_secs(1),
                    )
                })
            };
            [recovering.join().unwrap(), failing.join().unwrap()]
        });
        // Either one continuous episode, or recovery followed by a suppressed
        // episode, depending on lock order. Neither order may emit another ERROR.
        assert!(
            results
                .iter()
                .all(|result| !matches!(result, ExportDiagnostic::Unavailable(_)))
        );
        let suppressed = u64::from(matches!(results[1], ExportDiagnostic::Suppressed(_)));
        assert_eq!(
            matches!(results[0], ExportDiagnostic::Recovered(_)),
            suppressed == 1
        );
        {
            let state = availability.state.lock().unwrap();
            assert_eq!(state.unavailable_signals, 0b01);
            assert_eq!(state.total_failed_exports, 2);
            assert_eq!(state.total_suppressed_episodes, suppressed);
        }
        assert!(matches!(
            availability.observe_at("metrics", 0b01, &Ok(()), now + Duration::from_secs(2)),
            ExportDiagnostic::Recovered(_)
        ));
        assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0);
    }

    #[test]
    fn exporter_availability_counters_saturate_and_poison_does_not_panic() {
        let availability = ExportAvailability::default();
        let now = Instant::now();
        availability.observe_at("metrics", 0b01, &failure(), now);
        {
            let mut state = availability.state.lock().unwrap();
            state.total_failed_exports = u64::MAX;
            state.total_suppressed_episodes = u64::MAX;
            state.episode_failed_exports = u64::MAX;
        }
        availability.observe_at("traces", 0b10, &failure(), now);
        let recovered = {
            availability.observe_at("metrics", 0b01, &Ok(()), now);
            availability.observe_at("traces", 0b10, &Ok(()), now)
        };
        assert_eq!(summary(recovered).episode_failed_exports, u64::MAX);
        let _ = std::panic::catch_unwind(|| {
            let _guard = availability.state.lock().unwrap();
            panic!("poison fixture");
        });
        let suppressed = availability.observe_at("metrics", 0b01, &failure(), now);
        assert!(matches!(suppressed, ExportDiagnostic::Suppressed(_)));
        assert_eq!(summary(suppressed).total_failed_exports, u64::MAX);
        assert_eq!(summary(suppressed).total_suppressed_episodes, u64::MAX);
        // Also exercise the production clock/lock path with poisoned state.
        availability.observe("metrics", 0b01, &failure());
    }

    #[test]
    fn consent_wrappers_preserve_final_export_results_and_call_counts() {
        let _guard = TELEMETRY_TEST_LOCK.lock().expect("telemetry test lock");
        let _reset = TelemetryEnabledReset(super::super::telemetry_enabled());
        super::super::set_telemetry_enabled(true);
        let availability = Arc::new(ExportAvailability::default());
        let metric_calls = Arc::new(AtomicUsize::new(0));
        let span_calls = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(true));
        let metrics = ConsentGatedMetricExporter {
            inner: CountingMetricExporter {
                exports: metric_calls.clone(),
                fail: fail.clone(),
            },
            availability: availability.clone(),
        };
        let spans = ConsentGatedSpanExporter {
            inner: CountingSpanExporter {
                exports: span_calls.clone(),
                fail: fail.clone(),
            },
            availability: availability.clone(),
        };
        for failed in [true, false, true] {
            fail.store(failed, Ordering::Relaxed);
            let metric_result = await_ready(metrics.export(&ResourceMetrics::default()));
            let span_result = await_ready(spans.export(Vec::new()));
            for result in [metric_result, span_result] {
                if failed {
                    assert!(
                        matches!(result, Err(OTelSdkError::InternalFailure(ref message)) if message == "fixture final export failure")
                    );
                } else {
                    assert!(result.is_ok());
                }
            }
            assert_eq!(
                availability.state.lock().unwrap().unavailable_signals,
                if failed { 0b11 } else { 0 }
            );
        }
        assert_eq!(metric_calls.load(Ordering::Relaxed), 3);
        assert_eq!(span_calls.load(Ordering::Relaxed), 3);
        assert_eq!(availability.state.lock().unwrap().total_failed_exports, 4);
    }

    // Local, synchronous capture only: no default transport, initialization,
    // network, test feature, or global subscriber is needed.
    #[derive(Default)]
    struct TestTransport(Mutex<Vec<sentry::protocol::Event<'static>>>);

    impl sentry::Transport for TestTransport {
        fn send_envelope(&self, envelope: sentry::Envelope) {
            if let Some(event) = envelope.event() {
                self.0.lock().unwrap().push(event.clone());
            }
        }
    }

    #[test]
    fn exporter_outage_cooldown_through_real_sentry_mapper() {
        use tracing_subscriber::prelude::*;
        let _guard = TELEMETRY_TEST_LOCK.lock().expect("telemetry test lock");
        let _reset = TelemetryEnabledReset(super::super::telemetry_enabled());
        super::super::set_telemetry_enabled(true);
        let transport = Arc::new(TestTransport::default());
        let mut options = sentry::ClientOptions::default();
        options.dsn = Some("https://public@example.invalid/1".parse().unwrap());
        options.transport = Some(Arc::new(transport.clone()));
        options.default_integrations = false;
        let client = sentry::Client::from(options);
        let hub = Arc::new(sentry::Hub::new(
            Some(Arc::new(client)),
            Arc::new(Default::default()),
        ));
        let subscriber = tracing_subscriber::registry().with(super::super::sentry_tracing_layer());
        let availability = ExportAvailability::default();
        let now = Instant::now();
        sentry::Hub::run(hub, || {
            tracing::subscriber::with_default(subscriber, || {
                availability.observe_at("traces", 0b10, &failure(), now);
                assert_eq!(transport.0.lock().unwrap().len(), 1);
                availability.observe_at("traces", 0b10, &failure(), now + Duration::from_secs(1));
                assert_eq!(transport.0.lock().unwrap().len(), 1);
                availability.observe_at("traces", 0b10, &Ok(()), now + Duration::from_secs(2));
                availability.observe_at("traces", 0b10, &failure(), now + Duration::from_secs(3));
                availability.observe_at("metrics", 0b01, &failure(), now + Duration::from_secs(4));
                availability.observe_at("traces", 0b10, &Ok(()), now + Duration::from_secs(5));
                assert_eq!(transport.0.lock().unwrap().len(), 1);
                availability.observe_at("metrics", 0b01, &Ok(()), now + Duration::from_secs(6));
                assert_eq!(transport.0.lock().unwrap().len(), 1);
                tracing::error!(target: "pioneer::unrelated", "unrelated error fixture");
                assert_eq!(transport.0.lock().unwrap().len(), 2);
                availability.observe_at(
                    "metrics",
                    0b01,
                    &failure(),
                    now + EXPORT_OUTAGE_REPORT_COOLDOWN,
                );
                assert_eq!(transport.0.lock().unwrap().len(), 3);
                // Expiration during a continuous outage does not report again.
                availability.observe_at(
                    "traces",
                    0b10,
                    &failure(),
                    now + EXPORT_OUTAGE_REPORT_COOLDOWN * 2,
                );
            })
        });
        let events = transport.0.lock().unwrap();
        assert_eq!(events.len(), 3);
        for index in [0, 2] {
            assert_eq!(events[index].level, sentry::Level::Error);
            assert_eq!(
                events[index].message.as_deref(),
                Some("OTLP exporter became unavailable after retries")
            );
        }
        assert_eq!(
            events[1].message.as_deref(),
            Some("unrelated error fixture")
        );
        let recovery = events[1]
            .breadcrumbs
            .iter()
            .rev()
            .find(|breadcrumb| breadcrumb.message.as_deref() == Some("OTLP exporter recovered"))
            .unwrap();
        assert_eq!(recovery.level, sentry::Level::Info);
        assert_eq!(
            recovery.data["total_suppressed_episodes"],
            serde_json::json!(1)
        );
        assert_eq!(recovery.data["total_failed_exports"], serde_json::json!(4));
        assert_eq!(
            recovery.data["episode_failed_exports"],
            serde_json::json!(2)
        );
        assert_eq!(
            recovery.data["observed_episode_duration_ms"],
            serde_json::json!(3_000)
        );
    }

    #[test]
    fn exporter_availability_coalesces_signals_until_every_failed_signal_recovers() {
        let availability = ExportAvailability::default();
        let failure = Err(OTelSdkError::InternalFailure("network error".to_owned()));

        availability.observe("traces", 0b10, &failure);
        availability.observe("metrics", 0b01, &failure);
        assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0b11);

        availability.observe("traces", 0b10, &Ok(()));
        assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0b01);
        availability.observe("metrics", 0b01, &Ok(()));
        assert_eq!(availability.state.lock().unwrap().unavailable_signals, 0);
    }
}

// The marker is process-local bookkeeping, never an exported dimension.
fn startup_span_attributes_allowed(
    attributes: &mut Vec<opentelemetry::KeyValue>,
    epoch: i64,
) -> bool {
    let allowed = attributes
        .iter()
        .find(|a| a.key.as_str() == crate::turn_startup::CONSENT_EPOCH_ATTRIBUTE)
        .is_none_or(|a| a.value == opentelemetry::Value::I64(epoch));
    attributes.retain(|a| a.key.as_str() != crate::turn_startup::CONSENT_EPOCH_ATTRIBUTE);
    allowed
}
#[cfg(test)]
mod startup_consent_tests {
    #[test]
    fn late_startup_spans_cannot_cross_opt_out_and_opt_in() {
        use opentelemetry::KeyValue;
        let marker = crate::turn_startup::CONSENT_EPOCH_ATTRIBUTE;
        let mut old = vec![
            KeyValue::new(marker, 1_i64),
            KeyValue::new("stage", "cli.initialize"),
        ];
        assert!(!super::startup_span_attributes_allowed(&mut old, 3));
        let mut current = vec![
            KeyValue::new(marker, 3_i64),
            KeyValue::new("stage", "cli.initialize"),
        ];
        assert!(super::startup_span_attributes_allowed(&mut current, 3));
        assert_eq!(current, vec![KeyValue::new("stage", "cli.initialize")]);
        let mut unrelated = vec![KeyValue::new("stage", "unrelated")];
        assert!(super::startup_span_attributes_allowed(&mut unrelated, 3));
    }
}
