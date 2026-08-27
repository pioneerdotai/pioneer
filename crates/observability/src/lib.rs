//! Shared Sentry, tracing, and consent-gated OpenTelemetry setup for Pioneer runtime binaries.
//!
//! `PIONEER_SENTRY_DSN` is used by non-desktop binaries. `PIONEER_DESKTOP_SENTRY_DSN`
//! is used by the desktop app. A local `.env` file is loaded automatically before
//! reading these values. Runtime environment variables take precedence over `.env`
//! and build-time values with the same names.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};

use sentry::integrations::tracing::{
    EventFilter, EventMapping, breadcrumb_from_event, event_from_event,
};
use sentry::{ClientInitGuard, ClientOptions};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer as _;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::Context as TracingContext;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

mod metrics;
mod operations;
mod patch_telemetry;
mod performance;
mod startup;
mod telemetry;

pub use metrics::{
    DatabaseOperation, DatabasePoolSnapshot, DatabaseRole, PatchOperationMetric,
    record_database_operation, record_patch_mutation_fallback, record_patch_operation,
    register_database_pool_observer,
};
pub use operations::{
    GatewayCliRuntimeKind, GatewayOperation, GatewayOperationStage, GatewayOperationStageGuard,
    GatewayOperationTrace, GatewayProviderReadinessState, GatewayProviderType,
    GatewayProviderWarmupScope, GatewayProviderWarmupStage, GatewayProviderWarmupStageGuard,
    GatewayProviderWarmupTrace,
};
pub use patch_telemetry::{PatchTelemetrySnapshot, register_patch_telemetry_snapshot_provider};
pub use performance::{
    DesktopCodeHighlightCacheStatus, DesktopCodeHighlightFallbackReason,
    DesktopCodeHighlightMetric, DesktopCodeHighlightOutcome, DesktopCodeHighlightTheme,
    DesktopTimelineCacheStatus, DesktopTimelineContentKind, DesktopTimelineOutcome,
    DesktopTimelineStage, DesktopTimelineStageMetric, GatewayMarkdownMessageMetric,
    GatewayMarkdownMessageOutcome, GatewayMarkdownOutcome, GatewayMarkdownStage,
    GatewayMarkdownStageMetric, GatewayMarkdownStreamKind, record_desktop_code_highlight,
    record_desktop_timeline_stage, record_gateway_markdown_message, record_gateway_markdown_stage,
};
pub use startup::{
    DesktopStartupOutcome, DesktopStartupStage, DesktopStartupStageGuard, DesktopStartupTrace,
    GatewayStartupStage, GatewayStartupStageGuard, GatewayStartupTrace, MobileStartupOutcome,
    MobileStartupReport, MobileStartupStage, MobileStartupStageTiming, record_mobile_startup,
};
pub use telemetry::{
    OtlpTelemetryConfig, TelemetryTarget, force_flush_observability, init_otlp_observability,
    init_otlp_observability_for, schedule_observability_flush, shutdown_observability,
};

/// DSN for non-desktop runtime binaries.
pub const SENTRY_DSN_ENV: &str = "PIONEER_SENTRY_DSN";
/// DSN for the desktop app.
pub const DESKTOP_SENTRY_DSN_ENV: &str = "PIONEER_DESKTOP_SENTRY_DSN";
/// Optional Sentry environment value shared by all targets.
pub const SENTRY_ENVIRONMENT_ENV: &str = "PIONEER_SENTRY_ENVIRONMENT";

const BUILD_SENTRY_DSN: Option<&str> = option_env!("PIONEER_SENTRY_DSN");
const BUILD_DESKTOP_SENTRY_DSN: Option<&str> = option_env!("PIONEER_DESKTOP_SENTRY_DSN");
const BUILD_SENTRY_ENVIRONMENT: Option<&str> = option_env!("PIONEER_SENTRY_ENVIRONMENT");

static LOAD_DOTENV: Once = Once::new();
// Runtime binaries participate by default. The gateway explicitly closes this
// gate before Sentry initialization and reopens it only after loading consent.
// One atomic value keeps consent and its generation consistent. Bit zero is
// the current gate; upper bits change on every actual opt-in/opt-out
// transition so a long-running operation cannot be exported after consent was
// revoked and later re-enabled.
static TELEMETRY_CONSENT: AtomicU64 = AtomicU64::new(1);
static TELEMETRY_CONSENT_TRANSITION: Mutex<TelemetryConsentTransition> =
    Mutex::new(TelemetryConsentTransition {
        desired_enabled: true,
        revision: 0,
        enable_worker_running: false,
    });

#[derive(Debug)]
struct TelemetryConsentTransition {
    desired_enabled: bool,
    revision: u64,
    enable_worker_running: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum SentryTarget {
    Shared,
    Desktop,
}

impl SentryTarget {
    fn dsn_env(self) -> &'static str {
        match self {
            Self::Shared => SENTRY_DSN_ENV,
            Self::Desktop => DESKTOP_SENTRY_DSN_ENV,
        }
    }

    fn build_dsn(self) -> Option<&'static str> {
        match self {
            Self::Shared => BUILD_SENTRY_DSN,
            Self::Desktop => BUILD_DESKTOP_SENTRY_DSN,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Desktop => "desktop",
        }
    }
}

#[must_use]
pub fn init_sentry(target: SentryTarget) -> Option<ClientInitGuard> {
    load_local_dotenv();

    let dsn = configured_value(target.dsn_env(), target.build_dsn())?;
    let dsn = dsn.parse().ok()?;
    let environment =
        configured_value(SENTRY_ENVIRONMENT_ENV, BUILD_SENTRY_ENVIRONMENT).map(Cow::Owned);

    let guard = sentry::init(ClientOptions {
        dsn: Some(dsn),
        release: Some(Cow::Owned(format!("pioneer@{}", env!("CARGO_PKG_VERSION")))),
        environment,
        before_send: Some(Arc::new(|event| telemetry_enabled().then_some(event))),
        ..Default::default()
    });

    sentry::configure_scope(|scope| {
        scope.set_tag("pioneer.target", target.tag());
    });

    Some(guard)
}

pub fn init_tracing(sentry_enabled: bool) {
    let filter = Targets::new()
        .with_default(LevelFilter::INFO)
        .with_target("rmcp::service", LevelFilter::WARN);

    let format_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .without_time()
        .with_filter(filter.clone());
    let database_metrics_filter = Targets::new()
        .with_default(LevelFilter::OFF)
        .with_target("sqlx::pool::acquire", LevelFilter::TRACE);
    let subscriber = tracing_subscriber::registry()
        .with(metrics::DatabasePoolAcquireMetricsLayer.with_filter(database_metrics_filter))
        .with(format_layer);

    if sentry_enabled {
        let _ = subscriber
            .with(sentry_tracing_layer().with_filter(filter))
            .try_init();
    } else {
        let _ = subscriber.try_init();
    }
}

pub fn capture_anyhow(error: &anyhow::Error) {
    if telemetry_enabled() {
        sentry::integrations::anyhow::capture_anyhow(error);
    }
}

pub fn set_telemetry_enabled(enabled: bool) {
    let mut transition = TELEMETRY_CONSENT_TRANSITION
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    if transition.desired_enabled != enabled {
        transition.desired_enabled = enabled;
        transition.revision = transition.revision.wrapping_add(1);
    }

    if !enabled {
        set_telemetry_gate(false);
        return;
    }

    if telemetry_enabled() {
        return;
    }

    // During process startup no SDK buffers exist yet, so consent can be
    // applied synchronously without delaying initialization. At runtime the
    // old buffers must be drained while the exporter gate is still closed;
    // do that in one coalesced worker so a settings UI or Gateway request is
    // never blocked by an exporter timeout.
    if telemetry::state().is_none() {
        set_telemetry_gate(true);
        return;
    }

    if transition.enable_worker_running {
        return;
    }
    transition.enable_worker_running = true;
    drop(transition);

    if let Err(error) = std::thread::Builder::new()
        .name("pioneer-telemetry-consent".to_owned())
        .spawn(enable_telemetry_after_buffer_discard)
    {
        let mut transition = TELEMETRY_CONSENT_TRANSITION
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        transition.enable_worker_running = false;
        tracing::error!(
            error = %error,
            "telemetry remains disabled because the consent transition worker could not start"
        );
    }
}

fn enable_telemetry_after_buffer_discard() {
    loop {
        let revision = {
            let mut transition = TELEMETRY_CONSENT_TRANSITION
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !transition.desired_enabled {
                transition.enable_worker_running = false;
                return;
            }
            transition.revision
        };

        // Metrics and spans recorded before an opt-out can still be buffered
        // by the SDK. Drain them through the closed exporter gate before
        // reopening consent; otherwise a quick off -> on transition could
        // export data that predates the new opt-in.
        let flush_result = telemetry::force_flush_observability();

        let mut transition = TELEMETRY_CONSENT_TRANSITION
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !transition.desired_enabled {
            transition.enable_worker_running = false;
            return;
        }
        if transition.revision != revision {
            // Consent changed while buffers were being discarded. Keep the
            // gate closed and flush once more for the latest opt-in request.
            continue;
        }
        if let Err(error) = flush_result {
            transition.enable_worker_running = false;
            drop(transition);
            tracing::error!(
                error = %format!("{error:#}"),
                "telemetry remains disabled because buffered signals could not be discarded"
            );
            return;
        }

        set_telemetry_gate(true);
        transition.enable_worker_running = false;
        return;
    }
}

fn set_telemetry_gate(enabled: bool) {
    let current = TELEMETRY_CONSENT.load(Ordering::Acquire);
    if current & 1 == u64::from(enabled) {
        return;
    }
    let generation = (current >> 1).wrapping_add(1);
    let next = (generation << 1) | u64::from(enabled);
    TELEMETRY_CONSENT.store(next, Ordering::Release);
}

pub fn telemetry_enabled() -> bool {
    TELEMETRY_CONSENT.load(Ordering::Acquire) & 1 == 1
}

pub(crate) fn telemetry_consent_snapshot() -> (bool, u64) {
    let state = TELEMETRY_CONSENT.load(Ordering::Acquire);
    (state & 1 == 1, state >> 1)
}

pub(crate) fn telemetry_sample_allowed(started_generation: Option<u64>) -> bool {
    let (enabled, current_generation) = telemetry_consent_snapshot();
    telemetry_sample_allowed_for_state(started_generation, enabled, current_generation)
}

fn telemetry_sample_allowed_for_state(
    started_generation: Option<u64>,
    enabled: bool,
    current_generation: u64,
) -> bool {
    enabled && started_generation == Some(current_generation)
}

fn load_local_dotenv() {
    LOAD_DOTENV.call_once(|| match dotenvy::dotenv() {
        Ok(_) => {}
        Err(error) if error.not_found() => {}
        Err(error) => eprintln!("failed to load .env: {error}"),
    });
}

fn sentry_tracing_layer<S>() -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    sentry::integrations::tracing::layer().event_mapper(sentry_event_mapper)
}

fn sentry_event_mapper<S>(event: &tracing::Event<'_>, _ctx: TracingContext<'_, S>) -> EventMapping
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
{
    if !telemetry_enabled() {
        return EventMapping::Ignore;
    }
    let fields = tracing_event_fields(event);
    if should_demote_rmcp_transport_worker_failure(
        event.metadata().level(),
        event.metadata().target(),
        fields.message.as_deref(),
    ) || should_demote_tantivy_reader_commit_reload_not_found(
        event.metadata().level(),
        event.metadata().target(),
        fields.log_target.as_deref(),
        fields.message.as_deref(),
    ) || should_demote_gpui_asset_cache_http_not_found(
        event.metadata().level(),
        event.metadata().target(),
        fields.log_target.as_deref(),
        fields.message.as_deref(),
    ) || should_demote_rathole_client_control_channel_retry(
        event.metadata().level(),
        event.metadata().target(),
        fields.log_target.as_deref(),
        fields.message.as_deref(),
    ) {
        return EventMapping::Breadcrumb(breadcrumb_from_event(
            event,
            None::<&TracingContext<'_, S>>,
        ));
    }

    let filter = sentry_event_filter(event.metadata().level());
    let mut items = Vec::new();
    if filter.contains(EventFilter::Breadcrumb) {
        items.push(EventMapping::Breadcrumb(breadcrumb_from_event(
            event,
            None::<&TracingContext<'_, S>>,
        )));
    }
    if filter.contains(EventFilter::Event) {
        items.push(EventMapping::Event(event_from_event(
            event,
            None::<&TracingContext<'_, S>>,
        )));
    }
    EventMapping::Combined(items.into())
}

fn sentry_event_filter(level: &tracing::Level) -> EventFilter {
    match *level {
        tracing::Level::ERROR => EventFilter::Event,
        tracing::Level::TRACE => EventFilter::Ignore,
        _ => EventFilter::Breadcrumb,
    }
}

fn effective_event_target<'a>(target: &'a str, log_target: Option<&'a str>) -> &'a str {
    log_target.unwrap_or(target)
}

fn should_demote_rmcp_transport_worker_failure(
    level: &tracing::Level,
    target: &str,
    message: Option<&str>,
) -> bool {
    *level == tracing::Level::ERROR
        && target == "rmcp::transport::worker"
        && message.is_some_and(|message| {
            is_rmcp_streamable_http_initialize_response_failure(message)
                || is_rmcp_streamable_http_auth_rejection(message)
                || is_rmcp_streamable_http_transport_connect_failure(message)
                || is_rmcp_streamable_http_initialized_notification_send_failure(message)
        })
}

fn should_demote_tantivy_reader_commit_reload_not_found(
    level: &tracing::Level,
    target: &str,
    log_target: Option<&str>,
    message: Option<&str>,
) -> bool {
    *level == tracing::Level::ERROR
        && effective_event_target(target, log_target) == "tantivy::reader"
        && message.is_some_and(is_tantivy_reader_commit_reload_not_found)
}

fn should_demote_gpui_asset_cache_http_not_found(
    level: &tracing::Level,
    target: &str,
    log_target: Option<&str>,
    message: Option<&str>,
) -> bool {
    *level == tracing::Level::ERROR
        && effective_event_target(target, log_target) == "gpui::asset_cache"
        && message.is_some_and(is_gpui_asset_cache_http_not_found)
}

fn should_demote_rathole_client_control_channel_retry(
    level: &tracing::Level,
    target: &str,
    log_target: Option<&str>,
    message: Option<&str>,
) -> bool {
    *level == tracing::Level::ERROR
        && effective_event_target(target, log_target) == "rathole::client"
        && message.is_some_and(is_rathole_client_control_channel_retry)
}

fn is_rathole_client_control_channel_retry(message: &str) -> bool {
    message.starts_with("Failed to run the control channel:")
        && message.contains(". Retry in ")
        && !message.contains("Incorrect token")
        && !message.contains("Authentication failed")
        && !message.contains("Service not exist")
        && !message.contains("Service does not exist")
}

fn is_gpui_asset_cache_http_not_found(message: &str) -> bool {
    const PREFIX: &str = "Failed to load asset: unexpected http status for ";
    let Some(rest) = message.strip_prefix(PREFIX) else {
        return false;
    };
    let Some((uri, _)) = rest.split_once(": 404 Not Found") else {
        return false;
    };

    uri.starts_with("http://") || uri.starts_with("https://")
}

fn is_tantivy_reader_commit_reload_not_found(message: &str) -> bool {
    is_tantivy_reader_commit_reload_error(message)
        && (is_tantivy_reader_commit_reload_lock_not_found(message)
            || is_tantivy_reader_commit_reload_meta_json_missing(message))
}

fn is_tantivy_reader_commit_reload_error(message: &str) -> bool {
    message.contains("Error while loading searcher after commit was detected.")
}

fn is_tantivy_reader_commit_reload_lock_not_found(message: &str) -> bool {
    message.contains("LockFailure")
        && message.contains("kind: NotFound")
        && message.contains("No such file or directory")
}

fn is_tantivy_reader_commit_reload_meta_json_missing(message: &str) -> bool {
    message.contains("OpenReadError")
        && message.contains("FileDoesNotExist")
        && message.contains("meta.json")
}

fn is_rmcp_streamable_http_initialize_response_failure(message: &str) -> bool {
    message.contains("worker quit with fatal: unexpected server response")
        && message.contains("expect initialized, accepted")
        && message.contains("process initialize response")
}

fn is_rmcp_streamable_http_auth_rejection(message: &str) -> bool {
    message.contains("worker quit with fatal: Transport channel closed")
        && (message.contains("UnexpectedServerResponse(\"HTTP 401")
            || message.contains("UnexpectedServerResponse(\"HTTP 403")
            || message.contains("AuthRequired")
            || message.contains("InsufficientScope"))
}

fn is_rmcp_streamable_http_transport_connect_failure(message: &str) -> bool {
    message.contains("worker quit with fatal: Transport channel closed")
        && message.contains("reqwest::Error")
        && message.contains("kind: Request")
        && message.contains("hyper_util::client::legacy::Error(Connect")
}

fn is_rmcp_streamable_http_initialized_notification_send_failure(message: &str) -> bool {
    message.contains("worker quit with fatal: Client error:")
        && message.contains("error sending request for url (")
        && message.contains("), when send initialized notification")
}

fn tracing_event_fields(event: &tracing::Event<'_>) -> EventFieldVisitor {
    let mut visitor = EventFieldVisitor::default();
    event.record(&mut visitor);
    visitor
}

#[derive(Default)]
struct EventFieldVisitor {
    message: Option<String>,
    log_target: Option<String>,
}

impl Visit for EventFieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = Some(value.to_owned()),
            "log.target" => self.log_target = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = Some(format!("{value:?}")),
            "log.target" => {
                self.log_target = Some(format!("{value:?}").trim_matches('"').to_owned())
            }
            _ => {}
        }
    }
}

fn configured_value(env_name: &str, build_value: Option<&str>) -> Option<String> {
    std::env::var(env_name)
        .ok()
        .as_deref()
        .and_then(non_empty)
        .map(str::to_owned)
        .or_else(|| build_value.and_then(non_empty).map(str::to_owned))
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::{
        sentry_event_filter, should_demote_gpui_asset_cache_http_not_found,
        should_demote_rathole_client_control_channel_retry,
        should_demote_rmcp_transport_worker_failure,
        should_demote_tantivy_reader_commit_reload_not_found,
    };
    use sentry::integrations::tracing::EventFilter;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::filter::{LevelFilter, Targets};
    use tracing_subscriber::layer::{Context as TracingContext, Layer, SubscriberExt};

    struct CountingLayer(Arc<AtomicUsize>);

    impl<S> Layer<S> for CountingLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, _event: &Event<'_>, _ctx: TracingContext<'_, S>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn database_metrics_filter_does_not_suppress_normal_logs() {
        let events = Arc::new(AtomicUsize::new(0));
        let database_metrics_filter = Targets::new()
            .with_default(LevelFilter::OFF)
            .with_target("sqlx::pool::acquire", LevelFilter::TRACE);
        let log_filter = Targets::new().with_default(LevelFilter::INFO);
        let subscriber = tracing_subscriber::registry()
            .with(
                super::metrics::DatabasePoolAcquireMetricsLayer
                    .with_filter(database_metrics_filter),
            )
            .with(CountingLayer(events.clone()).with_filter(log_filter));

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "pioneer::test", "normal log event");
        });

        assert_eq!(events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn demotes_expected_rmcp_streamable_http_initialize_response_failure() {
        assert!(should_demote_rmcp_transport_worker_failure(
            &tracing::Level::ERROR,
            "rmcp::transport::worker",
            Some(
                "worker quit with fatal: unexpected server response: expect initialized, accepted, when process initialize response",
            ),
        ));
    }

    #[test]
    fn demotes_expected_rmcp_streamable_http_auth_rejection() {
        assert!(should_demote_rmcp_transport_worker_failure(
            &tracing::Level::ERROR,
            "rmcp::transport::worker",
            Some(
                "worker quit with fatal: Transport channel closed, when UnexpectedServerResponse(\"HTTP 403 Forbidden: forbidden: access denied\\n\")",
            ),
        ));
    }

    #[test]
    fn demotes_expected_rmcp_streamable_http_transport_connect_failure() {
        assert!(should_demote_rmcp_transport_worker_failure(
            &tracing::Level::ERROR,
            "rmcp::transport::worker",
            Some(
                "worker quit with fatal: Transport channel closed, when Client(reqwest::Error { kind: Request, url: \"https://mcp.posthog.com/mcp\", source: hyper_util::client::legacy::Error(Connect, Error { code: -9806, message: \"connection closed via error\" }) })",
            ),
        ));
    }

    #[test]
    fn demotes_expected_rmcp_streamable_http_initialized_notification_send_failure() {
        assert!(should_demote_rmcp_transport_worker_failure(
            &tracing::Level::ERROR,
            "rmcp::transport::worker",
            Some(
                "worker quit with fatal: Client error: error sending request for url (https://mcp.posthog.com/mcp), when send initialized notification",
            ),
        ));
    }

    #[test]
    fn keeps_rmcp_client_errors_from_other_stages_as_events() {
        assert!(!should_demote_rmcp_transport_worker_failure(
            &tracing::Level::ERROR,
            "rmcp::transport::worker",
            Some(
                "worker quit with fatal: Client error: error sending request for url (https://mcp.posthog.com/mcp), when call tools/list",
            ),
        ));
    }

    #[test]
    fn keeps_other_rmcp_worker_errors_as_events() {
        assert!(!should_demote_rmcp_transport_worker_failure(
            &tracing::Level::ERROR,
            "rmcp::transport::worker",
            Some("worker quit with fatal: transport channel closed"),
        ));
    }

    #[test]
    fn keeps_same_message_from_other_targets_as_events() {
        assert!(!should_demote_rmcp_transport_worker_failure(
            &tracing::Level::ERROR,
            "pioneer_gateway",
            Some(
                "worker quit with fatal: unexpected server response: expect initialized, accepted, when process initialize response",
            ),
        ));
    }

    #[test]
    fn demotes_tantivy_reader_commit_reload_lock_not_found_from_log_target() {
        assert!(should_demote_tantivy_reader_commit_reload_not_found(
            &tracing::Level::ERROR,
            "log",
            Some("tantivy::reader"),
            Some(
                "Error while loading searcher after commit was detected. LockFailure(IoError(Os { code: 2, kind: NotFound, message: \"No such file or directory\" }), None)",
            ),
        ));
    }

    #[test]
    fn demotes_tantivy_reader_commit_reload_meta_json_missing_from_log_target() {
        assert!(should_demote_tantivy_reader_commit_reload_not_found(
            &tracing::Level::ERROR,
            "log",
            Some("tantivy::reader"),
            Some(
                "Error while loading searcher after commit was detected. OpenReadError(FileDoesNotExist(\"meta.json\"))",
            ),
        ));
    }

    #[test]
    fn keeps_other_tantivy_reader_commit_reload_errors_as_events() {
        assert!(!should_demote_tantivy_reader_commit_reload_not_found(
            &tracing::Level::ERROR,
            "log",
            Some("tantivy::reader"),
            Some(
                "Error while loading searcher after commit was detected. LockFailure(IoError(Os { code: 13, kind: PermissionDenied, message: \"Permission denied\" }), None)",
            ),
        ));
    }

    #[test]
    fn keeps_other_tantivy_reader_missing_files_as_events() {
        assert!(!should_demote_tantivy_reader_commit_reload_not_found(
            &tracing::Level::ERROR,
            "log",
            Some("tantivy::reader"),
            Some(
                "Error while loading searcher after commit was detected. OpenReadError(FileDoesNotExist(\"segment_1.store\"))",
            ),
        ));
    }

    #[test]
    fn keeps_tantivy_lock_not_found_message_from_other_targets_as_events() {
        assert!(!should_demote_tantivy_reader_commit_reload_not_found(
            &tracing::Level::ERROR,
            "pioneer_memory",
            None,
            Some(
                "Error while loading searcher after commit was detected. LockFailure(IoError(Os { code: 2, kind: NotFound, message: \"No such file or directory\" }), None)",
            ),
        ));
    }

    #[test]
    fn demotes_gpui_asset_cache_http_not_found_from_log_target() {
        assert!(should_demote_gpui_asset_cache_http_not_found(
            &tracing::Level::ERROR,
            "log",
            Some("gpui::asset_cache"),
            Some(
                "Failed to load asset: unexpected http status for https://icons.duckduckgo.com/ip3/api.github.com.ico: 404 Not Found, body: PNG",
            ),
        ));
    }

    #[test]
    fn demotes_gpui_asset_cache_generic_http_not_found() {
        assert!(should_demote_gpui_asset_cache_http_not_found(
            &tracing::Level::ERROR,
            "gpui::asset_cache",
            None,
            Some(
                "Failed to load asset: unexpected http status for https://example.com/missing.png: 404 Not Found, body: not found",
            ),
        ));
    }

    #[test]
    fn keeps_gpui_asset_cache_http_server_error_as_event() {
        assert!(!should_demote_gpui_asset_cache_http_not_found(
            &tracing::Level::ERROR,
            "log",
            Some("gpui::asset_cache"),
            Some(
                "Failed to load asset: unexpected http status for https://example.com/favicon.ico: 500 Internal Server Error, body: error",
            ),
        ));
    }

    #[test]
    fn keeps_gpui_asset_cache_not_found_from_other_targets_as_event() {
        assert!(!should_demote_gpui_asset_cache_http_not_found(
            &tracing::Level::ERROR,
            "pioneer_desktop",
            None,
            Some(
                "Failed to load asset: unexpected http status for https://example.com/favicon.ico: 404 Not Found, body: not found",
            ),
        ));
    }

    #[test]
    fn demotes_rathole_client_control_channel_network_retry() {
        assert!(should_demote_rathole_client_control_channel_retry(
            &tracing::Level::ERROR,
            "rathole::client",
            None,
            Some(
                "Failed to run the control channel: Failed to read cmd: Connection reset by peer (os error 54). Retry in 507.880488ms...",
            ),
        ));
        assert!(should_demote_rathole_client_control_channel_retry(
            &tracing::Level::ERROR,
            "rathole::client",
            None,
            Some(
                "Failed to run the control channel: Failed to read cmd: early eof. Retry in 470.57167ms...",
            ),
        ));
        assert!(should_demote_rathole_client_control_channel_retry(
            &tracing::Level::ERROR,
            "rathole::client",
            None,
            Some(
                "Failed to run the control channel: Heartbeat timed out. Retry in 539.863684ms...",
            ),
        ));
    }

    #[test]
    fn keeps_rathole_client_control_channel_auth_failure_as_event() {
        assert!(!should_demote_rathole_client_control_channel_retry(
            &tracing::Level::ERROR,
            "rathole::client",
            None,
            Some(
                "Failed to run the control channel: Authentication failed: pioneer_gateway: Incorrect token. Retry in 1s...",
            ),
        ));
    }

    #[test]
    fn keeps_rathole_client_control_channel_retry_from_other_targets_as_event() {
        assert!(!should_demote_rathole_client_control_channel_retry(
            &tracing::Level::ERROR,
            "pioneer_gateway",
            None,
            Some(
                "Failed to run the control channel: Failed to read cmd: Connection reset by peer (os error 54). Retry in 507.880488ms...",
            ),
        ));
    }

    #[test]
    fn preserves_default_event_filtering() {
        assert_eq!(
            sentry_event_filter(&tracing::Level::ERROR).bits(),
            EventFilter::Event.bits()
        );
        assert_eq!(
            sentry_event_filter(&tracing::Level::TRACE).bits(),
            EventFilter::Ignore.bits()
        );
        assert_eq!(
            sentry_event_filter(&tracing::Level::INFO).bits(),
            EventFilter::Breadcrumb.bits()
        );
    }
}
