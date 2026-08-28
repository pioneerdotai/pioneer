use crate::telemetry::TelemetryTarget;
use opentelemetry::trace::{
    Span as _, SpanBuilder, SpanKind, Status, TraceContextExt, Tracer as _,
};
use opentelemetry::{Context, KeyValue};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayStartupStage {
    ConfigLoad,
    RuntimePrepare,
    SettingsLoad,
    ObservabilityInit,
    RuntimeBuild,
    SecurityValidate,
    SecretsOpen,
    SecurityInitialize,
    DatabaseOpen,
    DatabaseConfigure,
    DatabaseMigrate,
    DatabaseBootstrap,
    IdentityBootstrap,
    ServicesInitialize,
    ServicesPrepare,
    ListenerBind,
    ServicesStart,
    ServicesVoiceInputStart,
    ServicesSelfImprovementStart,
    ServicesNotificationsStart,
    ServicesResilienceStart,
    ServicesMcpStart,
    ServicesSkillsWatcherStart,
    ServicesDatabaseWorkersStart,
    ServicesRemoteAccessStart,
    ServicesProviderReadinessStart,
}

impl GatewayStartupStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfigLoad => "config.load",
            Self::RuntimePrepare => "runtime.prepare",
            Self::SettingsLoad => "settings.load",
            Self::ObservabilityInit => "observability.init",
            Self::RuntimeBuild => "runtime.build",
            Self::SecurityValidate => "security.validate",
            Self::SecretsOpen => "secrets.open",
            Self::SecurityInitialize => "security.initialize",
            Self::DatabaseOpen => "database.open",
            Self::DatabaseConfigure => "database.configure",
            Self::DatabaseMigrate => "database.migrate",
            Self::DatabaseBootstrap => "database.bootstrap",
            Self::IdentityBootstrap => "identity.bootstrap",
            Self::ServicesInitialize => "services.initialize",
            Self::ServicesPrepare => "services.prepare",
            Self::ListenerBind => "listener.bind",
            Self::ServicesStart => "services.start",
            Self::ServicesVoiceInputStart => "services.voice_input.start",
            Self::ServicesSelfImprovementStart => "services.self_improvement.start",
            Self::ServicesNotificationsStart => "services.notifications.start",
            Self::ServicesResilienceStart => "services.resilience.start",
            Self::ServicesMcpStart => "services.mcp.start",
            Self::ServicesSkillsWatcherStart => "services.skills_watcher.start",
            Self::ServicesDatabaseWorkersStart => "services.database_workers.start",
            Self::ServicesRemoteAccessStart => "services.remote_access.start",
            Self::ServicesProviderReadinessStart => "services.provider_readiness.start",
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum DesktopStartupStage {
    ConfigLoad,
    ConsentLoad,
    ObservabilityInit,
    LocaleInitialize,
    RuntimeHomePrepare,
    HttpClientInitialize,
    UiRuntimeInitialize,
    UiEventLoopEnter,
    UiComponentsInitialize,
    WindowOpen,
    GatewayRuntimeLoad,
    GatewayRuntimeUpdateCheck,
    GatewayRuntimeStateLoad,
    GatewayRuntimeLocalDiscovery,
    GatewayRuntimeLocalRecovery,
    GatewayRuntimeVersionReconcile,
    GatewayRuntimeReachabilityCheck,
    GatewayRuntimeServiceStatusCheck,
    GatewayRuntimeServiceStart,
    GatewayRuntimeSessionEnsure,
    GatewayRuntimeConnectionPrepare,
    GatewaySessionConnect,
    GatewaySessionAttempt,
    GatewaySessionBackoff,
    GatewaySessionIdentityVerify,
    AuthorizationLoad,
    GatewaySettingsLoad,
    WorkspaceLoad,
    ProviderLoad,
    ThreadTreeLoad,
    ThreadTreeRequest,
    ThreadTreeResponseApply,
    ActiveThreadResolve,
    ActiveThreadBootstrap,
    ActiveThreadSubscribe,
    ThreadCapabilitiesLoad,
    ComposerModelSelectionResolve,
    ComposerPolicyReconcile,
    ReadinessToRender,
    OperationalFrame,
}

impl DesktopStartupStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfigLoad => "config.load",
            Self::ConsentLoad => "consent.load",
            Self::ObservabilityInit => "observability.init",
            Self::LocaleInitialize => "locale.initialize",
            Self::RuntimeHomePrepare => "runtime_home.prepare",
            Self::HttpClientInitialize => "http_client.initialize",
            Self::UiRuntimeInitialize => "ui_runtime.initialize",
            Self::UiEventLoopEnter => "ui_runtime.event_loop.enter",
            Self::UiComponentsInitialize => "ui_components.initialize",
            Self::WindowOpen => "window.open",
            Self::GatewayRuntimeLoad => "gateway_runtime.load",
            Self::GatewayRuntimeUpdateCheck => "gateway_runtime.update_check",
            Self::GatewayRuntimeStateLoad => "gateway_runtime.state_load",
            Self::GatewayRuntimeLocalDiscovery => "gateway_runtime.local_discovery",
            Self::GatewayRuntimeLocalRecovery => "gateway_runtime.local_recovery",
            Self::GatewayRuntimeVersionReconcile => "gateway_runtime.version_reconcile",
            Self::GatewayRuntimeReachabilityCheck => "gateway_runtime.reachability_check",
            Self::GatewayRuntimeServiceStatusCheck => "gateway_runtime.service_status_check",
            Self::GatewayRuntimeServiceStart => "gateway_runtime.service_start",
            Self::GatewayRuntimeSessionEnsure => "gateway_runtime.session_ensure",
            Self::GatewayRuntimeConnectionPrepare => "gateway_runtime.connection_prepare",
            Self::GatewaySessionConnect => "gateway_session.connect",
            Self::GatewaySessionAttempt => "gateway_session.connect_attempt",
            Self::GatewaySessionBackoff => "gateway_session.backoff",
            Self::GatewaySessionIdentityVerify => "gateway_session.identity_verify",
            Self::AuthorizationLoad => "authorization.load",
            Self::GatewaySettingsLoad => "gateway_settings.load",
            Self::WorkspaceLoad => "workspace.load",
            Self::ProviderLoad => "providers.load",
            Self::ThreadTreeLoad => "thread_tree.load",
            Self::ThreadTreeRequest => "thread_tree.request",
            Self::ThreadTreeResponseApply => "thread_tree.response.apply",
            Self::ActiveThreadResolve => "active_thread.resolve",
            Self::ActiveThreadBootstrap => "active_thread.bootstrap",
            Self::ActiveThreadSubscribe => "active_thread.subscribe",
            Self::ThreadCapabilitiesLoad => "thread_capabilities.load",
            Self::ComposerModelSelectionResolve => "composer.model_selection.resolve",
            Self::ComposerPolicyReconcile => "composer.policy.reconcile",
            Self::ReadinessToRender => "ui.readiness_to_render",
            Self::OperationalFrame => "ui.operational_frame",
        }
    }
}

/// Stable, low-cardinality classification for a failed Desktop Gateway
/// transport attempt. Raw transport errors, endpoints and credentials must
/// never be attached to startup telemetry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopGatewayConnectFailureClass {
    AccessRefreshRequired,
    ConnectionRefused,
    ConnectionReset,
    Dns,
    Handshake,
    Network,
    Timeout,
    Tls,
    Transport,
}

impl DesktopGatewayConnectFailureClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AccessRefreshRequired => "access_refresh_required",
            Self::ConnectionRefused => "connection_refused",
            Self::ConnectionReset => "connection_reset",
            Self::Dns => "dns",
            Self::Handshake => "handshake",
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::Tls => "tls",
            Self::Transport => "transport",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopStartupOutcome {
    Ready,
    SetupRequired,
    AuthenticationRequired,
    Degraded,
}

impl DesktopStartupOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::SetupRequired => "setup_required",
            Self::AuthenticationRequired => "authentication_required",
            Self::Degraded => "degraded",
        }
    }

    const fn failed(self) -> bool {
        matches!(self, Self::Degraded)
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum MobileStartupStage {
    NativeLaunch,
    JavaScriptRuntime,
    FontsLoad,
    ClientInitialize,
    GatewayRegistryHydrate,
    NavigationMount,
    GatewaySessionConnect,
    GatewaySessionConnectAttempt,
    GatewaySessionIdentityVerify,
    AuthorizationLoad,
    AuthorizationRegistryLoad,
    AuthorizationCredentialsLoad,
    AuthorizationRefreshIntentPersist,
    AuthorizationRefreshRequest,
    AuthorizationCredentialsPersist,
    WorkspaceLoad,
    ThreadTreeLoad,
    ThreadTreeRequest,
    ThreadTreeResponseApply,
    ComposerPrepare,
    OperationalFrame,
    SplashHide,
}

impl MobileStartupStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeLaunch => "native.launch",
            Self::JavaScriptRuntime => "javascript.runtime",
            Self::FontsLoad => "fonts.load",
            Self::ClientInitialize => "client.initialize",
            Self::GatewayRegistryHydrate => "gateway_registry.hydrate",
            Self::NavigationMount => "navigation.mount",
            Self::GatewaySessionConnect => "gateway_session.connect",
            Self::GatewaySessionConnectAttempt => "gateway_session.connect_attempt",
            Self::GatewaySessionIdentityVerify => "gateway_session.identity_verify",
            Self::AuthorizationLoad => "authorization.load",
            Self::AuthorizationRegistryLoad => "authorization.registry.load",
            Self::AuthorizationCredentialsLoad => "authorization.credentials.load",
            Self::AuthorizationRefreshIntentPersist => "authorization.refresh_intent.persist",
            Self::AuthorizationRefreshRequest => "authorization.refresh.request",
            Self::AuthorizationCredentialsPersist => "authorization.credentials.persist",
            Self::WorkspaceLoad => "workspace.load",
            Self::ThreadTreeLoad => "thread_tree.load",
            Self::ThreadTreeRequest => "thread_tree.request",
            Self::ThreadTreeResponseApply => "thread_tree.response.apply",
            Self::ComposerPrepare => "composer.prepare",
            Self::OperationalFrame => "ui.operational_frame",
            Self::SplashHide => "splash.hide",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "native.launch" => Self::NativeLaunch,
            "javascript.runtime" => Self::JavaScriptRuntime,
            "fonts.load" => Self::FontsLoad,
            "client.initialize" => Self::ClientInitialize,
            "gateway_registry.hydrate" => Self::GatewayRegistryHydrate,
            "navigation.mount" => Self::NavigationMount,
            "gateway_session.connect" => Self::GatewaySessionConnect,
            "gateway_session.connect_attempt" => Self::GatewaySessionConnectAttempt,
            "gateway_session.identity_verify" => Self::GatewaySessionIdentityVerify,
            "authorization.load" => Self::AuthorizationLoad,
            "authorization.registry.load" => Self::AuthorizationRegistryLoad,
            "authorization.credentials.load" => Self::AuthorizationCredentialsLoad,
            "authorization.refresh_intent.persist" => Self::AuthorizationRefreshIntentPersist,
            "authorization.refresh.request" => Self::AuthorizationRefreshRequest,
            "authorization.credentials.persist" => Self::AuthorizationCredentialsPersist,
            "workspace.load" => Self::WorkspaceLoad,
            "thread_tree.load" => Self::ThreadTreeLoad,
            "thread_tree.request" => Self::ThreadTreeRequest,
            "thread_tree.response.apply" => Self::ThreadTreeResponseApply,
            "composer.prepare" => Self::ComposerPrepare,
            "ui.operational_frame" => Self::OperationalFrame,
            "splash.hide" => Self::SplashHide,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MobileStartupOutcome {
    Ready,
    SetupRequired,
    AuthenticationRequired,
    Degraded,
}

impl MobileStartupOutcome {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "ready" => Self::Ready,
            "setup_required" => Self::SetupRequired,
            "authentication_required" => Self::AuthenticationRequired,
            "degraded" => Self::Degraded,
            _ => return None,
        })
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::SetupRequired => "setup_required",
            Self::AuthenticationRequired => "authentication_required",
            Self::Degraded => "degraded",
        }
    }

    const fn failed(self) -> bool {
        matches!(self, Self::Degraded)
    }
}

#[derive(Clone, Debug)]
pub struct MobileStartupStageTiming {
    pub stage: MobileStartupStage,
    pub start_offset: Duration,
    pub duration: Duration,
    pub failed: bool,
    pub cancelled: bool,
}

#[derive(Clone, Debug)]
pub struct MobileStartupReport {
    pub started_at: SystemTime,
    pub duration: Duration,
    pub outcome: MobileStartupOutcome,
    pub stages: Vec<MobileStartupStageTiming>,
}

pub fn record_mobile_startup(report: MobileStartupReport) {
    let (telemetry_enabled, consent_generation) = super::telemetry_consent_snapshot();
    let stages = report
        .stages
        .into_iter()
        .map(|stage| StageRecord {
            name: stage.stage.as_str(),
            started_at: end_timestamp(report.started_at, stage.start_offset),
            elapsed: stage.duration,
            outcome: match (stage.failed, stage.cancelled) {
                (true, _) => StageOutcome::Error,
                (false, true) => StageOutcome::Cancelled,
                (false, false) => StageOutcome::Ok,
            },
            diagnostics: StageDiagnostics::default(),
        })
        .collect();
    emit_startup_observability(&StartupSnapshot {
        target: TelemetryTarget::Mobile,
        root_name: "mobile.startup",
        consent_generation: telemetry_enabled.then_some(consent_generation),
        started_at: report.started_at,
        elapsed: report.duration,
        stages,
        outcome: report.outcome.as_str(),
        failed: report.outcome.failed(),
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StageOutcome {
    Ok,
    Error,
    Cancelled,
}

impl StageOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }

    const fn status(self) -> Status {
        match self {
            Self::Ok => Status::Ok,
            Self::Error => Status::Error {
                description: std::borrow::Cow::Borrowed("startup stage failed"),
            },
            // Cancellation means the application reached a valid terminal
            // branch (for example setup UI) before this stage was required.
            // It is observable, but it is not a failed operation.
            Self::Cancelled => Status::Unset,
        }
    }
}

#[derive(Clone, Debug)]
struct StageRecord {
    name: &'static str,
    started_at: SystemTime,
    elapsed: Duration,
    outcome: StageOutcome,
    diagnostics: StageDiagnostics,
}

#[derive(Clone, Debug, Default)]
struct StageDiagnostics {
    connect_attempt: Option<i64>,
    reconnect_attempt: Option<i64>,
    backoff_delay_ms: Option<i64>,
    failure_class: Option<&'static str>,
}

#[derive(Debug)]
struct StartupState {
    consent_generation: Option<u64>,
    started_at: SystemTime,
    started_instant: Instant,
    stages: Vec<StageRecord>,
    finalized: bool,
}

#[derive(Clone, Debug)]
struct StartupTimeline {
    inner: Arc<Mutex<StartupState>>,
}

impl StartupTimeline {
    fn start() -> Self {
        Self {
            inner: Arc::new(Mutex::new(StartupState {
                // Startup begins before the persisted preference is known.
                // The application binds the timeline to the loaded consent
                // generation before initializing the exporter.
                consent_generation: None,
                started_at: SystemTime::now(),
                started_instant: Instant::now(),
                stages: Vec::new(),
                finalized: false,
            })),
        }
    }

    fn bind_consent(&self) {
        let (enabled, generation) = super::telemetry_consent_snapshot();
        let mut state = self.lock();
        if !state.finalized {
            state.consent_generation = enabled.then_some(generation);
        }
    }

    fn stage(&self, name: &'static str) -> StartupStageGuard {
        StartupStageGuard {
            timeline: self.clone(),
            name,
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
            diagnostics: StageDiagnostics::default(),
            finished: false,
        }
    }

    fn finish(
        &self,
        target: TelemetryTarget,
        root_name: &'static str,
        outcome: &'static str,
        failed: bool,
    ) {
        let snapshot = {
            let mut state = self.lock();
            if state.finalized {
                return;
            }
            state.finalized = true;
            StartupSnapshot {
                target,
                root_name,
                consent_generation: state.consent_generation,
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                stages: state.stages.clone(),
                outcome,
                failed,
            }
        };

        emit_startup_observability(&snapshot);
    }

    fn record_stage(
        &self,
        name: &'static str,
        started_at: SystemTime,
        elapsed: Duration,
        outcome: StageOutcome,
        diagnostics: StageDiagnostics,
    ) {
        let mut state = self.lock();
        if !state.finalized {
            state.stages.push(StageRecord {
                name,
                started_at,
                elapsed,
                outcome,
                diagnostics,
            });
        }
    }

    fn lock(&self) -> MutexGuard<'_, StartupState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct StartupStageGuard {
    timeline: StartupTimeline,
    name: &'static str,
    started_at: SystemTime,
    started_instant: Instant,
    diagnostics: StageDiagnostics,
    finished: bool,
}

impl StartupStageGuard {
    fn succeed(mut self) {
        self.finish(StageOutcome::Ok);
    }

    fn cancel(mut self) {
        self.finish(StageOutcome::Cancelled);
    }

    fn finish(&mut self, outcome: StageOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.timeline.record_stage(
            self.name,
            self.started_at,
            self.started_instant.elapsed(),
            outcome,
            self.diagnostics.clone(),
        );
    }
}

impl Drop for StartupStageGuard {
    fn drop(&mut self) {
        self.finish(StageOutcome::Error);
    }
}

#[derive(Clone, Debug)]
pub struct GatewayStartupTrace {
    timeline: StartupTimeline,
}

impl Default for GatewayStartupTrace {
    fn default() -> Self {
        Self::start()
    }
}

impl GatewayStartupTrace {
    #[must_use]
    pub fn start() -> Self {
        Self {
            timeline: StartupTimeline::start(),
        }
    }

    #[must_use = "the startup stage guard must be completed or deliberately dropped"]
    pub fn stage(&self, stage: GatewayStartupStage) -> GatewayStartupStageGuard {
        GatewayStartupStageGuard(self.timeline.stage(stage.as_str()))
    }

    /// Binds early startup timings to the persisted consent decision.
    pub fn bind_consent(&self) {
        self.timeline.bind_consent();
    }

    pub fn finish_success(&self) {
        self.timeline
            .finish(TelemetryTarget::Gateway, "gateway.startup", "ok", false);
    }

    pub fn finish_failure(&self) {
        self.timeline
            .finish(TelemetryTarget::Gateway, "gateway.startup", "error", true);
    }
}

#[must_use = "a startup stage must be marked successful; dropping it records a failure"]
pub struct GatewayStartupStageGuard(StartupStageGuard);

impl GatewayStartupStageGuard {
    pub fn succeed(self) {
        self.0.succeed();
    }
}

#[derive(Clone, Debug)]
pub struct DesktopStartupTrace {
    timeline: StartupTimeline,
}

impl Default for DesktopStartupTrace {
    fn default() -> Self {
        Self::start()
    }
}

impl DesktopStartupTrace {
    #[must_use]
    pub fn start() -> Self {
        Self {
            timeline: StartupTimeline::start(),
        }
    }

    #[must_use = "the startup stage guard must be completed or deliberately dropped"]
    pub fn stage(&self, stage: DesktopStartupStage) -> DesktopStartupStageGuard {
        DesktopStartupStageGuard(self.timeline.stage(stage.as_str()))
    }

    #[must_use = "the Gateway connection attempt must be completed"]
    pub fn gateway_session_attempt(&self, attempt: u32) -> DesktopStartupStageGuard {
        let mut guard = self
            .timeline
            .stage(DesktopStartupStage::GatewaySessionAttempt.as_str());
        guard.diagnostics.connect_attempt = Some(i64::from(attempt));
        DesktopStartupStageGuard(guard)
    }

    #[must_use = "the Gateway reconnect backoff must be completed"]
    pub fn gateway_session_backoff(
        &self,
        reconnect_attempt: u32,
        delay_ms: u64,
    ) -> DesktopStartupStageGuard {
        let mut guard = self
            .timeline
            .stage(DesktopStartupStage::GatewaySessionBackoff.as_str());
        guard.diagnostics.reconnect_attempt = Some(i64::from(reconnect_attempt));
        guard.diagnostics.backoff_delay_ms = Some(i64::try_from(delay_ms).unwrap_or(i64::MAX));
        DesktopStartupStageGuard(guard)
    }

    /// Binds early startup timings to the persisted consent decision.
    pub fn bind_consent(&self) {
        self.timeline.bind_consent();
    }

    pub fn finish(&self, outcome: DesktopStartupOutcome) {
        self.timeline.finish(
            TelemetryTarget::Desktop,
            "desktop.startup",
            outcome.as_str(),
            outcome.failed(),
        );
    }
}

#[must_use = "a startup stage must be marked successful; dropping it records a failure"]
pub struct DesktopStartupStageGuard(StartupStageGuard);

impl DesktopStartupStageGuard {
    pub fn succeed(self) {
        self.0.succeed();
    }

    pub fn cancel(self) {
        self.0.cancel();
    }

    pub fn fail_gateway_connect(mut self, class: DesktopGatewayConnectFailureClass) {
        self.0.diagnostics.failure_class = Some(class.as_str());
        self.0.finish(StageOutcome::Error);
    }
}

struct StartupSnapshot {
    target: TelemetryTarget,
    root_name: &'static str,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    elapsed: Duration,
    stages: Vec<StageRecord>,
    outcome: &'static str,
    failed: bool,
}

impl StartupSnapshot {
    fn failed_stage(&self) -> &'static str {
        self.stages
            .iter()
            .rev()
            .find(|record| record.outcome == StageOutcome::Error)
            .map(|record| record.name)
            .unwrap_or("unknown")
    }
}

fn emit_startup_observability(snapshot: &StartupSnapshot) {
    if !super::telemetry_sample_allowed(snapshot.consent_generation) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    if state.target != snapshot.target {
        return;
    }

    let failed_stage = if snapshot.failed {
        snapshot.failed_stage()
    } else {
        "none"
    };
    let root_attributes = [
        KeyValue::new("outcome", snapshot.outcome),
        KeyValue::new("startup.failed_stage", failed_stage),
        KeyValue::new("startup.type", "cold"),
    ];
    state
        .startup_metrics
        .duration
        .record(snapshot.elapsed.as_secs_f64() * 1_000.0, &root_attributes);
    if snapshot.failed {
        state.startup_metrics.failures.add(1, &root_attributes);
    }
    for stage in &snapshot.stages {
        let mut metric_attributes = vec![
            KeyValue::new("startup.stage", stage.name),
            KeyValue::new("outcome", stage.outcome.as_str()),
        ];
        if let Some(failure_class) = stage.diagnostics.failure_class {
            metric_attributes.push(KeyValue::new("failure.class", failure_class));
        }
        state
            .startup_metrics
            .stage_duration
            .record(stage.elapsed.as_secs_f64() * 1_000.0, &metric_attributes);
    }

    let root_builder = SpanBuilder::from_name(snapshot.root_name)
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(root_attributes);
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);

    for stage in &snapshot.stages {
        let mut attributes = vec![
            KeyValue::new("startup.stage", stage.name),
            KeyValue::new("outcome", stage.outcome.as_str()),
        ];
        if let Some(attempt) = stage.diagnostics.connect_attempt {
            attributes.push(KeyValue::new("gateway.connection.attempt", attempt));
        }
        if let Some(attempt) = stage.diagnostics.reconnect_attempt {
            attributes.push(KeyValue::new("gateway.reconnect.attempt", attempt));
        }
        if let Some(delay_ms) = stage.diagnostics.backoff_delay_ms {
            attributes.push(KeyValue::new("gateway.backoff.delay_ms", delay_ms));
        }
        if let Some(failure_class) = stage.diagnostics.failure_class {
            attributes.push(KeyValue::new("failure.class", failure_class));
        }
        let builder = SpanBuilder::from_name(stage.name)
            .with_kind(SpanKind::Internal)
            .with_start_time(stage.started_at)
            .with_attributes(attributes);
        let mut span = state.tracer.build_with_context(builder, &root_context);
        span.set_status(stage.outcome.status());
        span.end_with_timestamp(end_timestamp(stage.started_at, stage.elapsed));
    }

    root_context.span().set_status(if snapshot.failed {
        Status::Error {
            description: std::borrow::Cow::Borrowed("startup did not reach a healthy state"),
        }
    } else {
        Status::Ok
    });
    root_context
        .span()
        .end_with_timestamp(end_timestamp(snapshot.started_at, snapshot.elapsed));
}

fn end_timestamp(started_at: SystemTime, elapsed: Duration) -> SystemTime {
    started_at
        .checked_add(elapsed)
        .unwrap_or_else(SystemTime::now)
}

#[cfg(test)]
mod tests {
    use super::{
        DesktopGatewayConnectFailureClass, DesktopStartupOutcome, DesktopStartupStage,
        DesktopStartupTrace, GatewayStartupStage, GatewayStartupTrace, MobileStartupOutcome,
        MobileStartupStage, StageOutcome,
    };

    #[test]
    fn successful_and_dropped_gateway_stages_have_bounded_outcomes() {
        let trace = GatewayStartupTrace::start();
        trace.stage(GatewayStartupStage::ConfigLoad).succeed();
        drop(trace.stage(GatewayStartupStage::SettingsLoad));

        let state = trace.timeline.lock();
        assert_eq!(state.stages.len(), 2);
        assert_eq!(state.stages[0].name, "config.load");
        assert_eq!(state.stages[0].outcome, StageOutcome::Ok);
        assert_eq!(state.stages[1].name, "settings.load");
        assert_eq!(state.stages[1].outcome, StageOutcome::Error);
    }

    #[test]
    fn cancelled_stage_is_observable_without_becoming_an_error() {
        let timeline = super::StartupTimeline::start();
        timeline.stage("test.cancelled").cancel();

        let state = timeline.lock();
        assert_eq!(state.stages.len(), 1);
        assert_eq!(state.stages[0].outcome, StageOutcome::Cancelled);
        assert_eq!(state.stages[0].outcome.as_str(), "cancelled");
    }

    #[test]
    fn finalization_is_idempotent() {
        let trace = GatewayStartupTrace::start();
        trace.finish_failure();
        trace.finish_success();

        assert!(trace.timeline.lock().finalized);
    }

    #[test]
    fn desktop_and_mobile_contracts_remain_distinct() {
        let desktop = DesktopStartupTrace::start();
        desktop
            .stage(DesktopStartupStage::GatewayRuntimeLoad)
            .succeed();
        desktop.finish(DesktopStartupOutcome::Ready);

        assert_eq!(DesktopStartupStage::WindowOpen.as_str(), "window.open");
        assert_eq!(
            MobileStartupStage::GatewayRegistryHydrate.as_str(),
            "gateway_registry.hydrate"
        );
        assert_eq!(
            MobileStartupOutcome::parse("authentication_required"),
            Some(MobileStartupOutcome::AuthenticationRequired)
        );
        assert!(MobileStartupStage::parse("window.open").is_none());
    }

    #[test]
    fn startup_diagnostic_stages_have_stable_names() {
        assert_eq!(
            GatewayStartupStage::ServicesResilienceStart.as_str(),
            "services.resilience.start"
        );
        assert_eq!(
            GatewayStartupStage::ServicesProviderReadinessStart.as_str(),
            "services.provider_readiness.start"
        );
        assert_eq!(
            DesktopStartupStage::GatewaySessionAttempt.as_str(),
            "gateway_session.connect_attempt"
        );
        assert_eq!(
            DesktopStartupStage::GatewaySessionBackoff.as_str(),
            "gateway_session.backoff"
        );
        assert_eq!(
            DesktopStartupStage::ThreadTreeResponseApply.as_str(),
            "thread_tree.response.apply"
        );
        assert_eq!(
            DesktopStartupStage::UiEventLoopEnter.as_str(),
            "ui_runtime.event_loop.enter"
        );
        assert_eq!(
            DesktopStartupStage::ReadinessToRender.as_str(),
            "ui.readiness_to_render"
        );
        assert_eq!(
            DesktopStartupStage::GatewayRuntimeServiceStart.as_str(),
            "gateway_runtime.service_start"
        );
        assert_eq!(
            MobileStartupStage::AuthorizationRefreshRequest.as_str(),
            "authorization.refresh.request"
        );
        assert_eq!(
            MobileStartupStage::GatewaySessionIdentityVerify.as_str(),
            "gateway_session.identity_verify"
        );
        assert_eq!(
            MobileStartupStage::parse("thread_tree.response.apply"),
            Some(MobileStartupStage::ThreadTreeResponseApply)
        );
    }

    #[test]
    fn stage_names_are_stable_and_low_cardinality() {
        assert_eq!(GatewayStartupStage::DatabaseOpen.as_str(), "database.open");
        assert_eq!(GatewayStartupStage::ListenerBind.as_str(), "listener.bind");
        assert_eq!(
            DesktopStartupStage::OperationalFrame.as_str(),
            "ui.operational_frame"
        );
        assert_eq!(
            MobileStartupStage::ComposerPrepare.as_str(),
            "composer.prepare"
        );
    }

    #[test]
    fn gateway_connect_diagnostics_keep_raw_errors_out_of_startup_state() {
        let trace = DesktopStartupTrace::start();
        trace
            .gateway_session_attempt(2)
            .fail_gateway_connect(DesktopGatewayConnectFailureClass::ConnectionRefused);
        trace.gateway_session_backoff(1, 500).succeed();

        let state = trace.timeline.lock();
        assert_eq!(state.stages.len(), 2);
        assert_eq!(state.stages[0].diagnostics.connect_attempt, Some(2));
        assert_eq!(
            state.stages[0].diagnostics.failure_class,
            Some("connection_refused")
        );
        assert_eq!(state.stages[1].diagnostics.reconnect_attempt, Some(1));
        assert_eq!(state.stages[1].diagnostics.backoff_delay_ms, Some(500));
    }
}
