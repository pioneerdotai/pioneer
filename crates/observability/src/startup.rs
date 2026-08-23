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
    AgentDomainUpgrade,
    ServicesPrepare,
    ListenerBind,
    ServicesStart,
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
            Self::AgentDomainUpgrade => "agent_domain.upgrade",
            Self::ServicesPrepare => "services.prepare",
            Self::ListenerBind => "listener.bind",
            Self::ServicesStart => "services.start",
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
    UiComponentsInitialize,
    WindowOpen,
    GatewayRuntimeLoad,
    GatewaySessionConnect,
    AuthorizationLoad,
    GatewaySettingsLoad,
    WorkspaceLoad,
    ProviderLoad,
    CliRuntimeRequest,
    CliRuntimeResponseApply,
    ComposerCapabilityTargetResolve,
    ThreadTreeLoad,
    ActiveThreadResolve,
    ActiveThreadBootstrap,
    ActiveThreadSubscribe,
    ThreadCapabilitiesLoad,
    ComposerModelSelectionResolve,
    ComposerPolicyReconcile,
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
            Self::UiComponentsInitialize => "ui_components.initialize",
            Self::WindowOpen => "window.open",
            Self::GatewayRuntimeLoad => "gateway_runtime.load",
            Self::GatewaySessionConnect => "gateway_session.connect",
            Self::AuthorizationLoad => "authorization.load",
            Self::GatewaySettingsLoad => "gateway_settings.load",
            Self::WorkspaceLoad => "workspace.load",
            Self::ProviderLoad => "providers.load",
            Self::CliRuntimeRequest => "cli_runtimes.request",
            Self::CliRuntimeResponseApply => "cli_runtimes.response.apply",
            Self::ComposerCapabilityTargetResolve => "composer.capability_target.resolve",
            Self::ThreadTreeLoad => "thread_tree.load",
            Self::ActiveThreadResolve => "active_thread.resolve",
            Self::ActiveThreadBootstrap => "active_thread.bootstrap",
            Self::ActiveThreadSubscribe => "active_thread.subscribe",
            Self::ThreadCapabilitiesLoad => "thread_capabilities.load",
            Self::ComposerModelSelectionResolve => "composer.model_selection.resolve",
            Self::ComposerPolicyReconcile => "composer.policy.reconcile",
            Self::OperationalFrame => "ui.operational_frame",
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
    AuthorizationLoad,
    WorkspaceLoad,
    ThreadTreeLoad,
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
            Self::AuthorizationLoad => "authorization.load",
            Self::WorkspaceLoad => "workspace.load",
            Self::ThreadTreeLoad => "thread_tree.load",
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
            "authorization.load" => Self::AuthorizationLoad,
            "workspace.load" => Self::WorkspaceLoad,
            "thread_tree.load" => Self::ThreadTreeLoad,
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
}

#[derive(Clone, Debug)]
pub struct MobileStartupReport {
    pub started_at: SystemTime,
    pub duration: Duration,
    pub outcome: MobileStartupOutcome,
    pub stages: Vec<MobileStartupStageTiming>,
}

pub fn record_mobile_startup(report: MobileStartupReport) {
    let stages = report
        .stages
        .into_iter()
        .map(|stage| StageRecord {
            name: stage.stage.as_str(),
            started_at: end_timestamp(report.started_at, stage.start_offset),
            elapsed: stage.duration,
            outcome: if stage.failed {
                StageOutcome::Error
            } else {
                StageOutcome::Ok
            },
        })
        .collect();
    emit_startup_observability(&StartupSnapshot {
        target: TelemetryTarget::Mobile,
        root_name: "mobile.startup",
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
}

impl StageOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }

    const fn status(self) -> Status {
        match self {
            Self::Ok => Status::Ok,
            Self::Error => Status::Error {
                description: std::borrow::Cow::Borrowed("startup stage failed"),
            },
        }
    }
}

#[derive(Clone, Debug)]
struct StageRecord {
    name: &'static str,
    started_at: SystemTime,
    elapsed: Duration,
    outcome: StageOutcome,
}

#[derive(Debug)]
struct StartupState {
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
                started_at: SystemTime::now(),
                started_instant: Instant::now(),
                stages: Vec::new(),
                finalized: false,
            })),
        }
    }

    fn stage(&self, name: &'static str) -> StartupStageGuard {
        StartupStageGuard {
            timeline: self.clone(),
            name,
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
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
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                stages: state.stages.clone(),
                outcome,
                failed,
            }
        };

        if super::telemetry_enabled() {
            emit_startup_observability(&snapshot);
        }
    }

    fn record_stage(
        &self,
        name: &'static str,
        started_at: SystemTime,
        elapsed: Duration,
        outcome: StageOutcome,
    ) {
        let mut state = self.lock();
        if !state.finalized {
            state.stages.push(StageRecord {
                name,
                started_at,
                elapsed,
                outcome,
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
    finished: bool,
}

impl StartupStageGuard {
    fn succeed(mut self) {
        self.finish(StageOutcome::Ok);
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

    #[must_use]
    pub fn stage(&self, stage: GatewayStartupStage) -> GatewayStartupStageGuard {
        GatewayStartupStageGuard(self.timeline.stage(stage.as_str()))
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

    #[must_use]
    pub fn stage(&self, stage: DesktopStartupStage) -> DesktopStartupStageGuard {
        DesktopStartupStageGuard(self.timeline.stage(stage.as_str()))
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
}

struct StartupSnapshot {
    target: TelemetryTarget,
    root_name: &'static str,
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
    if !super::telemetry_enabled() {
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
        state.startup_metrics.stage_duration.record(
            stage.elapsed.as_secs_f64() * 1_000.0,
            &[
                KeyValue::new("startup.stage", stage.name),
                KeyValue::new("outcome", stage.outcome.as_str()),
            ],
        );
    }

    let root_builder = SpanBuilder::from_name(snapshot.root_name)
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(root_attributes);
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);

    for stage in &snapshot.stages {
        let builder = SpanBuilder::from_name(stage.name)
            .with_kind(SpanKind::Internal)
            .with_start_time(stage.started_at)
            .with_attributes([
                KeyValue::new("startup.stage", stage.name),
                KeyValue::new("outcome", stage.outcome.as_str()),
            ]);
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
        DesktopStartupOutcome, DesktopStartupStage, DesktopStartupTrace, GatewayStartupStage,
        GatewayStartupTrace, MobileStartupOutcome, MobileStartupStage, StageOutcome,
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
}
