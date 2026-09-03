use crate::telemetry::TelemetryTarget;
use opentelemetry::trace::{
    Span as _, SpanBuilder, SpanKind, Status, TraceContextExt, Tracer as _,
};
use opentelemetry::{Context, KeyValue};
use std::collections::HashSet;
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
    DatabaseReaderOpen,
    DatabaseReaderConfigure,
    DatabaseReaderValidate,
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
            Self::DatabaseReaderOpen => "database.reader.open",
            Self::DatabaseReaderConfigure => "database.reader.configure",
            Self::DatabaseReaderValidate => "database.reader.validate",
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopPostUpdateStage {
    GatewayVersionCheck,
    GatewayInstallerExecute,
    GatewayServiceStop,
    GatewayServiceStart,
    GatewayHealthWait,
    GatewaySessionConnect,
}

impl DesktopPostUpdateStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GatewayVersionCheck => "desktop_update.gateway.version_check",
            Self::GatewayInstallerExecute => "desktop_update.gateway.installer.execute",
            Self::GatewayServiceStop => "desktop_update.gateway.service.stop",
            Self::GatewayServiceStart => "desktop_update.gateway.service.start",
            Self::GatewayHealthWait => "desktop_update.gateway.health.wait",
            Self::GatewaySessionConnect => "desktop_update.gateway.session.connect",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DesktopPostUpdateContext {
    pub attempt_id: String,
    pub from_version: String,
    pub to_version: String,
    pub platform: String,
    pub process_exit_wait: Duration,
    pub apply_duration: Duration,
    pub relaunch_duration: Duration,
    pub total_duration: Duration,
    pub claimed_at: SystemTime,
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
        post_update: None,
        record_post_update_failure: false,
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
    post_update: Option<DesktopPostUpdateContext>,
    post_update_stages: HashSet<&'static str>,
    post_update_handoff_emitted: bool,
    post_update_stall_scheduled: bool,
    post_update_failure_emitted: bool,
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
                post_update: None,
                post_update_stages: HashSet::new(),
                post_update_handoff_emitted: false,
                post_update_stall_scheduled: false,
                post_update_failure_emitted: false,
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

    fn set_post_update_context(&self, context: DesktopPostUpdateContext) {
        let mut state = self.lock();
        if !state.finalized {
            state.post_update = Some(context);
        }
    }

    fn is_post_update(&self) -> bool {
        self.lock().post_update.is_some()
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

    fn post_update_stage(&self, name: &'static str) -> Option<StartupStageGuard> {
        let mut state = self.lock();
        if state.finalized || state.post_update.is_none() || !state.post_update_stages.insert(name)
        {
            return None;
        }
        drop(state);
        Some(self.stage(name))
    }

    fn record_post_update_stage(
        &self,
        name: &'static str,
        started_at: SystemTime,
        elapsed: Duration,
        succeeded: bool,
    ) {
        let mut state = self.lock();
        if state.finalized || state.post_update.is_none() || !state.post_update_stages.insert(name)
        {
            return;
        }
        state.stages.push(StageRecord {
            name,
            started_at,
            elapsed,
            outcome: if succeeded {
                StageOutcome::Ok
            } else {
                StageOutcome::Error
            },
            diagnostics: StageDiagnostics::default(),
        });
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
            let record_post_update_failure =
                failed && state.post_update.is_some() && !state.post_update_failure_emitted;
            if record_post_update_failure {
                state.post_update_failure_emitted = true;
            }
            StartupSnapshot {
                target,
                root_name,
                consent_generation: state.consent_generation,
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                stages: state.stages.clone(),
                post_update: state.post_update.clone(),
                record_post_update_failure,
                outcome,
                failed,
            }
        };

        emit_startup_observability(&snapshot);
    }

    fn emit_post_update_handoff(&self) {
        let snapshot = {
            let mut state = self.lock();
            if state.post_update_handoff_emitted {
                return;
            }
            let Some(context) = state.post_update.clone() else {
                return;
            };
            let Some(consent_generation) = state.consent_generation else {
                return;
            };
            state.post_update_handoff_emitted = true;
            PostUpdateHandoffSnapshot {
                consent_generation: Some(consent_generation),
                context,
            }
        };
        emit_post_update_handoff(&snapshot);
    }

    fn schedule_post_update_stall_checkpoint(&self, delay: Duration) {
        {
            let mut state = self.lock();
            if state.finalized
                || state.post_update.is_none()
                || state.consent_generation.is_none()
                || state.post_update_stall_scheduled
            {
                return;
            }
            state.post_update_stall_scheduled = true;
        }

        let timeline = self.clone();
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            timeline.emit_post_update_stall_checkpoint();
        });
    }

    fn emit_post_update_stall_checkpoint(&self) {
        let snapshot = {
            let mut state = self.lock();
            if state.finalized || state.post_update_failure_emitted {
                return;
            }
            let Some(context) = state.post_update.clone() else {
                return;
            };
            let Some(consent_generation) = state.consent_generation else {
                return;
            };
            state.post_update_failure_emitted = true;
            PostUpdateStallSnapshot {
                consent_generation: Some(consent_generation),
                context,
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                last_completed_stage: state
                    .stages
                    .last()
                    .map(|stage| stage.name)
                    .unwrap_or("none"),
            }
        };
        emit_post_update_stall(&snapshot);
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

    pub fn set_post_update_context(&self, context: DesktopPostUpdateContext) {
        self.timeline.set_post_update_context(context);
    }

    pub fn is_post_update(&self) -> bool {
        self.timeline.is_post_update()
    }

    #[must_use = "a post-update stage must be completed when it exists"]
    pub fn post_update_stage(
        &self,
        stage: DesktopPostUpdateStage,
    ) -> Option<DesktopPostUpdateStageGuard> {
        self.timeline
            .post_update_stage(stage.as_str())
            .map(DesktopPostUpdateStageGuard)
    }

    pub fn record_post_update_stage(
        &self,
        stage: DesktopPostUpdateStage,
        started_at: SystemTime,
        elapsed: Duration,
        succeeded: bool,
    ) {
        self.timeline
            .record_post_update_stage(stage.as_str(), started_at, elapsed, succeeded);
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

    pub fn emit_post_update_handoff(&self) {
        self.timeline.emit_post_update_handoff();
    }

    pub fn schedule_post_update_stall_checkpoint(&self, delay: Duration) {
        self.timeline.schedule_post_update_stall_checkpoint(delay);
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

#[must_use = "a post-update stage must be marked successful; dropping it records a failure"]
pub struct DesktopPostUpdateStageGuard(StartupStageGuard);

impl DesktopPostUpdateStageGuard {
    pub fn succeed(self) {
        self.0.succeed();
    }

    pub fn cancel(self) {
        self.0.cancel();
    }
}

struct StartupSnapshot {
    target: TelemetryTarget,
    root_name: &'static str,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    elapsed: Duration,
    stages: Vec<StageRecord>,
    post_update: Option<DesktopPostUpdateContext>,
    record_post_update_failure: bool,
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
    let startup_type = if snapshot.post_update.is_some() {
        "post_update"
    } else {
        "cold"
    };
    let metric_root_attributes = [
        KeyValue::new("outcome", snapshot.outcome),
        KeyValue::new("startup.failed_stage", failed_stage),
        KeyValue::new("startup.type", startup_type),
    ];
    state.startup_metrics.duration.record(
        snapshot.elapsed.as_secs_f64() * 1_000.0,
        &metric_root_attributes,
    );
    if snapshot.failed {
        state
            .startup_metrics
            .failures
            .add(1, &metric_root_attributes);
    }
    if snapshot.record_post_update_failure
        && let (Some(metrics), Some(context)) =
            (&state.desktop_update_metrics, &snapshot.post_update)
    {
        metrics.failures.add(
            1,
            &[
                KeyValue::new("stage", failed_stage),
                KeyValue::new("outcome", "error"),
                KeyValue::new("os", context.platform.clone()),
            ],
        );
    }
    for stage in &snapshot.stages {
        let mut metric_attributes = vec![
            KeyValue::new("startup.stage", stage.name),
            KeyValue::new("outcome", stage.outcome.as_str()),
            KeyValue::new("startup.type", startup_type),
        ];
        if let Some(failure_class) = stage.diagnostics.failure_class {
            metric_attributes.push(KeyValue::new("failure.class", failure_class));
        }
        state
            .startup_metrics
            .stage_duration
            .record(stage.elapsed.as_secs_f64() * 1_000.0, &metric_attributes);
    }

    let mut span_root_attributes = metric_root_attributes.to_vec();
    if let Some(context) = &snapshot.post_update {
        span_root_attributes.extend(post_update_trace_attributes(context));
    }
    let root_builder = SpanBuilder::from_name(snapshot.root_name)
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(span_root_attributes);
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

struct PostUpdateHandoffSnapshot {
    consent_generation: Option<u64>,
    context: DesktopPostUpdateContext,
}

struct PostUpdateStallSnapshot {
    consent_generation: Option<u64>,
    context: DesktopPostUpdateContext,
    started_at: SystemTime,
    elapsed: Duration,
    last_completed_stage: &'static str,
}

fn emit_post_update_handoff(snapshot: &PostUpdateHandoffSnapshot) {
    if !super::telemetry_sample_allowed(snapshot.consent_generation) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    if state.target != TelemetryTarget::Desktop {
        return;
    }
    let Some(metrics) = &state.desktop_update_metrics else {
        return;
    };

    let metric_attributes = [
        KeyValue::new("outcome", "ok"),
        KeyValue::new("os", snapshot.context.platform.clone()),
    ];
    metrics.apply_duration.record(
        snapshot.context.apply_duration.as_secs_f64() * 1_000.0,
        &metric_attributes,
    );
    metrics.relaunch_duration.record(
        snapshot.context.relaunch_duration.as_secs_f64() * 1_000.0,
        &metric_attributes,
    );

    let ended_at = snapshot.context.claimed_at;
    let started_at = ended_at
        .checked_sub(snapshot.context.total_duration)
        .unwrap_or(ended_at);
    let root_builder = SpanBuilder::from_name("desktop.update.handoff")
        .with_kind(SpanKind::Internal)
        .with_start_time(started_at)
        .with_attributes(post_update_trace_attributes(&snapshot.context));
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);

    emit_post_update_handoff_stage(
        state,
        &root_context,
        "desktop.update.process_exit.wait",
        started_at,
        snapshot.context.process_exit_wait,
    );
    let apply_started_at = started_at
        .checked_add(snapshot.context.process_exit_wait)
        .unwrap_or(started_at);
    emit_post_update_handoff_stage(
        state,
        &root_context,
        "desktop.update.apply",
        apply_started_at,
        snapshot.context.apply_duration,
    );
    let relaunch_started_at = ended_at
        .checked_sub(snapshot.context.relaunch_duration)
        .unwrap_or(ended_at);
    emit_post_update_handoff_stage(
        state,
        &root_context,
        "desktop.update.relaunch",
        relaunch_started_at,
        snapshot.context.relaunch_duration,
    );

    root_context.span().set_status(Status::Ok);
    root_context.span().end_with_timestamp(ended_at);
}

fn emit_post_update_handoff_stage(
    state: &super::telemetry::ObservabilityState,
    root_context: &Context,
    name: &'static str,
    started_at: SystemTime,
    elapsed: Duration,
) {
    let builder = SpanBuilder::from_name(name)
        .with_kind(SpanKind::Internal)
        .with_start_time(started_at)
        .with_attributes([KeyValue::new("outcome", "ok")]);
    let mut span = state.tracer.build_with_context(builder, root_context);
    span.set_status(Status::Ok);
    span.end_with_timestamp(end_timestamp(started_at, elapsed));
}

fn emit_post_update_stall(snapshot: &PostUpdateStallSnapshot) {
    if !super::telemetry_sample_allowed(snapshot.consent_generation) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    if state.target != TelemetryTarget::Desktop {
        return;
    }
    let Some(metrics) = &state.desktop_update_metrics else {
        return;
    };

    metrics.failures.add(
        1,
        &[
            KeyValue::new("stage", snapshot.last_completed_stage),
            KeyValue::new("outcome", "stalled"),
            KeyValue::new("os", snapshot.context.platform.clone()),
        ],
    );
    let mut attributes = post_update_trace_attributes(&snapshot.context);
    attributes.extend([
        KeyValue::new("startup.type", "post_update"),
        KeyValue::new(
            "startup.last_completed_stage",
            snapshot.last_completed_stage,
        ),
        KeyValue::new(
            "startup.elapsed_ms",
            i64::try_from(snapshot.elapsed.as_millis()).unwrap_or(i64::MAX),
        ),
    ]);
    let builder = SpanBuilder::from_name("desktop.update.stalled")
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(attributes);
    let mut span = state.tracer.build(builder);
    span.set_status(Status::Error {
        description: std::borrow::Cow::Borrowed(
            "post-update startup exceeded the diagnostic threshold",
        ),
    });
    span.end_with_timestamp(end_timestamp(snapshot.started_at, snapshot.elapsed));
}

fn post_update_trace_attributes(context: &DesktopPostUpdateContext) -> Vec<KeyValue> {
    vec![
        KeyValue::new("update.attempt_id", context.attempt_id.clone()),
        KeyValue::new("update.from_version", context.from_version.clone()),
        KeyValue::new("update.to_version", context.to_version.clone()),
        KeyValue::new("update.platform", context.platform.clone()),
    ]
}

fn end_timestamp(started_at: SystemTime, elapsed: Duration) -> SystemTime {
    started_at
        .checked_add(elapsed)
        .unwrap_or_else(SystemTime::now)
}

#[cfg(test)]
mod tests {
    use super::{
        DesktopGatewayConnectFailureClass, DesktopPostUpdateContext, DesktopPostUpdateStage,
        DesktopStartupOutcome, DesktopStartupStage, DesktopStartupTrace, GatewayStartupStage,
        GatewayStartupTrace, MobileStartupOutcome, MobileStartupStage, StageOutcome,
    };
    use std::time::{Duration, SystemTime};

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
        assert_eq!(
            GatewayStartupStage::DatabaseReaderOpen.as_str(),
            "database.reader.open"
        );
        assert_eq!(
            GatewayStartupStage::DatabaseReaderConfigure.as_str(),
            "database.reader.configure"
        );
        assert_eq!(
            GatewayStartupStage::DatabaseReaderValidate.as_str(),
            "database.reader.validate"
        );
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

    #[test]
    fn ordinary_startup_cannot_create_post_update_stages() {
        let trace = DesktopStartupTrace::start();

        trace.emit_post_update_handoff();
        trace.schedule_post_update_stall_checkpoint(Duration::ZERO);

        assert!(!trace.is_post_update());
        assert!(
            trace
                .post_update_stage(DesktopPostUpdateStage::GatewayVersionCheck)
                .is_none()
        );
        let state = trace.timeline.lock();
        assert!(state.stages.is_empty());
        assert!(!state.post_update_handoff_emitted);
        assert!(!state.post_update_stall_scheduled);
    }

    #[test]
    fn post_update_handoff_waits_for_bound_consent() {
        let trace = DesktopStartupTrace::start();
        trace.set_post_update_context(post_update_context());

        trace.emit_post_update_handoff();
        trace.schedule_post_update_stall_checkpoint(Duration::ZERO);

        let state = trace.timeline.lock();
        assert!(!state.post_update_handoff_emitted);
        assert!(!state.post_update_stall_scheduled);
    }

    #[test]
    fn confirmed_post_update_stage_is_recorded_once() {
        let trace = DesktopStartupTrace::start();
        trace.set_post_update_context(post_update_context());

        trace
            .post_update_stage(DesktopPostUpdateStage::GatewayVersionCheck)
            .unwrap()
            .succeed();

        assert!(trace.is_post_update());
        assert!(
            trace
                .post_update_stage(DesktopPostUpdateStage::GatewayVersionCheck)
                .is_none()
        );
        let state = trace.timeline.lock();
        assert_eq!(state.stages.len(), 1);
        assert_eq!(state.stages[0].name, "desktop_update.gateway.version_check");
        assert_eq!(state.stages[0].outcome, StageOutcome::Ok);
    }

    fn post_update_context() -> DesktopPostUpdateContext {
        DesktopPostUpdateContext {
            attempt_id: "A1b2C3d4E5f6G7h8I9j0K".to_owned(),
            from_version: "0.25.0".to_owned(),
            to_version: "0.26.0".to_owned(),
            platform: "macos".to_owned(),
            process_exit_wait: Duration::from_millis(50),
            apply_duration: Duration::from_millis(500),
            relaunch_duration: Duration::from_millis(100),
            total_duration: Duration::from_millis(650),
            claimed_at: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000),
        }
    }
}
