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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartupOutcome {
    Ok,
    Error,
}

impl StartupOutcome {
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
    stage: GatewayStartupStage,
    started_at: SystemTime,
    elapsed: Duration,
    outcome: StartupOutcome,
}

#[derive(Debug)]
struct StartupState {
    started_at: SystemTime,
    started_instant: Instant,
    stages: Vec<StageRecord>,
    finalized: bool,
}

/// A local, privacy-safe startup timeline that can begin before consent and the
/// OTLP pipeline are available. Only stable stage names and durations are kept.
#[derive(Clone, Debug)]
pub struct GatewayStartupTrace {
    inner: Arc<Mutex<StartupState>>,
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
            inner: Arc::new(Mutex::new(StartupState {
                started_at: SystemTime::now(),
                started_instant: Instant::now(),
                stages: Vec::new(),
                finalized: false,
            })),
        }
    }

    #[must_use]
    pub fn stage(&self, stage: GatewayStartupStage) -> GatewayStartupStageGuard {
        GatewayStartupStageGuard {
            trace: self.clone(),
            stage,
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
            finished: false,
        }
    }

    pub fn finish_success(&self) {
        self.finish(StartupOutcome::Ok);
    }

    pub fn finish_failure(&self) {
        self.finish(StartupOutcome::Error);
    }

    fn finish(&self, outcome: StartupOutcome) {
        let snapshot = {
            let mut state = self.lock();
            if state.finalized {
                return;
            }
            state.finalized = true;
            StartupSnapshot {
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                stages: state.stages.clone(),
                outcome,
            }
        };

        if super::telemetry_enabled() {
            emit_startup_observability(&snapshot);
        }
    }

    fn record_stage(
        &self,
        stage: GatewayStartupStage,
        started_at: SystemTime,
        elapsed: Duration,
        outcome: StartupOutcome,
    ) {
        let mut state = self.lock();
        if !state.finalized {
            state.stages.push(StageRecord {
                stage,
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

#[must_use = "a startup stage must be marked successful; dropping it records a failure"]
pub struct GatewayStartupStageGuard {
    trace: GatewayStartupTrace,
    stage: GatewayStartupStage,
    started_at: SystemTime,
    started_instant: Instant,
    finished: bool,
}

impl GatewayStartupStageGuard {
    pub fn succeed(mut self) {
        self.finish(StartupOutcome::Ok);
    }

    fn finish(&mut self, outcome: StartupOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.trace.record_stage(
            self.stage,
            self.started_at,
            self.started_instant.elapsed(),
            outcome,
        );
    }
}

impl Drop for GatewayStartupStageGuard {
    fn drop(&mut self) {
        self.finish(StartupOutcome::Error);
    }
}

struct StartupSnapshot {
    started_at: SystemTime,
    elapsed: Duration,
    stages: Vec<StageRecord>,
    outcome: StartupOutcome,
}

impl StartupSnapshot {
    fn failed_stage(&self) -> &'static str {
        self.stages
            .iter()
            .rev()
            .find(|record| record.outcome == StartupOutcome::Error)
            .map(|record| record.stage.as_str())
            .unwrap_or("unknown")
    }
}

fn emit_startup_observability(snapshot: &StartupSnapshot) {
    let Some(state) = super::telemetry::state() else {
        return;
    };

    let failed_stage = if snapshot.outcome == StartupOutcome::Error {
        snapshot.failed_stage()
    } else {
        "none"
    };
    let root_attributes = [
        KeyValue::new("outcome", snapshot.outcome.as_str()),
        KeyValue::new("startup.failed_stage", failed_stage),
    ];
    state
        .metrics
        .startup_duration
        .record(snapshot.elapsed.as_secs_f64() * 1_000.0, &root_attributes);
    if snapshot.outcome == StartupOutcome::Error {
        state.metrics.startup_failures.add(1, &root_attributes);
    }
    for stage in &snapshot.stages {
        state.metrics.startup_stage_duration.record(
            stage.elapsed.as_secs_f64() * 1_000.0,
            &[
                KeyValue::new("startup.stage", stage.stage.as_str()),
                KeyValue::new("outcome", stage.outcome.as_str()),
            ],
        );
    }

    let root_builder = SpanBuilder::from_name("gateway.startup")
        .with_kind(SpanKind::Internal)
        .with_start_time(snapshot.started_at)
        .with_attributes(root_attributes);
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);

    for stage in &snapshot.stages {
        let builder = SpanBuilder::from_name(stage.stage.as_str())
            .with_kind(SpanKind::Internal)
            .with_start_time(stage.started_at)
            .with_attributes([
                KeyValue::new("startup.stage", stage.stage.as_str()),
                KeyValue::new("outcome", stage.outcome.as_str()),
            ]);
        let mut span = state.tracer.build_with_context(builder, &root_context);
        span.set_status(stage.outcome.status());
        span.end_with_timestamp(end_timestamp(stage.started_at, stage.elapsed));
    }

    root_context.span().set_status(snapshot.outcome.status());
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
    use super::{GatewayStartupStage, GatewayStartupTrace, StartupOutcome};

    #[test]
    fn successful_and_dropped_stages_have_bounded_outcomes() {
        let trace = GatewayStartupTrace::start();
        trace.stage(GatewayStartupStage::ConfigLoad).succeed();
        drop(trace.stage(GatewayStartupStage::SettingsLoad));

        let state = trace.lock();
        assert_eq!(state.stages.len(), 2);
        assert_eq!(state.stages[0].stage, GatewayStartupStage::ConfigLoad);
        assert_eq!(state.stages[0].outcome, StartupOutcome::Ok);
        assert_eq!(state.stages[1].stage, GatewayStartupStage::SettingsLoad);
        assert_eq!(state.stages[1].outcome, StartupOutcome::Error);
    }

    #[test]
    fn finalization_is_idempotent() {
        let trace = GatewayStartupTrace::start();
        trace.finish_failure();
        trace.finish_success();

        assert!(trace.lock().finalized);
    }

    #[test]
    fn stage_names_are_stable_and_low_cardinality() {
        assert_eq!(GatewayStartupStage::DatabaseOpen.as_str(), "database.open");
        assert_eq!(GatewayStartupStage::ListenerBind.as_str(), "listener.bind");
        assert_eq!(
            GatewayStartupStage::ServicesStart.as_str(),
            "services.start"
        );
    }
}
