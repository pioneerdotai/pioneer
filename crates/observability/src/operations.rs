use crate::telemetry::TelemetryTarget;
use opentelemetry::trace::{
    Span as _, SpanBuilder, SpanKind, Status, TraceContextExt, Tracer as _,
};
use opentelemetry::{Context, KeyValue};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayCliRuntimeKind {
    Codex,
    Claude,
}

impl GatewayCliRuntimeKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayCliRuntimeRefreshStage {
    WorkspaceValidate,
    CatalogLoad,
    InstancesSelect,
    RuntimeProxyLoad,
    RuntimeAccountProbe,
    RuntimeMcpReadiness,
    DisclosureApply,
    ResponseSend,
}

impl GatewayCliRuntimeRefreshStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceValidate => "workspace.validate",
            Self::CatalogLoad => "catalog.load",
            Self::InstancesSelect => "instances.select",
            Self::RuntimeProxyLoad => "runtime.proxy.load",
            Self::RuntimeAccountProbe => "runtime.account.probe",
            Self::RuntimeMcpReadiness => "runtime.mcp.readiness",
            Self::DisclosureApply => "disclosure.apply",
            Self::ResponseSend => "response.send",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperationStageOutcome {
    Ok,
    Error,
}

impl OperationStageOutcome {
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
                description: std::borrow::Cow::Borrowed("operation stage failed"),
            },
        }
    }
}

#[derive(Clone, Debug)]
struct OperationStageRecord {
    name: &'static str,
    runtime_kind: Option<GatewayCliRuntimeKind>,
    started_at: SystemTime,
    elapsed: Duration,
    outcome: OperationStageOutcome,
}

#[derive(Debug)]
struct GatewayCliRuntimeRefreshState {
    started_at: SystemTime,
    started_instant: Instant,
    stages: Vec<OperationStageRecord>,
    finalized: bool,
}

#[derive(Clone, Debug)]
struct GatewayCliRuntimeRefreshTimeline {
    inner: Arc<Mutex<GatewayCliRuntimeRefreshState>>,
}

impl GatewayCliRuntimeRefreshTimeline {
    fn start() -> Self {
        Self {
            inner: Arc::new(Mutex::new(GatewayCliRuntimeRefreshState {
                started_at: SystemTime::now(),
                started_instant: Instant::now(),
                stages: Vec::new(),
                finalized: false,
            })),
        }
    }

    fn stage(
        &self,
        stage: GatewayCliRuntimeRefreshStage,
        runtime_kind: Option<GatewayCliRuntimeKind>,
    ) -> GatewayCliRuntimeRefreshStageGuard {
        GatewayCliRuntimeRefreshStageGuard {
            timeline: self.clone(),
            name: stage.as_str(),
            runtime_kind,
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
            finished: false,
        }
    }

    fn record_stage(
        &self,
        name: &'static str,
        runtime_kind: Option<GatewayCliRuntimeKind>,
        started_at: SystemTime,
        elapsed: Duration,
        outcome: OperationStageOutcome,
    ) {
        let mut state = self.lock();
        if !state.finalized {
            state.stages.push(OperationStageRecord {
                name,
                runtime_kind,
                started_at,
                elapsed,
                outcome,
            });
        }
    }

    fn finish(&self, outcome: &'static str, failed: bool) {
        let snapshot = {
            let mut state = self.lock();
            if state.finalized {
                return;
            }
            state.finalized = true;
            GatewayCliRuntimeRefreshSnapshot {
                started_at: state.started_at,
                elapsed: state.started_instant.elapsed(),
                stages: state.stages.clone(),
                outcome,
                failed,
            }
        };
        emit_gateway_cli_runtime_refresh(&snapshot);
    }

    fn lock(&self) -> MutexGuard<'_, GatewayCliRuntimeRefreshState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Records one complete `cli_runtime.refresh` request inside the Gateway.
///
/// The trace intentionally contains only bounded attributes. Runtime and
/// workspace identifiers are never exported; `runtime.kind` is limited to the
/// stable `codex` and `claude` values supplied by the caller.
#[must_use = "the CLI runtime refresh trace must be completed"]
pub struct GatewayCliRuntimeRefreshTrace {
    timeline: GatewayCliRuntimeRefreshTimeline,
    finished: bool,
}

impl GatewayCliRuntimeRefreshTrace {
    pub fn start() -> Self {
        Self {
            timeline: GatewayCliRuntimeRefreshTimeline::start(),
            finished: false,
        }
    }

    #[must_use]
    pub fn stage(
        &self,
        stage: GatewayCliRuntimeRefreshStage,
    ) -> GatewayCliRuntimeRefreshStageGuard {
        self.timeline.stage(stage, None)
    }

    #[must_use]
    pub fn runtime_stage(
        &self,
        stage: GatewayCliRuntimeRefreshStage,
        runtime_kind: GatewayCliRuntimeKind,
    ) -> GatewayCliRuntimeRefreshStageGuard {
        self.timeline.stage(stage, Some(runtime_kind))
    }

    pub fn finish_success(mut self) {
        self.timeline.finish("ok", false);
        self.finished = true;
    }
}

impl Drop for GatewayCliRuntimeRefreshTrace {
    fn drop(&mut self) {
        if !self.finished {
            self.timeline.finish("error", true);
            self.finished = true;
        }
    }
}

#[must_use = "an operation stage must be marked successful; dropping it records a failure"]
pub struct GatewayCliRuntimeRefreshStageGuard {
    timeline: GatewayCliRuntimeRefreshTimeline,
    name: &'static str,
    runtime_kind: Option<GatewayCliRuntimeKind>,
    started_at: SystemTime,
    started_instant: Instant,
    finished: bool,
}

impl GatewayCliRuntimeRefreshStageGuard {
    pub fn succeed(mut self) {
        self.finish(OperationStageOutcome::Ok);
    }

    fn finish(&mut self, outcome: OperationStageOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.timeline.record_stage(
            self.name,
            self.runtime_kind,
            self.started_at,
            self.started_instant.elapsed(),
            outcome,
        );
    }
}

impl Drop for GatewayCliRuntimeRefreshStageGuard {
    fn drop(&mut self) {
        self.finish(OperationStageOutcome::Error);
    }
}

struct GatewayCliRuntimeRefreshSnapshot {
    started_at: SystemTime,
    elapsed: Duration,
    stages: Vec<OperationStageRecord>,
    outcome: &'static str,
    failed: bool,
}

impl GatewayCliRuntimeRefreshSnapshot {
    fn failed_stage(&self) -> &'static str {
        self.stages
            .iter()
            .rev()
            .find(|stage| stage.outcome == OperationStageOutcome::Error)
            .map(|stage| stage.name)
            .unwrap_or("unknown")
    }
}

fn emit_gateway_cli_runtime_refresh(snapshot: &GatewayCliRuntimeRefreshSnapshot) {
    if !super::telemetry_enabled() {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    if state.target != TelemetryTarget::Gateway {
        return;
    }
    let Some(metrics) = state.gateway_metrics.as_ref() else {
        return;
    };

    let failed_stage = if snapshot.failed {
        snapshot.failed_stage()
    } else {
        "none"
    };
    let root_attributes = vec![
        KeyValue::new("operation.name", "cli_runtime.refresh"),
        KeyValue::new("outcome", snapshot.outcome),
        KeyValue::new("operation.failed_stage", failed_stage),
    ];
    metrics.cli_runtime_refresh_duration.record(
        snapshot.elapsed.as_secs_f64() * 1_000.0,
        root_attributes.as_slice(),
    );
    if snapshot.failed {
        metrics
            .cli_runtime_refresh_failures
            .add(1, root_attributes.as_slice());
    }

    for stage in &snapshot.stages {
        let attributes = stage_attributes(stage);
        metrics
            .cli_runtime_refresh_stage_duration
            .record(stage.elapsed.as_secs_f64() * 1_000.0, attributes.as_slice());
    }

    let root_builder = SpanBuilder::from_name("gateway.cli_runtime.refresh")
        .with_kind(SpanKind::Server)
        .with_start_time(snapshot.started_at)
        .with_attributes(root_attributes);
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);

    for stage in &snapshot.stages {
        let builder = SpanBuilder::from_name(stage.name)
            .with_kind(SpanKind::Internal)
            .with_start_time(stage.started_at)
            .with_attributes(stage_attributes(stage));
        let mut span = state.tracer.build_with_context(builder, &root_context);
        span.set_status(stage.outcome.status());
        span.end_with_timestamp(end_timestamp(stage.started_at, stage.elapsed));
    }

    root_context.span().set_status(if snapshot.failed {
        Status::Error {
            description: std::borrow::Cow::Borrowed("CLI runtime refresh failed"),
        }
    } else {
        Status::Ok
    });
    root_context
        .span()
        .end_with_timestamp(end_timestamp(snapshot.started_at, snapshot.elapsed));
}

fn stage_attributes(stage: &OperationStageRecord) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("operation.name", "cli_runtime.refresh"),
        KeyValue::new("operation.stage", stage.name),
        KeyValue::new("outcome", stage.outcome.as_str()),
    ];
    if let Some(runtime_kind) = stage.runtime_kind {
        attributes.push(KeyValue::new("runtime.kind", runtime_kind.as_str()));
    }
    attributes
}

fn end_timestamp(started_at: SystemTime, elapsed: Duration) -> SystemTime {
    started_at
        .checked_add(elapsed)
        .unwrap_or_else(SystemTime::now)
}

#[cfg(test)]
mod tests {
    use super::{
        GatewayCliRuntimeKind, GatewayCliRuntimeRefreshStage, GatewayCliRuntimeRefreshTrace,
        OperationStageOutcome,
    };

    #[test]
    fn stages_use_stable_names_and_bounded_runtime_kinds() {
        let trace = GatewayCliRuntimeRefreshTrace::start();
        trace
            .stage(GatewayCliRuntimeRefreshStage::WorkspaceValidate)
            .succeed();
        trace
            .runtime_stage(
                GatewayCliRuntimeRefreshStage::RuntimeAccountProbe,
                GatewayCliRuntimeKind::Codex,
            )
            .succeed();

        let state = trace.timeline.lock();
        assert_eq!(state.stages[0].name, "workspace.validate");
        assert_eq!(state.stages[0].runtime_kind, None);
        assert_eq!(state.stages[1].name, "runtime.account.probe");
        assert_eq!(
            state.stages[1].runtime_kind,
            Some(GatewayCliRuntimeKind::Codex)
        );
    }

    #[test]
    fn dropped_stage_and_trace_are_fail_closed() {
        let trace = GatewayCliRuntimeRefreshTrace::start();
        drop(trace.stage(GatewayCliRuntimeRefreshStage::CatalogLoad));
        let timeline = trace.timeline.clone();
        drop(trace);

        let state = timeline.lock();
        assert!(state.finalized);
        assert_eq!(state.stages[0].outcome, OperationStageOutcome::Error);
    }
}
