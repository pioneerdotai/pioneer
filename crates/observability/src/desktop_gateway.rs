use crate::startup::DesktopStartupStage;
use opentelemetry::trace::{Span, SpanBuilder, SpanKind, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, KeyValue};
use std::time::{Duration, Instant, SystemTime};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DesktopGatewayLifecycleOperation {
    Start,
    Update,
}

impl DesktopGatewayLifecycleOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Update => "update",
        }
    }

    const fn span_name(self) -> &'static str {
        match self {
            Self::Start => "desktop.gateway.start",
            Self::Update => "desktop.gateway.update",
        }
    }
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
                description: std::borrow::Cow::Borrowed("Desktop Gateway lifecycle stage failed"),
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

pub struct DesktopGatewayLifecycleTrace {
    operation: DesktopGatewayLifecycleOperation,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    started_instant: Instant,
    stages: Vec<StageRecord>,
    finished: bool,
}

impl DesktopGatewayLifecycleTrace {
    pub fn start(operation: DesktopGatewayLifecycleOperation) -> Self {
        let (telemetry_enabled, consent_generation) = super::telemetry_consent_snapshot();
        Self {
            operation,
            consent_generation: telemetry_enabled.then_some(consent_generation),
            started_at: SystemTime::now(),
            started_instant: Instant::now(),
            stages: Vec::new(),
            finished: false,
        }
    }

    pub fn record_stage(
        &mut self,
        stage: DesktopStartupStage,
        started_at: SystemTime,
        elapsed: Duration,
        succeeded: bool,
    ) {
        if self.finished {
            return;
        }
        self.stages.push(StageRecord {
            name: stage.as_str(),
            started_at,
            elapsed,
            outcome: if succeeded {
                StageOutcome::Ok
            } else {
                StageOutcome::Error
            },
        });
    }

    pub fn finish_success(mut self) {
        self.finish("ok", false);
    }

    pub fn finish_failure(mut self) {
        self.finish("error", true);
    }

    fn finish(&mut self, outcome: &'static str, failed: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        emit_desktop_gateway_lifecycle(
            self.operation,
            self.consent_generation,
            self.started_at,
            self.started_instant.elapsed(),
            self.stages.as_slice(),
            outcome,
            failed,
        );
    }
}

impl Drop for DesktopGatewayLifecycleTrace {
    fn drop(&mut self) {
        self.finish("cancelled", false);
    }
}

fn emit_desktop_gateway_lifecycle(
    operation: DesktopGatewayLifecycleOperation,
    consent_generation: Option<u64>,
    started_at: SystemTime,
    elapsed: Duration,
    stages: &[StageRecord],
    outcome: &'static str,
    failed: bool,
) {
    if !super::telemetry_sample_allowed(consent_generation) {
        return;
    }
    let Some(state) = super::telemetry::state() else {
        return;
    };
    let Some(metrics) = state.desktop_gateway_lifecycle_metrics.as_ref() else {
        return;
    };

    let failed_stage = if failed {
        stages
            .iter()
            .rev()
            .find(|stage| stage.outcome == StageOutcome::Error)
            .map(|stage| stage.name)
            .unwrap_or("unknown")
    } else {
        "none"
    };
    let root_attributes = [
        KeyValue::new("operation.name", operation.as_str()),
        KeyValue::new("outcome", outcome),
        KeyValue::new("operation.failed_stage", failed_stage),
    ];
    metrics
        .duration
        .record(elapsed.as_secs_f64() * 1_000.0, &root_attributes);
    if failed {
        metrics.failures.add(1, &root_attributes);
    }
    for stage in stages {
        let stage_attributes = [
            KeyValue::new("operation.name", operation.as_str()),
            KeyValue::new("operation.stage", stage.name),
            KeyValue::new("outcome", stage.outcome.as_str()),
        ];
        metrics
            .stage_duration
            .record(stage.elapsed.as_secs_f64() * 1_000.0, &stage_attributes);
    }

    let root_builder = SpanBuilder::from_name(operation.span_name())
        .with_kind(SpanKind::Internal)
        .with_start_time(started_at)
        .with_attributes(root_attributes);
    let root_span = state.tracer.build(root_builder);
    let root_context = Context::new().with_span(root_span);
    for stage in stages {
        let stage_attributes = [
            KeyValue::new("operation.name", operation.as_str()),
            KeyValue::new("operation.stage", stage.name),
            KeyValue::new("outcome", stage.outcome.as_str()),
        ];
        let builder = SpanBuilder::from_name(stage.name)
            .with_kind(SpanKind::Internal)
            .with_start_time(stage.started_at)
            .with_attributes(stage_attributes);
        let mut span = state.tracer.build_with_context(builder, &root_context);
        span.set_status(stage.outcome.status());
        span.end_with_timestamp(end_timestamp(stage.started_at, stage.elapsed));
    }
    root_context.span().set_status(if failed {
        Status::Error {
            description: std::borrow::Cow::Borrowed("Desktop Gateway lifecycle operation failed"),
        }
    } else if outcome == "cancelled" {
        Status::Unset
    } else {
        Status::Ok
    });
    root_context
        .span()
        .end_with_timestamp(end_timestamp(started_at, elapsed));
}

fn end_timestamp(started_at: SystemTime, elapsed: Duration) -> SystemTime {
    started_at
        .checked_add(elapsed)
        .unwrap_or_else(SystemTime::now)
}

#[cfg(test)]
mod tests {
    use super::{DesktopGatewayLifecycleOperation, DesktopGatewayLifecycleTrace};
    use crate::startup::DesktopStartupStage;
    use std::time::{Duration, SystemTime};

    #[test]
    fn lifecycle_names_and_stages_are_bounded() {
        assert_eq!(DesktopGatewayLifecycleOperation::Start.as_str(), "start");
        assert_eq!(
            DesktopGatewayLifecycleOperation::Update.span_name(),
            "desktop.gateway.update"
        );

        let mut trace =
            DesktopGatewayLifecycleTrace::start(DesktopGatewayLifecycleOperation::Start);
        trace.record_stage(
            DesktopStartupStage::GatewayServiceManagerActivate,
            SystemTime::UNIX_EPOCH,
            Duration::from_millis(10),
            true,
        );
        assert_eq!(trace.stages.len(), 1);
        assert_eq!(
            trace.stages[0].name,
            "gateway_runtime.service.manager.activate"
        );
        trace.finished = true;
    }
}
