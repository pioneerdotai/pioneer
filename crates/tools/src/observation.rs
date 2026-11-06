//! Invocation ownership follows the existing cloneable call envelope. Only the
//! runtime takes its terminal owner; abandoning the last envelope censors it.
use crate::{context::AnyToolResult, error::ToolError};
use pioneer_observability::{
    scenario::Correlation,
    tool_call::{Fact, Measurement, Phase},
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};

#[derive(Clone, Default)]
pub struct InvocationObservation {
    owner: Option<Arc<Mutex<Option<Measurement>>>>,
    correlation: Correlation,
}
impl std::fmt::Debug for InvocationObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InvocationObservation")
    }
}
impl InvocationObservation {
    pub fn start(family: pioneer_observability::tool_call::Family) -> Self {
        let mut measurement = Measurement::start(Phase::Invocation, &Correlation::capture());
        measurement.family(family);
        let correlation = measurement.correlation();
        let owner = if pioneer_observability::telemetry_enabled() && correlation.is_current() {
            Some(Arc::new(Mutex::new(Some(measurement))))
        } else {
            None
        };
        Self { owner, correlation }
    }
    pub fn correlation(&self) -> Correlation {
        self.correlation.clone()
    }
    pub(crate) fn take(&self) -> Option<Measurement> {
        self.owner
            .as_ref()
            .and_then(|owner| owner.lock().ok().and_then(|mut owner| owner.take()))
    }
    pub fn finish(&self, fact: Fact) {
        if let Some(mut measurement) = self.take() {
            measurement.finish(fact);
        }
    }
}
pub(crate) fn error_fact(error: &ToolError) -> Fact {
    match error {
        ToolError::InvalidArguments(_) => Fact::Invalid,
        ToolError::NotFound(_) | ToolError::NotVisible(_) => Fact::Unavailable,
        ToolError::Rejected(_) => Fact::Rejected,
        ToolError::Cancelled(_) => Fact::Cancelled,
        ToolError::ExecutionFailed(_) | ToolError::Internal(_) => Fact::Failed,
    }
}
pub(crate) fn result_fact(result: &AnyToolResult) -> Fact {
    if result.outcome.error_class == Some(crate::context::ToolErrorClass::Unknown) {
        return Fact::Unknown;
    }
    if result.outcome.incomplete {
        return Fact::Partial;
    }
    match result.outcome.status {
        crate::context::ToolOutcomeStatus::PartialSuccess => Fact::Partial,
        crate::context::ToolOutcomeStatus::Ok if result.success() => Fact::BusinessSuccess,
        _ => Fact::BusinessFailure,
    }
}
pub(crate) async fn measure<T>(
    phase: Phase,
    future: impl Future<Output = Result<T, ToolError>>,
) -> Result<T, ToolError> {
    let mut measurement = Measurement::start(phase, &Correlation::capture());
    let correlation = measurement.correlation();
    let carrier = if correlation.is_current() {
        correlation.run(|| {
            pioneer_observability::work::capture().execution_child(
                pioneer_observability::work::WorkKind::Child,
                pioneer_observability::work::ExecutorRole::Task,
            )
        })
    } else {
        Default::default()
    };
    let result = correlation
        .scope(carrier.scope_result(async {
            let _wait = match phase {
                Phase::McpRequest => Some(pioneer_observability::live::WaitGuard::new(
                    pioneer_observability::live::WaitKind::Network,
                )),
                Phase::ShellProcessWait | Phase::ShellContinuation => {
                    Some(pioneer_observability::live::WaitGuard::new(
                        pioneer_observability::live::WaitKind::Child,
                    ))
                }
                _ => None,
            };
            future.await
        }))
        .await;
    measurement.finish(match &result {
        Ok(_) => Fact::Returned,
        Err(error) => error_fact(error),
    });
    result
}

pub(crate) fn measure_sync<T>(
    phase: Phase,
    work: impl FnOnce() -> Result<T, ToolError>,
) -> Result<T, ToolError> {
    let mut observation = Measurement::start(phase, &Correlation::capture());
    let result = observation.correlation().run(work);
    observation.finish(match &result {
        Ok(_) => Fact::Returned,
        Err(error) => error_fact(error),
    });
    result
}

pub(crate) fn output_completeness(
    observation: &mut Measurement,
    projection: &crate::output_policy::ToolResultEnvelope,
) {
    use crate::output_policy::{ToolDisplayPayload, ToolResultView, ToolStoragePayload};
    let llm = match &projection.llm_view {
        ToolResultView::Text { truncated, .. } | ToolResultView::Json { truncated, .. } => {
            Some(*truncated)
        }
        ToolResultView::Empty => None,
    };
    let display = match &projection.display {
        ToolDisplayPayload::Shell { truncated, .. } => Some(*truncated),
        ToolDisplayPayload::Summary(summary) => Some(summary.truncated),
        ToolDisplayPayload::Progress { .. } | ToolDisplayPayload::Hidden => None,
    };
    let storage = match &projection.storage {
        ToolStoragePayload::Shell { truncated, .. } => Some(*truncated),
        ToolStoragePayload::Summary(summary) => Some(summary.truncated),
        ToolStoragePayload::Metadata { .. } | ToolStoragePayload::None => None,
    };
    observation.output_completeness(llm, display, storage);
}
