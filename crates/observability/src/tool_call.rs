//! Closed local tool facts. Durations are inclusive and must not be added.
//! Returned describes an API boundary; it never asserts business success.
use crate::scenario::{Boundary, Correlation, Observation, Outcome, Scenario};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use std::{sync::OnceLock, time::Instant};

static STARTED: OnceLock<Counter<u64>> = OnceLock::new();
static FINISHED: OnceLock<Histogram<f64>> = OnceLock::new();
pub(crate) fn init(meter: &Meter) {
    let _ = UNITS.set(meter.u64_histogram("pioneer.tool.units").build());
    let _ = STARTED.set(meter.u64_counter("pioneer.tool.boundary.started").build());
    let _ = FINISHED.set(
        meter
            .f64_histogram("pioneer.tool.boundary.duration")
            .with_unit("ms")
            .build(),
    );
}
#[derive(Clone, Copy, Debug)]
pub enum Phase {
    Invocation,
    Validation,
    BusinessResult,
    Attempt,
    SharedLock,
    MutationBarrier,
    ExclusiveLock,
    SessionLock,
    Policy,
    PermissionAudit,
    CachedGrant,
    Approval,
    Handler,
    Output,
    LifecycleStartAck,
    LifecycleTerminalAck,
    LifecycleOtherAck,
    BusStart,
    BusTerminal,
    BusOther,
    RetainedResult,
    NativeResultReady,
    BatchBarrier,
    BatchProgress,
    McpRequest,
    ShellPreparation,
    ShellProcessWait,
    ShellContinuation,
    ShellProcessState,
}
#[derive(Clone, Copy, Debug)]
pub enum Fact {
    Returned,
    Acquired,
    Inherited,
    Committed,
    BusOnly,
    Rejected,
    Invalid,
    Unavailable,
    Cancelled,
    Expired,
    Failed,
    Partial,
    Unknown,
    BusinessSuccess,
    BusinessFailure,
    Unconfirmed,
    ProcessRunning,
    ProcessExitedSuccess,
    ProcessExitedFailure,
}
impl Fact {
    fn outcome(self) -> Outcome {
        match self {
            Self::Cancelled => Outcome::CancelledWaiter,
            Self::Expired
            | Self::Failed
            | Self::Rejected
            | Self::Invalid
            | Self::Unavailable
            | Self::BusinessFailure => Outcome::Failed,
            Self::Acquired | Self::Committed | Self::BusinessSuccess => Outcome::Confirmed,
            Self::Unconfirmed | Self::Partial | Self::Unknown => Outcome::Unconfirmed,
            Self::Returned | Self::BusOnly | Self::ProcessRunning | Self::Inherited => {
                Outcome::Returned
            }
            Self::ProcessExitedSuccess | Self::ProcessExitedFailure => Outcome::Returned,
        }
    }
}
#[derive(Clone, Copy)]
pub enum Family {
    Shell,
    Mcp,
    Other,
}
#[derive(Clone, Copy)]
pub enum Unit {
    OutputBytes,
    BatchCalls,
    BatchRemaining,
}
static UNITS: OnceLock<Histogram<u64>> = OnceLock::new();

/// One terminal owner, no registry or retained business payload. Drop censors.
pub struct Measurement {
    observation: Option<Observation>,
    parent: Correlation,
    phase: Phase,
    epoch: Option<u64>,
    started: Instant,
}
impl Measurement {
    pub fn start(phase: Phase, parent: &Correlation) -> Self {
        Self::start_linked(phase, parent, &[])
    }
    pub fn start_linked(phase: Phase, parent: &Correlation, links: &[Correlation]) -> Self {
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        let allowed = enabled && parent.is_current();
        let mut observation = allowed.then(|| {
            Observation::start(
                Scenario::Tool,
                match phase {
                    Phase::Invocation => Boundary::Episode,
                    Phase::Attempt => Boundary::Attempt,
                    _ => Boundary::ExecutionResult,
                },
                parent,
                links,
            )
        });
        if let Some(observation) = &mut observation {
            observation.domain_fact("tool.phase", phase.label());
            observation.domain_fact(
                "tool.coverage",
                match phase {
                    Phase::Output => "raw_text_size_only",
                    Phase::McpRequest => "local_request_remote_timing_unknown",
                    Phase::ShellProcessWait => "coarse_includes_output_drain",
                    Phase::BusStart | Phase::BusTerminal | Phase::BusOther => "bus_only_no_commit",
                    Phase::BatchBarrier | Phase::BatchProgress => "local_native_round",
                    _ => "local_owner_boundary",
                },
            );
        }
        if allowed && crate::telemetry_sample_allowed(Some(epoch)) {
            if let Some(metric) = STARTED.get() {
                metric.add(1, &[KeyValue::new("phase", format!("{phase:?}"))]);
            }
        }
        Self {
            observation,
            parent: if allowed {
                parent.clone()
            } else if parent.is_current() {
                Correlation::inactive(epoch)
            } else {
                parent.clone()
            },
            phase,
            epoch: allowed.then_some(epoch),
            started: Instant::now(),
        }
    }
    pub fn family(&mut self, family: Family) {
        if let Some(observation) = &mut self.observation {
            observation.domain_fact(
                "tool.family",
                match family {
                    Family::Shell => "shell",
                    Family::Mcp => "mcp",
                    Family::Other => "other",
                },
            );
        }
    }
    pub fn output_completeness(
        &mut self,
        llm: Option<bool>,
        display: Option<bool>,
        storage: Option<bool>,
    ) {
        if let Some(observation) = &mut self.observation {
            for (key, value) in [
                ("tool.llm.completeness", llm),
                ("tool.display.completeness", display),
                ("tool.storage.completeness", storage),
            ] {
                observation.domain_fact(
                    key,
                    match value {
                        Some(true) => "truncated",
                        Some(false) => "complete",
                        None => "absent_or_unknown",
                    },
                );
            }
        }
    }
    pub fn units(&self, unit: Unit, count: usize) {
        if crate::telemetry_sample_allowed(self.epoch) {
            if let Some(metric) = UNITS.get() {
                metric.record(
                    count as u64,
                    &[KeyValue::new(
                        "unit",
                        match unit {
                            Unit::OutputBytes => "raw_text_bytes",
                            Unit::BatchCalls => "batch_calls",
                            Unit::BatchRemaining => "batch_remaining",
                        },
                    )],
                );
            }
        }
    }
    pub fn correlation(&self) -> Correlation {
        self.observation
            .as_ref()
            .map_or_else(|| self.parent.clone(), Observation::correlation)
    }
    pub fn finish(&mut self, fact: Fact) {
        let Some(mut observation) = self.observation.take() else {
            return;
        };
        if crate::telemetry_sample_allowed(self.epoch) {
            if let Some(metric) = FINISHED.get() {
                metric.record(
                    self.started.elapsed().as_secs_f64() * 1000.0,
                    &[
                        KeyValue::new("phase", format!("{:?}", self.phase)),
                        KeyValue::new("fact", format!("{fact:?}")),
                    ],
                );
            }
        }
        observation.domain_fact("tool.fact", fact.label());
        let outcome = match (self.phase, fact) {
            (Phase::RetainedResult | Phase::NativeResultReady, Fact::Committed) => {
                Outcome::Confirmed
            }
            (_, Fact::Acquired | Fact::Committed) => Outcome::Returned,
            _ => fact.outcome(),
        };
        observation.finish(outcome);
    }
}
impl Drop for Measurement {
    fn drop(&mut self) {
        self.finish(Fact::Unconfirmed);
    }
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Invocation => "Invocation",
            Self::Validation => "Validation",
            Self::BusinessResult => "BusinessResult",
            Self::Attempt => "Attempt",
            Self::SharedLock => "SharedLock",
            Self::MutationBarrier => "MutationBarrier",
            Self::ExclusiveLock => "ExclusiveLock",
            Self::SessionLock => "SessionLock",
            Self::Policy => "Policy",
            Self::PermissionAudit => "PermissionAudit",
            Self::CachedGrant => "CachedGrant",
            Self::Approval => "Approval",
            Self::Handler => "Handler",
            Self::Output => "Output",
            Self::LifecycleStartAck => "LifecycleStartAck",
            Self::LifecycleTerminalAck => "LifecycleTerminalAck",
            Self::LifecycleOtherAck => "LifecycleOtherAck",
            Self::BusStart => "BusStart",
            Self::BusTerminal => "BusTerminal",
            Self::BusOther => "BusOther",
            Self::RetainedResult => "RetainedResult",
            Self::NativeResultReady => "NativeResultReady",
            Self::BatchBarrier => "BatchBarrier",
            Self::BatchProgress => "BatchProgress",
            Self::McpRequest => "McpRequest",
            Self::ShellPreparation => "ShellPreparation",
            Self::ShellProcessWait => "ShellProcessWait",
            Self::ShellContinuation => "ShellContinuation",
            Self::ShellProcessState => "ShellProcessState",
        }
    }
}

impl Fact {
    fn label(self) -> &'static str {
        match self {
            Self::Returned => "Returned",
            Self::Acquired => "Acquired",
            Self::Inherited => "Inherited",
            Self::Committed => "Committed",
            Self::BusOnly => "BusOnly",
            Self::Rejected => "Rejected",
            Self::Invalid => "Invalid",
            Self::Unavailable => "Unavailable",
            Self::Cancelled => "Cancelled",
            Self::Expired => "Expired",
            Self::Failed => "Failed",
            Self::Partial => "Partial",
            Self::Unknown => "Unknown",
            Self::BusinessSuccess => "BusinessSuccess",
            Self::BusinessFailure => "BusinessFailure",
            Self::Unconfirmed => "Unconfirmed",
            Self::ProcessRunning => "ProcessRunning",
            Self::ProcessExitedSuccess => "ProcessExitedSuccess",
            Self::ProcessExitedFailure => "ProcessExitedFailure",
        }
    }
}
