//! Closed opening facts. Local phases share the existing scenario/work/live
//! owners; none of these confirmations means content presentation or receipt.
use crate::{
    live,
    scenario::{Boundary, Correlation, Observation, Outcome, Scenario},
    work::{ExecutorRole, Trigger, WorkCarrier, WorkKind},
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use std::{sync::OnceLock, time::Instant};

static STARTS: OnceLock<Counter<u64>> = OnceLock::new();
static ENDS: OnceLock<Counter<u64>> = OnceLock::new();
static FACTS: OnceLock<Counter<u64>> = OnceLock::new();
static ELAPSED: OnceLock<Histogram<f64>> = OnceLock::new();
pub(crate) fn init(meter: &Meter) {
    let _ = STARTS.set(meter.u64_counter("pioneer.opening.starts").with_description("Enabled opening phase ingress; Intent is the logical caller denominator, including early refusal; not sampled trace counts").build());
    let _ = ENDS.set(meter.u64_counter("pioneer.opening.outcomes").build());
    let _ = FACTS.set(meter.u64_counter("pioneer.opening.facts").build());
    let _ = ELAPSED.set(meter.f64_histogram("pioneer.opening.elapsed").with_unit("ms").with_description("Process-local elapsed for a named phase or milestone; never sum nested/parallel phases; presentation coverage is unknown").build());
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Phase {
    Intent,
    SubscriptionFlight,
    SubscriptionWait,
    SubscriptionPrerequisite,
    Prepare,
    Apply,
    Hydration,
    InitialHistory,
    NewestHistoryFlight,
    HistoryDependency,
    Core,
    Native,
    ReplayReservation,
    SnapshotFence,
    ReplyAdmission,
    Acknowledge,
    ReplayTail,
    TimelinePage,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Result {
    Confirmed,
    Rejected,
    Noop,
    Superseded,
    Cancelled,
    Deadline,
    TransportLost,
    AuthorizationDenied,
    Failed,
    Unconfirmed,
    CoreObserved,
    NativeObserved,
    Returned,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fact {
    IdOnly,
    CachedHint,
    Navigation,
    Workspace,
    Task,
    NewFlight,
    Join,
    ReadyReceipt,
    Retry,
    ColdStaged,
    ColdDropped,
    ColdOverflow,
    ColdRetired,
    SubscriptionAcknowledged,
    HistoryEmpty,
    HistoryNonempty,
    CoreReady,
    SetupRequired,
    ReadOnly,
    AuthRequired,
    Busy,
    VoiceDisabled,
    Degraded,
    NativeApplied,
    PresentationUnknown,
    HistoryJoined,
    HistoryRejected,
    CoverageRetired,
}
macro_rules! labels { ($ty:ident { $($variant:ident),* $(,)? }) => { impl $ty { pub fn label(self) -> &'static str { match self { $(Self::$variant => stringify!($variant)),* } } } }; }
labels!(Phase {
    Intent,
    SubscriptionFlight,
    SubscriptionWait,
    SubscriptionPrerequisite,
    Prepare,
    Apply,
    Hydration,
    InitialHistory,
    NewestHistoryFlight,
    HistoryDependency,
    Core,
    Native,
    ReplayReservation,
    SnapshotFence,
    ReplyAdmission,
    Acknowledge,
    ReplayTail,
    TimelinePage
});
labels!(Result {
    Confirmed,
    Rejected,
    Noop,
    Superseded,
    Cancelled,
    Deadline,
    TransportLost,
    AuthorizationDenied,
    Failed,
    Unconfirmed,
    CoreObserved,
    NativeObserved,
    Returned
});
labels!(Fact {
    IdOnly,
    CachedHint,
    Navigation,
    Workspace,
    Task,
    NewFlight,
    Join,
    ReadyReceipt,
    Retry,
    ColdStaged,
    ColdDropped,
    ColdOverflow,
    ColdRetired,
    SubscriptionAcknowledged,
    HistoryEmpty,
    HistoryNonempty,
    CoreReady,
    SetupRequired,
    ReadOnly,
    AuthRequired,
    Busy,
    VoiceDisabled,
    Degraded,
    NativeApplied,
    PresentationUnknown,
    HistoryJoined,
    HistoryRejected,
    CoverageRetired
});

/// Captured producer provenance, distinct from optional links on shared work.
#[derive(Clone, Copy, Debug)]
pub struct Consent {
    epoch: Option<u64>,
    origin: u64,
}
impl Consent {
    pub fn capture(parent: &Correlation) -> Self {
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        Self {
            origin: parent.origin_epoch().unwrap_or(epoch),
            epoch: (enabled && parent.origin_epoch().is_none_or(|origin| origin == epoch))
                .then_some(epoch),
        }
    }
    pub fn current(self) -> bool {
        crate::telemetry_sample_allowed(self.epoch)
    }
    pub fn fact(self, fact: Fact) {
        if self.current() {
            if let Some(metric) = FACTS.get() {
                metric.add(1, &[KeyValue::new("fact", fact.label())]);
            }
        }
    }
}

pub struct Measure {
    observation: Option<Observation>,
    correlation: Correlation,
    consent: Consent,
    started: Instant,
    phase: Phase,
    work: WorkCarrier,
    live: live::WorkGuard,
    wait: Option<live::WaitGuard>,
    finished: bool,
}
impl Measure {
    pub fn start(phase: Phase, parent: &Correlation, links: &[Correlation]) -> Self {
        Self::start_with_consent(phase, parent, links, Consent::capture(parent))
    }
    /// Delayed shared work carries its own producer consent, independently of
    /// waiter links. A fresh independent batch with stale links remains valid.
    pub fn start_with_consent(
        phase: Phase,
        parent: &Correlation,
        links: &[Correlation],
        consent: Consent,
    ) -> Self {
        let independent_origin = Correlation::empty_in_epoch(consent.origin);
        let parent = if parent.origin_epoch().is_none() {
            &independent_origin
        } else {
            parent
        };
        let observation = consent.current().then(|| {
            Observation::start(
                Scenario::Opening,
                if phase == Phase::Intent {
                    Boundary::Episode
                } else {
                    Boundary::Attempt
                },
                parent,
                links,
            )
        });
        if let Some(observation) = &observation {
            observation.annotate("opening.phase", phase.label());
        }
        let correlation = observation
            .as_ref()
            .map(Observation::correlation)
            .unwrap_or_else(|| Correlation::empty_in_epoch(consent.origin));
        let work = if consent.current() {
            correlation.run(|| {
                WorkCarrier::child(
                    crate::work::Boundary::ClientCommand,
                    WorkKind::ClientCommand,
                    Trigger::Event,
                    if matches!(
                        phase,
                        Phase::ReplayReservation
                            | Phase::SnapshotFence
                            | Phase::ReplyAdmission
                            | Phase::Acknowledge
                            | Phase::ReplayTail
                            | Phase::TimelinePage
                    ) {
                        ExecutorRole::Gateway
                    } else {
                        ExecutorRole::ClientController
                    },
                )
            })
        } else {
            WorkCarrier::default()
        };
        let live = live::WorkGuard::start(work.context(), false);
        crate::live::opening_phase(work.context(), phase);
        let mut result = Self {
            observation,
            correlation,
            consent,
            started: Instant::now(),
            phase,
            work,
            live,
            wait: None,
            finished: false,
        };
        if consent.current() {
            if let Some(metric) = STARTS.get() {
                metric.add(1, &[KeyValue::new("phase", phase.label())]);
            }
        }
        result.wait_for(live::WaitKind::Unknown);
        result
    }
    pub fn correlation(&self) -> Correlation {
        self.correlation.clone()
    }
    pub fn consent(&self) -> Consent {
        self.consent
    }
    pub fn fact(&self, fact: Fact) {
        if self.consent.current() {
            if let Some(metric) = FACTS.get() {
                metric.add(
                    1,
                    &[
                        KeyValue::new("phase", self.phase.label()),
                        KeyValue::new("fact", fact.label()),
                    ],
                );
            }
        }
    }
    pub fn milestone(&self, fact: Fact) {
        if !self.consent.current() {
            return;
        }
        self.fact(fact);
        if let Some(metric) = ELAPSED.get() {
            metric.record(
                self.started.elapsed().as_secs_f64() * 1000.,
                &[KeyValue::new("milestone", fact.label())],
            );
        }
    }
    pub fn working(&mut self) {
        self.wait.take();
        if !self.finished {
            self.work.run(|| live::progress(live::Stage::Executing));
        }
    }
    pub fn wait_for(&mut self, kind: live::WaitKind) {
        self.wait.take();
        if !self.finished {
            self.wait = Some(self.work.run(|| live::WaitGuard::new(kind)));
        }
    }
    pub fn finish(&mut self, result: Result) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.wait.take();
        let outcome = match result {
            Result::Confirmed => Outcome::Confirmed,
            Result::Rejected | Result::AuthorizationDenied | Result::Failed => Outcome::Failed,
            Result::Superseded => Outcome::Superseded,
            Result::Cancelled => Outcome::CancelledWaiter,
            Result::Deadline => Outcome::DeadlineExceeded,
            Result::TransportLost => Outcome::TransportLost,
            Result::Noop | Result::Returned => Outcome::Returned,
            Result::CoreObserved | Result::NativeObserved | Result::Unconfirmed => {
                Outcome::Unconfirmed
            }
        };
        if let Some(mut observation) = self.observation.take() {
            observation.annotate("opening.result", result.label());
            observation.finish(outcome);
        }
        self.live.finish(match result {
            Result::Confirmed
            | Result::Noop
            | Result::Returned
            | Result::CoreObserved
            | Result::NativeObserved => crate::work::Outcome::Completed,
            Result::Rejected
            | Result::AuthorizationDenied
            | Result::Failed
            | Result::Deadline
            | Result::TransportLost => crate::work::Outcome::Failed,
            _ => crate::work::Outcome::Cancelled,
        });
        if self.consent.current() {
            let labels = [
                KeyValue::new("phase", self.phase.label()),
                KeyValue::new("result", result.label()),
            ];
            if let Some(metric) = ENDS.get() {
                metric.add(1, &labels);
            }
            if let Some(metric) = ELAPSED.get() {
                metric.record(self.started.elapsed().as_secs_f64() * 1000., &labels);
            }
        }
    }
}
impl Drop for Measure {
    fn drop(&mut self) {
        self.finish(Result::Unconfirmed);
    }
}
