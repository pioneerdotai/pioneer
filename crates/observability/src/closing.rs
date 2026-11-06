//! Local closing facts. A phase is an observation, never a durable work owner.
//! Pending review/delivery survive in existing durable state; no tracing registry
//! attempts to reconstruct elapsed time across restart or process boundaries.
use crate::scenario::{Boundary, Correlation, Observation, Outcome, Scenario};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use std::{sync::OnceLock, time::Instant};

static EVENTS: OnceLock<Counter<u64>> = OnceLock::new();
static PENDING_AGE: OnceLock<Histogram<f64>> = OnceLock::new();
static ATTEMPT: OnceLock<Histogram<u64>> = OnceLock::new();
static LOSSES: OnceLock<Counter<u64>> = OnceLock::new();
static ELAPSED: OnceLock<Histogram<f64>> = OnceLock::new();
pub(crate) fn init(meter: &Meter) {
    let _ = LOSSES.set(
        meter
            .u64_counter("pioneer.closing.observation.loss")
            .build(),
    );
    let _ = PENDING_AGE.set(meter.f64_histogram("pioneer.closing.delivery.discovered_age").with_unit("s").with_description("Persisted wall-clock age at existing due discovery, clamped at zero; not process elapsed or a census of all pending deliveries").build());
    let _ = ATTEMPT.set(meter.u64_histogram("pioneer.closing.delivery.previous_attempts").with_description("Previous attempts at existing due discovery; superseded and rejected admission included").build());
    let _ = EVENTS.set(meter.u64_counter("pioneer.closing.facts").with_description("Local phase entries and terminal facts, including censored; entry is the denominator, not sampled spans or logical turns").build());
    let _ = ELAPSED.set(meter.f64_histogram("pioneer.closing.phase.duration").with_unit("ms").with_description("Process-local phase intervals; nested intervals must not be summed; no remote or restart elapsed inferred").build());
}
#[derive(Clone, Copy, Debug)]
pub enum Phase {
    TerminalCandidate,
    PrerequisiteFlush,
    DurableConsumer,
    DurableResidence,
    DurableAck,
    AckWait,
    NativeFinalization,
    NativePreparation,
    CliPreparation,
    CliCandidate,
    MessageCommit,
    ChildResult,
    ResultClosing,
    DestinationClosing,
    ReviewerResult,
    ResultExtraction,
    ReviewDecision,
    ReviewRequest,
    ReviewPreparation,
    RunPreparation,
    DeliveryAuthorization,
    DestinationAttempt,
    DestinationCommit,
    ClientSettlement,
    TaskState,
    DestinationState,
    ReviewWait,
    ResultSource,
    RunCommit,
    PendingDelivery,
    ArtifactCheck,
    TerminalTransition,
    ArtifactWait,
    ClientProjectionSettlement,
    ClientSnapshotSettlement,
}
#[derive(Clone, Copy, Debug)]
pub enum Fact {
    Entered,
    Confirmed,
    Duplicate,
    PendingReview,
    InvalidResult,
    Rejected,
    Pending,
    Superseded,
    Retryable,
    Returned,
    Unconfirmed,
    Accepted,
    ExecutionSucceeded,
    ExecutionFailed,
    Blocked,
    Cancelled,
    Scheduled,
    Retrying,
    Exhausted,
    Delivered,
    DeliveryFailed,
    Continuation,
}
#[derive(Clone, Copy, Debug)]
pub enum Loss {
    OwnerCapacity,
    IdentityCapacity,
    RecoveredFragment,
    LinkCapacity,
}
/// Fixed-sized, single terminal guard. Does not retain IDs, payloads or handles.
pub struct Closing {
    observation: Observation,
    work: crate::work::WorkCarrier,
    wait: Option<crate::live::WaitGuard>,
    live: crate::live::WorkGuard,
    phase: Phase,
    started: Instant,
    epoch: Option<u64>,
    finished: bool,
    coverage: &'static str,
}
impl Closing {
    pub fn start(phase: Phase, parent: &Correlation) -> Self {
        Self::start_linked(phase, parent, &[])
    }
    pub fn start_linked(phase: Phase, parent: &Correlation, links: &[Correlation]) -> Self {
        Self::start_covered(phase, parent, links, "local")
    }
    pub fn start_recovered(phase: Phase, parent: &Correlation) -> Self {
        Self::start_covered(phase, parent, &[], "recovered_fragment")
    }
    fn start_covered(
        phase: Phase,
        parent: &Correlation,
        links: &[Correlation],
        coverage: &'static str,
    ) -> Self {
        let boundary = match phase {
            Phase::DestinationAttempt
            | Phase::DestinationCommit
            | Phase::PendingDelivery
            | Phase::DestinationClosing => Boundary::Delivery,
            Phase::NativeFinalization
            | Phase::ChildResult
            | Phase::RunCommit
            | Phase::ResultClosing
            | Phase::MessageCommit => Boundary::ExecutionResult,
            _ => Boundary::Attempt,
        };
        let observation = Observation::start(Scenario::Closing, boundary, parent, links);
        let correlation = observation.correlation();
        let epoch = correlation.origin_epoch();
        // Aggregate owners use the live phase/wait leases below them rather
        // than registering a second running/CPU-looking lease during review.
        let work = if correlation.is_current()
            && !matches!(phase, Phase::ResultClosing | Phase::DestinationClosing)
        {
            let work = correlation.run(|| {
                crate::work::WorkCarrier::child(
                    crate::work::Boundary::EventLane,
                    crate::work::WorkKind::Event,
                    crate::work::Trigger::Event,
                    crate::work::ExecutorRole::Event,
                )
            });
            if work.context().map(|context| context.consent_generation) == epoch {
                work
            } else {
                crate::work::WorkCarrier::default()
            }
        } else {
            crate::work::WorkCarrier::default()
        };
        let result = Self {
            live: crate::live::WorkGuard::start(work.context(), false),
            work,
            wait: None,
            observation,
            phase,
            started: Instant::now(),
            epoch,
            finished: false,
            coverage,
        };
        if crate::telemetry_sample_allowed(result.epoch) {
            result
                .observation
                .domain_attribute("closing.phase", format!("{phase:?}"));
            result
                .observation
                .domain_attribute("closing.coverage", coverage.to_owned());
        }
        result.record(Fact::Entered);
        result
    }
    /// Event-consumer facts count received committed domain events, including
    /// replay. They are not a unique task count or a duration measurement.
    pub fn committed_event(phase: Phase, fact: Fact) {
        if !Self::capture().is_current() {
            return;
        }
        if let Some(metric) = EVENTS.get() {
            metric.add(
                1,
                &[
                    KeyValue::new("phase", format!("{phase:?}")),
                    KeyValue::new("fact", format!("{fact:?}")),
                ],
            );
        }
    }
    pub fn delivery_schedule(&self, persisted_age_seconds: u64, previous_attempts: u32) {
        if !crate::telemetry_sample_allowed(self.epoch) {
            return;
        }
        if let Some(metric) = PENDING_AGE.get() {
            metric.record(persisted_age_seconds as f64, &[]);
        }
        if let Some(metric) = ATTEMPT.get() {
            metric.record(u64::from(previous_attempts), &[]);
        }
    }
    pub fn loss(loss: Loss) {
        if crate::telemetry_enabled() {
            if let Some(metric) = LOSSES.get() {
                metric.add(1, &[KeyValue::new("reason", format!("{loss:?}"))]);
            }
        }
    }
    pub fn capture() -> Correlation {
        Correlation::capture()
    }
    pub fn current(phase: Phase) -> Self {
        Self::start(phase, &Self::capture())
    }
    pub fn correlation(&self) -> Correlation {
        self.observation.correlation()
    }
    /// An independently admitted current callback may begin a new fragment
    /// after consent rotation. A stale incoming callback must remain stale even
    /// if the same durable identity now has a current diagnostic slot.
    pub fn continue_from(retained: &Correlation) -> Correlation {
        let incoming = Self::capture();
        if !incoming.is_current() {
            return incoming;
        }
        if retained.is_current() {
            retained.clone()
        } else {
            Self::loss(Loss::RecoveredFragment);
            incoming
        }
    }
    pub fn progress(&self) {
        self.work
            .run(|| crate::live::progress(crate::live::Stage::Callback));
    }
    pub fn wait(&mut self, kind: crate::live::WaitKind) {
        if !self.finished && self.wait.is_none() {
            self.wait = Some(self.work.run(|| crate::live::WaitGuard::new(kind)));
        }
    }
    fn record(&self, fact: Fact) {
        if !crate::telemetry_sample_allowed(self.epoch) {
            return;
        }
        let labels = [
            KeyValue::new("phase", format!("{:?}", self.phase)),
            KeyValue::new("fact", format!("{fact:?}")),
            KeyValue::new("coverage", self.coverage),
        ];
        if let Some(metric) = EVENTS.get() {
            metric.add(1, &labels);
        }
        if !matches!(fact, Fact::Entered) {
            if let Some(metric) = ELAPSED.get() {
                metric.record(self.started.elapsed().as_secs_f64() * 1000.0, &labels);
            }
        }
    }
    pub fn finish(&mut self, fact: Fact) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.record(fact);
        self.wait = None;
        self.live.finish(if matches!(fact, Fact::Unconfirmed) {
            crate::work::Outcome::Abandoned
        } else {
            crate::work::Outcome::Completed
        });
        {
            let observation = &mut self.observation;
            if crate::telemetry_sample_allowed(self.epoch) {
                observation.domain_attribute("closing.fact", format!("{fact:?}"));
            }
            observation.finish(match (self.phase, fact) {
                (Phase::ResultClosing, Fact::ExecutionSucceeded)
                | (Phase::DestinationClosing, Fact::Delivered)
                | (_, Fact::Confirmed) => Outcome::Confirmed,
                (Phase::ResultClosing, Fact::ExecutionFailed)
                | (Phase::DestinationClosing, Fact::DeliveryFailed | Fact::Exhausted) => {
                    Outcome::Failed
                }
                (_, Fact::Superseded) => Outcome::Superseded,
                (_, Fact::Unconfirmed) => Outcome::Unconfirmed,
                _ => Outcome::Returned,
            });
        }
    }
}
impl Drop for Closing {
    fn drop(&mut self) {
        self.finish(Fact::Unconfirmed);
    }
}

impl std::fmt::Debug for Closing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Closing")
            .field("phase", &self.phase)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Default)]
    struct Capture(Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);
    impl opentelemetry_sdk::trace::SpanExporter for Capture {
        async fn export(
            &self,
            spans: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            self.0.lock().unwrap().extend(spans);
            Ok(())
        }
    }

    #[test]
    fn closing_exports_owner_fact_once_and_never_revives_stale_descendant() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let capture = Capture::default();
        let traces = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(capture.clone())
            .build();
        let metrics = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let state = crate::telemetry::tests::gateway_test_state_with_tracer(
            metrics.clone(),
            traces.clone(),
        );
        crate::telemetry::tests::with_test_state(state, || {
            let mut result = Closing::current(Phase::NativeFinalization);
            let parent = result.correlation();
            result.finish(Fact::Confirmed);
            result.finish(Fact::Returned);
            drop(result);
            crate::set_telemetry_enabled(false);
            let disabled = Closing::current(Phase::TerminalCandidate);
            crate::set_telemetry_enabled(true);
            let mut late = Closing::start(Phase::DurableConsumer, &disabled.correlation());
            late.finish(Fact::Confirmed);
            drop(disabled);
            let stale = Closing::start(Phase::DurableConsumer, &parent);
            // Propagation must retain the stale provenance even when no span
            // was created, otherwise a second handoff would become a fresh root.
            let mut descendant = Closing::start(Phase::DestinationCommit, &stale.correlation());
            descendant.finish(Fact::Confirmed);
            drop(stale);
            drop(Closing::current(Phase::ClientSettlement));
            let mut committed_result = Closing::current(Phase::ResultClosing);
            committed_result.finish(Fact::ExecutionSucceeded);
            let mut pending_destination = Closing::current(Phase::DestinationClosing);
            pending_destination.finish(Fact::Scheduled);
            drop(Closing::current(Phase::DestinationClosing));
            let mut stale_consumer = Closing::current(Phase::DurableConsumer);
            stale_consumer.finish(Fact::Superseded);
            stale_consumer.finish(Fact::Confirmed);
        });
        traces.force_flush().unwrap();
        {
            let spans = capture.0.lock().unwrap();
            assert_eq!(spans.len(), 6);
            assert!(spans.iter().any(|span| {
                span.attributes
                    .contains(&KeyValue::new("closing.phase", "ResultClosing"))
                    && span
                        .attributes
                        .contains(&KeyValue::new("outcome", "Confirmed"))
            }));
            let destinations: Vec<_> = spans
                .iter()
                .filter(|span| {
                    span.attributes
                        .contains(&KeyValue::new("closing.phase", "DestinationClosing"))
                })
                .collect();
            assert_eq!(destinations.len(), 2);
            assert!(destinations.iter().all(|span| {
                !span
                    .attributes
                    .contains(&KeyValue::new("outcome", "Confirmed"))
            }));
            assert!(spans.iter().any(|span| {
                span.attributes
                    .contains(&KeyValue::new("closing.fact", "Superseded"))
                    && span
                        .attributes
                        .contains(&KeyValue::new("outcome", "Superseded"))
            }));
            assert!(spans.iter().any(|span| {
                span.attributes
                    .contains(&KeyValue::new("closing.phase", "NativeFinalization"))
                    && span
                        .attributes
                        .contains(&KeyValue::new("closing.fact", "Confirmed"))
                    && span
                        .attributes
                        .contains(&KeyValue::new("outcome", "Confirmed"))
            }));
            assert!(spans.iter().any(|span| {
                span.attributes
                    .contains(&KeyValue::new("closing.phase", "ClientSettlement"))
                    && span
                        .attributes
                        .contains(&KeyValue::new("closing.fact", "Unconfirmed"))
                    && span
                        .attributes
                        .contains(&KeyValue::new("outcome", "Unconfirmed"))
            }));
        }
        traces.shutdown().unwrap();
        metrics.shutdown().unwrap();
        crate::set_telemetry_enabled(old);
    }
    // Rotates from the SDK's public construction callback, without a timing
    // race or a production hook between the owner's check and SDK creation.
    #[derive(Debug)]
    struct RotateOnStart(std::sync::atomic::AtomicBool);
    impl opentelemetry_sdk::trace::SpanProcessor for RotateOnStart {
        fn on_start(
            &self,
            _span: &mut opentelemetry_sdk::trace::Span,
            _parent: &opentelemetry::Context,
        ) {
            if self.0.swap(false, std::sync::atomic::Ordering::SeqCst) {
                crate::set_telemetry_enabled(false);
                crate::set_telemetry_enabled(true);
            }
        }
        fn on_end(&self, _span: opentelemetry_sdk::trace::SpanData) {}
        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(
            &self,
            _timeout: std::time::Duration,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            Ok(())
        }
        fn set_resource(&mut self, _resource: &opentelemetry_sdk::Resource) {}
    }

    #[test]
    fn rotation_during_sdk_construction_preserves_origin_through_drop_and_descendant() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let capture = Capture::default();
        let traces = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_span_processor(RotateOnStart(std::sync::atomic::AtomicBool::new(true)))
            .with_simple_exporter(capture.clone())
            .build();
        let metrics = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let state = crate::telemetry::tests::gateway_test_state_with_tracer(
            metrics.clone(),
            traces.clone(),
        );
        crate::telemetry::tests::with_test_state(state, || {
            let parent = Closing::capture();
            let origin = parent.origin_epoch();
            let mut closing = Closing::start(Phase::NativeFinalization, &parent);
            assert_ne!(origin, Some(crate::telemetry_consent_snapshot().1));
            assert_eq!(closing.epoch, origin);
            assert_eq!(closing.correlation().origin_epoch(), origin);
            let stale = closing.correlation();
            closing.finish(Fact::Confirmed);
            let mut descendant = Closing::start(Phase::DestinationCommit, &stale);
            assert_eq!(descendant.correlation().origin_epoch(), origin);
            descendant.finish(Fact::Confirmed);
            // Deterministically model rotation AFTER a caller's admission
            // check but BEFORE the actual Observation constructor. Bypass the
            // Closing wrapper: an outer stale check cannot repair this contract.
            let admitted = Closing::capture();
            assert!(admitted.is_current());
            crate::set_telemetry_enabled(false);
            crate::set_telemetry_enabled(true);
            let mut delayed =
                Observation::start(Scenario::Closing, Boundary::ExecutionResult, &admitted, &[]);
            let mut propagated = serde_json::json!({});
            delayed
                .correlation()
                .run(|| crate::scenario::inject_params("thread/open", &mut propagated));
            assert!(propagated.get("_pioneer_correlation").is_none());
            delayed.finish(Outcome::Confirmed);
            // A stale optional link does not censor a NEW independent action.
            let mut fresh =
                Closing::start_linked(Phase::ClientSettlement, &Correlation::default(), &[stale]);
            fresh.finish(Fact::Confirmed);
            let mut params = serde_json::json!({"_pioneer_correlation":{
                "v":1,"trace":"11111111111111111111111111111111",
                "span":"2222222222222222","flags":1}});
            let (remote, _) = crate::scenario::accept_params("thread/open", &mut params);
            let mut fresh_remote = Closing::start(Phase::MessageCommit, &remote);
            fresh_remote.finish(Fact::Confirmed);
        });
        traces.force_flush().unwrap();
        let epoch = crate::telemetry_consent_snapshot().1 as i64;
        {
            let spans = capture.0.lock().unwrap();
            let accepted: Vec<_> = spans
                .iter()
                .filter(|span| {
                    crate::telemetry::portable_summary_epoch_allowed(&span.attributes, epoch)
                })
                .collect();
            assert_eq!(accepted.len(), 2);
            assert!(accepted.iter().all(|span| {
                span.attributes
                    .contains(&KeyValue::new("closing.fact", "Confirmed"))
            }));
            assert!(!accepted.iter().any(|span| {
                span.attributes
                    .contains(&KeyValue::new("closing.phase", "NativeFinalization"))
            }));
        }
        traces.shutdown().unwrap();
        metrics.shutdown().unwrap();
        crate::set_telemetry_enabled(old);
    }
}
