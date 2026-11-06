//! Shell-neutral observation boundaries. RPC success confirms only a reply;
//! execution, delivery and presentation require their own domain producers.
use opentelemetry::{
    Context, KeyValue,
    trace::{
        Span as _, SpanBuilder, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags,
        TraceId, TraceState, Tracer as _,
    },
};
use std::{
    future::Future,
    sync::OnceLock,
    time::{Instant, SystemTime},
};

static INGRESS: OnceLock<opentelemetry::metrics::Counter<u64>> = OnceLock::new();
static DURATION: OnceLock<opentelemetry::metrics::Histogram<f64>> = OnceLock::new();
pub(crate) fn init(meter: &opentelemetry::metrics::Meter) {
    let _ = INGRESS.set(meter.u64_counter("pioneer.scenario.rpc.correlation").with_description("Selected Gateway RPC ingress observations, separate from work boundary denominators; disabled collection emits nothing").build());
    let _ = DURATION.set(meter.f64_histogram("pioneer.scenario.boundary.duration").with_unit("ms").with_description("Completed observation boundaries by outcome; RPC Confirmed means reply only, not episode readiness; unfinished boundaries remain in existing work live state").build());
}

#[derive(Clone, Copy, Debug)]
pub enum Scenario {
    Opening,
    Closing,
    Update,
    Tool,
}
#[derive(Clone, Copy, Debug)]
pub enum Boundary {
    Episode,
    Attempt,
    RpcOpen,
    RpcTimelinePage,
    GatewayRpcOpen,
    GatewayRpcTimelinePage,
    ExecutionResult,
    Delivery,
    Presentation,
}
#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Confirmed,
    Failed,
    CancelledWaiter,
    DeadlineExceeded,
    TransportLost,
    Superseded,
    Returned,
    Unconfirmed,
}

/// Cloneable causal reference, never a business owner or a completion handle.
/// Contains only span context, not remote baggage or database resources.
#[derive(Clone, Debug, Default)]
pub struct Correlation {
    context: Context,
    epoch: Option<u64>,
}
// Process-local provenance only; survives nested Context attachments without
// retaining the original span. Never serialized or used as remote authority.
#[derive(Clone, Copy, Debug)]
struct ScenarioEpoch(u64);

impl Correlation {
    pub fn capture() -> Self {
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        let ambient = Context::current();
        if !enabled {
            return ambient.get::<ScenarioEpoch>().map_or_else(
                || Self::empty_in_epoch(epoch),
                |origin| Self::independent_at(Some(origin.0)),
            );
        }
        let origin_epoch = ambient
            .get::<ScenarioEpoch>()
            .map_or(epoch, |origin| origin.0);
        Self::from_span(
            if enabled {
                ambient.span().span_context().clone()
            } else {
                SpanContext::empty_context()
            },
            origin_epoch,
        )
    }
    pub(crate) fn empty_in_epoch(epoch: u64) -> Self {
        Self::from_span(SpanContext::empty_context(), epoch)
    }
    fn from_span(span: SpanContext, epoch: u64) -> Self {
        // An invalid span is no parent. Attaching it still makes the SDK's
        // parent-based sampler see an active, unsampled parent and drop roots.
        let context = Context::new().with_value(ScenarioEpoch(epoch));
        let context = if span.is_valid() {
            context.with_remote_span_context(span)
        } else {
            context
        };
        Self {
            context,
            epoch: Some(epoch),
        }
    }
    pub(crate) fn epoch(&self) -> Option<u64> {
        self.epoch
    }
    /// Independent causal parent, but provenance still belongs to this work.
    /// Optional links must never choose the epoch of an independent operation.
    pub(crate) fn independent_at(epoch: Option<u64>) -> Self {
        epoch.map_or_else(Self::default, |epoch| {
            Self::from_span(
                SpanContext::new(
                    TraceId::INVALID,
                    SpanId::INVALID,
                    TraceFlags::default(),
                    false,
                    TraceState::default(),
                ),
                epoch,
            )
        })
    }
    pub(crate) fn context(&self) -> Context {
        if crate::telemetry_sample_allowed(self.epoch) {
            self.context.clone()
        } else {
            self.epoch.map_or_else(Context::new, |epoch| {
                Context::new().with_value(ScenarioEpoch(epoch))
            })
        }
    }
    pub(crate) fn fence_if_missing(self, epoch: u64) -> Self {
        if self.epoch.is_none() {
            Self::from_span(SpanContext::empty_context(), epoch)
        } else {
            self
        }
    }
    pub(crate) fn origin_epoch(&self) -> Option<u64> {
        self.epoch
    }
    pub(crate) fn inactive(epoch: u64) -> Self {
        Self::empty_in_epoch(epoch)
    }
    /// Missing provenance permits a fresh independent fragment.
    pub fn is_current(&self) -> bool {
        self.epoch.is_none() || crate::telemetry_sample_allowed(self.epoch)
    }
    pub fn run<T>(&self, f: impl FnOnce() -> T) -> T {
        let _guard = self.context().attach();
        f()
    }
    pub fn scope<F: Future>(&self, future: F) -> impl Future<Output = F::Output> {
        // Allocate before building the scope future. Large runtime futures must
        // not be embedded again in every nested correlation wrapper: moving
        // those inline states can exhaust an executor thread's stack.
        let mut future = Box::pin(future);
        std::future::poll_fn(move |cx| {
            let _guard = self.context().attach();
            future.as_mut().poll(cx)
        })
    }
}

/// Single terminal authority. Drop censors; clones of correlation cannot finish.
/// Instants measure process-local elapsed time; wall time only places trace spans.
pub struct Observation {
    correlation: Correlation,
    span: Option<Context>,
    started: Instant,
    wall: SystemTime,
    scenario: Scenario,
    boundary: Boundary,
}
impl Observation {
    pub fn start(
        scenario: Scenario,
        boundary: Boundary,
        parent: &Correlation,
        links: &[Correlation],
    ) -> Self {
        let (enabled, current_epoch) = crate::telemetry_consent_snapshot();
        // Provenance is selected ONCE, before any SDK construction. Rotation
        // after this snapshot can only censor E, never promote it to E'.
        let origin = parent.clone().fence_if_missing(current_epoch);
        let epoch = origin.epoch.expect("local observation origin is fenced");
        let mut result = Self {
            correlation: origin,
            span: None,
            started: Instant::now(),
            wall: SystemTime::now(),
            scenario,
            boundary,
        };
        if !enabled || epoch != current_epoch {
            return result;
        }
        let Some(state) = crate::telemetry::state() else {
            return result;
        };
        let parent_context = parent.context();
        let mut builder = SpanBuilder::from_name("pioneer.scenario.boundary")
            .with_kind(SpanKind::Internal)
            .with_start_time(result.wall)
            .with_attributes(vec![
                KeyValue::new("pioneer.consent.generation", epoch.to_string()),
                KeyValue::new("scenario", format!("{scenario:?}")),
                KeyValue::new("boundary", format!("{boundary:?}")),
                KeyValue::new(
                    "correlation",
                    if parent.epoch.is_some() && parent.epoch != Some(epoch) {
                        "stale"
                    } else if parent_context.span().span_context().is_valid() {
                        "parent"
                    } else {
                        "independent_fragment"
                    },
                ),
                KeyValue::new("links.truncated", links.len() > 4),
                KeyValue::new("links.provided", links.len().min(4) as i64),
                KeyValue::new(
                    "links.accepted",
                    links
                        .iter()
                        .take(4)
                        .filter(|link| link.context().span().span_context().is_valid())
                        .count() as i64,
                ),
                KeyValue::new(
                    "links.stale",
                    links
                        .iter()
                        .take(4)
                        .filter(|link| link.epoch.is_some_and(|origin| origin != epoch))
                        .count() as i64,
                ),
            ]);
        builder.links = Some(
            links
                .iter()
                .take(4)
                .filter_map(|link| {
                    let context = link.context();
                    let span = context.span().span_context().clone();
                    span.is_valid()
                        .then(|| opentelemetry::trace::Link::new(span, vec![], 0))
                })
                .collect(),
        );
        if let Some(attributes) = &mut builder.attributes {
            attributes.push(KeyValue::new("links.provided", links.len().min(4) as i64));
            attributes.push(KeyValue::new(
                "links.accepted",
                builder.links.as_ref().map_or(0, Vec::len) as i64,
            ));
        }
        let context =
            Context::new().with_span(state.tracer.build_with_context(builder, &parent_context));
        result.correlation = Correlation::from_span(context.span().span_context().clone(), epoch);
        result.span = Some(context);
        result
    }
    pub fn accept_rpc(method: &str, parent: &Correlation, ingress: Ingress) -> Option<Self> {
        let boundary = match rpc_boundary(method)? {
            Boundary::RpcOpen => Boundary::GatewayRpcOpen,
            _ => Boundary::GatewayRpcTimelinePage,
        };
        if crate::telemetry_enabled() {
            if let Some(metric) = INGRESS.get() {
                metric.add(
                    1,
                    &[
                        KeyValue::new("boundary", format!("{boundary:?}")),
                        KeyValue::new("correlation", format!("{ingress:?}")),
                    ],
                );
            }
        }
        let observation = Self::start(Scenario::Opening, boundary, parent, &[]);
        if let Some(span) = &observation.span {
            span.span()
                .set_attribute(KeyValue::new("rpc.ingress", format!("{ingress:?}")));
        }
        Some(observation)
    }
    pub(crate) fn annotate(&self, key: &'static str, value: &'static str) {
        if crate::telemetry_sample_allowed(self.correlation.epoch) {
            if let Some(span) = &self.span {
                span.span().set_attribute(KeyValue::new(key, value));
            }
        }
    }
    pub(crate) fn domain_attribute(&self, key: &'static str, value: String) {
        if crate::telemetry_sample_allowed(self.correlation.epoch) {
            if let Some(span) = &self.span {
                span.span().set_attribute(KeyValue::new(key, value));
            }
        }
    }
    pub(crate) fn attribute(&self, key: &'static str, value: impl Into<opentelemetry::Value>) {
        if crate::telemetry_sample_allowed(self.correlation.epoch) {
            if let Some(span) = &self.span {
                span.span().set_attribute(KeyValue::new(key, value));
            }
        }
    }
    pub fn correlation(&self) -> Correlation {
        self.correlation.clone()
    }
    pub(crate) fn domain_fact(&mut self, key: &'static str, value: &'static str) {
        if crate::telemetry_sample_allowed(self.correlation.epoch) {
            if let Some(span) = &self.span {
                span.span().set_attribute(KeyValue::new(key, value));
            }
        }
    }
    pub fn finish(&mut self, outcome: Outcome) {
        let Some(span) = self.span.take() else {
            return;
        };
        // An old epoch must never export after opt-out/off-on.
        if !crate::telemetry_sample_allowed(self.correlation.epoch) {
            return;
        }
        if let Some(metric) = DURATION.get() {
            metric.record(
                self.started.elapsed().as_secs_f64() * 1000.0,
                &[
                    KeyValue::new("scenario", format!("{:?}", self.scenario)),
                    KeyValue::new("boundary", format!("{:?}", self.boundary)),
                    KeyValue::new("outcome", format!("{outcome:?}")),
                ],
            );
        }
        span.span()
            .set_attribute(KeyValue::new("outcome", format!("{outcome:?}")));
        span.span()
            .end_with_timestamp(self.wall + self.started.elapsed());
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        self.finish(Outcome::Unconfirmed);
    }
}

pub fn rpc_boundary(method: &str) -> Option<Boundary> {
    match method {
        "thread/open" => Some(Boundary::RpcOpen),
        "thread/timeline/page" => Some(Boundary::RpcTimelinePage),
        _ => None,
    }
}
const WIRE: &str = "_pioneer_correlation";
/// Exactly four bounded fields. Never sends local tokens, epochs or baggage.
pub fn inject_params(method: &str, params: &mut serde_json::Value) {
    if rpc_boundary(method).is_none() || !crate::telemetry_enabled() {
        return;
    }
    let correlation = Correlation::capture();
    let context = correlation.context();
    let span = context.span().span_context().clone();
    if !span.is_valid() {
        return;
    }
    if let Some(map) = params.as_object_mut() {
        map.insert(WIRE.into(), serde_json::json!({"v":1,"trace":span.trace_id().to_string(),"span":span.span_id().to_string(),"flags":span.trace_flags().to_u8() & 1}));
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ingress {
    Missing,
    Invalid,
    Remote,
    Disabled,
    Unselected,
}
/// Removes the optional field before any business parser and independently of consent.
pub fn accept_params(method: &str, params: &mut serde_json::Value) -> (Correlation, Ingress) {
    let value = params.as_object_mut().and_then(|map| map.remove(WIRE));
    if rpc_boundary(method).is_none() {
        return (Correlation::default(), Ingress::Unselected);
    }
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return (Correlation::default(), Ingress::Disabled);
    }
    let Some(value) = value else {
        return (Correlation::default(), Ingress::Missing);
    };
    let parsed = (|| {
        let map = value.as_object()?;
        if map.len() != 4 || map.get("v")?.as_u64()? != 1 {
            return None;
        }
        let trace = map.get("trace")?.as_str()?;
        let span = map.get("span")?.as_str()?;
        let flags = map.get("flags")?.as_u64()?;
        if trace.len() != 32
            || span.len() != 16
            || flags > 1
            || !trace
                .bytes()
                .chain(span.bytes())
                .all(|b| b.is_ascii_hexdigit())
        {
            return None;
        }
        let context = SpanContext::new(
            TraceId::from_hex(trace).ok()?,
            SpanId::from_hex(span).ok()?,
            TraceFlags::new(flags as u8),
            true,
            TraceState::default(),
        );
        context.is_valid().then_some(context)
    })();
    match parsed {
        Some(span) => (Correlation::from_span(span, epoch), Ingress::Remote),
        None => (Correlation::default(), Ingress::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Debug, Clone, Default)]
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);
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
    fn tool_boundaries_preserve_facts_and_censor_stale_continuations() {
        use crate::tool_call::{Fact, Measurement, Phase};
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_gate(true);
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
            let mut rejected = Measurement::start(Phase::Invocation, &Correlation::capture());
            rejected.finish(Fact::Invalid);
            rejected.finish(Fact::BusinessSuccess);
            let mut call = Measurement::start(Phase::Invocation, &Correlation::capture());
            let parent = call.correlation();
            let mut handler = Measurement::start(Phase::Handler, &parent);
            handler.finish(Fact::Returned);
            let mut business = Measurement::start(Phase::BusinessResult, &parent);
            business.finish(Fact::Partial);
            call.finish(Fact::Returned);
            // A required downstream result may follow an ended invocation.
            let mut retained = Measurement::start(Phase::RetainedResult, &parent);
            retained.finish(Fact::Failed);
            drop(Measurement::start(Phase::NativeResultReady, &parent));
            crate::set_telemetry_gate(false);
            let disabled = Measurement::start(Phase::Invocation, &Correlation::capture());
            let disabled_parent = disabled.correlation();
            crate::set_telemetry_gate(true);
            // Both a formerly enabled parent and a disabled handoff stay retired.
            drop(Measurement::start(Phase::RetainedResult, &parent));
            drop(Measurement::start(Phase::RetainedResult, &disabled_parent));
            let mut fresh = Measurement::start(Phase::Invocation, &Correlation::capture());
            fresh.finish(Fact::Returned);
        });
        traces.force_flush().unwrap();
        {
            let spans = capture.0.lock().unwrap();
            assert_eq!(spans.len(), 7);
            let has = |span: &opentelemetry_sdk::trace::SpanData, phase, fact| {
                span.attributes
                    .contains(&KeyValue::new("tool.phase", phase))
                    && span.attributes.contains(&KeyValue::new("tool.fact", fact))
            };
            assert_eq!(
                spans
                    .iter()
                    .filter(|span| has(span, "Invocation", "Invalid"))
                    .count(),
                1
            );
            assert!(
                !spans
                    .iter()
                    .any(|span| has(span, "Invocation", "BusinessSuccess"))
            );
            let call = spans
                .iter()
                .find(|span| {
                    has(span, "Invocation", "Returned")
                        && spans.iter().any(|child| {
                            has(child, "Handler", "Returned")
                                && child.parent_span_id == span.span_context.span_id()
                        })
                })
                .unwrap();
            for (phase, fact) in [
                ("Handler", "Returned"),
                ("BusinessResult", "Partial"),
                ("RetainedResult", "Failed"),
                ("NativeResultReady", "Unconfirmed"),
            ] {
                let child = spans.iter().find(|span| has(span, phase, fact)).unwrap();
                assert_eq!(child.parent_span_id, call.span_context.span_id());
            }
            assert_eq!(
                spans
                    .iter()
                    .filter(|span| span
                        .attributes
                        .contains(&KeyValue::new("tool.phase", "RetainedResult")))
                    .count(),
                1
            );
        }
        traces.shutdown().unwrap();
        metrics.shutdown().unwrap();
        crate::set_telemetry_gate(old);
    }

    #[test]
    fn late_operation_emit_retains_ended_parent_and_boundary_finishes_once() {
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
            let mut params = json!({"_pioneer_correlation":{"v":1,"trace":"11111111111111111111111111111111","span":"2222222222222222","flags":1}});
            let (parent, ingress) = accept_params("thread/open", &mut params);
            let mut boundary = Observation::accept_rpc("thread/open", &parent, ingress).unwrap();
            let context = boundary.correlation();
            let operation = context
                .run(|| crate::GatewayOperationTrace::start(crate::GatewayOperation::ThreadOpen));
            boundary.finish(Outcome::Returned);
            boundary.finish(Outcome::Confirmed);
            drop(boundary);
            // Parent already ended; finish under a different ambient context.
            Correlation::default().run(|| operation.finish_success());
            drop(Observation::start(
                Scenario::Tool,
                Boundary::Attempt,
                &Correlation::default(),
                &[],
            ));
        });
        traces.force_flush().unwrap();
        {
            let spans = capture.0.lock().unwrap();
            let boundary = spans
                .iter()
                .find(|span| {
                    span.attributes
                        .contains(&KeyValue::new("boundary", "GatewayRpcOpen"))
                })
                .unwrap();
            let operation = spans
                .iter()
                .find(|span| span.name == "gateway.thread.open")
                .unwrap();
            assert_eq!(
                boundary.parent_span_id,
                SpanId::from_hex("2222222222222222").unwrap()
            );
            assert_eq!(operation.parent_span_id, boundary.span_context.span_id());
            assert_eq!(
                operation.span_context.trace_id(),
                TraceId::from_hex("11111111111111111111111111111111").unwrap()
            );
            assert_eq!(spans.len(), 3);
            assert!(
                boundary
                    .attributes
                    .contains(&KeyValue::new("outcome", "Returned"))
            );
            assert!(spans.iter().any(|span| {
                span.attributes
                    .contains(&KeyValue::new("outcome", "Unconfirmed"))
            }));
        }
        traces.shutdown().unwrap();
        metrics.shutdown().unwrap();
        crate::set_telemetry_enabled(old);
    }

    #[test]
    fn retained_provenance_and_independent_links_have_distinct_consent_semantics() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let previous = crate::telemetry_enabled();
        crate::set_telemetry_gate(true);
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
            let wire = |cause: &Correlation| {
                cause.run(|| {
                    let mut params = json!({});
                    inject_params("thread/open", &mut params);
                    params.get("_pioneer_correlation").cloned()
                })
            };
            let mut parent = Observation::start(
                Scenario::Opening,
                Boundary::Episode,
                &Correlation::capture(),
                &[],
            );
            let old = parent.correlation();
            let consent = crate::opening::Consent::capture(&old);
            parent.finish(Outcome::Returned);
            // Ending a parent does not revoke its still-current causal reference.
            let mut child = Observation::start(Scenario::Opening, Boundary::Attempt, &old, &[]);
            assert_eq!(
                wire(&child.correlation()).unwrap()["trace"],
                wire(&old).unwrap()["trace"]
            );
            child.finish(Outcome::Confirmed);
            crate::set_telemetry_gate(false);
            let disabled = Correlation::capture();
            let disabled_observation = Observation::start(
                Scenario::Opening,
                Boundary::Attempt,
                &Correlation::default(),
                &[],
            );
            crate::set_telemetry_gate(true);
            assert!(wire(&disabled).is_none());
            for cause in [
                old.clone(),
                old.run(Correlation::capture),
                disabled,
                disabled_observation.correlation(),
            ] {
                let mut delayed =
                    Observation::start(Scenario::Opening, Boundary::Attempt, &cause, &[]);
                assert!(wire(&delayed.correlation()).is_none());
                delayed.finish(Outcome::Confirmed);
            }
            let mut params = json!({"_pioneer_correlation":{"v":1,"trace":"77777777777777777777777777777777","span":"2222222222222222","flags":1}});
            let (remote, ingress) = accept_params("thread/open", &mut params);
            assert_eq!(ingress, Ingress::Remote);
            let mut fresh = Observation::start(Scenario::Opening, Boundary::Attempt, &remote, &[]);
            assert_eq!(
                wire(&fresh.correlation()).unwrap()["trace"],
                "77777777777777777777777777777777"
            );
            let current = fresh.correlation();
            fresh.finish(Outcome::Confirmed);
            // Optional links do not own independent work. A new batch is valid
            // even if all its links have retired; mixed links retain the fresh one.
            let mut independent = Observation::start(
                Scenario::Opening,
                Boundary::Attempt,
                &Correlation::default(),
                &[old.clone()],
            );
            assert!(wire(&independent.correlation()).is_some());
            independent.annotate("test.case", "stale_links");
            independent.finish(Outcome::Confirmed);
            let mut mixed = Observation::start(
                Scenario::Opening,
                Boundary::Attempt,
                &Correlation::default(),
                &[
                    old.clone(),
                    current.clone(),
                    old.clone(),
                    current.clone(),
                    old.clone(),
                ],
            );
            mixed.annotate("test.case", "mixed_links");
            mixed.finish(Outcome::Confirmed);
            // A delayed producer is separate from optional links: fresh links
            // cannot bless a shared attempt admitted under an old consent epoch.
            let mut delayed = crate::opening::Measure::start_with_consent(
                crate::opening::Phase::SubscriptionFlight,
                &Correlation::default(),
                &[current.clone()],
                consent,
            );
            assert!(wire(&delayed.correlation()).is_none());
            delayed.finish(crate::opening::Result::Confirmed);
            let mut new_work = crate::opening::Measure::start(
                crate::opening::Phase::SubscriptionFlight,
                &Correlation::default(),
                &[old],
            );
            assert!(wire(&new_work.correlation()).is_some());
            new_work.finish(crate::opening::Result::Confirmed);
        });
        traces.force_flush().unwrap();
        let spans = capture.0.lock().unwrap();
        let stale_links = spans
            .iter()
            .find(|span| {
                span.attributes
                    .contains(&KeyValue::new("test.case", "stale_links"))
            })
            .unwrap();
        assert!(stale_links.links.links.is_empty());
        assert!(
            stale_links
                .attributes
                .contains(&KeyValue::new("links.stale", 1_i64))
        );
        let mixed = spans
            .iter()
            .find(|span| {
                span.attributes
                    .contains(&KeyValue::new("test.case", "mixed_links"))
            })
            .unwrap();
        assert_eq!(mixed.links.links.len(), 2);
        assert!(
            mixed
                .attributes
                .contains(&KeyValue::new("links.truncated", true))
        );
        assert_eq!(
            spans.len(),
            6,
            "retired/disabled producers never create new observations"
        );
        drop(spans);
        traces.shutdown().unwrap();
        metrics.shutdown().unwrap();
        crate::set_telemetry_gate(previous);
    }

    #[test]
    fn optional_wire_is_stripped_without_changing_business_params_or_startup() {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let wire = json!({"v":1,"trace":"11111111111111111111111111111111","span":"2222222222222222","flags":1});
        for (metadata, expected) in [
            (None, Ingress::Missing),
            (Some(wire.clone()), Ingress::Remote),
            (Some(json!({"v":2})), Ingress::Invalid),
            (Some(json!("malformed")), Ingress::Invalid),
            (
                Some(
                    json!({"v":1,"trace":"00000000000000000000000000000000","span":"2222222222222222","flags":1}),
                ),
                Ingress::Invalid,
            ),
        ] {
            let mut params = json!({"thread_id":"business", "_pioneer_telemetry": {"future":true}});
            if let Some(metadata) = metadata {
                params[WIRE] = metadata;
            }
            let (_, actual) = accept_params("thread/open", &mut params);
            assert_eq!(actual, expected);
            assert_eq!(
                params,
                json!({"thread_id":"business", "_pioneer_telemetry":{"future":true}})
            );
            crate::turn_startup::accept_params("thread/open", &mut params);
            assert_eq!(params, json!({"thread_id":"business"}));
        }
        let mut params = json!({"turn_id":"unrelated", "_pioneer_correlation":wire});
        assert_eq!(
            accept_params("workspace/list", &mut params).1,
            Ingress::Unselected
        );
        assert_eq!(params, json!({"turn_id":"unrelated"}));
        crate::set_telemetry_enabled(old);
    }

    #[test]
    fn interleaved_remote_polls_restore_parent_and_reject_old_epoch() {
        use std::{
            future::poll_fn,
            pin::pin,
            task::{Context as TaskContext, Poll, Waker},
        };
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_enabled(true);
        let make = |trace| {
            let mut params = json!({"_pioneer_correlation":{"v":1,"trace":trace,"span":"2222222222222222","flags":1}});
            accept_params("thread/timeline/page", &mut params).0
        };
        let first = make("11111111111111111111111111111111");
        let second = make("33333333333333333333333333333333");
        let surrounding = Context::current().span().span_context().clone();
        let check = |expected| {
            poll_fn(move |_| {
                assert_eq!(
                    Context::current().span().span_context().trace_id(),
                    TraceId::from_hex(expected).unwrap()
                );
                Poll::<()>::Pending
            })
        };
        let mut a = pin!(first.scope(check("11111111111111111111111111111111")));
        let mut b = pin!(second.scope(check("33333333333333333333333333333333")));
        let mut cx = TaskContext::from_waker(Waker::noop());
        assert!(a.as_mut().poll(&mut cx).is_pending());
        assert!(b.as_mut().poll(&mut cx).is_pending());
        assert_eq!(Context::current().span().span_context(), &surrounding);
        first.run(|| {
            // The old span is still attached while consent changes. Recapture
            // must retain its provenance rather than bless it with the new epoch.
            crate::set_telemetry_enabled(false);
            crate::set_telemetry_enabled(true);
            let recaptured = Correlation::capture();
            recaptured.run(|| assert!(!Context::current().span().span_context().is_valid()));
            let fresh = make("44444444444444444444444444444444");
            fresh.run(|| {
                let recaptured = Correlation::capture();
                recaptured.run(|| {
                    assert_eq!(
                        Context::current().span().span_context().trace_id(),
                        TraceId::from_hex("44444444444444444444444444444444").unwrap()
                    )
                });
            });
        });
        first.run(|| assert!(!Context::current().span().span_context().is_valid()));
        crate::set_telemetry_enabled(old);
    }
}
