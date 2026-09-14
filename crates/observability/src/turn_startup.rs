//! Bounded, consent-gated startup observations. Keys are process-local only;
//! request contents, database identifiers and error strings are never exported.
use opentelemetry::{
    Context, KeyValue,
    metrics::{Counter, Histogram, Meter},
    trace::{
        SpanBuilder, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId,
        TraceState, Tracer as _,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

const LIMIT: usize = 1024;
const STAGE_LIMIT: usize = 128;
const TTL: Duration = Duration::from_secs(15 * 60);
const BOUNDS: &[f64] = &[
    5., 10., 25., 50., 100., 250., 500., 1000., 2000., 3000., 5000., 8000., 10000., 15000., 30000.,
    60000., 120000., 300000.,
];
/// Optional JSON-RPC params extension, removed before business deserialization.
/// Old peers ignore it. It is never an authorization or idempotency input.
pub const WIRE_FIELD: &str = "_pioneer_telemetry";

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Input {
    Text,
    Voice,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Runtime {
    Unknown,
    Native,
    Codex,
    Claude,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    #[default]
    Unknown,
    Desktop,
    Mobile,
}
#[derive(Clone, Copy, Debug)]
pub enum Output {
    Text,
    Reasoning,
    ToolCall,
    BufferedText,
    BufferedReasoning,
}
#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    OutputReceived,
    Rejected,
    Failed,
    Cancelled,
    DeadlineExceeded,
    Blocked,
    NoSpeech,
    CompletedWithoutOutput,
    ObservationLost,
}
impl Outcome {
    fn name(self) -> &'static str {
        match self {
            Self::OutputReceived => "output_received",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Blocked => "blocked",
            Self::NoSpeech => "no_speech",
            Self::CompletedWithoutOutput => "completed_without_output",
            Self::ObservationLost => "observation_lost",
        }
    }
}
macro_rules! stages { ($($variant:ident => $name:literal),* $(,)?) => {
    #[derive(Clone,Copy,Debug)] pub enum Stage { $($variant),* }
    impl Stage { pub fn name(self)-> &'static str { match self { $(Self::$variant=>$name),* } } }
}; }
stages! {
 ClientPrepare=>"client.prepare", ClientSessionWait=>"client.session.wait", ClientUpload=>"client.attachments.upload", ClientQueue=>"client.transport.queue", ClientWrite=>"client.transport.write", ClientBridge=>"client.bridge", ClientApply=>"client.apply", ClientEventQueue=>"client.events.queue", ClientWorkerWait=>"client.worker.wait",
 VoiceFinish=>"voice.capture.finish", VoiceFinalize=>"voice.finalize", VoiceVad=>"voice.vad", VoiceWorkerWait=>"voice.worker.wait", VoiceTranscriberWait=>"voice.transcriber.wait", VoiceTranscribe=>"voice.transcribe", VoiceInput=>"voice.transcript_to_input",
 DelegationCreate=>"task.create", DelegationWait=>"task.handoff.wait", ChildPrepare=>"task.child.prepare",
 GatewayDispatch=>"gateway.dispatch", Admission=>"turn.admission", Persist=>"turn.persist", History=>"turn.history", Artifacts=>"turn.artifacts", Skills=>"turn.skills", Security=>"turn.security", Environment=>"turn.environment",
 NativePrepare=>"native.prepare", ContextPrepare=>"native.context.prepare", Preflight=>"native.preflight", Hooks=>"native.hooks", HookPolicy=>"native.hooks.policy", HookContext=>"native.hooks.context", HookPostPreflight=>"native.hooks.post_preflight", HookCompile=>"native.hooks.compile", HookTools=>"native.hooks.tools", CompactionWait=>"native.compaction.wait", CompactionWork=>"native.compaction.work", ProviderConnect=>"native.provider.connect", RuntimeFirstOutput=>"runtime.wait_first_output",
 CliSessionWait=>"cli.session.wait", CliAcquire=>"cli.session.acquire", CliInitialize=>"cli.session.start", CliSpawn=>"cli.process.spawn", CliHandshake=>"cli.initialize", CliThread=>"cli.thread.start_resume", CliMcp=>"cli.mcp.prepare", CliDispatch=>"cli.dispatch", ReadinessWait=>"runtime.readiness.wait",
 Projection=>"first_output.projection", Fanout=>"first_output.fanout", SocketWrite=>"first_output.socket_write", OutboundQueue=>"first_output.outbound.queue", DbAdmission=>"db.admission.wait", DbAcquire=>"db.pool.acquire", DbExecute=>"db.execute", DbCommit=>"db.commit"
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireContext {
    pub version: u8,
    pub traceparent: String,
    pub input: Input,
    pub runtime: Runtime,
    #[serde(default)]
    pub platform: Platform,
}
impl WireContext {
    fn parent(&self) -> Option<Context> {
        if self.version != 1 || self.traceparent.len() != 55 {
            return None;
        }
        let s = self.traceparent.as_bytes();
        if &s[..3] != b"00-" || s[35] != b'-' || s[52] != b'-' {
            return None;
        }
        if s.iter()
            .enumerate()
            .any(|(i, b)| ![2, 35, 52].contains(&i) && !b.is_ascii_hexdigit())
        {
            return None;
        }
        let trace = TraceId::from_hex(&self.traceparent[3..35]).ok()?;
        let span = SpanId::from_hex(&self.traceparent[36..52]).ok()?;
        let flags = u8::from_str_radix(&self.traceparent[53..55], 16).ok()?;
        let sc = SpanContext::new(
            trace,
            span,
            TraceFlags::new(flags & 1),
            true,
            TraceState::default(),
        );
        sc.is_valid()
            .then(|| Context::new().with_remote_span_context(sc))
    }
}
struct Metrics {
    first: Histogram<f64>,
    text: Histogram<f64>,
    presented: Histogram<f64>,
    presented_text: Histogram<f64>,
    gateway: Histogram<f64>,
    runtime: Histogram<f64>,
    delivery: Histogram<f64>,
    stage: Histogram<f64>,
    observation: Histogram<f64>,
    unattributed: Histogram<f64>,
    attempts: Counter<u64>,
    outcomes: Counter<u64>,
    losses: Counter<u64>,
    ingress: Counter<u64>,
}
static METRICS: OnceLock<Metrics> = OnceLock::new();
static ACTIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
type Entry = Arc<Mutex<Observation>>;
static REGISTRY: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
fn registry() -> &'static Mutex<HashMap<String, Entry>> {
    REGISTRY.get_or_init(Default::default)
}

pub(crate) fn init(meter: &Meter) {
    let histogram = |name| {
        meter
            .f64_histogram(name)
            .with_unit("ms")
            .with_boundaries(BOUNDS.to_vec())
            .build()
    };
    let _ = METRICS.set(Metrics {
        ingress: meter.u64_counter("pioneer.turn.startup.ingress").build(),
        first: histogram("pioneer.turn.startup.first_output.duration"),
        text: histogram("pioneer.turn.startup.first_text.duration"),
        presented: histogram("pioneer.turn.startup.first_presented.duration"),
        presented_text: histogram("pioneer.turn.startup.first_text_presented.duration"),
        gateway: histogram("pioneer.turn.startup.gateway.first_output.duration"),
        runtime: histogram("pioneer.turn.startup.runtime.first_output.duration"),
        delivery: histogram("pioneer.turn.startup.first_output.delivery.duration"),
        stage: histogram("pioneer.turn.startup.stage.duration"),
        observation: histogram("pioneer.turn.startup.observation.duration"),
        unattributed: histogram("pioneer.turn.startup.unattributed.duration"),
        attempts: meter.u64_counter("pioneer.turn.startup.attempts").build(),
        outcomes: meter.u64_counter("pioneer.turn.startup.outcomes").build(),
        losses: meter
            .u64_counter("pioneer.turn.startup.observation.losses")
            .build(),
    });
    meter
        .u64_observable_gauge("pioneer.turn.startup.inflight")
        .with_callback(|observer| {
            sweep();
            let entries = entries();
            let count = entries
                .iter()
                .filter(|e| e.lock().is_ok_and(|o| !o.closed))
                .count();
            if super::telemetry_enabled() {
                observer.observe(count as u64, &[]);
            }
        })
        .build();
    meter
        .f64_observable_gauge("pioneer.turn.startup.oldest.age")
        .with_unit("ms")
        .with_callback(|observer| {
            let age = entries()
                .iter()
                .filter_map(|e| {
                    e.lock()
                        .ok()
                        .and_then(|o| (!o.closed).then(|| o.start.elapsed().as_secs_f64() * 1000.))
                })
                .fold(0., f64::max);
            if super::telemetry_enabled() {
                observer.observe(age, &[]);
            }
        })
        .build();
}
fn entries() -> Vec<Entry> {
    let Ok(map) = registry().lock() else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for entry in map.values() {
        if seen.insert(Arc::as_ptr(entry)) {
            entries.push(entry.clone());
        }
    }
    entries
}
fn sweep() {
    let expired: Vec<_> = entries()
        .into_iter()
        .filter(|e| {
            e.lock()
                .is_ok_and(|o| o.start.elapsed() > TTL || !super::telemetry_enabled())
        })
        .collect();
    if let Ok(mut map) = registry().lock() {
        map.retain(|_, e| !expired.iter().any(|x| Arc::ptr_eq(x, e)));
    }
    for entry in expired {
        if let Ok(mut o) = entry.lock() {
            o.finish(Outcome::ObservationLost);
        }
    }
}
fn get(key: &str) -> Option<Entry> {
    if !super::telemetry_enabled() {
        return None;
    }
    let entry = registry().lock().ok()?.get(key).cloned()?;
    if !entry.lock().ok()?.eligible() {
        return None;
    }
    Some(entry)
}
struct Observation {
    cx: Context,
    start: Instant,
    input: Input,
    runtime: Runtime,
    gateway: bool,
    registered: bool,
    consent_epoch: u64,
    closed: bool,
    first: bool,
    text: bool,
    presented: bool,
    output_at: Option<Instant>,
    dispatched: Option<Instant>,
    stages: usize,
    db_stages: usize,
    connection: Option<u64>,
    turn_key: Option<String>,
    delegation_parent: Option<String>,
    delegation_child: Option<String>,
    delegated_first_forwarded: bool,
    delegated_text_forwarded: bool,
    delegated_terminal_forwarded: bool,
    delegation_queued_at: Option<Instant>,
    thread_role: &'static str,
    mobile_presented: bool,
    mobile_text: bool,
    mobile_received: bool,
    mobile_received_text: bool,
    queued_at: Option<Instant>,
    applied: bool,
    prepared: bool,
    outbound_at: Option<Instant>,
    platform: Platform,
    model_family: &'static str,
    reasoning: &'static str,
    session: &'static str,
    runtime_span: Option<Context>,
    intervals: Vec<(Instant, Option<Instant>)>,
}
impl Observation {
    fn eligible(&self) -> bool {
        crate::telemetry_consent_snapshot() == (true, self.consent_epoch)
    }
    fn attrs(&self) -> Vec<KeyValue> {
        vec![
            KeyValue::new(
                "input.kind",
                match self.input {
                    Input::Text => "text",
                    Input::Voice => "voice",
                },
            ),
            KeyValue::new(
                "runtime.kind",
                match self.runtime {
                    Runtime::Unknown => "unknown",
                    Runtime::Native => "native",
                    Runtime::Codex => "codex",
                    Runtime::Claude => "claude",
                },
            ),
            KeyValue::new(
                "runtime.family",
                match self.runtime {
                    Runtime::Unknown => "unknown",
                    Runtime::Native => "native",
                    _ => "cli",
                },
            ),
            KeyValue::new(
                "observation.scope",
                if self.gateway { "gateway" } else { "client" },
            ),
            KeyValue::new("receive_boundary", "rust_transport"),
            KeyValue::new(
                "start_boundary",
                if self.gateway {
                    "gateway_ingress"
                } else {
                    "rust_intent"
                },
            ),
            KeyValue::new(
                "client.platform",
                match self.platform {
                    Platform::Desktop => "desktop",
                    Platform::Mobile => "mobile",
                    Platform::Unknown => "unknown",
                },
            ),
            KeyValue::new("model.family", self.model_family),
            KeyValue::new("reasoning.effort", self.reasoning),
            KeyValue::new("session.state", self.session),
            KeyValue::new(
                "launch.path",
                if self.delegation_parent.is_some() {
                    "delegated"
                } else {
                    "direct"
                },
            ),
            KeyValue::new("thread.role", self.thread_role),
        ]
    }
    fn finish(&mut self, outcome: Outcome) {
        if self.closed {
            return;
        }
        self.closed = true;
        if self.registered {
            ACTIVE.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        let now = Instant::now();
        let total = now.duration_since(self.start).as_secs_f64() * 1000.;
        let mut intervals: Vec<_> = self
            .intervals
            .iter()
            .map(|(start, end)| {
                (
                    start.saturating_duration_since(self.start).as_secs_f64() * 1000.,
                    end.unwrap_or(now)
                        .saturating_duration_since(self.start)
                        .as_secs_f64()
                        * 1000.,
                )
            })
            .collect();
        if let Some(start) = self.dispatched {
            intervals.push((
                start.saturating_duration_since(self.start).as_secs_f64() * 1000.,
                self.output_at
                    .unwrap_or(now)
                    .saturating_duration_since(self.start)
                    .as_secs_f64()
                    * 1000.,
            ));
        }
        let unattributed = (total - covered_duration(total, &mut intervals)).max(0.);
        self.cx
            .span()
            .set_attribute(KeyValue::new("startup.unattributed_ms", unattributed));
        if super::telemetry_enabled() {
            let mut attrs = self.attrs();
            attrs.push(KeyValue::new("outcome", outcome.name()));
            if let Some(m) = METRICS.get() {
                m.outcomes.add(1, &attrs);
                m.unattributed.record(unattributed, &attrs);
                m.observation
                    .record(self.start.elapsed().as_secs_f64() * 1000., &attrs);
            }
            self.cx.span().set_attributes(attrs);
            self.cx.span().set_attribute(KeyValue::new(
                "startup.duration_ms",
                self.start.elapsed().as_secs_f64() * 1000.,
            ));
            if matches!(
                outcome,
                Outcome::Failed | Outcome::Rejected | Outcome::DeadlineExceeded
            ) {
                self.cx.span().set_status(Status::error(outcome.name()));
            }
        }
        if let Some(cx) = self.runtime_span.take() {
            cx.span()
                .set_attribute(KeyValue::new("outcome", outcome.name()));
            cx.span().end();
        }
        self.cx.span().end();
    }
}
/// Start at the user action. A duplicate key never resets the clock.
pub fn begin(key: &str, input: Input, runtime: Runtime) {
    begin_inner(key, input, runtime, None, false);
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            o.platform = match crate::telemetry::state().map(|s| s.target) {
                Some(crate::telemetry::TelemetryTarget::Desktop) => Platform::Desktop,
                Some(crate::telemetry::TelemetryTarget::Mobile) => Platform::Mobile,
                _ => Platform::Unknown,
            };
        }
    }
}
fn begin_inner(key: &str, input: Input, runtime: Runtime, parent: Option<Context>, gateway: bool) {
    if key.is_empty() || key.len() > 512 || !super::telemetry_enabled() {
        return;
    }
    let Some(state) = crate::telemetry::state() else {
        return;
    };
    sweep();
    let Ok(mut map) = registry().lock() else {
        return;
    };
    if !super::telemetry_enabled() {
        return;
    }
    if map.contains_key(key) {
        return;
    }
    if map.len() >= LIMIT {
        if let Some(m) = METRICS.get() {
            m.losses.add(1, &[KeyValue::new("reason", "registry_full")]);
        }
        return;
    }
    let span = build_span_with_consent(
        &state.tracer,
        SpanBuilder::from_name(if gateway {
            "gateway.turn.startup"
        } else {
            "client.turn.startup"
        })
        .with_kind(if gateway {
            SpanKind::Server
        } else {
            SpanKind::Internal
        }),
        &parent.unwrap_or_default(),
    );
    let o = Observation {
        cx: Context::new().with_span(span),
        start: Instant::now(),
        input,
        runtime,
        gateway,
        registered: true,
        consent_epoch: crate::telemetry_consent_snapshot().1,
        closed: false,
        first: false,
        text: false,
        presented: false,
        output_at: None,
        dispatched: None,
        stages: 0,
        db_stages: 0,
        connection: None,
        turn_key: None,
        delegation_parent: None,
        delegation_child: None,
        delegated_first_forwarded: false,
        delegated_text_forwarded: false,
        delegated_terminal_forwarded: false,
        delegation_queued_at: None,
        thread_role: "unknown",
        mobile_presented: false,
        mobile_text: false,
        mobile_received: false,
        mobile_received_text: false,
        queued_at: None,
        applied: false,
        prepared: false,
        outbound_at: None,
        platform: Platform::Unknown,
        model_family: "unknown",
        reasoning: "unknown",
        session: "unknown",
        runtime_span: None,
        intervals: Vec::new(),
    };
    o.cx.span().set_attributes(o.attrs());
    if let Some(m) = METRICS.get() {
        let attrs: Vec<_> = o
            .attrs()
            .into_iter()
            .filter(|a| matches!(a.key.as_str(), "input.kind" | "observation.scope"))
            .collect();
        m.attempts.add(1, &attrs);
    }
    ACTIVE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    map.insert(key.to_owned(), Arc::new(Mutex::new(o)));
}
/// Add a process-local alias after the canonical turn ID becomes available.
pub fn bind(key: &str, turn: &str) {
    let Some(entry) = get(key) else {
        return;
    };
    if turn.is_empty() || turn.len() > 512 {
        return;
    }
    if let Ok(mut o) = entry.lock() {
        o.turn_key = Some(turn.to_owned());
    }
    if let Ok(mut map) = registry().lock() {
        if map.len() < LIMIT {
            map.entry(turn.to_owned()).or_insert(entry);
        }
    }
}
/// The role describes the thread where the user submitted the operation.
pub fn thread_role(key: &str, child: bool) {
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            o.thread_role = if child { "child" } else { "root" };
        }
    }
}
impl Observation {
    fn mark_delegated(&mut self, parent: &str) {
        if !self.closed && self.delegation_parent.is_none() {
            self.delegation_parent = Some(parent.to_owned());
            self.delegation_queued_at = Some(Instant::now());
        }
    }
    fn accepts_child(&self, parent: &str, child: &str) -> bool {
        !self.closed
            && parent != child
            && !child.is_empty()
            && child.len() <= 512
            && self.delegation_parent.as_deref() == Some(parent)
            && self
                .delegation_child
                .as_deref()
                .is_none_or(|bound| bound == child)
    }
    fn finish_for(&mut self, key: &str, outcome: Outcome) {
        if !self.ignores_terminal(key, outcome) {
            self.finish(outcome);
        }
    }
    fn ignores_terminal(&self, key: &str, outcome: Outcome) -> bool {
        matches!(outcome, Outcome::CompletedWithoutOutput)
            && self.delegation_parent.as_deref() == Some(key)
    }
}
pub fn delegated(parent: &str) {
    if parent.is_empty() || parent.len() > 512 {
        return;
    }
    if let Some(e) = get(parent) {
        if let Ok(mut o) = e.lock() {
            o.mark_delegated(parent);
        }
    }
}
/// Bind only the first execution child, never retries, reviewers or nested tasks.
pub fn delegated_child(parent: &str, child: &str) {
    let Some(e) = get(parent) else {
        return;
    };
    let Ok(mut map) = registry().lock() else {
        return;
    };
    bind_delegated_entry(&mut map, &e, parent, child);
}
fn bind_delegated_entry(map: &mut HashMap<String, Entry>, e: &Entry, parent: &str, child: &str) {
    if map.len() >= LIMIT || map.get(child).is_some_and(|other| !Arc::ptr_eq(other, e)) {
        return;
    }
    let Ok(mut o) = e.lock() else {
        return;
    };
    if !o.accepts_child(parent, child) {
        return;
    }
    o.delegation_child = Some(child.to_owned());
    o.turn_key = Some(child.to_owned());
    map.insert(child.to_owned(), e.clone());
}
/// Called in the executor's explicit startup scope, before preparation.
pub fn delegation_picked_up(parent: &str) {
    let elapsed = get(parent).and_then(|e| {
        e.lock()
            .ok()?
            .delegation_queued_at
            .take()
            .map(|t| t.elapsed())
    });
    if let Some(elapsed) = elapsed {
        record_current_duration(Stage::DelegationWait, elapsed);
    }
}
/// Bounded forwarding of actual first model events to the initiating connection.
/// Authorization and subscriber deduplication are owned by the gateway.
pub fn delegated_delivery_candidate(method: &str, params: &serde_json::Value) -> Option<u64> {
    let key = request_key(params)?;
    let e = get(&key)?;
    let mut o = e.lock().ok()?;
    o.delegated_delivery(
        &key,
        notification_output(method, params),
        notification_outcome(method, params),
    )
}
impl Observation {
    fn delegated_delivery(
        &mut self,
        key: &str,
        output: Option<Output>,
        terminal: Option<Outcome>,
    ) -> Option<u64> {
        if self.closed && self.output_at.is_none() {
            return None;
        }
        if self.delegation_child.as_deref() != Some(key) || !self.gateway {
            return None;
        }
        let connection = self.connection?;
        if let Some(output) = output {
            let text = matches!(output, Output::Text | Output::BufferedText);
            if self.delegated_first_forwarded && (!text || self.delegated_text_forwarded) {
                return None;
            }
            self.delegated_first_forwarded = true;
            self.delegated_text_forwarded |= text;
            Some(connection)
        } else if terminal.is_some() && !self.delegated_terminal_forwarded && !self.closed {
            self.delegated_terminal_forwarded = true;
            Some(connection)
        } else {
            None
        }
    }
}
pub fn delegation_requeued(parent: &str) {
    if let Some(e) = get(parent) {
        if let Ok(mut o) = e.lock() {
            if !o.closed && o.delegation_parent.as_deref() == Some(parent) {
                o.delegation_queued_at.get_or_insert_with(Instant::now);
            }
        }
    }
}
pub fn delegated_owner(parent: &str) -> Option<u64> {
    let e = get(parent)?;
    let o = e.lock().ok()?;
    if o.closed || o.delegation_parent.as_deref() != Some(parent) {
        return None;
    }
    o.connection
}
pub fn current_is_delegated() -> bool {
    current_key().and_then(|key| get(&key)).is_some_and(|e| {
        e.lock().is_ok_and(|o| {
            o.delegation_parent.is_some()
                && (!o.closed || o.output_at.is_some())
                && (!o.delegated_first_forwarded || !o.delegated_text_forwarded)
        })
    })
}
const NOTIFICATION_FIELD: &str = "_pioneer_startup";
/// Transport-only metadata; added after authorization, never persisted in domain events.
pub fn decorate_notification(params: &mut serde_json::Value) {
    let Some(key) = request_key(params) else {
        return;
    };
    let Some(e) = get(&key) else {
        return;
    };
    let Ok(o) = e.lock() else {
        return;
    };
    let metadata = serde_json::json!({"parent": o.delegation_parent, "role": o.thread_role});
    if let Some(params) = params.as_object_mut() {
        params.insert(NOTIFICATION_FIELD.into(), metadata);
    }
}
fn observe_notification_metadata(params: &serde_json::Value) {
    let Some(key) = request_key(params) else {
        return;
    };
    let Some(metadata) = params.get(NOTIFICATION_FIELD) else {
        return;
    };
    let parent = metadata
        .get("parent")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty() && s.len() <= 512);
    if let Some(parent) = parent {
        delegated(parent);
        if parent != key {
            delegated_child(parent, &key);
        }
    }
    match metadata.get("role").and_then(|v| v.as_str()) {
        Some("child") => thread_role(&key, true),
        Some("root") => thread_role(&key, false),
        _ => {}
    }
}
pub fn strip_notification_metadata(params: &mut serde_json::Value) {
    if let Some(params) = params.as_object_mut() {
        params.remove(NOTIFICATION_FIELD);
    }
}

pub fn set_runtime(key: &str, runtime: Runtime) {
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            o.runtime = runtime;
        }
    }
}
pub fn finish(key: &str, outcome: Outcome) {
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            o.finish_for(key, outcome);
        }
    }
}
pub fn dispatched(key: &str) {
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            if !o.closed && o.dispatched.is_none() {
                o.dispatched = Some(Instant::now());
                if let Some(state) = crate::telemetry::state() {
                    let span = build_span_with_consent(
                        &state.tracer,
                        SpanBuilder::from_name(Stage::RuntimeFirstOutput.name())
                            .with_attributes(o.attrs()),
                        &o.cx,
                    );
                    o.runtime_span = Some(Context::new().with_span(span));
                }
            }
        }
    }
}
pub fn wire(key: &str) -> Option<WireContext> {
    let entry = get(key)?;
    let o = entry.lock().ok()?;
    if !o.eligible() {
        return None;
    }
    let span = o.cx.span();
    let sc = span.span_context();
    Some(WireContext {
        version: 1,
        traceparent: format!(
            "00-{}-{}-{:02x}",
            sc.trace_id(),
            sc.span_id(),
            sc.trace_flags().to_u8()
        ),
        input: o.input,
        runtime: o.runtime,
        platform: o.platform,
    })
}
/// Malformed/unsupported metadata is ignored, never an RPC error.
pub fn accept(key: &str, value: &serde_json::Value) {
    // Reject oversized metadata before cloning any caller-controlled strings/maps.
    let Some(map) = value.as_object() else {
        return;
    };
    if map
        .keys()
        .any(|k| !["version", "traceparent", "input", "runtime", "platform"].contains(&k.as_str()))
        || map.get("version").and_then(|v| v.as_u64()) != Some(1)
        || !(4..=5).contains(&map.len())
        || !map
            .get("traceparent")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.len() == 55)
        || !map
            .get("input")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.len() <= 5)
        || !map
            .get("runtime")
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.len() <= 7)
    {
        return;
    }
    if map
        .get("platform")
        .is_some_and(|v| !v.as_str().is_some_and(|s| s.len() <= 7))
    {
        return;
    }
    let Ok(wire) = serde_json::from_value::<WireContext>(value.clone()) else {
        return;
    };
    let Some(parent) = wire.parent() else {
        return;
    };
    begin_inner(key, wire.input, wire.runtime, Some(parent), true);
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            o.platform = wire.platform;
        }
    }
}
/// Every RPC carries its own context. Only model turn ingress is accepted.
pub fn inject_params(method: &str, params: &mut serde_json::Value) {
    if !matches!(method, "turn/start" | "voice/session/finalize") {
        return;
    }
    let key = params
        .get("turn_id")
        .or_else(|| params.get("context").and_then(|c| c.get("turn_id")))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    if let Some(key) = key.as_deref() {
        configure_request(key, params);
    }
    if let Some(wire) = key.as_deref().and_then(wire) {
        if let Some(entry) = key.as_deref().and_then(get) {
            if let Ok(mut o) = entry.lock() {
                if !o.closed {
                    o.queued_at = Some(Instant::now());
                }
            }
        }
        if let Some(map) = params.as_object_mut() {
            if let Ok(value) = serde_json::to_value(wire) {
                map.insert(WIRE_FIELD.into(), value);
            }
        }
    }
}
pub fn accept_params(method: &str, params: &mut serde_json::Value) {
    let wire = params.as_object_mut().and_then(|p| p.remove(WIRE_FIELD));
    if !matches!(method, "turn/start" | "voice/session/finalize") {
        return;
    }
    let key = params
        .get("turn_id")
        .or_else(|| params.get("context").and_then(|c| c.get("turn_id")))
        .and_then(|v| v.as_str());
    if let Some(key) = key {
        let supplied = wire.is_some();
        if let Some(wire) = wire {
            accept(key, &wire);
        }
        let correlated = get(key).is_some();
        if super::telemetry_enabled() {
            if let Some(m) = METRICS.get() {
                m.ingress.add(
                    1,
                    &[KeyValue::new(
                        "correlation",
                        if correlated {
                            "remote"
                        } else if supplied {
                            "invalid"
                        } else {
                            "missing"
                        },
                    )],
                );
            }
        }
        if !correlated {
            begin_inner(
                key,
                if method == "voice/session/finalize" {
                    Input::Voice
                } else {
                    Input::Text
                },
                Runtime::Unknown,
                None,
                true,
            );
        }
        configure_request(key, params);
    }
}
/// Observation at the provider/CLI boundary, before projection and fanout.
pub fn runtime_output(key: &str, output: Output) {
    let Some(entry) = get(key) else {
        return;
    };
    let Ok(mut o) = entry.lock() else {
        return;
    };
    if o.output_at.is_some() || o.closed {
        return;
    }
    let now = Instant::now();
    o.output_at = Some(now);
    if let Some(cx) = o.runtime_span.take() {
        cx.span()
            .set_attribute(KeyValue::new("first_output.kind", output_name(output)));
        cx.span().end();
    }
    let mut attrs = o.attrs();
    attrs.push(KeyValue::new("first_output.kind", output_name(output)));
    attrs.push(KeyValue::new(
        "output.delivery",
        if matches!(output, Output::BufferedText | Output::BufferedReasoning) {
            "buffered"
        } else {
            "streamed"
        },
    ));
    if let Some(m) = METRICS.get() {
        m.gateway
            .record(now.duration_since(o.start).as_secs_f64() * 1000., &attrs);
        if let Some(start) = o.dispatched {
            m.runtime
                .record(now.duration_since(start).as_secs_f64() * 1000., &attrs);
        }
    }
    attrs.push(KeyValue::new(
        "runtime.observation",
        match o.runtime {
            Runtime::Native => "provider_chunk",
            _ => "cli_projection",
        },
    ));
    o.cx.span().set_attributes(attrs.clone());
    o.cx.span().add_event("runtime.first_output", attrs);
}
fn output_name(output: Output) -> &'static str {
    match output {
        Output::Text | Output::BufferedText => "assistant_text",
        Output::Reasoning | Output::BufferedReasoning => "reasoning",
        Output::ToolCall => "tool_call",
    }
}
pub fn received(key: &str, output: Output) {
    let Some(entry) = get(key) else {
        return;
    };
    let Ok(mut o) = entry.lock() else {
        return;
    };
    o.observe_received(output);
}
impl Observation {
    fn observe_received(&mut self, output: Output) {
        if self.gateway || (self.registered && !self.eligible()) {
            return;
        }
        let mut attrs = self.attrs();
        attrs.push(KeyValue::new("first_output.kind", output_name(output)));
        attrs.push(KeyValue::new(
            "output.delivery",
            if matches!(output, Output::BufferedText | Output::BufferedReasoning) {
                "buffered"
            } else {
                "streamed"
            },
        ));
        let elapsed = self.start.elapsed().as_secs_f64() * 1000.;
        if !self.first && !self.closed {
            self.first = true;
            self.output_at = Some(Instant::now());
            self.cx.span().set_attributes(attrs.clone());
            if let Some(m) = METRICS.get() {
                m.first.record(elapsed, &attrs);
            }
            self.finish(Outcome::OutputReceived);
        }
        if self.first && !self.text && matches!(output, Output::Text | Output::BufferedText) {
            self.text = true;
            if let Some(m) = METRICS.get() {
                m.text.record(elapsed, &attrs);
            }
        }
    }
}

pub fn presented(key: &str) {
    let Some(entry) = get(key) else {
        return;
    };
    let Ok(mut o) = entry.lock() else {
        return;
    };
    if !o.eligible() || o.gateway || !o.first || o.presented {
        return;
    }
    o.presented = true;
    let mut attrs = o.attrs();
    attrs.push(KeyValue::new("presentation.observation", "gpui_render"));
    if let Some(m) = METRICS.get() {
        m.presented
            .record(o.start.elapsed().as_secs_f64() * 1000., &attrs);
    }
}
pub fn sent(key: &str) {
    let Some(entry) = get(key) else {
        return;
    };
    let Ok(mut o) = entry.lock() else {
        return;
    };
    if !o.gateway || o.closed {
        return;
    }
    if let Some(start) = o.output_at {
        if let Some(m) = METRICS.get() {
            m.delivery
                .record(start.elapsed().as_secs_f64() * 1000., &o.attrs());
        }
        o.finish(Outcome::OutputReceived);
    }
}
/// Guards are Send and never attach thread-local context across await points.
pub struct StageGuard {
    inner: Option<(Context, Instant, Vec<KeyValue>)>,
    coverage: Option<(std::sync::Weak<Mutex<Observation>>, usize)>,
    consent_epoch: u64,
}
pub fn stage(key: &str, stage: Stage) -> StageGuard {
    let mut coverage = None;
    let mut consent_epoch = crate::telemetry_consent_snapshot().1;
    let inner = (|| {
        let e = get(key)?;
        let mut o = e.lock().ok()?;
        consent_epoch = o.consent_epoch;
        if o.closed {
            return None;
        }
        if o.stages >= STAGE_LIMIT {
            if o.stages == STAGE_LIMIT {
                o.stages += 1;
                if let Some(m) = METRICS.get() {
                    m.losses.add(1, &[KeyValue::new("reason", "stage_limit")]);
                }
            }
            return None;
        }
        if stage.name().starts_with("db.") {
            if o.db_stages >= 24 {
                return None;
            }
            o.db_stages += 1;
        }
        o.stages += 1;
        coverage = Some((Arc::downgrade(&e), o.intervals.len()));
        o.intervals.push((Instant::now(), None));
        let mut attrs = o.attrs();
        attrs.push(KeyValue::new("stage", stage.name()));
        let span = build_span_with_consent(
            &crate::telemetry::state()?.tracer,
            SpanBuilder::from_name(stage.name()).with_attributes(attrs.clone()),
            &if current_key().as_deref() == Some(key)
                && Context::current().span().span_context().is_valid()
            {
                Context::current()
            } else {
                o.cx.clone()
            },
        );
        Some((Context::new().with_span(span), Instant::now(), attrs))
    })();
    StageGuard {
        inner,
        coverage,
        consent_epoch,
    }
}
impl Drop for StageGuard {
    fn drop(&mut self) {
        if let Some((entry, index)) = self.coverage.take() {
            if let Some(entry) = entry.upgrade() {
                if let Ok(mut o) = entry.lock() {
                    if let Some(interval) = o.intervals.get_mut(index) {
                        interval.1 = Some(Instant::now());
                    }
                }
            }
        }
        if let Some((cx, start, attrs)) = self.inner.take() {
            if crate::telemetry_consent_snapshot() == (true, self.consent_epoch) {
                if let Some(m) = METRICS.get() {
                    m.stage
                        .record(start.elapsed().as_secs_f64() * 1000., &attrs);
                }
            }
            cx.span().set_attribute(KeyValue::new(
                "stage.duration_ms",
                start.elapsed().as_secs_f64() * 1000.,
            ));
            cx.span().end();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trace_context_validation_never_accepts_zero_or_malformed_ids() {
        let mut w = WireContext {
            version: 1,
            traceparent: "00-12345678901234567890123456789012-1234567890123456-01".into(),
            input: Input::Text,
            runtime: Runtime::Native,
            platform: Platform::Desktop,
        };
        assert!(w.parent().is_some());
        for bad in [
            "",
            "00-00000000000000000000000000000000-1234567890123456-01",
            "00-12345678901234567890123456789012-0000000000000000-01",
            "00-12345678901234567890123456789012-1234567890123456-zz",
            "éééééééééééééééééééééééééééa",
        ] {
            w.traceparent = bad.into();
            assert!(w.parent().is_none());
        }
    }
    #[test]
    fn malformed_extension_is_removed_without_changing_business_input() {
        let mut params = serde_json::json!({"turn_id":"turn",WIRE_FIELD:{"version":999},"input":[{"text":"private"}]});
        accept_params("turn/start", &mut params);
        assert_eq!(
            params,
            serde_json::json!({"turn_id":"turn","input":[{"text":"private"}]})
        );
    }
}

/// Classifies only model-authored content; lifecycle/usage/tool stdout is excluded.
pub fn notification_output(method: &str, params: &serde_json::Value) -> Option<Output> {
    if params.get("replay").and_then(|v| v.as_bool()) == Some(true) {
        return None;
    }
    if method == "item/agent_message/delta" {
        if params.get("delta")?.as_str()?.is_empty() {
            return None;
        }
        return match params.get("stream").and_then(|v| v.as_str()) {
            Some("agent_message") => Some(
                if params
                    .pointer("/payload/startup_output_kind")
                    .and_then(|v| v.as_str())
                    == Some("buffered_text")
                {
                    Output::BufferedText
                } else {
                    Output::Text
                },
            ),
            Some("generic") => match params
                .pointer("/payload/startup_output_kind")
                .or_else(|| params.pointer("/payload/runtimeDeltaKind"))
                .and_then(|v| v.as_str())
            {
                Some("buffered_reasoning") => Some(Output::BufferedReasoning),
                Some("reasoning" | "reasoning_text" | "reasoning_summary") => {
                    Some(Output::Reasoning)
                }
                _ => None,
            },
            _ => None,
        };
    }
    if method == "item/started"
        && matches!(
            params.pointer("/item/type").and_then(|v| v.as_str()),
            Some("commandExecution" | "fileChange" | "dynamicToolCall" | "webSearch")
        )
    {
        return Some(Output::ToolCall);
    }
    if method == "item/completed"
        && params.pointer("/item/type").and_then(|v| v.as_str()) == Some("agentMessage")
        && params
            .pointer("/item/text")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
    {
        return Some(Output::BufferedText);
    }
    if method == "item/completed"
        && params.pointer("/item/type").and_then(|v| v.as_str()) == Some("reasoning")
        && ["/item/summary", "/item/content"].iter().any(|path| {
            params
                .pointer(path)
                .and_then(|v| v.as_array())
                .is_some_and(|parts| {
                    parts
                        .iter()
                        .any(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                })
        })
    {
        return Some(Output::BufferedReasoning);
    }
    None
}
pub fn observe_notification(method: &str, params: &serde_json::Value) {
    observe_notification_metadata(params);
    let key = params
        .get("turn_id")
        .or_else(|| params.pointer("/turn/id"))
        .and_then(|v| v.as_str());
    let Some(key) = key else {
        return;
    };
    if let Some(output) = notification_output(method, params) {
        received(key, output);
    }
    if let Some(outcome) = notification_outcome(method, params) {
        finish(key, outcome);
    }
}
pub fn notification_outcome(method: &str, params: &serde_json::Value) -> Option<Outcome> {
    match method {
        "turn/startup/outcome" => match params.get("outcome").and_then(|v| v.as_str()) {
            Some("failed") => Some(Outcome::Failed),
            Some("cancelled") => Some(Outcome::Cancelled),
            Some("blocked") => Some(Outcome::Blocked),
            Some("rejected") => Some(Outcome::Rejected),
            _ => None,
        },
        "voice/session/result" => match params.get("outcome").and_then(|v| v.as_str()) {
            Some("no_speech") => Some(Outcome::NoSpeech),
            Some("cancelled") => Some(Outcome::Cancelled),
            Some("failed") => Some(Outcome::Failed),
            _ => None,
        },
        "turn/completed" | "turn/failed" | "turn/blocked" => {
            if params.pointer("/turn/status").and_then(|v| v.as_str()) == Some("interrupted") {
                return Some(Outcome::Cancelled);
            }
            Some(match method {
                "turn/failed" => Outcome::Failed,
                "turn/blocked" => Outcome::Blocked,
                _ => Outcome::CompletedWithoutOutput,
            })
        }
        _ => None,
    }
}

thread_local! { static CURRENT: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) }; }
/// Captures context for an explicitly spawned task or synchronous worker.
pub fn current_key() -> Option<String> {
    CURRENT.with(|v| v.borrow().clone())
}
pub fn current_stage(stage_name: Stage) -> StageGuard {
    current_key()
        .map(|key| stage(&key, stage_name))
        .unwrap_or(StageGuard {
            inner: None,
            coverage: None,
            consent_epoch: crate::telemetry_consent_snapshot().1,
        })
}
struct CurrentGuard(Option<String>);
impl CurrentGuard {
    fn enter(key: Option<String>) -> Self {
        Self(CURRENT.with(|v| v.replace(key)))
    }
}
impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|v| v.replace(self.0.take()));
    }
}
/// Installs context only for a single poll; Pending never leaks a thread-local guard.
pub fn scope<F: std::future::Future>(key: Option<String>, future: F) -> Scoped<F> {
    Scoped {
        key,
        parent: None,
        inner: Box::pin(future),
    }
}
pub struct Scoped<F> {
    key: Option<String>,
    parent: Option<Context>,
    inner: std::pin::Pin<Box<F>>,
}
impl<F: std::future::Future> std::future::Future for Scoped<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = CurrentGuard::enter(this.key.clone());
        let parent = this
            .parent
            .clone()
            .or_else(|| this.key.as_deref().and_then(observation_context))
            .unwrap_or_default();
        let _parent = parent.attach();
        this.inner.as_mut().poll(cx)
    }
}
pub fn scope_sync<T>(key: Option<String>, work: impl FnOnce() -> T) -> T {
    let parent = if current_key() == key {
        Context::current()
    } else {
        key.as_deref()
            .and_then(observation_context)
            .unwrap_or_default()
    };
    let _guard = CurrentGuard::enter(key);
    let _parent = parent.attach();
    work()
}
pub fn request_key(params: &serde_json::Value) -> Option<String> {
    params
        .get("turn_id")
        .or_else(|| params.pointer("/context/turn_id"))
        .or_else(|| params.pointer("/turn/id"))
        .and_then(|v| v.as_str())
        .filter(|s| s.len() <= 512)
        .map(str::to_owned)
}

/// Only the initiating connection owns gateway delivery latency.
pub fn connection_owner(key: &str, connection: u64) {
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            o.connection.get_or_insert(connection);
        }
    }
}
pub fn sent_on_connection(key: &str, connection: u64) {
    let matches =
        get(key).is_some_and(|e| e.lock().is_ok_and(|o| o.connection == Some(connection)));
    if matches {
        sent(key);
    }
}
/// Records a measured SDK duration without SQL text or SQL fingerprints.
pub fn record_current_duration(stage_name: Stage, elapsed: Duration) {
    if let Some(key) = current_key() {
        let Some(entry) = get(&key) else {
            return;
        };
        let Ok(mut o) = entry.lock() else {
            return;
        };
        if o.closed
            || o.stages >= STAGE_LIMIT
            || (stage_name.name().starts_with("db.") && o.db_stages >= 24)
        {
            return;
        }
        o.stages += 1;
        if stage_name.name().starts_with("db.") {
            o.db_stages += 1;
        }
        let now = Instant::now();
        o.intervals
            .push((now.checked_sub(elapsed).unwrap_or(now), Some(now)));
        let Some(state) = crate::telemetry::state() else {
            return;
        };
        let mut attrs = o.attrs();
        attrs.push(KeyValue::new("stage", stage_name.name()));
        let end = std::time::SystemTime::now();
        let start = end.checked_sub(elapsed).unwrap_or(end);
        let span = build_span_with_consent(
            &state.tracer,
            SpanBuilder::from_name(stage_name.name())
                .with_start_time(start)
                .with_attributes(attrs.clone()),
            &o.cx,
        );
        use opentelemetry::trace::Span as _;
        let mut span = span;
        span.end_with_timestamp(end);
        if let Some(m) = METRICS.get() {
            m.stage.record(elapsed.as_secs_f64() * 1000., &attrs);
        }
    }
}

/// Bridge lookup uses only a local alias, never a exported thread identifier.
pub fn canonical_key(key: &str) -> Option<String> {
    get(key)?.lock().ok()?.turn_key.clone()
}
/// JS supplies a JS→JS duration. No subtraction between JS and Rust clocks.
pub fn mobile_presented(key: &str, elapsed_ms: f64, text: bool) -> bool {
    if !elapsed_ms.is_finite() || !(0.0..=900_000.).contains(&elapsed_ms) {
        return false;
    }
    let Some(e) = get(key) else {
        return false;
    };
    let Ok(mut o) = e.lock() else {
        return false;
    };
    if !o.eligible() || o.gateway || !o.first {
        return false;
    }
    let mut attrs = o.attrs();
    attrs.retain(|a| !matches!(a.key.as_str(), "receive_boundary" | "start_boundary"));
    attrs.push(KeyValue::new("start_boundary", "js_intent"));
    attrs.push(KeyValue::new("receive_boundary", "js_commit"));
    attrs.push(KeyValue::new("presentation.observation", "react_commit"));
    if !o.mobile_presented {
        o.mobile_presented = true;
        if let Some(m) = METRICS.get() {
            m.presented.record(elapsed_ms, &attrs);
        }
        if let Some(state) = crate::telemetry::state() {
            let mut span = build_span_with_consent(
                &state.tracer,
                SpanBuilder::from_name("client.first_content.present")
                    .with_attributes(attrs.clone()),
                &o.cx,
            );
            use opentelemetry::trace::Span as _;
            span.set_attribute(KeyValue::new("startup.elapsed_ms", elapsed_ms));
            span.end();
        }
    }
    if text && !o.mobile_text {
        o.mobile_text = true;
        if let Some(m) = METRICS.get() {
            m.presented_text.record(elapsed_ms, &attrs);
        }
    }
    true
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Reused,
    New,
    Replaced,
    Resumed,
}
pub fn session_state(state: SessionState) {
    if let Some(key) = current_key() {
        if let Some(e) = get(&key) {
            if let Ok(mut o) = e.lock() {
                o.session = match state {
                    SessionState::Reused => "reused",
                    SessionState::New => "new",
                    SessionState::Replaced => "replaced",
                    SessionState::Resumed => "resumed",
                };
                o.cx.span()
                    .set_attribute(KeyValue::new("session.state", o.session));
            }
        }
    }
}

/// A connection loss is censored explicitly; it is not a successful latency sample.
pub fn connection_lost(connection: u64) {
    for entry in entries() {
        if let Ok(mut o) = entry.lock() {
            if o.connection == Some(connection) {
                o.finish(Outcome::ObservationLost);
            }
        }
    }
}
/// Only parse outbound envelopes while there are eligible observations.
pub fn active() -> bool {
    super::telemetry_enabled() && ACTIVE.load(std::sync::atomic::Ordering::Relaxed) != 0
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    fn observation() -> Observation {
        Observation {
            cx: Context::new(),
            start: Instant::now(),
            input: Input::Text,
            runtime: Runtime::Native,
            gateway: false,
            registered: false,
            consent_epoch: crate::telemetry_consent_snapshot().1,
            closed: false,
            first: false,
            text: false,
            presented: false,
            output_at: None,
            dispatched: None,
            stages: 0,
            db_stages: 0,
            connection: None,
            turn_key: None,
            delegation_parent: None,
            delegation_child: None,
            delegated_first_forwarded: false,
            delegated_text_forwarded: false,
            delegated_terminal_forwarded: false,
            delegation_queued_at: None,
            thread_role: "unknown",
            mobile_presented: false,
            mobile_text: false,
            mobile_received: false,
            mobile_received_text: false,
            queued_at: None,
            applied: false,
            prepared: false,
            outbound_at: None,
            platform: Platform::Unknown,
            model_family: "unknown",
            reasoning: "unknown",
            session: "unknown",
            runtime_span: None,
            intervals: Vec::new(),
        }
    }
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
    fn completed_startup_exports_one_span_with_the_remote_trace_parent() {
        use opentelemetry::trace::TracerProvider;
        let capture = Capture::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(capture.clone())
            .build();
        let tracer = provider.tracer("startup-test");
        let parent = WireContext {
            version: 1,
            traceparent: "00-12345678901234567890123456789012-1234567890123456-01".into(),
            input: Input::Voice,
            runtime: Runtime::Claude,
            platform: Platform::Mobile,
        }
        .parent()
        .unwrap();
        let mut o = observation();
        o.cx = Context::new().with_span(
            tracer.build_with_context(SpanBuilder::from_name("client.turn.startup"), &parent),
        );
        o.observe_received(Output::Text);
        o.observe_received(Output::Text);
        provider.force_flush().unwrap();
        let spans = capture.0.lock().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].span_context.trace_id(),
            parent.span().span_context().trace_id()
        );
        assert_eq!(
            spans[0].parent_span_id,
            parent.span().span_context().span_id()
        );
        assert!(spans[0].end_time >= spans[0].start_time);
    }
    #[test]
    fn reasoning_then_text_closes_start_once_but_preserves_first_text() {
        let mut o = observation();
        o.observe_received(Output::Reasoning);
        assert!(o.first && o.closed && !o.text);
        let started = o.start;
        o.observe_received(Output::Reasoning);
        o.observe_received(Output::Text);
        assert!(o.text);
        assert_eq!(o.start, started);
    }
    #[test]
    fn late_content_after_failure_never_becomes_a_success() {
        for outcome in [
            Outcome::Cancelled,
            Outcome::Failed,
            Outcome::DeadlineExceeded,
            Outcome::NoSpeech,
            Outcome::ObservationLost,
            Outcome::Blocked,
        ] {
            let mut o = observation();
            o.finish(outcome);
            o.observe_received(Output::Text);
            assert!(o.closed && !o.first && !o.text);
        }
    }
    #[test]
    fn delegated_parent_completion_waits_for_child_and_preserves_click_clock() {
        let mut o = observation();
        let start = o.start;
        o.mark_delegated("parent");
        o.finish_for("parent", Outcome::CompletedWithoutOutput);
        assert!(!o.closed);
        let e = Arc::new(Mutex::new(o));
        let mut map = HashMap::from([("parent".to_owned(), e.clone())]);
        bind_delegated_entry(&mut map, &e, "parent", "child");
        assert!(Arc::ptr_eq(&map["parent"], &map["child"]));
        let mut o = map["child"].lock().unwrap();
        assert_eq!(o.start, start);
        assert_eq!(o.turn_key.as_deref(), Some("child"));
        o.observe_received(Output::Reasoning);
        o.finish_for("parent", Outcome::CompletedWithoutOutput);
        o.observe_received(Output::Text);
        assert!(o.closed && o.first && o.text);
    }
    #[test]
    fn direct_child_followup_and_other_operations_cannot_be_rebound() {
        let delegated = Arc::new(Mutex::new(observation()));
        delegated.lock().unwrap().mark_delegated("parent");
        let followup = Arc::new(Mutex::new(observation()));
        let mut map = HashMap::from([("followup".into(), followup.clone())]);
        bind_delegated_entry(&mut map, &delegated, "parent", "followup");
        assert!(Arc::ptr_eq(&map["followup"], &followup));
        bind_delegated_entry(&mut map, &delegated, "parent", "child");
        bind_delegated_entry(&mut map, &delegated, "parent", "retry");
        bind_delegated_entry(&mut map, &delegated, "child", "grandchild");
        assert!(!map.contains_key("retry") && !map.contains_key("grandchild"));
        assert!(!followup.lock().unwrap().closed);
        map["child"].lock().unwrap().observe_received(Output::Text);
        assert!(!followup.lock().unwrap().first);
        followup.lock().unwrap().observe_received(Output::Text);
        assert!(followup.lock().unwrap().first);
    }
    #[test]
    fn delegated_forwarding_is_bounded_and_never_routes_another_turn() {
        let mut o = observation();
        o.gateway = true;
        o.connection = Some(42);
        o.mark_delegated("parent");
        o.delegation_child = Some("child".into());
        assert_eq!(
            o.delegated_delivery("other", Some(Output::Text), None),
            None
        );
        assert_eq!(
            o.delegated_delivery("parent", None, Some(Outcome::CompletedWithoutOutput)),
            None
        );
        assert_eq!(o.delegated_delivery("child", None, None), None);
        assert_eq!(
            o.delegated_delivery("child", Some(Output::Reasoning), None),
            Some(42)
        );
        assert_eq!(
            o.delegated_delivery("child", Some(Output::Reasoning), None),
            None
        );
        assert_eq!(
            o.delegated_delivery("child", Some(Output::Text), None),
            Some(42)
        );
        assert_eq!(
            o.delegated_delivery("child", Some(Output::BufferedText), None),
            None
        );
    }

    #[test]
    fn delegated_failure_or_empty_child_ends_observation_without_success() {
        for (key, outcome) in [
            ("parent", Outcome::Cancelled),
            ("parent", Outcome::Failed),
            ("child", Outcome::CompletedWithoutOutput),
        ] {
            let mut o = observation();
            o.mark_delegated("parent");
            o.finish_for(key, outcome);
            assert!(o.closed && !o.first);
            o.observe_received(Output::Text);
            assert!(!o.first);
        }
        let mut direct = observation();
        direct.finish_for("child", Outcome::CompletedWithoutOutput);
        assert!(direct.closed);
    }
    #[test]
    fn startup_outcome_never_counts_as_model_output_and_metadata_is_transport_only() {
        let mut params = serde_json::json!({"turn_id":"parent","outcome":"cancelled","_pioneer_startup":{"parent":"parent","role":"root"}});
        assert!(notification_output("turn/startup/outcome", &params).is_none());
        assert!(matches!(
            notification_outcome("turn/startup/outcome", &params),
            Some(Outcome::Cancelled)
        ));
        strip_notification_metadata(&mut params);
        assert_eq!(
            params,
            serde_json::json!({"turn_id":"parent","outcome":"cancelled"})
        );
    }

    #[test]
    fn gateway_content_cannot_record_client_latency() {
        let mut o = observation();
        o.gateway = true;
        o.observe_received(Output::Text);
        assert!(!o.first && !o.closed);
    }
    #[test]
    fn content_classifier_excludes_ack_empty_stdout_unknown_generic_and_replay() {
        for (method, params) in [
            ("turn/started", serde_json::json!({"turn_id":"t"})),
            (
                "item/agent_message/delta",
                serde_json::json!({"delta":"", "stream":"agent_message"}),
            ),
            (
                "item/agent_message/delta",
                serde_json::json!({"delta":"tool output", "stream":"stdout"}),
            ),
            (
                "item/agent_message/delta",
                serde_json::json!({"delta":"status", "stream":"generic"}),
            ),
            (
                "item/agent_message/delta",
                serde_json::json!({"delta":"old", "stream":"agent_message", "replay":true}),
            ),
            (
                "item/completed",
                serde_json::json!({"item":{"type":"agentMessage","text":""}}),
            ),
        ] {
            assert!(
                notification_output(method, &params).is_none(),
                "{method}: {params}"
            );
        }
        assert!(matches!(
            notification_output(
                "item/agent_message/delta",
                &serde_json::json!({"delta":"x","stream":"generic","payload":{"runtimeDeltaKind":"reasoning_text"}})
            ),
            Some(Output::Reasoning)
        ));
        assert!(matches!(
            notification_output(
                "item/completed",
                &serde_json::json!({"item":{"type":"agentMessage","text":"buffered"}})
            ),
            Some(Output::BufferedText)
        ));
        assert!(matches!(
            notification_output(
                "item/started",
                &serde_json::json!({"item":{"type":"commandExecution"}})
            ),
            Some(Output::ToolCall)
        ));
    }
    #[test]
    fn buffered_output_is_distinct_from_streamed_output() {
        for (stream, kind) in [
            ("agent_message", "buffered_text"),
            ("generic", "buffered_reasoning"),
        ] {
            let output = notification_output(
                "item/agent_message/delta",
                &serde_json::json!({"delta":"content","stream":stream,"payload":{"startup_output_kind":kind}}),
            );
            assert!(matches!(
                (kind, output),
                ("buffered_text", Some(Output::BufferedText))
                    | ("buffered_reasoning", Some(Output::BufferedReasoning))
            ));
        }
        for field in ["summary", "content"] {
            let mut item = serde_json::json!({"type":"reasoning", "summary":[], "content":[]});
            assert!(
                notification_output("item/completed", &serde_json::json!({"item":item})).is_none()
            );
            item[field] = serde_json::json!(["", "reasoning"]);
            assert!(matches!(
                notification_output("item/completed", &serde_json::json!({"item":item})),
                Some(Output::BufferedReasoning)
            ));
        }
    }

    #[test]
    fn interleaved_future_polls_restore_context_on_pending_ready_and_unwind() {
        use std::future::Future;
        use std::task::{Poll, Waker};
        let waker = Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut polls = 0;
        let first = std::future::poll_fn(move |_| {
            assert_eq!(current_key().as_deref(), Some("first"));
            polls += 1;
            if polls == 1 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        });
        let mut first = Box::pin(scope(Some("first".into()), first));
        scope_sync(Some("outer".into()), || {
            assert!(first.as_mut().poll(&mut cx).is_pending());
            assert_eq!(current_key().as_deref(), Some("outer"));
            let mut second = Box::pin(scope(Some("second".into()), async {
                assert_eq!(current_key().as_deref(), Some("second"));
            }));
            assert!(second.as_mut().poll(&mut cx).is_ready());
            assert_eq!(current_key().as_deref(), Some("outer"));
            assert!(first.as_mut().poll(&mut cx).is_ready());
        });
        assert!(current_key().is_none());
        let _ = std::panic::catch_unwind(|| scope_sync(Some("panic".into()), || panic!("test")));
        assert!(current_key().is_none());
    }
    #[test]
    fn exported_dimensions_never_include_local_identifiers() {
        let mut o = observation();
        o.turn_key = Some("private-turn".into());
        o.connection = Some(123);
        let attrs = format!("{:?}", o.attrs());
        assert!(!attrs.contains("private-turn") && !attrs.contains("123"));
    }
}

/// Nested stage parentage is installed per poll, so concurrent turns cannot inherit it.
pub async fn scope_stage<F: std::future::Future>(
    key: Option<String>,
    name: Stage,
    future: F,
) -> F::Output {
    let guard = key.as_deref().map(|key| stage(key, name));
    let parent = guard
        .as_ref()
        .and_then(|g| g.inner.as_ref().map(|(cx, _, _)| cx.clone()));
    Scoped {
        key,
        parent,
        inner: Box::pin(future),
    }
    .await
}
/// The queue marker is local monotonic time, never an RPC timestamp.
pub fn client_write(payload: &str, connection: u64) -> Option<StageGuard> {
    if !active() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    let key = request_key(value.get("params")?)?;
    connection_owner(&key, connection);
    let entry = get(&key)?;
    let queued = entry.lock().ok()?.queued_at.take();
    if let Some(queued) = queued {
        scope_sync(Some(key.clone()), || {
            record_current_duration(Stage::ClientQueue, queued.elapsed())
        });
    }
    Some(stage(&key, Stage::ClientWrite))
}

fn configure_request(key: &str, params: &serde_json::Value) {
    if let Some(e) = get(key) {
        if let Ok(mut o) = e.lock() {
            let params = params.get("context").unwrap_or(params);
            o.model_family = model_family(params.get("model").and_then(|v| v.as_str()));
            o.reasoning = match params.pointer("/reasoning/effort").and_then(|v| v.as_str()) {
                Some("none") => "none",
                Some("minimal") => "minimal",
                Some("low") => "low",
                Some("medium") => "medium",
                Some("high") => "high",
                Some("xhigh") => "xhigh",
                Some("max") => "max",
                Some("ultra") => "ultra",
                Some(_) => "other",
                None => "default",
            };
        }
    }
}
fn model_family(model: Option<&str>) -> &'static str {
    match model {
        Some(s) if s.starts_with("gpt-") => "gpt",
        Some(s) if s.starts_with("claude-") => "claude",
        Some(s) if s.starts_with("gemini-") => "gemini",
        Some(s) if s.starts_with("o3") || s.starts_with("o4") => "o_series",
        Some(_) => "other",
        None => "default",
    }
}

/// UI observation was interrupted. Export only a bounded reason, never a caller string.
pub fn mobile_lost(key: &str, reason: &str) -> bool {
    let reason = match reason {
        "background" => "mobile_background",
        "expired" => "mobile_expired",
        "registry_full" => "mobile_registry_full",
        _ => return false,
    };
    let Some(entry) = get(key) else {
        return false;
    };
    let Ok(mut o) = entry.lock() else {
        return false;
    };
    if !o.eligible() {
        return false;
    }
    if let Some(m) = METRICS.get() {
        m.losses.add(1, &[KeyValue::new("reason", reason)]);
    }
    o.finish(Outcome::ObservationLost);
    true
}

pub fn finish_on_connection(key: &str, connection: u64, outcome: Outcome) {
    if get(key).is_some_and(|e| e.lock().is_ok_and(|o| o.connection == Some(connection))) {
        finish(key, outcome);
    }
}

/// First JS observation following native receipt. Both endpoints use the JS clock;
/// this explicitly includes publication scheduling, and is not wire-arrival time.
pub fn mobile_received(key: &str, elapsed_ms: f64, text: bool) -> bool {
    if !elapsed_ms.is_finite() || !(0.0..=900_000.).contains(&elapsed_ms) {
        return false;
    }
    let Some(entry) = get(key) else {
        return false;
    };
    let Ok(mut o) = entry.lock() else {
        return false;
    };
    if !o.eligible() || o.gateway || !o.first || (text && !o.text) {
        return false;
    }
    let mut attrs = o.attrs();
    attrs.retain(|a| !matches!(a.key.as_str(), "receive_boundary" | "start_boundary"));
    attrs.push(KeyValue::new("receive_boundary", "js_publication"));
    attrs.push(KeyValue::new("start_boundary", "js_intent"));
    if !o.mobile_received {
        o.mobile_received = true;
        if let Some(m) = METRICS.get() {
            m.first.record(elapsed_ms, &attrs);
        }
        if let Some(state) = crate::telemetry::state() {
            let cx = Context::new().with_span(build_span_with_consent(
                &state.tracer,
                SpanBuilder::from_name("client.first_output.observe")
                    .with_attributes(attrs.clone()),
                &o.cx,
            ));
            cx.span()
                .set_attribute(KeyValue::new("startup.duration_ms", elapsed_ms));
            cx.span().end();
        }
    }
    if text && !o.mobile_received_text {
        o.mobile_received_text = true;
        if let Some(m) = METRICS.get() {
            m.text.record(elapsed_ms, &attrs);
        }
    }
    true
}

/// Union of process-local intervals, clipped to startup. Nested/parallel spans
/// count once; this is coverage, not an estimate of exclusive CPU work.
fn covered_duration(total: f64, intervals: &mut [(f64, f64)]) -> f64 {
    intervals.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut covered = 0.;
    let mut until: f64 = 0.;
    for (start, end) in intervals {
        let start = start.max(0.).min(total);
        let end = end.max(start).min(total);
        covered += (end - start.max(until)).max(0.);
        until = until.max(end);
    }
    covered
}
#[cfg(test)]
mod coverage_tests {
    #[test]
    fn overlapping_nested_and_out_of_bounds_intervals_count_once() {
        assert_eq!(
            super::covered_duration(
                100.,
                &mut [(10., 40.), (20., 30.), (35., 60.), (90., 150.), (-10., 5.)]
            ),
            65.
        );
        assert_eq!(super::covered_duration(100., &mut []), 0.);
        assert_eq!(
            super::covered_duration(100., &mut [(0., 200.), (1., 99.)]),
            100.
        );
    }
}

pub fn outgoing_key(serialized: &str) -> Option<String> {
    if !active() {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(serialized).ok()?;
    let params = v.get("params")?;
    notification_output(v.get("method")?.as_str()?, params)?;
    request_key(params)
}
pub fn queued_on_connection(key: &str, connection: u64) {
    if let Some(entry) = get(key) {
        if let Ok(mut o) = entry.lock() {
            if !o.closed && o.connection == Some(connection) {
                o.outbound_at.get_or_insert_with(Instant::now);
            }
        }
    }
}
pub fn socket_write_started(key: &str, connection: u64) {
    let queued = get(key).and_then(|e| {
        let mut o = e.lock().ok()?;
        if o.closed || o.connection != Some(connection) {
            return None;
        }
        o.outbound_at.take()
    });
    if let Some(queued) = queued {
        scope_sync(Some(key.to_owned()), || {
            record_current_duration(Stage::OutboundQueue, queued.elapsed())
        });
    }
}

fn observation_context(key: &str) -> Option<Context> {
    Some(get(key)?.lock().ok()?.cx.clone())
}

#[cfg(test)]
mod parent_context_tests {
    use super::*;
    #[test]
    fn switching_turns_cannot_inherit_another_turns_otel_parent() {
        use std::future::Future;
        let outer = WireContext {
            version: 1,
            traceparent: "00-12345678901234567890123456789012-1234567890123456-01".into(),
            input: Input::Text,
            runtime: Runtime::Native,
            platform: Platform::Desktop,
        }
        .parent()
        .unwrap();
        let id = outer.span().span_context().trace_id();
        let _outer = outer.attach();
        let mut future = Box::pin(scope(Some("unregistered-other-turn".into()), async {
            assert!(!Context::current().span().span_context().is_valid());
        }));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_ready());
        assert_eq!(Context::current().span().span_context().trace_id(), id);
    }
}

pub fn client_preparing(key: &str) {
    let elapsed = get(key).and_then(|e| {
        let mut o = e.lock().ok()?;
        if o.prepared || o.closed {
            return None;
        }
        o.prepared = true;
        Some(o.start.elapsed())
    });
    if let Some(elapsed) = elapsed {
        scope_sync(Some(key.to_owned()), || {
            record_current_duration(Stage::ClientWorkerWait, elapsed)
        });
    }
}
/// First state application follows receipt, so it is a linked child of the
/// already finished startup. Later deltas do not produce per-token spans.
pub fn client_apply(key: &str) -> Option<StageGuard> {
    let entry = get(key)?;
    let mut o = entry.lock().ok()?;
    if !o.eligible() || o.gateway || !o.first || o.applied {
        return None;
    }
    o.applied = true;
    let elapsed = o.output_at?.elapsed();
    let state = crate::telemetry::state()?;
    let mut attrs = o.attrs();
    attrs.push(KeyValue::new("stage", Stage::ClientEventQueue.name()));
    if let Some(m) = METRICS.get() {
        m.stage.record(elapsed.as_secs_f64() * 1000., &attrs);
    }
    let end = std::time::SystemTime::now();
    let mut span = build_span_with_consent(
        &state.tracer,
        SpanBuilder::from_name(Stage::ClientEventQueue.name())
            .with_start_time(end.checked_sub(elapsed).unwrap_or(end))
            .with_attributes(attrs),
        &o.cx,
    );
    use opentelemetry::trace::Span as _;
    span.end_with_timestamp(end);
    let mut attrs = o.attrs();
    attrs.push(KeyValue::new("stage", Stage::ClientApply.name()));
    let span = build_span_with_consent(
        &state.tracer,
        SpanBuilder::from_name(Stage::ClientApply.name()).with_attributes(attrs.clone()),
        &o.cx,
    );
    Some(StageGuard {
        inner: Some((Context::new().with_span(span), Instant::now(), attrs)),
        coverage: None,
        consent_epoch: o.consent_epoch,
    })
}

pub(crate) const CONSENT_EPOCH_ATTRIBUTE: &str = "_pioneer.startup.consent_epoch";
fn build_span_with_consent(
    tracer: &opentelemetry_sdk::trace::SdkTracer,
    mut builder: SpanBuilder,
    parent: &Context,
) -> opentelemetry_sdk::trace::Span {
    builder
        .attributes
        .get_or_insert_with(Vec::new)
        .push(KeyValue::new(
            CONSENT_EPOCH_ATTRIBUTE,
            crate::telemetry_consent_snapshot().1 as i64,
        ));
    tracer.build_with_context(builder, parent)
}
/// Runs with the exporter gate closed. Late child spans are fenced by epoch in
/// the exporter; stage metrics are independently fenced in StageGuard::drop.
pub(crate) fn discard_on_opt_out() {
    let entries: Vec<_> = registry()
        .lock()
        .map(|mut m| m.drain().map(|(_, e)| e).collect())
        .unwrap_or_default();
    for entry in entries {
        if let Ok(mut o) = entry.lock() {
            o.finish(Outcome::ObservationLost);
        }
    }
}

/// JS measures the synchronous bridge call on its own clock. Keep it as a
/// duration/event; no cross-clock wall-time interval is manufactured.
pub fn mobile_bridge(key: &str, elapsed_ms: f64) -> bool {
    if !elapsed_ms.is_finite() || !(0.0..=900_000.).contains(&elapsed_ms) {
        return false;
    }
    let Some(entry) = get(key) else {
        return false;
    };
    let Ok(o) = entry.lock() else {
        return false;
    };
    if !o.eligible() || o.gateway || o.closed {
        return false;
    }
    let mut attrs = o.attrs();
    attrs.retain(|a| !matches!(a.key.as_str(), "receive_boundary" | "start_boundary"));
    attrs.push(KeyValue::new("start_boundary", "js_intent"));
    attrs.push(KeyValue::new("receive_boundary", "js_bridge_return"));
    attrs.push(KeyValue::new("stage", Stage::ClientBridge.name()));
    if let Some(m) = METRICS.get() {
        m.stage.record(elapsed_ms, &attrs);
    }
    attrs.push(KeyValue::new("stage.duration_ms", elapsed_ms));
    o.cx.span().add_event("client.bridge.call", attrs);
    true
}
