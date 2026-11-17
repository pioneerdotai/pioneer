//! Passive local update facts. Returned bytes, native input acceptance and a
//! socket write are deliberately not content presentation acknowledgements.
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use std::{sync::OnceLock, time::Instant};

static PEERS: OnceLock<Counter<u64>> = OnceLock::new();
static FLUSHES: OnceLock<Counter<u64>> = OnceLock::new();
static FLUSH_BATCH: OnceLock<Histogram<u64>> = OnceLock::new();
static ENTRIES: OnceLock<Counter<u64>> = OnceLock::new();
static DURATION: OnceLock<Histogram<f64>> = OnceLock::new();
pub(crate) fn init(meter: &opentelemetry::metrics::Meter) {
    let _ = PEERS.set(
        meter
            .u64_counter("pioneer.update.notification.correlation")
            .build(),
    );
    let _ = FLUSHES.set(meter.u64_counter("pioneer.update.flush.units").build());
    let _ = FLUSH_BATCH.set(
        meter
            .u64_histogram("pioneer.update.flush.batch_units")
            .with_description(
                "Existing progress drain envelopes per batch, including heartbeat envelopes",
            )
            .build(),
    );
    let _ = ENTRIES.set(
        meter
            .u64_counter("pioneer.update.entries")
            .with_description(
                "Local owner entries by stage; not unique updates or presented content",
            )
            .build(),
    );
    let _ = DURATION.set(
        meter
            .f64_histogram("pioneer.update.stage.duration")
            .with_unit("ms")
            .with_description(
                "Process-local owner intervals including censored outcomes; stages may nest",
            )
            .build(),
    );
}

#[derive(Clone, Copy, Debug)]
pub enum Stage {
    ProgressOffer,
    SnapshotOffer,
    OrderedOffer,
    ProgressPending,
    NotificationEncode,
    FanoutAdmission,
    Publication,
    OutboundAdmission,
    QueuedSocketWrite,
    ClientDecode,
    ClientRoute,
    NativeInput,
    TerminalReader,
    TerminalMatch,
    RenderCoverage,
    GpuiRender,
    HtmlLayout,
    HtmlSource,
    NativeApply,
    TimelineBuild,
    TimelineMeasure,
    SemanticApply,
    Source,
}
#[derive(Clone, Copy, Debug)]
pub enum Fact {
    Returned,
    Rejected,
    Coalesced,
    Superseded,
    BytesReturned,
    Written,
    WriteFailed,
    Unconfirmed,
    Overflow,
    Matched,
    NoChange,
    Uncovered,
    Unsupported,
    Stale,
    Suppressed,
    Accepted,
}

/// One handle per existing owner. No registry, timer or retained content.
/// Drop records uncertainty; it cannot synthesize success.
#[derive(Debug)]
pub struct StageObservation {
    stage: Stage,
    started: Option<Instant>,
    epoch: Option<u64>,
}
impl StageObservation {
    pub fn start(stage: Stage) -> Self {
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        Self::start_in(stage, enabled, epoch)
    }
    fn start_at(stage: Stage, expected: Option<u64>) -> Self {
        let (enabled, epoch) = crate::telemetry_consent_snapshot();
        Self::start_in(stage, enabled && expected == Some(epoch), epoch)
    }
    fn start_in(stage: Stage, enabled: bool, epoch: u64) -> Self {
        if enabled {
            if let Some(entries) = ENTRIES.get() {
                entries.add(1, &[KeyValue::new("stage", format!("{stage:?}"))]);
            }
        }
        Self {
            stage,
            started: enabled.then(Instant::now),
            epoch: enabled.then_some(epoch),
        }
    }
    pub fn finish(&mut self, fact: Fact) {
        let Some(started) = self.started.take() else {
            return;
        };
        if !crate::telemetry_sample_allowed(self.epoch) {
            return;
        }
        if let Some(duration) = DURATION.get() {
            duration.record(
                started.elapsed().as_secs_f64() * 1000.,
                &[
                    KeyValue::new("stage", format!("{:?}", self.stage)),
                    KeyValue::new("fact", format!("{fact:?}")),
                ],
            );
        }
    }
}
impl Drop for StageObservation {
    fn drop(&mut self) {
        self.finish(Fact::Unconfirmed);
    }
}

use crate::scenario::{Boundary, Correlation, Observation, Outcome, Scenario};
use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

/// Diagnostic hint only; never consulted by a reducer or authorization owner.
/// IDs stay process-local and are capped before retention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentHint {
    pub thread: String,
    pub item: Option<String>,
    pub change: Change,
    pub bytes: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Append,
    Snapshot,
    Recovery,
    Removed,
}
impl ContentHint {
    pub fn new(thread: &str, item: Option<&str>, change: Change, bytes: usize) -> Option<Self> {
        if !crate::telemetry_enabled()
            || thread.len() > 256
            || item.is_some_and(|id| id.len() > 256)
        {
            return None;
        }
        Some(Self {
            thread: thread.into(),
            item: item.map(str::to_owned),
            change,
            bytes: bytes as u64,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Origin {
    Observed = 0,
    Replay = 1,
    Recovery = 2,
    Mixed = 3,
    Unknown = 4,
}

/// Immutable bounded sidecar. It retains no payload, snapshot, span or owner.
#[derive(Clone)]
pub struct UpdateContext {
    correlation: Correlation,
    epoch: Option<u64>,
    hint: Option<ContentHint>,
    first: Option<Instant>,
    connection: Option<u64>,
    authorization: Option<u64>,
    complete: bool,
    origin: Origin,
    sources: [Option<Correlation>; 4],
    source_count: u64,
}
impl std::fmt::Debug for UpdateContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateContext")
            .field("complete", &self.complete)
            .finish_non_exhaustive()
    }
}
thread_local! { static UPDATE: RefCell<Option<UpdateContext>> = const { RefCell::new(None) }; }
/// Local envelope handoffs retain even an obsolete epoch marker so a late
/// descendant cannot become a new independent observer after opt-out/off-on.
/// Use `capture` for recording; this function never grants observation authority.
pub fn carried() -> Option<UpdateContext> {
    UPDATE.with(|slot| slot.borrow().clone())
}
pub fn capture() -> Option<UpdateContext> {
    UPDATE.with(|slot| slot.borrow().as_ref().filter(|c| c.valid()).cloned())
}
pub(crate) struct UpdateScope {
    previous: Option<UpdateContext>,
    _otel: Option<opentelemetry::ContextGuard>,
}
impl Drop for UpdateScope {
    fn drop(&mut self) {
        UPDATE.with(|slot| slot.replace(self.previous.take()));
    }
}
pub(crate) fn enter(context: Option<UpdateContext>) -> UpdateScope {
    let had_context = context.is_some() || UPDATE.with(|slot| slot.borrow().is_some());
    let otel = context
        .as_ref()
        .map(|c| c.correlation.context())
        .unwrap_or_else(opentelemetry::Context::new);
    let otel = had_context.then(|| otel.attach());
    let previous = UPDATE.with(|slot| slot.replace(context));
    UpdateScope {
        previous,
        _otel: otel,
    }
}
/// Restore a captured source at a synchronous owner boundary. None explicitly
/// separates an independent snapshot from an unrelated materializer wakeup.
pub fn with_context<T>(context: Option<UpdateContext>, callback: impl FnOnce() -> T) -> T {
    let _scope = enter(context);
    callback()
}
impl UpdateContext {
    fn valid(&self) -> bool {
        crate::telemetry_sample_allowed(self.epoch)
    }
    /// Some can carry retired provenance without an SDK span. Preserve it in
    /// local handoffs via run; capture/wire/native consumers reject that epoch
    /// rather than replacing it with a fresh independent source.
    pub fn source(hint: Option<ContentHint>) -> Option<Self> {
        Self::source_inner(hint, None, None)
    }
    pub fn source_with_origin(hint: Option<ContentHint>, origin: Origin) -> Option<Self> {
        Self::source_inner(hint, Some(origin), None)
    }
    /// A domain producer can be more precise than its surrounding update work.
    /// Its immutable provenance owns this source; the ambient update is only a
    /// bounded contextual link, not proof of content coverage or a fresh epoch.
    pub fn source_for_producer(hint: Option<ContentHint>, producer: &Correlation) -> Option<Self> {
        Self::source_inner(hint, None, Some(producer))
    }
    fn source_inner(
        hint: Option<ContentHint>,
        explicit: Option<Origin>,
        producer: Option<&Correlation>,
    ) -> Option<Self> {
        let (enabled, _) = crate::telemetry_consent_snapshot();
        if !enabled
            || UPDATE.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .is_some_and(|context| !context.valid())
            })
        {
            return None;
        }
        let hint = hint.filter(|hint| {
            hint.thread.len() <= 256 && hint.item.as_ref().is_none_or(|item| item.len() <= 256)
        });
        let ambient = capture();
        let origin = explicit.unwrap_or_else(|| {
            ambient.as_ref().map_or_else(
                || {
                    if hint
                        .as_ref()
                        .is_some_and(|hint| hint.change == Change::Recovery)
                    {
                        Origin::Recovery
                    } else {
                        Origin::Observed
                    }
                },
                |context| context.origin,
            )
        });
        let parent = producer.cloned().unwrap_or_else(|| {
            ambient
                .as_ref()
                .map(|c| c.correlation())
                .unwrap_or_else(Correlation::capture)
        });
        // One optional link preserves the surrounding update without allowing
        // it to replace a retained-result/lifecycle producer's exact parent.
        let contributor = producer.and(ambient.as_ref()).map(|c| c.correlation());
        let mut source = Observation::start(
            Scenario::Update,
            Boundary::Episode,
            &parent,
            contributor.as_slice(),
        );
        let effective_epoch = source.correlation().epoch();
        let mut entry = StageObservation::start_at(Stage::Source, effective_epoch);
        if let Some(hint) = &hint {
            source.attribute("update.change", format!("{:?}", hint.change));
            let bytes_known = hint.change == Change::Append || hint.bytes != 0;
            source.attribute("update.input_bytes_known", bytes_known);
            if bytes_known {
                source.attribute("update.input_bytes", hint.bytes.min(i64::MAX as u64) as i64);
            }
        }
        let complete = hint.is_some();
        let correlation = source.correlation();
        let context = Self {
            sources: [Some(correlation.clone()), None, None, None],
            source_count: 1,
            correlation,
            epoch: effective_epoch,
            hint,
            first: Some(Instant::now()),
            connection: None,
            authorization: None,
            complete,
            origin,
        };
        source.attribute("update.origin", format!("{origin:?}"));
        source.finish(Outcome::Returned);
        entry.finish(Fact::Returned);
        Some(context)
    }
    pub fn run<T>(&self, callback: impl FnOnce() -> T) -> T {
        let _scope = enter(Some(self.clone()));
        callback()
    }
    pub fn received_at(&mut self, at: Option<Instant>) {
        self.first = at;
    }
    pub fn with_authorization(mut self, generation: u64) -> Self {
        self.authorization = Some(generation);
        self
    }
    pub fn correlation(&self) -> Correlation {
        self.correlation.clone()
    }
    pub fn hint(&self) -> Option<&ContentHint> {
        self.hint.as_ref()
    }
    pub fn complete(&self) -> bool {
        self.complete
    }
    /// Original source refs stay capped at four; a fifth sentinel reports
    /// overflow to the foundation without retaining an unbounded source list.
    pub fn merged(&self, newer: &Self) -> Self {
        if !self.valid() || !newer.valid() {
            // Mixing an obsolete source into fresh work must keep the entire
            // diagnostic fragment obsolete, without creating a new-epoch span.
            let mut stale = if !self.valid() {
                self.clone()
            } else {
                newer.clone()
            };
            stale.hint = None;
            stale.complete = false;
            return stale;
        }
        let source_count = self.source_count.saturating_add(newer.source_count);
        let mut links = self
            .sources
            .iter()
            .chain(newer.sources.iter())
            .flatten()
            .take(4)
            .cloned()
            .collect::<Vec<_>>();
        if source_count > 4 {
            while links.len() < 5 {
                links.push(Correlation::default());
            }
        }
        let mut batch = Observation::start(
            Scenario::Update,
            Boundary::Delivery,
            &Correlation::independent_at(self.epoch),
            &links,
        );
        batch.attribute(
            "update.source_offers",
            source_count.min(i64::MAX as u64) as i64,
        );
        let sources = std::array::from_fn(|index| links.get(index).cloned());
        let mut hint = newer.hint.clone();
        let same_append = self
            .hint
            .as_ref()
            .zip(hint.as_mut())
            .is_some_and(|(old, next)| {
                if old.thread == next.thread
                    && old.item == next.item
                    && old.change == Change::Append
                    && next.change == Change::Append
                {
                    next.bytes = next.bytes.saturating_add(old.bytes);
                    true
                } else {
                    false
                }
            });
        // A replacement covers only the newest state. Mixed scopes have no
        // single content hint; the causal links remain available.
        let same_target = self
            .hint
            .as_ref()
            .zip(hint.as_ref())
            .is_some_and(|(old, next)| old.thread == next.thread && old.item == next.item);
        if !same_target {
            hint = None;
        }
        let result = Self {
            correlation: batch.correlation(),
            epoch: self.epoch,
            hint,
            first: self.first,
            connection: self.connection,
            authorization: self.authorization,
            complete: self.complete && newer.complete && same_append && source_count <= 4,
            sources,
            source_count,
            origin: if self.origin == newer.origin {
                self.origin
            } else {
                Origin::Mixed
            },
        };
        batch.finish(Outcome::Returned);
        result
    }
    /// Include known refs in an independent batch with explicit missing sources.
    pub fn mixed_missing(&self) -> Self {
        if !self.valid() {
            let mut stale = self.clone();
            stale.hint = None;
            stale.complete = false;
            return stale;
        }
        let source_count = self.source_count.saturating_add(1);
        let mut links = self.sources.iter().flatten().cloned().collect::<Vec<_>>();
        if source_count > 4 {
            links.push(Correlation::default());
        }
        let mut batch = Observation::start(
            Scenario::Update,
            Boundary::Delivery,
            &Correlation::independent_at(self.epoch),
            &links,
        );
        batch.attribute("update.missing_sources", true);
        batch.attribute(
            "update.source_offers",
            source_count.min(i64::MAX as u64) as i64,
        );
        let result = Self {
            correlation: batch.correlation(),
            epoch: self.epoch,
            hint: None,
            first: None,
            connection: self.connection,
            authorization: self.authorization,
            complete: false,
            origin: Origin::Mixed,
            sources: self.sources.clone(),
            source_count,
        };
        batch.finish(Outcome::Returned);
        result
    }
    pub fn incomplete(&mut self) {
        self.complete = false;
    }
    pub fn boundary(&self, boundary: Boundary) -> Observation {
        Observation::start(Scenario::Update, boundary, &self.correlation(), &[])
    }
    pub fn elapsed(&self) -> Option<std::time::Duration> {
        self.first.map(|at| at.elapsed())
    }
}

const WIRE: &str = "_pioneer_update";
fn selected(method: &str) -> bool {
    matches!(
        method,
        "item/delta"
            | "item/agent_message/delta"
            | "item/command_execution/output_delta"
            | "item/file_change/output_delta"
            | "item/tool/progress"
            | "item/started"
            | "item/updated"
            | "item/completed"
            | "thread/updated"
            | "thread/timeline/blocks/changed"
            | "turn/work/items/changed"
            | "turn/work/state/changed"
            | "turn/started"
            | "turn/completed"
            | "turn/failed"
            | "turn/blocked"
            | "turn/message/edited"
            | "turn/message/deleted"
    )
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    Valid,
    Missing,
    Invalid,
    Disabled,
    Unselected,
}
fn hint(method: &str, params: &serde_json::Value) -> Option<ContentHint> {
    let thread = params
        .get("thread_id")
        .or_else(|| params.get("threadId"))
        .and_then(|v| v.as_str())
        .or_else(|| params.get("thread")?.get("id")?.as_str())?;
    let item = params
        .get("item_id")
        .or_else(|| params.get("itemId"))
        .and_then(|v| v.as_str())
        .or_else(|| params.get("item")?.get("id")?.as_str());
    let change = if method.ends_with("/delta") || method.ends_with("/output_delta") {
        Change::Append
    } else if method == "turn/message/deleted"
        || params
            .get("removed_block_ids")
            .or_else(|| params.get("removedBlockIds"))
            .or_else(|| params.get("removed_work_item_ids"))
            .or_else(|| params.get("removedWorkItemIds"))
            .and_then(|v| v.as_array())
            .is_some_and(|ids| !ids.is_empty())
    {
        Change::Removed
    } else if matches!(
        method,
        "thread/timeline/blocks/changed" | "turn/work/items/changed"
    ) {
        Change::Recovery
    } else {
        Change::Snapshot
    };
    ContentHint::new(
        thread,
        item,
        change,
        params
            .get("delta")
            .and_then(|v| v.as_str())
            .map_or(0, str::len),
    )
}
/// Call only after ordinary serialization. The business fields are untouched.
pub fn decorate(method: &str, params: &mut serde_json::Value) -> Option<UpdateContext> {
    if !selected(method) || !crate::telemetry_enabled() {
        return None;
    }
    let mut context = capture().or_else(|| UpdateContext::source(hint(method, params)))?;
    // Projection may change the envelope from delta to snapshot. Use the actual
    // encoded content hint; remote peers derive it again from business params.
    let encoded_hint = hint(method, params);
    if context
        .hint
        .as_ref()
        .zip(encoded_hint.as_ref())
        .is_some_and(|(source, encoded)| {
            source.thread != encoded.thread
                || source.item != encoded.item
                || source.change != encoded.change
                || (source.change == Change::Append && source.bytes != encoded.bytes)
        })
    {
        context.incomplete();
    }
    if params
        .get("payload")
        .and_then(|payload| payload.get("truncated"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        context.incomplete();
    }
    context.hint = encoded_hint;
    let payload_is_flags_only = params.get("payload").is_none_or(|payload| {
        payload.as_object().is_some_and(|map| {
            map.len() <= 2
                && map
                    .keys()
                    .all(|key| key == "coalesced" || key == "truncated")
        })
    });
    if context
        .hint
        .as_ref()
        .is_some_and(|hint| hint.change == Change::Append)
        && (!payload_is_flags_only || params.get("markdown").is_some())
    {
        // Concatenation proves the observed delta bytes, not overwritten
        // payload/Markdown snapshot editions carried beside those bytes.
        context.complete = false;
    }
    let otel = context.correlation.context();
    let span = otel.span().span_context().clone();
    if span.is_valid() {
        if let Some(map) = params.as_object_mut() {
            map.insert(
                WIRE.into(),
                serde_json::json!({"v":1,"trace":span.trace_id().to_string(),
                "span":span.span_id().to_string(),"flags":span.trace_flags().to_u8() & 1,"origin":context.origin as u8,"complete":context.complete}),
            );
        }
    }
    Some(context)
}
/// Strip before typed parse even when disabled/unselected. No baggage, local
/// token, consent generation or remote clock is accepted.
pub fn receive(
    method: &str,
    params: &mut serde_json::Value,
    connection: u64,
) -> (Option<UpdateContext>, Peer) {
    accept(method, params, connection, true)
}
fn accept(
    method: &str,
    params: &mut serde_json::Value,
    connection: u64,
    record_peer: bool,
) -> (Option<UpdateContext>, Peer) {
    let value = params.as_object_mut().and_then(|map| map.remove(WIRE));
    if !selected(method) {
        return (None, Peer::Unselected);
    }
    let (enabled, epoch) = crate::telemetry_consent_snapshot();
    if !enabled {
        return (None, Peer::Disabled);
    }
    let parsed = value.as_ref().and_then(|value| {
        let map = value.as_object()?;
        if map.len() != 6 || map.get("v")?.as_u64()? != 1 {
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
                .all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }
        let span = SpanContext::new(
            TraceId::from_hex(trace).ok()?,
            SpanId::from_hex(span).ok()?,
            TraceFlags::new(flags as u8),
            true,
            TraceState::default(),
        );
        let origin = match map.get("origin")?.as_u64()? {
            0 => Origin::Observed,
            1 => Origin::Replay,
            2 => Origin::Recovery,
            3 => Origin::Mixed,
            4 => Origin::Unknown,
            _ => return None,
        };
        let complete = map.get("complete")?.as_bool()?;
        span.is_valid().then_some((span, origin, complete))
    });
    let peer = if parsed.is_some() {
        Peer::Valid
    } else if value.is_some() {
        Peer::Invalid
    } else {
        Peer::Missing
    };
    let correlation = parsed
        .as_ref()
        .map(|(span, _, _)| {
            let _scope = opentelemetry::Context::new()
                .with_remote_span_context(span.clone())
                .attach();
            Correlation::capture()
        })
        .unwrap_or_else(|| {
            if !record_peer {
                return Correlation::default();
            }
            // A missing remote edge still has one real local ingress root;
            // downstream targets must not become unrelated ambient fragments.
            let mut entry = StageObservation::start(Stage::Source);
            let mut local = Observation::start(
                Scenario::Update,
                Boundary::Episode,
                &Correlation::default(),
                &[],
            );
            local.attribute("update.origin", "Unknown");
            local.attribute("update.notification_peer", format!("{peer:?}"));
            let correlation = local.correlation();
            local.finish(Outcome::Returned);
            entry.finish(Fact::Returned);
            correlation
        });
    if record_peer {
        if let Some(peers) = PEERS.get() {
            peers.add(1, &[KeyValue::new("peer", format!("{peer:?}"))]);
        }
    }
    let content = hint(method, params);
    let known_content = content.is_some();
    let wire_complete = parsed.as_ref().is_some_and(|(_, origin, complete)| {
        *complete && !matches!(origin, Origin::Mixed | Origin::Unknown)
    });
    (
        Some(UpdateContext {
            sources: [Some(correlation.clone()), None, None, None],
            source_count: 1,
            correlation,
            epoch: Some(epoch),
            hint: content,
            first: Some(Instant::now()),
            connection: Some(connection),
            authorization: None,
            origin: parsed.map_or(Origin::Unknown, |(_, origin, _)| origin),
            complete: known_content
                && wire_complete
                && peer == Peer::Valid
                && !params
                    .get("payload")
                    .and_then(|p| p.get("truncated"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
        }),
        peer,
    )
}

/// Exact native identity from an already applied immutable snapshot. No content
/// or snapshot is retained by this diagnostic owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenderTarget {
    pub thread: String,
    pub generation: u64,
    pub row: String,
    pub revision: u64,
    pub boundary: RendererBoundary,
    pub part: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RendererBoundary {
    GpuiRender,
    GpuiEmpty,
    GpuiStructureRender,
    TerminalReader,
    HtmlVisibleLayout,
}
struct RenderState {
    targets: Vec<(RenderTarget, Option<(Observation, StageObservation)>)>,
    context: UpdateContext,
    live: crate::live::WorkGuard,
    wait: Option<crate::live::WaitGuard>,
    validity: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ending: crate::work::Outcome,
}
#[derive(Clone, Default)]
pub struct RenderBatch(Option<Arc<Mutex<RenderState>>>);
impl std::fmt::Debug for RenderBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RenderBatch")
    }
}
impl RenderBatch {
    /// Four targets match foundation link budget and bound retained IDs/spans.
    /// Excess rows are explicitly excluded, never confirmed by a batch revision.
    pub fn new(
        context: Option<UpdateContext>,
        targets: impl IntoIterator<Item = RenderTarget>,
    ) -> Self {
        let Some(mut context) = context.filter(UpdateContext::valid) else {
            return Self::default();
        };
        let mut omitted = false;
        let mut selected: Vec<(RenderTarget, Option<(Observation, StageObservation)>)> = Vec::new();
        for target in targets.into_iter().take(5) {
            if selected.iter().any(|(existing, _)| existing == &target) {
                continue;
            }
            if selected.len() == 4 {
                let mut overflow = StageObservation::start(Stage::RenderCoverage);
                overflow.finish(Fact::Overflow);
                omitted = true;
                break;
            }
            if target.thread.len() > 256
                || target.row.len() > 256
                || target.part.as_ref().is_some_and(|part| part.len() > 528)
                || (target.boundary == RendererBoundary::HtmlVisibleLayout && target.part.is_none())
            {
                omitted = true;
                let mut coverage = StageObservation::start(Stage::RenderCoverage);
                coverage.finish(Fact::Uncovered);
                continue;
            }
            if target.boundary == RendererBoundary::TerminalReader {
                let mut coverage = StageObservation::start(Stage::RenderCoverage);
                coverage.finish(Fact::Unsupported);
            }
            let observation =
                context.boundary(if target.boundary == RendererBoundary::TerminalReader {
                    Boundary::Delivery
                } else {
                    Boundary::Presentation
                });
            observation.attribute("update.renderer_boundary", format!("{:?}", target.boundary));
            observation.attribute("update.origin", format!("{:?}", context.origin));
            observation.attribute("update.local_content_coverage_complete", context.complete());
            observation.attribute(
                "update.receive_connection_scoped",
                context.connection.is_some(),
            );
            let metric = StageObservation::start_at(
                match target.boundary {
                    RendererBoundary::GpuiRender
                    | RendererBoundary::GpuiEmpty
                    | RendererBoundary::GpuiStructureRender => Stage::GpuiRender,
                    RendererBoundary::HtmlVisibleLayout => Stage::HtmlLayout,
                    RendererBoundary::TerminalReader => Stage::TerminalMatch,
                },
                context.epoch,
            );
            selected.push((target, Some((observation, metric))));
        }
        if omitted {
            context.incomplete();
            for (_, observation) in &selected {
                if let Some((observation, _)) = observation {
                    observation.attribute("update.local_content_coverage_complete", false);
                }
            }
        }
        if selected.is_empty() {
            let mut coverage = StageObservation::start(Stage::RenderCoverage);
            coverage.finish(Fact::Uncovered);
            return Self::default();
        }
        let (live, wait) = context.run(|| {
            let work = crate::work::WorkCarrier::independent(
                crate::work::Boundary::ClientPublication,
                crate::work::WorkKind::ClientPublication,
                crate::work::Trigger::Event,
                crate::work::ExecutorRole::DesktopForeground,
            );
            let _scope = work.enter();
            let live = crate::live::WorkGuard::start(
                work.context()
                    .filter(|work| Some(work.consent_generation) == context.epoch),
                false,
            );
            crate::live::progress(crate::live::Stage::Callback);
            let wait = crate::live::WaitGuard::new(crate::live::WaitKind::Reply);
            (live, Some(wait))
        });
        Self(Some(Arc::new(Mutex::new(RenderState {
            targets: selected,
            context,
            live,
            wait,
            validity: None,
            ending: crate::work::Outcome::Completed,
        }))))
    }
    /// Causal reference only; it carries no target completion authority.
    pub fn correlation(&self) -> Option<Correlation> {
        self.0.as_ref().and_then(|state| {
            let state = state.lock().unwrap_or_else(|p| p.into_inner());
            state.context.valid().then(|| state.context.correlation())
        })
    }
    pub fn is_pending(&self) -> bool {
        self.0.as_ref().is_some_and(|state| {
            let state = state.lock().unwrap_or_else(|p| p.into_inner());
            state.context.valid()
                && state
                    .targets
                    .iter()
                    .any(|(_, observation)| observation.is_some())
        })
    }
    pub fn covers_row(&self, thread: &str, generation: u64, row: &str, revision: u64) -> bool {
        self.0.as_ref().is_some_and(|state| {
            let state = state.lock().unwrap_or_else(|p| p.into_inner());
            state.context.valid()
                && state.targets.iter().any(|(target, observation)| {
                    observation.is_some()
                        && target.thread == thread
                        && target.generation == generation
                        && target.row == row
                        && target.revision == revision
                })
        })
    }
    pub fn authorization_generation(&self) -> Option<u64> {
        self.0.as_ref().and_then(|state| {
            state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .context
                .authorization
        })
    }
    /// Shell adapter supplies a weak-owner fence. No core/renderer owner is retained.
    pub fn require_validity(&self, valid: impl Fn() -> bool + Send + Sync + 'static) {
        if let Some(state) = &self.0 {
            let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
            if state.validity.is_none() {
                state.validity = Some(Arc::new(valid));
            }
        }
    }
    pub fn observe(&self, target: &RenderTarget, boundary: RendererBoundary) -> bool {
        self.observe_row(
            &target.thread,
            target.generation,
            &target.row,
            target.revision,
            boundary,
            target.part.as_deref(),
        )
    }
    pub fn observe_row(
        &self,
        thread: &str,
        generation: u64,
        row: &str,
        revision: u64,
        boundary: RendererBoundary,
        part: Option<&str>,
    ) -> bool {
        let Some(state) = &self.0 else {
            return false;
        };
        let valid = state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .validity
            .clone();
        if valid.as_ref().is_some_and(|valid| !valid()) {
            self.retire(false);
            return false;
        }
        let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.context.valid() {
            return false;
        }
        let Some((_, observation)) = state.targets.iter_mut().find(|(expected, _)| {
            expected.thread == thread
                && expected.generation == generation
                && expected.row == row
                && expected.revision == revision
                && expected.boundary == boundary
                && expected.part.as_deref() == part
        }) else {
            return false;
        };
        let Some((mut observation, mut metric)) = observation.take() else {
            return false;
        };
        metric.finish(Fact::Matched);
        let elapsed = state.context.elapsed();
        observation.attribute("update.local_elapsed_known", elapsed.is_some());
        if let Some(elapsed) = elapsed {
            observation.attribute("update.local_elapsed_ms", elapsed.as_secs_f64() * 1000.);
        }
        observation.finish(Outcome::Confirmed);
        if state
            .targets
            .iter()
            .all(|(_, observation)| observation.is_none())
        {
            state.wait.take();
            let ending = state.ending;
            state.live.finish(ending);
        }
        true
    }
    pub fn end(&self, target: &RenderTarget, end: RenderEnd) {
        let Some(state) = &self.0 else {
            return;
        };
        let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
        if !state.context.valid() {
            return;
        }
        let Some((_, owner)) = state
            .targets
            .iter_mut()
            .find(|(expected, _)| expected == target)
        else {
            return;
        };
        let Some((mut observation, mut metric)) = owner.take() else {
            return;
        };
        let (outcome, fact, ending) = match end {
            RenderEnd::Failed => (
                Outcome::Failed,
                Fact::Rejected,
                crate::work::Outcome::Failed,
            ),
            RenderEnd::Superseded => (
                Outcome::Superseded,
                Fact::Superseded,
                crate::work::Outcome::Abandoned,
            ),
            RenderEnd::Unconfirmed => (
                Outcome::Unconfirmed,
                Fact::Unconfirmed,
                crate::work::Outcome::Abandoned,
            ),
        };
        observation.attribute("update.renderer_outcome", format!("{end:?}"));
        observation.finish(outcome);
        metric.finish(fact);
        if state.ending != crate::work::Outcome::Failed {
            state.ending = ending;
        }
        if state.targets.iter().all(|(_, owner)| owner.is_none()) {
            state.wait.take();
            let ending = state.ending;
            state.live.finish(ending);
        }
    }
    pub fn retire(&self, superseded: bool) {
        let Some(state) = &self.0 else {
            return;
        };
        let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
        state.wait.take();
        state.live.finish(crate::work::Outcome::Abandoned);
        for (_, observation) in &mut state.targets {
            if let Some((mut observation, mut metric)) = observation.take() {
                metric.finish(if superseded {
                    Fact::Superseded
                } else {
                    Fact::Unconfirmed
                });
                observation.attribute(
                    "update.censor_reason",
                    if superseded {
                        "superseded"
                    } else {
                        "hidden_or_detached"
                    },
                );
                observation.finish(if superseded {
                    Outcome::Superseded
                } else {
                    Outcome::Unconfirmed
                });
            }
        }
    }
}

/// Local outbound recovery of the optional envelope after serialization/fanout.
/// This cannot establish receipt and never subtracts remote clocks.
pub fn outgoing(text: &str) -> Option<UpdateContext> {
    if !crate::telemetry_enabled() || carried().is_some_and(|context| !context.valid()) {
        return None;
    }
    // Unknown business fields deserialize as IgnoredAny: no payload/history
    // Value or content string is copied just to recover diagnostic identity.
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Wire<'a> {
        v: u64,
        #[serde(borrow)]
        trace: &'a str,
        #[serde(borrow)]
        span: &'a str,
        flags: u64,
        origin: u64,
        complete: bool,
    }
    #[derive(serde::Deserialize)]
    struct Params<'a> {
        #[serde(rename = "_pioneer_update", borrow)]
        wire: Option<Wire<'a>>,
    }
    #[derive(serde::Deserialize)]
    struct Notification<'a> {
        #[serde(borrow)]
        method: &'a str,
        #[serde(borrow)]
        params: Option<Params<'a>>,
    }
    let notification: Notification<'_> = serde_json::from_str(text).ok()?;
    if !selected(notification.method) {
        return None;
    }
    let mut params = serde_json::Map::new();
    if let Some(wire) = notification.params?.wire {
        if wire.trace.len() != 32 || wire.span.len() != 16 {
            return None;
        }
        params.insert(
            WIRE.into(),
            serde_json::json!({"v":wire.v, "trace":wire.trace,
            "span":wire.span, "flags":wire.flags,"origin":wire.origin,"complete":wire.complete}),
        );
    }
    let mut context = accept(
        notification.method,
        &mut serde_json::Value::Object(params),
        0,
        false,
    )
    .0?;
    if let Some(ambient) = capture() {
        let encoded = context.correlation.context();
        let encoded = encoded.span().span_context().clone();
        let local = ambient.correlation.context();
        let local = local.span().span_context().clone();
        if encoded.is_valid()
            && local.trace_id() == encoded.trace_id()
            && local.span_id() == encoded.span_id()
        {
            // Correlation proves the cause, not encoded scope/coverage. The
            // borrowed extractor intentionally has no business content hint.
            context.first = ambient.first;
            context.connection = ambient.connection;
            context.authorization = ambient.authorization;
            context.sources = ambient.sources;
            context.source_count = ambient.source_count;
            return Some(context);
        }
    }
    // The wire has no source clock or local connection identity. Recovering
    // its correlation does not recover the earlier local first-offer clock.
    context.first = None;
    context.connection = None;
    Some(context)
}

thread_local! { static NATIVE: RefCell<Option<(RenderBatch, RenderTarget)>> = const { RefCell::new(None) }; }
pub fn capture_native() -> Option<(RenderBatch, RenderTarget)> {
    NATIVE.with(|slot| slot.borrow().clone())
}
impl RenderBatch {
    pub fn with_target<T>(&self, target: RenderTarget, callback: impl FnOnce() -> T) -> T {
        struct Restore(Option<(RenderBatch, RenderTarget)>);
        impl Drop for Restore {
            fn drop(&mut self) {
                NATIVE.with(|slot| slot.replace(self.0.take()));
            }
        }
        let previous = NATIVE.with(|slot| slot.replace(Some((self.clone(), target))));
        let _restore = Restore(previous);
        let context = self.0.as_ref().map(|state| {
            state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .context
                .clone()
        });
        if let Some(context) = context {
            context.run(callback)
        } else {
            callback()
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FlushReason {
    Key,
    Item,
    Turn,
    Forced,
    Timer,
    Ordered,
    Snapshot,
}
pub fn flushed(reason: FlushReason, units: u64) {
    if !crate::telemetry_enabled() {
        return;
    }
    if let Some(flushes) = FLUSHES.get() {
        flushes.add(units, &[KeyValue::new("reason", format!("{reason:?}"))]);
    }
}

pub fn flushed_batch(reason: FlushReason, units: usize) {
    if units == 0 || !crate::telemetry_enabled() {
        return;
    }
    if let Some(batch) = FLUSH_BATCH.get() {
        batch.record(
            units as u64,
            &[KeyValue::new("reason", format!("{reason:?}"))],
        );
    }
}

/// Exact artifact identity remains local; it is never an exported dimension.
pub fn html_part(artifact: &str, version: Option<&str>) -> Option<String> {
    if artifact.len() > 256 || version.is_some_and(|v| v.len() > 256) {
        return None;
    }
    Some(match version {
        Some(version) => format!(
            "{}:{}1{}:{}",
            artifact.len(),
            artifact,
            version.len(),
            version
        ),
        None => format!("{}:{}0", artifact.len(), artifact),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work::{Boundary as WorkBoundary, ExecutorRole, WorkItem, WorkKind};
    use serde_json::json;

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
    fn recorded(callback: impl FnOnce()) -> Vec<opentelemetry_sdk::trace::SpanData> {
        let _lock = crate::telemetry::tests::TELEMETRY_TEST_LOCK.lock().unwrap();
        let old = crate::telemetry_enabled();
        crate::set_telemetry_gate(true);
        let capture = Capture::default();
        let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(capture.clone())
            .build();
        let meter = opentelemetry_sdk::metrics::SdkMeterProvider::builder().build();
        let state =
            crate::telemetry::tests::gateway_test_state_with_tracer(meter.clone(), tracer.clone());
        crate::telemetry::tests::with_test_state(state, callback);
        tracer.force_flush().unwrap();
        tracer.shutdown().unwrap();
        meter.shutdown().unwrap();
        crate::set_telemetry_gate(old);
        let result = capture.0.lock().unwrap().clone();
        result
    }
    fn target(boundary: RendererBoundary) -> RenderTarget {
        RenderTarget {
            thread: "thread".into(),
            generation: 3,
            row: "row".into(),
            revision: 7,
            boundary,
            part: None,
        }
    }
    #[test]
    fn work_handoff_preserves_source_and_mixed_batch_links_without_adopting_an_ambient_parent() {
        let mut expected = Vec::new();
        let spans = recorded(|| {
            let first =
                UpdateContext::source(ContentHint::new("thread", Some("item"), Change::Append, 4))
                    .unwrap();
            let second =
                UpdateContext::source(ContentHint::new("thread", Some("item"), Change::Append, 6))
                    .unwrap();
            expected = vec![
                first.correlation.context().span().span_context().span_id(),
                second.correlation.context().span().span_context().span_id(),
            ];
            let mut item = first.run(|| {
                WorkItem::new(
                    11,
                    WorkBoundary::EventLane,
                    WorkKind::Event,
                    ExecutorRole::Event,
                )
            });
            let next = second.run(|| {
                WorkItem::new(
                    12,
                    WorkBoundary::EventLane,
                    WorkKind::Event,
                    ExecutorRole::Event,
                )
            });
            // The late ambient source must not own either queued input.
            let unrelated = UpdateContext::source(None).unwrap();
            unrelated.run(|| item.carrier.merge_link(&next.carrier));
            let (_, work) = item.received(
                WorkBoundary::EventLane,
                WorkKind::Event,
                ExecutorRole::Event,
            );
            let joined = Correlation::default().run(|| work.run(|| capture().unwrap()));
            let mut missing = WorkItem::new(
                0,
                WorkBoundary::EventLane,
                WorkKind::Event,
                ExecutorRole::Event,
            );
            missing.carrier.merge_link(&next.carrier);
            let (_, missing) = missing.received(
                WorkBoundary::EventLane,
                WorkKind::Event,
                ExecutorRole::Event,
            );
            let missing = missing.run(|| {
                let mixed = capture().unwrap();
                assert!(mixed.hint().is_none());
                assert!(!mixed.complete());
                assert!(mixed.elapsed().is_none());
                mixed
            });
            let expected = target(RendererBoundary::GpuiRender);
            let batch = RenderBatch::new(Some(missing), [expected.clone()]);
            assert!(batch.observe(&expected, RendererBoundary::GpuiRender));
            assert_eq!(joined.hint().unwrap().bytes, 10);
            assert!(joined.complete());
            let replacement = UpdateContext::source(ContentHint::new(
                "thread",
                Some("item"),
                Change::Snapshot,
                2,
            ))
            .unwrap();
            let replaced = joined.merged(&replacement);
            assert!(!replaced.complete());
            assert_eq!(replaced.hint().unwrap().bytes, 2);
            let other =
                UpdateContext::source(ContentHint::new("other", Some("item"), Change::Append, 1))
                    .unwrap();
            assert!(replaced.merged(&other).hint().is_none());
            let mut many = joined.clone();
            for _ in 0..4 {
                let next = UpdateContext::source(ContentHint::new(
                    "thread",
                    Some("item"),
                    Change::Append,
                    1,
                ))
                .unwrap();
                many = many.merged(&next);
            }
            assert!(!many.complete());
        });
        let joined = spans
            .iter()
            .find(|span| {
                span.links.len() == 2
                    && span
                        .links
                        .iter()
                        .map(|link| link.span_context.span_id())
                        .collect::<Vec<_>>()
                        == expected
            })
            .unwrap();
        assert_eq!(joined.parent_span_id, SpanId::INVALID);
        assert!(spans.iter().all(|span| span.links.len() <= 4));
        let unknown = spans
            .iter()
            .find(|span| {
                span.attributes
                    .contains(&KeyValue::new("update.local_elapsed_known", false))
            })
            .unwrap();
        assert!(
            !unknown
                .attributes
                .iter()
                .any(|attribute| attribute.key.as_str() == "update.local_elapsed_ms")
        );
        assert!(spans.iter().any(|span| {
            span.attributes
                .contains(&KeyValue::new("links.truncated", true))
        }));
    }
    #[test]
    fn retired_plain_parent_cannot_restart_source_sdk_or_native_lineage() {
        let spans = recorded(|| {
            assert!(carried().is_none());
            let mut parent = Observation::start(
                Scenario::Update,
                Boundary::Episode,
                &Correlation::default(),
                &[],
            );
            let old = parent.correlation();
            parent.finish(Outcome::Returned);
            let input = old.run(|| UpdateContext::source(None).unwrap());
            let prepared = Correlation::independent_at(input.epoch);
            let (disabled_recapture, recaptured) = old.run(|| {
                // Keep the old valid context attached across both toggles.
                crate::set_telemetry_gate(false);
                let disabled = Correlation::capture();
                crate::set_telemetry_gate(true);
                (disabled, Correlation::capture())
            });
            for retired in [old.clone(), recaptured, disabled_recapture] {
                let source = retired.run(|| {
                    UpdateContext::source(ContentHint::new(
                        "thread",
                        Some("item"),
                        Change::Append,
                        3,
                    ))
                    .unwrap()
                });
                source.run(|| {
                    assert!(capture().is_none());
                    let mut params = json!({"thread_id":"thread","item_id":"item","delta":"abc"});
                    assert!(decorate("item/agent_message/delta", &mut params).is_none());
                    assert!(params.get(WIRE).is_none());
                });
                let item = source.run(|| {
                    WorkItem::new(
                        1,
                        WorkBoundary::EventLane,
                        WorkKind::Event,
                        ExecutorRole::Event,
                    )
                });
                let (_, work) = item.received(
                    WorkBoundary::EventLane,
                    WorkKind::Event,
                    ExecutorRole::Event,
                );
                work.run(|| {
                    assert!(capture().is_none());
                    assert!(UpdateContext::source(None).is_none());
                });
                let target = target(RendererBoundary::GpuiRender);
                assert!(
                    !RenderBatch::new(Some(source), [target.clone()])
                        .observe(&target, RendererBoundary::GpuiRender)
                );
            }
            // Prepare in E, construct after rotation: deterministic race cut,
            // without sleeps or a test-only constructor hook.
            drop(Observation::start(
                Scenario::Update,
                Boundary::Delivery,
                &prepared,
                &[],
            ));
            let mut fresh = Observation::start(
                Scenario::Update,
                Boundary::Episode,
                &Correlation::default(),
                &[],
            );
            fresh.finish(Outcome::Returned);
            let mut optional = Observation::start(
                Scenario::Update,
                Boundary::Delivery,
                &Correlation::default(),
                &[old],
            );
            optional.finish(Outcome::Returned);
            let mut params = json!({"thread_id":"thread","item_id":"item","delta":"abc","_pioneer_update":{
                "v":1,"trace":"11111111111111111111111111111111","span":"2222222222222222","flags":1,"origin":0,"complete":true}});
            let (remote, peer) = receive("item/agent_message/delta", &mut params, 19);
            assert_eq!(peer, Peer::Valid);
            remote
                .unwrap()
                .boundary(Boundary::Delivery)
                .finish(Outcome::Returned);
            let fresh = UpdateContext::source(None).unwrap();
            for retired in [
                input.merged(&fresh),
                fresh.merged(&input),
                input.mixed_missing(),
            ] {
                let target = target(RendererBoundary::GpuiRender);
                assert!(
                    !RenderBatch::new(Some(retired), [target.clone()])
                        .observe(&target, RendererBoundary::GpuiRender)
                );
            }
            let target = target(RendererBoundary::GpuiRender);
            assert!(
                RenderBatch::new(Some(fresh), [target.clone()])
                    .observe(&target, RendererBoundary::GpuiRender)
            );
        });
        assert_eq!(
            spans.len(),
            7,
            "only pre-rotation ended producers and explicit fresh controls export, never stale SDK Drop"
        );
        assert_eq!(
            spans
                .iter()
                .filter(|span| span
                    .attributes
                    .contains(&KeyValue::new("outcome", "Confirmed")))
                .count(),
            1
        );
        assert!(spans.iter().all(|span| span.links.is_empty()));
    }

    #[test]
    fn optional_notification_metadata_is_bounded_stripped_and_independent_of_startup_and_rpc() {
        recorded(|| {
            let wire = json!({"v":1,"trace":"11111111111111111111111111111111","span":"2222222222222222","flags":1,"origin":0,"complete":true});
            for (metadata, expected) in [
                (Some(wire.clone()), Peer::Valid),
                (None, Peer::Missing),
                (Some(json!({"v":2})), Peer::Invalid),
                (Some(json!({"v":1,"trace":"0".repeat(4096)})), Peer::Invalid),
            ] {
                let mut params = json!({"thread_id":"thread","item_id":"item","delta":"abc","_pioneer_telemetry":{"sentinel":true},"_pioneer_correlation":{"sentinel":true}});
                if let Some(metadata) = metadata {
                    params
                        .as_object_mut()
                        .unwrap()
                        .insert(WIRE.into(), metadata);
                }
                let (context, peer) = receive("item/agent_message/delta", &mut params, 17);
                assert_eq!(peer, expected);
                assert!(params.get(WIRE).is_none());
                assert_eq!(params["delta"], "abc");
                assert_eq!(params["_pioneer_telemetry"]["sentinel"], true);
                assert_eq!(params["_pioneer_correlation"]["sentinel"], true);
                let context = context.unwrap();
                if expected != Peer::Valid {
                    assert!(
                        context
                            .correlation
                            .context()
                            .span()
                            .span_context()
                            .is_valid(),
                        "missing remote edge still needs a real independent local ingress root"
                    );
                    assert!(!context.complete());
                }
                let mut encoded = params.clone();
                context.run(|| {
                    decorate("item/agent_message/delta", &mut encoded);
                });
                if expected == Peer::Valid {
                    assert_eq!(encoded[WIRE].as_object().unwrap().len(), 6);
                    let frame = json!({"jsonrpc":"2.0", "method":"item/agent_message/delta",
                        "params":encoded})
                    .to_string();
                    let unrelated = UpdateContext::source(None).unwrap();
                    let recovered = unrelated
                        .run(|| outgoing(&frame))
                        .expect("serialized source must win over unrelated late ambient context");
                    assert!(recovered.elapsed().is_none());
                    assert_eq!(
                        recovered
                            .correlation
                            .context()
                            .span()
                            .span_context()
                            .trace_id(),
                        TraceId::from_hex("11111111111111111111111111111111").unwrap()
                    );
                    assert!(outgoing(r#"{"jsonrpc":"2.0","id":"response","result":{}}"#).is_none());
                }
            }
            let mut removal = json!({"threadId":"thread","removedBlockIds":["removed"]});
            let (removal, peer) = receive("thread/timeline/blocks/changed", &mut removal, 17);
            assert_eq!(peer, Peer::Missing);
            assert_eq!(removal.unwrap().hint().unwrap().change, Change::Removed);
            let replay = UpdateContext::source_with_origin(None, Origin::Replay).unwrap();
            let mut params = json!({"thread_id":"thread","item_id":"item","delta":"abc"});
            replay.run(|| {
                decorate("item/agent_message/delta", &mut params);
            });
            assert_eq!(params[WIRE]["origin"], 1);
            assert_eq!(
                receive("item/agent_message/delta", &mut params, 17)
                    .0
                    .unwrap()
                    .origin,
                Origin::Replay
            );
            let source =
                UpdateContext::source(ContentHint::new("first", Some("item"), Change::Append, 3))
                    .unwrap();
            let mut different = json!({"thread_id":"other","item_id":"item","delta":"abc"});
            source.run(|| {
                decorate("item/agent_message/delta", &mut different);
            });
            assert_eq!(
                different[WIRE]["complete"], false,
                "projection to another content scope cannot preserve complete coverage"
            );
            assert!(
                source.complete(),
                "fixture starts with known source coverage"
            );
            source.run(|| {
                for mut params in [
                    json!({"thread_id":"other","item_id":"item","delta":"abc"}),
                    json!({"thread_id":"first","item_id":"item","delta":"abc","payload":{"truncated":true}}),
                    json!({"thread_id":"first","item_id":"item","delta":"abc","markdown":{}}),
                    json!({"thread_id":"first","item_id":"item","delta":"abc"}),
                ] {
                    decorate("item/agent_message/delta", &mut params);
                    let wire_complete = params[WIRE]["complete"].as_bool().unwrap();
                    let frame = json!({"method":"item/agent_message/delta","params":params}).to_string();
                    let local = outgoing(&frame).unwrap();
                    assert!(local.elapsed().is_some(), "matching cause retains its local clock");
                    assert!(!local.complete() || wire_complete, "local recovery cannot raise encoded coverage");
                    assert!(local.hint().is_none(), "ignored payload does not prove encoded scope");
                }
            });
            crate::set_telemetry_gate(false);
            let mut params = json!({"thread_id":"thread","_pioneer_update":wire});
            assert_eq!(
                receive("item/agent_message/delta", &mut params, 17).1,
                Peer::Disabled
            );
            assert!(params.get(WIRE).is_none());
            crate::set_telemetry_gate(true);
        });
    }
    #[test]
    fn renderer_requires_exact_identity_boundary_and_live_fence_and_finishes_only_once() {
        let spans = recorded(|| {
            let source = UpdateContext::source(None).unwrap();
            let expected = target(RendererBoundary::GpuiRender);
            let batch = RenderBatch::new(Some(source.clone()), [expected.clone()]);
            let mut wrong = expected.clone();
            wrong.revision += 1;
            assert!(!batch.observe(&wrong, RendererBoundary::GpuiRender));
            wrong = expected.clone();
            wrong.generation += 1;
            assert!(!batch.observe(&wrong, RendererBoundary::GpuiRender));
            wrong = expected.clone();
            wrong.thread = "other".into();
            assert!(!batch.observe(&wrong, RendererBoundary::GpuiRender));
            assert!(!batch.observe(&expected, RendererBoundary::HtmlVisibleLayout));
            assert!(batch.observe(&expected, RendererBoundary::GpuiRender));
            assert!(!batch.observe(&expected, RendererBoundary::GpuiRender));
            let hidden = RenderBatch::new(Some(source.clone()), [expected.clone()]);
            hidden.require_validity(|| false);
            assert!(!hidden.observe(&expected, RendererBoundary::GpuiRender));
            let superseded = RenderBatch::new(Some(source.clone()), [expected.clone()]);
            superseded.retire(true);
            assert!(!superseded.observe(&expected, RendererBoundary::GpuiRender));
            let failed = RenderBatch::new(Some(source.clone()), [expected.clone()]);
            failed.end(&expected, RenderEnd::Failed);
            assert!(!failed.observe(&expected, RendererBoundary::GpuiRender));
            let delayed = source.run(|| {
                WorkItem::new(
                    1,
                    WorkBoundary::EventLane,
                    WorkKind::Event,
                    ExecutorRole::Event,
                )
            });
            let late = RenderBatch::new(Some(source.clone()), [expected.clone()]);
            let mut empty = target(RendererBoundary::GpuiEmpty);
            empty.row.clear();
            let empty_batch = RenderBatch::new(Some(source.clone()), [empty.clone()]);
            assert!(!empty_batch.observe(&expected, RendererBoundary::GpuiRender));
            let mut old_empty = empty.clone();
            old_empty.revision -= 1;
            assert!(!empty_batch.observe(&old_empty, RendererBoundary::GpuiEmpty));
            assert!(empty_batch.observe(&empty, RendererBoundary::GpuiEmpty));
            crate::set_telemetry_gate(false);
            let disabled_descendant = source.run(|| {
                WorkItem::new(
                    3,
                    WorkBoundary::EventLane,
                    WorkKind::Event,
                    ExecutorRole::Event,
                )
            });
            crate::set_telemetry_gate(true);
            let (_, disabled_work) = disabled_descendant.received(
                WorkBoundary::EventLane,
                WorkKind::Event,
                ExecutorRole::Event,
            );
            disabled_work.run(|| assert!(UpdateContext::source(None).is_none()));
            let fresh = UpdateContext::source(None).unwrap();
            source.run(|| {
                assert!(
                    UpdateContext::source_for_producer(None, &fresh.correlation()).is_none(),
                    "an explicit producer cannot erase a carried retired UPDATE marker"
                );
            });
            fresh.run(|| {
                let retired =
                    UpdateContext::source_for_producer(None, &source.correlation()).unwrap();
                retired.run(|| {
                    assert!(
                        capture().is_none(),
                        "fresh optional update links cannot refresh producer provenance"
                    );
                    let mut params = json!({"threadId":"thread","turnId":"turn"});
                    assert!(decorate("turn/completed", &mut params).is_none());
                    assert!(params.get(WIRE).is_none());
                });
            });
            for mixed in [
                source.merged(&fresh),
                fresh.merged(&source),
                source.mixed_missing(),
            ] {
                let batch = RenderBatch::new(Some(mixed), [expected.clone()]);
                assert!(!batch.observe(&expected, RendererBoundary::GpuiRender));
            }
            assert!(!late.observe(&expected, RendererBoundary::GpuiRender));
            let (_, work) = delayed.received(
                WorkBoundary::EventLane,
                WorkKind::Event,
                ExecutorRole::Event,
            );
            let descendant = work.run(|| {
                assert!(capture().is_none());
                assert!(UpdateContext::source(None).is_none());
                WorkItem::new(
                    2,
                    WorkBoundary::EventLane,
                    WorkKind::Event,
                    ExecutorRole::Event,
                )
            });
            let (_, work) = descendant.received(
                WorkBoundary::EventLane,
                WorkKind::Event,
                ExecutorRole::Event,
            );
            work.run(|| assert!(UpdateContext::source(None).is_none()));
            assert!(UpdateContext::source(None).is_some());
        });
        let rendered = spans
            .iter()
            .filter(|span| {
                span.attributes
                    .contains(&KeyValue::new("update.renderer_boundary", "GpuiRender"))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rendered
                .iter()
                .filter(|span| span
                    .attributes
                    .contains(&KeyValue::new("outcome", "Confirmed")))
                .count(),
            1
        );
        assert!(rendered.iter().any(|span| {
            span.attributes
                .contains(&KeyValue::new("outcome", "Superseded"))
        }));
        assert!(rendered.iter().any(|span| {
            span.attributes
                .contains(&KeyValue::new("outcome", "Unconfirmed"))
        }));
        assert!(rendered.iter().any(|span| {
            span.attributes
                .contains(&KeyValue::new("outcome", "Failed"))
        }));
    }
    #[test]
    fn html_part_and_bounded_overflow_cannot_confirm_another_document_or_omitted_target() {
        recorded(|| {
            let source = UpdateContext::source(None).unwrap();
            let targets = (0..6)
                .map(|i| {
                    let mut target = target(RendererBoundary::HtmlVisibleLayout);
                    target.part = Some(format!("artifact:{i}"));
                    target
                })
                .collect::<Vec<_>>();
            let batch = RenderBatch::new(Some(source), targets.clone());
            assert!(!batch.observe(&targets[4], RendererBoundary::HtmlVisibleLayout));
            assert!(!batch.observe(&targets[5], RendererBoundary::HtmlVisibleLayout));
            assert!(batch.observe(&targets[0], RendererBoundary::HtmlVisibleLayout));
            assert!(!batch.observe(&targets[0], RendererBoundary::HtmlVisibleLayout));
            batch.retire(false);
            assert!(!batch.observe(&targets[1], RendererBoundary::HtmlVisibleLayout));
        });
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DeliveryOutcome {
    Full,
    Disconnected,
    Oversized,
    Written,
    WriteFailed,
    Admitted,
    Discarded,
    Reauthorize,
    Unavailable,
}
pub fn finish_delivery(observation: &mut Observation, outcome: DeliveryOutcome) {
    observation.attribute("update.delivery_outcome", format!("{outcome:?}"));
    observation.finish(match outcome {
        DeliveryOutcome::Written | DeliveryOutcome::Admitted => Outcome::Returned,
        DeliveryOutcome::Disconnected | DeliveryOutcome::WriteFailed => Outcome::TransportLost,
        DeliveryOutcome::Full
        | DeliveryOutcome::Oversized
        | DeliveryOutcome::Discarded
        | DeliveryOutcome::Reauthorize
        | DeliveryOutcome::Unavailable => Outcome::Unconfirmed,
    });
}

#[derive(Clone, Copy, Debug)]
pub enum RenderEnd {
    Failed,
    Superseded,
    Unconfirmed,
}
