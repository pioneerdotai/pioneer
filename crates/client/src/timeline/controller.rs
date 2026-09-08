//! Process-local semantic timeline demand and request policy. No shell geometry.
use std::collections::{HashMap, HashSet};

use super::semantic::{
    self, SemanticTimelineRequestAction as PageAction, SemanticTimelineRequestKey as PageKey,
};

pub const WORK_ITEM_CHUNK_LIMIT: usize = 200;
const MAX_ATTEMPTS: u8 = 3;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimelineDemand {
    pub thread_id: String,
    pub consumer_id: String,
    pub generation: u64,
    pub source_revision: u64,
    pub row_ids: Vec<String>,
    pub threshold: usize,
    pub before: bool,
    pub after: bool,
    pub work: bool,
    pub presented_rows: bool,
    pub scroll_generation: u64,
    pub latest_user_turn_id: Option<String>,
    pub viewed_through_turn_id: Option<String>,
    #[serde(default)]
    pub read_requires_unread: bool,
    pub prefetch_on_visibility: bool,
    pub boundary_request_limit: usize,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimelineIntent {
    ConsumeScroll {
        thread_id: String,
        consumer_id: String,
        generation: u64,
        scroll_generation: u64,
    },
    Update {
        demand: TimelineDemand,
    },
    Exit {
        thread_id: String,
        consumer_id: String,
        generation: u64,
    },
    Retry {
        thread_id: String,
        consumer_id: String,
        generation: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryState {
    Loading,
    Ready,
    Backoff { retry_at_ms: u64 },
    Terminal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelineReadRequest {
    pub thread_id: String,
    pub through_turn_id: String,
    pub generation: u64,
}

#[derive(Clone)]
struct ReadState {
    request: TimelineReadRequest,
    attempts: u8,
    state: RetryState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TimelineEffectPlan {
    pub pages: Vec<TimelinePageRequest>,
    pub reads: Vec<TimelineReadRequest>,
    pub cancelled: Vec<PageKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelinePageRequest {
    pub action: PageAction,
    pub generation: u64,
}

#[derive(Default)]
pub struct ThreadTimelineController {
    pub(crate) in_flight: HashSet<PageKey>,
    pub(crate) pending: HashMap<PageKey, PageAction>,
    requests: HashMap<PageKey, (u64, PageAction)>,
    failures: HashMap<PageKey, (u8, u64)>,
    demands: HashMap<(String, String), TimelineDemand>,
    retired: HashMap<(String, String), u64>,
    demand_pages: HashMap<(String, String), HashSet<PageKey>>,
    scoped_pages: HashSet<PageKey>,
    durable_pages: HashSet<PageKey>,
    topology: HashMap<(String, String), Vec<String>>,
    consumed_scroll: HashMap<(String, String), u64>,
    reads: HashMap<(String, String), ReadState>,
    clock: u64,
}

impl ThreadTimelineController {
    pub(crate) fn retain_domain_request(&mut self, key: &PageKey) {
        self.durable_pages.insert(key.clone());
    }
    fn reconcile_topology(&mut self, demand: &TimelineDemand, rows: Vec<String>) {
        let id = (demand.thread_id.clone(), demand.consumer_id.clone());
        if self
            .retired
            .get(&id)
            .is_some_and(|generation| *generation >= demand.generation)
            || self
                .demands
                .get(&id)
                .is_some_and(|previous| previous.generation > demand.generation)
        {
            return;
        }
        if self
            .topology
            .get(&id)
            .is_some_and(|previous| previous != &rows)
        {
            self.consumed_scroll
                .insert(id.clone(), demand.scroll_generation);
        }
        self.topology.insert(id, rows);
    }

    fn consume_scroll(
        &mut self,
        thread: String,
        consumer: String,
        generation: u64,
        scroll_generation: u64,
    ) {
        let id = (thread, consumer);
        if self
            .demands
            .get(&id)
            .is_some_and(|d| d.generation == generation)
        {
            self.consumed_scroll
                .entry(id)
                .and_modify(|value| *value = (*value).max(scroll_generation))
                .or_insert(scroll_generation);
        }
    }
    pub(crate) fn begin(&mut self, action: PageAction) -> Option<PageAction> {
        let key = semantic::semantic_timeline_request_key(&action).clone();
        let action = semantic::enqueue_semantic_timeline_request(
            &mut self.in_flight,
            &mut self.pending,
            action,
        )?;
        self.clock = self.clock.saturating_add(1);
        self.requests.insert(key, (self.clock, action.clone()));
        Some(action)
    }

    pub(crate) fn request_generation(&self, key: &PageKey) -> u64 {
        self.requests.get(key).map_or(0, |r| r.0)
    }

    pub(crate) fn accepts(&self, key: &PageKey, generation: u64) -> bool {
        self.requests.get(key).is_some_and(|r| r.0 == generation) && self.in_flight.contains(key)
    }

    pub(crate) fn finish(
        &mut self,
        key: &PageKey,
        succeeded: bool,
        now_ms: u64,
    ) -> Option<PageAction> {
        self.requests.remove(key)?;
        if succeeded {
            self.failures.remove(key);
        } else {
            let failure = self.failures.entry(key.clone()).or_default();
            failure.0 = failure.0.saturating_add(1);
            failure.1 = now_ms.saturating_add(backoff(failure.0));
        }
        if !succeeded && self.scoped_pages.contains(key) && !self.durable_pages.contains(key) {
            self.pending.remove(key);
        }
        let next =
            semantic::finish_semantic_timeline_request(&mut self.in_flight, &mut self.pending, key);
        if next.is_none() {
            self.durable_pages.remove(key);
        }
        next
    }

    pub(crate) fn update(
        &mut self,
        demand: TimelineDemand,
        actions: Vec<PageAction>,
        now_ms: u64,
    ) -> TimelineEffectPlan {
        let id = (demand.thread_id.clone(), demand.consumer_id.clone());
        let mut plan = TimelineEffectPlan::default();
        if self
            .retired
            .get(&id)
            .is_some_and(|g| *g >= demand.generation)
            || self
                .demands
                .get(&id)
                .is_some_and(|old| old.generation > demand.generation || old == &demand)
        {
            return plan;
        }
        let boundary =
            demand.scroll_generation > self.consumed_scroll.get(&id).copied().unwrap_or(0);
        let previous_keys = self.demand_pages.remove(&id).unwrap_or_default();
        let mut keys = HashSet::new();
        let mut consumed = 0;
        for action in actions {
            let key = semantic::semantic_timeline_request_key(&action).clone();
            let is_boundary = matches!(
                key,
                PageKey::ThreadBefore { .. }
                    | PageKey::ThreadAfter { .. }
                    | PageKey::TurnWorkBefore { .. }
                    | PageKey::TurnWorkAfter { .. }
            );
            if is_boundary
                && (!boundary || consumed >= demand.boundary_request_limit)
                && !previous_keys.contains(&key)
                && !self.in_flight.contains(&key)
            {
                continue;
            }
            keys.insert(key.clone());
            if !self.in_flight.contains(&key) {
                self.scoped_pages.insert(key.clone());
            }
            if (!demand.prefetch_on_visibility && !boundary)
                || (is_boundary && (!boundary || consumed >= demand.boundary_request_limit))
            {
                continue;
            }
            if self
                .failures
                .get(&key)
                .is_some_and(|(attempt, deadline)| *attempt >= MAX_ATTEMPTS || now_ms < *deadline)
            {
                continue;
            }
            if !self.in_flight.contains(&key) {
                consumed += usize::from(is_boundary);
                if let Some(action) = self.begin(action) {
                    plan.pages.push(TimelinePageRequest {
                        generation: self.request_generation(&key),
                        action,
                    });
                }
            } else if let PageAction::TurnWorkItemsGet { params, .. } = &action {
                let mut missing = params.clone();
                if let Some((_, PageAction::TurnWorkItemsGet { params: active, .. })) =
                    self.requests.get(&key)
                {
                    missing
                        .work_item_ids
                        .retain(|id| !active.work_item_ids.contains(id));
                }
                if !missing.work_item_ids.is_empty() {
                    self.begin(PageAction::TurnWorkItemsGet {
                        key,
                        params: missing,
                    });
                }
            }
        }
        self.demand_pages.insert(id.clone(), keys);
        if consumed > 0 || (!demand.prefetch_on_visibility && boundary) {
            self.consumed_scroll
                .insert(id.clone(), demand.scroll_generation);
        }
        if let Some(turn) = demand
            .viewed_through_turn_id
            .as_ref()
            .filter(|turn| Some(*turn) == demand.latest_user_turn_id.as_ref())
        {
            let read_key = (demand.thread_id.clone(), turn.clone());
            if !self.reads.contains_key(&read_key) {
                self.clock = self.clock.saturating_add(1);
                let request = TimelineReadRequest {
                    thread_id: read_key.0.clone(),
                    through_turn_id: turn.clone(),
                    generation: self.clock,
                };
                self.reads.insert(
                    read_key,
                    ReadState {
                        request: request.clone(),
                        attempts: 1,
                        state: RetryState::Loading,
                    },
                );
                plan.reads.push(request);
            }
        }
        self.demands.insert(id, demand);
        plan.cancelled = self.cancel_unowned();
        plan
    }

    pub(crate) fn exit(
        &mut self,
        thread: &str,
        consumer: &str,
        generation: u64,
    ) -> TimelineEffectPlan {
        let id = (thread.to_owned(), consumer.to_owned());
        if self
            .demands
            .get(&id)
            .is_some_and(|d| d.generation > generation)
        {
            return TimelineEffectPlan::default();
        }
        self.retired
            .entry(id.clone())
            .and_modify(|g| *g = (*g).max(generation))
            .or_insert(generation);
        self.demands.remove(&id);
        self.demand_pages.remove(&id);
        self.topology.remove(&id);
        self.consumed_scroll.remove(&id);
        TimelineEffectPlan {
            cancelled: self.cancel_unowned(),
            ..Default::default()
        }
    }

    fn cancel_unowned(&mut self) -> Vec<PageKey> {
        let mut cancelled = self
            .scoped_pages
            .iter()
            .filter(|key| {
                !self.durable_pages.contains(*key)
                    && !self.demand_pages.values().any(|keys| keys.contains(*key))
            })
            .cloned()
            .collect::<Vec<_>>();
        cancelled.sort_by_key(|key| format!("{key:?}"));
        let mut active = Vec::new();
        for key in &cancelled {
            if self.in_flight.remove(key) {
                active.push(key.clone());
            }
            self.pending.remove(key);
            self.requests.remove(key);
            self.durable_pages.remove(key);
            self.scoped_pages.remove(key);
        }
        self.reads.retain(|(thread, turn), _| {
            self.demands
                .values()
                .any(|d| &d.thread_id == thread && d.viewed_through_turn_id.as_ref() == Some(turn))
        });
        active
    }

    pub(crate) fn accepts_read(&self, request: &TimelineReadRequest) -> bool {
        self.reads
            .get(&(request.thread_id.clone(), request.through_turn_id.clone()))
            .is_some_and(|r| r.request == *request && r.state == RetryState::Loading)
    }

    pub(crate) fn finish_read(
        &mut self,
        request: &TimelineReadRequest,
        succeeded: bool,
        now_ms: u64,
    ) -> bool {
        if !self.accepts_read(request) {
            return false;
        }
        let read = self
            .reads
            .get_mut(&(request.thread_id.clone(), request.through_turn_id.clone()))
            .unwrap();
        read.state = if succeeded {
            RetryState::Ready
        } else if read.attempts >= MAX_ATTEMPTS {
            RetryState::Terminal
        } else {
            RetryState::Backoff {
                retry_at_ms: now_ms.saturating_add(backoff(read.attempts)),
            }
        };
        true
    }

    pub(crate) fn due_reads(&mut self, now_ms: u64) -> Vec<TimelineReadRequest> {
        let mut requests = Vec::new();
        let mut keys = self.reads.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        for key in keys {
            let read = self.reads.get_mut(&key).unwrap();
            if matches!(read.state, RetryState::Backoff { retry_at_ms } if retry_at_ms <= now_ms) {
                read.attempts += 1;
                self.clock = self.clock.saturating_add(1);
                read.request.generation = self.clock;
                read.state = RetryState::Loading;
                requests.push(read.request.clone());
            }
        }
        requests
    }

    pub(crate) fn next_read_deadline(&self) -> Option<u64> {
        self.reads
            .values()
            .filter_map(|r| match r.state {
                RetryState::Backoff { retry_at_ms } => Some(retry_at_ms),
                _ => None,
            })
            .min()
    }

    pub(crate) fn retry(
        &mut self,
        thread: &str,
        consumer: &str,
        generation: u64,
    ) -> Option<TimelineDemand> {
        let id = (thread.to_owned(), consumer.to_owned());
        let demand = self
            .demands
            .get(&id)
            .filter(|d| d.generation == generation)?
            .clone();
        self.reads.retain(|(id, turn), r| {
            id != thread
                || demand.viewed_through_turn_id.as_ref() != Some(turn)
                || r.state != RetryState::Terminal
        });
        if let Some(keys) = self.demand_pages.get(&id) {
            self.failures.retain(|key, _| !keys.contains(key));
        }
        self.demands.remove(&id);
        self.consumed_scroll
            .insert(id, demand.scroll_generation.saturating_sub(1));
        Some(demand)
    }

    pub(crate) fn invalidate(&mut self, thread: Option<&str>) -> Vec<PageKey> {
        let keys = self
            .in_flight
            .iter()
            .filter(|key| thread.is_none_or(|id| request_thread_id(key) == id))
            .cloned()
            .collect::<Vec<_>>();
        for key in &keys {
            self.in_flight.remove(key);
            self.pending.remove(key);
            self.requests.remove(key);
            self.durable_pages.remove(key);
        }
        let ids = self
            .demands
            .keys()
            .filter(|(id, _)| thread.is_none_or(|t| id == t))
            .cloned()
            .collect::<Vec<_>>();
        for id in ids {
            let demand = self.demands.remove(&id).unwrap();
            self.retired.insert(id.clone(), demand.generation);
            self.demand_pages.remove(&id);
            self.topology.remove(&id);
            self.consumed_scroll.remove(&id);
        }
        self.cancel_unowned();
        self.failures
            .retain(|key, _| thread.is_some_and(|id| request_thread_id(key) != id));
        keys
    }
}

pub(crate) fn request_thread_id(key: &PageKey) -> &str {
    match key {
        PageKey::ThreadNewest { thread_id }
        | PageKey::ThreadBefore { thread_id, .. }
        | PageKey::ThreadAfter { thread_id, .. }
        | PageKey::TurnWorkInitial { thread_id, .. }
        | PageKey::TurnWorkBefore { thread_id, .. }
        | PageKey::TurnWorkAfter { thread_id, .. }
        | PageKey::TurnWorkItems { thread_id, .. } => thread_id,
    }
}
fn backoff(attempt: u8) -> u64 {
    1_000_u64.saturating_mul(1 << attempt.min(5).saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn demand(consumer: &str) -> TimelineDemand {
        TimelineDemand {
            thread_id: "a".into(),
            consumer_id: consumer.into(),
            generation: 1,
            source_revision: 1,
            row_ids: vec!["row".into()],
            threshold: 3,
            before: true,
            after: true,
            work: true,
            presented_rows: false,
            scroll_generation: 1,
            latest_user_turn_id: Some("turn".into()),
            viewed_through_turn_id: Some("turn".into()),
            read_requires_unread: false,
            prefetch_on_visibility: true,
            boundary_request_limit: 1,
        }
    }
    fn page() -> PageAction {
        PageAction::ThreadTimelinePage {
            key: PageKey::ThreadNewest {
                thread_id: "a".into(),
            },
            params: pioneer_protocol::ThreadTimelinePageParams {
                thread_id: "a".into(),
                anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                limit: Some(12),
            },
        }
    }
    #[test]
    fn coalesced_consumers_exit_independently_and_late_results_are_noop() {
        let mut policy = ThreadTimelineController::default();
        let first = policy.update(demand("desktop"), vec![page()], 0);
        assert_eq!(first.pages.len(), 1);
        assert_eq!(first.reads.len(), 1);
        assert_eq!(
            policy.update(demand("desktop"), vec![page()], 0),
            TimelineEffectPlan::default()
        );
        assert!(
            policy
                .update(demand("mobile"), vec![page()], 0)
                .pages
                .is_empty()
        );
        assert!(policy.exit("a", "desktop", 1).cancelled.is_empty());
        let key = semantic::semantic_timeline_request_key(&first.pages[0].action);
        assert!(policy.accepts(key, first.pages[0].generation));
        assert_eq!(policy.exit("a", "mobile", 1).cancelled, vec![key.clone()]);
        assert!(!policy.accepts(key, first.pages[0].generation));
        assert!(!policy.finish_read(&first.reads[0], true, 0));
        assert_eq!(
            policy.update(demand("mobile"), vec![page()], 0),
            TimelineEffectPlan::default()
        );
        let mut reentered = demand("mobile");
        reentered.generation = 2;
        let next = policy.update(reentered, vec![page()], 0);
        assert!(next.pages[0].generation > first.pages[0].generation);
    }
    #[test]
    fn persistent_read_failure_stops_at_three_attempts_until_explicit_retry() {
        let mut policy = ThreadTimelineController::default();
        let first = policy.update(demand("desktop"), vec![], 0).reads.remove(0);
        assert!(policy.finish_read(&first, false, 0));
        assert!(!policy.finish_read(&first, false, 0));
        assert!(policy.due_reads(999).is_empty());
        let second = policy.due_reads(1_000).remove(0);
        assert!(policy.finish_read(&second, false, 1_000));
        assert!(policy.due_reads(2_999).is_empty());
        let third = policy.due_reads(3_000).remove(0);
        assert!(policy.finish_read(&third, false, 3_000));
        assert!(policy.due_reads(u64::MAX).is_empty());
        assert_eq!(
            policy.reads[&("a".into(), "turn".into())].state,
            RetryState::Terminal
        );
        assert!(
            policy
                .update(demand("desktop"), vec![], u64::MAX)
                .reads
                .is_empty()
        );
        let retry = policy.retry("a", "desktop", 1).unwrap();
        assert_eq!(policy.update(retry, vec![], 4_000).reads.len(), 1);
    }
    #[test]
    fn prefetch_failure_requires_changed_demand_and_is_bounded() {
        let mut policy = ThreadTimelineController::default();
        let mut input = demand("desktop");
        for attempt in 0..3 {
            input.scroll_generation += 1;
            let plan = policy.update(input.clone(), vec![page()], attempt * 10_000);
            assert_eq!(plan.pages.len(), 1);
            let key = semantic::semantic_timeline_request_key(&plan.pages[0].action);
            policy.finish(key, false, attempt * 10_000);
            assert!(
                policy
                    .update(input.clone(), vec![page()], u64::MAX)
                    .pages
                    .is_empty()
            );
        }
        input.scroll_generation += 1;
        assert!(
            policy
                .update(input, vec![page()], u64::MAX)
                .pages
                .is_empty()
        );
    }
    #[test]
    fn wire_semantic_inputs_and_direct_inputs_produce_identical_plans() {
        let input = TimelineIntent::Update {
            demand: demand("viewport"),
        };
        let wire = serde_json::to_value(&input).unwrap();
        let text = wire.to_string();
        for forbidden in ["pixels", "offset", "width", "theme", "bounds"] {
            assert!(!text.contains(forbidden));
        }
        let TimelineIntent::Update { demand: decoded } = serde_json::from_value(wire).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            ThreadTimelineController::default().update(demand("viewport"), vec![page()], 0),
            ThreadTimelineController::default().update(decoded, vec![page()], 0)
        );
    }
    #[test]
    fn invalidating_one_thread_preserves_the_other_and_rejects_old_generations() {
        let mut policy = ThreadTimelineController::default();
        let a = policy.update(demand("viewport"), vec![page()], 0);
        let mut b = demand("viewport");
        b.thread_id = "b".into();
        let b = policy.update(b, vec![], 0);
        policy.invalidate(Some("a"));
        assert!(!policy.accepts_read(&a.reads[0]));
        assert!(policy.accepts_read(&b.reads[0]));
        policy.invalidate(None);
        assert!(!policy.accepts_read(&b.reads[0]));
        assert!(policy.due_reads(u64::MAX).is_empty());
    }
    #[test]
    fn page_arrival_consumes_loading_gestures_without_consuming_future_gestures() {
        let mut policy = ThreadTimelineController::default();
        let mut input = demand("viewport");
        policy.reconcile_topology(&input, vec!["row".into()]);
        policy.update(input.clone(), vec![], 0);
        input.scroll_generation = 2;
        policy.reconcile_topology(&input, vec!["older".into(), "row".into()]);
        assert_eq!(policy.consumed_scroll[&("a".into(), "viewport".into())], 2);
        input.scroll_generation = 3;
        policy.reconcile_topology(&input, vec!["older".into(), "row".into()]);
        assert_eq!(policy.consumed_scroll[&("a".into(), "viewport".into())], 2);
    }
    #[test]
    fn exit_after_completed_page_does_not_reset_published_ready_status() {
        let mut policy = ThreadTimelineController::default();
        let plan = policy.update(demand("viewport"), vec![page()], 0);
        policy.finish(
            semantic::semantic_timeline_request_key(&plan.pages[0].action),
            true,
            0,
        );
        assert!(policy.exit("a", "viewport", 1).cancelled.is_empty());
    }
}

impl crate::core::ClientCore {
    pub(crate) fn timeline_now_ms(&self) -> u64 {
        self.timeline_started
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }

    /// Applies normalized shell input using the same policy for direct Rust and FFI.
    pub fn timeline_intent(&self, intent: TimelineIntent) -> TimelineEffectPlan {
        let plan = self.plan_timeline_intent(intent, self.timeline_now_ms());
        self.publish_timeline_cancellation(&plan.cancelled);
        for page in &plan.pages {
            self.schedule_planned_timeline_request(page.clone());
        }
        if let Some(sender) = self
            .thread_request_sender
            .lock()
            .expect("thread sender poisoned")
            .as_ref()
        {
            for read in &plan.reads {
                if sender
                    .send(crate::threads::registry::ThreadControllerRequest::Read(
                        read.clone(),
                    ))
                    .is_err()
                {
                    self.thread_registry
                        .lock()
                        .expect("thread registry poisoned")
                        .timeline
                        .finish_read(read, false, self.timeline_now_ms());
                }
            }
        } else {
            for read in &plan.reads {
                let mut registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                if let Some(state) = registry
                    .timeline
                    .reads
                    .get_mut(&(read.thread_id.clone(), read.through_turn_id.clone()))
                {
                    state.state = RetryState::Terminal;
                }
            }
        }
        plan
    }

    /// Deterministic clock/effect seam; callers execute only the returned immutable plan.
    pub fn plan_timeline_intent(&self, intent: TimelineIntent, now_ms: u64) -> TimelineEffectPlan {
        if self.is_stopped() {
            return TimelineEffectPlan::default();
        }
        let mut demand = match intent {
            TimelineIntent::ConsumeScroll {
                thread_id,
                consumer_id,
                generation,
                scroll_generation,
            } => {
                self.thread_registry
                    .lock()
                    .expect("thread registry poisoned")
                    .timeline
                    .consume_scroll(thread_id, consumer_id, generation, scroll_generation);
                return TimelineEffectPlan::default();
            }
            TimelineIntent::Update { demand } => demand,
            TimelineIntent::Exit {
                thread_id,
                consumer_id,
                generation,
            } => {
                return self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned")
                    .timeline
                    .exit(&thread_id, &consumer_id, generation);
            }
            TimelineIntent::Retry {
                thread_id,
                consumer_id,
                generation,
            } => {
                let Some(demand) = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned")
                    .timeline
                    .retry(&thread_id, &consumer_id, generation)
                else {
                    return TimelineEffectPlan::default();
                };
                demand
            }
        };
        if demand.thread_id.is_empty() || demand.consumer_id.is_empty() {
            return TimelineEffectPlan::default();
        }
        let Some(snapshot) = self.thread_presentation_snapshot(&demand.thread_id) else {
            return TimelineEffectPlan::default();
        };
        if snapshot.timeline().source_revision() != demand.source_revision {
            return TimelineEffectPlan::default();
        }
        let actions = self.plan_thread_timeline_demand(
            &demand.thread_id,
            demand.source_revision,
            &demand.row_ids,
            demand.threshold,
            demand.before,
            demand.after,
            demand.work,
            demand.presented_rows,
        );
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if !registry.timeline_revision_matches(&demand.thread_id, demand.source_revision) {
            return TimelineEffectPlan::default();
        }
        if demand.read_requires_unread && !registry.timeline_has_unread(&demand.thread_id) {
            demand.viewed_through_turn_id = None;
        }
        registry.timeline.reconcile_topology(
            &demand,
            snapshot
                .timeline()
                .rows()
                .iter()
                .map(|row| row.id().as_str().to_owned())
                .collect(),
        );
        registry.timeline.update(demand, actions, now_ms)
    }

    pub(crate) fn next_timeline_read_delay(&self) -> Option<std::time::Duration> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .timeline
            .next_read_deadline()
            .map(|deadline| {
                std::time::Duration::from_millis(deadline.saturating_sub(self.timeline_now_ms()))
            })
    }

    pub(crate) fn drive_timeline_reads(&self) {
        let reads = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .timeline
            .due_reads(self.timeline_now_ms());
        for read in reads {
            self.execute_timeline_read(read);
        }
    }

    pub(crate) fn execute_timeline_read(&self, request: TimelineReadRequest) {
        if self.is_stopped()
            || !self
                .thread_registry
                .lock()
                .expect("thread registry poisoned")
                .timeline
                .accepts_read(&request)
        {
            return;
        }
        let result = self.read_thread_cursor(pioneer_protocol::ThreadReadParams {
            thread_id: request.thread_id.clone(),
            through_turn_id: request.through_turn_id.clone(),
        });
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if let Ok(response) = &result {
            if response.thread_id != request.thread_id
                || !registry.timeline_workspace_matches(&request.thread_id, &response.workspace_id)
            {
                return;
            }
        }
        let accepted =
            registry
                .timeline
                .finish_read(&request, result.is_ok(), self.timeline_now_ms());
        if accepted && let Ok(response) = result {
            self.apply_directory_read_locked(
                &mut registry,
                &response.workspace_id,
                &response.thread_id,
                &response.cursor,
                response.unread_count,
            );
        }
    }
}
