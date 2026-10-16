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
    /// Actual viewability, separate from pagination/prefetch row demand.
    #[serde(default)]
    pub viewed_row_ids: Vec<String>,
    // Resolved against the immutable canonical snapshot by ClientCore, never
    // accepted from the shell's serialized demand.
    #[serde(skip)]
    pub viewed_through_block: Option<pioneer_protocol::ThreadReadPosition>,
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
    pub through_block: Option<pioneer_protocol::ThreadReadPosition>,
    pub generation: u64,
}

impl TimelineDemand {
    fn read_key(&self) -> Option<String> {
        self.viewed_through_block
            .as_ref()
            .map(|position| format!("block:{}\0{}", position.block_id, position.sort_key))
            .or_else(|| {
                self.viewed_through_turn_id
                    .as_ref()
                    .map(|turn| format!("turn:{turn}"))
            })
    }
}
impl TimelineReadRequest {
    fn read_key(&self) -> String {
        self.through_block
            .as_ref()
            .map(|position| format!("block:{}\0{}", position.block_id, position.sort_key))
            .unwrap_or_else(|| format!("turn:{}", self.through_turn_id))
    }
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
struct PageFailure {
    attempts: u8,
    retry_at_ms: u64,
    retryable: bool,
}

#[derive(Default)]
pub struct ThreadTimelineController {
    pub(crate) in_flight: HashSet<PageKey>,
    pub(crate) pending: HashMap<PageKey, PageAction>,
    requests: HashMap<PageKey, (u64, PageAction)>,
    failures: HashMap<PageKey, PageFailure>,
    failed_refreshes: HashMap<String, PageAction>,
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
    pub(crate) fn has_failed_refresh(&self, thread_id: &str) -> bool {
        self.failed_refreshes.contains_key(thread_id)
    }

    pub(crate) fn has_active_refresh(&self, thread_id: &str) -> bool {
        let key = PageKey::ThreadNewest {
            thread_id: thread_id.to_owned(),
        };
        self.in_flight.contains(&key)
            && self
                .requests
                .get(&key)
                .is_some_and(|(_, action)| is_newest_refresh(action))
    }

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

    pub(crate) fn page_retry_allowed(&self, key: &PageKey, now_ms: u64) -> bool {
        self.failures
            .get(key)
            .is_none_or(|failure| failure.attempts < MAX_ATTEMPTS && now_ms >= failure.retry_at_ms)
    }

    pub(crate) fn finish(
        &mut self,
        key: &PageKey,
        succeeded: bool,
        now_ms: u64,
    ) -> Option<PageAction> {
        self.finish_with_retry(key, succeeded, true, now_ms)
    }

    pub(crate) fn finish_with_retry(
        &mut self,
        key: &PageKey,
        succeeded: bool,
        retryable: bool,
        now_ms: u64,
    ) -> Option<PageAction> {
        let (_, action) = self.requests.remove(key)?;
        let is_refresh = is_newest_refresh(&action);
        // Around/Oldest share the key, but do not own the failed Newest walk.
        let preserves_recovery = matches!(key, PageKey::ThreadNewest { .. })
            && !is_refresh
            && self.has_failed_refresh(request_thread_id(key));
        if !preserves_recovery {
            if succeeded {
                self.failures.remove(key);
                if is_refresh {
                    self.failed_refreshes.remove(request_thread_id(key));
                }
            } else {
                let failure = self.failures.entry(key.clone()).or_default();
                failure.attempts = if retryable {
                    failure.attempts.saturating_add(1)
                } else {
                    MAX_ATTEMPTS
                };
                failure.retry_at_ms = now_ms.saturating_add(backoff(failure.attempts));
                failure.retryable = retryable;
                if is_refresh {
                    // The retry obligation owns its exact action, independently
                    // of another anchor waiting in the ordinary pending slot.
                    self.failed_refreshes
                        .insert(request_thread_id(key).to_owned(), action);
                }
            }
        }
        if !succeeded
            && !is_refresh
            && (!retryable
                || (self.scoped_pages.contains(key) && !self.durable_pages.contains(key)))
        {
            self.pending.remove(key);
        }
        let mut next =
            semantic::finish_semantic_timeline_request(&mut self.in_flight, &mut self.pending, key);
        if is_refresh && !succeeded && next.as_ref().is_some_and(is_newest_refresh) {
            // A notification follow-up is covered by bounded recovery; it must
            // not turn a failure into an immediate request or reset the budget.
            next = None;
        }
        // Exhaustion stops the clock, not a domain-owned reconciliation. Keep
        // that ownership across viewport Exit until success or store retirement.
        let failed_newest = matches!(key, PageKey::ThreadNewest { .. })
            && self.has_failed_refresh(request_thread_id(key));
        if next.is_none() && !failed_newest {
            self.durable_pages.remove(key);
        }
        next
    }

    fn refresh_deadline(&self, key: &PageKey) -> Option<u64> {
        if !matches!(key, PageKey::ThreadNewest { .. })
            || !self.has_failed_refresh(request_thread_id(key))
        {
            return None;
        }
        self.failures
            .get(key)
            .filter(|failure| failure.attempts < MAX_ATTEMPTS)
            .map(|failure| failure.retry_at_ms)
    }

    /// A new event can reopen an exhausted transient recovery once. Keep the
    /// last failure's deadline as a cooldown; further events during Backoff or
    /// Loading neither replenish the budget nor move that deadline.
    pub(crate) fn rearm_exhausted_refresh(&mut self, thread: &str) -> bool {
        if !self.has_failed_refresh(thread) || self.has_active_refresh(thread) {
            return false;
        }
        let key = PageKey::ThreadNewest {
            thread_id: thread.to_owned(),
        };
        let Some(failure) = self.failures.get_mut(&key) else {
            return false;
        };
        if !failure.retryable || failure.attempts < MAX_ATTEMPTS {
            return false;
        }
        failure.attempts = 0;
        self.durable_pages.insert(key);
        true
    }

    pub(crate) fn reset_page_retry(&mut self, key: &PageKey) {
        self.failures.remove(key);
    }

    pub(crate) fn next_refresh_deadline(&self) -> Option<u64> {
        self.next_refresh_deadline_where(|_| true)
    }

    pub(crate) fn next_refresh_deadline_where(
        &self,
        eligible: impl Fn(&str) -> bool,
    ) -> Option<u64> {
        self.failed_refreshes
            .values()
            .map(semantic::semantic_timeline_request_key)
            .filter(|key| eligible(request_thread_id(key)) && !self.in_flight.contains(*key))
            .filter_map(|key| self.refresh_deadline(key))
            .min()
    }

    pub(crate) fn due_refreshes(
        &mut self,
        now_ms: u64,
        eligible: impl Fn(&str) -> bool,
    ) -> Vec<TimelinePageRequest> {
        let mut actions = self
            .failed_refreshes
            .values()
            .filter(|action| {
                let key = semantic::semantic_timeline_request_key(action);
                eligible(request_thread_id(key))
                    && !self.in_flight.contains(key)
                    && self
                        .refresh_deadline(key)
                        .is_some_and(|deadline| deadline <= now_ms)
            })
            .cloned()
            .collect::<Vec<_>>();
        actions.sort_by(|a, b| {
            request_thread_id(semantic::semantic_timeline_request_key(a)).cmp(request_thread_id(
                semantic::semantic_timeline_request_key(b),
            ))
        });
        actions
            .into_iter()
            .filter_map(|action| {
                let key = semantic::semantic_timeline_request_key(&action).clone();
                let action = self.begin(action)?;
                Some(TimelinePageRequest {
                    action,
                    generation: self.request_generation(&key),
                })
            })
            .collect()
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
            if !self.page_retry_allowed(&key, now_ms) {
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
        if let Some(turn) = demand.viewed_through_turn_id.as_ref().filter(|turn| {
            demand.viewed_through_block.is_some()
                || Some(*turn) == demand.latest_user_turn_id.as_ref()
        }) {
            let read_key = (demand.thread_id.clone(), demand.read_key().unwrap());
            if !self.reads.contains_key(&read_key) {
                self.clock = self.clock.saturating_add(1);
                let request = TimelineReadRequest {
                    thread_id: read_key.0.clone(),
                    through_turn_id: turn.clone(),
                    through_block: demand.viewed_through_block.clone(),
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
            if matches!(key, PageKey::ThreadNewest { .. }) {
                self.failed_refreshes.remove(request_thread_id(key));
                self.failures.remove(key);
            }
        }
        self.reads.retain(|(thread, turn), _| {
            self.demands
                .values()
                .any(|d| &d.thread_id == thread && d.read_key().as_deref() == Some(turn.as_str()))
        });
        active
    }

    pub(crate) fn accepts_read(&self, request: &TimelineReadRequest) -> bool {
        self.reads
            .get(&(request.thread_id.clone(), request.read_key()))
            .is_some_and(|r| r.request == *request && r.state == RetryState::Loading)
    }

    pub(crate) fn finish_read(
        &mut self,
        request: &TimelineReadRequest,
        succeeded: bool,
        now_ms: u64,
    ) -> bool {
        self.finish_read_with_retry(request, succeeded, true, now_ms)
    }

    fn finish_read_with_retry(
        &mut self,
        request: &TimelineReadRequest,
        succeeded: bool,
        retryable: bool,
        now_ms: u64,
    ) -> bool {
        if !self.accepts_read(request) {
            return false;
        }
        let read = self
            .reads
            .get_mut(&(request.thread_id.clone(), request.read_key()))
            .unwrap();
        read.state = if succeeded {
            RetryState::Ready
        } else if !retryable || read.attempts >= MAX_ATTEMPTS {
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
                || demand.read_key().as_deref() != Some(turn.as_str())
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
        self.pending
            .retain(|key, _| thread.is_some_and(|id| request_thread_id(key) != id));
        self.durable_pages
            .retain(|key| thread.is_some_and(|id| request_thread_id(key) != id));
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
        self.failed_refreshes
            .retain(|id, _| thread.is_some_and(|thread| id != thread));
        keys
    }
}

pub(crate) fn is_newest_refresh(action: &PageAction) -> bool {
    matches!(action,
        PageAction::ThreadTimelinePage { key: PageKey::ThreadNewest { .. }, params }
            if params.anchor == pioneer_protocol::TimelinePageAnchor::Newest)
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
            viewed_row_ids: Vec::new(),
            viewed_through_block: None,
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

    fn initial_work_page() -> PageAction {
        PageAction::TurnWorkPage {
            key: PageKey::TurnWorkInitial {
                thread_id: "a".into(),
                turn_id: "turn".into(),
            },
            params: pioneer_protocol::TurnWorkPageParams {
                thread_id: "a".into(),
                turn_id: "turn".into(),
                anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                limit: Some(12),
            },
        }
    }

    #[test]
    fn canonical_position_can_retry_after_a_speculative_live_position_failed() {
        let mut policy = ThreadTimelineController::default();
        let mut input = demand("mobile");
        input.viewed_through_block = Some(pioneer_protocol::ThreadReadPosition {
            block_id: "answer".into(),
            sort_key: "old".into(),
        });
        let old = policy.update(input.clone(), Vec::new(), 0).reads.remove(0);
        assert!(policy.finish_read(&old, false, 0));
        input.source_revision += 1;
        input.viewed_through_block.as_mut().unwrap().sort_key = "canonical".into();
        let canonical = policy.update(input, Vec::new(), 1).reads.remove(0);
        assert_eq!(
            old.through_block.as_ref().unwrap().block_id,
            canonical.through_block.as_ref().unwrap().block_id
        );
        assert!(!policy.accepts_read(&old));
        assert!(
            policy.due_reads(u64::MAX).is_empty(),
            "old-position retries are retired"
        );
        assert!(policy.finish_read(&canonical, true, 1));
    }

    #[test]
    fn separate_final_blocks_in_one_turn_are_read_and_deduplicated_by_block() {
        let mut policy = ThreadTimelineController::default();
        let mut input = demand("desktop");
        input.viewed_through_block = Some(pioneer_protocol::ThreadReadPosition {
            block_id: "turn:turn:assistant:first".into(),
            sort_key: "1".into(),
        });
        let first = policy.update(input.clone(), Vec::new(), 0).reads.remove(0);
        assert!(policy.finish_read(&first, true, 0));
        input.source_revision += 1;
        assert!(policy.update(input.clone(), Vec::new(), 1).reads.is_empty());
        input.viewed_through_block = Some(pioneer_protocol::ThreadReadPosition {
            block_id: "turn:turn:assistant:second".into(),
            sort_key: "2".into(),
        });
        let second = policy.update(input.clone(), Vec::new(), 2).reads.remove(0);
        assert_eq!(first.through_turn_id, second.through_turn_id);
        assert_ne!(first.through_block, second.through_block);
        assert!(!policy.accepts_read(&first));
        assert!(policy.finish_read(&second, false, 2));
        policy.exit("a", "desktop", input.generation);
        assert!(
            policy.due_reads(u64::MAX).is_empty(),
            "background/retired view cannot retry reads"
        );
    }

    #[test]
    fn initial_work_loading_does_not_queue_a_follow_up() {
        let mut policy = ThreadTimelineController::default();
        let action = initial_work_page();
        let key = semantic::semantic_timeline_request_key(&action).clone();
        let plan = policy.update(demand("desktop"), vec![action.clone()], 0);
        assert_eq!(plan.pages.len(), 1);
        for revision in 2..5 {
            let mut input = demand("desktop");
            input.source_revision = revision;
            assert!(
                policy
                    .update(input, vec![action.clone()], 0)
                    .pages
                    .is_empty()
            );
        }
        assert!(policy.finish(&key, true, 0).is_none());
        assert!(policy.in_flight.is_empty());
        assert!(policy.pending.is_empty());
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
    fn failed_refresh_retry_preserves_the_consumers_anchor_and_scroll_generation() {
        let mut policy = ThreadTimelineController::default();
        let mut input = demand("desktop");
        input.row_ids = vec!["stable-parent-row".into()];
        input.scroll_generation = 17;
        policy.update(input.clone(), vec![], 0);
        let action = page();
        let key = semantic::semantic_timeline_request_key(&action).clone();
        policy.retain_domain_request(&key);
        policy.begin(action.clone()).unwrap();
        let generation = policy.request_generation(&key);
        policy.begin(action); // one coalesced notification, not another owner
        assert_eq!(policy.request_generation(&key), generation);
        assert!(policy.finish(&key, false, 0).is_none());
        assert!(!policy.accepts(&key, generation));
        assert!(policy.has_failed_refresh("a"));
        let retry = policy.retry("a", "desktop", 1).unwrap();
        assert_eq!(retry.row_ids, input.row_ids);
        assert_eq!(retry.scroll_generation, input.scroll_generation);
        assert_eq!(retry.generation, input.generation);
    }

    #[test]
    fn exhausted_refresh_rearms_once_after_cooldown_and_keeps_a_bounded_budget() {
        for retryable in [false, true] {
            let mut policy = ThreadTimelineController::default();
            let action = page();
            let key = semantic::semantic_timeline_request_key(&action).clone();
            assert_eq!(
                policy
                    .update(demand("desktop"), vec![action.clone()], 0)
                    .pages
                    .len(),
                1
            );
            // A notification makes the scoped request a domain obligation.
            policy.retain_domain_request(&key);
            let old_generation = policy.request_generation(&key);
            for now in [0, 1_000, 3_000] {
                assert!(!policy.rearm_exhausted_refresh("a"));
                for _ in 0..10 {
                    assert!(policy.begin(action.clone()).is_none());
                }
                assert_eq!(policy.pending.len(), 1);
                assert!(
                    policy
                        .finish_with_retry(&key, false, now < 3_000 || retryable, now)
                        .is_none()
                );
                if now < 3_000 {
                    assert!(!policy.rearm_exhausted_refresh("a"));
                    assert!(policy.due_refreshes(now, |_| true).is_empty());
                    let deadline = policy.next_refresh_deadline().unwrap();
                    assert_eq!(policy.due_refreshes(deadline, |_| true).len(), 1);
                }
            }
            assert_eq!(policy.next_refresh_deadline(), None);
            assert!(policy.due_refreshes(u64::MAX, |_| true).is_empty());
            assert!(policy.exit("a", "desktop", 1).cancelled.is_empty());
            assert!(
                policy.has_failed_refresh("a"),
                "viewport Exit retains domain recovery"
            );
            assert_eq!(policy.rearm_exhausted_refresh("a"), retryable);
            if !retryable {
                // Protocol/auth/dispatch failures remain terminal for automatic triggers.
                assert_eq!(policy.next_refresh_deadline(), None);
                continue;
            }
            assert_eq!(policy.next_refresh_deadline(), Some(7_000));
            assert!(!policy.rearm_exhausted_refresh("a"));
            assert!(policy.due_refreshes(6_999, |_| true).is_empty());
            assert!(policy.due_refreshes(u64::MAX, |_| false).is_empty());
            assert_eq!(policy.next_refresh_deadline_where(|_| false), None);
            // Other anchors share the key, but cannot consume/reset Newest recovery.
            for anchor in [
                pioneer_protocol::TimelinePageAnchor::Oldest,
                pioneer_protocol::TimelinePageAnchor::Around {
                    cursor: pioneer_protocol::TimelineCursor {
                        value: "opaque".into(),
                    },
                },
            ] {
                let mut other = action.clone();
                let PageAction::ThreadTimelinePage { params, .. } = &mut other else {
                    unreachable!()
                };
                params.anchor = anchor;
                policy.begin(other).unwrap();
                assert_eq!(policy.next_refresh_deadline(), None);
                assert!(policy.finish(&key, true, 6_000).is_none());
                assert_eq!(policy.next_refresh_deadline(), Some(7_000));
            }
            let next = policy.due_refreshes(7_000, |_| true).remove(0);
            assert_eq!(next.action, action);
            assert!(next.generation > old_generation);
            assert!(!policy.accepts(&key, old_generation));
            for now in [7_000, 8_000, 10_000] {
                assert!(!policy.rearm_exhausted_refresh("a"));
                assert!(policy.due_refreshes(u64::MAX, |_| true).is_empty());
                assert!(policy.finish(&key, false, now).is_none());
                if now < 10_000 {
                    assert!(!policy.rearm_exhausted_refresh("a"));
                    let deadline = policy.next_refresh_deadline().unwrap();
                    assert_eq!(policy.due_refreshes(deadline, |_| true).len(), 1);
                }
            }
            assert_eq!(policy.next_refresh_deadline(), None);
            assert!(policy.due_refreshes(u64::MAX, |_| true).is_empty());
            assert!(policy.has_failed_refresh("a"));
        }
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
            policy.reads[&("a".into(), first.read_key())].state,
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
        for action in [page(), initial_work_page()] {
            let mut policy = ThreadTimelineController::default();
            let mut input = demand("desktop");
            for attempt in 0..3 {
                input.scroll_generation += 1;
                let now = attempt * 10_000;
                let plan = policy.update(input.clone(), vec![action.clone()], now);
                assert_eq!(plan.pages.len(), 1);
                let key = semantic::semantic_timeline_request_key(&plan.pages[0].action);
                assert!(policy.finish(key, false, now).is_none());
                assert!(!policy.page_retry_allowed(key, now));
                assert!(
                    policy
                        .update(input.clone(), vec![action.clone()], u64::MAX)
                        .pages
                        .is_empty()
                );
            }
            input.scroll_generation += 1;
            assert!(
                policy
                    .update(input, vec![action.clone()], u64::MAX)
                    .pages
                    .is_empty()
            );
            let retry = policy.retry("a", "desktop", 1).unwrap();
            assert_eq!(policy.update(retry, vec![action], u64::MAX).pages.len(), 1);
        }
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
        let key = semantic::semantic_timeline_request_key(&a.pages[0].action);
        policy.finish(key, false, 0);
        assert_eq!(policy.next_refresh_deadline(), Some(1_000));
        policy.invalidate(Some("a"));
        assert!(policy.due_refreshes(u64::MAX, |_| true).is_empty());
        assert_eq!(policy.next_refresh_deadline(), None);
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
                        .finish_read_with_retry(read, false, false, self.timeline_now_ms());
                }
            }
        } else {
            for read in &plan.reads {
                let mut registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                registry.timeline.finish_read_with_retry(
                    read,
                    false,
                    false,
                    self.timeline_now_ms(),
                );
            }
        }
        plan
    }

    /// Deterministic clock/effect seam; callers execute only the returned immutable plan.
    pub fn plan_timeline_intent(&self, intent: TimelineIntent, now_ms: u64) -> TimelineEffectPlan {
        if self.is_stopped() {
            return TimelineEffectPlan::default();
        }
        let mut retry_refresh = None;
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
                let mut registry = self
                    .thread_registry
                    .lock()
                    .expect("thread registry poisoned");
                let Some(demand) = registry
                    .timeline
                    .retry(&thread_id, &consumer_id, generation)
                else {
                    return TimelineEffectPlan::default();
                };
                retry_refresh = registry.timeline.failed_refreshes.get(&thread_id).cloned();
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
        demand.viewed_through_block = None;
        let semantic_ids = snapshot.timeline().semantic_row_ids();
        let viewed_blocks = demand
            .viewed_row_ids
            .iter()
            .filter_map(|id| semantic_ids.get(id))
            .filter_map(|id| match id {
                super::semantic::SemanticTimelineRowId::TopLevelBlock { block_id } => {
                    Some(block_id)
                }
                _ => None,
            })
            .collect::<HashSet<_>>();
        let semantic_rows = snapshot.timeline().semantic_rows();
        let viewed = semantic_rows
            .rows
            .iter()
            .filter_map(|row| match &row.kind {
                super::semantic::SemanticTimelineRowKind::UserBlock { block } => Some(block),
                super::semantic::SemanticTimelineRowKind::AssistantMessage { block }
                    if matches!(
                        &block.kind,
                        pioneer_protocol::TimelineBlockKind::AssistantMessage {
                            status: pioneer_protocol::TurnWorkItemStatus::Completed,
                            ..
                        }
                    ) =>
                {
                    Some(block)
                }
                _ => None,
            })
            .filter(|block| viewed_blocks.contains(&block.block_id))
            .max_by(|a, b| a.sort_key.cmp(&b.sort_key));
        if let Some(block) = viewed {
            demand.viewed_through_turn_id = block.turn_id.clone();
            demand.viewed_through_block = Some(pioneer_protocol::ThreadReadPosition {
                block_id: block.block_id.clone(),
                sort_key: block.sort_key.clone(),
            });
        }
        let already_read = demand.viewed_through_turn_id.as_ref().is_some_and(|turn| {
            registry.timeline_read_is_confirmed(
                &demand.thread_id,
                turn,
                demand.viewed_through_block.as_ref(),
            )
        });
        if already_read
            || (demand.read_requires_unread && !registry.timeline_has_unread(&demand.thread_id))
        {
            demand.viewed_through_turn_id = None;
            demand.viewed_through_block = None;
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
        let mut plan = registry.timeline.update(demand, actions, now_ms);
        if let Some(action) = retry_refresh {
            let key = semantic::semantic_timeline_request_key(&action).clone();
            registry.timeline.reset_page_retry(&key);
            registry.timeline.retain_domain_request(&key);
            if let Some(action) = registry.timeline.begin(action) {
                plan.pages.push(TimelinePageRequest {
                    action,
                    generation: registry.timeline.request_generation(&key),
                });
            }
        }
        plan
    }

    pub(crate) fn next_timeline_request_delay(&self) -> Option<std::time::Duration> {
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .next_timeline_request_deadline()
            .map(|deadline| {
                std::time::Duration::from_millis(deadline.saturating_sub(self.timeline_now_ms()))
            })
    }

    pub(crate) fn drive_timeline_requests(&self) {
        self.drive_timeline_requests_at(self.timeline_now_ms());
    }

    pub(crate) fn drive_timeline_requests_at(&self, now_ms: u64) {
        if self.is_stopped() {
            return;
        }
        let pages = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .due_timeline_refreshes(now_ms);
        for page in pages {
            self.schedule_planned_timeline_request(page);
        }
        let reads = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .timeline
            .due_reads(now_ms);
        for read in reads {
            self.execute_timeline_read(read);
        }
    }

    pub(crate) fn execute_timeline_read(&self, request: TimelineReadRequest) {
        if self.is_stopped() {
            return;
        }
        let Some(token) = self.thread_operation_token(&request.thread_id) else {
            return;
        };
        let auth = self.current_auth_ticket();
        {
            let mut registry = self
                .thread_registry
                .lock()
                .expect("thread registry poisoned");
            if !registry.timeline.accepts_read(&request)
                || !registry.timeline_operation_matches(&token)
            {
                return;
            }
            // Another session can advance the confirmed cursor while this
            // request is queued or waiting for retry.
            if registry.timeline_read_is_confirmed(
                &request.thread_id,
                &request.through_turn_id,
                request.through_block.as_ref(),
            ) {
                registry
                    .timeline
                    .finish_read(&request, true, self.timeline_now_ms());
                return;
            }
        }
        let result = self.read_thread_cursor(pioneer_protocol::ThreadReadParams {
            through_block: request.through_block.clone(),
            thread_id: request.thread_id.clone(),
            through_turn_id: request.through_turn_id.clone(),
        });
        let auth_is_current = self.current_auth_ticket() == auth;
        let mut registry = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned");
        if self.is_stopped() || !registry.timeline_operation_matches(&token) {
            return;
        }
        if !auth_is_current {
            registry
                .timeline
                .finish_read(&request, false, self.timeline_now_ms());
            return;
        }
        if let Ok(response) = &result {
            if response.thread_id != request.thread_id
                || !registry.timeline_workspace_matches(&request.thread_id, &response.workspace_id)
            {
                registry.timeline.finish_read_with_retry(
                    &request,
                    false,
                    false,
                    self.timeline_now_ms(),
                );
                return;
            }
        }
        registry
            .timeline
            .finish_read(&request, result.is_ok(), self.timeline_now_ms());
        // read_thread_cursor already fences auth/thread lifetime. A successful
        // server cursor stays authoritative even if the viewport has moved.
        if let Ok(response) = result {
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
