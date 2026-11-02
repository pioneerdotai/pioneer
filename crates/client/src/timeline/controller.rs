//! Process-local semantic timeline demand and request policy. No shell geometry.
use std::collections::{HashMap, HashSet};

use super::semantic::{
    self, SemanticTimelineRequestAction as PageAction, SemanticTimelineRequestKey as PageKey,
};

pub const WORK_ITEM_CHUNK_LIMIT: usize = 200;
const MAX_ATTEMPTS: u8 = 3;

// Defensive owner budgets, not measured latency optima. Rejected input remains
// wholly owned by its caller; accepted ID sets are never truncated.
pub(crate) const HISTORY_KEY_LIMIT: usize = 128;
pub(crate) const HISTORY_IDS_PER_ACTION: usize = 4_096;
const HISTORY_IDS_LIMIT: usize = 32_768;
const HISTORY_ACTION_BYTES_LIMIT: usize = 2 * 1024 * 1024;

fn action_resources(action: &PageAction) -> (usize, usize) {
    fn anchor_bytes(anchor: &pioneer_protocol::TimelinePageAnchor) -> usize {
        use pioneer_protocol::TimelinePageAnchor::*;
        match anchor {
            Newest | Oldest => 0,
            Before { cursor } | After { cursor } | Around { cursor } => cursor.value.len(),
        }
    }
    let key = semantic::semantic_timeline_request_key(action);
    let key_bytes = match key {
        PageKey::ThreadNewest { thread_id } => thread_id.len(),
        PageKey::ThreadBefore { thread_id, cursor }
        | PageKey::ThreadAfter { thread_id, cursor } => thread_id.len() + cursor.len(),
        PageKey::TurnWorkInitial { thread_id, turn_id }
        | PageKey::TurnWorkItems { thread_id, turn_id } => thread_id.len() + turn_id.len(),
        PageKey::TurnWorkBefore {
            thread_id,
            turn_id,
            cursor,
        }
        | PageKey::TurnWorkAfter {
            thread_id,
            turn_id,
            cursor,
        } => thread_id.len() + turn_id.len() + cursor.len(),
    };
    match action {
        PageAction::ThreadTimelinePage { params, .. } => (
            0,
            key_bytes + params.thread_id.len() + anchor_bytes(&params.anchor),
        ),
        PageAction::TurnWorkPage { params, .. } => (
            0,
            key_bytes
                + params.thread_id.len()
                + params.turn_id.len()
                + anchor_bytes(&params.anchor),
        ),
        PageAction::TurnWorkItemsGet { params, .. } => (
            params.work_item_ids.len(),
            key_bytes
                + params.thread_id.len()
                + params.turn_id.len()
                + params.work_item_ids.iter().map(String::len).sum::<usize>(),
        ),
    }
}

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

fn task_inbox_view_is_renewed(previous: Option<&TimelineDemand>, next: &TimelineDemand) -> bool {
    previous.is_none_or(|previous| {
        previous.generation != next.generation
            || previous.viewed_row_ids.iter().collect::<HashSet<_>>()
                != next.viewed_row_ids.iter().collect::<HashSet<_>>()
    })
}

fn viewed_task_inbox_params(
    workspace: &str,
    demand: &TimelineDemand,
    rows: &semantic::SemanticTimelineRows,
    ids: &HashMap<String, semantic::SemanticTimelineRowId>,
) -> Option<pioneer_protocol::TaskCompletionInboxViewedParams> {
    use pioneer_protocol::{TimelineBlockKind as B, TurnItem, TurnWorkItemStatus as Status};
    use semantic::SemanticTimelineRowKind as K;
    let viewed = demand
        .viewed_row_ids
        .iter()
        .filter_map(|id| ids.get(id))
        .collect::<HashSet<_>>();
    let mut runs = std::collections::BTreeSet::new();
    let mut turns = std::collections::BTreeSet::new();
    let mut result_items = std::collections::BTreeSet::new();
    for row in rows.rows.iter().filter(|row| viewed.contains(&row.id)) {
        let task = match &row.kind {
            K::DetachedTaskRun { block } => match &block.kind {
                B::DetachedTaskRun { task, .. } => Some(task),
                _ => None,
            },
            K::WorkItem { item } => match &item.item {
                TurnItem::Task { item } => Some(item),
                _ => None,
            },
            _ => None,
        };
        if let Some(task) = task.filter(|task| task.status.is_terminal()) {
            if let Some(run) = &task.run_id {
                runs.insert(run.clone());
            }
        }
        match &row.kind {
            K::WorkItem { item } if item.status == Status::Completed => {
                if let TurnItem::AgentMessage {
                    id,
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    ..
                } = &item.item
                {
                    result_items.insert((item.turn_id.clone(), id.clone()));
                }
            }
            K::AssistantMessage { block } => {
                if let B::AssistantMessage {
                    item_id,
                    status: Status::Completed,
                    ..
                } = &block.kind
                {
                    if let Some(turn) = &block.turn_id {
                        result_items.insert((turn.clone(), item_id.clone()));
                    }
                }
            }
            K::TurnState { block }
                if matches!(
                    &block.kind,
                    B::TurnState {
                        state: pioneer_protocol::TurnWorkState::Completed
                            | pioneer_protocol::TurnWorkState::Blocked
                            | pioneer_protocol::TurnWorkState::Failed
                            | pioneer_protocol::TurnWorkState::Interrupted,
                        ..
                    }
                ) =>
            {
                if let Some(turn) = &block.turn_id {
                    turns.insert(turn.clone());
                }
            }
            _ => {}
        }
    }
    if runs.is_empty() && turns.is_empty() && result_items.is_empty() {
        return None;
    }
    let run_ids = runs.into_iter().collect::<Vec<_>>();
    let result_items = result_items
        .into_iter()
        .map(
            |(turn_id, item_id)| pioneer_protocol::TaskCompletionInboxViewedResult {
                turn_id,
                item_id,
            },
        )
        .collect::<Vec<_>>();
    let result_turn_ids = turns.into_iter().collect();
    Some(pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: workspace.into(),
        thread_id: demand.thread_id.clone(),
        run_ids,
        result_turn_ids,
        result_items,
    })
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
    /// Entire actions refused by finite owner capacity; no subset was accepted.
    pub rejected: Vec<TimelinePageRejection>,
    pub rejected_reads: Vec<TimelineReadRequest>,
    /// A planned job was refused by the physical controller admission boundary.
    pub dispatch_failed: bool,
    pub inbox_viewed: Option<pioneer_protocol::TaskCompletionInboxViewedParams>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimelinePageRejection {
    pub action: PageAction,
    pub request_generation: u64,
    pub store_generation: u64,
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

// Evidence belongs to one admitted generation, including queued/RPC quanta.
// It is not a cache tombstone: completion/cancellation drops the whole value.
const REMOVAL_ID_LIMIT: usize = 4_096;
const REMOVAL_BYTE_LIMIT: usize = 64 * 1024;

#[derive(Clone, Default)]
pub(crate) struct PageRemovals {
    ids: HashSet<String>,
    bytes: usize,
    full: bool,
}
impl PageRemovals {
    fn record(&mut self, id: &str) {
        if self.full || self.ids.contains(id) {
            return;
        }
        if self.ids.len() >= REMOVAL_ID_LIMIT
            || id.len() > REMOVAL_BYTE_LIMIT.saturating_sub(self.bytes)
        {
            self.full = true;
            return;
        }
        self.bytes += id.len();
        self.ids.insert(id.to_owned());
    }
    pub(crate) fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }
    pub(crate) fn is_full(&self) -> bool {
        self.full
    }
}
struct ActivePageRequest {
    generation: u64,
    action: PageAction,
    removals: PageRemovals,
}

#[derive(Default)]
pub struct ThreadTimelineController {
    pub(crate) in_flight: HashSet<PageKey>,
    pub(crate) pending: HashMap<PageKey, PageAction>,
    pending_explicit: HashSet<PageKey>,
    requests: HashMap<PageKey, ActivePageRequest>,
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
                .is_some_and(|request| is_newest_refresh(&request.action))
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
    pub(crate) fn begin(
        &mut self,
        mut action: PageAction,
    ) -> Result<Option<PageAction>, PageAction> {
        // Bound even admission's sorting work before normalizing the set.
        // Oversized input is refused whole, including duplicate-heavy input.
        if matches!(&action, PageAction::TurnWorkItemsGet { params, .. } if params.work_item_ids.len() > HISTORY_IDS_PER_ACTION)
        {
            return Err(action);
        }
        // Duplicate IDs in an admitted-size input do not consume union capacity.
        if let PageAction::TurnWorkItemsGet { params, .. } = &mut action {
            params.work_item_ids.sort();
            params.work_item_ids.dedup();
        }
        let (incoming_ids, incoming_bytes) = action_resources(&action);
        if incoming_ids > HISTORY_IDS_PER_ACTION || incoming_bytes > HISTORY_ACTION_BYTES_LIMIT {
            return Err(action);
        }
        // Clone only after checking finite lengths, so retained allocations
        // cannot inherit arbitrarily large spare caller capacities.
        action = action.clone();
        let key = semantic::semantic_timeline_request_key(&action).clone();
        let coalescing = self.in_flight.contains(&key);
        let mut candidate = action.clone();
        if coalescing && let Some(pending) = self.pending.get(&key) {
            candidate = pending.clone();
            semantic::coalesce_semantic_timeline_request_action(&mut candidate, action.clone());
        }
        let mut keys = self
            .requests
            .keys()
            .chain(self.pending.keys())
            .chain(self.failures.keys())
            .chain(
                self.failed_refreshes
                    .values()
                    .map(semantic::semantic_timeline_request_key),
            )
            .collect::<HashSet<_>>();
        keys.insert(&key);
        let (candidate_ids, candidate_bytes) = action_resources(&candidate);
        let (ids, bytes) = self
            .requests
            .values()
            .map(|request| &request.action)
            .chain(
                self.pending
                    .iter()
                    .filter(|(pending_key, _)| !coalescing || *pending_key != &key)
                    .map(|(_, action)| action),
            )
            // A retry carries one logical obligation, even while the active
            // incarnation holds a fixed-size clone of its exact saved action.
            .chain(self.failed_refreshes.values().filter(|saved| {
                let saved_key = semantic::semantic_timeline_request_key(saved);
                !(saved_key == &key && *saved == &candidate && !self.in_flight.contains(&key))
                    && !self
                        .requests
                        .get(saved_key)
                        .is_some_and(|request| &request.action == *saved)
            }))
            .map(action_resources)
            .fold(
                (candidate_ids, candidate_bytes),
                |(ids, bytes), (next_ids, next_bytes)| {
                    (
                        ids.saturating_add(next_ids),
                        bytes.saturating_add(next_bytes),
                    )
                },
            );
        if keys.len() > HISTORY_KEY_LIMIT
            || candidate_ids > HISTORY_IDS_PER_ACTION
            || ids > HISTORY_IDS_LIMIT
            || bytes > HISTORY_ACTION_BYTES_LIMIT
        {
            return Err(action);
        }
        let Some(action) = semantic::enqueue_semantic_timeline_request(
            &mut self.in_flight,
            &mut self.pending,
            action,
        ) else {
            return Ok(None);
        };
        self.clock = self.clock.saturating_add(1);
        self.requests.insert(
            key,
            ActivePageRequest {
                generation: self.clock,
                action: action.clone(),
                removals: PageRemovals::default(),
            },
        );
        Ok(Some(action))
    }

    pub(crate) fn mark_pending_explicit(&mut self, key: &PageKey) {
        self.pending_explicit
            .retain(|key| self.pending.contains_key(key));
        if self.pending.contains_key(key) {
            self.pending_explicit.insert(key.clone());
        }
    }

    pub(crate) fn take_pending_explicit(&mut self, key: &PageKey) -> bool {
        self.pending_explicit.remove(key)
    }

    /// A full targeted collection fails as one collection, including any older
    /// accepted trailing IDs. Invalidate the old generation before publishing
    /// Failed so a late RPC cannot report a partial success. Exact refresh
    /// recovery retains its independently owned action and retry budget.
    pub(crate) fn reject_admission(&mut self, key: &PageKey) -> bool {
        if matches!(key, PageKey::ThreadNewest { .. })
            && (self.has_failed_refresh(request_thread_id(key))
                || self.has_active_refresh(request_thread_id(key)))
        {
            return false;
        }
        self.in_flight.remove(key);
        self.requests.remove(key);
        self.pending.remove(key);
        self.pending_explicit.remove(key);
        self.durable_pages.remove(key);
        self.scoped_pages.remove(key);
        true
    }

    /// Scheduler saturation does not consume a network attempt or retire the
    /// exact domain-owned Newest obligation. A small admission delay prevents
    /// a full scheduler from turning its due timer into a busy loop.
    pub(crate) fn defer_dispatch(&mut self, key: &PageKey, generation: u64, now_ms: u64) -> bool {
        if !self.accepts(key, generation)
            || !self
                .requests
                .get(key)
                .is_some_and(|request| is_newest_refresh(&request.action))
        {
            return false;
        }
        let action = self.requests.remove(key).unwrap().action;
        self.in_flight.remove(key);
        self.failed_refreshes
            .insert(request_thread_id(key).to_owned(), action);
        self.durable_pages.insert(key.clone());
        let failure = self.failures.entry(key.clone()).or_insert(PageFailure {
            retryable: true,
            ..PageFailure::default()
        });
        failure.retry_at_ms = failure.retry_at_ms.max(now_ms.saturating_add(50));
        true
    }

    pub(crate) fn has_current_demand(&self, thread: &str) -> bool {
        self.demands.iter().any(|(consumer, demand)| {
            demand.thread_id == thread
                && !self
                    .retired
                    .get(consumer)
                    .is_some_and(|retired| *retired >= demand.generation)
        })
    }

    /// Read at dispatch, after every quantum. Each retained consumer owns its
    /// own current generation; one consumer exiting cannot demote another's key.
    pub(crate) fn dispatch_priority(&self, key: &PageKey, generation: u64) -> Option<bool> {
        if !self.accepts(key, generation) {
            return None;
        }
        Some(self.demand_pages.iter().any(|(consumer, keys)| {
            let Some(demand) = self.demands.get(consumer) else {
                return false;
            };
            if self
                .retired
                .get(consumer)
                .is_some_and(|retired| *retired >= demand.generation)
                || !keys.contains(key)
            {
                return false;
            }
            match key {
                PageKey::ThreadBefore { .. } | PageKey::TurnWorkBefore { .. } => demand.before,
                PageKey::ThreadAfter { .. } | PageKey::TurnWorkAfter { .. } => demand.after,
                _ => !demand.row_ids.is_empty() || !demand.viewed_row_ids.is_empty(),
            }
        }))
    }

    pub(crate) fn record_removals(&mut self, thread: &str, turn: Option<&str>, ids: &[String]) {
        for request in self.requests.values_mut() {
            if request.removals.is_full() {
                continue;
            }
            match &request.action {
                PageAction::ThreadTimelinePage { params, .. }
                    if turn.is_none() && params.thread_id == thread =>
                {
                    for id in ids {
                        request.removals.record(id);
                        if request.removals.is_full() {
                            break;
                        }
                    }
                }
                PageAction::TurnWorkItemsGet { params, .. }
                    if params.thread_id == thread && Some(params.turn_id.as_str()) == turn =>
                {
                    for id in ids
                        .iter()
                        .filter(|id| params.work_item_ids.binary_search(id).is_ok())
                    {
                        request.removals.record(id);
                        if request.removals.is_full() {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn request_removals(&self, key: &PageKey, generation: u64) -> Option<&PageRemovals> {
        self.requests
            .get(key)
            .filter(|request| request.generation == generation)
            .map(|request| &request.removals)
    }

    pub(crate) fn request_generation(&self, key: &PageKey) -> u64 {
        self.requests
            .get(key)
            .map_or(0, |request| request.generation)
    }

    pub(crate) fn accepts(&self, key: &PageKey, generation: u64) -> bool {
        self.requests
            .get(key)
            .is_some_and(|request| request.generation == generation)
            && self.in_flight.contains(key)
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
        let action = self.requests.remove(key)?.action;
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
        if self
            .failed_refreshes
            .values()
            .any(|saved| semantic::semantic_timeline_request_key(saved) == key)
        {
            // Manual Retry resets the budget, but the saved obligation must
            // retain an immediate deadline even if re-admission cannot run.
            self.failures.insert(
                key.clone(),
                PageFailure {
                    attempts: 0,
                    retry_at_ms: 0,
                    retryable: true,
                },
            );
        } else {
            self.failures.remove(key);
        }
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
                // This is the exact saved obligation counted by begin's
                // capacity accounting, so retry admission cannot need new space.
                let action = self
                    .begin(action)
                    .expect("retained recovery fits its reservation")?;
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
            if self.in_flight.contains(&key) || self.failures.contains_key(&key) {
                // Failed demanded pages remain addressable by explicit Retry,
                // without retaining never-admitted candidate cursor keys.
                keys.insert(key.clone());
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
                match self.begin(action) {
                    Ok(Some(action)) => {
                        keys.insert(key.clone());
                        self.scoped_pages.insert(key.clone());
                        plan.pages.push(TimelinePageRequest {
                            generation: self.request_generation(&key),
                            action,
                        });
                    }
                    Ok(None) => {}
                    Err(action) => plan.rejected.push(TimelinePageRejection {
                        request_generation: self
                            .request_generation(semantic::semantic_timeline_request_key(&action)),
                        action,
                        store_generation: 0,
                    }),
                }
            } else if let PageAction::TurnWorkItemsGet { params, .. } = &action {
                let mut missing = params.clone();
                if let Some(ActivePageRequest {
                    action: PageAction::TurnWorkItemsGet { params: active, .. },
                    ..
                }) = self.requests.get(&key)
                {
                    missing
                        .work_item_ids
                        .retain(|id| !active.work_item_ids.contains(id));
                }
                if !missing.work_item_ids.is_empty() {
                    if let Err(action) = self.begin(PageAction::TurnWorkItemsGet {
                        key,
                        params: missing,
                    }) {
                        plan.rejected.push(TimelinePageRejection {
                            request_generation: self.request_generation(
                                semantic::semantic_timeline_request_key(&action),
                            ),
                            action,
                            store_generation: 0,
                        });
                    }
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
                let payload_bytes = request.thread_id.len()
                    + request.through_turn_id.len()
                    + request
                        .through_block
                        .as_ref()
                        .map_or(0, |block| block.block_id.len() + block.sort_key.len());
                // Reads have their own finite reserve, including the cursor
                // strings retained by retry state. Refusal creates no Loading.
                if self.reads.len() >= HISTORY_KEY_LIMIT || payload_bytes > 32 * 1024 {
                    plan.rejected_reads.push(request);
                } else {
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
            self.pending_explicit.remove(key);
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
            self.pending_explicit.remove(key);
            self.requests.remove(key);
            self.durable_pages.remove(key);
        }
        self.pending
            .retain(|key, _| thread.is_some_and(|id| request_thread_id(key) != id));
        self.pending_explicit
            .retain(|key| self.pending.contains_key(key));
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
    fn inbox_error_renewal_requires_real_visibility_or_consumer_generation_change() {
        let mut previous = demand("desktop-or-mobile");
        previous.viewed_row_ids = vec!["answer".into(), "status".into()];
        let mut next = previous.clone();
        next.source_revision += 1;
        next.viewed_row_ids.reverse();
        assert!(
            !task_inbox_view_is_renewed(Some(&previous), &next),
            "streaming/reorder is not a fresh view"
        );
        next.viewed_row_ids = vec!["other".into()];
        assert!(task_inbox_view_is_renewed(Some(&previous), &next));
        next.viewed_row_ids = previous.viewed_row_ids.clone();
        next.generation += 1;
        assert!(task_inbox_view_is_renewed(Some(&previous), &next));
        assert!(task_inbox_view_is_renewed(None, &previous));
    }

    #[test]
    fn visible_task_evidence_uses_only_viewed_semantic_rows_and_exact_terminal_run() {
        use super::semantic::{
            SemanticTimelineRow, SemanticTimelineRowId as Id, SemanticTimelineRowKind as K,
            SemanticTimelineRows,
        };
        let mut input = demand("desktop-or-mobile");
        let mut task: pioneer_protocol::TaskTurnItem = serde_json::from_value(serde_json::json!({
            "id":"task-row", "taskId":"task", "runId":"old-run", "title":"Same title",
            "status":"completed", "attachment":"detached", "triggerKind":"immediate", "executorKind":"tool",
            "depth":0, "maxDepth":1, "createdAt":1, "updatedAt":2
        })).unwrap();
        let block = |task| pioneer_protocol::TimelineBlock {
            workspace_id: "workspace".into(),
            thread_id: "a".into(),
            block_id: "block".into(),
            turn_id: Some("parent-turn".into()),
            sort_key: "key".into(),
            started_at_unix_ms: None,
            updated_at_unix_ms: None,
            kind: pioneer_protocol::TimelineBlockKind::DetachedTaskRun {
                task,
                author: None,
                codex_retry_notices: Vec::new(),
            },
        };
        let id = Id::TopLevelBlock {
            block_id: "block".into(),
        };
        let mut rows = SemanticTimelineRows {
            thread_id: "a".into(),
            rows: vec![SemanticTimelineRow {
                id: id.clone(),
                author: None,
                kind: K::DetachedTaskRun {
                    block: block(task.clone()),
                },
            }],
            request_hints: vec![],
        };
        let ids = HashMap::from([("row".into(), id)]);
        // Opening, prefetch and loading a row into the store provide no evidence.
        assert!(viewed_task_inbox_params("workspace", &input, &rows, &ids).is_none());
        input.viewed_row_ids = vec!["row".into(), "unknown".into()];
        let proof = viewed_task_inbox_params("workspace", &input, &rows, &ids).unwrap();
        assert_eq!(proof.run_ids, ["old-run"]);
        assert!(
            proof.result_turn_ids.is_empty(),
            "never use the containing turn as a run"
        );
        task.run_id = Some("repeat-run".into());
        rows.rows[0].kind = K::DetachedTaskRun {
            block: block(task.clone()),
        };
        assert_eq!(
            viewed_task_inbox_params("workspace", &input, &rows, &ids)
                .unwrap()
                .run_ids,
            ["repeat-run"]
        );
        task.status = pioneer_protocol::TaskStatus::Running;
        rows.rows[0].kind = K::DetachedTaskRun { block: block(task) };
        assert!(viewed_task_inbox_params("workspace", &input, &rows, &ids).is_none());
        let K::DetachedTaskRun { block } = &rows.rows[0].kind else {
            unreachable!()
        };
        let mut answer = block.clone();
        answer.turn_id = Some("exact-result-turn".into());
        answer.kind = pioneer_protocol::TimelineBlockKind::AssistantMessage {
            html_publications: vec![],
            item_id: "answer".into(),
            text: "Result".into(),
            status: pioneer_protocol::TurnWorkItemStatus::Completed,
            markdown: None,
            author: None,
            route: None,
        };
        rows.rows[0].kind = K::AssistantMessage { block: answer };
        let proof = viewed_task_inbox_params("workspace", &input, &rows, &ids).unwrap();
        assert!(proof.run_ids.is_empty());
        assert!(proof.result_turn_ids.is_empty());
        assert_eq!(
            proof.result_items,
            [pioneer_protocol::TaskCompletionInboxViewedResult {
                turn_id: "exact-result-turn".into(),
                item_id: "answer".into()
            }]
        );
        input.viewed_row_ids.clear();
        assert!(
            viewed_task_inbox_params("workspace", &input, &rows, &ids).is_none(),
            "a stored final answer is not proof of viewing"
        );
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
        policy.begin(action.clone()).unwrap().unwrap();
        let generation = policy.request_generation(&key);
        assert!(policy.begin(action).unwrap().is_none()); // one coalesced notification, not another owner
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
                    assert!(policy.begin(action.clone()).unwrap().is_none());
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
                policy.begin(other).unwrap().unwrap();
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
    fn logical_capacity_refuses_whole_new_keys_and_oversized_payloads() {
        let mut policy = ThreadTimelineController::default();
        for index in 0..HISTORY_KEY_LIMIT {
            let id = format!("thread-{index}");
            let action = PageAction::ThreadTimelinePage {
                key: PageKey::ThreadNewest {
                    thread_id: id.clone(),
                },
                params: pioneer_protocol::ThreadTimelinePageParams {
                    thread_id: id,
                    anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                    limit: Some(12),
                },
            };
            assert!(policy.begin(action).unwrap().is_some());
        }
        let refused = page();
        let key = semantic::semantic_timeline_request_key(&refused).clone();
        assert_eq!(policy.begin(refused.clone()), Err(refused));
        assert_eq!(policy.request_generation(&key), 0);
        assert!(!policy.in_flight.contains(&key));
        let mut empty = ThreadTimelineController::default();
        let oversized = PageAction::ThreadTimelinePage {
            key: PageKey::ThreadBefore {
                thread_id: "a".into(),
                cursor: "x".repeat(HISTORY_ACTION_BYTES_LIMIT),
            },
            params: pioneer_protocol::ThreadTimelinePageParams {
                thread_id: "a".into(),
                anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                limit: Some(12),
            },
        };
        assert_eq!(empty.begin(oversized.clone()), Err(oversized));
        assert!(empty.in_flight.is_empty());

        let mut ids = ThreadTimelineController::default();
        let collection = |turn: usize, count: usize| PageAction::TurnWorkItemsGet {
            key: PageKey::TurnWorkItems {
                thread_id: "a".into(),
                turn_id: turn.to_string(),
            },
            params: pioneer_protocol::TurnWorkItemsGetParams {
                thread_id: "a".into(),
                turn_id: turn.to_string(),
                work_item_ids: (0..count).map(|id| format!("item-{id}")).collect(),
            },
        };
        for turn in 0..8 {
            assert!(ids.begin(collection(turn, 4_096)).unwrap().is_some());
        }
        assert!(
            ids.begin(collection(8, 1)).is_err(),
            "total ID payload is bounded across distinct keys"
        );
        assert_eq!(ids.in_flight.len(), 8);
    }

    #[test]
    fn targeted_id_union_at_capacity_keeps_all_accepted_ids_and_refuses_whole_overflow() {
        let mut policy = ThreadTimelineController::default();
        let action = |ids: Vec<String>| PageAction::TurnWorkItemsGet {
            key: PageKey::TurnWorkItems {
                thread_id: "a".into(),
                turn_id: "turn".into(),
            },
            params: pioneer_protocol::TurnWorkItemsGetParams {
                thread_id: "a".into(),
                turn_id: "turn".into(),
                work_item_ids: ids,
            },
        };
        let initial = action(vec!["active".into()]);
        let key = semantic::semantic_timeline_request_key(&initial).clone();
        policy.begin(initial).unwrap().unwrap();
        let accepted = (0..HISTORY_IDS_PER_ACTION - 1)
            .map(|index| format!("item-{index}"))
            .collect::<Vec<_>>();
        assert!(policy.begin(action(accepted.clone())).unwrap().is_none());
        assert!(
            policy
                .begin(action(vec![accepted[0].clone(), "last".into()]))
                .unwrap()
                .is_none()
        );
        let overflow = action(vec!["overflow".into()]);
        assert_eq!(policy.begin(overflow.clone()), Err(overflow));
        let pending = policy.pending.get(&key).unwrap();
        let PageAction::TurnWorkItemsGet { params, .. } = pending else {
            panic!()
        };
        assert_eq!(params.work_item_ids.len(), HISTORY_IDS_PER_ACTION);
        assert!(accepted.iter().all(|id| params.work_item_ids.contains(id)));
        assert!(params.work_item_ids.contains(&"last".into()));
        assert!(!params.work_item_ids.contains(&"overflow".into()));
        let old_generation = policy.request_generation(&key);
        assert!(policy.reject_admission(&key));
        assert!(!policy.accepts(&key, old_generation));
        assert!(!policy.pending.contains_key(&key));
    }

    #[test]
    fn manual_retry_keeps_saved_recovery_reserved_during_readmission_gap() {
        let mut policy = ThreadTimelineController::default();
        let mut saved = page();
        if let PageAction::ThreadTimelinePage { params, .. } = &mut saved {
            params.limit = Some(17);
        }
        let key = semantic::semantic_timeline_request_key(&saved).clone();
        let first = policy.update(demand("visible"), vec![saved.clone()], 0);
        assert_eq!(first.pages.len(), 1);
        assert!(policy.finish(&key, false, 0).is_none());
        let other = |index| PageAction::ThreadTimelinePage {
            key: PageKey::ThreadNewest {
                thread_id: format!("other-{index}"),
            },
            params: pioneer_protocol::ThreadTimelinePageParams {
                thread_id: format!("other-{index}"),
                anchor: pioneer_protocol::TimelinePageAnchor::Newest,
                limit: Some(12),
            },
        };
        for index in 0..HISTORY_KEY_LIMIT - 1 {
            assert!(policy.begin(other(index)).unwrap().is_some());
        }
        // Retry releases its caller's registry lock before presentation/re-admission.
        // A competing key must not take the retained recovery's logical slot.
        assert!(policy.retry("a", "visible", 1).is_some());
        let competitor = other(HISTORY_KEY_LIMIT);
        assert_eq!(policy.begin(competitor.clone()), Err(competitor.clone()));
        assert_eq!(
            policy.next_refresh_deadline(),
            None,
            "Retry has removed the old failure; saved action alone reserves its key"
        );
        // ClientCore performs this within the same registry critical section as
        // Retry, before any presentation lookup can return early.
        policy.reset_page_retry(&key);
        assert_eq!(policy.next_refresh_deadline(), Some(0));
        let next = policy.due_refreshes(0, |_| true);
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].action, saved);
        assert!(next[0].generation > first.pages[0].generation);
        assert!(policy.finish(&key, true, 0).is_none());
        assert_eq!(policy.next_refresh_deadline(), None);
        assert!(
            policy.begin(competitor).unwrap().is_some(),
            "success releases exactly the saved reservation"
        );
    }

    #[test]
    fn saturated_scheduler_defers_exact_recovery_without_consuming_network_retry_budget() {
        let mut policy = ThreadTimelineController::default();
        let action = page();
        let key = semantic::semantic_timeline_request_key(&action).clone();
        policy.begin(action.clone()).unwrap().unwrap();
        let old_generation = policy.request_generation(&key);
        let around = PageAction::ThreadTimelinePage {
            key: key.clone(),
            params: pioneer_protocol::ThreadTimelinePageParams {
                thread_id: "a".into(),
                anchor: pioneer_protocol::TimelinePageAnchor::Oldest,
                limit: Some(12),
            },
        };
        assert!(policy.begin(around.clone()).unwrap().is_none());
        assert!(policy.defer_dispatch(&key, old_generation, 0));
        assert!(!policy.accepts(&key, old_generation));
        assert_eq!(policy.next_refresh_deadline(), Some(50));
        assert!(policy.due_refreshes(49, |_| true).is_empty());
        let retry = policy.due_refreshes(50, |_| true);
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].action, action);
        assert_eq!(policy.pending.get(&key), Some(&around));
        assert_eq!(policy.finish(&key, false, 50), Some(around));
        assert_eq!(policy.next_refresh_deadline(), Some(1_050));
    }

    #[test]
    fn dispatch_demand_uses_all_current_consumers_and_rejects_retired_generations() {
        let mut policy = ThreadTimelineController::default();
        let first = policy.update(demand("desktop"), vec![page()], 0);
        let key = semantic::semantic_timeline_request_key(&first.pages[0].action).clone();
        let generation = first.pages[0].generation;
        policy.update(demand("mobile"), vec![page()], 0);
        assert_eq!(policy.dispatch_priority(&key, generation), Some(true));
        policy.exit("a", "desktop", 1);
        assert_eq!(policy.dispatch_priority(&key, generation), Some(true));
        policy.exit("a", "mobile", 1);
        assert_eq!(policy.dispatch_priority(&key, generation), None);
        let mut renewed = demand("desktop");
        renewed.generation = 2;
        let next = policy.update(renewed, vec![page()], 0);
        assert_eq!(policy.dispatch_priority(&key, generation), None);
        assert_eq!(
            policy.dispatch_priority(&key, next.pages[0].generation),
            Some(true)
        );
        assert!(
            policy
                .update(demand("desktop"), vec![page()], 0)
                .pages
                .is_empty()
        );
        assert_eq!(
            policy.dispatch_priority(&key, next.pages[0].generation),
            Some(true)
        );
    }

    #[test]
    fn read_capacity_refusal_does_not_create_a_loading_obligation() {
        let mut policy = ThreadTimelineController::default();
        for index in 0..HISTORY_KEY_LIMIT {
            let mut input = demand(&format!("consumer-{index}"));
            input.thread_id = format!("thread-{index}");
            assert_eq!(policy.update(input, vec![], 0).reads.len(), 1);
        }
        let refused = policy.update(demand("overflow"), vec![], 0);
        assert!(refused.reads.is_empty());
        assert_eq!(refused.rejected_reads.len(), 1);
        assert!(!policy.accepts_read(&refused.rejected_reads[0]));
        assert_eq!(policy.reads.len(), HISTORY_KEY_LIMIT);
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
        let mut plan = self.plan_timeline_intent(intent, self.timeline_now_ms());
        self.publish_timeline_cancellation(&plan.cancelled);
        for rejected in &plan.rejected {
            self.fail_timeline_admission(
                &rejected.action,
                rejected.store_generation,
                rejected.request_generation,
            );
        }
        if let Some(params) = &plan.inbox_viewed {
            self.task_notification_intent(
                crate::tasks::notifications::TaskNotificationIntent::Viewed {
                    params: params.clone(),
                },
            );
        }
        for page in &plan.pages {
            plan.dispatch_failed |= self
                .schedule_planned_timeline_request(page.clone())
                .is_err();
        }
        for read in &plan.reads {
            plan.dispatch_failed |= !self.schedule_timeline_read(read.clone());
        }
        // A demand-only transition can change the order of existing jobs.
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread sender poisoned")
            .clone();
        if let Some(sender) = sender {
            let _ = sender.send(crate::threads::registry::ThreadControllerRequest::Resume);
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
                if let Some(saved) = &retry_refresh {
                    // Restore the reset budget's deadline before releasing this
                    // lock. Presentation can be missing/stale on re-admission.
                    registry
                        .timeline
                        .reset_page_retry(semantic::semantic_timeline_request_key(saved));
                }
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
        let Some(source) = self.thread_snapshot(&demand.thread_id) else {
            return TimelineEffectPlan::default();
        };
        let workspace = source.coordinator().workspace_id.clone();
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
        if !registry.timeline_revision_matches(&demand.thread_id, demand.source_revision)
            || !registry.timeline_workspace_matches(&demand.thread_id, &workspace)
        {
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
        let current_demand = demand.clone();
        let renewed_view = task_inbox_view_is_renewed(
            registry
                .timeline
                .demands
                .get(&(demand.thread_id.clone(), demand.consumer_id.clone())),
            &demand,
        );
        let changed_demand = registry
            .timeline
            .demands
            .get(&(demand.thread_id.clone(), demand.consumer_id.clone()))
            != Some(&demand);
        // Reserve the exact saved action before demand planning can admit a Newest
        // with freshly planned parameters for the same logical key.
        let retry_request = retry_refresh.and_then(|action| {
            let key = semantic::semantic_timeline_request_key(&action).clone();
            if registry
                .timeline
                .has_active_refresh(request_thread_id(&key))
            {
                return None;
            }
            registry.timeline.reset_page_retry(&key);
            registry.timeline.retain_domain_request(&key);
            match registry.timeline.begin(action) {
                Ok(Some(action)) => Some(Ok(TimelinePageRequest {
                    action,
                    generation: registry.timeline.request_generation(&key),
                })),
                Ok(None) => None,
                Err(action) => Some(Err(TimelinePageRejection {
                    request_generation: registry.timeline.request_generation(&key),
                    action,
                    store_generation: 0,
                })),
            }
        });
        let mut plan = registry.timeline.update(demand, actions, now_ms);
        match retry_request {
            Some(Ok(page)) => plan.pages.insert(0, page),
            Some(Err(rejected)) => plan.rejected.push(rejected),
            None => {}
        }
        if changed_demand
            && registry.timeline.demands.get(&(
                current_demand.thread_id.clone(),
                current_demand.consumer_id.clone(),
            )) == Some(&current_demand)
        {
            plan.inbox_viewed = viewed_task_inbox_params(
                &workspace,
                &current_demand,
                &semantic_rows,
                &semantic_ids,
            );
        }
        for rejected in &mut plan.rejected {
            rejected.store_generation = registry.timeline_store_generation(request_thread_id(
                semantic::semantic_timeline_request_key(&rejected.action),
            ));
        }
        // Keep the registry -> notifications and navigation -> registry paths
        // from owning the two stores in opposite orders.
        drop(registry);
        if renewed_view && let Some(params) = &plan.inbox_viewed {
            self.renew_task_inbox_view(params);
        }
        plan
    }

    pub(crate) fn refresh_viewed_task_inbox(&self, workspace: &str) {
        let demands = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .timeline
            .demands
            .values()
            .filter(|demand| !demand.viewed_row_ids.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        for demand in demands {
            let Some(source) = self.thread_snapshot(&demand.thread_id) else {
                continue;
            };
            if source.coordinator().workspace_id.as_str() != workspace {
                continue;
            }
            let Some(snapshot) = self.thread_presentation_snapshot(&demand.thread_id) else {
                continue;
            };
            let timeline = snapshot.timeline();
            if timeline.source_revision() != demand.source_revision {
                continue;
            }
            if let Some(params) = viewed_task_inbox_params(
                workspace,
                &demand,
                &timeline.semantic_rows(),
                &timeline.semantic_row_ids(),
            ) {
                self.task_notification_intent(
                    crate::tasks::notifications::TaskNotificationIntent::Viewed { params },
                );
            }
        }
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
            let _ = self.schedule_planned_timeline_request(page);
        }
        let reads = self
            .thread_registry
            .lock()
            .expect("thread registry poisoned")
            .timeline
            .due_reads(now_ms);
        for read in reads {
            self.schedule_timeline_read(read);
        }
    }

    fn schedule_timeline_read(&self, read: TimelineReadRequest) -> bool {
        let sender = self
            .thread_request_sender
            .lock()
            .expect("thread sender poisoned")
            .clone();
        if sender.is_some_and(|sender| {
            sender
                .send(crate::threads::registry::ThreadControllerRequest::Read(
                    read.clone(),
                ))
                .is_ok()
        }) {
            return true;
        }
        self.thread_registry
            .lock()
            .expect("thread registry poisoned")
            .timeline
            .finish_read_with_retry(&read, false, false, self.timeline_now_ms());
        false
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
