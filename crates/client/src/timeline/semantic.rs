//! Shared semantic timeline state.
//!
//! This module is intentionally platform-neutral. Desktop and mobile shells can
//! keep scroll/list/rendering state locally, but semantic block and work-range
//! cache ownership lives here.

use crate::conversation::ConversationEvent;
use pioneer_protocol::{
    AgentMessagePhase, ItemDeltaStream, MarkdownDocument, SystemEventLevel,
    ThreadTimelineBlocksChangedNotification, ThreadTimelinePageParams, ThreadTimelinePageResponse,
    TimelineBlock, TimelineBlockKind, TimelineCursor, TimelinePageAnchor, TimelinePageInfo, Turn,
    TurnItem, TurnStatus, TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus,
    TurnWorkItemsChangedNotification, TurnWorkPageParams, TurnWorkPageResponse,
    TurnWorkPresentation, TurnWorkState, TurnWorkStateChangedNotification, UserMessageAttachment,
};
use std::collections::{HashMap, HashSet};

pub const DEFAULT_TOP_LEVEL_PAGE_LIMIT: u32 = 12;
pub const DEFAULT_TURN_WORK_PAGE_LIMIT: u32 = 30;
pub const DEFAULT_PREFETCH_THRESHOLD_ROWS: usize = 3;

pub type ThreadId = String;
pub type TurnId = String;
pub type TimelineBlockId = String;
pub type TurnWorkItemId = String;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum SemanticTimelineRowId {
    TopLevelBlock {
        block_id: TimelineBlockId,
    },
    TurnWorkItem {
        turn_id: TurnId,
        work_item_id: TurnWorkItemId,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct SemanticTimelineRow {
    pub id: SemanticTimelineRowId,
    pub kind: SemanticTimelineRowKind,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum SemanticTimelineRowKind {
    UserBlock {
        block: TimelineBlock,
    },
    WorkHeader {
        block: TimelineBlock,
        work: TurnWorkBlock,
        expanded: bool,
        loaded_range: Option<TimelineLoadedRange>,
    },
    WorkItem {
        item: TurnWorkItem,
    },
    AssistantMessage {
        block: TimelineBlock,
    },
    PendingRequest {
        block: TimelineBlock,
    },
    TurnState {
        block: TimelineBlock,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SemanticTimelineRequestHint {
    TopLevelBefore {
        thread_id: ThreadId,
        cursor: TimelineCursor,
    },
    TopLevelAfter {
        thread_id: ThreadId,
        cursor: TimelineCursor,
    },
    TurnWorkInitial {
        thread_id: ThreadId,
        turn_id: TurnId,
    },
    TurnWorkBefore {
        thread_id: ThreadId,
        turn_id: TurnId,
        cursor: TimelineCursor,
    },
    TurnWorkAfter {
        thread_id: ThreadId,
        turn_id: TurnId,
        cursor: TimelineCursor,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct SemanticTimelineRows {
    pub thread_id: ThreadId,
    pub rows: Vec<SemanticTimelineRow>,
    pub request_hints: Vec<SemanticTimelineRequestHint>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct SemanticTimelineCachePatch {
    pub workspace_id: String,
    pub thread_id: ThreadId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_blocks: Vec<TimelineBlock>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_block_ids: Vec<TimelineBlockId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_work_items: Vec<TurnWorkItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_work_items: Vec<SemanticTimelineRemovedWorkItem>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SemanticTimelineRemovedWorkItem {
    pub turn_id: TurnId,
    pub work_item_id: TurnWorkItemId,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum SemanticTimelineRequestKey {
    ThreadNewest {
        thread_id: ThreadId,
    },
    ThreadBefore {
        thread_id: ThreadId,
        cursor: String,
    },
    ThreadAfter {
        thread_id: ThreadId,
        cursor: String,
    },
    TurnWorkInitial {
        thread_id: ThreadId,
        turn_id: TurnId,
    },
    TurnWorkBefore {
        thread_id: ThreadId,
        turn_id: TurnId,
        cursor: String,
    },
    TurnWorkAfter {
        thread_id: ThreadId,
        turn_id: TurnId,
        cursor: String,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SemanticTimelineRequestAction {
    ThreadTimelinePage {
        key: SemanticTimelineRequestKey,
        params: ThreadTimelinePageParams,
    },
    TurnWorkPage {
        key: SemanticTimelineRequestKey,
        params: TurnWorkPageParams,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SemanticTimelineVisibleRow {
    pub row_id: SemanticTimelineRowId,
    pub top_offset_px: i32,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SemanticTimelineStableAnchor {
    pub row_id: SemanticTimelineRowId,
    pub top_offset_px: i32,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SemanticTimelineRequestPlannerInput {
    pub visible_rows: Vec<SemanticTimelineVisibleRow>,
    pub leading_threshold_rows: usize,
    pub trailing_threshold_rows: usize,
    pub top_level_limit: u32,
    pub turn_work_limit: u32,
    pub in_flight: HashSet<SemanticTimelineRequestKey>,
}

impl Default for SemanticTimelineRequestPlannerInput {
    fn default() -> Self {
        Self {
            visible_rows: Vec::new(),
            leading_threshold_rows: DEFAULT_PREFETCH_THRESHOLD_ROWS,
            trailing_threshold_rows: DEFAULT_PREFETCH_THRESHOLD_ROWS,
            top_level_limit: DEFAULT_TOP_LEVEL_PAGE_LIMIT,
            turn_work_limit: DEFAULT_TURN_WORK_PAGE_LIMIT,
            in_flight: HashSet::new(),
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SemanticTimelineRequestPlan {
    pub anchor: Option<SemanticTimelineStableAnchor>,
    pub actions: Vec<SemanticTimelineRequestAction>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SemanticTimelineLiveUpdate {
    ThreadTimelineBlocksChanged(ThreadTimelineBlocksChangedNotification),
    TurnWorkItemsChanged(TurnWorkItemsChangedNotification),
    TurnWorkStateChanged(TurnWorkStateChangedNotification),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TimelineRequestStatus {
    Idle,
    Loading { request_key: String },
    Ready,
    Failed { message: String },
}

impl Default for TimelineRequestStatus {
    fn default() -> Self {
        Self::Idle
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TopLevelPageMergeMode {
    Reset,
    Merge,
    MergeBefore,
    MergeAfter,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum WorkPageMergeMode {
    Reset,
    Merge,
    MergeBefore,
    MergeAfter,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineLoadedRange {
    pub before_cursor: Option<TimelineCursor>,
    pub after_cursor: Option<TimelineCursor>,
    pub has_more_before: bool,
    pub has_more_after: bool,
}

impl From<TimelinePageInfo> for TimelineLoadedRange {
    fn from(page: TimelinePageInfo) -> Self {
        Self {
            before_cursor: page.before_cursor,
            after_cursor: page.after_cursor,
            has_more_before: page.has_more_before,
            has_more_after: page.has_more_after,
        }
    }
}

impl From<&TimelinePageInfo> for TimelineLoadedRange {
    fn from(page: &TimelinePageInfo) -> Self {
        Self {
            before_cursor: page.before_cursor.clone(),
            after_cursor: page.after_cursor.clone(),
            has_more_before: page.has_more_before,
            has_more_after: page.has_more_after,
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct TopLevelTimelineCache {
    pub blocks_by_id: HashMap<TimelineBlockId, TimelineBlock>,
    pub ordered_block_ids: Vec<TimelineBlockId>,
    pub stale_block_ids: HashSet<TimelineBlockId>,
    pub loaded_range: TimelineLoadedRange,
    pub request_status: TimelineRequestStatus,
}

impl TopLevelTimelineCache {
    pub fn block(&self, block_id: &str) -> Option<&TimelineBlock> {
        self.blocks_by_id.get(block_id)
    }

    pub fn ordered_blocks(&self) -> impl Iterator<Item = &TimelineBlock> {
        self.ordered_block_ids
            .iter()
            .filter_map(|block_id| self.blocks_by_id.get(block_id))
    }

    pub fn is_empty(&self) -> bool {
        self.ordered_block_ids.is_empty()
    }

    pub fn stale_block_ids(&self) -> Vec<&str> {
        let mut ids = self
            .stale_block_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub fn take_stale_block_ids(&mut self) -> Vec<TimelineBlockId> {
        let mut ids = self.stale_block_ids.drain().collect::<Vec<_>>();
        ids.sort();
        ids
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct TurnWorkRangeCache {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub work: Option<TurnWorkBlock>,
    pub items_by_id: HashMap<TurnWorkItemId, TurnWorkItem>,
    pub ordered_item_ids: Vec<TurnWorkItemId>,
    pub stale_work_item_ids: HashSet<TurnWorkItemId>,
    pub loaded_range: TimelineLoadedRange,
    pub request_status: TimelineRequestStatus,
}

impl TurnWorkRangeCache {
    pub fn item(&self, work_item_id: &str) -> Option<&TurnWorkItem> {
        self.items_by_id.get(work_item_id)
    }

    pub fn ordered_items(&self) -> impl Iterator<Item = &TurnWorkItem> {
        self.ordered_item_ids
            .iter()
            .filter_map(|work_item_id| self.items_by_id.get(work_item_id))
    }

    pub fn is_empty(&self) -> bool {
        self.ordered_item_ids.is_empty()
    }

    pub fn stale_work_item_ids(&self) -> Vec<&str> {
        let mut ids = self
            .stale_work_item_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub fn take_stale_work_item_ids(&mut self) -> Vec<TurnWorkItemId> {
        let mut ids = self.stale_work_item_ids.drain().collect::<Vec<_>>();
        ids.sort();
        ids
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TurnWorkExpansionDecision {
    ProtocolDefault,
    Expanded,
    Collapsed,
}

impl Default for TurnWorkExpansionDecision {
    fn default() -> Self {
        Self::ProtocolDefault
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineExpansionState {
    pub expanded_turn_work: HashSet<TurnId>,
    pub collapsed_turn_work: HashSet<TurnId>,
}

impl TimelineExpansionState {
    pub fn decision_for_turn(&self, turn_id: &str) -> TurnWorkExpansionDecision {
        if self.expanded_turn_work.contains(turn_id) {
            TurnWorkExpansionDecision::Expanded
        } else if self.collapsed_turn_work.contains(turn_id) {
            TurnWorkExpansionDecision::Collapsed
        } else {
            TurnWorkExpansionDecision::ProtocolDefault
        }
    }

    pub fn set_turn_work_expanded(&mut self, turn_id: impl Into<String>, expanded: bool) {
        let turn_id = turn_id.into();
        if expanded {
            self.collapsed_turn_work.remove(turn_id.as_str());
            self.expanded_turn_work.insert(turn_id);
        } else {
            self.expanded_turn_work.remove(turn_id.as_str());
            self.collapsed_turn_work.insert(turn_id);
        }
    }

    pub fn clear_turn_work_override(&mut self, turn_id: &str) {
        self.expanded_turn_work.remove(turn_id);
        self.collapsed_turn_work.remove(turn_id);
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ThreadSemanticTimelineState {
    pub thread_id: ThreadId,
    pub top_level: TopLevelTimelineCache,
    pub work_ranges_by_turn: HashMap<TurnId, TurnWorkRangeCache>,
    pub expansion: TimelineExpansionState,
}

impl ThreadSemanticTimelineState {
    pub fn new(thread_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            ..Self::default()
        }
    }

    pub fn work_range(&self, turn_id: &str) -> Option<&TurnWorkRangeCache> {
        self.work_ranges_by_turn.get(turn_id)
    }

    pub fn work_range_mut(&mut self, turn_id: impl Into<String>) -> &mut TurnWorkRangeCache {
        let turn_id = turn_id.into();
        self.work_ranges_by_turn
            .entry(turn_id.clone())
            .or_insert_with(|| TurnWorkRangeCache {
                thread_id: self.thread_id.clone(),
                turn_id,
                ..TurnWorkRangeCache::default()
            })
    }

    pub fn cached_turn_work_block(&self, turn_id: &str) -> Option<&TurnWorkBlock> {
        self.work_ranges_by_turn
            .get(turn_id)
            .and_then(|range| range.work.as_ref())
            .or_else(|| {
                self.top_level
                    .ordered_blocks()
                    .find_map(|block| match &block.kind {
                        TimelineBlockKind::TurnWork { work } if work.turn_id == turn_id => {
                            Some(work)
                        }
                        _ => None,
                    })
            })
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct SemanticTimelineState {
    pub threads_by_id: HashMap<ThreadId, ThreadSemanticTimelineState>,
}

impl SemanticTimelineState {
    pub fn thread(&self, thread_id: &str) -> Option<&ThreadSemanticTimelineState> {
        self.threads_by_id.get(thread_id)
    }

    pub fn thread_mut(&mut self, thread_id: impl Into<String>) -> &mut ThreadSemanticTimelineState {
        let thread_id = thread_id.into();
        self.threads_by_id
            .entry(thread_id.clone())
            .or_insert_with(|| ThreadSemanticTimelineState::new(thread_id))
    }
}

pub fn top_level_cache_from_page(page: ThreadTimelinePageResponse) -> TopLevelTimelineCache {
    let mut blocks_by_id = HashMap::with_capacity(page.blocks.len());
    let mut ordered_block_ids = Vec::with_capacity(page.blocks.len());
    for block in page.blocks {
        ordered_block_ids.push(block.block_id.clone());
        blocks_by_id.insert(block.block_id.clone(), block);
    }
    TopLevelTimelineCache {
        blocks_by_id,
        ordered_block_ids,
        stale_block_ids: HashSet::new(),
        loaded_range: page.page.into(),
        request_status: TimelineRequestStatus::Ready,
    }
}

pub fn turn_work_range_from_page(page: TurnWorkPageResponse) -> TurnWorkRangeCache {
    let mut items_by_id = HashMap::with_capacity(page.items.len());
    let mut ordered_item_ids = Vec::with_capacity(page.items.len());
    for item in page.items {
        ordered_item_ids.push(item.work_item_id.clone());
        items_by_id.insert(item.work_item_id.clone(), item);
    }
    let mut range = TurnWorkRangeCache {
        thread_id: page.thread_id,
        turn_id: page.turn_id,
        work: Some(page.work),
        items_by_id,
        ordered_item_ids,
        stale_work_item_ids: HashSet::new(),
        loaded_range: page.page.into(),
        request_status: TimelineRequestStatus::Ready,
    };
    sort_work_items(&mut range);
    range
}

pub fn protocol_default_work_expanded(work: &TurnWorkBlock) -> bool {
    matches!(
        work.presentation,
        TurnWorkPresentation::ExpandedLive | TurnWorkPresentation::ExpandedTerminalNoFinal
    )
}

pub fn resolve_work_expanded(work: &TurnWorkBlock, expansion: &TimelineExpansionState) -> bool {
    match expansion.decision_for_turn(work.turn_id.as_str()) {
        TurnWorkExpansionDecision::ProtocolDefault => protocol_default_work_expanded(work),
        TurnWorkExpansionDecision::Expanded => true,
        TurnWorkExpansionDecision::Collapsed => false,
    }
}

pub fn block_turn_id(block: &TimelineBlock) -> Option<&str> {
    match &block.kind {
        TimelineBlockKind::TurnWork { work } => Some(work.turn_id.as_str()),
        _ => block.turn_id.as_deref(),
    }
}

pub fn flatten_thread_semantic_timeline(
    thread: &ThreadSemanticTimelineState,
) -> SemanticTimelineRows {
    let mut rows = Vec::new();
    let mut request_hints = Vec::new();
    push_top_level_request_hints(thread, &mut request_hints);

    for block in thread.top_level.ordered_blocks() {
        match &block.kind {
            TimelineBlockKind::UserMessage { .. } => rows.push(SemanticTimelineRow {
                id: SemanticTimelineRowId::TopLevelBlock {
                    block_id: block.block_id.clone(),
                },
                kind: SemanticTimelineRowKind::UserBlock {
                    block: block.clone(),
                },
            }),
            TimelineBlockKind::TurnWork { work } => {
                let expanded = resolve_work_expanded(work, &thread.expansion);
                let work_range = thread.work_range(work.turn_id.as_str());
                rows.push(SemanticTimelineRow {
                    id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: block.block_id.clone(),
                    },
                    kind: SemanticTimelineRowKind::WorkHeader {
                        block: block.clone(),
                        work: work.clone(),
                        expanded,
                        loaded_range: work_range.map(|range| range.loaded_range.clone()),
                    },
                });
                if expanded {
                    push_turn_work_rows_and_hints(
                        thread.thread_id.as_str(),
                        work,
                        work_range,
                        &mut rows,
                        &mut request_hints,
                    );
                }
            }
            TimelineBlockKind::AssistantMessage { .. } => rows.push(SemanticTimelineRow {
                id: SemanticTimelineRowId::TopLevelBlock {
                    block_id: block.block_id.clone(),
                },
                kind: SemanticTimelineRowKind::AssistantMessage {
                    block: block.clone(),
                },
            }),
            TimelineBlockKind::PendingRequest { .. } => rows.push(SemanticTimelineRow {
                id: SemanticTimelineRowId::TopLevelBlock {
                    block_id: block.block_id.clone(),
                },
                kind: SemanticTimelineRowKind::PendingRequest {
                    block: block.clone(),
                },
            }),
            TimelineBlockKind::TurnState { .. } => rows.push(SemanticTimelineRow {
                id: SemanticTimelineRowId::TopLevelBlock {
                    block_id: block.block_id.clone(),
                },
                kind: SemanticTimelineRowKind::TurnState {
                    block: block.clone(),
                },
            }),
        }
    }

    SemanticTimelineRows {
        thread_id: thread.thread_id.clone(),
        rows,
        request_hints,
    }
}

pub fn flatten_semantic_timeline(
    state: &SemanticTimelineState,
    thread_id: &str,
) -> Option<SemanticTimelineRows> {
    state
        .thread(thread_id)
        .map(flatten_thread_semantic_timeline)
}

pub fn plan_semantic_timeline_requests(
    thread: &ThreadSemanticTimelineState,
    input: &SemanticTimelineRequestPlannerInput,
) -> SemanticTimelineRequestPlan {
    let flattened = flatten_thread_semantic_timeline(thread);
    plan_semantic_timeline_requests_from_rows(&flattened, input)
}

pub fn plan_semantic_timeline_requests_from_rows(
    flattened: &SemanticTimelineRows,
    input: &SemanticTimelineRequestPlannerInput,
) -> SemanticTimelineRequestPlan {
    let anchor = input
        .visible_rows
        .first()
        .map(|row| SemanticTimelineStableAnchor {
            row_id: row.row_id.clone(),
            top_offset_px: row.top_offset_px,
        });
    let row_positions = flattened
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| (row.id.clone(), index))
        .collect::<HashMap<_, _>>();
    let visible_positions = input
        .visible_rows
        .iter()
        .filter_map(|row| row_positions.get(&row.row_id).copied())
        .collect::<Vec<_>>();
    if visible_positions.is_empty() {
        return SemanticTimelineRequestPlan {
            anchor,
            actions: Vec::new(),
        };
    }

    let min_visible = *visible_positions
        .iter()
        .min()
        .expect("visible positions should not be empty");
    let max_visible = *visible_positions
        .iter()
        .max()
        .expect("visible positions should not be empty");
    let mut actions = Vec::new();
    let mut planned = HashSet::<SemanticTimelineRequestKey>::new();

    for hint in &flattened.request_hints {
        match hint {
            SemanticTimelineRequestHint::TopLevelBefore { thread_id, cursor } => {
                if min_visible <= input.leading_threshold_rows {
                    push_request_action(
                        &mut actions,
                        &mut planned,
                        &input.in_flight,
                        SemanticTimelineRequestAction::ThreadTimelinePage {
                            key: SemanticTimelineRequestKey::ThreadBefore {
                                thread_id: thread_id.clone(),
                                cursor: cursor.value.clone(),
                            },
                            params: ThreadTimelinePageParams {
                                thread_id: thread_id.clone(),
                                anchor: TimelinePageAnchor::Before {
                                    cursor: cursor.clone(),
                                },
                                limit: Some(input.top_level_limit),
                            },
                        },
                    );
                }
            }
            SemanticTimelineRequestHint::TopLevelAfter { thread_id, cursor } => {
                let trailing_boundary = flattened
                    .rows
                    .len()
                    .saturating_sub(1)
                    .saturating_sub(input.trailing_threshold_rows);
                if max_visible >= trailing_boundary {
                    push_request_action(
                        &mut actions,
                        &mut planned,
                        &input.in_flight,
                        SemanticTimelineRequestAction::ThreadTimelinePage {
                            key: SemanticTimelineRequestKey::ThreadAfter {
                                thread_id: thread_id.clone(),
                                cursor: cursor.value.clone(),
                            },
                            params: ThreadTimelinePageParams {
                                thread_id: thread_id.clone(),
                                anchor: TimelinePageAnchor::After {
                                    cursor: cursor.clone(),
                                },
                                limit: Some(input.top_level_limit),
                            },
                        },
                    );
                }
            }
            SemanticTimelineRequestHint::TurnWorkInitial { thread_id, turn_id } => {
                if turn_span_is_visible(flattened, turn_id.as_str(), &visible_positions) {
                    push_request_action(
                        &mut actions,
                        &mut planned,
                        &input.in_flight,
                        SemanticTimelineRequestAction::TurnWorkPage {
                            key: SemanticTimelineRequestKey::TurnWorkInitial {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                            },
                            params: TurnWorkPageParams {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                anchor: TimelinePageAnchor::Newest,
                                limit: Some(input.turn_work_limit),
                            },
                        },
                    );
                }
            }
            SemanticTimelineRequestHint::TurnWorkBefore {
                thread_id,
                turn_id,
                cursor,
            } => {
                if turn_span_near_leading(
                    flattened,
                    turn_id.as_str(),
                    &visible_positions,
                    input.leading_threshold_rows,
                ) {
                    push_request_action(
                        &mut actions,
                        &mut planned,
                        &input.in_flight,
                        SemanticTimelineRequestAction::TurnWorkPage {
                            key: SemanticTimelineRequestKey::TurnWorkBefore {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                cursor: cursor.value.clone(),
                            },
                            params: TurnWorkPageParams {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                anchor: TimelinePageAnchor::Before {
                                    cursor: cursor.clone(),
                                },
                                limit: Some(input.turn_work_limit),
                            },
                        },
                    );
                }
            }
            SemanticTimelineRequestHint::TurnWorkAfter {
                thread_id,
                turn_id,
                cursor,
            } => {
                if turn_span_near_trailing(
                    flattened,
                    turn_id.as_str(),
                    &visible_positions,
                    input.trailing_threshold_rows,
                ) {
                    push_request_action(
                        &mut actions,
                        &mut planned,
                        &input.in_flight,
                        SemanticTimelineRequestAction::TurnWorkPage {
                            key: SemanticTimelineRequestKey::TurnWorkAfter {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                cursor: cursor.value.clone(),
                            },
                            params: TurnWorkPageParams {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                anchor: TimelinePageAnchor::After {
                                    cursor: cursor.clone(),
                                },
                                limit: Some(input.turn_work_limit),
                            },
                        },
                    );
                }
            }
        }
    }

    SemanticTimelineRequestPlan { anchor, actions }
}

pub fn apply_thread_timeline_page(
    state: &mut SemanticTimelineState,
    page: ThreadTimelinePageResponse,
    merge_mode: TopLevelPageMergeMode,
) -> bool {
    let thread_id = page.thread_id.clone();
    let thread = state.thread_mut(thread_id);
    apply_top_level_page(&mut thread.top_level, page, merge_mode)
}

pub fn apply_top_level_page(
    cache: &mut TopLevelTimelineCache,
    page: ThreadTimelinePageResponse,
    merge_mode: TopLevelPageMergeMode,
) -> bool {
    let before = cache.clone();
    let was_empty = cache.ordered_block_ids.is_empty();

    if merge_mode == TopLevelPageMergeMode::Reset {
        cache.blocks_by_id.clear();
        cache.ordered_block_ids.clear();
        cache.stale_block_ids.clear();
    }

    for block in page.blocks {
        cache.stale_block_ids.remove(block.block_id.as_str());
        cache.blocks_by_id.insert(block.block_id.clone(), block);
    }
    sort_top_level_blocks(cache);
    merge_top_level_loaded_range(cache, &page.page, merge_mode, was_empty);
    cache.request_status = TimelineRequestStatus::Ready;

    before != *cache
}

pub fn apply_thread_timeline_blocks_changed(
    state: &mut SemanticTimelineState,
    notification: ThreadTimelineBlocksChangedNotification,
) -> bool {
    let thread = state.thread_mut(notification.thread_id);
    let cache = &mut thread.top_level;
    let before = cache.clone();

    for block_id in notification.removed_block_ids {
        cache.blocks_by_id.remove(block_id.as_str());
        cache.stale_block_ids.remove(block_id.as_str());
    }
    if !cache.ordered_block_ids.is_empty() {
        cache
            .ordered_block_ids
            .retain(|block_id| cache.blocks_by_id.contains_key(block_id));
    }
    for block_id in notification.changed_block_ids {
        cache.stale_block_ids.insert(block_id);
    }
    cache.request_status = TimelineRequestStatus::Ready;

    before != *cache
}

pub fn apply_semantic_timeline_live_update(
    state: &mut SemanticTimelineState,
    update: SemanticTimelineLiveUpdate,
) -> bool {
    match update {
        SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(notification) => {
            apply_thread_timeline_blocks_changed(state, notification)
        }
        SemanticTimelineLiveUpdate::TurnWorkItemsChanged(notification) => {
            apply_turn_work_items_changed(state, notification)
        }
        SemanticTimelineLiveUpdate::TurnWorkStateChanged(notification) => {
            apply_turn_work_state_changed(state, notification)
        }
    }
}

pub fn apply_conversation_event_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    event: &ConversationEvent,
    now_unix_ms: i64,
) -> bool {
    match event {
        ConversationEvent::LocalTurnStartRequested {
            thread_id,
            turn_id,
            user_text,
            attachments,
            ..
        } => apply_local_turn_start_requested_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            user_text,
            attachments,
            now_unix_ms,
        ),
        ConversationEvent::LocalTurnStartAccepted {
            thread_id, turn_id, ..
        }
        | ConversationEvent::TurnStarted {
            thread_id,
            turn: Turn { id: turn_id, .. },
        } => apply_turn_state_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            TurnWorkState::Running,
            None,
            now_unix_ms,
        ),
        ConversationEvent::LocalTurnStartRejected {
            thread_id, turn_id, ..
        } => apply_turn_state_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            TurnWorkState::Failed,
            Some(now_unix_ms),
            now_unix_ms,
        ),
        ConversationEvent::TurnCompleted { thread_id, turn } => {
            apply_terminal_turn_to_semantic_timeline(
                state,
                workspace_id,
                thread_id,
                turn,
                TurnWorkState::Completed,
                now_unix_ms,
            )
        }
        ConversationEvent::TurnFailed { thread_id, turn } => {
            apply_terminal_turn_to_semantic_timeline(
                state,
                workspace_id,
                thread_id,
                turn,
                TurnWorkState::Failed,
                now_unix_ms,
            )
        }
        ConversationEvent::TurnBlocked {
            thread_id, turn, ..
        } => apply_terminal_turn_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn,
            TurnWorkState::Blocked,
            now_unix_ms,
        ),
        ConversationEvent::ItemStarted {
            thread_id,
            turn_id,
            item,
        } => apply_item_started_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            item,
            now_unix_ms,
        ),
        ConversationEvent::ItemDelta {
            thread_id,
            turn_id,
            item_id,
            delta,
            stream,
            markdown,
            ..
        } => apply_item_delta_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            delta,
            *stream,
            markdown.as_ref(),
            now_unix_ms,
        ),
        ConversationEvent::ItemCompleted {
            thread_id,
            turn_id,
            item,
        }
        | ConversationEvent::ItemUpdated {
            thread_id,
            turn_id,
            item,
        } => apply_item_completed_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            item,
            now_unix_ms,
        ),
        _ => false,
    }
}

pub fn apply_conversation_event_to_semantic_timeline_with_patch(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    event: &ConversationEvent,
    now_unix_ms: i64,
) -> SemanticTimelineCachePatch {
    let Some(thread_id) = event.thread_id().map(str::to_owned) else {
        return SemanticTimelineCachePatch::default();
    };
    let before = state.thread(thread_id.as_str()).cloned();
    if !apply_conversation_event_to_semantic_timeline(state, workspace_id, event, now_unix_ms) {
        return SemanticTimelineCachePatch {
            workspace_id: workspace_id.to_owned(),
            thread_id,
            ..SemanticTimelineCachePatch::default()
        };
    }
    let after = state.thread(thread_id.as_str());
    semantic_timeline_cache_patch_from_diff(workspace_id, thread_id, before.as_ref(), after)
}

fn apply_local_turn_start_requested_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    user_text: &str,
    attachments: &[UserMessageAttachment],
    now_unix_ms: i64,
) -> bool {
    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    upsert_user_message_block(
        thread,
        workspace_id,
        thread_id,
        turn_id,
        None,
        user_text.to_owned(),
        attachments.to_vec(),
        now_unix_ms,
    );
    upsert_turn_work_summary(
        thread,
        workspace_id,
        thread_id,
        turn_id,
        TurnWorkState::Starting,
        TurnWorkPresentation::ExpandedLive,
        None,
        now_unix_ms,
    );
    before != *thread
}

fn apply_turn_state_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    work_state: TurnWorkState,
    completed_at_unix_ms: Option<i64>,
    now_unix_ms: i64,
) -> bool {
    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    let presentation = if turn_has_assistant_block(thread, turn_id) {
        TurnWorkPresentation::CollapsedAfterFinal
    } else if completed_at_unix_ms.is_some() {
        TurnWorkPresentation::ExpandedTerminalNoFinal
    } else {
        TurnWorkPresentation::ExpandedLive
    };
    upsert_turn_work_summary(
        thread,
        workspace_id,
        thread_id,
        turn_id,
        work_state,
        presentation,
        completed_at_unix_ms,
        now_unix_ms,
    );
    before != *thread
}

fn apply_terminal_turn_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn: &Turn,
    fallback_state: TurnWorkState,
    now_unix_ms: i64,
) -> bool {
    let work_state = turn_status_to_work_state(turn.status).unwrap_or(fallback_state);
    apply_turn_state_to_semantic_timeline(
        state,
        workspace_id,
        thread_id,
        turn.id.as_str(),
        work_state,
        Some(now_unix_ms),
        now_unix_ms,
    )
}

fn apply_item_started_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item: &TurnItem,
    now_unix_ms: i64,
) -> bool {
    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    match live_item_placement(item) {
        LiveItemPlacement::TopLevelUser => {
            let TurnItem::UserMessage {
                id,
                text,
                attachments,
            } = item
            else {
                return false;
            };
            upsert_user_message_block(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                Some(id.clone()),
                text.clone(),
                attachments.clone(),
                now_unix_ms,
            );
        }
        LiveItemPlacement::TopLevelAssistant => {
            upsert_assistant_message_block(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                item,
                TurnWorkItemStatus::Running,
                now_unix_ms,
            );
            upsert_turn_work_summary(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                TurnWorkState::Running,
                TurnWorkPresentation::CollapsedAfterFinal,
                None,
                now_unix_ms,
            );
        }
        LiveItemPlacement::TurnWork => {
            upsert_turn_work_item(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                item.clone(),
                TurnWorkItemStatus::Running,
                now_unix_ms,
            );
            upsert_turn_work_summary(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                TurnWorkState::Running,
                current_or_live_work_presentation(thread, turn_id),
                None,
                now_unix_ms,
            );
        }
        LiveItemPlacement::Hidden => {}
    }
    before != *thread
}

fn apply_item_delta_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    delta: &str,
    stream: Option<ItemDeltaStream>,
    markdown: Option<&MarkdownDocument>,
    now_unix_ms: i64,
) -> bool {
    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    let assistant_block_id = assistant_block_id(turn_id, item_id);
    if let Some(block) = thread
        .top_level
        .blocks_by_id
        .get_mut(assistant_block_id.as_str())
        && let TimelineBlockKind::AssistantMessage {
            text,
            status,
            markdown: block_markdown,
            ..
        } = &mut block.kind
    {
        text.push_str(delta);
        *status = TurnWorkItemStatus::Running;
        if let Some(markdown) = markdown {
            *block_markdown = Some(markdown.clone());
        }
        block.updated_at_unix_ms = Some(now_unix_ms);
        upsert_turn_work_summary(
            thread,
            workspace_id,
            thread_id,
            turn_id,
            TurnWorkState::Running,
            TurnWorkPresentation::CollapsedAfterFinal,
            None,
            now_unix_ms,
        );
    } else {
        let work_item_id = work_item_projection_id(turn_id, item_id);
        if let Some(item) = thread
            .work_range_mut(turn_id.to_owned())
            .items_by_id
            .get_mut(work_item_id.as_str())
        {
            append_delta_to_turn_item(&mut item.item, delta, markdown);
            item.status = TurnWorkItemStatus::Running;
            item.completed_at_unix_ms = None;
        } else if matches!(stream, Some(ItemDeltaStream::AgentMessage)) {
            let item = TurnItem::AgentMessage {
                id: item_id.to_owned(),
                text: delta.to_owned(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: markdown.cloned(),
                markdown_version: None,
            };
            upsert_assistant_message_block(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                &item,
                TurnWorkItemStatus::Running,
                now_unix_ms,
            );
            upsert_turn_work_summary(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                TurnWorkState::Running,
                TurnWorkPresentation::CollapsedAfterFinal,
                None,
                now_unix_ms,
            );
        }
    }
    before != *thread
}

fn apply_item_completed_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item: &TurnItem,
    now_unix_ms: i64,
) -> bool {
    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    match live_item_placement(item) {
        LiveItemPlacement::TopLevelUser => {
            if let TurnItem::UserMessage {
                id,
                text,
                attachments,
            } = item
            {
                upsert_user_message_block(
                    thread,
                    workspace_id,
                    thread_id,
                    turn_id,
                    Some(id.clone()),
                    text.clone(),
                    attachments.clone(),
                    now_unix_ms,
                );
            }
        }
        LiveItemPlacement::TopLevelAssistant => {
            remove_turn_work_item(thread, turn_id, item.item_id());
            upsert_assistant_message_block(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                item,
                TurnWorkItemStatus::Completed,
                now_unix_ms,
            );
            upsert_turn_work_summary(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                TurnWorkState::Running,
                TurnWorkPresentation::CollapsedAfterFinal,
                None,
                now_unix_ms,
            );
        }
        LiveItemPlacement::TurnWork => {
            upsert_turn_work_item(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                item.clone(),
                completed_work_status_for_item(item),
                now_unix_ms,
            );
            upsert_turn_work_summary(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                TurnWorkState::Running,
                current_or_live_work_presentation(thread, turn_id),
                None,
                now_unix_ms,
            );
        }
        LiveItemPlacement::Hidden => {
            remove_turn_work_item(thread, turn_id, item.item_id());
        }
    }
    before != *thread
}

pub fn remove_thread_semantic_timeline(state: &mut SemanticTimelineState, thread_id: &str) -> bool {
    state.threads_by_id.remove(thread_id).is_some()
}

pub fn expand_turn_work(
    state: &mut SemanticTimelineState,
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
) -> bool {
    set_turn_work_expanded(state, thread_id, turn_id, true)
}

pub fn collapse_turn_work(
    state: &mut SemanticTimelineState,
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
) -> bool {
    set_turn_work_expanded(state, thread_id, turn_id, false)
}

pub fn toggle_turn_work_expansion(
    state: &mut SemanticTimelineState,
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
) -> bool {
    let thread_id = thread_id.into();
    let turn_id = turn_id.into();
    let is_expanded = state
        .thread(thread_id.as_str())
        .and_then(|thread| {
            thread
                .cached_turn_work_block(turn_id.as_str())
                .map(|work| resolve_work_expanded(work, &thread.expansion))
        })
        .unwrap_or_else(|| {
            state.thread(thread_id.as_str()).is_some_and(|thread| {
                thread.expansion.decision_for_turn(turn_id.as_str())
                    == TurnWorkExpansionDecision::Expanded
            })
        });
    set_turn_work_expanded(state, thread_id, turn_id, !is_expanded)
}

pub fn reset_thread_expansion(
    state: &mut SemanticTimelineState,
    thread_id: impl Into<String>,
) -> bool {
    let thread_id = thread_id.into();
    let Some(thread) = state.threads_by_id.get_mut(thread_id.as_str()) else {
        return false;
    };
    let before = thread.expansion.clone();
    thread.expansion = TimelineExpansionState::default();
    before != thread.expansion
}

pub fn evict_turn_work_range(
    state: &mut SemanticTimelineState,
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
) -> bool {
    let thread_id = thread_id.into();
    let turn_id = turn_id.into();
    state
        .threads_by_id
        .get_mut(thread_id.as_str())
        .is_some_and(|thread| {
            thread
                .work_ranges_by_turn
                .remove(turn_id.as_str())
                .is_some()
        })
}

pub fn apply_turn_work_page(
    state: &mut SemanticTimelineState,
    page: TurnWorkPageResponse,
    merge_mode: WorkPageMergeMode,
) -> bool {
    let thread_id = page.thread_id.clone();
    let turn_id = page.turn_id.clone();
    let thread = state.thread_mut(thread_id);
    let range = thread.work_range_mut(turn_id);
    apply_work_range_page(range, page, merge_mode)
}

pub fn apply_work_range_page(
    range: &mut TurnWorkRangeCache,
    page: TurnWorkPageResponse,
    merge_mode: WorkPageMergeMode,
) -> bool {
    let before = range.clone();
    let was_empty = range.ordered_item_ids.is_empty();

    if merge_mode == WorkPageMergeMode::Reset {
        range.items_by_id.clear();
        range.ordered_item_ids.clear();
        range.stale_work_item_ids.clear();
    }

    range.thread_id = page.thread_id;
    range.turn_id = page.turn_id;
    range.work = Some(page.work);
    for item in page.items {
        remove_existing_work_items_for_item_id(
            range,
            item.item_id.as_str(),
            item.work_item_id.as_str(),
        );
        range.stale_work_item_ids.remove(item.work_item_id.as_str());
        range.items_by_id.insert(item.work_item_id.clone(), item);
    }
    sort_work_items(range);
    merge_work_loaded_range(range, &page.page, merge_mode, was_empty);
    range.request_status = TimelineRequestStatus::Ready;

    before != *range
}

fn remove_existing_work_items_for_item_id(
    range: &mut TurnWorkRangeCache,
    item_id: &str,
    keep_work_item_id: &str,
) {
    let duplicate_ids = range
        .items_by_id
        .values()
        .filter(|item| item.item_id == item_id && item.work_item_id != keep_work_item_id)
        .map(|item| item.work_item_id.clone())
        .collect::<Vec<_>>();
    for duplicate_id in duplicate_ids {
        range.items_by_id.remove(duplicate_id.as_str());
        range.stale_work_item_ids.remove(duplicate_id.as_str());
    }
    range
        .ordered_item_ids
        .retain(|work_item_id| range.items_by_id.contains_key(work_item_id));
}

pub fn apply_turn_work_items_changed(
    state: &mut SemanticTimelineState,
    notification: TurnWorkItemsChangedNotification,
) -> bool {
    let thread = state.thread_mut(notification.thread_id);
    let range = thread.work_range_mut(notification.turn_id);
    let before = range.clone();

    for work_item_id in notification.removed_work_item_ids {
        range.items_by_id.remove(work_item_id.as_str());
        range.stale_work_item_ids.remove(work_item_id.as_str());
    }
    if !range.ordered_item_ids.is_empty() {
        range
            .ordered_item_ids
            .retain(|work_item_id| range.items_by_id.contains_key(work_item_id));
    }
    for work_item_id in notification.changed_work_item_ids {
        range.stale_work_item_ids.insert(work_item_id);
    }
    range.request_status = TimelineRequestStatus::Ready;

    before != *range
}

pub fn apply_turn_work_state_changed(
    state: &mut SemanticTimelineState,
    notification: TurnWorkStateChangedNotification,
) -> bool {
    let thread = state.thread_mut(notification.thread_id);
    let before = thread.clone();
    let turn_id = notification.turn_id;
    let work = notification.work;

    thread.work_range_mut(turn_id.as_str()).work = Some(work.clone());
    for block in thread.top_level.blocks_by_id.values_mut() {
        if let TimelineBlockKind::TurnWork { work: block_work } = &mut block.kind
            && block_work.turn_id == turn_id
        {
            *block_work = work.clone();
        }
    }

    before != *thread
}

fn set_turn_work_expanded(
    state: &mut SemanticTimelineState,
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    expanded: bool,
) -> bool {
    let thread = state.thread_mut(thread_id);
    let before = thread.expansion.clone();
    thread
        .expansion
        .set_turn_work_expanded(turn_id.into(), expanded);
    before != thread.expansion
}

fn semantic_timeline_cache_patch_from_diff(
    workspace_id: &str,
    thread_id: ThreadId,
    before: Option<&ThreadSemanticTimelineState>,
    after: Option<&ThreadSemanticTimelineState>,
) -> SemanticTimelineCachePatch {
    let mut changed_blocks = Vec::new();
    let mut removed_block_ids = Vec::new();
    let mut changed_work_items = Vec::new();
    let mut removed_work_items = Vec::new();

    if let Some(after) = after {
        for block in after.top_level.blocks_by_id.values() {
            if before.and_then(|before| before.top_level.blocks_by_id.get(block.block_id.as_str()))
                != Some(block)
            {
                changed_blocks.push(block.clone());
            }
        }
        for range in after.work_ranges_by_turn.values() {
            for item in range.items_by_id.values() {
                let before_item = before
                    .and_then(|before| before.work_ranges_by_turn.get(range.turn_id.as_str()))
                    .and_then(|range| range.items_by_id.get(item.work_item_id.as_str()));
                if before_item != Some(item) {
                    changed_work_items.push(item.clone());
                }
            }
        }
    }

    if let Some(before) = before {
        for block_id in before.top_level.blocks_by_id.keys() {
            if after.is_none_or(|after| !after.top_level.blocks_by_id.contains_key(block_id)) {
                removed_block_ids.push(block_id.clone());
            }
        }
        for range in before.work_ranges_by_turn.values() {
            for work_item_id in range.items_by_id.keys() {
                let item_still_exists = after
                    .and_then(|after| after.work_ranges_by_turn.get(range.turn_id.as_str()))
                    .is_some_and(|range| range.items_by_id.contains_key(work_item_id));
                if !item_still_exists {
                    removed_work_items.push(SemanticTimelineRemovedWorkItem {
                        turn_id: range.turn_id.clone(),
                        work_item_id: work_item_id.clone(),
                    });
                }
            }
        }
    }

    changed_blocks.sort_by(|left, right| {
        left.sort_key
            .cmp(&right.sort_key)
            .then_with(|| left.block_id.cmp(&right.block_id))
    });
    removed_block_ids.sort();
    changed_work_items.sort_by(|left, right| {
        left.turn_id
            .cmp(&right.turn_id)
            .then_with(|| left.order_key.cmp(&right.order_key))
            .then_with(|| left.work_item_id.cmp(&right.work_item_id))
    });
    removed_work_items.sort_by(|left, right| {
        left.turn_id
            .cmp(&right.turn_id)
            .then_with(|| left.work_item_id.cmp(&right.work_item_id))
    });

    SemanticTimelineCachePatch {
        workspace_id: workspace_id.to_owned(),
        thread_id,
        changed_blocks,
        removed_block_ids,
        changed_work_items,
        removed_work_items,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveItemPlacement {
    TopLevelUser,
    TurnWork,
    TopLevelAssistant,
    Hidden,
}

fn live_item_placement(item: &TurnItem) -> LiveItemPlacement {
    match item {
        TurnItem::UserMessage { .. } => LiveItemPlacement::TopLevelUser,
        TurnItem::AgentMessage { phase, .. } if matches!(phase, AgentMessagePhase::FinalAnswer) => {
            LiveItemPlacement::TopLevelAssistant
        }
        TurnItem::SystemEvent {
            level,
            message,
            code,
            details,
            ..
        } => {
            if matches!(level, SystemEventLevel::Error | SystemEventLevel::Warning) {
                LiveItemPlacement::TurnWork
            } else if system_event_visible_in_work(message, code.as_deref(), details.as_ref()) {
                LiveItemPlacement::TurnWork
            } else {
                LiveItemPlacement::Hidden
            }
        }
        _ => LiveItemPlacement::TurnWork,
    }
}

fn system_event_visible_in_work(
    message: &str,
    code: Option<&str>,
    details: Option<&serde_json::Value>,
) -> bool {
    match code {
        Some("agent_context_compaction") | Some("agent_review") => true,
        Some(code) if is_recovery_system_code(code) || is_execution_window_system_code(code) => {
            true
        }
        Some("agent_runtime_event") => details
            .and_then(|details| details.get("nativeMethod"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|method| !is_hidden_runtime_method(method)),
        Some(_) => false,
        None if message.starts_with("Runtime event: ") => message
            .strip_prefix("Runtime event: ")
            .is_some_and(|method| !is_hidden_runtime_method(method)),
        None if message.starts_with("Thread status changed:")
            || message == "Diff updated"
            || message == "Plan updated" =>
        {
            false
        }
        None => false,
    }
}

fn is_hidden_runtime_method(method: &str) -> bool {
    matches!(
        method,
        "thread/tokenUsage/updated"
            | "fuzzyFileSearch/sessionUpdated"
            | "fuzzyFileSearch/sessionCompleted"
            | "windowsSandbox/setupCompleted"
    )
}

fn is_recovery_system_code(code: &str) -> bool {
    matches!(
        code,
        "turn_start_rejected"
            | "turn_blocked_resumable"
            | "item_timeout_detected"
            | "item_recovery_opened"
            | "item_recovery_attached"
            | "item_retry_scheduled"
            | "item_retry_attempt_started"
            | "item_recovery_succeeded"
            | "item_recovery_exhausted"
            | "item_tool_retry_scheduled"
            | "item_tool_retry_resolved"
            | "item_tool_retry_exhausted"
            | "turn_tool_loop_budget_exceeded"
    )
}

fn is_execution_window_system_code(code: &str) -> bool {
    matches!(
        code,
        "turn_execution_window_exhausted"
            | "turn_execution_window_continued"
            | "turn_execution_window_blocked"
    )
}

fn user_block_id(turn_id: &str) -> String {
    format!("turn:{turn_id}:user")
}

fn work_block_id(turn_id: &str) -> String {
    format!("turn:{turn_id}:work")
}

fn assistant_block_id(turn_id: &str, item_id: &str) -> String {
    format!("turn:{turn_id}:assistant:{item_id}")
}

fn work_item_projection_id(turn_id: &str, item_id: &str) -> String {
    format!("turn:{turn_id}:work:{item_id}")
}

fn turn_sort_base(thread: &ThreadSemanticTimelineState, turn_id: &str, now_unix_ms: i64) -> String {
    for block in thread.top_level.blocks_by_id.values() {
        if block_turn_id(block) == Some(turn_id)
            && let Some((millis, rest)) = block.sort_key.split_once(':')
            && let Some((existing_turn_id, _)) = rest.split_once(':')
            && existing_turn_id == turn_id
        {
            return format!("{millis}:{turn_id}");
        }
    }
    format!("{:020}:{turn_id}", now_unix_ms.max(0))
}

fn turn_block_sort_key(
    thread: &ThreadSemanticTimelineState,
    turn_id: &str,
    rank: u16,
    suffix: &str,
    now_unix_ms: i64,
) -> String {
    format!(
        "{}:{:03}:{}",
        turn_sort_base(thread, turn_id, now_unix_ms),
        rank,
        suffix
    )
}

fn work_item_order_key(
    range: Option<&TurnWorkRangeCache>,
    item_id: &str,
    now_unix_ms: i64,
) -> String {
    if let Some(range) = range {
        for item in range.items_by_id.values() {
            if item.item_id == item_id {
                return item.order_key.clone();
            }
        }
    }
    format!("{:020}:{}", now_unix_ms.max(0), item_id)
}

fn upsert_top_level_block(thread: &mut ThreadSemanticTimelineState, block: TimelineBlock) {
    thread
        .top_level
        .stale_block_ids
        .remove(block.block_id.as_str());
    thread
        .top_level
        .blocks_by_id
        .insert(block.block_id.clone(), block);
    sort_top_level_blocks(&mut thread.top_level);
}

fn upsert_user_message_block(
    thread: &mut ThreadSemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: Option<String>,
    text: String,
    attachments: Vec<UserMessageAttachment>,
    now_unix_ms: i64,
) {
    let block_id = user_block_id(turn_id);
    let existing = thread.top_level.blocks_by_id.get(block_id.as_str());
    let block = TimelineBlock {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        block_id,
        turn_id: Some(turn_id.to_owned()),
        sort_key: existing
            .map(|block| block.sort_key.clone())
            .unwrap_or_else(|| turn_block_sort_key(thread, turn_id, 0, "user", now_unix_ms)),
        started_at_unix_ms: existing
            .and_then(|block| block.started_at_unix_ms)
            .or(Some(now_unix_ms)),
        updated_at_unix_ms: Some(now_unix_ms),
        kind: TimelineBlockKind::UserMessage {
            item_id,
            inputs: Vec::new(),
            text,
            attachments,
        },
    };
    upsert_top_level_block(thread, block);
}

fn upsert_assistant_message_block(
    thread: &mut ThreadSemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item: &TurnItem,
    status: TurnWorkItemStatus,
    now_unix_ms: i64,
) {
    let TurnItem::AgentMessage {
        id, text, markdown, ..
    } = item
    else {
        return;
    };
    let block_id = assistant_block_id(turn_id, id);
    let existing = thread.top_level.blocks_by_id.get(block_id.as_str());
    let block = TimelineBlock {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        block_id,
        turn_id: Some(turn_id.to_owned()),
        sort_key: existing
            .map(|block| block.sort_key.clone())
            .unwrap_or_else(|| {
                turn_block_sort_key(
                    thread,
                    turn_id,
                    200,
                    work_item_order_key(thread.work_range(turn_id), id, now_unix_ms).as_str(),
                    now_unix_ms,
                )
            }),
        started_at_unix_ms: existing
            .and_then(|block| block.started_at_unix_ms)
            .or(Some(now_unix_ms)),
        updated_at_unix_ms: Some(now_unix_ms),
        kind: TimelineBlockKind::AssistantMessage {
            item_id: id.clone(),
            text: text.clone(),
            status,
            markdown: markdown.clone(),
        },
    };
    upsert_top_level_block(thread, block);
}

fn upsert_turn_work_item(
    thread: &mut ThreadSemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item: TurnItem,
    status: TurnWorkItemStatus,
    now_unix_ms: i64,
) {
    let item_id = item.item_id().to_owned();
    let work_item_id = work_item_projection_id(turn_id, item_id.as_str());
    let order_key = work_item_order_key(thread.work_range(turn_id), item_id.as_str(), now_unix_ms);
    let item_type = item.item_type();
    let range = thread.work_range_mut(turn_id.to_owned());
    range.thread_id = thread_id.to_owned();
    range.turn_id = turn_id.to_owned();
    range.stale_work_item_ids.remove(work_item_id.as_str());
    let existing_started_at = range
        .items_by_id
        .get(work_item_id.as_str())
        .and_then(|item| item.started_at_unix_ms);
    range.items_by_id.insert(
        work_item_id.clone(),
        TurnWorkItem {
            work_item_id,
            item_id,
            turn_id: turn_id.to_owned(),
            order_key,
            item_type,
            status,
            started_at_unix_ms: existing_started_at.or(Some(now_unix_ms)),
            completed_at_unix_ms: if is_terminal_turn_work_item_status(status) {
                Some(now_unix_ms)
            } else {
                None
            },
            item,
            metadata: Some(serde_json::json!({
                "workspaceId": workspace_id,
                "live": true,
            })),
        },
    );
    sort_work_items(range);
}

fn remove_turn_work_item(thread: &mut ThreadSemanticTimelineState, turn_id: &str, item_id: &str) {
    if let Some(range) = thread.work_ranges_by_turn.get_mut(turn_id) {
        let work_item_id = work_item_projection_id(turn_id, item_id);
        range.items_by_id.remove(work_item_id.as_str());
        range
            .ordered_item_ids
            .retain(|candidate| candidate != work_item_id.as_str());
        range.stale_work_item_ids.remove(work_item_id.as_str());
    }
}

fn upsert_turn_work_summary(
    thread: &mut ThreadSemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    state: TurnWorkState,
    presentation: TurnWorkPresentation,
    completed_at_unix_ms: Option<i64>,
    now_unix_ms: i64,
) {
    let existing = thread.cached_turn_work_block(turn_id).cloned();
    let visible_count_from_range = thread
        .work_range(turn_id)
        .map(|range| range.items_by_id.len() as u64)
        .unwrap_or(0);
    let visible_work_count = existing
        .as_ref()
        .map(|work| work.visible_work_count.max(visible_count_from_range))
        .unwrap_or(visible_count_from_range);
    let hidden_work_count = existing
        .as_ref()
        .map(|work| work.hidden_work_count)
        .unwrap_or(0);
    let (first_work_item_id, last_work_item_id) = live_first_last_work_item_ids(thread, turn_id);
    let work = TurnWorkBlock {
        turn_id: turn_id.to_owned(),
        presentation,
        state,
        started_at_unix_ms: existing
            .as_ref()
            .and_then(|work| work.started_at_unix_ms)
            .or(Some(now_unix_ms)),
        completed_at_unix_ms,
        elapsed_ms: existing
            .as_ref()
            .and_then(|work| work.started_at_unix_ms)
            .map(|started_at| now_unix_ms.saturating_sub(started_at).max(0) as u64),
        work_count: visible_work_count.saturating_add(hidden_work_count),
        visible_work_count,
        hidden_work_count,
        has_more_before: existing
            .as_ref()
            .map(|work| work.has_more_before)
            .unwrap_or(false),
        has_more_after: existing
            .as_ref()
            .map(|work| work.has_more_after)
            .unwrap_or(false),
        before_cursor: existing
            .as_ref()
            .and_then(|work| work.before_cursor.clone()),
        after_cursor: existing.as_ref().and_then(|work| work.after_cursor.clone()),
        first_work_item_id: first_work_item_id.or_else(|| {
            existing
                .as_ref()
                .and_then(|work| work.first_work_item_id.clone())
        }),
        last_work_item_id: last_work_item_id.or_else(|| {
            existing
                .as_ref()
                .and_then(|work| work.last_work_item_id.clone())
        }),
    };
    thread.work_range_mut(turn_id.to_owned()).work = Some(work.clone());
    let block_id = work_block_id(turn_id);
    let existing_block = thread.top_level.blocks_by_id.get(block_id.as_str());
    let block = TimelineBlock {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        block_id,
        turn_id: Some(turn_id.to_owned()),
        sort_key: existing_block
            .map(|block| block.sort_key.clone())
            .unwrap_or_else(|| turn_block_sort_key(thread, turn_id, 100, "work", now_unix_ms)),
        started_at_unix_ms: existing_block
            .and_then(|block| block.started_at_unix_ms)
            .or(work.started_at_unix_ms),
        updated_at_unix_ms: Some(now_unix_ms),
        kind: TimelineBlockKind::TurnWork { work },
    };
    upsert_top_level_block(thread, block);
}

fn live_first_last_work_item_ids(
    thread: &ThreadSemanticTimelineState,
    turn_id: &str,
) -> (Option<String>, Option<String>) {
    let Some(range) = thread.work_range(turn_id) else {
        return (None, None);
    };
    (
        range.ordered_item_ids.first().cloned(),
        range.ordered_item_ids.last().cloned(),
    )
}

fn current_or_live_work_presentation(
    thread: &ThreadSemanticTimelineState,
    turn_id: &str,
) -> TurnWorkPresentation {
    thread
        .cached_turn_work_block(turn_id)
        .map(|work| work.presentation)
        .unwrap_or(TurnWorkPresentation::ExpandedLive)
}

fn turn_has_assistant_block(thread: &ThreadSemanticTimelineState, turn_id: &str) -> bool {
    thread.top_level.blocks_by_id.values().any(|block| {
        block.turn_id.as_deref() == Some(turn_id)
            && matches!(block.kind, TimelineBlockKind::AssistantMessage { .. })
    })
}

fn turn_status_to_work_state(status: TurnStatus) -> Option<TurnWorkState> {
    match status {
        TurnStatus::InProgress => Some(TurnWorkState::Running),
        TurnStatus::Completed => Some(TurnWorkState::Completed),
        TurnStatus::Failed => Some(TurnWorkState::Failed),
        TurnStatus::Interrupted => Some(TurnWorkState::Interrupted),
        TurnStatus::Blocked => Some(TurnWorkState::Blocked),
    }
}

fn completed_work_status_for_item(item: &TurnItem) -> TurnWorkItemStatus {
    match item {
        TurnItem::CommandExecution { status, .. }
        | TurnItem::FileChange { status, .. }
        | TurnItem::WebSearch { status, .. }
        | TurnItem::WebFetch { status, .. }
        | TurnItem::Download { status, .. }
        | TurnItem::DynamicToolCall { status, .. } => match status {
            pioneer_protocol::ToolCallStatus::InProgress => TurnWorkItemStatus::Running,
            pioneer_protocol::ToolCallStatus::Completed => TurnWorkItemStatus::Completed,
            pioneer_protocol::ToolCallStatus::Failed => TurnWorkItemStatus::Failed,
        },
        TurnItem::SystemEvent { level, .. } => match level {
            SystemEventLevel::Error => TurnWorkItemStatus::Failed,
            SystemEventLevel::Info | SystemEventLevel::Warning => TurnWorkItemStatus::Completed,
        },
        _ => TurnWorkItemStatus::Completed,
    }
}

fn is_terminal_turn_work_item_status(status: TurnWorkItemStatus) -> bool {
    !matches!(status, TurnWorkItemStatus::Running)
}

fn append_delta_to_turn_item(
    item: &mut TurnItem,
    delta: &str,
    markdown: Option<&MarkdownDocument>,
) {
    match item {
        TurnItem::AgentMessage {
            text,
            markdown: item_markdown,
            ..
        } => {
            text.push_str(delta);
            if let Some(markdown) = markdown {
                *item_markdown = Some(markdown.clone());
            }
        }
        TurnItem::Reasoning { content, .. } => {
            if let Some(last) = content.last_mut() {
                last.push_str(delta);
            } else {
                content.push(delta.to_owned());
            }
        }
        TurnItem::SystemEvent { message, .. } => {
            message.push_str(delta);
        }
        _ => {}
    }
}

fn push_request_action(
    actions: &mut Vec<SemanticTimelineRequestAction>,
    planned: &mut HashSet<SemanticTimelineRequestKey>,
    in_flight: &HashSet<SemanticTimelineRequestKey>,
    action: SemanticTimelineRequestAction,
) {
    let key = match &action {
        SemanticTimelineRequestAction::ThreadTimelinePage { key, .. }
        | SemanticTimelineRequestAction::TurnWorkPage { key, .. } => key,
    };
    if in_flight.contains(key) || !planned.insert(key.clone()) {
        return;
    }
    actions.push(action);
}

fn turn_span_is_visible(
    flattened: &SemanticTimelineRows,
    turn_id: &str,
    visible_positions: &[usize],
) -> bool {
    let Some((span_start, span_end)) = turn_span(flattened, turn_id) else {
        return false;
    };
    visible_positions
        .iter()
        .any(|position| (*position >= span_start) && (*position <= span_end))
}

fn turn_span_near_leading(
    flattened: &SemanticTimelineRows,
    turn_id: &str,
    visible_positions: &[usize],
    threshold_rows: usize,
) -> bool {
    let Some((span_start, _)) = turn_span(flattened, turn_id) else {
        return false;
    };
    visible_positions
        .iter()
        .copied()
        .filter(|position| *position >= span_start)
        .min()
        .is_some_and(|position| position <= span_start + threshold_rows)
}

fn turn_span_near_trailing(
    flattened: &SemanticTimelineRows,
    turn_id: &str,
    visible_positions: &[usize],
    threshold_rows: usize,
) -> bool {
    let Some((_, span_end)) = turn_span(flattened, turn_id) else {
        return false;
    };
    let boundary = span_end.saturating_sub(threshold_rows);
    visible_positions
        .iter()
        .copied()
        .filter(|position| *position <= span_end)
        .max()
        .is_some_and(|position| position >= boundary)
}

fn turn_span(flattened: &SemanticTimelineRows, turn_id: &str) -> Option<(usize, usize)> {
    let mut span_start = None;
    let mut span_end = None;
    for (index, row) in flattened.rows.iter().enumerate() {
        let row_matches_turn = match &row.kind {
            SemanticTimelineRowKind::WorkHeader { work, .. } => work.turn_id == turn_id,
            SemanticTimelineRowKind::WorkItem { item } => item.turn_id == turn_id,
            SemanticTimelineRowKind::UserBlock { block }
            | SemanticTimelineRowKind::AssistantMessage { block }
            | SemanticTimelineRowKind::PendingRequest { block }
            | SemanticTimelineRowKind::TurnState { block } => {
                block.turn_id.as_deref() == Some(turn_id)
            }
        };
        if row_matches_turn {
            span_start.get_or_insert(index);
            span_end = Some(index);
        }
    }
    span_start.zip(span_end)
}

fn push_top_level_request_hints(
    thread: &ThreadSemanticTimelineState,
    request_hints: &mut Vec<SemanticTimelineRequestHint>,
) {
    if thread.top_level.loaded_range.has_more_before
        && let Some(cursor) = thread.top_level.loaded_range.before_cursor.clone()
    {
        request_hints.push(SemanticTimelineRequestHint::TopLevelBefore {
            thread_id: thread.thread_id.clone(),
            cursor,
        });
    }
    if thread.top_level.loaded_range.has_more_after
        && let Some(cursor) = thread.top_level.loaded_range.after_cursor.clone()
    {
        request_hints.push(SemanticTimelineRequestHint::TopLevelAfter {
            thread_id: thread.thread_id.clone(),
            cursor,
        });
    }
}

fn push_turn_work_rows_and_hints(
    thread_id: &str,
    work: &TurnWorkBlock,
    work_range: Option<&TurnWorkRangeCache>,
    rows: &mut Vec<SemanticTimelineRow>,
    request_hints: &mut Vec<SemanticTimelineRequestHint>,
) {
    let Some(work_range) = work_range else {
        if work.visible_work_count > 0 || work.has_more_before || work.has_more_after {
            request_hints.push(SemanticTimelineRequestHint::TurnWorkInitial {
                thread_id: thread_id.to_owned(),
                turn_id: work.turn_id.clone(),
            });
        }
        return;
    };

    if work_range.loaded_range.has_more_before
        && let Some(cursor) = work_range.loaded_range.before_cursor.clone()
    {
        request_hints.push(SemanticTimelineRequestHint::TurnWorkBefore {
            thread_id: thread_id.to_owned(),
            turn_id: work.turn_id.clone(),
            cursor,
        });
    }
    for item in work_range.ordered_items() {
        rows.push(SemanticTimelineRow {
            id: SemanticTimelineRowId::TurnWorkItem {
                turn_id: item.turn_id.clone(),
                work_item_id: item.work_item_id.clone(),
            },
            kind: SemanticTimelineRowKind::WorkItem { item: item.clone() },
        });
    }
    if work_range.loaded_range.has_more_after
        && let Some(cursor) = work_range.loaded_range.after_cursor.clone()
    {
        request_hints.push(SemanticTimelineRequestHint::TurnWorkAfter {
            thread_id: thread_id.to_owned(),
            turn_id: work.turn_id.clone(),
            cursor,
        });
    }
}

fn sort_top_level_blocks(cache: &mut TopLevelTimelineCache) {
    let mut ids = cache.blocks_by_id.keys().cloned().collect::<Vec<_>>();
    ids.sort_by(|left, right| {
        let left_block = cache
            .blocks_by_id
            .get(left)
            .expect("top-level block id should exist while sorting");
        let right_block = cache
            .blocks_by_id
            .get(right)
            .expect("top-level block id should exist while sorting");
        left_block
            .sort_key
            .cmp(&right_block.sort_key)
            .then_with(|| left.cmp(right))
    });
    cache.ordered_block_ids = ids;
}

fn sort_work_items(range: &mut TurnWorkRangeCache) {
    let mut ids = range.items_by_id.keys().cloned().collect::<Vec<_>>();
    ids.sort_by(|left, right| {
        let left_item = range
            .items_by_id
            .get(left)
            .expect("work item id should exist while sorting");
        let right_item = range
            .items_by_id
            .get(right)
            .expect("work item id should exist while sorting");
        left_item
            .order_key
            .cmp(&right_item.order_key)
            .then_with(|| left.cmp(right))
    });
    range.ordered_item_ids = ids;
}

fn merge_top_level_loaded_range(
    cache: &mut TopLevelTimelineCache,
    page: &TimelinePageInfo,
    merge_mode: TopLevelPageMergeMode,
    was_empty: bool,
) {
    if was_empty || merge_mode == TopLevelPageMergeMode::Reset {
        cache.loaded_range = page.into();
        return;
    }

    match merge_mode {
        TopLevelPageMergeMode::Reset => unreachable!("reset handled before merge"),
        TopLevelPageMergeMode::Merge => {
            cache.loaded_range.before_cursor = page
                .before_cursor
                .clone()
                .or(cache.loaded_range.before_cursor.clone());
            cache.loaded_range.after_cursor = page
                .after_cursor
                .clone()
                .or(cache.loaded_range.after_cursor.clone());
            cache.loaded_range.has_more_before |= page.has_more_before;
            cache.loaded_range.has_more_after |= page.has_more_after;
        }
        TopLevelPageMergeMode::MergeBefore => {
            cache.loaded_range.before_cursor = page.before_cursor.clone();
            cache.loaded_range.has_more_before = page.has_more_before;
            if cache.loaded_range.after_cursor.is_none() {
                cache.loaded_range.after_cursor = page.after_cursor.clone();
                cache.loaded_range.has_more_after = page.has_more_after;
            }
        }
        TopLevelPageMergeMode::MergeAfter => {
            cache.loaded_range.after_cursor = page.after_cursor.clone();
            cache.loaded_range.has_more_after = page.has_more_after;
            if cache.loaded_range.before_cursor.is_none() {
                cache.loaded_range.before_cursor = page.before_cursor.clone();
                cache.loaded_range.has_more_before = page.has_more_before;
            }
        }
    }
}

fn merge_work_loaded_range(
    range: &mut TurnWorkRangeCache,
    page: &TimelinePageInfo,
    merge_mode: WorkPageMergeMode,
    was_empty: bool,
) {
    if was_empty || merge_mode == WorkPageMergeMode::Reset {
        range.loaded_range = page.into();
        return;
    }

    match merge_mode {
        WorkPageMergeMode::Reset => unreachable!("reset handled before merge"),
        WorkPageMergeMode::Merge => {
            range.loaded_range.before_cursor = page
                .before_cursor
                .clone()
                .or(range.loaded_range.before_cursor.clone());
            range.loaded_range.after_cursor = page
                .after_cursor
                .clone()
                .or(range.loaded_range.after_cursor.clone());
            range.loaded_range.has_more_before |= page.has_more_before;
            range.loaded_range.has_more_after |= page.has_more_after;
        }
        WorkPageMergeMode::MergeBefore => {
            range.loaded_range.before_cursor = page.before_cursor.clone();
            range.loaded_range.has_more_before = page.has_more_before;
            if range.loaded_range.after_cursor.is_none() {
                range.loaded_range.after_cursor = page.after_cursor.clone();
                range.loaded_range.has_more_after = page.has_more_after;
            }
        }
        WorkPageMergeMode::MergeAfter => {
            range.loaded_range.after_cursor = page.after_cursor.clone();
            range.loaded_range.has_more_after = page.has_more_after;
            if range.loaded_range.before_cursor.is_none() {
                range.loaded_range.before_cursor = page.before_cursor.clone();
                range.loaded_range.has_more_before = page.has_more_before;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SystemEventLevel, TimelineBlockKind, TurnItem, TurnItemType, TurnWorkItemStatus,
        TurnWorkPresentation, TurnWorkState,
    };

    #[test]
    fn applying_same_top_level_page_twice_is_idempotent() {
        let mut state = SemanticTimelineState::default();
        let page = thread_page(vec![block("thread_a", "block_b", "002")]);

        assert!(apply_thread_timeline_page(
            &mut state,
            page.clone(),
            TopLevelPageMergeMode::Reset
        ));
        assert!(!apply_thread_timeline_page(
            &mut state,
            page,
            TopLevelPageMergeMode::Merge
        ));
        let thread = state.thread("thread_a").expect("thread cache should exist");
        assert_eq!(thread.top_level.ordered_block_ids, vec!["block_b"]);
    }

    #[test]
    fn live_commentary_agent_message_delta_stays_in_turn_work() {
        let mut state = SemanticTimelineState::default();

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemStarted {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item: TurnItem::AgentMessage {
                    id: "item_comment".to_owned(),
                    text: "thinking".to_owned(),
                    phase: AgentMessagePhase::Commentary,
                    markdown: None,
                    markdown_version: None,
                },
            },
            10,
        ));

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemDelta {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item_id: "item_comment".to_owned(),
                delta: " more".to_owned(),
                stream: Some(ItemDeltaStream::AgentMessage),
                payload: None,
                markdown: None,
                markdown_version: None,
            },
            11,
        ));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        assert!(
            thread
                .top_level
                .ordered_blocks()
                .all(|block| !matches!(block.kind, TimelineBlockKind::AssistantMessage { .. })),
            "commentary agent messages must not become top-level final assistant blocks"
        );
        let item = thread
            .work_range("turn_a")
            .and_then(|range| range.items_by_id.get("turn:turn_a:work:item_comment"))
            .expect("commentary message should be a turn work item");
        assert_eq!(item.status, TurnWorkItemStatus::Running);
        assert!(
            matches!(&item.item, TurnItem::AgentMessage { text, phase, .. }
                if text == "thinking more" && *phase == AgentMessagePhase::Commentary)
        );
    }

    #[test]
    fn local_turn_start_patch_contains_shared_semantic_blocks() {
        let mut state = SemanticTimelineState::default();
        let patch = apply_conversation_event_to_semantic_timeline_with_patch(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                pending_request_id: "pending_a".to_owned(),
                user_text: "hello".to_owned(),
                attachments: Vec::new(),
            },
            10,
        );

        assert_eq!(patch.workspace_id, "workspace_a");
        assert_eq!(patch.thread_id, "thread_a");
        assert_eq!(patch.removed_block_ids, Vec::<String>::new());
        assert_eq!(patch.changed_work_items, Vec::<TurnWorkItem>::new());
        assert_eq!(
            patch
                .changed_blocks
                .iter()
                .map(|block| block.block_id.as_str())
                .collect::<Vec<_>>(),
            vec!["turn:turn_a:user", "turn:turn_a:work"]
        );
        assert!(matches!(
            patch.changed_blocks[0].kind,
            TimelineBlockKind::UserMessage { .. }
        ));
        assert!(matches!(
            patch.changed_blocks[1].kind,
            TimelineBlockKind::TurnWork { .. }
        ));
    }

    #[test]
    fn adjacent_top_level_pages_merge_without_duplicates_and_sort_by_server_key() {
        let mut state = SemanticTimelineState::default();
        let newer = thread_page(vec![
            block("thread_a", "block_c", "003"),
            block("thread_a", "block_d", "004"),
        ]);
        let older = thread_page(vec![
            block("thread_a", "block_a", "001"),
            block("thread_a", "block_c", "003"),
        ]);

        assert!(apply_thread_timeline_page(
            &mut state,
            newer,
            TopLevelPageMergeMode::Reset
        ));
        assert!(apply_thread_timeline_page(
            &mut state,
            older,
            TopLevelPageMergeMode::MergeBefore
        ));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        assert_eq!(
            thread.top_level.ordered_block_ids,
            vec!["block_a", "block_c", "block_d"]
        );
    }

    #[test]
    fn blocks_changed_marks_stale_ids_and_refetched_page_clears_them() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![block("thread_a", "block_a", "001")]),
            TopLevelPageMergeMode::Reset
        ));
        assert!(apply_thread_timeline_blocks_changed(
            &mut state,
            ThreadTimelineBlocksChangedNotification {
                workspace_id: "workspace_a".to_owned(),
                thread_id: "thread_a".to_owned(),
                changed_block_ids: vec!["block_a".to_owned(), "block_b".to_owned()],
                removed_block_ids: Vec::new(),
                before_cursor: None,
                after_cursor: None,
                reason: pioneer_protocol::TimelineChangeReason::LiveEvent,
            },
        ));
        let thread = state.thread("thread_a").expect("thread cache should exist");
        assert_eq!(
            thread.top_level.stale_block_ids(),
            vec!["block_a", "block_b"]
        );

        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![block("thread_a", "block_a", "001")]),
            TopLevelPageMergeMode::Merge
        ));
        let thread = state.thread("thread_a").expect("thread cache should exist");
        assert_eq!(thread.top_level.stale_block_ids(), vec!["block_b"]);
    }

    #[test]
    fn work_pages_merge_without_replacing_existing_items() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_b", "002"), work_item("work_c", "003")]),
            WorkPageMergeMode::Reset
        ));
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001"), work_item("work_b", "002")]),
            WorkPageMergeMode::MergeBefore
        ));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        let range = thread
            .work_range("turn_a")
            .expect("work range should exist");
        assert_eq!(range.ordered_item_ids, vec!["work_a", "work_b", "work_c"]);
    }

    #[test]
    fn collapse_does_not_evict_cached_work_range() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001")]),
            WorkPageMergeMode::Reset
        ));
        assert!(collapse_turn_work(&mut state, "thread_a", "turn_a"));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        assert_eq!(
            thread.expansion.decision_for_turn("turn_a"),
            TurnWorkExpansionDecision::Collapsed
        );
        assert!(
            thread
                .work_range("turn_a")
                .is_some_and(|range| range.item("work_a").is_some()),
            "collapse should keep cached work items"
        );
    }

    #[test]
    fn work_items_changed_marks_stale_ids_and_refetched_page_clears_them() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001")]),
            WorkPageMergeMode::Reset
        ));
        assert!(apply_turn_work_items_changed(
            &mut state,
            TurnWorkItemsChangedNotification {
                workspace_id: "workspace_a".to_owned(),
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                changed_work_item_ids: vec!["work_a".to_owned(), "work_b".to_owned()],
                removed_work_item_ids: Vec::new(),
                before_cursor: None,
                after_cursor: None,
                reason: pioneer_protocol::TimelineChangeReason::LiveEvent,
            },
        ));
        let thread = state.thread("thread_a").expect("thread cache should exist");
        let range = thread
            .work_range("turn_a")
            .expect("work range should exist");
        assert_eq!(range.stale_work_item_ids(), vec!["work_a", "work_b"]);

        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001")]),
            WorkPageMergeMode::Merge
        ));
        let thread = state.thread("thread_a").expect("thread cache should exist");
        let range = thread
            .work_range("turn_a")
            .expect("work range should exist");
        assert_eq!(range.stale_work_item_ids(), vec!["work_b"]);
    }

    #[test]
    fn turn_work_state_changed_updates_cached_range_and_top_level_block() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![turn_work_block("thread_a", "block_work", "002")]),
            TopLevelPageMergeMode::Reset
        ));
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001")]),
            WorkPageMergeMode::Reset
        ));

        let mut updated_work = work_block("turn_a");
        updated_work.state = TurnWorkState::WaitingForApproval;
        updated_work.visible_work_count = 10;
        assert!(apply_turn_work_state_changed(
            &mut state,
            TurnWorkStateChangedNotification {
                workspace_id: "workspace_a".to_owned(),
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                work: updated_work,
                reason: pioneer_protocol::TimelineChangeReason::LiveEvent,
            }
        ));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        let range = thread
            .work_range("turn_a")
            .expect("work range should exist");
        assert_eq!(
            range.work.as_ref().map(|work| work.state),
            Some(TurnWorkState::WaitingForApproval)
        );
        let block = thread
            .top_level
            .block("block_work")
            .expect("top-level work block should exist");
        assert!(matches!(
            &block.kind,
            TimelineBlockKind::TurnWork { work }
                if work.state == TurnWorkState::WaitingForApproval
        ));
    }

    #[test]
    fn flatten_expanded_work_shows_loaded_rows_and_request_hints() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![turn_work_block("thread_a", "block_work", "002")]),
            TopLevelPageMergeMode::Reset
        ));
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001"), work_item("work_b", "002")]),
            WorkPageMergeMode::Reset
        ));
        let thread = state.thread_mut("thread_a");
        let range = thread.work_range_mut("turn_a");
        range.loaded_range.before_cursor = Some(TimelineCursor {
            value: "before-work".to_owned(),
        });
        range.loaded_range.after_cursor = Some(TimelineCursor {
            value: "after-work".to_owned(),
        });
        range.loaded_range.has_more_before = true;
        range.loaded_range.has_more_after = true;

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        assert_eq!(flattened.rows.len(), 3);
        assert!(matches!(
            &flattened.rows[0].kind,
            SemanticTimelineRowKind::WorkHeader { expanded: true, .. }
        ));
        assert!(matches!(
            &flattened.rows[1].id,
            SemanticTimelineRowId::TurnWorkItem { work_item_id, .. }
                if work_item_id == "work_a"
        ));
        assert!(matches!(
            &flattened.rows[2].id,
            SemanticTimelineRowId::TurnWorkItem { work_item_id, .. }
                if work_item_id == "work_b"
        ));
        assert_eq!(flattened.request_hints.len(), 2);
        assert!(flattened.request_hints.iter().any(|hint| {
            matches!(
                hint,
                SemanticTimelineRequestHint::TurnWorkBefore { turn_id, .. }
                    if turn_id == "turn_a"
            )
        }));
        assert!(flattened.request_hints.iter().any(|hint| {
            matches!(
                hint,
                SemanticTimelineRequestHint::TurnWorkAfter { turn_id, .. }
                    if turn_id == "turn_a"
            )
        }));
    }

    #[test]
    fn flatten_collapsed_work_does_not_emit_work_rows_or_load_rows() {
        let mut block = turn_work_block("thread_a", "block_work", "002");
        if let TimelineBlockKind::TurnWork { work } = &mut block.kind {
            work.presentation = TurnWorkPresentation::CollapsedAfterFinal;
            work.visible_work_count = 10;
            work.has_more_after = true;
        }
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![block]),
            TopLevelPageMergeMode::Reset
        ));

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        assert_eq!(flattened.rows.len(), 1);
        assert!(matches!(
            &flattened.rows[0].kind,
            SemanticTimelineRowKind::WorkHeader {
                expanded: false,
                ..
            }
        ));
        assert!(
            flattened.request_hints.is_empty(),
            "collapsed work should not ask the UI to show or trigger manual load rows"
        );
    }

    #[test]
    fn no_final_live_work_is_expanded_by_protocol_default_without_eager_items() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![
                user_block("thread_a", "block_user", "001", "turn_a"),
                turn_work_block("thread_a", "block_work", "002"),
            ]),
            TopLevelPageMergeMode::Reset
        ));

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        assert_eq!(flattened.rows.len(), 2);
        assert!(matches!(
            &flattened.rows[1].kind,
            SemanticTimelineRowKind::WorkHeader {
                expanded: true,
                loaded_range: None,
                work,
                ..
            } if work.presentation == TurnWorkPresentation::ExpandedLive
        ));
        assert!(
            flattened
                .request_hints
                .iter()
                .any(|hint| matches!(hint, SemanticTimelineRequestHint::TurnWorkInitial { turn_id, .. } if turn_id == "turn_a")),
            "expanded live work should request a bounded initial work page, not synthesize all items"
        );
    }

    #[test]
    fn final_answer_keeps_work_collapsed_until_explicit_expand_and_preserves_markdown() {
        let mut work = turn_work_block("thread_a", "block_work", "002");
        if let TimelineBlockKind::TurnWork { work } = &mut work.kind {
            work.presentation = TurnWorkPresentation::CollapsedAfterFinal;
            work.state = TurnWorkState::Completed;
            work.work_count = 70_000;
            work.visible_work_count = 70_000;
            work.has_more_after = true;
        }
        let markdown = pioneer_protocol::MarkdownDocument::from_plain_text("final markdown");
        let assistant = assistant_block(
            "thread_a",
            "block_assistant",
            "003",
            "turn_a",
            Some(markdown.clone()),
        );

        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![
                user_block("thread_a", "block_user", "001", "turn_a"),
                work,
                assistant,
            ]),
            TopLevelPageMergeMode::Reset
        ));

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        assert_eq!(flattened.rows.len(), 3);
        assert!(matches!(
            &flattened.rows[1].kind,
            SemanticTimelineRowKind::WorkHeader {
                expanded: false,
                work,
                ..
            } if work.presentation == TurnWorkPresentation::CollapsedAfterFinal
        ));
        assert!(matches!(
            &flattened.rows[2].kind,
            SemanticTimelineRowKind::AssistantMessage { block }
                if matches!(
                    &block.kind,
                    TimelineBlockKind::AssistantMessage { markdown: Some(value), .. }
                        if value == &markdown
                )
        ));
        assert!(
            flattened.request_hints.iter().all(|hint| {
                !matches!(hint, SemanticTimelineRequestHint::TurnWorkInitial { .. })
            }),
            "collapsed final work should not auto-load work pages before explicit expansion"
        );

        assert!(expand_turn_work(&mut state, "thread_a", "turn_a"));
        let expanded =
            flatten_semantic_timeline(&state, "thread_a").expect("expanded rows should exist");
        assert!(matches!(
            &expanded.rows[1].kind,
            SemanticTimelineRowKind::WorkHeader { expanded: true, .. }
        ));
        assert!(
            expanded
                .request_hints
                .iter()
                .any(|hint| matches!(hint, SemanticTimelineRequestHint::TurnWorkInitial { turn_id, .. } if turn_id == "turn_a")),
            "explicit expand should request one bounded initial work page"
        );
    }

    #[test]
    fn hidden_event_flood_absent_from_work_page_does_not_create_visible_rows() {
        let mut top_work = turn_work_block("thread_a", "block_work", "002");
        let mut page_work = work_block("turn_a");
        if let TimelineBlockKind::TurnWork { work } = &mut top_work.kind {
            work.work_count = 10_001;
            work.visible_work_count = 1;
            work.hidden_work_count = 10_000;
        }
        page_work.work_count = 10_001;
        page_work.visible_work_count = 1;
        page_work.hidden_work_count = 10_000;

        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![top_work]),
            TopLevelPageMergeMode::Reset
        ));
        assert!(apply_turn_work_page(
            &mut state,
            work_page_with_work(page_work, vec![command_work_item("work_visible", "001")]),
            WorkPageMergeMode::Reset
        ));

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        let work_rows = flattened
            .rows
            .iter()
            .filter(|row| matches!(&row.kind, SemanticTimelineRowKind::WorkItem { .. }))
            .collect::<Vec<_>>();
        assert_eq!(work_rows.len(), 1);
        assert!(matches!(
            &work_rows[0].kind,
            SemanticTimelineRowKind::WorkItem { item }
                if item.item_type == TurnItemType::CommandExecution
                    && item.item_id == "item_work_visible"
        ));
    }

    #[test]
    fn prepending_older_top_level_page_preserves_visible_anchor_identity() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![
                block("thread_a", "block_b", "002"),
                block("thread_a", "block_c", "003"),
            ]),
            TopLevelPageMergeMode::Reset
        ));
        let thread = state.thread_mut("thread_a");
        thread.top_level.loaded_range.before_cursor = Some(TimelineCursor {
            value: "older".to_owned(),
        });
        thread.top_level.loaded_range.has_more_before = true;
        let anchor_row = SemanticTimelineVisibleRow {
            row_id: SemanticTimelineRowId::TopLevelBlock {
                block_id: "block_b".to_owned(),
            },
            top_offset_px: 13,
        };

        let plan = plan_semantic_timeline_requests(
            thread,
            &SemanticTimelineRequestPlannerInput {
                visible_rows: vec![anchor_row.clone()],
                leading_threshold_rows: 0,
                ..SemanticTimelineRequestPlannerInput::default()
            },
        );
        assert_eq!(
            plan.anchor,
            Some(SemanticTimelineStableAnchor {
                row_id: anchor_row.row_id.clone(),
                top_offset_px: 13,
            })
        );
        assert_eq!(plan.actions.len(), 1);

        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![block("thread_a", "block_a", "001")]),
            TopLevelPageMergeMode::MergeBefore
        ));
        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        assert_eq!(
            flattened
                .rows
                .iter()
                .position(|row| row.id == anchor_row.row_id),
            Some(1),
            "same semantic row id must survive prepending so desktop can restore scroll"
        );
    }

    #[test]
    fn planner_requests_top_level_before_only_near_boundary_and_dedupes_in_flight() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![
                block("thread_a", "block_a", "001"),
                block("thread_a", "block_b", "002"),
                block("thread_a", "block_c", "003"),
            ]),
            TopLevelPageMergeMode::Reset
        ));
        let thread = state.thread_mut("thread_a");
        thread.top_level.loaded_range.before_cursor = Some(TimelineCursor {
            value: "older".to_owned(),
        });
        thread.top_level.loaded_range.has_more_before = true;

        let visible = SemanticTimelineVisibleRow {
            row_id: SemanticTimelineRowId::TopLevelBlock {
                block_id: "block_a".to_owned(),
            },
            top_offset_px: -7,
        };
        let plan = plan_semantic_timeline_requests(
            thread,
            &SemanticTimelineRequestPlannerInput {
                visible_rows: vec![visible.clone()],
                leading_threshold_rows: 0,
                top_level_limit: 25,
                ..SemanticTimelineRequestPlannerInput::default()
            },
        );
        assert_eq!(
            plan.anchor,
            Some(SemanticTimelineStableAnchor {
                row_id: visible.row_id.clone(),
                top_offset_px: -7
            })
        );
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(
            &plan.actions[0],
            SemanticTimelineRequestAction::ThreadTimelinePage {
                params: ThreadTimelinePageParams {
                    anchor: TimelinePageAnchor::Before { cursor },
                    limit: Some(25),
                    ..
                },
                ..
            } if cursor.value == "older"
        ));

        let mut in_flight = HashSet::new();
        in_flight.insert(SemanticTimelineRequestKey::ThreadBefore {
            thread_id: "thread_a".to_owned(),
            cursor: "older".to_owned(),
        });
        let deduped = plan_semantic_timeline_requests(
            thread,
            &SemanticTimelineRequestPlannerInput {
                visible_rows: vec![visible],
                leading_threshold_rows: 0,
                in_flight,
                ..SemanticTimelineRequestPlannerInput::default()
            },
        );
        assert!(deduped.actions.is_empty());
    }

    #[test]
    fn planner_does_not_request_top_level_before_away_from_boundary() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![
                block("thread_a", "block_a", "001"),
                block("thread_a", "block_b", "002"),
                block("thread_a", "block_c", "003"),
            ]),
            TopLevelPageMergeMode::Reset
        ));
        let thread = state.thread_mut("thread_a");
        thread.top_level.loaded_range.before_cursor = Some(TimelineCursor {
            value: "older".to_owned(),
        });
        thread.top_level.loaded_range.has_more_before = true;

        let plan = plan_semantic_timeline_requests(
            thread,
            &SemanticTimelineRequestPlannerInput {
                visible_rows: vec![SemanticTimelineVisibleRow {
                    row_id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: "block_b".to_owned(),
                    },
                    top_offset_px: 0,
                }],
                leading_threshold_rows: 0,
                ..SemanticTimelineRequestPlannerInput::default()
            },
        );
        assert!(plan.actions.is_empty());
    }

    #[test]
    fn planner_requests_initial_work_page_for_visible_expanded_header() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![turn_work_block("thread_a", "block_work", "002")]),
            TopLevelPageMergeMode::Reset
        ));
        let thread = state.thread("thread_a").expect("thread cache should exist");

        let plan = plan_semantic_timeline_requests(
            thread,
            &SemanticTimelineRequestPlannerInput {
                visible_rows: vec![SemanticTimelineVisibleRow {
                    row_id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: "block_work".to_owned(),
                    },
                    top_offset_px: 0,
                }],
                turn_work_limit: 40,
                ..SemanticTimelineRequestPlannerInput::default()
            },
        );
        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(
            &plan.actions[0],
            SemanticTimelineRequestAction::TurnWorkPage {
                params: TurnWorkPageParams {
                    turn_id,
                    anchor: TimelinePageAnchor::Newest,
                    limit: Some(40),
                    ..
                },
                ..
            } if turn_id == "turn_a"
        ));
    }

    fn thread_page(blocks: Vec<TimelineBlock>) -> ThreadTimelinePageResponse {
        ThreadTimelinePageResponse {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            projection_version: 1,
            blocks,
            page: TimelinePageInfo {
                before_cursor: Some(TimelineCursor {
                    value: "before".to_owned(),
                }),
                after_cursor: Some(TimelineCursor {
                    value: "after".to_owned(),
                }),
                has_more_before: false,
                has_more_after: false,
            },
        }
    }

    fn block(thread_id: &str, block_id: &str, sort_key: &str) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: thread_id.to_owned(),
            block_id: block_id.to_owned(),
            turn_id: Some(format!("turn_{block_id}")),
            sort_key: sort_key.to_owned(),
            started_at_unix_ms: None,
            updated_at_unix_ms: None,
            kind: TimelineBlockKind::TurnState {
                state: TurnWorkState::Running,
                message: None,
            },
        }
    }

    fn user_block(thread_id: &str, block_id: &str, sort_key: &str, turn_id: &str) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: thread_id.to_owned(),
            block_id: block_id.to_owned(),
            turn_id: Some(turn_id.to_owned()),
            sort_key: sort_key.to_owned(),
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(1),
            kind: TimelineBlockKind::UserMessage {
                item_id: Some(format!("item_{block_id}")),
                inputs: Vec::new(),
                text: "user input".to_owned(),
                attachments: Vec::new(),
            },
        }
    }

    fn turn_work_block(thread_id: &str, block_id: &str, sort_key: &str) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: thread_id.to_owned(),
            block_id: block_id.to_owned(),
            turn_id: Some("turn_a".to_owned()),
            sort_key: sort_key.to_owned(),
            started_at_unix_ms: None,
            updated_at_unix_ms: None,
            kind: TimelineBlockKind::TurnWork {
                work: work_block("turn_a"),
            },
        }
    }

    fn assistant_block(
        thread_id: &str,
        block_id: &str,
        sort_key: &str,
        turn_id: &str,
        markdown: Option<pioneer_protocol::MarkdownDocument>,
    ) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: thread_id.to_owned(),
            block_id: block_id.to_owned(),
            turn_id: Some(turn_id.to_owned()),
            sort_key: sort_key.to_owned(),
            started_at_unix_ms: Some(3),
            updated_at_unix_ms: Some(3),
            kind: TimelineBlockKind::AssistantMessage {
                item_id: format!("item_{block_id}"),
                text: "final **markdown**".to_owned(),
                status: TurnWorkItemStatus::Completed,
                markdown,
            },
        }
    }

    fn work_page(items: Vec<TurnWorkItem>) -> TurnWorkPageResponse {
        work_page_with_work(work_block("turn_a"), items)
    }

    fn work_page_with_work(work: TurnWorkBlock, items: Vec<TurnWorkItem>) -> TurnWorkPageResponse {
        TurnWorkPageResponse {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            projection_version: 1,
            work,
            items,
            page: TimelinePageInfo {
                before_cursor: Some(TimelineCursor {
                    value: "work-before".to_owned(),
                }),
                after_cursor: Some(TimelineCursor {
                    value: "work-after".to_owned(),
                }),
                has_more_before: false,
                has_more_after: false,
            },
        }
    }

    fn work_block(turn_id: &str) -> TurnWorkBlock {
        TurnWorkBlock {
            turn_id: turn_id.to_owned(),
            presentation: TurnWorkPresentation::ExpandedLive,
            state: TurnWorkState::Running,
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
            elapsed_ms: None,
            work_count: 1,
            visible_work_count: 1,
            hidden_work_count: 0,
            has_more_before: false,
            has_more_after: false,
            before_cursor: None,
            after_cursor: None,
            first_work_item_id: None,
            last_work_item_id: None,
        }
    }

    fn command_work_item(work_item_id: &str, order_key: &str) -> TurnWorkItem {
        TurnWorkItem {
            work_item_id: work_item_id.to_owned(),
            item_id: format!("item_{work_item_id}"),
            turn_id: "turn_a".to_owned(),
            order_key: order_key.to_owned(),
            item_type: TurnItemType::CommandExecution,
            status: TurnWorkItemStatus::Completed,
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
            item: TurnItem::CommandExecution {
                id: format!("item_{work_item_id}"),
                tool_name: "exec_command".to_owned(),
                arguments: serde_json::json!({ "command": ["echo", "ok"] }),
                status: pioneer_protocol::ToolCallStatus::Completed,
                recovery_policy: None,
                output_policy: pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(
                    "exec_command",
                ),
                display: pioneer_protocol::ToolDisplayPayload::Hidden,
                storage: pioneer_protocol::ToolStoragePayload::None,
                recovery: None,
                command: vec!["echo".to_owned(), "ok".to_owned()],
                cwd: None,
                success: Some(true),
                outcome: None,
                observation: None,
            },
            metadata: None,
        }
    }

    fn work_item(work_item_id: &str, order_key: &str) -> TurnWorkItem {
        TurnWorkItem {
            work_item_id: work_item_id.to_owned(),
            item_id: format!("item_{work_item_id}"),
            turn_id: "turn_a".to_owned(),
            order_key: order_key.to_owned(),
            item_type: TurnItemType::SystemEvent,
            status: TurnWorkItemStatus::Completed,
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
            item: TurnItem::SystemEvent {
                id: format!("item_{work_item_id}"),
                level: SystemEventLevel::Info,
                message: work_item_id.to_owned(),
                code: None,
                details: None,
            },
            metadata: None,
        }
    }
}
