//! Shared semantic timeline state.
//!
//! This module is intentionally platform-neutral. Desktop and mobile shells can
//! keep scroll/list/rendering state locally, but semantic block and work-range
//! cache ownership lives here.

use crate::conversation::ConversationEvent;
use pioneer_protocol::{
    AgentMessagePhase, ItemDeltaStream, MarkdownDocument, SystemEventLevel, TaskAttachmentMode,
    TaskStatus, TaskTriggerKind, ThreadComposerExecutionMode, ThreadMode,
    ThreadTimelineBlocksChangedNotification, ThreadTimelinePageParams, ThreadTimelinePageResponse,
    TimelineBlock, TimelineBlockKind, TimelineCursor, TimelinePageAnchor, TimelinePageInfo, Turn,
    TurnItem, TurnKind, TurnOrigin, TurnStatus, TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus,
    TurnWorkItemsChangedNotification, TurnWorkItemsGetParams, TurnWorkItemsGetResponse,
    TurnWorkPageParams, TurnWorkPageResponse, TurnWorkPresentation, TurnWorkState,
    TurnWorkStateChangedNotification, UserMessageAttachment,
};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

pub const DEFAULT_TOP_LEVEL_PAGE_LIMIT: u32 = 12;
pub const DEFAULT_TURN_WORK_PAGE_LIMIT: u32 = 30;
pub const DEFAULT_PREFETCH_THRESHOLD_ROWS: usize = 3;

pub type ThreadId = String;
pub type TurnId = String;
pub type TimelineBlockId = String;
pub type TurnWorkItemId = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalTurnWorkReconciliation {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub running_work_item_ids: Vec<TurnWorkItemId>,
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<pioneer_protocol::TurnAuthorSnapshot>,
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
    DetachedTaskRun {
        block: TimelineBlock,
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
    TurnWorkItems {
        thread_id: ThreadId,
        turn_id: TurnId,
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
    TurnWorkItemsGet {
        key: SemanticTimelineRequestKey,
        params: TurnWorkItemsGetParams,
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
    pub source_high_watermark: i64,
    pub projection_updated_at_unix_micros: i64,
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

    pub fn running_work_item_ids(&self) -> Vec<TurnWorkItemId> {
        let mut ids = self
            .items_by_id
            .values()
            .filter(|item| item.status == TurnWorkItemStatus::Running)
            .map(|item| item.work_item_id.clone())
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }
}

/// Returns the canonical work items that a shell must reconcile when a Turn
/// reaches a non-running lifecycle state. This closes the notification-gap
/// case where the shell received the Turn terminal event but missed one or
/// more preceding item completion events.
pub fn terminal_turn_work_reconciliation(
    state: &SemanticTimelineState,
    event: &ConversationEvent,
) -> Option<TerminalTurnWorkReconciliation> {
    let (thread_id, turn_id) = match event {
        ConversationEvent::TurnCompleted { thread_id, turn }
        | ConversationEvent::TurnFailed { thread_id, turn }
        | ConversationEvent::TurnBlocked {
            thread_id, turn, ..
        } => (thread_id, turn.id.as_str()),
        _ => return None,
    };
    let running_work_item_ids = state
        .thread(thread_id)
        .and_then(|thread| thread.work_range(turn_id))
        .map(TurnWorkRangeCache::running_work_item_ids)
        .unwrap_or_default();
    if running_work_item_ids.is_empty() {
        return None;
    }
    Some(TerminalTurnWorkReconciliation {
        thread_id: thread_id.clone(),
        turn_id: turn_id.to_owned(),
        running_work_item_ids,
    })
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
    #[serde(skip)]
    #[cfg_attr(any(feature = "schema", test), schemars(skip))]
    detached_task_run_turn_ids: HashSet<TurnId>,
    #[serde(skip)]
    #[cfg_attr(any(feature = "schema", test), schemars(skip))]
    suppressed_optimistic_turn_work_ids: HashSet<TurnId>,
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
    let source_high_watermark = page.source_high_watermark;
    let projection_updated_at_unix_micros = page.projection_updated_at_unix_micros;
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
        source_high_watermark,
        projection_updated_at_unix_micros,
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
    work.presentation == TurnWorkPresentation::ExpandedLive
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
            TimelineBlockKind::UserMessage { author, .. } => rows.push(SemanticTimelineRow {
                id: SemanticTimelineRowId::TopLevelBlock {
                    block_id: block.block_id.clone(),
                },
                author: author.clone(),
                kind: SemanticTimelineRowKind::UserBlock {
                    block: block.clone(),
                },
            }),
            TimelineBlockKind::TurnWork { work } => {
                let Some(author) = ready_agent_timeline_author(work.author.as_ref()) else {
                    continue;
                };
                let expanded = resolve_work_expanded(work, &thread.expansion);
                let work_range = thread.work_range(work.turn_id.as_str());
                rows.push(SemanticTimelineRow {
                    id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: block.block_id.clone(),
                    },
                    author: Some(author.clone()),
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
            TimelineBlockKind::DetachedTaskRun { author, .. } => {
                let Some(author) = ready_agent_timeline_author(author.as_ref()) else {
                    continue;
                };
                rows.push(SemanticTimelineRow {
                    id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: block.block_id.clone(),
                    },
                    author: Some(author.clone()),
                    kind: SemanticTimelineRowKind::DetachedTaskRun {
                        block: block.clone(),
                    },
                });
            }
            TimelineBlockKind::AssistantMessage { author, .. } => {
                let Some(author) = ready_agent_timeline_author(author.as_ref()) else {
                    continue;
                };
                rows.push(SemanticTimelineRow {
                    id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: block.block_id.clone(),
                    },
                    author: Some(author.clone()),
                    kind: SemanticTimelineRowKind::AssistantMessage {
                        block: block.clone(),
                    },
                });
            }
            TimelineBlockKind::PendingRequest { author, .. } => {
                let Some(author) = ready_agent_timeline_author(author.as_ref()) else {
                    continue;
                };
                rows.push(SemanticTimelineRow {
                    id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: block.block_id.clone(),
                    },
                    author: Some(author.clone()),
                    kind: SemanticTimelineRowKind::PendingRequest {
                        block: block.clone(),
                    },
                });
            }
            TimelineBlockKind::TurnState { author, .. } => {
                let Some(author) = ready_timeline_state_author(author.as_ref()) else {
                    continue;
                };
                rows.push(SemanticTimelineRow {
                    id: SemanticTimelineRowId::TopLevelBlock {
                        block_id: block.block_id.clone(),
                    },
                    author: author.cloned(),
                    kind: SemanticTimelineRowKind::TurnState {
                        block: block.clone(),
                    },
                });
            }
        }
    }

    SemanticTimelineRows {
        thread_id: thread.thread_id.clone(),
        rows,
        request_hints,
    }
}

fn ready_timeline_state_author(
    author: Option<&pioneer_protocol::TurnAuthorSnapshot>,
) -> Option<Option<&pioneer_protocol::TurnAuthorSnapshot>> {
    match author {
        None => Some(None),
        Some(author) => ready_agent_timeline_author(Some(author)).map(Some),
    }
}

fn ready_agent_timeline_author(
    author: Option<&pioneer_protocol::TurnAuthorSnapshot>,
) -> Option<&pioneer_protocol::TurnAuthorSnapshot> {
    let author = author?;
    let pioneer_protocol::PersistedActorRef::AgentExecution(execution_id) = &author.actor else {
        return None;
    };
    author
        .agent
        .as_ref()
        .is_some_and(|agent| &agent.agent_execution_id == execution_id)
        .then_some(author)
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
    let detached_turn_ids = page
        .blocks
        .iter()
        .filter_map(|block| {
            matches!(&block.kind, TimelineBlockKind::DetachedTaskRun { .. })
                .then(|| block.turn_id.clone())
                .flatten()
        })
        .collect::<HashSet<_>>();
    let thread = state.thread_mut(thread_id);
    let before = thread.clone();
    if merge_mode == TopLevelPageMergeMode::Reset {
        thread.detached_task_run_turn_ids.clear();
    }
    thread
        .detached_task_run_turn_ids
        .extend(detached_turn_ids.iter().cloned());
    apply_top_level_page(&mut thread.top_level, page, merge_mode);
    for turn_id in detached_turn_ids {
        remove_turn_work_state(thread, turn_id.as_str());
    }
    before != *thread
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

    for incoming in page.blocks {
        let existing = cache
            .blocks_by_id
            .get(incoming.block_id.as_str())
            .or_else(|| {
                (merge_mode == TopLevelPageMergeMode::Reset)
                    .then(|| before.blocks_by_id.get(incoming.block_id.as_str()))
                    .flatten()
            });
        let block = newest_top_level_block(existing, incoming);
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

pub fn apply_semantic_timeline_live_update_with_patch(
    state: &mut SemanticTimelineState,
    update: SemanticTimelineLiveUpdate,
) -> SemanticTimelineCachePatch {
    let (workspace_id, thread_id) = match &update {
        SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(notification) => (
            notification.workspace_id.clone(),
            notification.thread_id.clone(),
        ),
        SemanticTimelineLiveUpdate::TurnWorkItemsChanged(notification) => (
            notification.workspace_id.clone(),
            notification.thread_id.clone(),
        ),
        SemanticTimelineLiveUpdate::TurnWorkStateChanged(notification) => (
            notification.workspace_id.clone(),
            notification.thread_id.clone(),
        ),
    };
    let before = state.thread(thread_id.as_str()).cloned();
    if !apply_semantic_timeline_live_update(state, update) {
        return SemanticTimelineCachePatch {
            workspace_id,
            thread_id,
            ..SemanticTimelineCachePatch::default()
        };
    }
    let after = state.thread(thread_id.as_str());
    semantic_timeline_cache_patch_from_diff(&workspace_id, thread_id, before.as_ref(), after)
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
            mode,
            user_text,
            attachments,
            ..
        } => apply_local_turn_start_requested_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            *mode,
            user_text,
            attachments,
            now_unix_ms,
        ),
        ConversationEvent::LocalTurnStartAccepted {
            thread_id,
            turn_id,
            mode,
            ..
        } => {
            if *mode == ThreadMode::Message {
                let thread = state.thread_mut(thread_id.to_owned());
                let before = thread.clone();
                remove_turn_work_state(thread, turn_id);
                before != *thread
            } else {
                apply_turn_state_to_semantic_timeline(
                    state,
                    workspace_id,
                    thread_id,
                    turn_id,
                    TurnWorkState::Running,
                    None,
                    now_unix_ms,
                )
            }
        }
        ConversationEvent::TurnStarted {
            thread_id, turn, ..
        } => apply_started_turn_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn,
            now_unix_ms,
        ),
        ConversationEvent::LocalTurnStartRejected {
            thread_id,
            turn_id,
            mode,
            error,
            ..
        } => apply_local_turn_start_rejected_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            *mode,
            error,
            now_unix_ms,
        ),
        ConversationEvent::TurnCompleted { thread_id, turn } => {
            apply_terminal_or_detached_turn_to_semantic_timeline(
                state,
                workspace_id,
                thread_id,
                turn,
                TurnWorkState::Completed,
                now_unix_ms,
            )
        }
        ConversationEvent::TurnFailed { thread_id, turn } => {
            apply_terminal_or_detached_turn_to_semantic_timeline(
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
        } => apply_terminal_or_detached_turn_to_semantic_timeline(
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
        } => apply_item_completed_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            item,
            now_unix_ms,
        ),
        ConversationEvent::ItemUpdated {
            thread_id,
            turn_id,
            item,
        } => apply_item_updated_to_semantic_timeline(
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

/// Applies an optimistic local Composer event according to the thread product policy.
///
/// A collaborative parent submission is admission for a detached Task. Its user
/// message is optimistic, but foreground work is not: the canonical Task card
/// will arrive from the gateway after admission.
pub fn apply_local_composer_event_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    event: &ConversationEvent,
    execution_mode: ThreadComposerExecutionMode,
    now_unix_ms: i64,
) -> bool {
    let changed =
        apply_conversation_event_to_semantic_timeline(state, workspace_id, event, now_unix_ms);
    if execution_mode != ThreadComposerExecutionMode::DetachedTask {
        return changed;
    }
    let ConversationEvent::LocalTurnStartRequested {
        thread_id, turn_id, ..
    } = event
    else {
        return changed;
    };

    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    thread
        .suppressed_optimistic_turn_work_ids
        .insert(turn_id.to_owned());
    remove_turn_work_state(thread, turn_id);
    changed || before != *thread
}

pub fn apply_local_composer_event_to_semantic_timeline_with_patch(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    event: &ConversationEvent,
    execution_mode: ThreadComposerExecutionMode,
    now_unix_ms: i64,
) -> SemanticTimelineCachePatch {
    let Some(thread_id) = event.thread_id().map(str::to_owned) else {
        return SemanticTimelineCachePatch::default();
    };
    let before = state.thread(thread_id.as_str()).cloned();
    if !apply_local_composer_event_to_semantic_timeline(
        state,
        workspace_id,
        event,
        execution_mode,
        now_unix_ms,
    ) {
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
    mode: ThreadMode,
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
        mode,
        user_text.to_owned(),
        attachments.to_vec(),
        now_unix_ms,
    );
    if mode == ThreadMode::Message {
        remove_turn_work_state(thread, turn_id);
    } else {
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
    }
    before != *thread
}

fn apply_started_turn_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn: &Turn,
    now_unix_ms: i64,
) -> bool {
    if turn.mode == ThreadMode::Message {
        let thread = state.thread_mut(thread_id.to_owned());
        let before = thread.clone();
        remove_turn_work_state(thread, turn.id.as_str());
        return before != *thread;
    }
    if !turn_is_detached_task_run(turn) {
        return apply_turn_state_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn.id.as_str(),
            TurnWorkState::Running,
            None,
            now_unix_ms,
        );
    }

    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    thread.detached_task_run_turn_ids.insert(turn.id.clone());
    remove_turn_work_state(thread, turn.id.as_str());
    before != *thread
}

fn apply_terminal_or_detached_turn_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn: &Turn,
    fallback_state: TurnWorkState,
    now_unix_ms: i64,
) -> bool {
    if turn.mode == ThreadMode::Message {
        let thread = state.thread_mut(thread_id.to_owned());
        let before = thread.clone();
        remove_turn_work_state(thread, turn.id.as_str());
        return before != *thread;
    }
    if turn_is_detached_task_run(turn) {
        let thread = state.thread_mut(thread_id.to_owned());
        let before = thread.clone();
        thread.detached_task_run_turn_ids.insert(turn.id.clone());
        remove_turn_work_state(thread, turn.id.as_str());
        return before != *thread;
    }

    apply_terminal_turn_to_semantic_timeline(
        state,
        workspace_id,
        thread_id,
        turn,
        fallback_state,
        now_unix_ms,
    )
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
    if thread.suppressed_optimistic_turn_work_ids.contains(turn_id) {
        remove_turn_work_state(thread, turn_id);
        return before != *thread;
    }
    let has_final = turn_has_assistant_block(thread, turn_id);
    let presentation = if has_final {
        TurnWorkPresentation::CollapsedAfterFinal
    } else if completed_at_unix_ms.is_some() {
        TurnWorkPresentation::CollapsedAfterFinal
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
    if completed_at_unix_ms.is_some()
        && !has_final
        && matches!(
            work_state,
            TurnWorkState::Failed | TurnWorkState::Interrupted | TurnWorkState::Blocked
        )
    {
        upsert_terminal_state_block(
            thread,
            workspace_id,
            thread_id,
            turn_id,
            work_state,
            None,
            now_unix_ms,
        );
    } else {
        remove_terminal_state_block(thread, turn_id);
    }
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
    if work_state == TurnWorkState::Completed {
        let thread = state.thread_mut(thread_id.to_owned());
        let has_final = turn_has_assistant_block(thread, turn.id.as_str());
        let has_work = thread
            .work_range(turn.id.as_str())
            .is_some_and(|range| !range.items_by_id.is_empty())
            || thread
                .cached_turn_work_block(turn.id.as_str())
                .is_some_and(|work| work.work_count > 0);
        if !has_final && !has_work {
            let before = thread.clone();
            remove_turn_work_state(thread, turn.id.as_str());
            return before != *thread;
        }
    }
    let changed = apply_turn_state_to_semantic_timeline(
        state,
        workspace_id,
        thread_id,
        turn.id.as_str(),
        work_state,
        Some(now_unix_ms),
        now_unix_ms,
    );
    if !matches!(
        work_state,
        TurnWorkState::Failed | TurnWorkState::Interrupted | TurnWorkState::Blocked
    ) {
        return changed;
    }

    let thread = state.thread_mut(thread_id.to_owned());
    if turn_has_assistant_block(thread, turn.id.as_str()) {
        return changed;
    }
    let before = thread.clone();
    upsert_terminal_state_block(
        thread,
        workspace_id,
        thread_id,
        turn.id.as_str(),
        work_state,
        turn.error.clone(),
        now_unix_ms,
    );
    changed || before != *thread
}

fn apply_local_turn_start_rejected_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    mode: ThreadMode,
    error: &str,
    now_unix_ms: i64,
) -> bool {
    let changed = if mode == ThreadMode::Message {
        let thread = state.thread_mut(thread_id.to_owned());
        let before = thread.clone();
        remove_turn_work_state(thread, turn_id);
        before != *thread
    } else {
        apply_turn_state_to_semantic_timeline(
            state,
            workspace_id,
            thread_id,
            turn_id,
            TurnWorkState::Failed,
            Some(now_unix_ms),
            now_unix_ms,
        )
    };
    let thread = state.thread_mut(thread_id.to_owned());
    let before = thread.clone();
    upsert_terminal_state_block(
        thread,
        workspace_id,
        thread_id,
        turn_id,
        TurnWorkState::Failed,
        Some(error.to_owned()),
        now_unix_ms,
    );
    changed || before != *thread
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
    reconcile_task_attachment_marker(thread, turn_id, item);
    match live_item_placement(thread, turn_id, item) {
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
                ThreadMode::Agent,
                text.clone(),
                attachments.clone(),
                now_unix_ms,
            );
        }
        LiveItemPlacement::TopLevelAssistant => {
            remove_turn_work_item(thread, turn_id, item.item_id());
            upsert_assistant_message_block(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                item,
                TurnWorkItemStatus::Running,
                now_unix_ms,
            );
            remove_terminal_state_block(thread, turn_id);
            if thread_has_detached_task_run(thread, turn_id) {
                remove_turn_work_state(thread, turn_id);
            } else {
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
        LiveItemPlacement::DetachedTaskRun => {
            remove_turn_work_item(thread, turn_id, item.item_id());
            upsert_detached_task_run_block(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                item,
                now_unix_ms,
            );
            remove_turn_work_state(thread, turn_id);
        }
        LiveItemPlacement::TurnWork => {
            if thread_has_detached_task_run(thread, turn_id) {
                remove_turn_work_state(thread, turn_id);
                return before != *thread;
            }
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
                phase: AgentMessagePhase::Commentary,
                markdown: markdown.cloned(),
                markdown_version: None,
            };
            upsert_turn_work_item(
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
                current_or_live_work_presentation(thread, turn_id),
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
    reconcile_task_attachment_marker(thread, turn_id, item);
    match live_item_placement(thread, turn_id, item) {
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
                    ThreadMode::Agent,
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
            remove_terminal_state_block(thread, turn_id);
            if thread_has_detached_task_run(thread, turn_id) {
                remove_turn_work_state(thread, turn_id);
            } else {
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
        LiveItemPlacement::DetachedTaskRun => {
            remove_turn_work_item(thread, turn_id, item.item_id());
            upsert_detached_task_run_block(
                thread,
                workspace_id,
                thread_id,
                turn_id,
                item,
                now_unix_ms,
            );
            remove_turn_work_state(thread, turn_id);
        }
        LiveItemPlacement::TurnWork => {
            if thread_has_detached_task_run(thread, turn_id) {
                remove_turn_work_state(thread, turn_id);
                return before != *thread;
            }
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

fn apply_item_updated_to_semantic_timeline(
    state: &mut SemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item: &TurnItem,
    now_unix_ms: i64,
) -> bool {
    if matches!(
        item,
        TurnItem::Task { item } if item.attachment == TaskAttachmentMode::Attached
    ) {
        let work_item_id = work_item_projection_id(turn_id, item.item_id());
        let Some(existing) = state
            .thread(thread_id)
            .and_then(|thread| thread.work_range(turn_id))
            .and_then(|range| range.items_by_id.get(work_item_id.as_str()))
        else {
            // A late task snapshot may refer to a historical turn outside the
            // loaded window. Let the canonical pagination APIs refresh that
            // item; do not resurrect its old Worked group in the live tail.
            return false;
        };
        let source_updated_at_unix_micros = existing
            .source_updated_at_unix_micros
            .max(now_unix_ms.saturating_mul(1_000));
        let thread = state.thread_mut(thread_id.to_owned());
        let before = thread.clone();
        if let Some(existing) = thread
            .work_range_mut(turn_id.to_owned())
            .items_by_id
            .get_mut(work_item_id.as_str())
        {
            existing.item = item.clone();
            existing.item_type = item.item_type();
            existing.source_updated_at_unix_micros = source_updated_at_unix_micros;
        }
        return before != *thread;
    }

    apply_item_completed_to_semantic_timeline(
        state,
        workspace_id,
        thread_id,
        turn_id,
        item,
        now_unix_ms,
    )
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
    let incoming_projection_revision = (
        page.source_high_watermark,
        page.projection_updated_at_unix_micros,
    );
    let cached_projection_revision = (
        range.source_high_watermark,
        range.projection_updated_at_unix_micros,
    );
    let page_can_advance_newest_boundary =
        incoming_projection_revision >= cached_projection_revision;
    let live_running_items = if merge_mode == WorkPageMergeMode::Reset {
        live_running_work_items(range)
    } else {
        Vec::new()
    };

    if merge_mode == WorkPageMergeMode::Reset {
        range.items_by_id.clear();
        range.ordered_item_ids.clear();
        range.stale_work_item_ids.clear();
    }

    range.thread_id = page.thread_id;
    range.turn_id = page.turn_id;
    if page_can_advance_newest_boundary {
        let mut incoming_work = page.work;
        if let Some(existing_work) = range.work.as_ref() {
            preserve_newer_agent_work_graph(existing_work, &mut incoming_work);
        }
        range.work = Some(incoming_work);
        range.source_high_watermark = page.source_high_watermark;
        range.projection_updated_at_unix_micros = page.projection_updated_at_unix_micros;
    }
    for item in page.items {
        merge_turn_work_item(range, item);
    }
    restore_missing_live_running_work_items(range, live_running_items);
    sort_work_items(range);
    merge_work_loaded_range(
        range,
        &page.page,
        merge_mode,
        was_empty,
        page_can_advance_newest_boundary,
    );
    range.request_status = TimelineRequestStatus::Ready;

    before != *range
}

pub fn apply_turn_work_items_get_response(
    state: &mut SemanticTimelineState,
    response: TurnWorkItemsGetResponse,
) -> bool {
    let thread = state.thread_mut(response.thread_id.clone());
    let range = thread.work_range_mut(response.turn_id.clone());
    apply_work_items_get_response(range, response)
}

pub fn apply_work_items_get_response(
    range: &mut TurnWorkRangeCache,
    response: TurnWorkItemsGetResponse,
) -> bool {
    let before = range.clone();
    range.thread_id = response.thread_id;
    range.turn_id = response.turn_id;
    let response_revision = (
        response.source_high_watermark,
        response.projection_updated_at_unix_micros,
    );
    let cached_revision = (
        range.source_high_watermark,
        range.projection_updated_at_unix_micros,
    );

    for item in response.items {
        merge_turn_work_item(range, item);
    }
    if response_revision >= cached_revision {
        for work_item_id in response.removed_work_item_ids {
            range.items_by_id.remove(work_item_id.as_str());
            range.stale_work_item_ids.remove(work_item_id.as_str());
        }
        range.source_high_watermark = response.source_high_watermark;
        range.projection_updated_at_unix_micros = response.projection_updated_at_unix_micros;
    }
    range
        .ordered_item_ids
        .retain(|work_item_id| range.items_by_id.contains_key(work_item_id));
    sort_work_items(range);
    range.request_status = TimelineRequestStatus::Ready;

    before != *range
}

fn merge_turn_work_item(range: &mut TurnWorkRangeCache, item: TurnWorkItem) {
    if let Some(existing) = range.items_by_id.get(item.work_item_id.as_str())
        && !incoming_work_item_is_newer(existing, &item)
    {
        range.stale_work_item_ids.remove(item.work_item_id.as_str());
        return;
    }

    remove_existing_work_items_for_item_id(
        range,
        item.item_id.as_str(),
        item.work_item_id.as_str(),
    );
    range.stale_work_item_ids.remove(item.work_item_id.as_str());
    range.items_by_id.insert(item.work_item_id.clone(), item);
}

fn incoming_work_item_is_newer(existing: &TurnWorkItem, incoming: &TurnWorkItem) -> bool {
    // Work-item terminality is monotonic. A terminal canonical projection is
    // always allowed to close an optimistic/live running row, even when an
    // older server encoded the item's first-event sequence as its revision or
    // the local receipt timestamp is later than the server timestamp.
    if existing.status == TurnWorkItemStatus::Running
        && is_terminal_turn_work_item_status(incoming.status)
    {
        return true;
    }
    if is_terminal_turn_work_item_status(existing.status)
        && incoming.status == TurnWorkItemStatus::Running
    {
        return false;
    }

    match incoming.source_sequence.cmp(&existing.source_sequence) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => {
            incoming.source_updated_at_unix_micros >= existing.source_updated_at_unix_micros
        }
    }
}

fn live_running_work_items(range: &TurnWorkRangeCache) -> Vec<TurnWorkItem> {
    range
        .ordered_item_ids
        .iter()
        .filter_map(|work_item_id| range.items_by_id.get(work_item_id.as_str()))
        .filter(|item| item.status == TurnWorkItemStatus::Running)
        .cloned()
        .collect()
}

fn restore_missing_live_running_work_items(
    range: &mut TurnWorkRangeCache,
    items: Vec<TurnWorkItem>,
) {
    for item in items {
        if range.items_by_id.contains_key(item.work_item_id.as_str()) {
            continue;
        }
        if range
            .items_by_id
            .values()
            .any(|existing| existing.item_id == item.item_id)
        {
            continue;
        }
        range.stale_work_item_ids.remove(item.work_item_id.as_str());
        range.items_by_id.insert(item.work_item_id.clone(), item);
    }
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
    let mut work = notification.work;
    let incoming_revision = (
        notification.source_high_watermark,
        notification.projection_updated_at_unix_micros,
    );

    let range = thread.work_range_mut(turn_id.as_str());
    if incoming_revision
        < (
            range.source_high_watermark,
            range.projection_updated_at_unix_micros,
        )
    {
        return false;
    }
    if let Some(existing_work) = range.work.as_ref() {
        preserve_newer_agent_work_graph(existing_work, &mut work);
    }
    range.work = Some(work.clone());
    range.source_high_watermark = notification.source_high_watermark;
    range.projection_updated_at_unix_micros = notification.projection_updated_at_unix_micros;
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
    DetachedTaskRun,
    TurnWork,
    TopLevelAssistant,
    Hidden,
}

fn live_item_placement(
    thread: &ThreadSemanticTimelineState,
    turn_id: &str,
    item: &TurnItem,
) -> LiveItemPlacement {
    match item {
        TurnItem::UserMessage { .. } => LiveItemPlacement::TopLevelUser,
        TurnItem::Task { item }
            if item.attachment == TaskAttachmentMode::Detached
                && thread_has_detached_task_run(thread, turn_id) =>
        {
            LiveItemPlacement::DetachedTaskRun
        }
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

fn detached_task_run_block_id(turn_id: &str, item_id: &str) -> String {
    format!("turn:{turn_id}:detached-task-run:{item_id}")
}

fn assistant_block_id(turn_id: &str, item_id: &str) -> String {
    format!("turn:{turn_id}:assistant:{item_id}")
}

fn terminal_state_block_id(turn_id: &str) -> String {
    format!("turn:{turn_id}:terminal-state")
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

fn timeline_event_block_sort_key(
    turn_id: &str,
    rank: u16,
    suffix: &str,
    now_unix_ms: i64,
) -> String {
    format!("{:020}:{turn_id}:{rank:03}:{suffix}", now_unix_ms.max(0))
}

fn work_item_order_key(range: Option<&TurnWorkRangeCache>, item_id: &str) -> String {
    if let Some(range) = range {
        for item in range.items_by_id.values() {
            if item.item_id == item_id {
                return item.order_key.clone();
            }
        }
    }

    // Durable work order keys use the turn event sequence. Keep provisional live keys in that
    // same ordinal domain so a missed or delayed canonical reconciliation cannot leave an older
    // live item below newer durable items. Wall-clock milliseconds are several orders of
    // magnitude larger than event sequences and therefore cannot safely be mixed here.
    let ordinal = range
        .map(|range| {
            range
                .items_by_id
                .values()
                .filter_map(|item| {
                    item.order_key
                        .split_once(':')
                        .map(|(ordinal, _)| ordinal)
                        .unwrap_or(item.order_key.as_str())
                        .parse::<i64>()
                        .ok()
                })
                .max()
                .unwrap_or_default()
                .max(range.source_high_watermark)
                .saturating_add(1)
        })
        .unwrap_or(1);
    format!("{ordinal:020}:{item_id}")
}

fn upsert_top_level_block(thread: &mut ThreadSemanticTimelineState, block: TimelineBlock) {
    let block = newest_top_level_block(
        thread.top_level.blocks_by_id.get(block.block_id.as_str()),
        block,
    );
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

fn newest_top_level_block(
    existing: Option<&TimelineBlock>,
    mut incoming: TimelineBlock,
) -> TimelineBlock {
    let Some(existing) = existing else {
        return incoming;
    };
    if let (
        TimelineBlockKind::TurnWork {
            work: existing_work,
        },
        TimelineBlockKind::TurnWork {
            work: incoming_work,
        },
    ) = (&existing.kind, &mut incoming.kind)
    {
        preserve_newer_agent_work_graph(existing_work, incoming_work);
        return incoming;
    }
    let (
        TimelineBlockKind::DetachedTaskRun {
            task: existing_task,
            ..
        },
        TimelineBlockKind::DetachedTaskRun {
            task: incoming_task,
            ..
        },
    ) = (&existing.kind, &incoming.kind)
    else {
        return incoming;
    };
    if existing_task.id != incoming_task.id {
        return incoming;
    }

    // Task terminality is monotonic. A live optimistic update may carry a
    // later client timestamp than the authoritative task projection, but it
    // must never keep a completed/failed/blocked/cancelled task rendered as
    // running. Conversely, a delayed non-terminal event cannot reopen a task
    // that the server has already made terminal.
    match (
        existing_task.status.is_terminal(),
        incoming_task.status.is_terminal(),
    ) {
        (true, false) => return existing.clone(),
        (false, true) => return incoming,
        _ => {}
    }

    let existing_revision = (
        existing_task.updated_at,
        detached_task_status_rank(existing_task.status),
    );
    let incoming_revision = (
        incoming_task.updated_at,
        detached_task_status_rank(incoming_task.status),
    );
    if existing_revision > incoming_revision {
        existing.clone()
    } else {
        incoming
    }
}

fn preserve_newer_agent_work_graph(existing: &TurnWorkBlock, incoming: &mut TurnWorkBlock) {
    let Some(existing_graph) = existing.agent_work_graph.as_ref() else {
        return;
    };
    if incoming
        .agent_work_graph
        .as_ref()
        .is_none_or(|incoming_graph| {
            incoming_graph.updated_at_unix_micros < existing_graph.updated_at_unix_micros
        })
    {
        incoming.agent_work_graph = Some(existing_graph.clone());
    }
}

const fn detached_task_status_rank(status: TaskStatus) -> u8 {
    match status {
        TaskStatus::Draft => 0,
        TaskStatus::Scheduled => 1,
        TaskStatus::Queued => 2,
        TaskStatus::Running => 3,
        TaskStatus::Waiting => 4,
        TaskStatus::WaitingReview => 5,
        TaskStatus::Completed
        | TaskStatus::Failed
        | TaskStatus::Blocked
        | TaskStatus::Cancelled => 6,
    }
}

fn upsert_user_message_block(
    thread: &mut ThreadSemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: Option<String>,
    mode: ThreadMode,
    text: String,
    attachments: Vec<UserMessageAttachment>,
    now_unix_ms: i64,
) {
    let block_id = user_block_id(turn_id);
    let existing = thread.top_level.blocks_by_id.get(block_id.as_str());
    let (author, route) = existing
        .and_then(|block| match &block.kind {
            TimelineBlockKind::UserMessage { author, route, .. } => {
                Some((author.clone(), route.clone()))
            }
            _ => None,
        })
        .unwrap_or_default();
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
            mode,
            author,
            route,
            reply: None,
            mentions: Vec::new(),
            revision: 0,
            edited: false,
            deleted: false,
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
    let (existing_author, route) = existing
        .and_then(|block| match &block.kind {
            TimelineBlockKind::AssistantMessage { author, route, .. } => {
                Some((author.clone(), route.clone()))
            }
            _ => None,
        })
        .unwrap_or_default();
    let author = existing_author;
    let block = TimelineBlock {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        block_id,
        turn_id: Some(turn_id.to_owned()),
        sort_key: existing
            .map(|block| block.sort_key.clone())
            .unwrap_or_else(|| {
                let order_key = work_item_order_key(thread.work_range(turn_id), id);
                if thread_has_detached_task_run(thread, turn_id) {
                    timeline_event_block_sort_key(turn_id, 200, order_key.as_str(), now_unix_ms)
                } else {
                    turn_block_sort_key(thread, turn_id, 200, order_key.as_str(), now_unix_ms)
                }
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
            author,
            route,
        },
    };
    upsert_top_level_block(thread, block);
}

fn upsert_detached_task_run_block(
    thread: &mut ThreadSemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    item: &TurnItem,
    now_unix_ms: i64,
) {
    let TurnItem::Task { item: task } = item else {
        return;
    };
    thread.detached_task_run_turn_ids.insert(turn_id.to_owned());
    let block_id = detached_task_run_block_id(turn_id, task.id.as_str());
    let existing = thread.top_level.blocks_by_id.get(block_id.as_str());
    let author = existing.and_then(|block| match &block.kind {
        TimelineBlockKind::DetachedTaskRun { author, .. } => author.clone(),
        _ => None,
    });
    let suffix = format!("detached-task-run:{}", task.id);
    let block = TimelineBlock {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        block_id,
        turn_id: Some(turn_id.to_owned()),
        sort_key: existing
            .map(|block| block.sort_key.clone())
            .unwrap_or_else(|| {
                let causal_turn_id = (task.trigger_kind == TaskTriggerKind::Immediate)
                    .then_some(task.created_by_turn_id.as_deref())
                    .flatten()
                    .filter(|causal_turn_id| *causal_turn_id != turn_id);
                match causal_turn_id {
                    Some(causal_turn_id) => turn_block_sort_key(
                        thread,
                        causal_turn_id,
                        100,
                        suffix.as_str(),
                        task.created_at.saturating_mul(1_000),
                    ),
                    None => turn_block_sort_key(thread, turn_id, 100, suffix.as_str(), now_unix_ms),
                }
            }),
        started_at_unix_ms: task
            .started_at
            .map(|started_at| started_at.saturating_mul(1_000))
            .or_else(|| existing.and_then(|block| block.started_at_unix_ms))
            .or(Some(now_unix_ms)),
        updated_at_unix_ms: Some(now_unix_ms),
        kind: TimelineBlockKind::DetachedTaskRun {
            task: task.clone(),
            author,
        },
    };
    upsert_top_level_block(thread, block);
}

fn remove_turn_work_state(thread: &mut ThreadSemanticTimelineState, turn_id: &str) {
    thread.work_ranges_by_turn.remove(turn_id);
    thread
        .top_level
        .blocks_by_id
        .remove(work_block_id(turn_id).as_str());
    thread
        .top_level
        .blocks_by_id
        .remove(terminal_state_block_id(turn_id).as_str());
    thread
        .top_level
        .ordered_block_ids
        .retain(|block_id| thread.top_level.blocks_by_id.contains_key(block_id));
    thread.expansion.clear_turn_work_override(turn_id);
    sort_top_level_blocks(&mut thread.top_level);
}

fn reconcile_task_attachment_marker(
    thread: &mut ThreadSemanticTimelineState,
    turn_id: &str,
    item: &TurnItem,
) {
    if let TurnItem::Task { item } = item
        && item.attachment == TaskAttachmentMode::Attached
    {
        thread.detached_task_run_turn_ids.remove(turn_id);
    }
}

fn thread_has_detached_task_run(thread: &ThreadSemanticTimelineState, turn_id: &str) -> bool {
    thread.detached_task_run_turn_ids.contains(turn_id)
        || thread.top_level.blocks_by_id.values().any(|block| {
            block.turn_id.as_deref() == Some(turn_id)
                && matches!(&block.kind, TimelineBlockKind::DetachedTaskRun { .. })
        })
}

fn turn_is_detached_task_run(turn: &Turn) -> bool {
    turn.turn_kind == TurnKind::TaskRun
        && matches!(
            &turn.origin,
            TurnOrigin::ScheduledTask | TurnOrigin::DetachedTask
        )
}

fn upsert_terminal_state_block(
    thread: &mut ThreadSemanticTimelineState,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    state: TurnWorkState,
    message: Option<String>,
    now_unix_ms: i64,
) {
    let block_id = terminal_state_block_id(turn_id);
    let existing = thread.top_level.blocks_by_id.get(block_id.as_str());
    let (existing_author, route) = existing
        .and_then(|block| match &block.kind {
            TimelineBlockKind::TurnState { author, route, .. } => {
                Some((author.clone(), route.clone()))
            }
            _ => None,
        })
        .unwrap_or_default();
    let author = existing_author;
    let block = TimelineBlock {
        workspace_id: workspace_id.to_owned(),
        thread_id: thread_id.to_owned(),
        block_id,
        turn_id: Some(turn_id.to_owned()),
        sort_key: existing
            .map(|block| block.sort_key.clone())
            .unwrap_or_else(|| {
                turn_block_sort_key(thread, turn_id, 300, "terminal-state", now_unix_ms)
            }),
        started_at_unix_ms: existing
            .and_then(|block| block.started_at_unix_ms)
            .or(Some(now_unix_ms)),
        updated_at_unix_ms: Some(now_unix_ms),
        kind: TimelineBlockKind::TurnState {
            state,
            message,
            author,
            route,
        },
    };
    upsert_top_level_block(thread, block);
}

fn remove_terminal_state_block(thread: &mut ThreadSemanticTimelineState, turn_id: &str) {
    if thread
        .top_level
        .blocks_by_id
        .remove(terminal_state_block_id(turn_id).as_str())
        .is_some()
    {
        sort_top_level_blocks(&mut thread.top_level);
    }
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
    let order_key = work_item_order_key(thread.work_range(turn_id), item_id.as_str());
    let item_type = item.item_type();
    let range = thread.work_range_mut(turn_id.to_owned());
    range.thread_id = thread_id.to_owned();
    range.turn_id = turn_id.to_owned();
    range.stale_work_item_ids.remove(work_item_id.as_str());
    let existing_started_at = range
        .items_by_id
        .get(work_item_id.as_str())
        .and_then(|item| item.started_at_unix_ms);
    let existing_source_sequence = range
        .items_by_id
        .get(work_item_id.as_str())
        .map(|item| item.source_sequence)
        .unwrap_or_default();
    let existing_source_updated_at_unix_micros = range
        .items_by_id
        .get(work_item_id.as_str())
        .map(|item| item.source_updated_at_unix_micros)
        .unwrap_or_default();
    range.items_by_id.insert(
        work_item_id.clone(),
        TurnWorkItem {
            work_item_id,
            item_id,
            turn_id: turn_id.to_owned(),
            order_key,
            source_sequence: existing_source_sequence,
            source_updated_at_unix_micros: existing_source_updated_at_unix_micros
                .max(now_unix_ms.saturating_mul(1_000)),
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
    let preserve_terminal_state = existing.as_ref().is_some_and(|work| {
        is_terminal_turn_work_state(work.state) && !is_terminal_turn_work_state(state)
    });
    let state = if preserve_terminal_state {
        existing.as_ref().map(|work| work.state).unwrap_or(state)
    } else {
        state
    };
    let presentation = if preserve_terminal_state {
        existing
            .as_ref()
            .map(|work| work.presentation)
            .unwrap_or(presentation)
    } else {
        presentation
    };
    let completed_at_unix_ms = completed_at_unix_ms.or_else(|| {
        preserve_terminal_state
            .then(|| existing.as_ref().and_then(|work| work.completed_at_unix_ms))
            .flatten()
    });
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
        agent_work_graph: existing
            .as_ref()
            .and_then(|work| work.agent_work_graph.clone()),
        author: existing.as_ref().and_then(|work| work.author.clone()),
        started_at_unix_ms: existing
            .as_ref()
            .and_then(|work| work.started_at_unix_ms)
            .or(Some(now_unix_ms)),
        completed_at_unix_ms,
        elapsed_ms: existing
            .as_ref()
            .and_then(|work| work.started_at_unix_ms)
            .map(|started_at| {
                completed_at_unix_ms
                    .unwrap_or(now_unix_ms)
                    .saturating_sub(started_at)
                    .max(0) as u64
            }),
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

fn is_terminal_turn_work_state(state: TurnWorkState) -> bool {
    matches!(
        state,
        TurnWorkState::Completed
            | TurnWorkState::Blocked
            | TurnWorkState::Failed
            | TurnWorkState::Interrupted
    )
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
        | SemanticTimelineRequestAction::TurnWorkPage { key, .. }
        | SemanticTimelineRequestAction::TurnWorkItemsGet { key, .. } => key,
    };
    if in_flight.contains(key) || !planned.insert(key.clone()) {
        return;
    }
    actions.push(action);
}

pub fn coalesce_semantic_timeline_request_action(
    existing: &mut SemanticTimelineRequestAction,
    incoming: SemanticTimelineRequestAction,
) {
    if let (
        SemanticTimelineRequestAction::TurnWorkItemsGet {
            params: existing_params,
            ..
        },
        SemanticTimelineRequestAction::TurnWorkItemsGet {
            params: incoming_params,
            ..
        },
    ) = (&mut *existing, &incoming)
    {
        existing_params
            .work_item_ids
            .extend(incoming_params.work_item_ids.iter().cloned());
        existing_params.work_item_ids.sort();
        existing_params.work_item_ids.dedup();
        return;
    }

    *existing = incoming;
}

pub fn semantic_timeline_request_key(
    action: &SemanticTimelineRequestAction,
) -> &SemanticTimelineRequestKey {
    match action {
        SemanticTimelineRequestAction::ThreadTimelinePage { key, .. }
        | SemanticTimelineRequestAction::TurnWorkPage { key, .. }
        | SemanticTimelineRequestAction::TurnWorkItemsGet { key, .. } => key,
    }
}

pub fn enqueue_semantic_timeline_request(
    in_flight: &mut HashSet<SemanticTimelineRequestKey>,
    pending: &mut HashMap<SemanticTimelineRequestKey, SemanticTimelineRequestAction>,
    action: SemanticTimelineRequestAction,
) -> Option<SemanticTimelineRequestAction> {
    let key = semantic_timeline_request_key(&action).clone();
    if in_flight.insert(key.clone()) {
        return Some(action);
    }

    pending
        .entry(key)
        .and_modify(|queued| {
            coalesce_semantic_timeline_request_action(queued, action.clone());
        })
        .or_insert(action);
    None
}

pub fn finish_semantic_timeline_request(
    in_flight: &mut HashSet<SemanticTimelineRequestKey>,
    pending: &mut HashMap<SemanticTimelineRequestKey, SemanticTimelineRequestAction>,
    key: &SemanticTimelineRequestKey,
) -> Option<SemanticTimelineRequestAction> {
    in_flight.remove(key);
    pending.remove(key)
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
            | SemanticTimelineRowKind::DetachedTaskRun { block }
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
        if !work_item_is_visible_in_timeline(item) {
            continue;
        }
        rows.push(SemanticTimelineRow {
            id: SemanticTimelineRowId::TurnWorkItem {
                turn_id: item.turn_id.clone(),
                work_item_id: item.work_item_id.clone(),
            },
            author: work.author.clone(),
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

fn work_item_is_visible_in_timeline(item: &TurnWorkItem) -> bool {
    if item.status == TurnWorkItemStatus::Running {
        return true;
    }

    match &item.item {
        TurnItem::Reasoning {
            summary, content, ..
        } => summary
            .iter()
            .chain(content)
            .any(|part| !part.trim().is_empty()),
        _ => true,
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
    page_can_advance_newest_boundary: bool,
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
            if page_can_advance_newest_boundary {
                range.loaded_range.after_cursor = page.after_cursor.clone();
                range.loaded_range.has_more_after = page.has_more_after;
            }
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
        ArtifactKind, ArtifactRef, ArtifactStatus, PersistedActorRef, PrincipalId,
        SystemEventLevel, TaskAttachmentMode, TaskExecutorKind, TaskStatus, TaskTriggerKind,
        TaskTurnItem, ThreadMode, TimelineBlockKind, TimelineReplySummary, TurnAuthorSnapshot,
        TurnItem, TurnItemType, TurnMention, TurnWorkItemStatus, TurnWorkPresentation,
        TurnWorkState,
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
    fn authoritative_user_message_snapshot_replaces_optimism_and_preserves_exact_refs() {
        let principal_id = PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("valid principal id");
        let author = TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(principal_id.clone()),
            display_name: "Alice".to_owned(),
            nickname: "alice".to_owned(),
            avatar_revision: Some("avatar-r3".to_owned()),
            agent: None,
        };
        let mention = TurnMention {
            principal_id,
            nickname: "alice".to_owned(),
        };
        let artifact = ArtifactRef {
            artifact_id: "artifact_a".to_owned(),
            version_id: Some("version_exact".to_owned()),
            display_name: "report.pdf".to_owned(),
            kind: ArtifactKind::Pdf,
            mime_type: Some("application/pdf".to_owned()),
            size_bytes: Some(42),
            sha256: Some("sha256-exact".to_owned()),
            status: ArtifactStatus::Ready,
            preview: None,
        };
        let authoritative = TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "turn:turn_message:user".to_owned(),
            turn_id: Some("turn_message".to_owned()),
            sort_key: "001".to_owned(),
            started_at_unix_ms: Some(10),
            updated_at_unix_ms: Some(20),
            kind: TimelineBlockKind::UserMessage {
                item_id: Some("item_message".to_owned()),
                inputs: Vec::new(),
                text: "edited body".to_owned(),
                attachments: vec![UserMessageAttachment::Artifact {
                    artifact: artifact.clone(),
                }],
                mode: ThreadMode::Message,
                author: Some(author.clone()),
                route: None,
                reply: Some(TimelineReplySummary {
                    turn_id: "turn_parent".to_owned(),
                    author: Some(author.clone()),
                    text: Some("parent".to_owned()),
                    deleted: false,
                }),
                mentions: vec![mention.clone()],
                revision: 1,
                edited: true,
                deleted: false,
            },
        };

        let mut state = SemanticTimelineState::default();
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_message".to_owned(),
                pending_request_id: "pending_message".to_owned(),
                mode: ThreadMode::Message,
                user_text: "optimistic body".to_owned(),
                attachments: Vec::new(),
            },
            1,
        ));
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![authoritative.clone()]),
            TopLevelPageMergeMode::Reset,
        ));

        let cached = state
            .thread("thread_a")
            .and_then(|thread| thread.top_level.block("turn:turn_message:user"))
            .expect("authoritative user message");
        assert_eq!(cached, &authoritative);
        let TimelineBlockKind::UserMessage { attachments, .. } = &cached.kind else {
            panic!("expected user message block");
        };
        assert_eq!(
            attachments,
            &vec![UserMessageAttachment::Artifact { artifact }],
            "timeline state retains only the exact typed artifact ref"
        );

        let changed = ThreadTimelineBlocksChangedNotification {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            changed_block_ids: vec!["turn:turn_message:user".to_owned()],
            removed_block_ids: Vec::new(),
            before_cursor: None,
            after_cursor: None,
            reason: pioneer_protocol::TimelineChangeReason::LiveEvent,
        };
        assert!(apply_thread_timeline_blocks_changed(
            &mut state,
            changed.clone(),
        ));
        assert!(!apply_thread_timeline_blocks_changed(&mut state, changed));

        let mut tombstone = authoritative;
        tombstone.updated_at_unix_ms = Some(30);
        tombstone.kind = TimelineBlockKind::UserMessage {
            item_id: Some("item_message".to_owned()),
            inputs: Vec::new(),
            text: String::new(),
            attachments: Vec::new(),
            mode: ThreadMode::Message,
            author: Some(author),
            route: None,
            reply: None,
            mentions: vec![mention],
            revision: 2,
            edited: true,
            deleted: true,
        };
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![tombstone.clone()]),
            TopLevelPageMergeMode::Reset,
        ));
        let cached = state
            .thread("thread_a")
            .and_then(|thread| thread.top_level.block("turn:turn_message:user"))
            .expect("authoritative tombstone");
        assert_eq!(cached, &tombstone);
        assert!(
            state
                .thread("thread_a")
                .expect("thread cache")
                .top_level
                .stale_block_ids()
                .is_empty()
        );
    }

    #[test]
    fn stale_detached_task_page_cannot_regress_running_card_to_queued() {
        let mut state = SemanticTimelineState::default();
        let running = detached_task_block(TaskStatus::Running, 20);

        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![running]),
            TopLevelPageMergeMode::Reset,
        ));
        apply_thread_timeline_page(
            &mut state,
            thread_page(vec![detached_task_block(TaskStatus::Queued, 10)]),
            TopLevelPageMergeMode::Reset,
        );

        assert_eq!(detached_task_status(&state), Some(TaskStatus::Running));

        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![detached_task_block(TaskStatus::Queued, 30)]),
            TopLevelPageMergeMode::Merge,
        ));
        assert_eq!(detached_task_status(&state), Some(TaskStatus::Queued));
    }

    #[test]
    fn equal_revision_detached_task_event_prefers_running_over_queued() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![detached_task_block(TaskStatus::Running, 20)]),
            TopLevelPageMergeMode::Reset,
        ));

        upsert_top_level_block(
            state.thread_mut("thread_a"),
            detached_task_block(TaskStatus::Queued, 20),
        );

        assert_eq!(detached_task_status(&state), Some(TaskStatus::Running));
    }

    #[test]
    fn detached_task_card_stays_at_start_position_and_result_uses_delivery_position() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "source_turn".to_owned(),
                pending_request_id: "pending_source".to_owned(),
                mode: ThreadMode::Agent,
                user_text: "Start background analysis".to_owned(),
                attachments: Vec::new(),
            },
            1_000,
        ));
        let task_turn = detached_task_turn("task_turn");
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnStarted {
                thread_id: "thread_a".to_owned(),
                turn: task_turn,
            },
            1_000,
        ));

        let running_task = task_turn_item(TaskAttachmentMode::Detached, TaskStatus::Running);
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemStarted {
                thread_id: "thread_a".to_owned(),
                turn_id: "task_turn".to_owned(),
                item: running_task.clone(),
            },
            1_000,
        ));
        let initial_sort_key = state
            .thread("thread_a")
            .and_then(|thread| {
                thread
                    .top_level
                    .block("turn:task_turn:detached-task-run:task_anchor")
            })
            .map(|block| block.sort_key.clone())
            .expect("detached task card should exist");

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "user_turn".to_owned(),
                pending_request_id: "pending_user".to_owned(),
                mode: ThreadMode::Agent,
                user_text: "A later parent message".to_owned(),
                attachments: Vec::new(),
            },
            2_000,
        ));

        let completed_task = task_turn_item(TaskAttachmentMode::Detached, TaskStatus::Completed);
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemUpdated {
                thread_id: "thread_a".to_owned(),
                turn_id: "task_turn".to_owned(),
                item: completed_task,
            },
            3_000,
        ));
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemCompleted {
                thread_id: "thread_a".to_owned(),
                turn_id: "task_turn".to_owned(),
                item: TurnItem::AgentMessage {
                    id: "task_result".to_owned(),
                    text: "Background analysis is ready".to_owned(),
                    phase: AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            4_000,
        ));

        let thread = state.thread("thread_a").expect("thread state should exist");
        let task_block = thread
            .top_level
            .block("turn:task_turn:detached-task-run:task_anchor")
            .expect("detached task card should remain");
        assert_eq!(task_block.sort_key, initial_sort_key);
        assert!(matches!(
            &task_block.kind,
            TimelineBlockKind::DetachedTaskRun { task, .. }
                if task.status == TaskStatus::Completed
        ));
        assert!(
            thread.top_level.block("turn:task_turn:work").is_none(),
            "detached task work must not appear in the Worked group"
        );

        let ordered_ids = thread
            .top_level
            .ordered_blocks()
            .map(|block| block.block_id.as_str())
            .collect::<Vec<_>>();
        let task_index = ordered_ids
            .iter()
            .position(|id| *id == "turn:task_turn:detached-task-run:task_anchor")
            .unwrap();
        let source_user_index = ordered_ids
            .iter()
            .position(|id| *id == "turn:source_turn:user")
            .unwrap();
        let user_index = ordered_ids
            .iter()
            .position(|id| *id == "turn:user_turn:user")
            .unwrap();
        let result_index = ordered_ids
            .iter()
            .position(|id| *id == "turn:task_turn:assistant:task_result")
            .unwrap();
        assert!(source_user_index < task_index);
        assert!(task_index < user_index);
        assert!(user_index < result_index);
    }

    #[test]
    fn detached_task_running_clock_reanchors_after_queue_wait() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnStarted {
                thread_id: "thread_a".to_owned(),
                turn: detached_task_turn("task_turn"),
            },
            1_000,
        ));

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemStarted {
                thread_id: "thread_a".to_owned(),
                turn_id: "task_turn".to_owned(),
                item: task_turn_item(TaskAttachmentMode::Detached, TaskStatus::Queued),
            },
            1_000,
        ));
        let block_id = "turn:task_turn:detached-task-run:task_anchor";
        assert_eq!(
            state
                .thread("thread_a")
                .and_then(|thread| thread.top_level.block(block_id))
                .and_then(|block| block.started_at_unix_ms),
            Some(1_000),
        );

        let mut running_task = task_turn_item(TaskAttachmentMode::Detached, TaskStatus::Running);
        let TurnItem::Task { item } = &mut running_task else {
            unreachable!("task fixture must stay a task item");
        };
        item.started_at = Some(20);
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemUpdated {
                thread_id: "thread_a".to_owned(),
                turn_id: "task_turn".to_owned(),
                item: running_task,
            },
            20_500,
        ));
        assert_eq!(
            state
                .thread("thread_a")
                .and_then(|thread| thread.top_level.block(block_id))
                .and_then(|block| block.started_at_unix_ms),
            Some(20_000),
            "running elapsed time must exclude the queued interval",
        );
    }

    #[test]
    fn detached_task_diagnostics_do_not_recreate_parent_work_group() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnStarted {
                thread_id: "thread_a".to_owned(),
                turn: detached_task_turn("task_turn"),
            },
            1_000,
        ));
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemStarted {
                thread_id: "thread_a".to_owned(),
                turn_id: "task_turn".to_owned(),
                item: task_turn_item(TaskAttachmentMode::Detached, TaskStatus::Running),
            },
            1_100,
        ));

        apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemCompleted {
                thread_id: "thread_a".to_owned(),
                turn_id: "task_turn".to_owned(),
                item: TurnItem::SystemEvent {
                    id: "delivery_error".to_owned(),
                    level: SystemEventLevel::Error,
                    message: "Task delivery failed".to_owned(),
                    code: Some("task_delivery_failed".to_owned()),
                    details: None,
                },
            },
            2_000,
        );

        let thread = state.thread("thread_a").expect("thread state should exist");
        assert!(
            thread
                .top_level
                .block("turn:task_turn:detached-task-run:task_anchor")
                .is_some(),
            "the detached Task card must remain visible"
        );
        assert!(
            thread.top_level.block("turn:task_turn:work").is_none(),
            "wrapper diagnostics belong to the Task card, not a parent Worked group"
        );
        assert!(thread.work_range("task_turn").is_none());
    }

    #[test]
    fn unloaded_historical_task_update_does_not_create_live_work_group() {
        let mut state = SemanticTimelineState::default();

        assert!(!apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemUpdated {
                thread_id: "thread_a".to_owned(),
                turn_id: "historical_turn".to_owned(),
                item: task_turn_item(TaskAttachmentMode::Attached, TaskStatus::Scheduled),
            },
            4_000_000,
        ));
        assert!(
            state.thread("thread_a").is_none(),
            "a late snapshot outside the loaded window must not enter the live tail"
        );
    }

    #[test]
    fn historical_task_update_preserves_terminal_work_summary_timing() {
        let mut state = SemanticTimelineState::default();
        let original_task = task_turn_item(TaskAttachmentMode::Attached, TaskStatus::Scheduled);
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemCompleted {
                thread_id: "thread_a".to_owned(),
                turn_id: "historical_turn".to_owned(),
                item: original_task,
            },
            1_000,
        ));
        let mut terminal_turn = detached_task_turn("historical_turn");
        terminal_turn.status = TurnStatus::Completed;
        terminal_turn.turn_kind = TurnKind::default();
        terminal_turn.origin = TurnOrigin::default();
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnCompleted {
                thread_id: "thread_a".to_owned(),
                turn: terminal_turn,
            },
            2_000,
        ));

        let before_work = state
            .thread("thread_a")
            .and_then(|thread| thread.cached_turn_work_block("historical_turn"))
            .cloned()
            .expect("terminal work summary should exist");
        assert_eq!(before_work.elapsed_ms, Some(1_000));

        let mut updated_task = task_turn_item(TaskAttachmentMode::Attached, TaskStatus::Scheduled);
        let TurnItem::Task { item } = &mut updated_task else {
            unreachable!("task fixture must stay a task item");
        };
        item.updated_at = 4_000;
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemUpdated {
                thread_id: "thread_a".to_owned(),
                turn_id: "historical_turn".to_owned(),
                item: updated_task,
            },
            4_000_000,
        ));

        let thread = state.thread("thread_a").expect("thread state should exist");
        assert_eq!(
            thread.cached_turn_work_block("historical_turn"),
            Some(&before_work),
            "a task snapshot must not restart, resize, or move terminal work"
        );
        let updated_item = thread
            .work_range("historical_turn")
            .and_then(|range| {
                range
                    .items_by_id
                    .get("turn:historical_turn:work:task_anchor")
            })
            .expect("existing task anchor should update in place");
        assert!(matches!(
            &updated_item.item,
            TurnItem::Task { item } if item.updated_at == 4_000
        ));
        assert_eq!(updated_item.started_at_unix_ms, Some(1_000));
        assert_eq!(updated_item.completed_at_unix_ms, Some(1_000));

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemUpdated {
                thread_id: "thread_a".to_owned(),
                turn_id: "historical_turn".to_owned(),
                item: TurnItem::SystemEvent {
                    id: "late_diagnostic".to_owned(),
                    level: SystemEventLevel::Warning,
                    message: "Context compacted".to_owned(),
                    code: Some("agent_context_compaction".to_owned()),
                    details: None,
                },
            },
            5_000_000,
        ));
        let work = state
            .thread("thread_a")
            .and_then(|thread| thread.cached_turn_work_block("historical_turn"))
            .expect("terminal work summary should remain");
        assert_eq!(work.state, TurnWorkState::Completed);
        assert_eq!(work.presentation, before_work.presentation);
        assert_eq!(work.completed_at_unix_ms, Some(2_000));
        assert_eq!(work.elapsed_ms, Some(1_000));
    }

    #[test]
    fn attached_task_remains_inside_turn_work() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemStarted {
                thread_id: "thread_a".to_owned(),
                turn_id: "conversation_turn".to_owned(),
                item: task_turn_item(TaskAttachmentMode::Attached, TaskStatus::Running),
            },
            1_000,
        ));

        let thread = state.thread("thread_a").expect("thread state should exist");
        assert!(
            thread
                .top_level
                .ordered_blocks()
                .all(|block| !matches!(&block.kind, TimelineBlockKind::DetachedTaskRun { .. }))
        );
        assert!(thread.work_range("conversation_turn").is_some_and(|range| {
            range
                .items_by_id
                .contains_key("turn:conversation_turn:work:task_anchor")
        }));
    }

    #[test]
    fn live_work_items_keep_arrival_order_with_same_or_earlier_client_time() {
        let mut state = SemanticTimelineState::default();

        for (item_id, now_unix_ms) in [
            ("z_first", 1_000),
            ("m_second", 1_000),
            ("a_third", 1_000),
            ("b_fourth_after_clock_rollback", 900),
        ] {
            assert!(apply_conversation_event_to_semantic_timeline(
                &mut state,
                "workspace_a",
                &ConversationEvent::ItemStarted {
                    thread_id: "thread_a".to_owned(),
                    turn_id: "turn_a".to_owned(),
                    item: TurnItem::SystemEvent {
                        id: item_id.to_owned(),
                        level: SystemEventLevel::Warning,
                        message: item_id.to_owned(),
                        code: None,
                        details: None,
                    },
                },
                now_unix_ms,
            ));
        }

        let range = state
            .thread("thread_a")
            .and_then(|thread| thread.work_range("turn_a"))
            .expect("live work range should exist");
        assert_eq!(
            range
                .ordered_items()
                .map(|item| item.item_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "z_first",
                "m_second",
                "a_third",
                "b_fourth_after_clock_rollback",
            ],
            "live child work must follow event arrival order until durable sequence arrives",
        );
        assert!(
            range
                .ordered_items()
                .map(|item| item.order_key.as_str())
                .collect::<Vec<_>>()
                .windows(2)
                .all(|pair| pair[0] < pair[1]),
            "temporary live order keys must be strictly monotonic",
        );
    }

    #[test]
    fn unreconciled_live_work_item_stays_before_later_canonical_items() {
        let mut state = SemanticTimelineState::default();
        let mut prior = work_item("prior", "00000000000000003651:prior");
        prior.source_sequence = 3_651;
        prior.source_updated_at_unix_micros = 3_651;
        let mut initial = work_page(vec![prior]);
        initial.source_high_watermark = 3_651;
        initial.projection_updated_at_unix_micros = 3_651;
        assert!(apply_turn_work_page(
            &mut state,
            initial,
            WorkPageMergeMode::MergeAfter,
        ));

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemStarted {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item: TurnItem::SystemEvent {
                    id: "old_live".to_owned(),
                    level: SystemEventLevel::Warning,
                    message: "old live item".to_owned(),
                    code: None,
                    details: None,
                },
            },
            1_785_154_473_000,
        ));

        let mut newer = work_item("newer", "00000000000000003895:newer");
        newer.source_sequence = 3_895;
        newer.source_updated_at_unix_micros = 3_895;
        assert!(apply_turn_work_items_get_response(
            &mut state,
            work_items_response("thread_a", "turn_a", 3_895, vec![newer]),
        ));

        let range = state
            .thread("thread_a")
            .and_then(|thread| thread.work_range("turn_a"))
            .expect("work range should exist");
        assert_eq!(
            range
                .ordered_items()
                .map(|item| item.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["item_prior", "old_live", "item_newer"],
            "a live item must not jump below later canonical work while reconciliation is pending",
        );

        let mut reconciled_old = range
            .items_by_id
            .get("turn:turn_a:work:old_live")
            .cloned()
            .expect("live work item should be cached");
        reconciled_old.order_key = "00000000000000003652:old_live".to_owned();
        reconciled_old.source_sequence = 3_652;
        reconciled_old.source_updated_at_unix_micros = 3_652;
        assert!(apply_turn_work_items_get_response(
            &mut state,
            work_items_response("thread_a", "turn_a", 3_895, vec![reconciled_old]),
        ));
        assert_eq!(
            state
                .thread("thread_a")
                .and_then(|thread| thread.work_range("turn_a"))
                .expect("work range should exist")
                .ordered_items()
                .map(|item| item.item_id.as_str())
                .collect::<Vec<_>>(),
            vec!["item_prior", "old_live", "item_newer"],
            "canonical reconciliation must preserve the order already shown live",
        );
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
    fn turn_work_reset_preserves_live_running_items() {
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

        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001")]),
            WorkPageMergeMode::Reset
        ));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        let range = thread
            .work_range("turn_a")
            .expect("work range should exist");
        assert!(
            range
                .items_by_id
                .contains_key("turn:turn_a:work:item_comment"),
            "resetting a paged work range must not drop live running commentary"
        );
        assert!(
            range.items_by_id.contains_key("work_a"),
            "server page item should still be merged"
        );
    }

    #[test]
    fn unknown_agent_message_delta_is_work_item_not_final_block() {
        let mut state = SemanticTimelineState::default();

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemDelta {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item_id: "item_comment".to_owned(),
                delta: "thinking".to_owned(),
                stream: Some(ItemDeltaStream::AgentMessage),
                payload: None,
                markdown: None,
                markdown_version: None,
            },
            10,
        ));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        assert!(
            thread
                .top_level
                .ordered_blocks()
                .all(|block| !matches!(block.kind, TimelineBlockKind::AssistantMessage { .. })),
            "agent-message deltas without an existing final block must not create final answers"
        );
        let item = thread
            .work_range("turn_a")
            .and_then(|range| range.items_by_id.get("turn:turn_a:work:item_comment"))
            .expect("unknown agent-message delta should be recovered as turn work");
        assert!(
            matches!(&item.item, TurnItem::AgentMessage { text, phase, .. }
                if text == "thinking" && *phase == AgentMessagePhase::Commentary)
        );
    }

    #[test]
    fn local_turn_start_patch_contains_shared_semantic_blocks() {
        let mut state = SemanticTimelineState::default();
        let patch = apply_local_composer_event_to_semantic_timeline_with_patch(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                pending_request_id: "pending_a".to_owned(),
                mode: ThreadMode::Agent,
                user_text: "hello".to_owned(),
                attachments: Vec::new(),
            },
            ThreadComposerExecutionMode::ForegroundTurn,
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
    fn detached_composer_optimism_projects_user_without_foreground_running_work() {
        let mut state = SemanticTimelineState::default();
        let turn_id = "turn_detached_admission";
        let event = ConversationEvent::LocalTurnStartRequested {
            thread_id: "thread_a".to_owned(),
            turn_id: turn_id.to_owned(),
            pending_request_id: "pending_a".to_owned(),
            mode: ThreadMode::Agent,
            user_text: "run this asynchronously".to_owned(),
            attachments: Vec::new(),
        };

        let patch = apply_local_composer_event_to_semantic_timeline_with_patch(
            &mut state,
            "workspace_a",
            &event,
            ThreadComposerExecutionMode::DetachedTask,
            10,
        );

        assert_eq!(
            patch
                .changed_blocks
                .iter()
                .map(|block| block.block_id.as_str())
                .collect::<Vec<_>>(),
            vec!["turn:turn_detached_admission:user"]
        );
        let thread = state.thread("thread_a").expect("thread cache");
        assert!(
            thread
                .top_level
                .block("turn:turn_detached_admission:user")
                .is_some()
        );
        assert!(
            thread
                .top_level
                .block("turn:turn_detached_admission:work")
                .is_none(),
            "a collaborative parent must wait for the canonical Task card instead of flashing a foreground running row"
        );

        assert!(!apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartAccepted {
                thread_id: "thread_a".to_owned(),
                turn_id: turn_id.to_owned(),
                pending_request_id: "pending_a".to_owned(),
                mode: ThreadMode::Agent,
            },
            20,
        ));
        assert!(!apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnStarted {
                thread_id: "thread_a".to_owned(),
                turn: Turn {
                    id: turn_id.to_owned(),
                    status: TurnStatus::InProgress,
                    turn_kind: TurnKind::Conversation,
                    origin: TurnOrigin::User,
                    mode: ThreadMode::Agent,
                    author: None,
                    reply_to_turn_id: None,
                    mentions: Vec::new(),
                    message_revision: 0,
                    message_deleted: false,
                    error: None,
                    prompt_manifest: None,
                    permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(
                    ),
                },
            },
            30,
        ));
        assert!(
            state
                .thread("thread_a")
                .expect("thread cache")
                .top_level
                .block("turn:turn_detached_admission:work")
                .is_none(),
            "turn/start acknowledgement must not resurrect the suppressed foreground work row"
        );
    }

    #[test]
    fn message_only_turn_never_creates_an_optimistic_work_block() {
        let mut state = SemanticTimelineState::default();
        apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_message_only".to_owned(),
                pending_request_id: "pending_message_only".to_owned(),
                mode: ThreadMode::Message,
                user_text: "run this in the background".to_owned(),
                attachments: Vec::new(),
            },
            10,
        );
        assert!(
            state
                .thread("thread_a")
                .expect("thread cache")
                .top_level
                .block("turn:turn_message_only:work")
                .is_none()
        );

        let patch = apply_conversation_event_to_semantic_timeline_with_patch(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnCompleted {
                thread_id: "thread_a".to_owned(),
                turn: Turn {
                    id: "turn_message_only".to_owned(),
                    status: TurnStatus::Completed,
                    turn_kind: TurnKind::Conversation,
                    origin: TurnOrigin::User,
                    mode: ThreadMode::Message,
                    author: None,
                    reply_to_turn_id: None,
                    mentions: Vec::new(),
                    message_revision: 0,
                    message_deleted: false,
                    error: None,
                    prompt_manifest: None,
                    permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(
                    ),
                },
            },
            20,
        );

        assert!(patch.removed_block_ids.is_empty());
        let thread = state.thread("thread_a").expect("thread cache");
        assert!(
            thread
                .top_level
                .block("turn:turn_message_only:user")
                .is_some(),
            "the durable user message must remain visible"
        );
        assert!(
            thread
                .top_level
                .block("turn:turn_message_only:work")
                .is_none(),
            "a successful message-only turn must not render an empty Worked group"
        );
    }

    #[test]
    fn durable_pack_snapshot_replaces_optimistic_user_block_without_live_lookup() {
        let mut state = SemanticTimelineState::default();
        let pack_id = pioneer_protocol::SkillPackId::new("P".repeat(21)).expect("pack id");
        let attachments = vec![
            pioneer_protocol::UserMessageAttachment::SkillPack {
                capability: pioneer_protocol::TurnSkillPackCapabilitySummary {
                    pack_id: pack_id.clone(),
                    label: "Research Pack".to_owned(),
                },
            },
            pioneer_protocol::UserMessageAttachment::Skill {
                capability: pioneer_protocol::TurnSkillCapabilitySummary {
                    skill_id: pioneer_protocol::SkillId::new("S".repeat(21)).expect("skill id"),
                    label: "Search".to_owned(),
                    owner: None,
                    slug: "search".to_owned(),
                    source_kind: "user".to_owned(),
                    pack: Some(pioneer_protocol::TurnSkillPackPresentationSummary {
                        pack_id,
                        label: "Research Pack".to_owned(),
                    }),
                },
            },
        ];
        apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                pending_request_id: "pending_a".to_owned(),
                mode: ThreadMode::Agent,
                user_text: "research".to_owned(),
                attachments: attachments.clone(),
            },
            10,
        );
        apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::ItemStarted {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                item: TurnItem::UserMessage {
                    id: "user_turn_a".to_owned(),
                    text: "research".to_owned(),
                    attachments: attachments.clone(),
                },
            },
            11,
        );

        let thread = state.thread("thread_a").expect("thread cache");
        let user_blocks = thread
            .top_level
            .ordered_blocks()
            .filter_map(|block| match &block.kind {
                TimelineBlockKind::UserMessage { attachments, .. } => Some(attachments),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(user_blocks, vec![&attachments]);
    }

    #[test]
    fn live_timeline_update_patch_removes_stale_top_level_blocks() {
        let mut state = SemanticTimelineState::default();
        apply_conversation_event_to_semantic_timeline_with_patch(
            &mut state,
            "workspace_a",
            &ConversationEvent::LocalTurnStartRequested {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                pending_request_id: "pending_a".to_owned(),
                mode: ThreadMode::Agent,
                user_text: "hello".to_owned(),
                attachments: Vec::new(),
            },
            10,
        );

        let patch = apply_semantic_timeline_live_update_with_patch(
            &mut state,
            SemanticTimelineLiveUpdate::ThreadTimelineBlocksChanged(
                ThreadTimelineBlocksChangedNotification {
                    workspace_id: "workspace_a".to_owned(),
                    thread_id: "thread_a".to_owned(),
                    changed_block_ids: Vec::new(),
                    removed_block_ids: vec!["turn:turn_a:work".to_owned()],
                    before_cursor: None,
                    after_cursor: None,
                    reason: pioneer_protocol::TimelineChangeReason::LiveEvent,
                },
            ),
        );

        assert_eq!(patch.workspace_id, "workspace_a");
        assert_eq!(patch.thread_id, "thread_a");
        assert_eq!(patch.removed_block_ids, vec!["turn:turn_a:work"]);
        assert!(
            state
                .thread("thread_a")
                .and_then(|thread| thread.top_level.blocks_by_id.get("turn:turn_a:work"))
                .is_none()
        );
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
    fn newest_work_batches_accumulate_without_replacing_stable_rows() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![turn_work_block("thread_a", "block_work", "001")]),
            TopLevelPageMergeMode::Reset
        ));

        let first_batch = (0..50)
            .map(|index| work_item(&format!("work_{index:03}"), &format!("{index:03}")))
            .collect();
        let mut first_page = work_page(first_batch);
        first_page.page.before_cursor = Some(TimelineCursor {
            value: "oldest-loaded".to_owned(),
        });
        first_page.page.after_cursor = Some(TimelineCursor {
            value: "first-newest".to_owned(),
        });
        first_page.page.has_more_before = true;
        assert!(apply_turn_work_page(
            &mut state,
            first_page,
            WorkPageMergeMode::MergeAfter
        ));
        let first_row_ids = flatten_semantic_timeline(&state, "thread_a")
            .expect("flattened rows should exist")
            .rows
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();

        let second_batch = (50..100)
            .map(|index| work_item(&format!("work_{index:03}"), &format!("{index:03}")))
            .collect();
        let mut second_page = work_page(second_batch);
        second_page.page.before_cursor = Some(TimelineCursor {
            value: "newest-window-start".to_owned(),
        });
        second_page.page.after_cursor = Some(TimelineCursor {
            value: "latest-newest".to_owned(),
        });
        assert!(apply_turn_work_page(
            &mut state,
            second_page,
            WorkPageMergeMode::MergeAfter
        ));

        let thread = state.thread("thread_a").expect("thread cache should exist");
        let range = thread
            .work_range("turn_a")
            .expect("work range should exist");
        assert_eq!(range.ordered_item_ids.len(), 100);
        assert_eq!(
            range.ordered_item_ids.first().map(String::as_str),
            Some("work_000")
        );
        assert_eq!(
            range.ordered_item_ids.last().map(String::as_str),
            Some("work_099")
        );
        assert_eq!(
            range
                .loaded_range
                .before_cursor
                .as_ref()
                .map(|cursor| cursor.value.as_str()),
            Some("oldest-loaded")
        );
        assert_eq!(
            range
                .loaded_range
                .after_cursor
                .as_ref()
                .map(|cursor| cursor.value.as_str()),
            Some("latest-newest")
        );

        let all_row_ids = flatten_semantic_timeline(&state, "thread_a")
            .expect("flattened rows should exist")
            .rows
            .into_iter()
            .map(|row| row.id)
            .collect::<Vec<_>>();
        assert!(all_row_ids.starts_with(first_row_ids.as_slice()));
    }

    #[test]
    fn late_running_page_cannot_regress_terminal_work_item() {
        let mut state = SemanticTimelineState::default();
        let mut completed = work_item("work_a", "001");
        completed.status = TurnWorkItemStatus::Completed;
        completed.source_sequence = 12;
        completed.source_updated_at_unix_micros = 12;
        let mut completed_page = work_page(vec![completed.clone()]);
        completed_page.source_high_watermark = 12;
        completed_page.projection_updated_at_unix_micros = 12;
        assert!(apply_turn_work_page(
            &mut state,
            completed_page,
            WorkPageMergeMode::MergeAfter
        ));

        let mut stale_running = completed;
        stale_running.status = TurnWorkItemStatus::Running;
        stale_running.source_sequence = 11;
        stale_running.source_updated_at_unix_micros = 11;
        let mut stale_page = work_page(vec![stale_running]);
        stale_page.source_high_watermark = 11;
        stale_page.projection_updated_at_unix_micros = 11;
        assert!(!apply_turn_work_page(
            &mut state,
            stale_page,
            WorkPageMergeMode::MergeAfter
        ));

        let item = state
            .thread("thread_a")
            .and_then(|thread| thread.work_range("turn_a"))
            .and_then(|range| range.item("work_a"))
            .expect("work item should remain cached");
        assert_eq!(item.status, TurnWorkItemStatus::Completed);
        assert_eq!(item.source_sequence, 12);
    }

    #[test]
    fn canonical_terminal_item_closes_live_running_item_despite_legacy_revision() {
        let mut state = SemanticTimelineState::default();
        let mut running = work_item("work_a", "001");
        running.status = TurnWorkItemStatus::Running;
        running.source_sequence = 20;
        running.source_updated_at_unix_micros = 20_000;
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![running.clone()]),
            WorkPageMergeMode::MergeAfter
        ));

        let mut completed = running;
        completed.status = TurnWorkItemStatus::Completed;
        completed.source_sequence = 8;
        completed.source_updated_at_unix_micros = 8_000;
        assert!(apply_turn_work_items_get_response(
            &mut state,
            work_items_response("thread_a", "turn_a", 20, vec![completed]),
        ));

        let item = state
            .thread("thread_a")
            .and_then(|thread| thread.work_range("turn_a"))
            .and_then(|range| range.item("work_a"))
            .expect("work item should remain cached");
        assert_eq!(item.status, TurnWorkItemStatus::Completed);
    }

    #[test]
    fn terminal_turn_reconciles_cached_running_items_after_notification_gap() {
        let mut state = SemanticTimelineState::default();
        let mut running = work_item("work_a", "001");
        running.status = TurnWorkItemStatus::Running;
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![running]),
            WorkPageMergeMode::MergeAfter
        ));

        let mut turn = detached_task_turn("turn_a");
        turn.status = TurnStatus::Completed;
        turn.turn_kind = TurnKind::Conversation;
        turn.origin = TurnOrigin::User;
        let reconciliation = terminal_turn_work_reconciliation(
            &state,
            &ConversationEvent::TurnCompleted {
                thread_id: "thread_a".to_owned(),
                turn,
            },
        )
        .expect("a terminal Turn must reconcile cached running items");

        assert_eq!(reconciliation.thread_id, "thread_a");
        assert_eq!(reconciliation.turn_id, "turn_a");
        assert_eq!(reconciliation.running_work_item_ids, vec!["work_a"]);
    }

    #[test]
    fn terminal_work_item_never_regresses_to_running_even_with_newer_transport_revision() {
        let mut state = SemanticTimelineState::default();
        let mut completed = work_item("work_a", "001");
        completed.status = TurnWorkItemStatus::Completed;
        completed.source_sequence = 12;
        completed.source_updated_at_unix_micros = 12;
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![completed.clone()]),
            WorkPageMergeMode::MergeAfter
        ));

        let mut running = completed;
        running.status = TurnWorkItemStatus::Running;
        running.source_sequence = 13;
        running.source_updated_at_unix_micros = 13;
        let mut response = work_items_response("thread_a", "turn_a", 13, vec![running]);
        response.projection_updated_at_unix_micros = 13;
        assert!(apply_turn_work_items_get_response(&mut state, response));

        let item = state
            .thread("thread_a")
            .and_then(|thread| thread.work_range("turn_a"))
            .and_then(|range| range.item("work_a"))
            .expect("work item should remain cached");
        assert_eq!(item.status, TurnWorkItemStatus::Completed);
        assert_eq!(item.source_sequence, 12);
    }

    #[test]
    fn targeted_reconciliation_updates_item_outside_newest_window_without_replacing_range() {
        let mut state = SemanticTimelineState::default();
        let items = (0..100)
            .map(|index| work_item(&format!("work_{index:03}"), &format!("{index:03}")))
            .collect();
        assert!(apply_turn_work_page(
            &mut state,
            work_page(items),
            WorkPageMergeMode::MergeAfter
        ));

        let mut updated = work_item("work_000", "000");
        updated.status = TurnWorkItemStatus::Failed;
        updated.source_sequence = 20;
        updated.source_updated_at_unix_micros = 20;
        assert!(apply_turn_work_items_get_response(
            &mut state,
            work_items_response("thread_a", "turn_a", 20, vec![updated])
        ));

        let range = state
            .thread("thread_a")
            .and_then(|thread| thread.work_range("turn_a"))
            .expect("work range should exist");
        assert_eq!(range.ordered_item_ids.len(), 100);
        assert_eq!(
            range.item("work_000").map(|item| item.status),
            Some(TurnWorkItemStatus::Failed)
        );
        assert_eq!(
            range.ordered_item_ids.last().map(String::as_str),
            Some("work_099")
        );
    }

    #[test]
    fn targeted_reconciliation_is_scoped_to_its_thread() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_turn_work_page(
            &mut state,
            work_page(vec![work_item("work_a", "001")]),
            WorkPageMergeMode::MergeAfter
        ));

        let mut thread_b_item = work_item("work_b", "001");
        thread_b_item.turn_id = "turn_b".to_owned();
        let response = work_items_response("thread_b", "turn_b", 2, vec![thread_b_item]);
        assert!(apply_turn_work_items_get_response(&mut state, response));

        assert!(
            state
                .thread("thread_a")
                .and_then(|thread| thread.work_range("turn_a"))
                .is_some_and(|range| range.item("work_a").is_some())
        );
        assert!(
            state
                .thread("thread_b")
                .and_then(|thread| thread.work_range("turn_b"))
                .is_some_and(|range| range.item("work_b").is_some())
        );
    }

    #[test]
    fn repeated_targeted_requests_coalesce_changed_ids() {
        let key = SemanticTimelineRequestKey::TurnWorkItems {
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
        };
        let mut pending = SemanticTimelineRequestAction::TurnWorkItemsGet {
            key: key.clone(),
            params: TurnWorkItemsGetParams {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                work_item_ids: vec!["work_a".to_owned()],
            },
        };
        coalesce_semantic_timeline_request_action(
            &mut pending,
            SemanticTimelineRequestAction::TurnWorkItemsGet {
                key,
                params: TurnWorkItemsGetParams {
                    thread_id: "thread_a".to_owned(),
                    turn_id: "turn_a".to_owned(),
                    work_item_ids: vec!["work_b".to_owned(), "work_a".to_owned()],
                },
            },
        );

        let SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } = pending else {
            panic!("coalesced action should remain a targeted work request");
        };
        assert_eq!(params.work_item_ids, vec!["work_a", "work_b"]);
    }

    #[test]
    fn invalidation_during_in_flight_request_schedules_trailing_reconciliation() {
        let key = SemanticTimelineRequestKey::TurnWorkItems {
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
        };
        let action = |work_item_id: &str| SemanticTimelineRequestAction::TurnWorkItemsGet {
            key: key.clone(),
            params: TurnWorkItemsGetParams {
                thread_id: "thread_a".to_owned(),
                turn_id: "turn_a".to_owned(),
                work_item_ids: vec![work_item_id.to_owned()],
            },
        };
        let mut in_flight = HashSet::new();
        let mut pending = HashMap::new();

        assert!(
            enqueue_semantic_timeline_request(&mut in_flight, &mut pending, action("work_a"))
                .is_some()
        );
        assert!(
            enqueue_semantic_timeline_request(&mut in_flight, &mut pending, action("work_b"))
                .is_none()
        );

        let trailing = finish_semantic_timeline_request(&mut in_flight, &mut pending, &key)
            .expect("in-flight invalidation should leave a trailing request");
        let SemanticTimelineRequestAction::TurnWorkItemsGet { params, .. } = trailing else {
            panic!("trailing request should preserve its action kind");
        };
        assert_eq!(params.work_item_ids, vec!["work_b"]);
        assert!(!in_flight.contains(&key));
        assert!(pending.is_empty());
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
                source_high_watermark: 2,
                projection_updated_at_unix_micros: 2,
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
    fn terminal_without_final_collapses_work_and_adds_outcome_block() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![
                user_block("thread_a", "block_user", "001", "turn_a"),
                turn_work_block("thread_a", "block_work", "002"),
            ]),
            TopLevelPageMergeMode::Reset
        ));

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnFailed {
                thread_id: "thread_a".to_owned(),
                turn: Turn {
                    id: "turn_a".to_owned(),
                    status: TurnStatus::Failed,
                    turn_kind: pioneer_protocol::TurnKind::Conversation,
                    origin: pioneer_protocol::TurnOrigin::User,
                    mode: Default::default(),
                    author: None,
                    reply_to_turn_id: None,
                    mentions: Vec::new(),
                    message_revision: 0,
                    message_deleted: false,
                    error: Some("provider disconnected".to_owned()),
                    prompt_manifest: None,
                    permission_profile: pioneer_protocol::compile_turn_permission_profile(
                        pioneer_protocol::TurnPermissionMode::FullAccess,
                        pioneer_protocol::TurnPermissionProfileSource::Composer,
                    ),
                },
            },
            10,
        ));

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        let work_row = flattened
            .rows
            .iter()
            .find(|row| matches!(row.kind, SemanticTimelineRowKind::WorkHeader { .. }))
            .expect("terminal work header should remain visible");
        assert!(matches!(
            &work_row.kind,
            SemanticTimelineRowKind::WorkHeader {
                expanded: false,
                work,
                ..
            } if work.presentation == TurnWorkPresentation::CollapsedAfterFinal
                && work.state == TurnWorkState::Failed
        ));
        let terminal_row = flattened
            .rows
            .iter()
            .find(|row| matches!(row.kind, SemanticTimelineRowKind::TurnState { .. }))
            .expect("terminal outcome block should be visible");
        assert!(matches!(
            &terminal_row.kind,
            SemanticTimelineRowKind::TurnState { block }
                if matches!(
                    &block.kind,
                    TimelineBlockKind::TurnState {
                        state: TurnWorkState::Failed,
                        message: Some(message),
                        ..
                    } if message == "provider disconnected"
                )
        ));
        assert!(flattened.request_hints.is_empty());
    }

    #[test]
    fn terminal_with_final_keeps_answer_without_outcome_block() {
        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![
                user_block("thread_a", "block_user", "001", "turn_a"),
                turn_work_block("thread_a", "block_work", "002"),
                assistant_block("thread_a", "block_assistant", "003", "turn_a", None),
            ]),
            TopLevelPageMergeMode::Reset
        ));

        assert!(apply_conversation_event_to_semantic_timeline(
            &mut state,
            "workspace_a",
            &ConversationEvent::TurnFailed {
                thread_id: "thread_a".to_owned(),
                turn: Turn {
                    id: "turn_a".to_owned(),
                    status: TurnStatus::Failed,
                    turn_kind: pioneer_protocol::TurnKind::Conversation,
                    origin: pioneer_protocol::TurnOrigin::User,
                    mode: Default::default(),
                    author: None,
                    reply_to_turn_id: None,
                    mentions: Vec::new(),
                    message_revision: 0,
                    message_deleted: false,
                    error: Some("failed after final".to_owned()),
                    prompt_manifest: None,
                    permission_profile: pioneer_protocol::compile_turn_permission_profile(
                        pioneer_protocol::TurnPermissionMode::FullAccess,
                        pioneer_protocol::TurnPermissionProfileSource::Composer,
                    ),
                },
            },
            10,
        ));

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        assert!(
            flattened
                .rows
                .iter()
                .any(|row| matches!(row.kind, SemanticTimelineRowKind::AssistantMessage { .. }))
        );
        assert!(
            !flattened
                .rows
                .iter()
                .any(|row| matches!(row.kind, SemanticTimelineRowKind::TurnState { .. }))
        );
        assert!(flattened.rows.iter().any(|row| matches!(
            &row.kind,
            SemanticTimelineRowKind::WorkHeader { work, expanded, .. }
                if !expanded
                    && work.presentation == TurnWorkPresentation::CollapsedAfterFinal
                    && work.state == TurnWorkState::Failed
        )));
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
    fn empty_terminal_reasoning_is_hidden_but_running_and_contentful_reasoning_remain() {
        let mut top_work = turn_work_block("thread_a", "block_work", "002");
        if let TimelineBlockKind::TurnWork { work } = &mut top_work.kind {
            work.work_count = 3;
            work.visible_work_count = 3;
        }
        let mut page_work = work_block("turn_a");
        page_work.work_count = 3;
        page_work.visible_work_count = 3;

        let mut state = SemanticTimelineState::default();
        assert!(apply_thread_timeline_page(
            &mut state,
            thread_page(vec![top_work]),
            TopLevelPageMergeMode::Reset
        ));
        assert!(apply_turn_work_page(
            &mut state,
            work_page_with_work(
                page_work,
                vec![
                    reasoning_work_item(
                        "work_completed_empty",
                        "001",
                        TurnWorkItemStatus::Completed,
                        vec!["  "],
                        Vec::new(),
                    ),
                    reasoning_work_item(
                        "work_running_empty",
                        "002",
                        TurnWorkItemStatus::Running,
                        Vec::new(),
                        Vec::new(),
                    ),
                    reasoning_work_item(
                        "work_completed_content",
                        "003",
                        TurnWorkItemStatus::Completed,
                        Vec::new(),
                        vec!["analysis"],
                    ),
                ],
            ),
            WorkPageMergeMode::Reset
        ));

        let flattened =
            flatten_semantic_timeline(&state, "thread_a").expect("flattened rows should exist");
        let visible_work_item_ids = flattened
            .rows
            .iter()
            .filter_map(|row| match &row.kind {
                SemanticTimelineRowKind::WorkItem { item } => Some(item.work_item_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            visible_work_item_ids,
            vec!["work_running_empty", "work_completed_content"]
        );
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
                author: None,
                route: None,
            },
        }
    }

    fn detached_task_block(status: TaskStatus, updated_at: i64) -> TimelineBlock {
        let TurnItem::Task { mut item } = task_turn_item(TaskAttachmentMode::Detached, status)
        else {
            unreachable!("task fixture must stay a task item");
        };
        item.updated_at = updated_at;
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "turn:task_turn:detached-task-run:task_anchor".to_owned(),
            turn_id: Some("task_turn".to_owned()),
            sort_key: "002".to_owned(),
            started_at_unix_ms: item.started_at.map(|value| value.saturating_mul(1_000)),
            updated_at_unix_ms: Some(updated_at.saturating_mul(1_000)),
            kind: TimelineBlockKind::DetachedTaskRun {
                task: item,
                author: None,
            },
        }
    }

    fn detached_task_status(state: &SemanticTimelineState) -> Option<TaskStatus> {
        state
            .thread("thread_a")
            .and_then(|thread| {
                thread
                    .top_level
                    .block("turn:task_turn:detached-task-run:task_anchor")
            })
            .and_then(|block| match &block.kind {
                TimelineBlockKind::DetachedTaskRun { task, .. } => Some(task.status),
                _ => None,
            })
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
                mode: Default::default(),
                author: None,
                route: None,
                reply: None,
                mentions: Vec::new(),
                revision: 0,
                edited: false,
                deleted: false,
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
                author: None,
                route: None,
            },
        }
    }

    fn detached_task_turn(turn_id: &str) -> Turn {
        Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::TaskRun,
            origin: TurnOrigin::DetachedTask,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::compile_turn_permission_profile(
                pioneer_protocol::TurnPermissionMode::FullAccess,
                pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
            ),
        }
    }

    fn task_turn_item(attachment: TaskAttachmentMode, status: TaskStatus) -> TurnItem {
        TurnItem::Task {
            item: TaskTurnItem {
                id: "task_anchor".to_owned(),
                task_id: "task_a".to_owned(),
                created_by_turn_id: Some("source_turn".to_owned()),
                run_id: Some("run_a".to_owned()),
                parent_task_id: None,
                root_task_id: None,
                title: "Background analysis".to_owned(),
                status,
                attachment,
                trigger_kind: TaskTriggerKind::Immediate,
                executor_kind: TaskExecutorKind::Agent,
                child_thread_id: Some("child_a".to_owned()),
                child_turn_id: Some("child_turn_a".to_owned()),
                agent_role: None,
                depth: 0,
                max_depth: 3,
                next_fire_at: None,
                progress_preview: Some("Collecting sources".to_owned()),
                result_preview: None,
                error_preview: None,
                started_at: (!matches!(
                    status,
                    TaskStatus::Draft | TaskStatus::Scheduled | TaskStatus::Queued
                ))
                .then_some(2),
                created_at: 1,
                updated_at: 2,
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
            source_high_watermark: 1,
            projection_updated_at_unix_micros: 1,
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

    fn work_items_response(
        thread_id: &str,
        turn_id: &str,
        source_high_watermark: i64,
        items: Vec<TurnWorkItem>,
    ) -> TurnWorkItemsGetResponse {
        TurnWorkItemsGetResponse {
            workspace_id: "workspace_a".to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            projection_version: 1,
            source_high_watermark,
            projection_updated_at_unix_micros: source_high_watermark,
            items,
            removed_work_item_ids: Vec::new(),
        }
    }

    fn work_block(turn_id: &str) -> TurnWorkBlock {
        TurnWorkBlock {
            turn_id: turn_id.to_owned(),
            presentation: TurnWorkPresentation::ExpandedLive,
            state: TurnWorkState::Running,
            agent_work_graph: None,
            author: None,
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
            source_sequence: 1,
            source_updated_at_unix_micros: 1,
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

    fn reasoning_work_item(
        work_item_id: &str,
        order_key: &str,
        status: TurnWorkItemStatus,
        summary: Vec<&str>,
        content: Vec<&str>,
    ) -> TurnWorkItem {
        TurnWorkItem {
            work_item_id: work_item_id.to_owned(),
            item_id: format!("item_{work_item_id}"),
            turn_id: "turn_a".to_owned(),
            order_key: order_key.to_owned(),
            source_sequence: 1,
            source_updated_at_unix_micros: 1,
            item_type: TurnItemType::Reasoning,
            status,
            started_at_unix_ms: Some(1),
            completed_at_unix_ms: (status != TurnWorkItemStatus::Running).then_some(2),
            item: TurnItem::Reasoning {
                id: format!("item_{work_item_id}"),
                summary: summary.into_iter().map(str::to_owned).collect(),
                content: content.into_iter().map(str::to_owned).collect(),
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
            source_sequence: 1,
            source_updated_at_unix_micros: 1,
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
