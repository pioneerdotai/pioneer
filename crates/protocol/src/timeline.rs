use crate::{
    CLIRuntimePendingRequestStatus, CLIRuntimeRequestKind, MarkdownDocument, TurnItem,
    TurnItemType, UserInput, UserMessageAttachment,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TimelineCursor {
    pub value: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimelinePageAnchor {
    #[default]
    Newest,
    Oldest,
    Before {
        cursor: TimelineCursor,
    },
    After {
        cursor: TimelineCursor,
    },
    Around {
        cursor: TimelineCursor,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TimelinePageInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_cursor: Option<TimelineCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_cursor: Option<TimelineCursor>,
    pub has_more_before: bool,
    pub has_more_after: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnWorkPresentation {
    ExpandedLive,
    CollapsedAfterFinal,
    ExpandedTerminalNoFinal,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnWorkState {
    Starting,
    Running,
    WaitingForApproval,
    Stalled,
    Completed,
    Blocked,
    Failed,
    Interrupted,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnWorkBlock {
    pub turn_id: String,
    pub presentation: TurnWorkPresentation,
    pub state: TurnWorkState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
    pub work_count: u64,
    pub visible_work_count: u64,
    pub hidden_work_count: u64,
    pub has_more_before: bool,
    pub has_more_after: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_cursor: Option<TimelineCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_cursor: Option<TimelineCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_work_item_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_work_item_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimelineBlockKind {
    #[serde(rename_all = "camelCase")]
    UserMessage {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        item_id: Option<String>,
        #[serde(default)]
        inputs: Vec<UserInput>,
        #[serde(default)]
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        attachments: Vec<UserMessageAttachment>,
    },
    #[serde(rename_all = "camelCase")]
    TurnWork { work: TurnWorkBlock },
    #[serde(rename_all = "camelCase")]
    AssistantMessage {
        item_id: String,
        text: String,
        #[serde(default = "completed_turn_work_item_status")]
        status: TurnWorkItemStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        markdown: Option<MarkdownDocument>,
    },
    #[serde(rename_all = "camelCase")]
    TurnState {
        state: TurnWorkState,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    PendingRequest {
        request_id: String,
        request_kind: CLIRuntimeRequestKind,
        status: CLIRuntimePendingRequestStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TimelineBlock {
    pub workspace_id: String,
    pub thread_id: String,
    pub block_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub sort_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at_unix_ms: Option<i64>,
    pub kind: TimelineBlockKind,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTimelinePageParams {
    pub thread_id: String,
    #[serde(default)]
    pub anchor: TimelinePageAnchor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTimelinePageResponse {
    pub workspace_id: String,
    pub thread_id: String,
    pub projection_version: i64,
    #[serde(default)]
    pub blocks: Vec<TimelineBlock>,
    pub page: TimelinePageInfo,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TurnWorkItemStatus {
    Running,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}

fn completed_turn_work_item_status() -> TurnWorkItemStatus {
    TurnWorkItemStatus::Completed
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TurnWorkItem {
    pub work_item_id: String,
    pub item_id: String,
    pub turn_id: String,
    pub order_key: String,
    pub item_type: TurnItemType,
    pub status: TurnWorkItemStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    pub item: TurnItem,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnWorkPageParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub anchor: TimelinePageAnchor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TurnWorkPageResponse {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub projection_version: i64,
    pub work: TurnWorkBlock,
    #[serde(default)]
    pub items: Vec<TurnWorkItem>,
    pub page: TimelinePageInfo,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TimelineChangeReason {
    Backfill,
    LiveEvent,
    StateChanged,
    PageInvalidated,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTimelineBlocksChangedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_block_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_block_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_cursor: Option<TimelineCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_cursor: Option<TimelineCursor>,
    pub reason: TimelineChangeReason,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnWorkItemsChangedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_work_item_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_work_item_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_cursor: Option<TimelineCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_cursor: Option<TimelineCursor>,
    pub reason: TimelineChangeReason,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurnWorkStateChangedNotification {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub work: TurnWorkBlock,
    pub reason: TimelineChangeReason,
}
