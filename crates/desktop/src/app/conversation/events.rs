use pioneer_protocol::{
    ItemDeltaStream, MarkdownDocument, ToolLoopBudgetAction, ToolLoopBudgetLimitKind,
    ToolRetryBudgetUsage, ToolRetryErrorClass, ToolRetryExhaustionKind, ToolRetryResolution, Turn,
    TurnItem, TurnItemTimeoutReason, TurnItemType, TurnStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventKind {
    LocalTurnStartRequested,
    LocalTurnStartAccepted,
    LocalTurnStartRejected,
    LocalTurnCancelRequested,
    LocalTurnCancelRejected,
    TurnStarted,
    TurnCompleted,
    TurnFailed,
    TurnCancelled,
    ItemStarted,
    ItemDelta,
    ItemTimeoutDetected,
    ItemRecoveryOpened,
    ItemRecoveryAttached,
    ItemRetryScheduled,
    ItemRetryAttemptStarted,
    ItemRecoverySucceeded,
    ItemRecoveryExhausted,
    ItemToolRetryScheduled,
    ItemToolRetryResolved,
    ItemToolRetryExhausted,
    TurnToolLoopBudgetExceeded,
    ItemCompleted,
    ItemUpdated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ConversationEvent {
    LocalTurnStartRequested {
        thread_id: String,
        turn_id: String,
        pending_request_id: String,
        user_text: String,
    },
    LocalTurnStartAccepted {
        thread_id: String,
        turn_id: String,
        pending_request_id: String,
    },
    LocalTurnStartRejected {
        thread_id: String,
        turn_id: String,
        pending_request_id: String,
        error: String,
    },
    LocalTurnCancelRequested {
        thread_id: String,
        turn_id: String,
    },
    LocalTurnCancelRejected {
        thread_id: String,
        turn_id: String,
        error: String,
    },
    TurnStarted {
        thread_id: String,
        turn: Turn,
    },
    TurnCompleted {
        thread_id: String,
        turn: Turn,
    },
    TurnFailed {
        thread_id: String,
        turn: Turn,
    },
    ItemStarted {
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    ItemDelta {
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
        stream: Option<ItemDeltaStream>,
        payload: Option<JsonValue>,
        markdown: Option<MarkdownDocument>,
        markdown_version: Option<u16>,
    },
    ItemTimeoutDetected {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        attempt_number: u32,
        reason: TurnItemTimeoutReason,
        recovery_job_id: Option<String>,
    },
    ItemRecoveryOpened {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    ItemRecoveryAttached {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        recovery_item_id: String,
        recovery_item_type: TurnItemType,
        existing_status: pioneer_protocol::RecoveryJobStatus,
        next_attempt_number: u32,
    },
    ItemRetryScheduled {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        next_run_at_unix: i64,
        reason: Option<String>,
    },
    ItemRetryAttemptStarted {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    ItemRecoverySucceeded {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
    },
    ItemRecoveryExhausted {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_job_id: String,
        attempt_number: u32,
        status: pioneer_protocol::RecoveryJobStatus,
        error_message: String,
    },
    ItemToolRetryScheduled {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        retry_hint: String,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    ItemToolRetryResolved {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        resolution: ToolRetryResolution,
        budgets: Vec<ToolRetryBudgetUsage>,
        reason: String,
    },
    ItemToolRetryExhausted {
        thread_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        tool_retry_episode_id: String,
        tool_name: String,
        attempt_number: u32,
        error_class: ToolRetryErrorClass,
        exhaustion_kind: ToolRetryExhaustionKind,
        budgets: Vec<ToolRetryBudgetUsage>,
        failure_signature_fingerprint: String,
        reason: String,
    },
    TurnToolLoopBudgetExceeded {
        thread_id: String,
        turn_id: String,
        limit_kind: ToolLoopBudgetLimitKind,
        limit: u32,
        observed: u32,
        action: ToolLoopBudgetAction,
        reason: String,
    },
    ItemCompleted {
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
    ItemUpdated {
        thread_id: String,
        turn_id: String,
        item: TurnItem,
    },
}

impl ConversationEvent {
    pub(super) fn thread_id(&self) -> Option<&str> {
        match self {
            Self::LocalTurnStartRequested { thread_id, .. }
            | Self::LocalTurnStartAccepted { thread_id, .. }
            | Self::LocalTurnStartRejected { thread_id, .. }
            | Self::LocalTurnCancelRequested { thread_id, .. }
            | Self::LocalTurnCancelRejected { thread_id, .. }
            | Self::TurnStarted { thread_id, .. }
            | Self::TurnCompleted { thread_id, .. }
            | Self::TurnFailed { thread_id, .. }
            | Self::ItemStarted { thread_id, .. }
            | Self::ItemDelta { thread_id, .. }
            | Self::ItemTimeoutDetected { thread_id, .. }
            | Self::ItemRecoveryOpened { thread_id, .. }
            | Self::ItemRecoveryAttached { thread_id, .. }
            | Self::ItemRetryScheduled { thread_id, .. }
            | Self::ItemRetryAttemptStarted { thread_id, .. }
            | Self::ItemRecoverySucceeded { thread_id, .. }
            | Self::ItemRecoveryExhausted { thread_id, .. }
            | Self::ItemToolRetryScheduled { thread_id, .. }
            | Self::ItemToolRetryResolved { thread_id, .. }
            | Self::ItemToolRetryExhausted { thread_id, .. }
            | Self::TurnToolLoopBudgetExceeded { thread_id, .. }
            | Self::ItemCompleted { thread_id, .. }
            | Self::ItemUpdated { thread_id, .. } => Some(thread_id.as_str()),
        }
    }

    pub(super) fn turn_id(&self) -> Option<&str> {
        match self {
            Self::LocalTurnStartRequested { turn_id, .. }
            | Self::LocalTurnStartAccepted { turn_id, .. }
            | Self::LocalTurnStartRejected { turn_id, .. }
            | Self::LocalTurnCancelRequested { turn_id, .. }
            | Self::LocalTurnCancelRejected { turn_id, .. }
            | Self::ItemStarted { turn_id, .. }
            | Self::ItemDelta { turn_id, .. }
            | Self::ItemTimeoutDetected { turn_id, .. }
            | Self::ItemRecoveryOpened { turn_id, .. }
            | Self::ItemRecoveryAttached { turn_id, .. }
            | Self::ItemRetryScheduled { turn_id, .. }
            | Self::ItemRetryAttemptStarted { turn_id, .. }
            | Self::ItemRecoverySucceeded { turn_id, .. }
            | Self::ItemRecoveryExhausted { turn_id, .. }
            | Self::ItemToolRetryScheduled { turn_id, .. }
            | Self::ItemToolRetryResolved { turn_id, .. }
            | Self::ItemToolRetryExhausted { turn_id, .. }
            | Self::TurnToolLoopBudgetExceeded { turn_id, .. }
            | Self::ItemCompleted { turn_id, .. }
            | Self::ItemUpdated { turn_id, .. } => Some(turn_id.as_str()),
            Self::TurnStarted { turn, .. }
            | Self::TurnCompleted { turn, .. }
            | Self::TurnFailed { turn, .. } => Some(turn.id.as_str()),
        }
    }

    pub(super) fn item_id(&self) -> Option<&str> {
        match self {
            Self::ItemDelta { item_id, .. } => Some(item_id.as_str()),
            Self::ItemTimeoutDetected { item_id, .. }
            | Self::ItemRecoveryOpened { item_id, .. }
            | Self::ItemRecoveryAttached { item_id, .. }
            | Self::ItemRetryScheduled { item_id, .. }
            | Self::ItemRetryAttemptStarted { item_id, .. }
            | Self::ItemRecoverySucceeded { item_id, .. }
            | Self::ItemRecoveryExhausted { item_id, .. }
            | Self::ItemToolRetryScheduled { item_id, .. }
            | Self::ItemToolRetryResolved { item_id, .. }
            | Self::ItemToolRetryExhausted { item_id, .. } => Some(item_id.as_str()),
            Self::ItemStarted { item, .. }
            | Self::ItemCompleted { item, .. }
            | Self::ItemUpdated { item, .. } => Some(turn_item_id(item)),
            Self::LocalTurnStartRequested { .. }
            | Self::LocalTurnStartAccepted { .. }
            | Self::LocalTurnStartRejected { .. }
            | Self::LocalTurnCancelRequested { .. }
            | Self::LocalTurnCancelRejected { .. }
            | Self::TurnStarted { .. }
            | Self::TurnCompleted { .. }
            | Self::TurnFailed { .. }
            | Self::TurnToolLoopBudgetExceeded { .. } => None,
        }
    }

    pub(super) fn kind(&self) -> EventKind {
        match self {
            Self::LocalTurnStartRequested { .. } => EventKind::LocalTurnStartRequested,
            Self::LocalTurnStartAccepted { .. } => EventKind::LocalTurnStartAccepted,
            Self::LocalTurnStartRejected { .. } => EventKind::LocalTurnStartRejected,
            Self::LocalTurnCancelRequested { .. } => EventKind::LocalTurnCancelRequested,
            Self::LocalTurnCancelRejected { .. } => EventKind::LocalTurnCancelRejected,
            Self::TurnStarted { .. } => EventKind::TurnStarted,
            Self::TurnCompleted { .. } => EventKind::TurnCompleted,
            Self::TurnFailed { turn, .. } => {
                if turn.status == TurnStatus::Interrupted {
                    EventKind::TurnCancelled
                } else {
                    EventKind::TurnFailed
                }
            }
            Self::ItemStarted { .. } => EventKind::ItemStarted,
            Self::ItemDelta { .. } => EventKind::ItemDelta,
            Self::ItemTimeoutDetected { .. } => EventKind::ItemTimeoutDetected,
            Self::ItemRecoveryOpened { .. } => EventKind::ItemRecoveryOpened,
            Self::ItemRecoveryAttached { .. } => EventKind::ItemRecoveryAttached,
            Self::ItemRetryScheduled { .. } => EventKind::ItemRetryScheduled,
            Self::ItemRetryAttemptStarted { .. } => EventKind::ItemRetryAttemptStarted,
            Self::ItemRecoverySucceeded { .. } => EventKind::ItemRecoverySucceeded,
            Self::ItemRecoveryExhausted { .. } => EventKind::ItemRecoveryExhausted,
            Self::ItemToolRetryScheduled { .. } => EventKind::ItemToolRetryScheduled,
            Self::ItemToolRetryResolved { .. } => EventKind::ItemToolRetryResolved,
            Self::ItemToolRetryExhausted { .. } => EventKind::ItemToolRetryExhausted,
            Self::TurnToolLoopBudgetExceeded { .. } => EventKind::TurnToolLoopBudgetExceeded,
            Self::ItemCompleted { .. } => EventKind::ItemCompleted,
            Self::ItemUpdated { .. } => EventKind::ItemUpdated,
        }
    }

    pub(super) fn payload_value(&self) -> JsonValue {
        match serde_json::to_value(self) {
            Ok(value) => value,
            Err(error) => json!({
                "error": format!("failed to serialize event payload: {error}")
            }),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct EventEnvelope {
    pub sequence: u64,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub kind: EventKind,
    pub payload: JsonValue,
    pub ts_unix_ms: i64,
}

pub(super) fn turn_item_id(item: &TurnItem) -> &str {
    match item {
        TurnItem::UserMessage { id, .. }
        | TurnItem::AgentMessage { id, .. }
        | TurnItem::Reasoning { id, .. }
        | TurnItem::SystemEvent { id, .. }
        | TurnItem::Task {
            item: pioneer_protocol::TaskTurnItem { id, .. },
        }
        | TurnItem::CommandExecution { id, .. }
        | TurnItem::FileChange { id, .. }
        | TurnItem::WebSearch { id, .. }
        | TurnItem::WebFetch { id, .. }
        | TurnItem::Download { id, .. }
        | TurnItem::DynamicToolCall { id, .. } => id.as_str(),
    }
}

pub(super) fn turn_item_type(item: &TurnItem) -> &'static str {
    match item {
        TurnItem::UserMessage { .. } => "user_message",
        TurnItem::AgentMessage { .. } => "agent_message",
        TurnItem::Reasoning { .. } => "reasoning",
        TurnItem::SystemEvent { .. } => "system_event",
        TurnItem::Task { .. } => "task",
        TurnItem::CommandExecution { .. } => "command_execution",
        TurnItem::FileChange { .. } => "file_change",
        TurnItem::WebSearch { .. } => "web_search",
        TurnItem::WebFetch { .. } => "web_fetch",
        TurnItem::Download { .. } => "download",
        TurnItem::DynamicToolCall { .. } => "dynamic_tool_call",
    }
}
