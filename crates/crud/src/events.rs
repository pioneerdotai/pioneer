use pioneer_protocol::constants::events;
use pioneer_protocol::{
    ItemCompletedNotification, ItemRecoveryAttachedNotification, ItemRecoveryExhaustedNotification,
    ItemRecoveryOpenedNotification, ItemRecoverySucceededNotification,
    ItemRetryAttemptStartedNotification, ItemRetryScheduledNotification, ItemStartedNotification,
    ItemTimeoutDetectedNotification, ItemToolRetryExhaustedNotification,
    ItemToolRetryResolvedNotification, ItemToolRetryScheduledNotification, ItemUpdatedNotification,
    SandboxMode, Thread, Turn, TurnCompletedNotification, TurnFailedNotification,
    TurnToolLoopBudgetExceededNotification, UserInput,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnStartedEventPayload {
    pub thread: Thread,
    pub sandbox_mode: SandboxMode,
    pub turn: Turn,
    pub input: Vec<UserInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum TurnEventPayload {
    TurnStarted(TurnStartedEventPayload),
    ItemStarted(ItemStartedNotification),
    ItemCompleted(ItemCompletedNotification),
    ItemUpdated(ItemUpdatedNotification),
    ItemTimeoutDetected(ItemTimeoutDetectedNotification),
    ItemRecoveryOpened(ItemRecoveryOpenedNotification),
    ItemRecoveryAttached(ItemRecoveryAttachedNotification),
    ItemRetryScheduled(ItemRetryScheduledNotification),
    ItemRetryAttemptStarted(ItemRetryAttemptStartedNotification),
    ItemRecoverySucceeded(ItemRecoverySucceededNotification),
    ItemRecoveryExhausted(ItemRecoveryExhaustedNotification),
    ItemToolRetryScheduled(ItemToolRetryScheduledNotification),
    ItemToolRetryResolved(ItemToolRetryResolvedNotification),
    ItemToolRetryExhausted(ItemToolRetryExhaustedNotification),
    TurnToolLoopBudgetExceeded(TurnToolLoopBudgetExceededNotification),
    TurnCompleted(TurnCompletedNotification),
    TurnFailed(TurnFailedNotification),
}

impl TurnEventPayload {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::TurnStarted(_) => events::TURN_STARTED,
            Self::ItemStarted(_) => events::ITEM_STARTED,
            Self::ItemCompleted(_) => events::ITEM_COMPLETED,
            Self::ItemUpdated(_) => events::ITEM_UPDATED,
            Self::ItemTimeoutDetected(_) => events::ITEM_TIMEOUT_DETECTED,
            Self::ItemRecoveryOpened(_) => events::ITEM_RECOVERY_OPENED,
            Self::ItemRecoveryAttached(_) => events::ITEM_RECOVERY_ATTACHED,
            Self::ItemRetryScheduled(_) => events::ITEM_RETRY_SCHEDULED,
            Self::ItemRetryAttemptStarted(_) => events::ITEM_RETRY_ATTEMPT_STARTED,
            Self::ItemRecoverySucceeded(_) => events::ITEM_RECOVERY_SUCCEEDED,
            Self::ItemRecoveryExhausted(_) => events::ITEM_RECOVERY_EXHAUSTED,
            Self::ItemToolRetryScheduled(_) => events::ITEM_TOOL_RETRY_SCHEDULED,
            Self::ItemToolRetryResolved(_) => events::ITEM_TOOL_RETRY_RESOLVED,
            Self::ItemToolRetryExhausted(_) => events::ITEM_TOOL_RETRY_EXHAUSTED,
            Self::TurnToolLoopBudgetExceeded(_) => events::TURN_TOOL_LOOP_BUDGET_EXCEEDED,
            Self::TurnCompleted(_) => events::TURN_COMPLETED,
            Self::TurnFailed(_) => events::TURN_FAILED,
        }
    }

    pub fn thread_id(&self) -> &str {
        match self {
            Self::TurnStarted(payload) => payload.thread.id.as_str(),
            Self::ItemStarted(payload) => payload.thread_id.as_str(),
            Self::ItemCompleted(payload) => payload.thread_id.as_str(),
            Self::ItemUpdated(payload) => payload.thread_id.as_str(),
            Self::ItemTimeoutDetected(payload) => payload.thread_id.as_str(),
            Self::ItemRecoveryOpened(payload) => payload.thread_id.as_str(),
            Self::ItemRecoveryAttached(payload) => payload.thread_id.as_str(),
            Self::ItemRetryScheduled(payload) => payload.thread_id.as_str(),
            Self::ItemRetryAttemptStarted(payload) => payload.thread_id.as_str(),
            Self::ItemRecoverySucceeded(payload) => payload.thread_id.as_str(),
            Self::ItemRecoveryExhausted(payload) => payload.thread_id.as_str(),
            Self::ItemToolRetryScheduled(payload) => payload.thread_id.as_str(),
            Self::ItemToolRetryResolved(payload) => payload.thread_id.as_str(),
            Self::ItemToolRetryExhausted(payload) => payload.thread_id.as_str(),
            Self::TurnToolLoopBudgetExceeded(payload) => payload.thread_id.as_str(),
            Self::TurnCompleted(payload) => payload.thread_id.as_str(),
            Self::TurnFailed(payload) => payload.thread_id.as_str(),
        }
    }

    pub fn turn_id(&self) -> &str {
        match self {
            Self::TurnStarted(payload) => payload.turn.id.as_str(),
            Self::ItemStarted(payload) => payload.turn_id.as_str(),
            Self::ItemCompleted(payload) => payload.turn_id.as_str(),
            Self::ItemUpdated(payload) => payload.turn_id.as_str(),
            Self::ItemTimeoutDetected(payload) => payload.turn_id.as_str(),
            Self::ItemRecoveryOpened(payload) => payload.turn_id.as_str(),
            Self::ItemRecoveryAttached(payload) => payload.turn_id.as_str(),
            Self::ItemRetryScheduled(payload) => payload.turn_id.as_str(),
            Self::ItemRetryAttemptStarted(payload) => payload.turn_id.as_str(),
            Self::ItemRecoverySucceeded(payload) => payload.turn_id.as_str(),
            Self::ItemRecoveryExhausted(payload) => payload.turn_id.as_str(),
            Self::ItemToolRetryScheduled(payload) => payload.turn_id.as_str(),
            Self::ItemToolRetryResolved(payload) => payload.turn_id.as_str(),
            Self::ItemToolRetryExhausted(payload) => payload.turn_id.as_str(),
            Self::TurnToolLoopBudgetExceeded(payload) => payload.turn_id.as_str(),
            Self::TurnCompleted(payload) => payload.turn.id.as_str(),
            Self::TurnFailed(payload) => payload.turn.id.as_str(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendedTurnEvent {
    pub payload: TurnEventPayload,
    pub created_at: DateTimeWithTimeZone,
}
