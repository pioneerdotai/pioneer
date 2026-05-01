mod core;
mod display;
mod events;
mod handlers;
mod history;
mod reducer;
mod state_machine;

use self::events::EventEnvelope;
use self::handlers::TurnItemHandlerRegistry;
use self::reducer::ConversationProjector;
use self::state_machine::TurnStateMachine;
use pioneer_protocol::{
    SystemEventLevel, TaskEvent, ThreadHistoryEvent, ThreadHistoryEventPayload, TimelineOriginKind,
    TimelinePayload, TurnItem, TurnItemEventPayload, TurnTimelineResponse,
};
use std::collections::VecDeque;

pub(in crate::app) use self::display::tool_display_text;
pub(super) use self::events::ConversationEvent;
pub(super) use self::reducer::{
    ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus,
};

#[cfg(test)]
pub(super) use self::reducer::{TurnPhase, TurnView};

pub(super) struct Conversation {
    thread_id: String,
    next_sequence: u64,
    event_log: VecDeque<EventEnvelope>,
    pending_completion_turn_id: Option<String>,
    projector: ConversationProjector,
    state_machine: TurnStateMachine,
    item_handlers: TurnItemHandlerRegistry,
}

const MAX_EVENT_LOG_LEN: usize = 2_000;

fn history_event_thread_id(payload: &ThreadHistoryEventPayload) -> &str {
    match payload {
        ThreadHistoryEventPayload::TurnStarted { thread_id, .. }
        | ThreadHistoryEventPayload::ItemStarted { thread_id, .. }
        | ThreadHistoryEventPayload::ItemDelta { thread_id, .. }
        | ThreadHistoryEventPayload::ItemCompleted { thread_id, .. }
        | ThreadHistoryEventPayload::ItemUpdated { thread_id, .. }
        | ThreadHistoryEventPayload::ItemTimeoutDetected { thread_id, .. }
        | ThreadHistoryEventPayload::ItemRecoveryOpened { thread_id, .. }
        | ThreadHistoryEventPayload::ItemRecoveryAttached { thread_id, .. }
        | ThreadHistoryEventPayload::ItemRetryScheduled { thread_id, .. }
        | ThreadHistoryEventPayload::ItemRetryAttemptStarted { thread_id, .. }
        | ThreadHistoryEventPayload::ItemRecoverySucceeded { thread_id, .. }
        | ThreadHistoryEventPayload::ItemRecoveryExhausted { thread_id, .. }
        | ThreadHistoryEventPayload::ItemToolRetryScheduled { thread_id, .. }
        | ThreadHistoryEventPayload::ItemToolRetryResolved { thread_id, .. }
        | ThreadHistoryEventPayload::ItemToolRetryExhausted { thread_id, .. }
        | ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded { thread_id, .. }
        | ThreadHistoryEventPayload::TurnCompleted { thread_id, .. }
        | ThreadHistoryEventPayload::TurnFailed { thread_id, .. } => thread_id.as_str(),
    }
}

fn now_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests;
