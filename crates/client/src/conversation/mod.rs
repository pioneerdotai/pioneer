pub mod core;
pub mod display;
pub mod events;
pub mod handlers;
#[cfg(test)]
mod history;
pub mod reducer;
pub mod state_machine;

use self::events::EventEnvelope;
use self::handlers::TurnItemHandlerRegistry;
use self::reducer::ConversationProjector;
use self::state_machine::TurnStateMachine;
#[cfg(test)]
use pioneer_protocol::{ThreadHistoryEvent, ThreadHistoryEventPayload};
use std::collections::VecDeque;

pub use self::display::tool_display_text;
pub use self::events::ConversationEvent;
pub use self::reducer::{
    ConversationViewState, ItemView, PermissionAuditDisplayItem, TimelineEntry, TimelineEntryStatus,
};

#[cfg(test)]
pub use self::reducer::{TurnPhase, TurnView};

pub struct Conversation {
    thread_id: String,
    next_sequence: u64,
    event_log: VecDeque<EventEnvelope>,
    pending_completion_turn_id: Option<String>,
    projector: ConversationProjector,
    state_machine: TurnStateMachine,
    item_handlers: TurnItemHandlerRegistry,
}

impl Conversation {
    pub(crate) fn snapshot_copy(&self) -> Self {
        Self {
            thread_id: self.thread_id.clone(),
            next_sequence: self.next_sequence,
            event_log: self.event_log.clone(),
            pending_completion_turn_id: self.pending_completion_turn_id.clone(),
            projector: self.projector.clone(),
            state_machine: self.state_machine.clone(),
            item_handlers: TurnItemHandlerRegistry::default(),
        }
    }
}

pub const MAX_EVENT_LOG_LEN: usize = 2_000;

#[cfg(test)]
mod tests;

#[cfg(test)]
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
        | ThreadHistoryEventPayload::TurnExecutionWindowStarted(
            pioneer_protocol::TurnExecutionWindowStartedNotification { thread_id, .. },
        )
        | ThreadHistoryEventPayload::TurnExecutionWindowExhausted(
            pioneer_protocol::TurnExecutionWindowExhaustedNotification { thread_id, .. },
        )
        | ThreadHistoryEventPayload::TurnExecutionWindowCheckpointed(
            pioneer_protocol::TurnExecutionWindowCheckpointedNotification { thread_id, .. },
        )
        | ThreadHistoryEventPayload::TurnExecutionWindowContinued(
            pioneer_protocol::TurnExecutionWindowContinuedNotification { thread_id, .. },
        )
        | ThreadHistoryEventPayload::TurnExecutionWindowBlocked(
            pioneer_protocol::TurnExecutionWindowBlockedNotification { thread_id, .. },
        )
        | ThreadHistoryEventPayload::TurnPermissionAudit(
            pioneer_protocol::TurnPermissionAuditEvent { thread_id, .. },
        )
        | ThreadHistoryEventPayload::TurnCompleted { thread_id, .. }
        | ThreadHistoryEventPayload::TurnFailed { thread_id, .. }
        | ThreadHistoryEventPayload::TurnBlocked { thread_id, .. } => thread_id.as_str(),
    }
}

fn now_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}
