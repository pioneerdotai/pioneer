pub mod core;
pub mod display;
pub mod events;
pub mod handlers;
pub mod reducer;
pub mod state_machine;

use self::events::EventEnvelope;
use self::handlers::TurnItemHandlerRegistry;
use self::reducer::ConversationProjector;
use self::state_machine::TurnStateMachine;
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
    pub(crate) fn turn_phase_marker(
        &self,
        turn_id: &str,
    ) -> Option<std::sync::Arc<self::reducer::TurnPhase>> {
        self.projector.turn_phase_marker(turn_id).cloned()
    }

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

fn now_unix_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}
