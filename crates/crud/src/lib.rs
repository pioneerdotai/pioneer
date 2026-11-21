//! Database access shared by the Gateway and its services.

mod api;
mod store;

use crate::events::TurnEventPayload;
pub use api::*;
#[cfg(test)]
use pioneer_crud_access::authorization_scope::principal_current_thread_access_kind;
#[cfg(test)]
use pioneer_crud_conversations::message_commands::message_mutation_actor_current_thread_write_kind;
use pioneer_crud_conversations::{events, model_history};
use pioneer_crud_execution::{terminal_cleanup, turn_execution, turn_item_terminal};
use pioneer_crud_memory::thread_episodic;
pub(crate) const DB_ID_LEN: usize = 21;

#[cfg(test)]
mod tests;
