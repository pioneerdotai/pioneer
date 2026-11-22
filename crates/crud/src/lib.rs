//! Database access shared by the Gateway and its services.

mod api;
mod store;

#[cfg(test)]
use crate::events::TurnEventPayload;
pub use api::*;
#[cfg(test)]
use pioneer_crud_access::authorization_scope::principal_current_thread_access_kind;
use pioneer_crud_conversations::events;
#[cfg(test)]
use pioneer_crud_conversations::message_commands::message_mutation_actor_current_thread_write_kind;
#[cfg(test)]
use pioneer_crud_execution::turn_execution;
use pioneer_crud_execution::{terminal_cleanup, turn_item_terminal};
pub(crate) const DB_ID_LEN: usize = 21;

#[cfg(test)]
mod tests;
