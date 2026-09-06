mod actions;
pub(in crate::app) mod binding;
mod message_mutations;
pub(in crate::app) mod message_revisions;
pub(crate) mod view;

pub(crate) use pioneer_client::threads::coordinator::ThreadCoordinator;
pub(crate) use pioneer_client::threads::title::thread_display_title;
