mod actions;
mod coordinator;
mod title;
pub(crate) mod view;

pub(crate) use coordinator::ThreadCoordinator;
pub(crate) use title::fallback_title_from_first_user_text;
