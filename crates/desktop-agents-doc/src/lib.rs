//! Retained Agents document editor over the process-local Client owner.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod binding;
mod editor;
mod view;
pub use editor::{AgentsDocumentConfig, AgentsDocumentEditor};

/// Uses the editor's existing error vocabulary for a deferred window close.
pub fn present_close_error(
    error: &pioneer_client::agents_doc::controller::AgentsDocumentCloseError,
    window: &mut gpui_kit::Window,
    cx: &mut gpui_kit::App,
) {
    use gpui_kit::component::{WindowExt, notification::Notification};
    use pioneer_client::agents_doc::controller::AgentsDocumentCloseError;
    let message = match error {
        AgentsDocumentCloseError::Conflict(_) => t!("editor.agents_doc.save_conflict"),
        _ => t!("editor.agents_doc.save_state.dirty"),
    };
    window.push_notification(
        Notification::error(message.to_string()).title(t!("editor.agents_doc.title").to_string()),
        cx,
    );
}
