//! Retained thread presentation over the process-local Client.
#[macro_use]
extern crate rust_i18n;
rust_i18n::i18n!("locales", fallback = "en");
mod approvals;
mod artifacts;
mod assets;
mod avatar;
mod binding;
mod buttons;
mod code_highlight;
mod composer;
mod file_opener;
mod footer;
mod header;
mod member_picker;
mod members;
mod message_deletion;
mod message_history;
mod model_picker;
mod panel_layout;
mod panels;
mod ports;
mod qualification_diagnostics;
mod screen;
mod task_review;
#[cfg(test)]
mod test_support;
mod thread;
mod timeline;
pub use thread::{ThreadNavigationEvent, ThreadView, ThreadViewConfig};

pub use ports::{
    ThreadAudioCompletion, ThreadAudioError, ThreadAudioErrorKind, ThreadAudioPort,
    ThreadAudioRequest,
};

pub use ports::{
    ThreadExternalNavigationPort, ThreadExternalNavigationRequest, ThreadFileOpenRequest,
    ThreadFileOpenerChoice, ThreadFileOpenerPresentation, ThreadFilePort,
    ThreadPresentationOperation,
};

#[cfg(test)]
mod localization_tests;
