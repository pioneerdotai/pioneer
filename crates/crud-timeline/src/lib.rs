//! Semantic timeline projection and display classification.

pub mod acp_compactions;
pub mod timeline_live_projection;
pub mod timeline_projection;
pub mod timeline_projection_model;

pub mod codex_retry_notice;

pub mod conversation_display;
pub mod thread_display;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support;
