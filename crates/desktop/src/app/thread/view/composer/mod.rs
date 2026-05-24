mod capabilities;
mod mode_selector;
mod model_selector;
mod view;

use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::prelude::*;

fn composer_has_sendable_content(
    text: &str,
    has_attachments: bool,
    has_capabilities: bool,
) -> bool {
    !text.trim().is_empty() || has_attachments || has_capabilities
}

impl PioneerDesktop {
    pub(in crate::app::thread) fn can_submit_message(&self, cx: &Context<Self>) -> bool {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return false;
        }

        if self.composer_upload_in_progress {
            return false;
        }

        if self.current_active_thread_id().is_none() {
            return false;
        }

        if !self.has_complete_composer_model_selection() {
            return false;
        }

        let Some(conversation) = self.active_thread_conversation() else {
            return false;
        };

        if !conversation.can_submit_message() {
            return false;
        }

        composer_has_sendable_content(
            self.composer_state.read(cx).value().as_str(),
            !self.composer_attachments.is_empty(),
            !self.composer_capabilities.is_empty(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::composer_has_sendable_content;

    #[test]
    fn composer_content_gate_allows_capability_only_turns() {
        assert!(composer_has_sendable_content("", false, true));
        assert!(composer_has_sendable_content("   ", true, false));
        assert!(composer_has_sendable_content("hello", false, false));
        assert!(!composer_has_sendable_content("   ", false, false));
    }
}
