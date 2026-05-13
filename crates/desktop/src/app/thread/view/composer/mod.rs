mod mode_selector;
mod model_selector;
mod view;

use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::prelude::*;

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

        !self.composer_state.read(cx).value().trim().is_empty()
            || !self.composer_attachments.is_empty()
    }
}
