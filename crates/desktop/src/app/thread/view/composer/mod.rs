mod capabilities;
mod mode_selector;
mod model_selector;
mod permission_selector;
mod view;

use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::prelude::*;
use pioneer_client::composer::turn_prepare::{
    ComposerSubmitAvailabilityInput, can_submit_composer_message,
};

impl PioneerDesktop {
    pub(in crate::app::thread) fn can_submit_message(&self, cx: &Context<Self>) -> bool {
        if self.active_task_thread_navigation().is_some() {
            return false;
        }

        let composer_text = self.composer_state.read(cx).value();
        let cli_runtime_selected = self.composer_selected_provider_is_cli_runtime();
        can_submit_composer_message(ComposerSubmitAvailabilityInput {
            gateway_connected: self.gateway.connection_state == GatewayConnectionState::Connected,
            upload_in_progress: self.composer_upload_in_progress,
            has_active_thread: self.current_active_thread_id().is_some(),
            has_complete_model_selection: self.has_complete_composer_model_selection(),
            conversation_can_submit: self
                .active_thread_conversation()
                .is_some_and(|conversation| conversation.can_submit_message()),
            text: composer_text.as_str(),
            has_attachments: !self.composer_attachments.is_empty(),
            has_capabilities: !cli_runtime_selected && !self.composer_capabilities.is_empty(),
        })
    }
}
