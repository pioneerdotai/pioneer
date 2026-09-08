mod capabilities;
pub(crate) use capabilities::CapabilityPickerState;
mod mode_selector;

mod model_selector;
pub(crate) use crate::model_picker::ComposerModelPickerView;
mod commands;
mod owner;
mod permission_selector;
mod queries;
mod view;
mod voice;
mod voice_owner;
pub(crate) use owner::ComposerView;
pub(crate) use pioneer_client::composer::attachments::{
    ComposerAttachment, ComposerAttachmentUploadState,
};
pub(crate) use pioneer_client::composer::capabilities::{
    ComposerCapability, ComposerCapabilityKind,
};
pub(crate) use pioneer_client::state::client_state::GatewayConnectionState;

use gpui_kit::prelude::*;
use pioneer_client::composer::turn_prepare::ComposerSubmitAvailabilityInput;
use pioneer_client::composer::turn_prepare::can_submit_composer_message;

fn desktop_composer_transport_ready(connection_state: GatewayConnectionState) -> bool {
    connection_state == GatewayConnectionState::Connected
}

impl ComposerView {
    pub(crate) fn desktop_microphone_error_message(&self, cx: &Context<Self>) -> Option<String> {
        self.voice
            .read(cx)
            .desktop_voice_error_message()
            .map(str::to_owned)
    }

    pub(crate) fn can_submit_message(&self, cx: &Context<Self>) -> bool {
        let composer_text = self.composer_state.read(cx).value();
        let effective_capabilities = self.effective_composer_capabilities();
        let scoped_write_allowed = if self.composer_domain().selected_mode
            == pioneer_client::timeline::types::ThreadMode::Message
        {
            self.can_write_active_thread_presentation()
        } else {
            self.can_start_active_thread_agent_presentation()
        };
        can_submit_composer_message(ComposerSubmitAvailabilityInput {
            gateway_connected: desktop_composer_transport_ready(self.connection_state)
                && scoped_write_allowed,
            upload_in_progress: self.composer_upload_in_progress(),
            has_active_thread: self.current_active_thread_id().is_some(),
            selected_mode: self.composer_domain().selected_mode,
            has_complete_model_selection: self.has_complete_composer_model_selection(),
            conversation_can_submit: self
                .active_thread_conversation()
                .is_some_and(|conversation| conversation.can_submit_message()),
            text: composer_text.as_str(),
            has_attachments: !self.composer_domain().attachments.is_empty(),
            has_capabilities: !effective_capabilities.is_empty()
                || !self.composer_domain().skill_selections.is_empty(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composer_transport_tracks_the_connection_not_background_workspace_sync() {
        assert!(desktop_composer_transport_ready(
            GatewayConnectionState::Connected
        ));
        assert!(!desktop_composer_transport_ready(
            GatewayConnectionState::Reconnecting
        ));
    }
}
