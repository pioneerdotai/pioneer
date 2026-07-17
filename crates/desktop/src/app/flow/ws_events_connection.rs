use super::*;
use pioneer_client::state::reducers::{
    GatewayConnectionReduction, GatewaySettingsConnectionError, GatewayStatusMessage,
};

impl PioneerDesktop {
    pub(in crate::app::flow) fn apply_gateway_connection_reduction(
        &mut self,
        reduction: GatewayConnectionReduction,
        mut cx: Option<&mut Context<Self>>,
    ) {
        self.gateway.status = gateway_status_message_text(&reduction.status);
        self.gateway.status_level = reduction.status_level;
        self.gateway.connection_state = reduction.connection_state;
        self.gateway.error = reduction.gateway_error;

        if let Some(settings) = reduction.settings {
            if settings.clear_settings {
                self.gateway.settings = None;
            }
            self.gateway.settings_loading = settings.loading;
            self.gateway.settings_error =
                settings.error.map(gateway_settings_connection_error_text);
        }

        if let Some(loading) = reduction.thread_list_loading {
            self.thread_list_loading = loading;
        }
        if let Some(loading) = reduction.workspaces_loading {
            self.set_workspaces_loading(loading);
        }
        if let Some(error) = reduction.workspaces_error {
            self.set_workspaces_error(error);
        }
        if reduction.clear_active_thread {
            self.set_active_thread_id(None);
        }
        if reduction.reset_thread_start {
            self.reset_thread_start_state();
        }
        if reduction.clear_thread_start_queue {
            self.clear_thread_start_queue();
        }
        if reduction.clear_turn_resume_queue {
            self.clear_turn_resume_queue();
        }

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.desktop_voice_status = pioneer_protocol::VoiceStatus::Unavailable;
            self.desktop_voice_status_error = None;
            self.voice_input_action_error = None;
            self.voice_input_action_generation = self.voice_input_action_generation.wrapping_add(1);
            self.pending_voice_input_enabled = None;
        }

        if let Some(cx) = cx.as_deref_mut() {
            if self.gateway.connection_state == GatewayConnectionState::Connected {
                self.refresh_desktop_voice_status(cx);
            }
            execute_desktop_client_effects(self, reduction.effects, cx);
        }
    }
}

fn gateway_settings_connection_error_text(error: GatewaySettingsConnectionError) -> String {
    match error {
        GatewaySettingsConnectionError::GatewayNotConnected => {
            t!("settings.gateway_not_connected").to_string()
        }
    }
}

pub(in crate::app::flow) fn gateway_status_message_text(status: &GatewayStatusMessage) -> String {
    match status {
        GatewayStatusMessage::Connecting => t!("gateway.status.connecting").to_string(),
        GatewayStatusMessage::ConnectingNamed { endpoint_name } => t!(
            "gateway.status.connecting_named",
            gateway_name = endpoint_name.as_str()
        )
        .to_string(),
        GatewayStatusMessage::StartingLocal => t!("gateway.status.starting_local").to_string(),
        GatewayStatusMessage::Reconnecting {
            endpoint_name,
            attempt,
            delay_ms,
        } => format!(
            "{} (attempt {attempt}, {} ms)",
            t!(
                "gateway.status.connecting_named",
                gateway_name = endpoint_name.as_str()
            ),
            delay_ms
        ),
        GatewayStatusMessage::Connected => t!("gateway.status.connected").to_string(),
        GatewayStatusMessage::ConnectedEndpoint {
            endpoint_name,
            address,
        } => format!(
            "{}: {} ({})",
            t!("gateway.status.connected"),
            endpoint_name.as_str(),
            address.as_str()
        ),
        GatewayStatusMessage::LocalStopped { address } => t!(
            "gateway.status.local_stopped",
            gateway_address = address.as_str()
        )
        .to_string(),
        GatewayStatusMessage::RemoteUnavailable {
            endpoint_name,
            address,
        } => t!(
            "gateway.status.remote_unavailable",
            gateway_name = endpoint_name.as_str(),
            gateway_address = address.as_str()
        )
        .to_string(),
        GatewayStatusMessage::NotConfigured => t!("gateway.status.not_configured").to_string(),
        GatewayStatusMessage::Unavailable => t!("gateway.status.unavailable").to_string(),
        GatewayStatusMessage::LocalConflictAt { address } => t!(
            "gateway.status.local_conflict_at",
            gateway_address = address.as_str()
        )
        .to_string(),
        GatewayStatusMessage::LocalConflict => t!("gateway.status.local_conflict").to_string(),
        GatewayStatusMessage::FailedCheck { error } => {
            t!("gateway.status.failed_check", error = error.as_str()).to_string()
        }
        GatewayStatusMessage::SubsystemFailed { error } => {
            t!("gateway.status.subsystem_failed", error = error.as_str()).to_string()
        }
        GatewayStatusMessage::SubsystemNotReady => {
            t!("gateway.status.subsystem_not_ready").to_string()
        }
    }
}
