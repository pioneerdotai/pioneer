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
        let was_connected = self.gateway.connection_state == GatewayConnectionState::Connected;
        // Authorization revisions are monotonic only within one Gateway
        // process/connection epoch. A reconnect can land on a restarted
        // Gateway whose next revision is lower than the previously observed
        // value. Access-change delivery can also be missed while the transport
        // is down, so protected projections must be reloaded from current-ACL
        // endpoints instead of surviving as readable cache.
        self.gateway.authorization_revision = None;
        self.clear_authorization_epoch_cache();
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
            self.active_thread_resubscribe_pending = false;
            self.workspace_members_loading.clear();
            self.semantic_timeline_in_flight.clear();
            self.semantic_timeline_pending.clear();
            self.desktop_voice_status = pioneer_protocol::VoiceStatus::Unavailable;
            self.desktop_voice_status_error = None;
            self.voice_input_action_error = None;
            self.voice_input_action_generation = self.voice_input_action_generation.wrapping_add(1);
            self.pending_voice_input_enabled = None;
        }

        if let Some(cx) = cx.as_deref_mut() {
            self.rebuild_sidebar_tree_state(cx);
            if self.gateway.connection_state == GatewayConnectionState::Connected {
                self.resolve_agent_avatar(cx);
                self.refresh_current_principal(cx);
                self.active_thread_resubscribe_pending = self.current_active_thread_id().is_some();
                self.refresh_desktop_voice_status(cx);
                if !was_connected {
                    self.reconcile_semantic_timeline_after_reconnect(cx);
                }
            }
            if self.gateway.connection_state != GatewayConnectionState::Connected {
                self.gateway.current_auth = None;
                self.administration.clear_for_session_termination();
                self.member_avatar_state.clear();
                self.members_loading = false;
                self.member_workspaces_saving = false;
                self.members_error = None;
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
            gateway_base_url,
        } => format!(
            "{}: {} ({})",
            t!("gateway.status.connected"),
            endpoint_name.as_str(),
            gateway_base_url.as_str()
        ),
        GatewayStatusMessage::LocalStopped { gateway_base_url } => t!(
            "gateway.status.local_stopped",
            gateway_address = gateway_base_url.as_str()
        )
        .to_string(),
        GatewayStatusMessage::RemoteUnavailable {
            endpoint_name,
            gateway_base_url,
        } => t!(
            "gateway.status.remote_unavailable",
            gateway_name = endpoint_name.as_str(),
            gateway_address = gateway_base_url.as_str()
        )
        .to_string(),
        GatewayStatusMessage::NotConfigured => t!("gateway.status.not_configured").to_string(),
        GatewayStatusMessage::Unavailable => t!("gateway.status.unavailable").to_string(),
        GatewayStatusMessage::LocalConflictAt { gateway_base_url } => t!(
            "gateway.status.local_conflict_at",
            gateway_address = gateway_base_url.as_str()
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

#[cfg(test)]
mod authorization_epoch_tests {
    #[test]
    fn connection_epoch_clears_protected_cache_without_touching_session_or_registry() {
        let connection_source = include_str!("ws_events_connection.rs");
        let mutations_source = include_str!("../root/mutations.rs");

        assert!(connection_source.contains("self.clear_authorization_epoch_cache()"));
        for required in [
            "self.workspaces.clear()",
            "self.clear_thread_conversations()",
            "self.thread_artifacts = Default::default()",
            "self.active_agents_doc_editor_scope = None",
            "self.clear_workspace_capability_projections()",
        ] {
            assert!(
                mutations_source.contains(required),
                "authorization-epoch cleanup is missing `{required}`"
            );
        }
        for forbidden in [
            "remove_gateway(",
            "delete_gateway(",
            "clear_refresh_credential(",
            "revoke_auth_session(",
        ] {
            assert!(
                !mutations_source.contains(forbidden),
                "authorization-epoch cleanup must preserve endpoint/session state: `{forbidden}`"
            );
        }
    }
}
