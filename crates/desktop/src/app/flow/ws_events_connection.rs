use super::*;
use pioneer_client::state::reducers::{
    GatewayConnectionEvent, GatewayConnectionReduction, GatewayStatusMessage,
    reduce_gateway_connection_event,
};

impl PioneerDesktop {
    pub(in crate::app::flow) fn apply_ws_connecting_event(
        &mut self,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
    ) {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Connecting {
            endpoint_name,
            endpoint_kind,
        });
        self.apply_gateway_connection_reduction(reduction, None);
    }

    pub(in crate::app::flow) fn apply_ws_connected_event(
        &mut self,
        endpoint_name: String,
        address: String,
        cx: &mut Context<Self>,
    ) {
        let queue_skills_refresh = matches!(
            self.main_content_view,
            MainContentView::Skills | MainContentView::SkillDetails
        );
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Connected {
            endpoint_name,
            address,
            queue_skills_refresh,
        });
        self.apply_gateway_connection_reduction(reduction, Some(cx));
    }

    pub(in crate::app::flow) fn apply_ws_reconnecting_event(
        &mut self,
        endpoint_name: String,
        attempt: u32,
        delay_ms: u64,
        reason: String,
    ) {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Reconnecting {
            endpoint_name,
            attempt,
            delay_ms,
            reason,
            should_resume_in_flight_turn: self.should_resume_in_flight_turn(),
        });
        self.apply_gateway_connection_reduction(reduction, None);
    }

    pub(in crate::app::flow) fn apply_ws_disconnected_event(
        &mut self,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        reason: String,
    ) {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::Disconnected {
            endpoint_name,
            endpoint_kind,
            address,
            reason,
            should_resume_in_flight_turn: self.should_resume_in_flight_turn(),
        });
        self.apply_gateway_connection_reduction(reduction, None);
    }

    pub(in crate::app::flow) fn apply_ws_connect_failed_event(
        &mut self,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        error: String,
    ) {
        let reduction = reduce_gateway_connection_event(GatewayConnectionEvent::ConnectFailed {
            endpoint_name,
            endpoint_kind,
            address,
            error,
            should_resume_in_flight_turn: self.should_resume_in_flight_turn(),
        });
        self.apply_gateway_connection_reduction(reduction, None);
    }

    fn apply_gateway_connection_reduction(
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
            self.gateway.settings_error = settings.error;
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

        if let Some(cx) = cx.as_deref_mut() {
            execute_desktop_client_effects(self, reduction.effects, cx);
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
