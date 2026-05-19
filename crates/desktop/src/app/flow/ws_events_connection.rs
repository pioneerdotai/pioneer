use super::*;

impl PioneerDesktop {
    pub(in crate::app::flow) fn apply_ws_connecting_event(
        &mut self,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
    ) {
        self.gateway.status = t!(
            "gateway.status.connecting_named",
            gateway_name = endpoint_name.as_str()
        )
        .to_string();
        self.gateway.status_level = GatewayStatusLevel::Neutral;
        self.gateway.connection_state = GatewayConnectionState::Connecting;
        self.gateway.error = None;

        if endpoint_kind == GatewayEndpointKind::Local {
            self.gateway.status = t!("gateway.status.starting_local").to_string();
        }
    }

    pub(in crate::app::flow) fn apply_ws_connected_event(
        &mut self,
        endpoint_name: String,
        address: String,
        cx: &mut Context<Self>,
    ) {
        self.gateway.status = format!(
            "{}: {} ({})",
            t!("gateway.status.connected"),
            endpoint_name.as_str(),
            address.as_str()
        );
        self.gateway.status_level = GatewayStatusLevel::Connected;
        self.gateway.connection_state = GatewayConnectionState::Connected;
        self.gateway.error = None;
        self.thread_list_loading = false;
        self.set_workspaces_loading(false);
        self.set_workspaces_error(None);
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
        self.refresh_workspace_list(cx);
        self.refresh_gateway_settings(cx);

        if matches!(
            self.main_content_view,
            MainContentView::Skills | MainContentView::SkillDetails
        ) {
            self.queue_skills_refresh();
        }

        self.enqueue_in_flight_turns_for_resume();
    }

    pub(in crate::app::flow) fn apply_ws_reconnecting_event(
        &mut self,
        endpoint_name: String,
        attempt: u32,
        delay_ms: u64,
        reason: String,
    ) {
        self.gateway.status = format!(
            "{} (attempt {attempt}, {} ms)",
            t!(
                "gateway.status.connecting_named",
                gateway_name = endpoint_name.as_str()
            ),
            delay_ms
        );
        self.gateway.status_level = GatewayStatusLevel::Degraded;
        self.gateway.connection_state = GatewayConnectionState::Reconnecting;
        self.gateway.error = Some(reason);
        self.thread_list_loading = false;
        self.set_workspaces_loading(false);
        if !self.should_resume_in_flight_turn() {
            self.set_active_thread_id(None);
        }
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
    }

    pub(in crate::app::flow) fn apply_ws_disconnected_event(
        &mut self,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        reason: String,
    ) {
        self.gateway.status = match endpoint_kind {
            GatewayEndpointKind::Local => t!(
                "gateway.status.local_stopped",
                gateway_address = address.as_str()
            )
            .to_string(),
            GatewayEndpointKind::Remote => t!(
                "gateway.status.remote_unavailable",
                gateway_name = endpoint_name.as_str(),
                gateway_address = address.as_str()
            )
            .to_string(),
        };
        self.gateway.status_level = GatewayStatusLevel::Failed;
        self.gateway.connection_state = GatewayConnectionState::Disconnected;
        self.gateway.error = Some(reason);
        self.thread_list_loading = false;
        self.set_workspaces_loading(false);
        if !self.should_resume_in_flight_turn() {
            self.set_active_thread_id(None);
        }
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
    }

    pub(in crate::app::flow) fn apply_ws_connect_failed_event(
        &mut self,
        endpoint_name: String,
        endpoint_kind: GatewayEndpointKind,
        address: String,
        error: String,
    ) {
        self.gateway.status = match endpoint_kind {
            GatewayEndpointKind::Local => t!(
                "gateway.status.local_stopped",
                gateway_address = address.as_str()
            )
            .to_string(),
            GatewayEndpointKind::Remote => t!(
                "gateway.status.remote_unavailable",
                gateway_name = endpoint_name.as_str(),
                gateway_address = address.as_str()
            )
            .to_string(),
        };
        self.gateway.status_level = GatewayStatusLevel::Failed;
        self.gateway.connection_state = GatewayConnectionState::Disconnected;
        self.gateway.error = Some(error);
        self.thread_list_loading = false;
        self.set_workspaces_loading(false);
        if !self.should_resume_in_flight_turn() {
            self.set_active_thread_id(None);
        }
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
    }
}
