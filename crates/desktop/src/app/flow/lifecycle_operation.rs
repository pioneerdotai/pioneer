use super::*;
use pioneer_client::state::reducers::{
    GatewayStatusEndpoint, GatewayStatusInput, GatewayStatusProjection, GatewayStatusTextUpdate,
    project_gateway_status,
};

impl PioneerDesktop {
    pub(in crate::app::flow) fn begin_gateway_operation(
        &mut self,
        status: impl Into<String>,
        setup_action: Option<GatewaySetupAction>,
        cx: &mut Context<Self>,
    ) -> Option<u64> {
        if self.gateway.connecting {
            return None;
        }

        let operation_epoch = self.next_gateway_connection_epoch();
        self.gateway.connecting = true;
        self.gateway.connection_state = GatewayConnectionState::Connecting;
        self.gateway.setup_action = setup_action;
        self.gateway.error = None;
        self.gateway.status = status.into();
        self.gateway.status_level = GatewayStatusLevel::Neutral;
        cx.notify();
        Some(operation_epoch)
    }

    pub(in crate::app::flow) fn update_gateway_operation_status(
        &mut self,
        operation_epoch: u64,
        status: String,
        cx: &mut Context<Self>,
    ) {
        if !should_apply_gateway_operation_result(self.gateway.connection_epoch, operation_epoch) {
            return;
        }

        if !self.gateway.connecting {
            return;
        }

        self.gateway.status = status;
        self.gateway.status_level = GatewayStatusLevel::Neutral;
        self.gateway.connection_state = GatewayConnectionState::Connecting;
        cx.notify();
    }

    pub(in crate::app::flow) fn finish_gateway_operation(
        &mut self,
        operation_epoch: u64,
        result: Result<GatewayOperationSuccess, anyhow::Error>,
        cx: &mut Context<Self>,
    ) {
        if !should_apply_gateway_operation_result(self.gateway.connection_epoch, operation_epoch) {
            return;
        }

        self.gateway.connecting = false;
        self.gateway.setup_action = None;
        self.gateway.bootstrap_complete = true;

        match result {
            Ok(success) => {
                self.gateway.runtime = Some(success.runtime);
                self.gateway.ws_connection_id = success.ws_connection_id;
                self.gateway.error = None;
                self.reset_thread_start_state();
                self.clear_thread_start_queue();
                self.clear_turn_resume_queue();
                if self.gateway.ws_connection_id.is_none() {
                    self.set_active_thread_id(None);
                }

                if self.gateway.ws_connection_id.is_none() {
                    let _ = self.gateway.ws_command_sender.disconnect();
                }

                if self.gateway.ws_connection_id.is_some() && !success.ws_connected_ready {
                    self.gateway.status = t!("gateway.status.connecting").to_string();
                    self.gateway.status_level = GatewayStatusLevel::Neutral;
                    self.gateway.connection_state = GatewayConnectionState::Connecting;
                    cx.notify();
                    return;
                }

                self.gateway.connection_state = if self.gateway.ws_connection_id.is_some() {
                    GatewayConnectionState::Connected
                } else {
                    GatewayConnectionState::Idle
                };
            }
            Err(error) => {
                self.gateway.ws_connection_id = None;
                let _ = self.gateway.ws_command_sender.disconnect();
                self.gateway.connection_state = GatewayConnectionState::Disconnected;
                self.gateway.error = Some(format!("{error:#}"));
                self.set_active_thread_id(None);
                self.reset_thread_start_state();
                self.clear_thread_start_queue();
                self.clear_turn_resume_queue();
            }
        }

        if self.gateway.connection_state == GatewayConnectionState::Connected {
            self.refresh_workspace_list(cx);
            self.refresh_gateway_settings(cx);
            self.enqueue_in_flight_turns_for_resume();
            let _ = self.drive_turn_resume_queue(cx);
        }

        self.refresh_gateway_status();
        self.sync_gateway_setup_form_state(Some(&self.gateway_setup_form_state), cx);
        cx.notify();
    }

    pub(in crate::app::flow) fn refresh_gateway_status(&mut self) {
        let runtime_state = if self.gateway.connecting {
            None
        } else {
            self.gateway.runtime.as_ref().map(|runtime| {
                runtime
                    .active_gateway_state()
                    .map_err(|error| format!("{error:#}"))
            })
        };
        let active_endpoint = if self.gateway.connecting {
            None
        } else {
            self.gateway
                .runtime
                .as_ref()
                .and_then(GatewayRuntime::active_gateway)
                .map(|active| {
                    GatewayStatusEndpoint::new(
                        active.name.clone(),
                        active.address.clone(),
                        active.kind,
                    )
                })
        };
        let has_ready_ws_connection = gateway_has_ready_ws_connection(
            self.gateway.connection_state,
            self.gateway.ws_connection_id,
        );

        let projection = project_gateway_status(GatewayStatusInput {
            connecting: self.gateway.connecting,
            current_status_is_empty: self.gateway.status.is_empty(),
            runtime_state,
            active_endpoint,
            has_ready_ws_connection,
            gateway_error: self.gateway.error.clone(),
        });

        self.apply_gateway_status_projection(projection);
    }

    fn apply_gateway_status_projection(&mut self, projection: GatewayStatusProjection) {
        if let GatewayStatusTextUpdate::Set(status) = projection.status {
            self.gateway.status = super::ws_events_connection::gateway_status_message_text(&status);
        }
        self.gateway.status_level = projection.status_level;
        self.gateway.connection_state = projection.connection_state;
        if projection.clear_gateway_error {
            self.gateway.error = None;
        }
    }
}
