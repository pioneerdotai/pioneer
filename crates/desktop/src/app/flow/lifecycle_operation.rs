use super::*;
use pioneer_client::state::reducers::{
    GatewayOperationFinishOutcome, GatewayOperationSuccessInfo, GatewayStatusEndpoint,
    GatewayStatusInput, GatewayStatusProjection, GatewayStatusTextUpdate, project_gateway_status,
    reduce_gateway_operation_begin, reduce_gateway_operation_finish,
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
        let reduction = reduce_gateway_operation_begin();
        self.gateway.connecting = reduction.connecting;
        self.gateway.connection_state = reduction.connection_state;
        self.gateway.setup_action = setup_action;
        self.gateway.error = reduction.gateway_error;
        self.gateway.status = status.into();
        self.gateway.status_level = reduction.status_level;
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

        let mut schedule_session_refresh = false;
        let outcome = match result {
            Ok(success) => {
                schedule_session_refresh = success.ws_connection_id.is_some();
                self.gateway.runtime = Some(success.runtime);
                GatewayOperationFinishOutcome::Success(GatewayOperationSuccessInfo {
                    ws_connection_id: success.ws_connection_id,
                    ws_connected_ready: success.ws_connected_ready,
                })
            }
            Err(error) => GatewayOperationFinishOutcome::Failure {
                error: format!("{error:#}"),
            },
        };
        let reduction = reduce_gateway_operation_finish(outcome);

        self.gateway.connecting = reduction.connecting;
        if reduction.clear_setup_action {
            self.gateway.setup_action = None;
        }
        self.gateway.bootstrap_complete = reduction.bootstrap_complete;
        self.gateway.ws_connection_id = reduction.ws_connection_id;
        self.gateway.error = reduction.gateway_error;
        self.gateway.connection_state = reduction.connection_state;
        if let Some(status) = reduction.status {
            self.gateway.status = super::ws_events_connection::gateway_status_message_text(&status);
        }
        if let Some(status_level) = reduction.status_level {
            self.gateway.status_level = status_level;
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
        if reduction.disconnect_ws {
            let _ = self.gateway.ws_command_sender.disconnect();
        }
        if self.gateway.ws_connection_id.is_some() {
            self.replay_deferred_gateway_ws_events(cx);
        } else {
            self.discard_deferred_gateway_ws_events();
        }

        execute_desktop_client_effects(self, reduction.effects, cx);
        if schedule_session_refresh {
            self.schedule_gateway_session_refresh(cx);
        }
        if reduction.drive_turn_resume_queue {
            let _ = self.drive_turn_resume_queue(cx);
        }

        if !reduction.refresh_gateway_status {
            cx.notify();
            return;
        }

        self.refresh_gateway_status();
        if reduction.sync_gateway_setup_form_state {
            self.sync_gateway_setup_form_state(Some(&self.gateway_setup_form_state), cx);
        }
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
