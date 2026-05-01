use super::*;

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
            self.refresh_thread_list(cx);
            self.enqueue_in_flight_turns_for_resume();
            let _ = self.drive_turn_resume_queue(cx);
        }

        self.refresh_gateway_status();
        cx.notify();
    }

    fn refresh_gateway_status(&mut self) {
        if self.gateway.connecting {
            if self.gateway.status.is_empty() {
                self.gateway.status = t!("gateway.status.connecting").to_string();
            }
            self.gateway.status_level = GatewayStatusLevel::Neutral;
            self.gateway.connection_state = GatewayConnectionState::Connecting;
            return;
        }

        if let Some(runtime) = self.gateway.runtime.as_ref() {
            match runtime.active_gateway_state() {
                Ok(ActiveGatewayState::NotConfigured) => {
                    self.gateway.status = t!("gateway.status.not_configured").to_string();
                    self.gateway.status_level = GatewayStatusLevel::Degraded;
                    self.gateway.connection_state = GatewayConnectionState::Idle;
                }
                Ok(ActiveGatewayState::Connected) => {
                    if let Some(active) = runtime.active_gateway() {
                        self.gateway.status = format!(
                            "{}: {} ({})",
                            t!("gateway.status.connected"),
                            active.name.as_str(),
                            active.address.as_str()
                        );
                    } else {
                        self.gateway.status = t!("gateway.status.connected").to_string();
                    }
                    self.gateway.status_level = GatewayStatusLevel::Connected;
                    self.gateway.connection_state = GatewayConnectionState::Connected;
                }
                Ok(ActiveGatewayState::Unreachable) => {
                    if let Some(active) = runtime.active_gateway() {
                        self.gateway.status = match active.kind {
                            GatewayEndpointKind::Local => t!(
                                "gateway.status.local_stopped",
                                gateway_address = active.address.as_str()
                            )
                            .to_string(),
                            GatewayEndpointKind::Remote => t!(
                                "gateway.status.remote_unavailable",
                                gateway_name = active.name.as_str(),
                                gateway_address = active.address.as_str()
                            )
                            .to_string(),
                        };
                    } else {
                        self.gateway.status = t!("gateway.status.unavailable").to_string();
                    }
                    self.gateway.status_level = GatewayStatusLevel::Failed;
                    self.gateway.connection_state = GatewayConnectionState::Disconnected;
                }
                Ok(ActiveGatewayState::LocalAddressConflict) => {
                    if let Some(active) = runtime.active_gateway() {
                        self.gateway.status = t!(
                            "gateway.status.local_conflict_at",
                            gateway_address = active.address.as_str()
                        )
                        .to_string();
                    } else {
                        self.gateway.status = t!("gateway.status.local_conflict").to_string();
                    }
                    self.gateway.status_level = GatewayStatusLevel::Failed;
                    self.gateway.connection_state = GatewayConnectionState::Disconnected;
                }
                Err(error) => {
                    self.gateway.status =
                        t!("gateway.status.failed_check", error = format!("{error:#}")).to_string();
                    self.gateway.status_level = GatewayStatusLevel::Failed;
                    self.gateway.connection_state = GatewayConnectionState::Disconnected;
                }
            }
        } else if let Some(error) = self.gateway.error.as_ref() {
            self.gateway.status =
                t!("gateway.status.subsystem_failed", error = error.as_str()).to_string();
            self.gateway.status_level = GatewayStatusLevel::Failed;
            self.gateway.connection_state = GatewayConnectionState::Disconnected;
        } else {
            self.gateway.status = t!("gateway.status.subsystem_not_ready").to_string();
            self.gateway.status_level = GatewayStatusLevel::Neutral;
            self.gateway.connection_state = GatewayConnectionState::Idle;
        }
    }
}
