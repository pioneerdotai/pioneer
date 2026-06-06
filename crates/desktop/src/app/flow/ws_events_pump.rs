use super::*;

impl PioneerDesktop {
    pub(crate) fn start_gateway_ws_event_pump(&self, cx: &mut Context<Self>) {
        let ws_client = self.gateway.ws_client.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let ws_client = ws_client.clone();

            async move {
                loop {
                    let first_event = cx
                        .background_spawn({
                            let ws_client = ws_client.clone();
                            async move { ws_client.recv_event() }
                        })
                        .await;
                    let should_break = first_event.is_none();

                    let updated = this.update(&mut cx, |view, cx| {
                        view.handle_gateway_ws_events(first_event, cx);
                    });

                    if updated.is_err() {
                        break;
                    }

                    if should_break {
                        break;
                    }
                }
            }
        })
        .detach();
    }

    pub(crate) fn handle_gateway_ws_events(
        &mut self,
        first_event: Option<GatewayWsEvent>,
        cx: &mut Context<Self>,
    ) {
        let mut changed = false;

        for event in first_event
            .into_iter()
            .chain(self.gateway.ws_client.drain_events())
        {
            if !should_apply_ws_event(self.gateway.ws_connection_id, &event) {
                continue;
            }

            self.apply_gateway_ws_event(event, cx);
            changed = true;
        }

        if self.take_thread_list_refresh_request() {
            self.refresh_thread_list(cx);
            changed = true;
        }

        if self.take_skills_refresh_request() {
            self.refresh_installed_skills(cx);
            changed = true;
        }

        if self.take_mcp_refresh_request() {
            self.refresh_mcp_servers(cx);
            changed = true;
        }

        if self.take_mcp_details_refresh_request() {
            self.refresh_mcp_server_details(cx);
            changed = true;
        }

        let started_or_retried = self.drive_thread_start_queue(cx);
        let resumed = self.drive_turn_resume_queue(cx);
        let finalized_completion = self.tick_thread_conversations();

        if changed || started_or_retried || resumed || finalized_completion {
            cx.notify();
        }
    }

    pub(in crate::app::flow) fn next_gateway_connection_epoch(&mut self) -> u64 {
        self.gateway.connection_epoch =
            pioneer_client::gateway::runtime::next_gateway_operation_epoch(
                self.gateway.connection_epoch,
            );
        self.gateway.connection_epoch
    }

    pub(in crate::app::flow) fn apply_gateway_ws_event(
        &mut self,
        event: GatewayWsEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            GatewayWsEvent::Connecting {
                endpoint_id: _endpoint_id,
                endpoint_name,
                endpoint_kind,
                ..
            } => self.apply_ws_connecting_event(endpoint_name, endpoint_kind),
            GatewayWsEvent::Connected {
                endpoint_id: _endpoint_id,
                endpoint_name,
                address,
                ..
            } => self.apply_ws_connected_event(endpoint_name, address, cx),
            GatewayWsEvent::Reconnecting {
                endpoint_id: _endpoint_id,
                endpoint_name,
                attempt,
                delay_ms,
                reason,
                ..
            } => self.apply_ws_reconnecting_event(endpoint_name, attempt, delay_ms, reason),
            GatewayWsEvent::Disconnected {
                endpoint_id: _endpoint_id,
                endpoint_name,
                endpoint_kind,
                address,
                reason,
                ..
            } => self.apply_ws_disconnected_event(endpoint_name, endpoint_kind, address, reason),
            GatewayWsEvent::ConnectFailed {
                endpoint_id: _endpoint_id,
                endpoint_name,
                endpoint_kind,
                address,
                error,
                ..
            } => self.apply_ws_connect_failed_event(endpoint_name, endpoint_kind, address, error),
            GatewayWsEvent::Notification { notification, .. } => {
                self.apply_gateway_notification(notification, cx);
            }
        }
    }
}
