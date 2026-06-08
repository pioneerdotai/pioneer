use super::*;

impl PioneerDesktop {
    pub(crate) fn start_gateway_ws_event_pump(&self, cx: &mut Context<Self>) {
        let client_runtime = self.gateway.client_runtime.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let client_runtime = client_runtime.clone();

            async move {
                loop {
                    let first_event = cx
                        .background_spawn({
                            let client_runtime = client_runtime.clone();
                            async move { client_runtime.recv_ws_event() }
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

        for event in self
            .gateway
            .client_runtime
            .drain_applicable_ws_events(self.gateway.ws_connection_id, first_event)
        {
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
        let context = pioneer_client::runtime::ClientRuntimeWsEventContext {
            queue_skills_refresh: matches!(
                self.main_content_view,
                MainContentView::Skills | MainContentView::SkillDetails
            ),
            should_resume_in_flight_turn: self.should_resume_in_flight_turn(),
        };
        match self.gateway.client_runtime.reduce_ws_event(event, context) {
            pioneer_client::runtime::ClientRuntimeWsEvent::Connection(reduction) => {
                self.apply_gateway_connection_reduction(reduction, Some(cx));
            }
            pioneer_client::runtime::ClientRuntimeWsEvent::Notification(notification) => {
                self.apply_gateway_notification(notification, cx);
            }
        }
    }
}
