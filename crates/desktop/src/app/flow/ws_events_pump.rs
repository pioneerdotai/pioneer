use super::*;
use pioneer_client::runtime::ClientRuntimePostEventSink;

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
        let mut events_applied = false;

        for event in self
            .gateway
            .client_runtime
            .drain_applicable_ws_events(self.gateway.ws_connection_id, first_event)
        {
            self.apply_gateway_ws_event(event, cx);
            events_applied = true;
        }

        let outcome = {
            let client_runtime = self.gateway.client_runtime.clone();
            let mut sink = DesktopPostEventSink { app: self, cx };
            client_runtime.drive_post_event_batch(events_applied, &mut sink)
        };

        if outcome.should_notify() {
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

struct DesktopPostEventSink<'a, 'cx> {
    app: &'a mut PioneerDesktop,
    cx: &'a mut Context<'cx, PioneerDesktop>,
}

impl ClientRuntimePostEventSink for DesktopPostEventSink<'_, '_> {
    fn refresh_thread_list_if_requested(&mut self) -> bool {
        if !self.app.take_thread_list_refresh_request() {
            return false;
        }
        self.app.refresh_thread_list(self.cx);
        true
    }

    fn refresh_skills_if_requested(&mut self) -> bool {
        if !self.app.take_skills_refresh_request() {
            return false;
        }
        self.app.refresh_installed_skills(self.cx);
        true
    }

    fn refresh_mcp_if_requested(&mut self) -> bool {
        if !self.app.take_mcp_refresh_request() {
            return false;
        }
        self.app.refresh_mcp_servers(self.cx);
        true
    }

    fn refresh_mcp_details_if_requested(&mut self) -> bool {
        if !self.app.take_mcp_details_refresh_request() {
            return false;
        }
        self.app.refresh_mcp_server_details(self.cx);
        true
    }

    fn drive_thread_start_queue(&mut self) -> bool {
        self.app.drive_thread_start_queue(self.cx)
    }

    fn drive_turn_resume_queue(&mut self) -> bool {
        self.app.drive_turn_resume_queue(self.cx)
    }

    fn tick_thread_conversations(&mut self) -> bool {
        self.app.tick_thread_conversations()
    }
}
