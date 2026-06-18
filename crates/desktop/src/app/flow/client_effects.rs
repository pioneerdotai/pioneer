use super::*;
use pioneer_client::notifications::effects::{
    self as client_effects, ClientEffect, ClientEffectSink,
};

pub(in crate::app::flow) fn execute_desktop_client_effects(
    app: &mut PioneerDesktop,
    effects: Vec<ClientEffect>,
    cx: &mut Context<PioneerDesktop>,
) {
    let mut sink = DesktopClientEffectSink { app, cx };
    client_effects::execute_client_effects(&mut sink, effects);
}

pub(in crate::app::flow) fn execute_gateway_command_client_effects(
    ws_sender: &crate::gateway::GatewayWsCommandSender,
    effects: Vec<ClientEffect>,
) {
    let mut sink = GatewayCommandClientEffectSink { ws_sender };
    client_effects::execute_client_effects(&mut sink, effects);
}

struct DesktopClientEffectSink<'a, 'cx> {
    app: &'a mut PioneerDesktop,
    cx: &'a mut Context<'cx, PioneerDesktop>,
}

impl ClientEffectSink for DesktopClientEffectSink<'_, '_> {
    fn refresh_workspace_list(&mut self) {
        self.app.refresh_workspace_list(self.cx);
    }

    fn refresh_gateway_settings(&mut self) {
        self.app.refresh_gateway_settings(self.cx);
    }

    fn refresh_provider_lists(&mut self) {
        if self.app.active_workspace_id().is_none() {
            return;
        }
        self.app.refresh_configured_providers(self.cx);
        self.app.refresh_cli_providers_auto(self.cx);
    }

    fn queue_skills_refresh(&mut self) {
        self.app.queue_skills_refresh();
    }

    fn enqueue_in_flight_turns_for_resume(&mut self) {
        self.app.enqueue_in_flight_turns_for_resume();
    }

    fn unsubscribe_threads(&mut self, thread_ids: Vec<String>) {
        for thread_id in thread_ids {
            let _ = self
                .app
                .gateway
                .ws_command_sender
                .thread_unsubscribe(thread_id);
        }
    }
}

struct GatewayCommandClientEffectSink<'a> {
    ws_sender: &'a crate::gateway::GatewayWsCommandSender,
}

impl ClientEffectSink for GatewayCommandClientEffectSink<'_> {
    fn refresh_workspace_list(&mut self) {}

    fn refresh_gateway_settings(&mut self) {}

    fn refresh_provider_lists(&mut self) {}

    fn queue_skills_refresh(&mut self) {}

    fn enqueue_in_flight_turns_for_resume(&mut self) {}

    fn unsubscribe_threads(&mut self, thread_ids: Vec<String>) {
        for thread_id in thread_ids {
            let _ = self.ws_sender.thread_unsubscribe(thread_id);
        }
    }
}
