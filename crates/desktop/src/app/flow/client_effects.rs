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

struct DesktopClientEffectSink<'a, 'cx> {
    app: &'a mut PioneerDesktop,
    cx: &'a mut Context<'cx, PioneerDesktop>,
}

impl ClientEffectSink for DesktopClientEffectSink<'_, '_> {
    fn refresh_workspace_list(&mut self) {}

    fn refresh_gateway_settings(&mut self) {
        self.app.refresh_gateway_settings(self.cx);
    }

    fn refresh_provider_lists(&mut self) {
        if self.app.active_workspace_id().is_none() {
            return;
        }
        self.app.refresh_configured_providers(self.cx);
        self.app.load_cli_provider_snapshot(self.cx);
    }

    fn queue_skills_refresh(&mut self) {
        if let Some(workspace) = self.app.active_workspace_id() {
            self.app
                .gateway
                .client_runtime
                .client_core()
                .refresh_skills(workspace);
        }
    }

    fn enqueue_in_flight_turns_for_resume(&mut self) {
        self.app.enqueue_in_flight_turns_for_resume();
    }

    fn unsubscribe_threads(&mut self, thread_ids: Vec<String>) {
        for thread_id in thread_ids {
            let _ = self
                .app
                .gateway
                .client_runtime
                .ws_command_sender()
                .thread_unsubscribe(thread_id);
        }
    }
}
