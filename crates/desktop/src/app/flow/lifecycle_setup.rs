use super::*;
use pioneer_client::notifications::effects::ClientEffect;
use pioneer_client::state::reducers as client_state_reducers;

impl PioneerDesktop {
    pub(in crate::app::flow) fn prepare_gateway_switch(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Vec<ClientEffect> {
        let plan = client_state_reducers::plan_gateway_switch_cleanup(
            &self.thread_coordinators,
            self.current_active_thread_id(),
        );
        self.thread_list_loading = plan.thread_list_loading;
        if plan.clear_active_thread {
            self.set_active_thread_id(None);
        }
        if plan.clear_thread_conversations {
            self.clear_thread_conversations();
        }
        if plan.rebuild_thread_tree {
            self.rebuild_sidebar_tree_state(cx);
        }
        if plan.reset_thread_start {
            self.reset_thread_start_state();
        }
        if plan.clear_thread_start_queue {
            self.clear_thread_start_queue();
        }
        if plan.clear_turn_resume_queue {
            self.clear_turn_resume_queue();
        }
        if plan.clear_gateway_settings {
            self.gateway.settings = None;
        }
        self.providers.clear_for_gateway_switch();
        self.gateway.settings_loading = plan.gateway_settings_loading;
        self.gateway.settings_error = plan.gateway_settings_error;
        self.voice_input_action_error = None;
        plan.effects
    }

    pub(crate) fn is_gateway_setup_required(&self) -> bool {
        let runtime_setup_required = self
            .gateway
            .runtime
            .as_ref()
            .map(GatewayRuntime::setup_required);
        pioneer_client::gateway::runtime::gateway_setup_required(
            self.gateway.bootstrap_complete,
            runtime_setup_required,
        )
    }
}
