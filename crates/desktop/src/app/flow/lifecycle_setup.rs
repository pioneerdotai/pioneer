use super::*;

impl PioneerDesktop {
    fn collect_known_thread_ids_for_unsubscribe(&self) -> Vec<String> {
        let mut known_thread_ids = std::collections::HashSet::new();

        for thread_id in self.thread_coordinators.keys() {
            known_thread_ids.insert(thread_id.to_owned());
        }

        if let Some(thread_id) = self.current_active_thread_id() {
            known_thread_ids.insert(thread_id.to_owned());
        }

        known_thread_ids.into_iter().collect()
    }

    pub(in crate::app::flow) fn prepare_gateway_switch(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Vec<String> {
        let thread_ids = self.collect_known_thread_ids_for_unsubscribe();
        self.thread_list_loading = false;
        self.set_active_thread_id(None);
        self.clear_thread_conversations();
        self.rebuild_sidebar_tree_state(cx);
        self.reset_thread_start_state();
        self.clear_thread_start_queue();
        self.clear_turn_resume_queue();
        self.gateway.settings = None;
        self.gateway.settings_loading = false;
        self.gateway.settings_error = None;
        thread_ids
    }

    pub(crate) fn is_gateway_setup_required(&self) -> bool {
        if !self.gateway.bootstrap_complete {
            return false;
        }

        self.gateway
            .runtime
            .as_ref()
            .is_none_or(GatewayRuntime::setup_required)
    }
}
