use super::*;
use pioneer_client::threads::start as thread_start;

impl PioneerDesktop {
    pub(in crate::app) fn default_thread_start_scope(&self) -> String {
        let preferred_workspace_id = self.preferred_workspace_id().map(str::to_owned);
        let runtime_workspace_id = self
            .gateway
            .client_runtime
            .client_core()
            .gateway_registry()
            .as_ref()
            .and_then(pioneer_client::gateway::types::GatewayRegistry::active_workspace_id)
            .map(str::to_owned);

        thread_start::default_thread_start_scope(
            preferred_workspace_id.as_deref(),
            runtime_workspace_id.as_deref(),
        )
    }

    pub(in crate::app) fn persist_active_gateway_workspace_id(&mut self, workspace_id: String) {
        let core = self.gateway.client_runtime.client_core();
        let Some(endpoint) = core.active_gateway_endpoint() else {
            return;
        };
        core.onboarding_intent(
            pioneer_client::gateway::onboarding_runtime::OnboardingIntent::SetWorkspace {
                endpoint_id: endpoint.id,
                workspace_id: Some(workspace_id),
            },
        );
    }
    pub(in crate::app::flow) fn clear_persisted_active_gateway_workspace_id(&mut self) {
        let core = self.gateway.client_runtime.client_core();
        let Some(endpoint) = core.active_gateway_endpoint() else {
            return;
        };
        core.onboarding_intent(
            pioneer_client::gateway::onboarding_runtime::OnboardingIntent::SetWorkspace {
                endpoint_id: endpoint.id,
                workspace_id: None,
            },
        );
    }
}
