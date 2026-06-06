use super::*;
use pioneer_client::threads::start as thread_start;
use pioneer_client::workspaces::actions as workspace_actions;

impl PioneerDesktop {
    pub(in crate::app) fn default_thread_start_scope(&self) -> String {
        let preferred_workspace_id = self.preferred_workspace_id().map(str::to_owned);
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id)
            .map(str::to_owned);

        thread_start::default_thread_start_scope(
            preferred_workspace_id.as_deref(),
            runtime_workspace_id.as_deref(),
        )
    }

    pub(in crate::app::flow) fn persist_active_gateway_workspace_id(
        &mut self,
        workspace_id: String,
    ) {
        let Some(runtime) = self.gateway.runtime.as_mut() else {
            return;
        };
        let Some(plan) = workspace_actions::plan_active_gateway_workspace_persist(
            runtime.active_gateway_id(),
            workspace_id,
        ) else {
            return;
        };
        let gateway_id = plan.gateway_id;
        let workspace_id = plan.workspace_id;

        if let Err(error) =
            runtime.set_gateway_workspace_id(gateway_id.as_str(), Some(workspace_id))
        {
            warn!(
                gateway_id = gateway_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist gateway workspace id"
            );
        }
    }
}
