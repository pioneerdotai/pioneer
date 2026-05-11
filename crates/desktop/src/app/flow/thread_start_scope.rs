use super::*;

impl PioneerDesktop {
    pub(crate) fn thread_start_scope(workspace_id: Option<&str>) -> Option<String> {
        normalize_workspace_id(workspace_id.map(str::to_owned))
    }

    pub(in crate::app) fn default_thread_start_scope(&self) -> String {
        let preferred_workspace_id = self.preferred_workspace_id().map(str::to_owned);
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id)
            .map(str::to_owned);

        Self::thread_start_scope(
            preferred_workspace_id
                .as_deref()
                .or(runtime_workspace_id.as_deref()),
        )
        .unwrap_or_else(|| WORKSPACE_START_SCOPE_BOOTSTRAP.to_owned())
    }

    pub(in crate::app::flow) fn persist_active_gateway_workspace_id(
        &mut self,
        workspace_id: String,
    ) {
        let Some(runtime) = self.gateway.runtime.as_mut() else {
            return;
        };
        let Some(active_gateway_id) = runtime.active_gateway_id().map(str::to_owned) else {
            return;
        };

        if let Err(error) =
            runtime.set_gateway_workspace_id(active_gateway_id.as_str(), Some(workspace_id))
        {
            warn!(
                gateway_id = active_gateway_id.as_str(),
                error = %format!("{error:#}"),
                "failed to persist gateway workspace id"
            );
        }
    }
}
