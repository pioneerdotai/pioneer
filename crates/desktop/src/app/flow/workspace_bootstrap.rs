use super::*;
use pioneer_client::threads::start as thread_start;

pub(crate) fn resolve_workspace_id_for_thread_start(
    ws_sender: &crate::gateway::GatewayWsCommandSender,
    requested_workspace_id: Option<String>,
) -> anyhow::Result<String> {
    match thread_start::plan_workspace_id_for_thread_start(requested_workspace_id) {
        thread_start::ThreadStartWorkspaceResolution::Requested(workspace_id) => Ok(workspace_id),
        thread_start::ThreadStartWorkspaceResolution::LoadDefaultWorkspace => {
            let response = ws_sender.workspace_default()?;
            thread_start::normalize_default_workspace_id_for_thread_start(response.workspace.id)
                .ok_or_else(|| anyhow!("{}", t!("workspace.error.default_workspace_empty")))
        }
    }
}

impl PioneerDesktop {
    pub(crate) fn persisted_workspace_preference(&self) -> Option<String> {
        self.gateway
            .client_runtime
            .client_core()
            .gateway_registry()
            .as_ref()
            .and_then(pioneer_client::gateway::types::GatewayRegistry::active_workspace_id)
            .map(str::to_owned)
    }
}
