use super::*;
use pioneer_client::threads::start as thread_start;

pub(crate) fn default_user_command_bin_dir_label() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        return r"%LOCALAPPDATA%\Pioneer\bin";
    }

    #[cfg(not(target_os = "windows"))]
    {
        "~/.local/bin"
    }
}

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
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id)
            .map(str::to_owned)
    }
}
