use super::*;

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
    if let Some(workspace_id) = normalize_workspace_id(requested_workspace_id) {
        return Ok(workspace_id);
    }

    let response = ws_sender.workspace_default()?;
    normalize_workspace_id(Some(response.workspace.id))
        .ok_or_else(|| anyhow!("workspace/default returned an empty workspace id"))
}

pub(crate) fn normalize_workspace_id(value: Option<String>) -> Option<String> {
    value.and_then(|workspace_id| {
        let trimmed = workspace_id.trim();
        (!trimmed.is_empty()).then_some(trimmed.to_owned())
    })
}
