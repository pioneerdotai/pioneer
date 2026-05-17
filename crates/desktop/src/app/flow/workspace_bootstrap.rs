use super::*;

struct WorkspaceBootstrapOutcome {
    workspace_id: String,
    workspaces: Vec<Workspace>,
}

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

pub(crate) fn upsert_workspace_catalog_item(workspaces: &mut Vec<Workspace>, workspace: Workspace) {
    if let Some(existing) = workspaces
        .iter_mut()
        .find(|candidate| candidate.id == workspace.id)
    {
        *existing = workspace;
    } else {
        workspaces.push(workspace);
    }
}

impl PioneerDesktop {
    pub(crate) fn refresh_workspace_list(&mut self, cx: &mut Context<Self>) {
        if self.workspaces_loading() {
            return;
        }

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };

        self.set_workspaces_loading(true);
        self.set_workspaces_error(None);

        let persisted_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id)
            .map(str::to_owned);
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let mut workspaces = ws_sender.workspace_list()?.workspaces;
                        let mut workspace_id = resolve_active_workspace_id(
                            persisted_workspace_id.as_deref(),
                            workspaces.as_slice(),
                        )
                        .map(str::to_owned);

                        if workspace_id.is_none() {
                            let workspace = ws_sender.workspace_default()?.workspace;
                            workspace_id = normalize_workspace_id(Some(workspace.id.clone()));
                            upsert_workspace_catalog_item(&mut workspaces, workspace);
                        }

                        let workspace_id = workspace_id.ok_or_else(|| {
                            anyhow!("workspace bootstrap could not resolve an active workspace")
                        })?;
                        let response = ws_sender.workspace_select(WorkspaceSelectParams {
                            workspace_id: workspace_id.clone(),
                            make_current: false,
                        })?;
                        upsert_workspace_catalog_item(&mut workspaces, response.workspace);

                        Ok::<_, anyhow::Error>(WorkspaceBootstrapOutcome {
                            workspace_id,
                            workspaces,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.set_workspaces_loading(false);

                    match result {
                        Ok(outcome) => {
                            let workspace_id = outcome.workspace_id;
                            view.set_workspaces(outcome.workspaces);
                            view.set_workspaces_error(None);
                            view.persist_active_gateway_workspace_id(workspace_id.clone());
                            view.set_preferred_workspace_id(Some(workspace_id.clone()));
                            view.load_thread_folder_expansion_for_workspace(
                                workspace_id.as_str(),
                                cx,
                            );
                            view.refresh_thread_list(cx);
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            warn!(
                                error = message.as_str(),
                                "failed to bootstrap workspace list"
                            );
                            view.set_workspaces_error(Some(message));
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }
}
