use super::*;
use pioneer_client::threads::start as thread_start;
use pioneer_client::workspaces::{actions as workspace_actions, bootstrap as workspace_bootstrap};

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
    fn apply_workspace_bootstrap_success_reduction(
        &mut self,
        reduction: workspace_actions::WorkspaceBootstrapSuccessReduction,
        cx: &mut Context<Self>,
    ) {
        let workspace_id = reduction.selected.workspace_id.clone();
        self.set_workspaces(reduction.workspaces);
        if reduction.clear_workspaces_error {
            self.set_workspaces_error(None);
        }
        self.persist_active_gateway_workspace_id(
            reduction
                .selected
                .persist_active_gateway_workspace_id
                .clone(),
        );
        self.set_preferred_workspace_id(Some(reduction.selected.set_preferred_workspace_id));
        self.load_thread_folder_expansion_for_workspace(workspace_id.as_str(), cx);
        if reduction.selected.refresh_thread_list {
            self.refresh_thread_list(cx);
        }
    }

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
                        workspace_bootstrap::bootstrap_workspace_catalog(
                            &ws_sender,
                            workspace_bootstrap::WorkspaceBootstrapRequest {
                                persisted_workspace_id,
                            },
                        )
                        .map_err(|error| match error {
                            workspace_bootstrap::WorkspaceBootstrapError::DefaultWorkspaceEmpty => {
                                anyhow!("{}", t!("workspace.error.default_workspace_empty"))
                            }
                            workspace_bootstrap::WorkspaceBootstrapError::Transport(error) => error,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !workspace_actions::workspace_action_result_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    view.set_workspaces_loading(false);

                    match result {
                        Ok(reduction) => {
                            view.apply_workspace_bootstrap_success_reduction(reduction, cx);
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
