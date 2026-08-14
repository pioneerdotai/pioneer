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

fn workspace_id_for_connection_bootstrap(
    active_thread_workspace_id: Option<&str>,
    persisted_workspace_id: Option<&str>,
) -> Option<String> {
    active_thread_workspace_id
        .or(persisted_workspace_id)
        .map(str::to_owned)
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
        self.clear_task_user_notification_inbox();
        self.refresh_current_principal(cx);
        // The connection effect can run before workspace bootstrap establishes a scope,
        // so ensure both API and CLI runtime summaries are loaded once that scope is available.
        self.refresh_configured_providers(cx);
        self.refresh_cli_providers_auto(cx);
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

        // A refreshed access token creates a new server-side connection. Keep
        // that connection scoped to the thread which is still open in the UI;
        // the registry value can lag behind an already-open legacy thread.
        let active_thread_workspace_id = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id));
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id);
        let persisted_workspace_id =
            workspace_id_for_connection_bootstrap(active_thread_workspace_id, runtime_workspace_id);
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

#[cfg(test)]
mod tests {
    use super::workspace_id_for_connection_bootstrap;

    #[test]
    fn connection_bootstrap_keeps_the_open_threads_workspace() {
        assert_eq!(
            workspace_id_for_connection_bootstrap(Some("thread_ws"), Some("registry_ws"))
                .as_deref(),
            Some("thread_ws")
        );
        assert_eq!(
            workspace_id_for_connection_bootstrap(None, Some("registry_ws")).as_deref(),
            Some("registry_ws")
        );
    }
}
