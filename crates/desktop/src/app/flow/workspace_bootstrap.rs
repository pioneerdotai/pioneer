use super::*;
use pioneer_client::threads::start as thread_start;
use pioneer_client::workspaces::actions as workspace_actions;

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
    match thread_start::plan_workspace_id_for_thread_start(requested_workspace_id) {
        thread_start::ThreadStartWorkspaceResolution::Requested(workspace_id) => Ok(workspace_id),
        thread_start::ThreadStartWorkspaceResolution::LoadDefaultWorkspace => {
            let response = ws_sender.workspace_default()?;
            thread_start::normalize_default_workspace_id_for_thread_start(response.workspace.id)
                .ok_or_else(|| anyhow!("workspace/default returned an empty workspace id"))
        }
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
                        let mut workspace_id = match workspace_actions::plan_workspace_bootstrap_after_list(
                            persisted_workspace_id.as_deref(),
                            workspaces.as_slice(),
                        ) {
                            workspace_actions::WorkspaceBootstrapAfterList::SelectWorkspace {
                                workspace_id,
                            } => workspace_id,
                            workspace_actions::WorkspaceBootstrapAfterList::LoadDefaultWorkspace => {
                                let workspace = ws_sender.workspace_default()?.workspace;
                                workspace_actions::apply_workspace_default_for_bootstrap(
                                    &mut workspaces,
                                    workspace,
                                )
                                .ok_or_else(|| {
                                    anyhow!("workspace/default returned an empty workspace id")
                                })?
                            }
                        };

                        let response = ws_sender.workspace_select(
                            workspace_actions::workspace_select_params(workspace_id.clone(), false),
                        )?;
                        workspace_id = workspace_actions::apply_workspace_select_response_to_catalog(
                            &mut workspaces,
                            response.workspace,
                        );

                        Ok::<_, anyhow::Error>(WorkspaceBootstrapOutcome {
                            workspace_id,
                            workspaces,
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
