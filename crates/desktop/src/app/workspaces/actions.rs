use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::{prelude::*, *};
use pioneer_client::workspaces::actions as workspace_actions;
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn create_workspace_from_dialog(
        &mut self,
        name: String,
        cx: &mut Context<Self>,
    ) -> bool {
        let create_plan =
            workspace_actions::plan_workspace_create(name, self.workspace_action_in_progress());
        let create_params = match create_plan {
            workspace_actions::WorkspaceCreatePlan::Request(params) => params,
            workspace_actions::WorkspaceCreatePlan::Skip(_) => return false,
        };

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            return false;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            return false;
        };

        self.set_workspace_action_in_progress(true);
        self.set_workspaces_error(None);

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let create_params = create_params.clone();

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.workspace_create(create_params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !workspace_actions::workspace_action_result_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    view.set_workspace_action_in_progress(false);

                    match result {
                        Ok(response) => {
                            let workspaces = std::mem::take(&mut view.workspaces);
                            let reduction = workspace_actions::reduce_workspace_create_success(
                                workspaces,
                                response.workspace,
                            );
                            view.set_workspaces(reduction.workspaces);
                            if reduction.clear_workspaces_error {
                                view.set_workspaces_error(None);
                            }
                            view.switch_workspace_from_ui(reduction.switch_workspace_id, cx);
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            warn!(error = message.as_str(), "failed to create workspace");
                            view.set_workspaces_error(Some(message));
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();

        true
    }

    pub(in crate::app) fn rename_workspace_from_dialog(
        &mut self,
        workspace_id: String,
        name: String,
        cx: &mut Context<Self>,
    ) -> bool {
        let action_in_progress = self.workspace_action_in_progress();
        let current_workspace = self.workspace_by_id(workspace_id.as_str());
        let rename_plan = workspace_actions::plan_workspace_rename(
            workspace_id,
            name,
            action_in_progress,
            current_workspace,
        );
        let update_params = match rename_plan {
            workspace_actions::WorkspaceRenamePlan::Request(params) => params,
            workspace_actions::WorkspaceRenamePlan::Skip(
                workspace_actions::WorkspaceActionRejection::Unchanged,
            ) => return true,
            workspace_actions::WorkspaceRenamePlan::Skip(_) => return false,
        };

        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            return false;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.set_workspaces_error(Some(
                t!("workspace.error.gateway_not_connected").to_string(),
            ));
            return false;
        };

        self.set_workspace_action_in_progress(true);
        self.set_workspaces_error(None);

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let update_params = update_params.clone();

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.workspace_update(update_params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !workspace_actions::workspace_action_result_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    view.set_workspace_action_in_progress(false);

                    match result {
                        Ok(response) => {
                            let workspaces = std::mem::take(&mut view.workspaces);
                            let reduction = workspace_actions::reduce_workspace_rename_success(
                                workspaces,
                                response.workspace,
                            );
                            view.set_workspaces(reduction.workspaces);
                            if reduction.clear_workspaces_error {
                                view.set_workspaces_error(None);
                            }
                        }
                        Err(error) => {
                            let message = format!("{error:#}");
                            warn!(error = message.as_str(), "failed to rename workspace");
                            view.set_workspaces_error(Some(message));
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();

        true
    }
}
