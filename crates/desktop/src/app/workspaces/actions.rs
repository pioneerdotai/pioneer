use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::{prelude::*, *};
use pioneer_protocol::{WorkspaceCreateParams, WorkspaceUpdateParams, generate_id};
use tracing::warn;

const WORKSPACE_ID_LEN: usize = 21;

impl PioneerDesktop {
    pub(in crate::app) fn create_workspace_from_dialog(
        &mut self,
        name: String,
        cx: &mut Context<Self>,
    ) -> bool {
        let name = name.trim().to_owned();
        if name.is_empty() || self.workspace_action_in_progress() {
            return false;
        }

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
        let workspace_id = generate_id(WORKSPACE_ID_LEN);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let name = name.clone();
            let workspace_id = workspace_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.workspace_create(WorkspaceCreateParams {
                            workspace_id,
                            name: Some(name),
                            make_current: false,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.set_workspace_action_in_progress(false);

                    match result {
                        Ok(response) => {
                            let workspace_id = response.workspace.id.clone();
                            crate::app::flow::upsert_workspace_catalog_item(
                                &mut view.workspaces,
                                response.workspace,
                            );
                            view.set_workspaces_error(None);
                            view.switch_workspace_from_ui(workspace_id, cx);
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
        let name = name.trim().to_owned();
        if name.is_empty() || self.workspace_action_in_progress() {
            return false;
        }

        if self
            .workspace_by_id(workspace_id.as_str())
            .is_some_and(|workspace| workspace.name == name)
        {
            return true;
        }

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
            let workspace_id = workspace_id.clone();
            let name = name.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.workspace_update(WorkspaceUpdateParams {
                            workspace_id,
                            name: Some(name),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.set_workspace_action_in_progress(false);

                    match result {
                        Ok(response) => {
                            crate::app::flow::upsert_workspace_catalog_item(
                                &mut view.workspaces,
                                response.workspace,
                            );
                            view.set_workspaces_error(None);
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
