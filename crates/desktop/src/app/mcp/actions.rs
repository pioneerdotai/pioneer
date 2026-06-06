use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop};
use gpui::{prelude::*, *};
use pioneer_client::mcp::{actions as mcp_actions, list as mcp_list};
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn set_mcp_policy(
        &mut self,
        name: String,
        enabled: bool,
        allow_implicit_invocation: bool,
        cx: &mut Context<Self>,
    ) {
        let Some(name) = mcp_list::normalize_mcp_server_name(name.as_str()) else {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        };
        let scope = match mcp_actions::plan_mcp_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.mcp_workspace_scope(),
        ) {
            mcp_actions::McpActionScopePlan::Send(scope) => scope,
            mcp_actions::McpActionScopePlan::Unavailable(reason) => {
                self.apply_mcp_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        let previous_policy =
            mcp_actions::mcp_policy_values(self.mcp_servers.as_slice(), name.as_str());
        self.mcp_error = None;
        self.mark_mcp_pending(name.as_str(), true);
        self.apply_local_mcp_policy(name.as_str(), enabled, allow_implicit_invocation);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let name_for_request = name.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_policy_set(mcp_actions::mcp_policy_set_params(
                            workspace_id,
                            name_for_request,
                            enabled,
                            allow_implicit_invocation,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !mcp_actions::mcp_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    view.mark_mcp_pending(name.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.mcp_error = None;
                            view.queue_mcp_refresh();
                            if mcp_actions::mcp_action_should_refresh_details(
                                view.mcp_selected_server_id.as_deref(),
                            ) {
                                view.queue_mcp_details_refresh();
                            }
                        }
                        Err(error) => {
                            if let Some((prev_enabled, prev_implicit)) = previous_policy {
                                view.apply_local_mcp_policy(
                                    name.as_str(),
                                    prev_enabled,
                                    prev_implicit,
                                );
                            }
                            let details = format!("{error:#}");
                            view.mcp_error = Some(
                                t!("mcp.error.policy_update_failed", error = details.as_str())
                                    .to_string(),
                            );
                            warn!(
                                name = name.as_str(),
                                error = %format!("{error:#}"),
                                "failed to set MCP policy"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn restart_mcp_server(&mut self, name: String, cx: &mut Context<Self>) {
        let Some(name) = mcp_list::normalize_mcp_server_name(name.as_str()) else {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        };
        let scope = match mcp_actions::plan_mcp_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.mcp_workspace_scope(),
        ) {
            mcp_actions::McpActionScopePlan::Send(scope) => scope,
            mcp_actions::McpActionScopePlan::Unavailable(reason) => {
                self.apply_mcp_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.mcp_error = None;
        self.mark_mcp_pending(name.as_str(), true);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let name_for_request = name.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_server_restart(mcp_actions::mcp_server_restart_params(
                            workspace_id,
                            name_for_request,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !mcp_actions::mcp_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    view.mark_mcp_pending(name.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.mcp_error = None;
                            view.queue_mcp_refresh();
                            if mcp_actions::mcp_action_should_refresh_details(
                                view.mcp_selected_server_id.as_deref(),
                            ) {
                                view.queue_mcp_details_refresh();
                            }
                        }
                        Err(error) => {
                            let details = format!("{error:#}");
                            view.mcp_error = Some(
                                t!("mcp.error.restart_failed", error = details.as_str())
                                    .to_string(),
                            );
                            warn!(
                                name = name.as_str(),
                                error = %format!("{error:#}"),
                                "failed to restart MCP server"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn uninstall_mcp_server(&mut self, name: String, cx: &mut Context<Self>) {
        let Some(name) = mcp_list::normalize_mcp_server_name(name.as_str()) else {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        };
        let scope = match mcp_actions::plan_mcp_action_scope(
            matches!(
                self.gateway.connection_state,
                GatewayConnectionState::Connected
            ),
            self.gateway.ws_connection_id,
            self.mcp_workspace_scope(),
        ) {
            mcp_actions::McpActionScopePlan::Send(scope) => scope,
            mcp_actions::McpActionScopePlan::Unavailable(reason) => {
                self.apply_mcp_action_unavailable(reason);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.mcp_error = None;
        self.mark_mcp_pending(name.as_str(), true);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let name_for_request = name.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_uninstall(mcp_actions::mcp_uninstall_params(
                            workspace_id,
                            name_for_request,
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !mcp_actions::mcp_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    view.mark_mcp_pending(name.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.mcp_error = None;
                            mcp_actions::apply_mcp_uninstall_success(
                                &mut view.mcp_selected_server_id,
                                &mut view.mcp_server_details,
                            );
                            if view.main_content_view == MainContentView::McpDetails {
                                view.set_main_content_view(MainContentView::Mcp, cx);
                            }
                            view.queue_mcp_refresh();
                        }
                        Err(error) => {
                            let details = format!("{error:#}");
                            view.mcp_error = Some(
                                t!("mcp.error.uninstall_failed", error = details.as_str())
                                    .to_string(),
                            );
                            warn!(
                                name = name.as_str(),
                                error = %format!("{error:#}"),
                                "failed to uninstall MCP server"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn apply_local_mcp_policy(
        &mut self,
        name: &str,
        enabled: bool,
        allow_implicit_invocation: bool,
    ) {
        mcp_actions::apply_local_mcp_policy(
            &mut self.mcp_servers,
            &mut self.mcp_server_details,
            name,
            enabled,
            allow_implicit_invocation,
        );
    }

    fn apply_mcp_action_unavailable(&mut self, reason: mcp_actions::McpActionUnavailable) {
        self.mcp_error = Some(match reason {
            mcp_actions::McpActionUnavailable::GatewayNotConnected => {
                t!("mcp.error.gateway_not_connected").to_string()
            }
            mcp_actions::McpActionUnavailable::WorkspaceNotSelected => {
                t!("mcp.error.workspace_not_selected").to_string()
            }
        });
    }
}
