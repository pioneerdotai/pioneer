use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop};
use gpui::{prelude::*, *};
use pioneer_protocol::{
    McpPolicySetParams, McpScopeKind, McpServerRestartParams, McpServerStatus, McpUninstallParams,
};
use tracing::warn;

pub(super) const MCP_INSTALL_PENDING_KEY: &str = "__install__";

impl PioneerDesktop {
    pub(super) fn set_mcp_policy(
        &mut self,
        name: String,
        enabled: bool,
        allow_implicit_invocation: bool,
        cx: &mut Context<Self>,
    ) {
        let name = name.trim().to_owned();
        if name.is_empty() {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        };
        let Some(workspace_id) = self.mcp_workspace_scope() else {
            self.mcp_error = Some(t!("mcp.error.workspace_not_selected").to_string());
            return;
        };

        let previous_policy = self.mcp_policy_values(name.as_str());
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
                        ws_sender.mcp_policy_set(McpPolicySetParams {
                            workspace_id,
                            name: name_for_request,
                            scope_kind: McpScopeKind::Workspace,
                            enabled: Some(enabled),
                            allow_implicit_invocation: Some(allow_implicit_invocation),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mark_mcp_pending(name.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.mcp_error = None;
                            view.queue_mcp_refresh();
                            if view.mcp_selected_server_id.is_some() {
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
        let name = name.trim().to_owned();
        if name.is_empty() {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        };
        let Some(workspace_id) = self.mcp_workspace_scope() else {
            self.mcp_error = Some(t!("mcp.error.workspace_not_selected").to_string());
            return;
        };

        self.mcp_error = None;
        self.mark_mcp_pending(name.as_str(), true);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let name_for_request = name.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_server_restart(McpServerRestartParams {
                            workspace_id,
                            name: name_for_request,
                            scope_kind: McpScopeKind::Workspace,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mark_mcp_pending(name.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.mcp_error = None;
                            view.queue_mcp_refresh();
                            if view.mcp_selected_server_id.is_some() {
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
        let name = name.trim().to_owned();
        if name.is_empty() {
            self.mcp_error = Some(t!("mcp.error.server_name_required").to_string());
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        };
        let Some(workspace_id) = self.mcp_workspace_scope() else {
            self.mcp_error = Some(t!("mcp.error.workspace_not_selected").to_string());
            return;
        };

        self.mcp_error = None;
        self.mark_mcp_pending(name.as_str(), true);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let name_for_request = name.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_uninstall(McpUninstallParams {
                            workspace_id,
                            name: name_for_request,
                            scope_kind: McpScopeKind::Workspace,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mark_mcp_pending(name.as_str(), false);
                    match result {
                        Ok(_) => {
                            view.mcp_error = None;
                            view.mcp_selected_server_id = None;
                            view.mcp_server_details = None;
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

    fn mcp_policy_values(&self, name: &str) -> Option<(bool, bool)> {
        self.mcp_servers
            .iter()
            .find(|server| server.name == name)
            .map(|server| {
                (
                    server.policy.enabled,
                    server.policy.allow_implicit_invocation,
                )
            })
    }

    fn apply_local_mcp_policy(
        &mut self,
        name: &str,
        enabled: bool,
        allow_implicit_invocation: bool,
    ) {
        for server in &mut self.mcp_servers {
            if server.name == name {
                server.policy.enabled = enabled;
                server.policy.allow_implicit_invocation = allow_implicit_invocation;
                if !enabled {
                    server.status = McpServerStatus::Disabled;
                }
            }
        }

        if let Some(details) = self.mcp_server_details.as_mut() {
            if details.server.name == name {
                details.server.policy.enabled = enabled;
                details.server.policy.allow_implicit_invocation = allow_implicit_invocation;
                if !enabled {
                    details.server.status = McpServerStatus::Disabled;
                    details.health.status = McpServerStatus::Disabled;
                }
            }
        }
    }
}
