use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop},
    gateway::GatewayRuntime,
};
use gpui::{prelude::*, *};
use pioneer_protocol::{
    McpChangedNotification, McpListItem, McpListParams, McpServerCatalogChangedNotification,
    McpServerDetailsParams, McpServerDetailsResponse, McpServerStatusChangedNotification,
};
use std::time::Duration;
use tracing::warn;

const MCP_POLL_INTERVAL_SECS: u64 = 20;

impl PioneerDesktop {
    pub(in crate::app) fn open_mcp_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        self.mcp_selected_server_id = None;
        self.mcp_server_details = None;
        self.set_main_content_view(MainContentView::Mcp, cx);
        self.ensure_mcp_poller(cx);
        self.refresh_mcp_servers(cx);
    }

    pub(in crate::app) fn open_mcp_server_details(
        &mut self,
        server_id: String,
        cx: &mut Context<Self>,
    ) {
        let exists = self.mcp_servers.iter().any(|server| server.id == server_id);
        if !exists {
            self.mcp_error = Some(t!("mcp.error.server_not_available").to_string());
            return;
        }

        self.mcp_selected_server_id = Some(server_id);
        self.mcp_server_details = None;
        self.set_main_content_view(MainContentView::McpDetails, cx);
        self.ensure_mcp_poller(cx);
        self.refresh_mcp_server_details(cx);
    }

    pub(in crate::app) fn open_mcp_server_details_from_timeline(
        &mut self,
        server_id: String,
        cx: &mut Context<Self>,
    ) {
        let server_id = self
            .mcp_servers
            .iter()
            .find(|server| server.id == server_id || server.name == server_id)
            .map(|server| server.id.clone())
            .unwrap_or(server_id);

        self.mcp_selected_server_id = Some(server_id);
        self.mcp_server_details = None;
        self.set_main_content_view(MainContentView::McpDetails, cx);
        self.ensure_mcp_poller(cx);
        self.refresh_mcp_servers(cx);
        self.refresh_mcp_server_details(cx);
    }

    pub(in crate::app) fn close_mcp_details_screen(&mut self, cx: &mut Context<Self>) {
        self.set_main_content_view(MainContentView::Mcp, cx);
    }

    pub(in crate::app) fn queue_mcp_refresh(&mut self) {
        self.mcp_refresh_requested = true;
    }

    pub(in crate::app) fn queue_mcp_details_refresh(&mut self) {
        self.mcp_details_refresh_requested = true;
    }

    pub(in crate::app) fn take_mcp_refresh_request(&mut self) -> bool {
        if !self.mcp_refresh_requested {
            return false;
        }
        self.mcp_refresh_requested = false;
        true
    }

    pub(in crate::app) fn take_mcp_details_refresh_request(&mut self) -> bool {
        if !self.mcp_details_refresh_requested {
            return false;
        }
        self.mcp_details_refresh_requested = false;
        true
    }

    pub(in crate::app) fn refresh_mcp_servers(&mut self, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.mcp_loading = false;
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.mcp_loading = false;
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        };

        let Some(workspace_id) = self.mcp_workspace_scope() else {
            self.mcp_loading = false;
            self.mcp_error = Some(t!("mcp.error.workspace_not_selected").to_string());
            return;
        };

        self.mcp_loading = true;
        self.mcp_error = None;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(
                        async move { ws_sender.mcp_list(McpListParams { workspace_id }) },
                    )
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mcp_loading = false;
                    match result {
                        Ok(response) => {
                            view.apply_mcp_snapshot(response.servers, cx);
                            view.mcp_error = None;
                        }
                        Err(error) => {
                            let details = format!("{error:#}");
                            view.mcp_error = Some(
                                t!("mcp.error.load_servers_failed", error = details.as_str())
                                    .to_string(),
                            );
                            warn!(error = %format!("{error:#}"), "failed to fetch MCP servers");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn refresh_mcp_server_details(&mut self, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.mcp_details_loading = false;
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.mcp_details_loading = false;
            self.mcp_error = Some(t!("mcp.error.gateway_not_connected").to_string());
            return;
        };

        let Some(workspace_id) = self.mcp_workspace_scope() else {
            self.mcp_details_loading = false;
            self.mcp_error = Some(t!("mcp.error.workspace_not_selected").to_string());
            return;
        };

        let Some(server_id) = self.mcp_selected_server_id.clone() else {
            self.mcp_details_loading = false;
            return;
        };

        self.mcp_details_loading = true;
        self.mcp_error = None;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_server_details(McpServerDetailsParams {
                            workspace_id,
                            server_id: server_id.clone(),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mcp_details_loading = false;
                    match result {
                        Ok(details) => {
                            view.apply_mcp_details_response(details);
                            view.mcp_error = None;
                        }
                        Err(error) => {
                            let details = format!("{error:#}");
                            view.mcp_error = Some(
                                t!("mcp.error.load_details_failed", error = details.as_str())
                                    .to_string(),
                            );
                            warn!(error = %format!("{error:#}"), "failed to fetch MCP details");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn apply_mcp_changed_notification(
        &mut self,
        notification: McpChangedNotification,
    ) {
        if !self.mcp_notification_workspace_matches(notification.workspace_id.as_str()) {
            return;
        }

        self.queue_mcp_refresh();
        if self.mcp_selected_server_id.is_some() {
            self.queue_mcp_details_refresh();
        }
    }

    pub(in crate::app) fn apply_mcp_server_status_changed_notification(
        &mut self,
        notification: McpServerStatusChangedNotification,
    ) {
        if !self.mcp_notification_workspace_matches(notification.workspace_id.as_str()) {
            return;
        }

        for server in &mut self.mcp_servers {
            if server.id == notification.server.id {
                server.runtime = notification.server.runtime.clone();
                server.status = notification.server.status;
                server.status_reason = notification.server.status_reason.clone();
            }
        }

        if self.mcp_selected_server_id.as_deref() == Some(notification.server.id.as_str()) {
            if let Some(details) = self.mcp_server_details.as_mut() {
                details.server.runtime = notification.server.runtime.clone();
                details.server.status = notification.server.status;
                details.server.status_reason = notification.server.status_reason.clone();
                details.health.runtime = notification.server.runtime;
                details.health.status = notification.server.status;
                details.health.status_reason = notification.server.status_reason;
            } else {
                self.queue_mcp_details_refresh();
            }
        }
    }

    pub(in crate::app) fn apply_mcp_server_catalog_changed_notification(
        &mut self,
        notification: McpServerCatalogChangedNotification,
    ) {
        if !self.mcp_notification_workspace_matches(notification.workspace_id.as_str()) {
            return;
        }

        for server in &mut self.mcp_servers {
            if server.id == notification.server_id {
                server.tools_count = notification.tools_count;
                server.resources_count = notification.resources_count;
                server.resource_templates_count = notification.resource_templates_count;
                server.prompts_count = notification.prompts_count;
            }
        }

        if self.mcp_selected_server_id.as_deref() == Some(notification.server_id.as_str()) {
            self.queue_mcp_details_refresh();
        }
    }

    pub(super) fn mcp_workspace_scope(&self) -> Option<String> {
        self.preferred_workspace_id()
            .map(str::to_owned)
            .or_else(|| {
                self.gateway
                    .runtime
                    .as_ref()
                    .and_then(GatewayRuntime::active_workspace_id)
                    .map(str::to_owned)
            })
            .and_then(|workspace_id| {
                let trimmed = workspace_id.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_owned())
                }
            })
    }

    pub(super) fn is_mcp_pending(&self, name: &str) -> bool {
        self.mcp_pending_actions.contains(name)
    }

    pub(super) fn mark_mcp_pending(&mut self, name: &str, pending: bool) {
        if pending {
            self.mcp_pending_actions.insert(name.to_owned());
        } else {
            self.mcp_pending_actions.remove(name);
        }
    }

    fn apply_mcp_snapshot(&mut self, servers: Vec<McpListItem>, cx: &mut Context<Self>) {
        self.mcp_pending_actions.retain(|name| {
            name == "__install__" || servers.iter().any(|server| server.name == *name)
        });

        self.mcp_servers = servers;

        if let Some(server_id) = self.mcp_selected_server_id.clone() {
            let still_present = self.mcp_servers.iter().any(|server| server.id == server_id);
            if still_present {
                if self.main_content_view == MainContentView::McpDetails {
                    self.refresh_mcp_server_details(cx);
                }
            } else {
                self.mcp_selected_server_id = None;
                self.mcp_server_details = None;
                if self.main_content_view == MainContentView::McpDetails {
                    self.set_main_content_view(MainContentView::Mcp, cx);
                }
            }
        }
    }

    fn apply_mcp_details_response(&mut self, details: McpServerDetailsResponse) {
        self.mcp_selected_server_id = Some(details.server.id.clone());
        for server in &mut self.mcp_servers {
            if server.id == details.server.id {
                *server = details.server.clone();
            }
        }
        self.mcp_server_details = Some(details);
    }

    fn mcp_notification_workspace_matches(&self, workspace_id: &str) -> bool {
        self.mcp_workspace_scope()
            .as_deref()
            .is_some_and(|current| current == workspace_id)
    }

    fn ensure_mcp_poller(&mut self, cx: &mut Context<Self>) {
        if self.mcp_poller_started {
            return;
        }

        self.mcp_poller_started = true;
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                loop {
                    Timer::after(Duration::from_secs(MCP_POLL_INTERVAL_SECS)).await;

                    let updated = this.update(&mut cx, |view, cx| {
                        if matches!(
                            view.main_content_view,
                            MainContentView::Mcp | MainContentView::McpDetails
                        ) && view.gateway.connection_state == GatewayConnectionState::Connected
                        {
                            view.queue_mcp_refresh();
                            if view.main_content_view == MainContentView::McpDetails {
                                view.queue_mcp_details_refresh();
                            }
                            cx.notify();
                        }
                    });
                    if updated.is_err() {
                        break;
                    }
                }
            }
        })
        .detach();
    }
}
