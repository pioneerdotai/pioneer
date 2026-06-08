use crate::{
    app::root::{GatewayConnectionState, MainContentView, PioneerDesktop},
    gateway::GatewayRuntime,
};
use gpui::{prelude::*, *};
use pioneer_client::mcp::{
    actions as mcp_actions, details as mcp_details, list as mcp_list,
    notifications::{
        McpRefreshReduction, McpServerCatalogChangedReduction, McpServerStatusChangedReduction,
        apply_mcp_server_catalog_changed_to_catalog, apply_mcp_server_status_changed_to_catalog,
        apply_mcp_server_status_changed_to_details,
    },
};
use pioneer_client::workspaces::selectors as workspace_selectors;
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
        if !mcp_list::mcp_server_exists_by_id(self.mcp_servers.as_slice(), server_id.as_str()) {
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
        let server_id =
            mcp_list::resolve_mcp_server_id_from_timeline(self.mcp_servers.as_slice(), server_id);

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
                self.apply_mcp_refresh_unavailable(reason, false);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

        self.mcp_loading = true;
        self.mcp_error = None;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.mcp_list(mcp_list::mcp_list_params(workspace_id))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mcp_loading = false;
                    match result {
                        Ok(response) => {
                            let reduction = mcp_list::reduce_mcp_list_refresh_success(
                                std::mem::take(&mut view.mcp_servers),
                                std::mem::take(&mut view.mcp_pending_actions),
                                view.mcp_selected_server_id.take(),
                                view.mcp_server_details.take(),
                                response.servers,
                            );
                            view.apply_mcp_list_refresh_success_reduction(reduction, cx);
                        }
                        Err(error) => {
                            let details = format!("{error:#}");
                            let reduction = mcp_list::reduce_mcp_list_refresh_failure(
                                t!("mcp.error.load_servers_failed", error = details.as_str())
                                    .to_string(),
                            );
                            view.apply_mcp_list_refresh_failure_reduction(reduction);
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
                self.apply_mcp_refresh_unavailable(reason, true);
                return;
            }
        };
        let connection_id = scope.connection_id;
        let workspace_id = scope.workspace_id;

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
                        ws_sender.mcp_server_details(mcp_details::mcp_server_details_params(
                            workspace_id,
                            server_id.clone(),
                        ))
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.mcp_details_loading = false;
                    match result {
                        Ok(details) => {
                            let reduction = mcp_details::reduce_mcp_details_refresh_success(
                                std::mem::take(&mut view.mcp_servers),
                                view.mcp_selected_server_id.take(),
                                view.mcp_server_details.take(),
                                details,
                            );
                            view.apply_mcp_details_refresh_success_reduction(reduction);
                        }
                        Err(error) => {
                            let details = format!("{error:#}");
                            let reduction = mcp_details::reduce_mcp_details_refresh_failure(
                                t!("mcp.error.load_details_failed", error = details.as_str())
                                    .to_string(),
                            );
                            view.apply_mcp_details_refresh_failure_reduction(reduction);
                            warn!(error = %format!("{error:#}"), "failed to fetch MCP details");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn apply_mcp_refresh_reduction(&mut self, reduction: McpRefreshReduction) {
        if reduction.queue_mcp_refresh {
            self.queue_mcp_refresh();
        }
        if reduction.queue_mcp_details_refresh {
            self.queue_mcp_details_refresh();
        }
    }

    pub(in crate::app) fn apply_mcp_server_status_changed_reduction(
        &mut self,
        reduction: McpServerStatusChangedReduction,
    ) {
        if !reduction.workspace_matches {
            return;
        }

        apply_mcp_server_status_changed_to_catalog(&mut self.mcp_servers, &reduction.notification);

        if reduction.update_selected_details {
            if let Some(details) = self.mcp_server_details.as_mut() {
                apply_mcp_server_status_changed_to_details(details, &reduction.notification);
            }
        }

        if reduction.queue_mcp_details_refresh {
            self.queue_mcp_details_refresh();
        }
    }

    pub(in crate::app) fn apply_mcp_server_catalog_changed_reduction(
        &mut self,
        reduction: McpServerCatalogChangedReduction,
    ) {
        if !reduction.workspace_matches {
            return;
        }

        apply_mcp_server_catalog_changed_to_catalog(&mut self.mcp_servers, &reduction.notification);

        if reduction.queue_mcp_details_refresh {
            self.queue_mcp_details_refresh();
        }
    }

    pub(in crate::app) fn mcp_workspace_scope(&self) -> Option<String> {
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id);
        workspace_selectors::resolve_workspace_scope(
            None,
            self.preferred_workspace_id(),
            runtime_workspace_id,
        )
    }

    fn apply_mcp_refresh_unavailable(
        &mut self,
        reason: mcp_actions::McpActionUnavailable,
        details: bool,
    ) {
        if details {
            self.mcp_details_loading = false;
        } else {
            self.mcp_loading = false;
        }
        self.mcp_error = Some(match reason {
            mcp_actions::McpActionUnavailable::GatewayNotConnected => {
                t!("mcp.error.gateway_not_connected").to_string()
            }
            mcp_actions::McpActionUnavailable::WorkspaceNotSelected => {
                t!("mcp.error.workspace_not_selected").to_string()
            }
        });
    }

    pub(super) fn is_mcp_pending(&self, name: &str) -> bool {
        mcp_list::is_mcp_pending(&self.mcp_pending_actions, name)
    }

    pub(super) fn mark_mcp_pending(&mut self, name: &str, pending: bool) {
        mcp_list::set_mcp_pending(&mut self.mcp_pending_actions, name, pending);
    }

    fn apply_mcp_list_refresh_success_reduction(
        &mut self,
        reduction: mcp_list::McpListRefreshSuccessReduction,
        cx: &mut Context<Self>,
    ) {
        self.mcp_servers = reduction.servers;
        self.mcp_pending_actions = reduction.pending_actions;
        self.mcp_selected_server_id = reduction.selected_server_id;
        self.mcp_server_details = reduction.server_details;
        self.mcp_error = None;

        if reduction.application.selected_still_present
            && self.main_content_view == MainContentView::McpDetails
        {
            self.refresh_mcp_server_details(cx);
        } else if reduction.application.selected_removed
            && self.main_content_view == MainContentView::McpDetails
        {
            self.set_main_content_view(MainContentView::Mcp, cx);
        }
    }

    fn apply_mcp_list_refresh_failure_reduction(
        &mut self,
        reduction: mcp_list::McpListRefreshFailureReduction,
    ) {
        self.mcp_error = Some(reduction.error);
    }

    fn apply_mcp_details_refresh_success_reduction(
        &mut self,
        reduction: mcp_details::McpDetailsRefreshSuccessReduction,
    ) {
        self.mcp_servers = reduction.servers;
        self.mcp_selected_server_id = reduction.selected_server_id;
        self.mcp_server_details = reduction.server_details;
        self.mcp_error = None;
    }

    fn apply_mcp_details_refresh_failure_reduction(
        &mut self,
        reduction: mcp_details::McpDetailsRefreshFailureReduction,
    ) {
        self.mcp_error = Some(reduction.error);
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
