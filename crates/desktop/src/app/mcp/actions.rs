use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop};
use gpui_kit::{prelude::*, *};
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
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

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
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

                    let outcome = match result {
                        Ok(_) => mcp_actions::McpActionFinishOutcome::Success,
                        Err(error) => {
                            let details = format!("{error:#}");
                            let error =
                                t!("mcp.error.policy_update_failed", error = details.as_str())
                                    .to_string();
                            warn!(
                                name = name.as_str(),
                                error = %details,
                                "failed to set MCP policy"
                            );
                            mcp_actions::McpActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = mcp_actions::reduce_mcp_action_finish(
                        mcp_actions::McpActionFinishKind::Policy(
                            mcp_actions::McpActionTarget::new(name.clone()),
                        ),
                        outcome,
                        view.mcp_selected_server_id.as_deref(),
                    );
                    if reduction.rollback_policy
                        && let Some((prev_enabled, prev_implicit)) = previous_policy
                    {
                        view.apply_local_mcp_policy(name.as_str(), prev_enabled, prev_implicit);
                    }
                    view.apply_mcp_action_finish_reduction(reduction, cx);

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn restart_mcp_server(&mut self, name: String, cx: &mut Context<Self>) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
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

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
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

                    let outcome = match result {
                        Ok(_) => mcp_actions::McpActionFinishOutcome::Success,
                        Err(error) => {
                            let details = format!("{error:#}");
                            let error = t!("mcp.error.restart_failed", error = details.as_str())
                                .to_string();
                            warn!(
                                name = name.as_str(),
                                error = %details,
                                "failed to restart MCP server"
                            );
                            mcp_actions::McpActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = mcp_actions::reduce_mcp_action_finish(
                        mcp_actions::McpActionFinishKind::Restart(
                            mcp_actions::McpActionTarget::new(name.clone()),
                        ),
                        outcome,
                        view.mcp_selected_server_id.as_deref(),
                    );
                    view.apply_mcp_action_finish_reduction(reduction, cx);

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn uninstall_mcp_server(&mut self, name: String, cx: &mut Context<Self>) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return;
        }
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

        let ws_sender = self.gateway.client_runtime.ws_command_sender().clone();
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

                    let outcome = match result {
                        Ok(_) => mcp_actions::McpActionFinishOutcome::Success,
                        Err(error) => {
                            let details = format!("{error:#}");
                            let error = t!("mcp.error.uninstall_failed", error = details.as_str())
                                .to_string();
                            warn!(
                                name = name.as_str(),
                                error = %details,
                                "failed to uninstall MCP server"
                            );
                            mcp_actions::McpActionFinishOutcome::Failure { error }
                        }
                    };
                    let reduction = mcp_actions::reduce_mcp_action_finish(
                        mcp_actions::McpActionFinishKind::Uninstall(
                            mcp_actions::McpActionTarget::new(name.clone()),
                        ),
                        outcome,
                        view.mcp_selected_server_id.as_deref(),
                    );
                    view.apply_mcp_action_finish_reduction(reduction, cx);

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

    fn apply_mcp_action_finish_reduction(
        &mut self,
        reduction: mcp_actions::McpActionFinishReduction,
        cx: &mut Context<Self>,
    ) {
        self.mark_mcp_pending(
            reduction.pending.target.name.as_str(),
            reduction.pending.pending,
        );
        if reduction.clear_selected_details {
            mcp_actions::apply_mcp_uninstall_success(
                &mut self.mcp_selected_server_id,
                &mut self.mcp_server_details,
            );
            if self.main_content_view == MainContentView::McpDetails {
                self.set_main_content_view(MainContentView::Mcp, cx);
            }
        }
        self.mcp_error = reduction.error;
        if reduction.queue_refresh {
            self.queue_mcp_refresh();
        }
        if reduction.queue_details_refresh {
            self.queue_mcp_details_refresh();
        }
    }
}
