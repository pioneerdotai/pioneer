use super::{
    PROVIDERS_FILTER_API_NODE_ID, PROVIDERS_FILTER_CLI_NODE_ID, PROVIDERS_FILTER_CONNECTED_NODE_ID,
};
use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop, ProviderFilter};
use gpui::{prelude::*, *};
use gpui_component::tree::TreeItem;
use pioneer_client::providers::{list as provider_list, selectors};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn open_providers_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        self.sync_provider_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Providers, cx);
        self.refresh_gateway_settings(cx);
        self.refresh_configured_providers(cx);
        self.refresh_cli_providers_auto(cx);
    }

    pub(in crate::app) fn sync_provider_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let selected_ix = Some(selectors::provider_filter_tree_index(
            self.providers.filter(),
        ));
        let provider_tree_state = self.provider_tree_state.clone();
        provider_tree_state.update(cx, |state, cx| {
            state.set_items(
                vec![
                    TreeItem::new(PROVIDERS_FILTER_API_NODE_ID, "api"),
                    TreeItem::new(PROVIDERS_FILTER_CONNECTED_NODE_ID, "connected"),
                    TreeItem::new(PROVIDERS_FILTER_CLI_NODE_ID, "cli"),
                ],
                cx,
            );
            state.set_selected_index(selected_ix, cx);
        });
    }

    pub(super) fn set_provider_filter(&mut self, filter: ProviderFilter, cx: &mut Context<Self>) {
        self.providers.set_filter(filter);
        self.sync_provider_sidebar_tree_state(cx);
        cx.notify();
    }

    pub(in crate::app) fn refresh_configured_providers(&mut self, cx: &mut Context<Self>) {
        let plan = provider_list::plan_provider_list_refresh(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
        );
        let request = match plan {
            provider_list::ProviderListRefreshPlan::Send(request) => request,
            provider_list::ProviderListRefreshPlan::Unavailable(reason) => {
                self.apply_provider_list_refresh_unavailable(reason);
                return;
            }
        };

        self.providers.mark_refresh_started();

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let params = request.params;
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.provider_list(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_list::provider_list_refresh_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.providers.apply_refresh_response(response);
                        }
                        Err(error) => {
                            view.providers.apply_refresh_failed(format!(
                                "{}: {error:#}",
                                t!("providers.error.load_failed")
                            ));
                            warn!(error = %format!("{error:#}"), "failed to fetch configured providers");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn refresh_cli_providers(&mut self, cx: &mut Context<Self>) {
        self.refresh_cli_providers_with_trigger(
            provider_list::CLIRuntimeRefreshTrigger::Manual,
            cx,
        );
    }

    pub(in crate::app) fn refresh_cli_providers_auto(&mut self, cx: &mut Context<Self>) {
        self.refresh_cli_providers_with_trigger(provider_list::CLIRuntimeRefreshTrigger::Auto, cx);
    }

    fn refresh_cli_providers_with_trigger(
        &mut self,
        trigger: provider_list::CLIRuntimeRefreshTrigger,
        cx: &mut Context<Self>,
    ) {
        let now_unix_ms = provider_now_unix_ms();
        let plan = provider_list::plan_cli_runtime_refresh_with_policy(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            self.providers.cli_refresh_status(),
            trigger,
            now_unix_ms,
        );
        let request = match plan {
            provider_list::CLIRuntimeRefreshPlan::Send(request) => request,
            provider_list::CLIRuntimeRefreshPlan::Skip(reason) => {
                if trigger == provider_list::CLIRuntimeRefreshTrigger::Manual {
                    self.providers
                        .apply_cli_runtime_login_message(cli_runtime_refresh_skip_message(reason));
                    cx.notify();
                }
                return;
            }
            provider_list::CLIRuntimeRefreshPlan::Unavailable(reason) => {
                self.apply_cli_provider_refresh_unavailable(reason);
                return;
            }
        };

        self.providers.mark_cli_runtime_refresh_started(now_unix_ms);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let params = request.params;
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.cli_runtime_refresh(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_list::provider_list_refresh_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.providers.apply_cli_runtime_refresh_response(
                                response,
                                provider_now_unix_ms(),
                            );
                            view.refresh_composer_capability_target_for_selected_provider();
                        }
                        Err(error) => {
                            view.providers.apply_cli_runtime_refresh_failed(
                                format!("{}: {error:#}", t!("providers.error.load_failed")),
                                provider_now_unix_ms(),
                            );
                            warn!(error = %format!("{error:#}"), "failed to fetch CLI runtimes");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn refresh_cli_provider(
        &mut self,
        runtime_id: String,
        cx: &mut Context<Self>,
    ) {
        let now_unix_ms = provider_now_unix_ms();
        let plan = provider_list::plan_cli_runtime_instance_refresh_with_policy(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            runtime_id,
            self.providers.cli_refresh_status(),
            provider_list::CLIRuntimeRefreshTrigger::Manual,
            now_unix_ms,
        );
        let request = match plan {
            provider_list::CLIRuntimeRefreshPlan::Send(request) => request,
            provider_list::CLIRuntimeRefreshPlan::Skip(reason) => {
                self.providers
                    .apply_cli_runtime_login_message(cli_runtime_refresh_skip_message(reason));
                cx.notify();
                return;
            }
            provider_list::CLIRuntimeRefreshPlan::Unavailable(reason) => {
                self.apply_cli_provider_refresh_unavailable(reason);
                return;
            }
        };

        self.providers.mark_cli_runtime_refresh_started(now_unix_ms);

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let params = request.params;
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.cli_runtime_refresh(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_list::provider_list_refresh_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.providers.apply_cli_runtime_instance_refresh_response(
                                response,
                                provider_now_unix_ms(),
                            );
                            view.refresh_composer_capability_target_for_selected_provider();
                        }
                        Err(error) => {
                            view.providers.apply_cli_runtime_refresh_failed(
                                format!("{}: {error:#}", t!("providers.error.load_failed")),
                                provider_now_unix_ms(),
                            );
                            warn!(error = %format!("{error:#}"), "failed to fetch CLI runtime");
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn apply_provider_list_refresh_unavailable(
        &mut self,
        reason: provider_list::ProviderListRefreshUnavailable,
    ) {
        let error = match reason {
            provider_list::ProviderListRefreshUnavailable::GatewayNotConnected => {
                t!("providers.error.gateway_not_connected").to_string()
            }
            provider_list::ProviderListRefreshUnavailable::WorkspaceNotSelected => {
                t!("providers.error.workspace_not_selected").to_string()
            }
        };
        self.providers.apply_unavailable(error);
    }

    fn apply_cli_provider_refresh_unavailable(
        &mut self,
        reason: provider_list::ProviderListRefreshUnavailable,
    ) {
        let error = match reason {
            provider_list::ProviderListRefreshUnavailable::GatewayNotConnected => {
                t!("providers.error.gateway_not_connected").to_string()
            }
            provider_list::ProviderListRefreshUnavailable::WorkspaceNotSelected => {
                t!("providers.error.workspace_not_selected").to_string()
            }
        };
        self.providers
            .apply_cli_runtime_refresh_failed(error, provider_now_unix_ms());
    }
}

fn provider_now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn cli_runtime_refresh_skip_message(reason: provider_list::CLIRuntimeRefreshSkipReason) -> String {
    match reason {
        provider_list::CLIRuntimeRefreshSkipReason::AlreadyRefreshing => {
            t!("providers.cli.refresh_already_running").to_string()
        }
        provider_list::CLIRuntimeRefreshSkipReason::Throttled { .. }
        | provider_list::CLIRuntimeRefreshSkipReason::BackingOff { .. } => {
            t!("providers.cli.refresh_wait").to_string()
        }
    }
}
