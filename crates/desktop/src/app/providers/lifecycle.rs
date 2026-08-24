use super::{
    PROVIDERS_FILTER_API_NODE_ID, PROVIDERS_FILTER_CLI_NODE_ID, PROVIDERS_FILTER_CONNECTED_NODE_ID,
};
use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop, ProviderFilter};
use gpui::{prelude::*, *};
use gpui_component::tree::TreeItem;
use pioneer_client::providers::{list as provider_list, selectors};
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn open_providers_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        let can_manage = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;
        self.sync_provider_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Providers, cx);
        self.refresh_configured_providers(cx);
        if can_manage {
            self.refresh_gateway_settings(cx);
            self.load_cli_provider_snapshot(cx);
        }
    }

    pub(in crate::app) fn sync_provider_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let can_manage = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;
        if !can_manage && self.providers.filter() == ProviderFilter::Cli {
            self.providers.set_filter(ProviderFilter::Api);
        }
        let selected_ix = Some(selectors::provider_filter_tree_index(
            self.providers.filter(),
        ));
        let provider_tree_state = self.provider_tree_state.clone();
        provider_tree_state.update(cx, |state, cx| {
            let mut items = vec![
                TreeItem::new(PROVIDERS_FILTER_API_NODE_ID, "api"),
                TreeItem::new(PROVIDERS_FILTER_CONNECTED_NODE_ID, "connected"),
            ];
            if can_manage {
                items.push(TreeItem::new(PROVIDERS_FILTER_CLI_NODE_ID, "cli"));
            }
            state.set_items(items, cx);
            state.set_selected_index(selected_ix, cx);
        });
    }

    pub(super) fn set_provider_filter(&mut self, filter: ProviderFilter, cx: &mut Context<Self>) {
        if filter == ProviderFilter::Cli
            && !self
                .principal_presentation_capabilities()
                .can_manage_capabilities
        {
            return;
        }
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

        self.startup
            .begin(pioneer_observability::DesktopStartupStage::ProviderLoad);
        self.providers.mark_refresh_started();

        let ws_sender = self.gateway.ws_command_sender.clone();
        let workspace_id = request.params.workspace_id.clone();
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
                    ) || view.active_workspace_id() != Some(workspace_id.as_str())
                    {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            view.providers.apply_refresh_response(response);
                            view.startup.succeed(
                                pioneer_observability::DesktopStartupStage::ProviderLoad,
                            );
                        }
                        Err(error) => {
                            view.providers.apply_refresh_failed(format!(
                                "{}: {error:#}",
                                t!("providers.error.load_failed")
                            ));
                            warn!(error = %format!("{error:#}"), "failed to fetch configured providers");
                            view.startup.fail(
                                pioneer_observability::DesktopStartupStage::ProviderLoad,
                            );
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

    pub(in crate::app) fn load_cli_provider_snapshot(&mut self, cx: &mut Context<Self>) {
        if self.providers.cli_loading() {
            return;
        }
        let plan = provider_list::plan_cli_runtime_list(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
        );
        let request = match plan {
            provider_list::CLIRuntimeListPlan::Send(request) => request,
            provider_list::CLIRuntimeListPlan::Unavailable(reason) => {
                self.apply_cli_provider_snapshot_load_unavailable(reason);
                return;
            }
        };

        self.providers.mark_cli_runtime_snapshot_load_started();
        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let workspace_id = request.params.workspace_id.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.cli_runtime_list(request.params) })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_list::provider_list_refresh_matches_connection(
                        request.connection_id,
                        view.gateway.ws_connection_id,
                    ) || view.active_workspace_id() != Some(workspace_id.as_str())
                    {
                        return;
                    }
                    match result {
                        Ok(response) => {
                            match view.providers.apply_cli_runtime_snapshot_response(response) {
                                provider_list::CliRuntimeSnapshotLoad::Applied => {
                                    view.refresh_composer_capability_target_for_selected_provider();
                                    view.sync_open_model_selector_cli_runtime_snapshot();
                                }
                                provider_list::CliRuntimeSnapshotLoad::RetryRequired => {
                                    view.load_cli_provider_snapshot(cx);
                                }
                            }
                        }
                        Err(error) => {
                            view.providers
                                .apply_cli_runtime_snapshot_load_failed(format!(
                                    "{}: {error:#}",
                                    t!("providers.error.load_failed")
                                ))
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn apply_cli_provider_snapshot_load_unavailable(
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
        self.providers.apply_cli_runtime_snapshot_load_failed(error);
    }
}
