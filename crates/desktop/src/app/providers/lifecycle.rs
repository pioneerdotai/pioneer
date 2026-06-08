use super::{PROVIDERS_FILTER_ALL_NODE_ID, PROVIDERS_FILTER_CONNECTED_NODE_ID};
use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop, ProviderFilter};
use gpui::{prelude::*, *};
use gpui_component::tree::TreeItem;
use pioneer_client::providers::{list as provider_list, selectors};
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn open_providers_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        self.sync_provider_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Providers, cx);
        self.refresh_configured_providers(cx);
    }

    pub(in crate::app) fn sync_provider_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let selected_ix = Some(selectors::provider_filter_tree_index(
            self.providers.filter(),
        ));
        let provider_tree_state = self.provider_tree_state.clone();
        provider_tree_state.update(cx, |state, cx| {
            state.set_items(
                vec![
                    TreeItem::new(PROVIDERS_FILTER_ALL_NODE_ID, "all"),
                    TreeItem::new(PROVIDERS_FILTER_CONNECTED_NODE_ID, "connected"),
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
}
