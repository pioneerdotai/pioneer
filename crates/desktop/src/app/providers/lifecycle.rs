use super::{PROVIDERS_FILTER_ALL_NODE_ID, PROVIDERS_FILTER_CONNECTED_NODE_ID};
use crate::app::root::{GatewayConnectionState, MainContentView, PioneerDesktop, ProviderFilter};
use gpui::{prelude::*, *};
use gpui_component::tree::TreeItem;
use std::collections::HashSet;
use tracing::warn;

impl PioneerDesktop {
    pub(in crate::app) fn open_providers_screen_from_bottom_bar(&mut self, cx: &mut Context<Self>) {
        self.sync_provider_sidebar_tree_state(cx);
        self.set_main_content_view(MainContentView::Providers, cx);
        self.refresh_configured_providers(cx);
    }

    pub(in crate::app) fn sync_provider_sidebar_tree_state(&mut self, cx: &mut Context<Self>) {
        let selected_ix = match self.provider_filter {
            ProviderFilter::All => Some(0),
            ProviderFilter::Connected => Some(1),
        };
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
        self.provider_filter = filter;
        self.sync_provider_sidebar_tree_state(cx);
        cx.notify();
    }

    pub(super) fn refresh_configured_providers(&mut self, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.providers_loading = false;
            self.providers_error = Some(t!("providers.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.providers_loading = false;
            self.providers_error = Some(t!("providers.error.gateway_not_connected").to_string());
            return;
        };

        self.providers_loading = true;
        self.providers_error = None;

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.provider_list() })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    view.providers_loading = false;

                    match result {
                        Ok(response) => {
                            let configured = response
                                .providers
                                .into_iter()
                                .map(|provider| Self::canonical_provider_id(provider.name.as_str()))
                                .collect::<HashSet<_>>();
                            view.provider_configured_names = configured;
                            view.providers_error = None;
                        }
                        Err(error) => {
                            view.providers_error = Some(format!(
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
}
