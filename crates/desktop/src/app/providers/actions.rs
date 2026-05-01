use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::{prelude::*, *};
use pioneer_protocol::{ProviderDeleteApiKeyParams, ProviderSetApiKeyParams};
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn set_provider_api_key(
        &mut self,
        provider_id: String,
        api_key: String,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.providers_error = Some(t!("providers.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.providers_error = Some(t!("providers.error.gateway_not_connected").to_string());
            return;
        };

        let canonical_provider_id = Self::canonical_provider_id(provider_id.as_str());
        let provider_for_request = provider_id;
        let ws_sender = self.gateway.ws_command_sender.clone();
        self.providers_error = None;

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let canonical_provider_id = canonical_provider_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.provider_set_api_key(ProviderSetApiKeyParams {
                            provider: provider_for_request,
                            api_key,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok(_) => {
                            view.provider_configured_names
                                .insert(canonical_provider_id.clone());
                            view.providers_error = None;
                        }
                        Err(error) => {
                            view.providers_error =
                                Some(format!("{}: {error:#}", t!("providers.error.save_failed")));
                            warn!(
                                provider = canonical_provider_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to set provider api key"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn delete_provider_api_key(&mut self, provider_id: String, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.providers_error = Some(t!("providers.error.gateway_not_connected").to_string());
            return;
        }

        let Some(connection_id) = self.gateway.ws_connection_id else {
            self.providers_error = Some(t!("providers.error.gateway_not_connected").to_string());
            return;
        };

        let canonical_provider_id = Self::canonical_provider_id(provider_id.as_str());
        let provider_for_request = provider_id;
        let ws_sender = self.gateway.ws_command_sender.clone();
        self.providers_error = None;

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let canonical_provider_id = canonical_provider_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.provider_delete_api_key(ProviderDeleteApiKeyParams {
                            provider: provider_for_request,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok(_) => {
                            view.provider_configured_names
                                .remove(canonical_provider_id.as_str());
                            view.providers_error = None;
                        }
                        Err(error) => {
                            view.providers_error = Some(format!(
                                "{}: {error:#}",
                                t!("providers.error.delete_failed")
                            ));
                            warn!(
                                provider = canonical_provider_id.as_str(),
                                error = %format!("{error:#}"),
                                "failed to delete provider api key"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }
}
