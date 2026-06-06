use crate::app::root::{GatewayConnectionState, PioneerDesktop};
use gpui::{prelude::*, *};
use pioneer_client::providers::actions as provider_actions;
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn set_provider_api_key(
        &mut self,
        provider_id: String,
        api_key: String,
        cx: &mut Context<Self>,
    ) {
        let plan = provider_actions::plan_provider_set_api_key(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            provider_id,
            api_key,
        );
        let request = match plan {
            provider_actions::ProviderSetApiKeyPlan::Send(request) => request,
            provider_actions::ProviderSetApiKeyPlan::Unavailable(reason) => {
                self.apply_provider_api_key_unavailable(reason);
                return;
            }
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        provider_actions::mark_provider_api_key_action_started(&mut self.providers);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let canonical_provider_id = request.canonical_provider_id.clone();
            let params = request.params;

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.provider_set_api_key(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_actions::provider_api_key_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(_) => {
                            provider_actions::apply_provider_set_api_key_success(
                                &mut view.providers,
                                canonical_provider_id.clone(),
                            );
                        }
                        Err(error) => {
                            provider_actions::apply_provider_api_key_failure(
                                &mut view.providers,
                                format!("{}: {error:#}", t!("providers.error.save_failed")),
                            );
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
        let plan = provider_actions::plan_provider_delete_api_key(
            self.gateway.connection_state == GatewayConnectionState::Connected,
            self.gateway.ws_connection_id,
            self.active_workspace_id().map(str::to_owned),
            provider_id,
        );
        let request = match plan {
            provider_actions::ProviderDeleteApiKeyPlan::Send(request) => request,
            provider_actions::ProviderDeleteApiKeyPlan::Unavailable(reason) => {
                self.apply_provider_api_key_unavailable(reason);
                return;
            }
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        provider_actions::mark_provider_api_key_action_started(&mut self.providers);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let connection_id = request.connection_id;
            let canonical_provider_id = request.canonical_provider_id.clone();
            let params = request.params;

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.provider_delete_api_key(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if !provider_actions::provider_api_key_action_matches_connection(
                        connection_id,
                        view.gateway.ws_connection_id,
                    ) {
                        return;
                    }

                    match result {
                        Ok(_) => {
                            provider_actions::apply_provider_delete_api_key_success(
                                &mut view.providers,
                                canonical_provider_id.as_str(),
                            );
                        }
                        Err(error) => {
                            provider_actions::apply_provider_api_key_failure(
                                &mut view.providers,
                                format!("{}: {error:#}", t!("providers.error.delete_failed")),
                            );
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

    fn apply_provider_api_key_unavailable(
        &mut self,
        reason: provider_actions::ProviderApiKeyActionUnavailable,
    ) {
        let error = match reason {
            provider_actions::ProviderApiKeyActionUnavailable::GatewayNotConnected => {
                t!("providers.error.gateway_not_connected").to_string()
            }
            provider_actions::ProviderApiKeyActionUnavailable::WorkspaceNotSelected => {
                t!("providers.error.workspace_not_selected").to_string()
            }
        };
        provider_actions::apply_provider_api_key_failure(&mut self.providers, error);
    }
}
