use super::*;

impl PioneerDesktop {
    pub(crate) fn connect_remote_gateway(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let address = self
            .gateway_address_input_state
            .read(cx)
            .value()
            .to_string();
        let name = self.gateway_name_input_state.read(cx).value().to_string();
        let token = self.gateway_token_input_state.read(cx).value().to_string();
        let ws_sender = self.gateway.ws_command_sender.clone();

        let Some(operation_epoch) = self.begin_gateway_operation(
            t!("gateway.status.connecting_remote").to_string(),
            Some(GatewaySetupAction::ConnectRemote),
            cx,
        ) else {
            return;
        };
        let threads_to_unsubscribe = self.prepare_gateway_switch(cx);

        self.gateway_name_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.gateway_address_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));
        self.gateway_token_input_state
            .update(cx, |state, cx| state.set_value("", window, cx));

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let ws_sender = ws_sender.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        for thread_id in threads_to_unsubscribe {
                            let _ = ws_sender.thread_unsubscribe(thread_id);
                        }
                        let mut runtime = GatewayRuntime::load()?;
                        let endpoint = runtime.add_remote_gateway(
                            name.as_str(),
                            address.as_str(),
                            Some(token.as_str()),
                        )?;
                        let spec = build_ws_connect_spec(&runtime, &endpoint);
                        let connection_id = ws_sender.connect_and_wait(spec)?;
                        runtime.activate_gateway(endpoint.id.as_str())?;

                        Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                            runtime,
                            ws_connection_id: Some(connection_id),
                            ws_connected_ready: true,
                            install_warnings: Vec::new(),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    view.finish_gateway_operation(operation_epoch, result, cx);
                });
            }
        })
        .detach();
    }

    pub(crate) fn start_local_gateway(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let ws_sender = self.gateway.ws_command_sender.clone();

        let Some(operation_epoch) = self.begin_gateway_operation(
            t!("gateway.status.starting_local").to_string(),
            Some(GatewaySetupAction::StartLocal),
            cx,
        ) else {
            return;
        };
        let threads_to_unsubscribe = self.prepare_gateway_switch(cx);

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                let ws_sender = ws_sender.clone();

                async move {
                    let (local_install_required, local_update_required) = cx
                        .background_spawn(async move {
                            (
                                GatewayRuntime::local_gateway_install_required(),
                                GatewayRuntime::local_gateway_update_required(),
                            )
                        })
                        .await;

                    if local_install_required {
                        let _ = this.update_in(&mut cx, |view, _window, cx| {
                            view.update_gateway_operation_status(
                                operation_epoch,
                                t!("gateway.status.installing").to_string(),
                                cx,
                            );
                        });
                    } else if local_update_required {
                        let _ = this.update_in(&mut cx, |view, _window, cx| {
                            view.update_gateway_operation_status(
                                operation_epoch,
                                t!("gateway.status.updating").to_string(),
                                cx,
                            );
                        });
                    }

                    let result = cx
                        .background_spawn(async move {
                            for thread_id in threads_to_unsubscribe {
                                let _ = ws_sender.thread_unsubscribe(thread_id);
                            }
                            let mut runtime = GatewayRuntime::load()?;
                            let local_start = runtime.ensure_local_gateway_started()?;
                            let spec = build_ws_connect_spec(&runtime, &local_start.endpoint);
                            let connection_id = ws_sender.connect_and_wait(spec)?;
                            runtime.activate_gateway(local_start.endpoint.id.as_str())?;

                            Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                                runtime,
                                ws_connection_id: Some(connection_id),
                                ws_connected_ready: true,
                                install_warnings: local_start.warnings,
                            })
                        })
                        .await;

                    let _ = this.update_in(&mut cx, |view, window, cx| match result {
                        Ok(success) => {
                            let install_warnings = success.install_warnings.clone();
                            view.finish_gateway_operation(operation_epoch, Ok(success), cx);
                            view.push_install_warnings_notification(
                                install_warnings.as_slice(),
                                window,
                                cx,
                            );
                        }
                        Err(error) => {
                            view.finish_gateway_operation(operation_epoch, Err(error), cx);
                        }
                    });
                }
            },
        )
        .detach();
    }

    pub(crate) fn activate_gateway(
        &mut self,
        gateway_id: String,
        gateway_name: String,
        cx: &mut Context<Self>,
    ) -> bool {
        if self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway_id())
            == Some(gateway_id.as_str())
        {
            return false;
        }

        let status = t!(
            "gateway.status.connecting_named",
            gateway_name = gateway_name
        )
        .to_string();
        let ws_sender = self.gateway.ws_command_sender.clone();
        let Some(operation_epoch) = self.begin_gateway_operation(status, None, cx) else {
            return false;
        };
        let threads_to_unsubscribe = self.prepare_gateway_switch(cx);

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let ws_sender = ws_sender.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        for thread_id in threads_to_unsubscribe {
                            let _ = ws_sender.thread_unsubscribe(thread_id);
                        }
                        let mut runtime = GatewayRuntime::load()?;
                        let endpoint = runtime.endpoint(gateway_id.as_str()).ok_or_else(|| {
                            anyhow!(
                                "{}",
                                t!("errors.gateway.id_not_found", id = gateway_id.as_str())
                            )
                        })?;
                        let spec = build_ws_connect_spec(&runtime, &endpoint);
                        let connection_id = ws_sender.connect_and_wait(spec)?;
                        runtime.activate_gateway(gateway_id.as_str())?;

                        Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                            runtime,
                            ws_connection_id: Some(connection_id),
                            ws_connected_ready: true,
                            install_warnings: Vec::new(),
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    view.finish_gateway_operation(operation_epoch, result, cx);
                });
            }
        })
        .detach();

        true
    }
}
