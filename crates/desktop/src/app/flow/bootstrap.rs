use super::*;

impl PioneerDesktop {
    pub(crate) fn bootstrap_gateway_runtime(&mut self, cx: &mut Context<Self>) {
        let operation_epoch = self.next_gateway_connection_epoch();
        self.gateway.connecting = true;
        self.gateway.setup_action = None;
        self.gateway.connection_state = GatewayConnectionState::Connecting;
        self.gateway.status = t!("gateway.status.connecting").to_string();
        self.gateway.status_level = GatewayStatusLevel::Neutral;
        self.gateway.error = None;
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let ws_sender = ws_sender.clone();

            async move {
                let local_update_required = cx
                    .background_spawn(
                        async move { GatewayRuntime::local_gateway_update_required() },
                    )
                    .await;

                if local_update_required {
                    let _ = this.update(&mut cx, |view, cx| {
                        view.update_gateway_operation_status(
                            operation_epoch,
                            t!("gateway.status.updating").to_string(),
                            cx,
                        );
                    });
                }

                let result = cx
                    .background_spawn(async move {
                        let mut runtime = GatewayRuntime::load()?;
                        runtime.discover_and_adopt_existing_local_gateway_once()?;
                        runtime.try_recover_active_local_gateway_once()?;
                        let ws_connection_id = if runtime.setup_required() {
                            None
                        } else if let Some(endpoint) = runtime.active_gateway().cloned() {
                            let spec = build_ws_connect_spec(&mut runtime, &endpoint)?;
                            Some(ws_sender.connect_with_retry(spec)?)
                        } else {
                            None
                        };

                        Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                            runtime,
                            ws_connection_id,
                            ws_connected_ready: false,
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
}
