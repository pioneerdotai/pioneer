use super::*;

impl PioneerDesktop {
    pub(crate) fn bootstrap_gateway_runtime(&mut self, cx: &mut Context<Self>) {
        self.startup
            .begin(pioneer_observability::DesktopStartupStage::GatewayRuntimeLoad);
        let operation_epoch = self.next_gateway_connection_epoch();
        self.gateway.connecting = true;
        self.gateway.setup_action = None;
        self.gateway.connection_state = GatewayConnectionState::Connecting;
        self.gateway.status = t!("gateway.status.connecting").to_string();
        self.gateway.status_level = GatewayStatusLevel::Neutral;
        self.gateway.error = None;
        let client_core = self.gateway.client_runtime.client_core().clone();
        let startup_trace = self.startup.diagnostic_trace();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let startup_trace = startup_trace.clone();

            async move {
                let update_check_trace = startup_trace.clone();
                let local_update_required = cx
                    .background_spawn(async move {
                        let stage = update_check_trace.stage(
                            pioneer_observability::DesktopStartupStage::GatewayRuntimeUpdateCheck,
                        );
                        let update_required = GatewayRuntime::local_gateway_update_required();
                        stage.succeed();
                        update_required
                    })
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
                        let mut runtime = crate::gateway::observe_startup_stage(
                            &startup_trace,
                            pioneer_observability::DesktopStartupStage::GatewayRuntimeStateLoad,
                            || GatewayRuntime::load(client_core),
                        )?;
                        runtime.discover_and_adopt_existing_local_gateway_once(&startup_trace)?;
                        runtime.try_recover_active_local_gateway_once(&startup_trace)?;
                        let ws_connection_id = if runtime.setup_required() {
                            None
                        } else if let Some(endpoint) = runtime.active_gateway().cloned() {
                            let spec = crate::gateway::observe_startup_stage(
                                &startup_trace,
                                pioneer_observability::DesktopStartupStage::GatewayRuntimeConnectionPrepare,
                                || build_ws_connect_spec(&mut runtime, &endpoint),
                            )?;
                            Some(runtime.start_gateway_session_transport(spec, true)?)
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
