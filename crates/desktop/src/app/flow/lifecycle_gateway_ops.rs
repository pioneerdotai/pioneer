use super::*;

impl PioneerDesktop {
    pub(in crate::app) fn connect_remote_gateway_from_values(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        source: GatewayOperationSource,
        name: String,
        address: String,
        token: String,
        form_state: Option<Entity<GatewaySetupFormState>>,
    ) {
        if source == GatewayOperationSource::AddGatewayDialog {
            self.connect_remote_gateway_from_add_dialog(
                window, cx, name, address, token, form_state,
            );
            return;
        }

        let ws_sender = self.gateway.ws_command_sender.clone();

        let Some(operation_epoch) = self.begin_gateway_operation(
            t!("gateway.status.connecting_remote").to_string(),
            Some(GatewaySetupAction::ConnectRemote),
            cx,
        ) else {
            return;
        };
        let threads_to_unsubscribe = self.prepare_gateway_switch(cx);
        self.sync_gateway_setup_form_state(form_state.as_ref(), cx);

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
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
                            let spec = build_ws_connect_spec(&runtime, &endpoint)?;
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

                    let _ = this.update_in(&mut cx, |view, window, cx| match result {
                        Ok(success) => {
                            view.finish_gateway_operation(operation_epoch, Ok(success), cx);
                            if let Some(form_state) = form_state.as_ref() {
                                form_state.update(cx, |state, cx| {
                                    state.clear_inputs(window, cx);
                                });
                            }
                            if source.close_dialog_on_success() {
                                window.close_dialog(cx);
                            }
                        }
                        Err(error) => {
                            view.finish_gateway_operation(operation_epoch, Err(error), cx);
                            view.sync_gateway_setup_form_state(form_state.as_ref(), cx);
                        }
                    });
                }
            },
        )
        .detach();
    }

    fn connect_remote_gateway_from_add_dialog(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        name: String,
        address: String,
        token: String,
        form_state: Option<Entity<GatewaySetupFormState>>,
    ) {
        let ws_sender = self.gateway.ws_command_sender.clone();

        let Some(operation_epoch) = self.begin_gateway_operation(
            t!("gateway.status.connecting_remote").to_string(),
            Some(GatewaySetupAction::ConnectRemote),
            cx,
        ) else {
            return;
        };
        self.sync_gateway_setup_form_state(form_state.as_ref(), cx);

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                let ws_sender = ws_sender.clone();

                async move {
                    let staged_result = cx
                        .background_spawn(async move {
                            let mut runtime = GatewayRuntime::load()?;
                            validate_remote_candidate_gateway_connection(
                                &runtime,
                                name.as_str(),
                                address.as_str(),
                                token.as_str(),
                            )?;
                            let endpoint = runtime.add_remote_gateway(
                                name.as_str(),
                                address.as_str(),
                                Some(token.as_str()),
                            )?;
                            let spec = build_ws_connect_spec(&runtime, &endpoint)?;

                            Ok::<(GatewayRuntime, String, GatewayWsConnectSpec), anyhow::Error>((
                                runtime,
                                endpoint.id,
                                spec,
                            ))
                        })
                        .await;

                    let (mut runtime, endpoint_id, spec) = match staged_result {
                        Ok(staged) => staged,
                        Err(error) => {
                            let _ = this.update_in(&mut cx, |view, _window, cx| {
                                view.finish_add_gateway_form_error_without_switch(
                                    operation_epoch,
                                    error,
                                    form_state.as_ref(),
                                    cx,
                                );
                            });
                            return;
                        }
                    };

                    let mut threads_to_unsubscribe = None;
                    let _ = this.update_in(&mut cx, |view, _window, cx| {
                        if should_apply_gateway_operation_result(
                            view.gateway.connection_epoch,
                            operation_epoch,
                        ) {
                            threads_to_unsubscribe = Some(view.prepare_gateway_switch(cx));
                            view.sync_gateway_setup_form_state(form_state.as_ref(), cx);
                        }
                    });

                    let Some(threads_to_unsubscribe) = threads_to_unsubscribe else {
                        return;
                    };

                    let result = cx
                        .background_spawn(async move {
                            for thread_id in threads_to_unsubscribe {
                                let _ = ws_sender.thread_unsubscribe(thread_id);
                            }
                            let connection_id = ws_sender.connect_and_wait(spec)?;
                            runtime.activate_gateway(endpoint_id.as_str())?;

                            Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                                runtime,
                                ws_connection_id: Some(connection_id),
                                ws_connected_ready: true,
                                install_warnings: Vec::new(),
                            })
                        })
                        .await;

                    let _ = this.update_in(&mut cx, |view, window, cx| match result {
                        Ok(success) => {
                            view.finish_gateway_operation(operation_epoch, Ok(success), cx);
                            if let Some(form_state) = form_state.as_ref() {
                                form_state.update(cx, |state, cx| {
                                    state.clear_inputs(window, cx);
                                });
                            }
                            window.close_dialog(cx);
                        }
                        Err(error) => {
                            view.finish_gateway_operation(operation_epoch, Err(error), cx);
                            view.sync_gateway_setup_form_state(form_state.as_ref(), cx);
                        }
                    });
                }
            },
        )
        .detach();
    }

    pub(in crate::app) fn start_local_gateway_from_form(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        source: GatewayOperationSource,
        form_state: Option<Entity<GatewaySetupFormState>>,
    ) {
        let ws_sender = self.gateway.ws_command_sender.clone();

        let Some(operation_epoch) = self.begin_gateway_operation(
            t!("gateway.status.starting_local").to_string(),
            Some(GatewaySetupAction::StartLocal),
            cx,
        ) else {
            return;
        };
        let threads_to_unsubscribe = self.prepare_gateway_switch(cx);
        self.sync_gateway_setup_form_state(form_state.as_ref(), cx);

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
                            view.sync_gateway_setup_form_state(form_state.as_ref(), cx);
                        });
                    } else if local_update_required {
                        let _ = this.update_in(&mut cx, |view, _window, cx| {
                            view.update_gateway_operation_status(
                                operation_epoch,
                                t!("gateway.status.updating").to_string(),
                                cx,
                            );
                            view.sync_gateway_setup_form_state(form_state.as_ref(), cx);
                        });
                    }

                    let result = cx
                        .background_spawn(async move {
                            for thread_id in threads_to_unsubscribe {
                                let _ = ws_sender.thread_unsubscribe(thread_id);
                            }
                            let mut runtime = GatewayRuntime::load()?;
                            let local_start = runtime.ensure_local_gateway_started()?;
                            let spec = build_ws_connect_spec(&runtime, &local_start.endpoint)?;
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
                            if let Some(form_state) = form_state.as_ref() {
                                form_state.update(cx, |state, cx| {
                                    state.clear_inputs(window, cx);
                                });
                            }
                            view.push_install_warnings_notification(
                                install_warnings.as_slice(),
                                window,
                                cx,
                            );
                            if source.close_dialog_on_success() {
                                window.close_dialog(cx);
                            }
                        }
                        Err(error) => {
                            view.finish_gateway_operation(operation_epoch, Err(error), cx);
                            view.sync_gateway_setup_form_state(form_state.as_ref(), cx);
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
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        let active_gateway_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway_id());

        if gateway_activation_is_noop(
            active_gateway_id,
            gateway_id.as_str(),
            self.gateway.connection_state,
            self.gateway.ws_connection_id,
        ) {
            return false;
        }

        let endpoint_kind = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.endpoint(gateway_id.as_str()))
            .map(|endpoint| endpoint.kind);
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

        cx.spawn_in(
            window,
            move |this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                let ws_sender = ws_sender.clone();

                async move {
                    if gateway_activation_requires_local_start(endpoint_kind) {
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
                    }

                    let result = cx
                        .background_spawn(async move {
                            for thread_id in threads_to_unsubscribe {
                                let _ = ws_sender.thread_unsubscribe(thread_id);
                            }
                            let mut runtime = GatewayRuntime::load()?;
                            let mut endpoint =
                                runtime.endpoint(gateway_id.as_str()).ok_or_else(|| {
                                    anyhow!(
                                        "{}",
                                        t!("errors.gateway.id_not_found", id = gateway_id.as_str())
                                    )
                                })?;
                            let mut install_warnings = Vec::new();
                            if endpoint.kind == GatewayEndpointKind::Local {
                                let local_start = runtime.ensure_local_gateway_started()?;
                                endpoint = local_start.endpoint;
                                install_warnings = local_start.warnings;
                            }
                            let spec = build_ws_connect_spec(&runtime, &endpoint)?;
                            let (connection_id, ws_connected_ready) =
                                if endpoint.kind == GatewayEndpointKind::Remote {
                                    (ws_sender.connect_with_retry(spec)?, false)
                                } else {
                                    (ws_sender.connect_and_wait(spec)?, true)
                                };
                            runtime.activate_gateway(endpoint.id.as_str())?;

                            Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                                runtime,
                                ws_connection_id: Some(connection_id),
                                ws_connected_ready,
                                install_warnings,
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

        true
    }

    pub(in crate::app) fn sync_gateway_setup_form_state(
        &self,
        form_state: Option<&Entity<GatewaySetupFormState>>,
        cx: &mut Context<Self>,
    ) {
        if let Some(form_state) = form_state {
            let operation = self.gateway_setup_dialog_state();
            form_state.update(cx, |state, cx| {
                state.set_operation_state(operation, cx);
            });
        }
    }

    fn finish_add_gateway_form_error_without_switch(
        &mut self,
        operation_epoch: u64,
        error: anyhow::Error,
        form_state: Option<&Entity<GatewaySetupFormState>>,
        cx: &mut Context<Self>,
    ) {
        if !should_apply_gateway_operation_result(self.gateway.connection_epoch, operation_epoch) {
            return;
        }

        self.gateway.connecting = false;
        self.gateway.setup_action = None;
        self.refresh_gateway_status();
        let error = format!("{error:#}");
        if let Some(form_state) = form_state {
            let operation = self.gateway_setup_dialog_state().with_error(error);
            form_state.update(cx, |state, cx| {
                state.set_operation_state(operation, cx);
            });
        } else {
            self.gateway.error = Some(error);
        }
        cx.notify();
    }
}
