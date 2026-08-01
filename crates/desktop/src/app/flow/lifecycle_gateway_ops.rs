use super::*;

impl PioneerDesktop {
    pub(in crate::app) fn connect_remote_gateway_from_values(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        source: GatewayOperationSource,
        name: String,
        gateway_base_url: String,
        activation_code: String,
        form_state: Option<Entity<GatewaySetupFormState>>,
    ) {
        if source == GatewayOperationSource::AddGatewayDialog {
            self.connect_remote_gateway_from_add_dialog(
                window,
                cx,
                name,
                gateway_base_url,
                activation_code,
                form_state,
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
                            execute_gateway_command_client_effects(
                                &ws_sender,
                                threads_to_unsubscribe,
                            );
                            let mut runtime = GatewayRuntime::load()?;
                            let endpoint = runtime.add_remote_gateway(
                                name.as_str(),
                                gateway_base_url.as_str(),
                                Some(activation_code.as_str()),
                            )?;
                            let spec = build_ws_connect_spec(&mut runtime, &endpoint)?;
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

    pub(in crate::app) fn reauthenticate_remote_gateway_from_form(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        endpoint_id: String,
        activation_code: String,
        form_state: Option<Entity<GatewaySetupFormState>>,
        close_dialog_on_success: bool,
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
                            let mut runtime =
                                GatewayRuntime::load().map_err(|error| (None, error))?;
                            let endpoint = runtime
                                .reauthenticate_remote_gateway(
                                    endpoint_id.as_str(),
                                    activation_code.as_str(),
                                )
                                .map_err(|error| (None, error))?;
                            let spec = match build_ws_connect_spec(&mut runtime, &endpoint) {
                                Ok(spec) => spec,
                                Err(error) => return Err((Some(runtime), error)),
                            };

                            Ok::<
                                (GatewayRuntime, String, GatewayWsConnectSpec),
                                (Option<GatewayRuntime>, anyhow::Error),
                            >((runtime, endpoint.id, spec))
                        })
                        .await;

                    let (mut runtime, endpoint_id, spec) = match staged_result {
                        Ok(staged) => staged,
                        Err((durable_runtime, error)) => {
                            let _ = this.update_in(&mut cx, |view, window, cx| {
                                if let Some(runtime) = durable_runtime
                                    && should_apply_gateway_operation_result(
                                        view.gateway.connection_epoch,
                                        operation_epoch,
                                    )
                                {
                                    // Activation already committed a durable
                                    // session. Keep it and stop asking for the
                                    // consumed one-time code.
                                    view.gateway.runtime = Some(runtime);
                                    view.finish_gateway_form_error_without_switch(
                                        operation_epoch,
                                        error,
                                        None,
                                        cx,
                                    );
                                    if close_dialog_on_success {
                                        window.close_dialog(cx);
                                    }
                                } else {
                                    view.finish_gateway_form_error_without_switch(
                                        operation_epoch,
                                        error,
                                        form_state.as_ref(),
                                        cx,
                                    );
                                }
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
                            execute_gateway_command_client_effects(
                                &ws_sender,
                                threads_to_unsubscribe,
                            );
                            let connection_id = match ws_sender.connect_and_wait(spec) {
                                Ok(connection_id) => connection_id,
                                Err(error) => return Err((runtime, error.into())),
                            };
                            if let Err(error) = runtime.activate_gateway(endpoint_id.as_str()) {
                                return Err((runtime, error));
                            }

                            Ok::<GatewayOperationSuccess, (GatewayRuntime, anyhow::Error)>(
                                GatewayOperationSuccess {
                                    runtime,
                                    ws_connection_id: Some(connection_id),
                                    ws_connected_ready: true,
                                    install_warnings: Vec::new(),
                                },
                            )
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
                            if close_dialog_on_success {
                                window.close_dialog(cx);
                            }
                        }
                        Err((runtime, error)) => {
                            if should_apply_gateway_operation_result(
                                view.gateway.connection_epoch,
                                operation_epoch,
                            ) {
                                // Device activation and secure persistence have
                                // already succeeded. Keep that durable runtime
                                // in the UI even when the first ordinary
                                // connection fails, otherwise the same
                                // one-time code would be requested again.
                                view.gateway.runtime = Some(runtime);
                            }
                            view.finish_gateway_operation(operation_epoch, Err(error), cx);
                            view.sync_gateway_setup_form_state(form_state.as_ref(), cx);
                            if close_dialog_on_success {
                                window.close_dialog(cx);
                            }
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
        gateway_base_url: String,
        activation_code: String,
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
                            let endpoint = runtime.add_remote_gateway(
                                name.as_str(),
                                gateway_base_url.as_str(),
                                Some(activation_code.as_str()),
                            )?;
                            let spec = build_ws_connect_spec(&mut runtime, &endpoint)?;

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
                                view.finish_gateway_form_error_without_switch(
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
                            execute_gateway_command_client_effects(
                                &ws_sender,
                                threads_to_unsubscribe,
                            );
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
                            execute_gateway_command_client_effects(
                                &ws_sender,
                                threads_to_unsubscribe,
                            );
                            let mut runtime = GatewayRuntime::load()?;
                            let local_start = runtime.ensure_local_gateway_started()?;
                            let spec = build_local_ws_connect_spec_with_recovery(
                                &mut runtime,
                                &local_start.endpoint,
                            )?;
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

    pub(in crate::app) fn save_gateway_from_edit_dialog(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        endpoint_id: String,
        name: String,
        gateway_base_url: String,
        form_state: Option<Entity<GatewaySetupFormState>>,
    ) {
        let ws_sender = self.gateway.ws_command_sender.clone();

        let Some(operation_epoch) = self.begin_gateway_operation(
            t!("gateway.status.saving").to_string(),
            Some(GatewaySetupAction::SaveGateway),
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
                            let current =
                                runtime.endpoint(endpoint_id.as_str()).ok_or_else(|| {
                                    anyhow!(
                                        "{}",
                                        t!(
                                            "errors.gateway.id_not_found",
                                            id = endpoint_id.as_str()
                                        )
                                    )
                                })?;
                            if current.kind != GatewayEndpointKind::Remote {
                                anyhow::bail!("{}", t!("errors.gateway.remote_edit_only"));
                            }

                            let endpoint = runtime.update_remote_gateway(
                                endpoint_id.as_str(),
                                name.as_str(),
                                gateway_base_url.as_str(),
                            )?;
                            let was_active =
                                runtime.active_gateway_id() == Some(endpoint.id.as_str());
                            let spec = build_ws_connect_spec(&mut runtime, &endpoint)?;

                            Ok::<
                                (GatewayRuntime, GatewayEndpoint, GatewayWsConnectSpec, bool),
                                anyhow::Error,
                            >((runtime, endpoint, spec, was_active))
                        })
                        .await;

                    let (mut runtime, endpoint, spec, was_active) = match staged_result {
                        Ok(staged) => staged,
                        Err(error) => {
                            let _ = this.update_in(&mut cx, |view, _window, cx| {
                                view.finish_gateway_form_error_without_switch(
                                    operation_epoch,
                                    error,
                                    form_state.as_ref(),
                                    cx,
                                );
                            });
                            return;
                        }
                    };

                    if !was_active {
                        let _ = this.update_in(&mut cx, |view, window, cx| {
                            let ws_connection_id = view.gateway.ws_connection_id;
                            view.finish_gateway_operation(
                                operation_epoch,
                                Ok(GatewayOperationSuccess {
                                    runtime,
                                    ws_connection_id,
                                    ws_connected_ready: true,
                                    install_warnings: Vec::new(),
                                }),
                                cx,
                            );
                            window.close_dialog(cx);
                        });
                        return;
                    }

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
                            execute_gateway_command_client_effects(
                                &ws_sender,
                                threads_to_unsubscribe,
                            );

                            let ws_connection_id = match ws_sender.connect_and_wait(spec) {
                                Ok(connection_id) => Some(connection_id),
                                Err(error) => {
                                    warn!(
                                        error = %format!("{error:#}"),
                                        gateway_id = endpoint.id.as_str(),
                                        "saved active gateway but failed to reconnect"
                                    );
                                    None
                                }
                            };
                            runtime.activate_gateway(endpoint.id.as_str())?;

                            Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                                runtime,
                                ws_connection_id,
                                ws_connected_ready: true,
                                install_warnings: Vec::new(),
                            })
                        })
                        .await;

                    let _ = this.update_in(&mut cx, |view, window, cx| match result {
                        Ok(success) => {
                            view.finish_gateway_operation(operation_epoch, Ok(success), cx);
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

    pub(in crate::app) fn delete_gateway_from_edit_dialog(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        endpoint_id: String,
        form_state: Option<Entity<GatewaySetupFormState>>,
    ) {
        let ws_sender = self.gateway.ws_command_sender.clone();

        let Some(operation_epoch) = self.begin_gateway_operation(
            t!("gateway.status.deleting").to_string(),
            Some(GatewaySetupAction::DeleteGateway),
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
                            let outcome = runtime.delete_gateway(endpoint_id.as_str())?;

                            Ok::<_, anyhow::Error>((runtime, outcome))
                        })
                        .await;

                    let (runtime, outcome) = match staged_result {
                        Ok(staged) => staged,
                        Err(error) => {
                            let _ = this.update_in(&mut cx, |view, _window, cx| {
                                view.finish_gateway_form_error_without_switch(
                                    operation_epoch,
                                    error,
                                    form_state.as_ref(),
                                    cx,
                                );
                            });
                            return;
                        }
                    };

                    if !outcome.deleted_active {
                        let _ = this.update_in(&mut cx, |view, window, cx| {
                            let ws_connection_id = view.gateway.ws_connection_id;
                            view.finish_gateway_operation(
                                operation_epoch,
                                Ok(GatewayOperationSuccess {
                                    runtime,
                                    ws_connection_id,
                                    ws_connected_ready: true,
                                    install_warnings: Vec::new(),
                                }),
                                cx,
                            );
                            window.close_dialog(cx);
                        });
                        return;
                    }

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
                            execute_gateway_command_client_effects(&ws_sender, threads_to_unsubscribe);

                            let mut runtime = runtime;
                            let mut install_warnings = Vec::new();
                            let mut ws_connection_id = None;
                            let mut ws_connected_ready = true;

                            if let Some(mut endpoint) = outcome.fallback_endpoint {
                                let connect_result = (|| -> anyhow::Result<(Option<u64>, bool)> {
                                    if endpoint.kind == GatewayEndpointKind::Local {
                                        let local_start = runtime.ensure_local_gateway_started()?;
                                        endpoint = local_start.endpoint;
                                        install_warnings = local_start.warnings;
                                    }

                                    let spec = if endpoint.kind == GatewayEndpointKind::Local {
                                        build_local_ws_connect_spec_with_recovery(
                                            &mut runtime,
                                            &endpoint,
                                        )?
                                    } else {
                                        build_ws_connect_spec(&mut runtime, &endpoint)?
                                    };
                                    let connection_id = if endpoint.kind == GatewayEndpointKind::Remote {
                                        ws_connected_ready = false;
                                        ws_sender.connect_with_retry(spec)?
                                    } else {
                                        ws_sender.connect_and_wait(spec)?
                                    };
                                    runtime.activate_gateway(endpoint.id.as_str())?;
                                    Ok((Some(connection_id), ws_connected_ready))
                                })();

                                match connect_result {
                                    Ok((connection_id, connected_ready)) => {
                                        ws_connection_id = connection_id;
                                        ws_connected_ready = connected_ready;
                                    }
                                    Err(error) => {
                                        warn!(
                                            error = %format!("{error:#}"),
                                            "deleted active gateway but failed to connect fallback gateway"
                                        );
                                    }
                                }
                            } else {
                                let _ = ws_sender.disconnect();
                            }

                            Ok::<GatewayOperationSuccess, anyhow::Error>(GatewayOperationSuccess {
                                runtime,
                                ws_connection_id,
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
                            execute_gateway_command_client_effects(
                                &ws_sender,
                                threads_to_unsubscribe,
                            );
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
                            let spec = if endpoint.kind == GatewayEndpointKind::Local {
                                build_local_ws_connect_spec_with_recovery(&mut runtime, &endpoint)?
                            } else {
                                build_ws_connect_spec(&mut runtime, &endpoint)?
                            };
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

    fn finish_gateway_form_error_without_switch(
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
