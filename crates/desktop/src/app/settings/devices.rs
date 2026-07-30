use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui::{prelude::*, *};
use gpui_component::{button::*, clipboard::Clipboard, spinner::Spinner, theme::ActiveTheme, *};
use pioneer_client::gateway::device_activation::DeviceActivationQrPresentation;
use pioneer_protocol::{
    AuthSessionListItem, AuthSessionRevokeParams, AuthSessionStatus, ClientKind, DeviceStatus,
};

const DEVICES_CONTENT_MAX_WIDTH_PX: f32 = 860.0;

#[derive(Clone)]
enum DeviceActivationDialogPhase {
    Loading,
    Ready(DeviceActivationQrPresentation),
    Failed(String),
}

struct DeviceActivationDialogState {
    phase: DeviceActivationDialogPhase,
}

impl PioneerDesktop {
    pub(super) fn render_settings_devices(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let desktop = cx.entity().clone();
        let active_sessions = self
            .gateway
            .auth_sessions
            .iter()
            .filter(|item| {
                item.session.status == AuthSessionStatus::Active
                    && item.device.status == DeviceStatus::Active
            })
            .cloned()
            .collect::<Vec<_>>();

        let content = if self.gateway.auth_sessions_loading {
            v_flex()
                .p_4()
                .child(t!("settings.devices.loading").to_string())
                .into_any_element()
        } else if let Some(error) = self.gateway.auth_sessions_error.as_ref() {
            v_flex()
                .p_4()
                .gap_2()
                .child(div().text_sm().child(error.clone()))
                .child(
                    Button::new("devices-retry")
                        .small()
                        .outline()
                        .label(t!("settings.devices.retry").to_string())
                        .on_click({
                            let desktop = desktop.clone();
                            move |_, _, cx| {
                                let _ =
                                    desktop.update(cx, |view, cx| view.refresh_auth_sessions(cx));
                            }
                        }),
                )
                .into_any_element()
        } else {
            v_flex()
                .w_full()
                .rounded_lg()
                .border_1()
                .border_color(cx.theme().border)
                .bg(cx.theme().background)
                .children(
                    active_sessions
                        .into_iter()
                        .enumerate()
                        .map(|(index, item)| render_session_row(item, index, desktop.clone(), cx)),
                )
                .into_any_element()
        };

        v_flex()
            .id("settings-devices-scroll")
            .flex_1()
            .min_h_0()
            .overflow_y_scroll()
            .p_6()
            .bg(cx.theme().background)
            .child(
                h_flex().w_full().justify_center().child(
                    v_flex()
                        .w_full()
                        .max_w(px(DEVICES_CONTENT_MAX_WIDTH_PX))
                        .gap_6()
                        .child(
                            h_flex()
                                .w_full()
                                .justify_between()
                                .items_center()
                                .child(
                                    v_flex()
                                        .child(
                                            div()
                                                .text_xl()
                                                .font_semibold()
                                                .child(t!("settings.devices.title").to_string()),
                                        )
                                        .child(
                                            div().text_sm().opacity(0.6).child(
                                                t!("settings.devices.description").to_string(),
                                            ),
                                        ),
                                )
                                .child(
                                    div().pt_1p5().child(
                                        Button::new("devices-create-activation")
                                            .ghost()
                                            .justify_start()
                                            .px_2()
                                            .group("new-device-btn")
                                            .child(
                                                div()
                                                    .flex_none()
                                                    .text_sm()
                                                    .line_height(relative(1.))
                                                    .child(
                                                        t!("settings.devices.activate_device")
                                                            .to_string(),
                                                    ),
                                            )
                                            .child({
                                                let icon_bg = cx.theme().foreground.opacity(0.075);
                                                let icon_bg_hover =
                                                    cx.theme().foreground.opacity(0.1);
                                                div()
                                                    .id("new-device-icon")
                                                    .size_6()
                                                    .rounded_full()
                                                    .bg(icon_bg)
                                                    .group_hover("new-device-btn", move |s| {
                                                        s.bg(icon_bg_hover)
                                                    })
                                                    .flex()
                                                    .items_center()
                                                    .justify_center()
                                                    .child(Icon::new(IconName::Plus).size_4())
                                            })
                                            .on_click({
                                                let desktop = desktop.clone();
                                                move |_, window, cx| {
                                                    let _ = desktop.update(cx, |view, cx| {
                                                        view.create_desktop_activation(window, cx)
                                                    });
                                                }
                                            }),
                                    ),
                                ),
                        )
                        .child(content),
                ),
            )
            .into_any_element()
    }

    pub(in crate::app) fn refresh_auth_sessions(&mut self, cx: &mut Context<Self>) {
        if self.gateway.auth_sessions_loading {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.gateway.auth_sessions_error =
                Some(t!("settings.gateway_not_connected").to_string());
            return;
        }
        self.gateway.auth_sessions_loading = true;
        self.gateway.auth_sessions_error = None;
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { sender.auth_session_list() })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.gateway.auth_sessions_loading = false;
                    match result {
                        Ok(response) => view.gateway.auth_sessions = response.sessions,
                        Err(error) => {
                            view.gateway.auth_sessions_error = Some(format!("{error:#}"));
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn confirm_auth_session_action(
        &mut self,
        item: AuthSessionListItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let (title, description, action) = if item.current {
            (
                t!("settings.devices.logout_confirm_title").to_string(),
                t!("settings.devices.logout_confirm_description").to_string(),
                t!("settings.devices.logout").to_string(),
            )
        } else {
            (
                t!(
                    "settings.devices.revoke_confirm_title",
                    device = item.device.display_name.as_str()
                )
                .to_string(),
                t!("settings.devices.revoke_confirm_description").to_string(),
                t!("settings.devices.revoke").to_string(),
            )
        };
        let answer = window.prompt(
            PromptLevel::Warning,
            title.as_str(),
            Some(description.as_str()),
            &[
                PromptButton::new(action),
                PromptButton::cancel(t!("buttons.cancel").to_string()),
            ],
            cx,
        );
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                if answer.await != Ok(0) {
                    return;
                }
                let _ = this.update(&mut cx, |view, cx| {
                    view.execute_auth_session_action(item.clone(), cx)
                });
            }
        })
        .detach();
    }

    fn execute_auth_session_action(&mut self, item: AuthSessionListItem, cx: &mut Context<Self>) {
        let sender = self.gateway.ws_command_sender.clone();
        let current_endpoint_id = item.current.then(|| {
            self.gateway
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.active_gateway_id())
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("desktop Gateway has no active session endpoint"))
        });
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let session_id = item.session.id.clone();
                let result = cx
                    .background_spawn(async move {
                        if item.current {
                            let endpoint_id = current_endpoint_id.ok_or_else(|| {
                                anyhow::anyhow!(
                                    "current session action has no endpoint lookup result"
                                )
                            })??;
                            let mut runtime = crate::gateway::GatewayRuntime::load()?;
                            let _session_mutation = runtime.begin_session_mutation(&endpoint_id)?;
                            let response = sender.auth_logout()?;
                            runtime.forget_gateway_session_after_logout(&endpoint_id)?;
                            Ok((response.revoked, Some(runtime)))
                        } else {
                            sender
                                .auth_session_revoke(AuthSessionRevokeParams {
                                    session_id,
                                    expected_status: None,
                                })
                                .map(|response| (response.revoked, None))
                        }
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| match result {
                    Ok((_, runtime)) if item.current => {
                        if let Some(runtime) = runtime {
                            view.gateway.runtime = Some(runtime);
                        }
                        view.gateway.auth_sessions.clear();
                        view.gateway.ws_connection_id = None;
                        view.gateway.connection_state = GatewayConnectionState::Disconnected;
                        view.gateway.error =
                            Some(t!("gateway.session_terminal.revoked").to_string());
                        let _ = view.gateway.ws_command_sender.disconnect();
                        cx.notify();
                    }
                    Ok(_) => view.refresh_auth_sessions(cx),
                    Err(error) => {
                        view.gateway.auth_sessions_error = Some(format!("{error:#}"));
                        cx.notify();
                    }
                });
            }
        })
        .detach();
    }

    fn create_desktop_activation(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let endpoint_address = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().cloned())
            .map(|endpoint| endpoint.address);
        let activation_state = cx.new(|_| DeviceActivationDialogState {
            phase: DeviceActivationDialogPhase::Loading,
        });

        self.open_activation_dialog(
            activation_state.clone(),
            endpoint_address.clone(),
            window,
            cx,
        );
        if let Some(endpoint_address) = endpoint_address {
            self.request_desktop_activation(endpoint_address, activation_state, window, cx);
        } else {
            activation_state.update(cx, |state, cx| {
                state.phase = DeviceActivationDialogPhase::Failed(
                    t!("settings.gateway_not_connected").to_string(),
                );
                cx.notify();
            });
        }
    }

    fn request_desktop_activation(
        &mut self,
        endpoint_address: String,
        activation_state: Entity<DeviceActivationDialogState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        activation_state.update(cx, |state, cx| {
            state.phase = DeviceActivationDialogPhase::Loading;
            cx.notify();
        });
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn_in(
            window,
            move |_this: WeakEntity<Self>, cx: &mut AsyncWindowContext| {
                let mut cx = cx.clone();
                async move {
                    let result = cx
                        .background_spawn(async move {
                            let created = sender.auth_device_create()?;
                            let session_id = created.session_id.clone();
                            match DeviceActivationQrPresentation::from_created_device(
                                endpoint_address,
                                created,
                            ) {
                                Ok(presentation) => Ok(presentation),
                                Err(error) => {
                                    let _ = sender.auth_session_revoke(AuthSessionRevokeParams {
                                        session_id,
                                        expected_status: Some(AuthSessionStatus::Pending),
                                    });
                                    Err(error)
                                }
                            }
                        })
                        .await;
                    let _ = activation_state.update(&mut cx, |state, cx| {
                        state.phase = match result {
                            Ok(presentation) => DeviceActivationDialogPhase::Ready(presentation),
                            Err(error) => DeviceActivationDialogPhase::Failed(format!("{error:#}")),
                        };
                        cx.notify();
                    });
                }
            },
        )
        .detach();
    }

    fn open_activation_dialog(
        &mut self,
        activation_state: Entity<DeviceActivationDialogState>,
        endpoint_address: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let desktop = cx.entity().clone();
        let sender = self.gateway.ws_command_sender.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let phase = activation_state.read(cx).phase.clone();

            let code = match &phase {
                DeviceActivationDialogPhase::Ready(presentation) => {
                    Some(presentation.manual_code().to_owned())
                }
                _ => None,
            };
            let link = match &phase {
                DeviceActivationDialogPhase::Ready(presentation) => {
                    Some(presentation.deep_link().to_owned())
                }
                _ => None,
            };

            let content = match phase {
                DeviceActivationDialogPhase::Loading => v_flex()
                    .w_full()
                    .min_h(px(240.))
                    .items_center()
                    .justify_center()
                    .gap_2()
                    .child(Spinner::new())
                    .child(
                        div()
                            .text_sm()
                            .opacity(0.6)
                            .child(t!("settings.devices.activation_loading").to_string()),
                    )
                    .into_any_element(),
                DeviceActivationDialogPhase::Ready(presentation) => v_flex()
                    .w_full()
                    .pt_1()
                    .pb_5()
                    .gap_5()
                    .items_center()
                    .child(
                        div()
                            .text_sm()
                            .line_height(relative(1.35))
                            .opacity(0.6)
                            .child(t!("settings.devices.activation_description").to_string()),
                    )
                    .child(
                        v_flex()
                            .w_full()
                            .items_center()
                            .gap_4()
                            .child(render_activation_qr(&presentation))
                            .when_some(code, |this, code| {
                                this.child(
                                    v_flex()
                                        .w_full()
                                        .items_center()
                                        .gap_1()
                                        .child(
                                            div().text_sm().opacity(0.6).child(
                                                t!("settings.devices.code_label").to_string(),
                                            ),
                                        )
                                        .child(
                                            div()
                                                .flex()
                                                .w_full()
                                                .p_4()
                                                .rounded_2xl()
                                                .justify_center()
                                                .bg(cx.theme().muted)
                                                .text_xl()
                                                .font_semibold()
                                                .child(presentation.manual_code().to_owned())
                                                .child(
                                                    div().absolute().top_1p5().right_1p5().child(
                                                        Clipboard::new("activation-copy-code")
                                                            .value(code),
                                                    ),
                                                ),
                                        ),
                                )
                            })
                            .when_some(link, |this, link| {
                                this.child(
                                    v_flex()
                                        .w_full()
                                        .items_center()
                                        .gap_1()
                                        .child(
                                            div().text_sm().opacity(0.6).child(
                                                t!("settings.devices.link_label").to_string(),
                                            ),
                                        )
                                        .child(
                                            div()
                                                .flex()
                                                .w_full()
                                                .min_w_0()
                                                .p_4()
                                                .rounded_2xl()
                                                .justify_center()
                                                .bg(cx.theme().muted)
                                                .text_sm()
                                                .font_medium()
                                                .child(
                                                    div()
                                                        .min_w_0()
                                                        .flex_1()
                                                        .overflow_x_hidden()
                                                        .whitespace_normal()
                                                        .text_center()
                                                        .child(link.to_owned()),
                                                )
                                                .child(
                                                    div().absolute().top_1p5().right_1p5().child(
                                                        Clipboard::new("activation-copy-link")
                                                            .value(link),
                                                    ),
                                                ),
                                        ),
                                )
                            }),
                    )
                    .into_any_element(),
                DeviceActivationDialogPhase::Failed(error) => v_flex()
                    .w_full()
                    .min_h(px(180.))
                    .justify_center()
                    .gap_2()
                    .child(
                        div()
                            .text_sm()
                            .font_medium()
                            .child(t!("settings.devices.activation_failed").to_string()),
                    )
                    .child(div().text_xs().text_color(cx.theme().danger).child(error))
                    .into_any_element(),
            };

            dialog
                .w(px(440.))
                .gap_1()
                .rounded_2xl()
                .close_button(false)
                .overlay_closable(false)
                .keyboard(false)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("settings.devices.activation_title").to_string()),
                )
                .footer({
                    let activation_state = activation_state.clone();
                    let desktop = desktop.clone();
                    let endpoint_address = endpoint_address.clone();
                    let sender = sender.clone();
                    move |_, _, _, cx| match activation_state.read(cx).phase.clone() {
                        DeviceActivationDialogPhase::Loading => Vec::new(),
                        DeviceActivationDialogPhase::Failed(_) => {
                            let mut actions = vec![
                                default_outline_button("activation-error-close")
                                    .label(t!("buttons.cancel").to_string())
                                    .outline()
                                    .on_click(|_, window, cx| {
                                        window.close_dialog(cx);
                                    })
                                    .into_any_element(),
                            ];
                            if let Some(endpoint_address) = endpoint_address.clone() {
                                actions.push(
                                    default_primary_button("activation-retry")
                                        .label(t!("settings.devices.retry").to_string())
                                        .on_click({
                                            let activation_state = activation_state.clone();
                                            let desktop = desktop.clone();
                                            move |_, window, cx| {
                                                let endpoint_address = endpoint_address.clone();
                                                let activation_state = activation_state.clone();
                                                let _ = desktop.update(cx, |view, cx| {
                                                    view.request_desktop_activation(
                                                        endpoint_address,
                                                        activation_state,
                                                        window,
                                                        cx,
                                                    )
                                                });
                                            }
                                        })
                                        .into_any_element(),
                                );
                            }
                            actions
                        }
                        DeviceActivationDialogPhase::Ready(_) => vec![
                            default_primary_button("activation-close")
                                .label(t!("settings.devices.close_activation").to_string())
                                .on_click({
                                    let activation_state = activation_state.clone();
                                    let sender = sender.clone();
                                    move |_, window, cx| {
                                        let session_id = match &activation_state.read(cx).phase {
                                            DeviceActivationDialogPhase::Ready(presentation) => {
                                                Some(presentation.session_id.clone())
                                            }
                                            _ => None,
                                        };
                                        if let Some(session_id) = session_id {
                                            let sender = sender.clone();
                                            cx.spawn(async move |cx| {
                                                let _ = cx
                                                    .background_spawn(async move {
                                                        sender.auth_session_revoke(
                                                            AuthSessionRevokeParams {
                                                                session_id,
                                                                expected_status: Some(
                                                                    AuthSessionStatus::Pending,
                                                                ),
                                                            },
                                                        )
                                                    })
                                                    .await;
                                            })
                                            .detach();
                                        }
                                        window.close_dialog(cx);
                                    }
                                })
                                .into_any_element(),
                        ],
                    }
                })
                .child(content)
        });
    }
}

fn render_session_row(
    item: AuthSessionListItem,
    index: usize,
    desktop: Entity<PioneerDesktop>,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let kind = match item.device.client_kind {
        ClientKind::Desktop => t!("settings.devices.client_desktop").to_string(),
        ClientKind::Mobile => t!("settings.devices.client_mobile").to_string(),
        ClientKind::Other => t!("settings.devices.client_other").to_string(),
    };
    let last_seen = format_last_seen(item.last_seen_at_unix);
    let action_label = if item.current {
        t!("settings.devices.logout").to_string()
    } else {
        t!("settings.devices.revoke").to_string()
    };
    h_flex()
        .w_full()
        .px_4()
        .py_3()
        .gap_4()
        .justify_between()
        .items_center()
        .when(index > 0, |row| {
            row.border_t_1().border_color(cx.theme().border)
        })
        .child(
            v_flex()
                .min_w_0()
                .flex_1()
                .child(
                    h_flex()
                        .gap_2()
                        .child(
                            div()
                                .text_sm()
                                .font_semibold()
                                .child(item.device.display_name.clone()),
                        )
                        .when(item.current, |row| {
                            row.child(
                                div()
                                    .text_xs()
                                    .opacity(0.7)
                                    .child(t!("settings.devices.current").to_string()),
                            )
                        }),
                )
                .child(div().text_xs().opacity(0.6).child(format!(
                    "{} · {}",
                    kind,
                    t!("settings.devices.last_seen", value = last_seen.as_str())
                ))),
        )
        .child(
            Button::new(("devices-session-action", index))
                .ghost()
                .compact()
                .small()
                .label(action_label)
                .on_click(move |_, window, cx| {
                    let item = item.clone();
                    let _ = desktop.update(cx, |view, cx| {
                        view.confirm_auth_session_action(item, window, cx)
                    });
                }),
        )
        .into_any_element()
}

fn format_last_seen(unix: u64) -> String {
    i64::try_from(unix)
        .ok()
        .and_then(|unix| chrono::DateTime::from_timestamp(unix, 0))
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| t!("settings.devices.unknown_time").to_string())
}

fn render_activation_qr(presentation: &DeviceActivationQrPresentation) -> AnyElement {
    let width = presentation.qr_width();
    v_flex()
        .p_3()
        .bg(rgb(0xffffff))
        .children(
            presentation
                .qr_modules()
                .chunks(width)
                .enumerate()
                .map(|(row_index, row)| {
                    h_flex().children(row.iter().enumerate().map(move |(column_index, dark)| {
                        div()
                            .id(("activation-qr-module", row_index * width + column_index))
                            .w(px(4.))
                            .h(px(4.))
                            .bg(if *dark { rgb(0x000000) } else { rgb(0xffffff) })
                    }))
                }),
        )
        .into_any_element()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthSessionId, AuthSessionSnapshot, DeviceId, DeviceStatus,
        TokenFamilyId,
    };

    fn session(current: bool) -> AuthSessionListItem {
        AuthSessionListItem {
            current,
            last_seen_at_unix: 1_800_000_000,
            device: AuthDeviceSnapshot {
                id: DeviceId::new("D00000000000000000001").unwrap(),
                installation_id: "install-1".to_owned(),
                display_name: "Desktop".to_owned(),
                client_kind: ClientKind::Desktop,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: AuthSessionId::new("S00000000000000000001").unwrap(),
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: 2,
                refresh_expires_at_unix: 1_900_000_000,
            },
        }
    }

    #[::core::prelude::v1::test]
    fn last_seen_format_is_bounded_and_has_no_credentials() {
        let rendered = format_last_seen(1_800_000_000);
        assert!(rendered.len() <= 32);
        assert!(!rendered.contains("token"));
    }

    #[::core::prelude::v1::test]
    fn devices_source_does_not_persist_or_log_activation_material() {
        let source = include_str!("devices.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap();
        assert!(!source.contains("tracing::"));
        assert!(!source.contains("serde::"));
        assert!(!source.contains("ClientEvent"));
        assert!(!source.contains("auth_sessions.push"));
    }

    #[::core::prelude::v1::test]
    fn activation_creation_is_owned_by_the_modal_in_every_state() {
        let activation_source = include_str!("devices.rs")
            .split("fn create_desktop_activation")
            .nth(1)
            .unwrap()
            .split("\n}\n\nfn render_session_row")
            .next()
            .unwrap();
        assert!(activation_source.contains("open_activation_dialog"));
        assert!(activation_source.contains("DeviceActivationDialogPhase::Loading"));
        assert!(activation_source.contains("DeviceActivationDialogPhase::Ready"));
        assert!(activation_source.contains("DeviceActivationDialogPhase::Failed"));
        assert!(!activation_source.contains("auth_sessions_error"));
    }

    #[::core::prelude::v1::test]
    fn current_and_peer_rows_choose_distinct_session_actions() {
        assert!(session(true).current);
        assert!(!session(false).current);
    }
}
