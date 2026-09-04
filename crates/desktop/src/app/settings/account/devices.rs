use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    components::{
        buttonts::{default_outline_button, default_primary_button},
        device_activation_form::{DeviceActivationForm, DeviceActivationFormPhase},
    },
};
use gpui_kit::component::{button::*, dialog::DialogFooter, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    authorization::{SessionStatusPresentation, session_list_row_presentation},
    gateway::{device_activation::DeviceActivationQrPresentation, endpoint::GatewayBaseUrl},
};
use pioneer_protocol::{
    AuthSessionListItem, AuthSessionRevokeParams, AuthSessionStatus, ClientKind,
};

struct DeviceActivationDialogState {
    phase: DeviceActivationFormPhase,
}

impl PioneerDesktop {
    pub(super) fn render_auth_sessions_content(
        &self,
        desktop: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let sessions = self.gateway.auth_sessions.clone();
        if self.gateway.auth_sessions_loading {
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
                .children(sessions.into_iter().enumerate().map(|(index, item)| {
                    let pending =
                        self.gateway.auth_session_action_pending.as_ref() == Some(&item.session.id);
                    render_session_row(item, index, pending, desktop.clone(), cx)
                }))
                .into_any_element()
        }
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
        if self.gateway.auth_session_action_pending.is_some() {
            return;
        }
        self.gateway.auth_session_action_pending = Some(item.session.id.clone());
        self.gateway.auth_sessions_error = None;
        cx.notify();
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
                        view.gateway.auth_session_action_pending = None;
                        if let Some(runtime) = runtime {
                            view.gateway.runtime = Some(runtime);
                        }
                        view.clear_authorization_epoch_cache();
                        view.gateway.current_auth = None;
                        view.administration.clear_for_session_termination();
                        view.member_avatar_state.clear();
                        view.member_workspaces_saving = false;
                        view.gateway.auth_sessions.clear();
                        view.gateway.ws_connection_id = None;
                        view.gateway.connection_state = GatewayConnectionState::Disconnected;
                        view.gateway.error =
                            Some(t!("gateway.session_terminal.revoked").to_string());
                        let _ = view.gateway.ws_command_sender.disconnect();
                        cx.notify();
                    }
                    Ok(_) => {
                        view.gateway.auth_session_action_pending = None;
                        view.refresh_auth_sessions(cx)
                    }
                    Err(error) => {
                        view.gateway.auth_session_action_pending = None;
                        view.gateway.auth_sessions_error = Some(format!("{error:#}"));
                        cx.notify();
                    }
                });
            }
        })
        .detach();
    }

    pub(super) fn create_desktop_activation(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let endpoint_address = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().cloned())
            .map(|endpoint| endpoint.gateway_base_url);
        let activation_state = cx.new(|_| DeviceActivationDialogState {
            phase: DeviceActivationFormPhase::Loading,
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
                state.phase = DeviceActivationFormPhase::Failed(
                    t!("settings.gateway_not_connected").to_string(),
                );
                cx.notify();
            });
        }
    }

    fn request_desktop_activation(
        &mut self,
        endpoint_address: GatewayBaseUrl,
        activation_state: Entity<DeviceActivationDialogState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        activation_state.update(cx, |state, cx| {
            state.phase = DeviceActivationFormPhase::Loading;
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
                                &endpoint_address,
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
                            Ok(presentation) => DeviceActivationFormPhase::Ready(presentation),
                            Err(error) => DeviceActivationFormPhase::Failed(format!("{error:#}")),
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
        endpoint_address: Option<GatewayBaseUrl>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let desktop = cx.entity().clone();
        let sender = self.gateway.ws_command_sender.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let phase = activation_state.read(cx).phase.clone();
            let content = DeviceActivationForm::new(
                phase,
                t!("settings.devices.activation_description").to_string(),
            );

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
                .footer(DialogFooter::new().children({
                    let activation_state = activation_state.clone();
                    let desktop = desktop.clone();
                    let endpoint_address = endpoint_address.clone();
                    let sender = sender.clone();
                    match activation_state.read(cx).phase.clone() {
                        DeviceActivationFormPhase::Loading => Vec::new(),
                        DeviceActivationFormPhase::Failed(_) => {
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
                        DeviceActivationFormPhase::Ready(_) => vec![
                            default_primary_button("activation-close")
                                .label(t!("settings.devices.close_activation").to_string())
                                .on_click({
                                    let activation_state = activation_state.clone();
                                    let sender = sender.clone();
                                    move |_, window, cx| {
                                        let session_id = match &activation_state.read(cx).phase {
                                            DeviceActivationFormPhase::Ready(presentation) => {
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
                }))
                .child(content)
        });
    }
}

fn render_session_row(
    item: AuthSessionListItem,
    index: usize,
    pending: bool,
    desktop: Entity<PioneerDesktop>,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let kind = match item.device.client_kind {
        ClientKind::Desktop => t!("settings.devices.client_desktop").to_string(),
        ClientKind::Mobile => t!("settings.devices.client_mobile").to_string(),
        ClientKind::Other => t!("settings.devices.client_other").to_string(),
    };
    let last_seen = format_last_seen(item.last_seen_at_unix);
    let presentation = session_list_row_presentation(&item);
    let status = match presentation.status {
        SessionStatusPresentation::Active => t!("settings.devices.status_active").to_string(),
        SessionStatusPresentation::Pending => t!("settings.devices.status_pending").to_string(),
        SessionStatusPresentation::Expired => t!("settings.devices.status_expired").to_string(),
        SessionStatusPresentation::Revoked => t!("settings.devices.status_revoked").to_string(),
    };
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
                    "{} · {} · {}",
                    kind,
                    status,
                    t!("settings.devices.last_seen", value = last_seen.as_str())
                ))),
        )
        .child(
            Button::new(("devices-session-action", index))
                .ghost()
                .compact()
                .small()
                .disabled(pending || !presentation.actionable)
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
        assert!(activation_source.contains("DeviceActivationForm::new"));
        assert!(activation_source.contains("DeviceActivationFormPhase::Loading"));
        assert!(activation_source.contains("DeviceActivationFormPhase::Ready"));
        assert!(activation_source.contains("DeviceActivationFormPhase::Failed"));
        assert!(!activation_source.contains("auth_sessions_error"));
    }

    #[::core::prelude::v1::test]
    fn current_and_peer_rows_choose_distinct_session_actions() {
        assert!(session(true).current);
        assert!(!session(false).current);
    }

    #[::core::prelude::v1::test]
    fn every_authoritative_session_state_has_a_non_color_label() {
        let mut pending = session(false);
        pending.session.status = AuthSessionStatus::Pending;
        pending.device.status = DeviceStatus::Pending;
        let mut expired = session(false);
        expired.session.status = AuthSessionStatus::Expired;
        let mut revoked = session(false);
        revoked.session.status = AuthSessionStatus::Revoked;
        revoked.device.status = DeviceStatus::Revoked;
        assert_eq!(
            session_list_row_presentation(&session(true)).status,
            SessionStatusPresentation::Active
        );
        assert_eq!(
            session_list_row_presentation(&pending).status,
            SessionStatusPresentation::Pending
        );
        assert_eq!(
            session_list_row_presentation(&expired).status,
            SessionStatusPresentation::Expired
        );
        assert_eq!(
            session_list_row_presentation(&revoked).status,
            SessionStatusPresentation::Revoked
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_session_rows_use_the_shared_presentation_owner() {
        let source = include_str!("devices.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap();
        assert!(source.contains("session_list_row_presentation(&item)"));
        assert!(!source.contains("fn session_status_presentation"));
    }
}
