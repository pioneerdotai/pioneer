use crate::{
    binding::SettingsBinding,
    buttons::{default_outline_button, default_primary_button},
    device_activation_form::{DeviceActivationForm, DeviceActivationFormPhase},
    screen::SettingsScreenView,
};
use gpui_kit::component::{button::*, dialog::DialogFooter, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    authorization::{SessionStatusPresentation, session_list_row_presentation},
    core::{ClientCore, ClientScope},
    settings::{
        device_activation::DeviceActivationPublication,
        runtime::SettingsIntent,
        types::{AuthSessionListItem, ClientKind},
    },
};
use std::sync::Arc;
impl SettingsScreenView {
    pub(super) fn render_auth_sessions_content(
        &self,
        desktop: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let snapshot = self.gateway.client_runtime.client_core().auth_sessions();
        let sessions = snapshot.sessions;
        if snapshot.loading {
            v_flex()
                .p_4()
                .child(t!("settings.devices.loading").to_string())
                .into_any_element()
        } else if let Some(error) = snapshot.error.as_ref() {
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
                    let pending = snapshot.revoking.as_ref() == Some(&item.session.id);
                    render_session_row(item, index, pending, desktop.clone(), cx)
                }))
                .into_any_element()
        }
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
        let expected_owner = self.config.client.auth_sessions().owner_generation;
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
        self.confirmation = Some(cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                if answer.await != Ok(0) {
                    return;
                }
                let _ = this.update(&mut cx, |view, cx| {
                    view.execute_auth_session_action(item.clone(), expected_owner, cx)
                });
            }
        }));
    }

    fn execute_auth_session_action(
        &mut self,
        item: AuthSessionListItem,
        expected_owner: u64,
        _: &mut Context<Self>,
    ) {
        self.config
            .client
            .settings_intent(SettingsIntent::RevokeSession {
                expected_owner,
                session_id: item.session.id,
                expected_status: Some(item.session.status),
            });
    }
    pub fn create_desktop_activation(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let client = self.config.client.clone();
        client.create_current_device_activation();
        let binding = SettingsBinding::new(
            vec![
                ClientScope::DeviceActivation,
                ClientScope::Administration { workspace_id: None },
            ],
            &self.config.bindings,
        );
        let content = cx.new(|cx| {
            let mut changed = binding.changed.subscribe();
            let task = cx.spawn(async move |view: WeakEntity<ActivationView>, cx| {
                while changed.changed().await.is_ok() {
                    if view
                        .update(cx, |view, cx| {
                            view.input = view.client.device_activation_publication();
                            cx.notify();
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            ActivationView {
                input: client.device_activation_publication(),
                client: client.clone(),
                _binding: binding,
                _task: task,
            }
        });
        window.open_dialog(cx, move |dialog, _, _| {
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
                .child(content.clone())
        });
    }
}
struct ActivationView {
    client: Arc<ClientCore>,
    input: DeviceActivationPublication,
    _binding: Arc<SettingsBinding>,
    _task: Task<()>,
}
impl Drop for ActivationView {
    fn drop(&mut self) {
        self.client.close_device_activation(self.input.generation);
    }
}
impl Render for ActivationView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let presentation = self
            .client
            .device_activation_presentation(self.input.generation);
        let phase = if self.input.loading {
            DeviceActivationFormPhase::Loading
        } else if let Some(p) = presentation {
            DeviceActivationFormPhase::Ready(p)
        } else {
            DeviceActivationFormPhase::Failed(
                if self.input.error.as_deref() == Some("gateway_not_connected") {
                    t!("settings.gateway_not_connected").to_string()
                } else {
                    t!("settings.devices.activation_failed").to_string()
                },
            )
        };
        let mut buttons = Vec::new();
        if self.input.ready {
            buttons.push(
                default_primary_button("activation-close")
                    .label(t!("settings.devices.close_activation").to_string())
                    .on_click(|_, window, cx| window.close_dialog(cx))
                    .into_any_element(),
            );
        } else if !self.input.loading {
            buttons.push(
                default_outline_button("activation-error-close")
                    .label(t!("buttons.cancel").to_string())
                    .on_click(|_, window, cx| window.close_dialog(cx))
                    .into_any_element(),
            );
            buttons.push(
                default_primary_button("activation-retry")
                    .label(t!("settings.devices.retry").to_string())
                    .on_click(cx.listener(|view, _, _, cx| {
                        view.client.create_current_device_activation();
                        view.input = view.client.device_activation_publication();
                        cx.notify();
                    }))
                    .into_any_element(),
            );
        }
        v_flex()
            .gap_1()
            .child(DeviceActivationForm::new(
                phase,
                t!("settings.devices.activation_description").to_string(),
            ))
            .child(DialogFooter::new().children(buttons))
    }
}
fn render_session_row(
    item: AuthSessionListItem,
    index: usize,
    pending: bool,
    desktop: Entity<SettingsScreenView>,
    cx: &mut Context<SettingsScreenView>,
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
            Button::new((
                ElementId::Name("devices-session-action".into()),
                SharedString::from(item.session.id.to_string()),
            ))
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
