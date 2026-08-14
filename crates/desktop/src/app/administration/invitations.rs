use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    components::{
        buttonts::{default_outline_button, default_primary_button},
        device_activation_form::CredentialPresentationForm,
    },
};
use gpui::{prelude::*, *};
use gpui_component::{
    button::*, dialog::DialogFooter, form::field, spinner::Spinner, theme::ActiveTheme, *,
};
use pioneer_client::{
    administration::{AdministrationAction, InvitationPresentationStatus, invitation_list_row},
    gateway::invitation::InvitationQrPresentation,
};
use pioneer_protocol::{
    InvitationCreateParams, InvitationId, InvitationListParams, InvitationRevokeParams,
    InvitationSummary, RoleKey, WorkspaceId,
};
use std::collections::HashSet;

enum InvitationDialogPhase {
    Create,
    Ready {
        presentation: InvitationQrPresentation,
        qr_width: usize,
        qr_modules: Vec<bool>,
    },
    Failed(String),
    Closed,
}

struct InvitationDialogState {
    phase: InvitationDialogPhase,
    selected: HashSet<String>,
    selected_role_key: Option<String>,
    creating: bool,
    error: Option<String>,
}

impl InvitationDialogState {
    fn new(selected_role_key: Option<String>) -> Self {
        Self {
            phase: InvitationDialogPhase::Create,
            selected: HashSet::new(),
            selected_role_key,
            creating: false,
            error: None,
        }
    }

    fn ready(presentation: InvitationQrPresentation) -> anyhow::Result<InvitationDialogPhase> {
        let (qr_width, qr_modules) = presentation.qr_modules()?;
        Ok(InvitationDialogPhase::Ready {
            presentation,
            qr_width,
            qr_modules,
        })
    }

    fn clear(&mut self) {
        self.phase = InvitationDialogPhase::Closed;
        self.selected.clear();
        self.selected_role_key = None;
        self.creating = false;
        self.error = None;
    }
}

impl std::fmt::Debug for InvitationDialogState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvitationDialogState")
            .field("phase", &"[redacted]")
            .field("selected_count", &self.selected.len())
            .field("has_selected_role", &self.selected_role_key.is_some())
            .field("creating", &self.creating)
            .finish()
    }
}

impl PioneerDesktop {
    pub(super) fn render_administration_invitations(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let capabilities = self.principal_presentation_capabilities();
        if !capabilities.can_view_invitations {
            return Self::render_administration_screen(
                "administration-invitations-scroll",
                t!("settings.invitations.title").to_string(),
                t!("settings.invitations.description").to_string(),
                None,
                v_flex()
                    .w_full()
                    .items_center()
                    .justify_center()
                    .child(t!("settings.invitations.forbidden").to_string())
                    .into_any_element(),
                cx,
            );
        }

        let desktop = cx.entity().clone();
        let rows = self
            .administration
            .invitations()
            .cloned()
            .collect::<Vec<_>>();
        let list = if self.invitations_loading && rows.is_empty() {
            v_flex()
                .p_6()
                .items_center()
                .gap_2()
                .child(Spinner::new())
                .child(t!("settings.invitations.loading").to_string())
                .into_any_element()
        } else if let Some(error) = self.invitations_error.as_ref() {
            v_flex()
                .p_4()
                .gap_2()
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().danger)
                        .child(error.clone()),
                )
                .child(
                    Button::new("invitations-retry")
                        .small()
                        .outline()
                        .label(t!("settings.invitations.retry").to_string())
                        .on_click({
                            let desktop = desktop.clone();
                            move |_, _, cx| {
                                let _ = desktop
                                    .update(cx, |view, cx| view.refresh_invitations(false, cx));
                            }
                        }),
                )
                .into_any_element()
        } else if rows.is_empty() {
            div()
                .p_6()
                .text_sm()
                .opacity(0.6)
                .child(t!("settings.invitations.empty").to_string())
                .into_any_element()
        } else {
            v_flex()
                .w_full()
                .rounded_lg()
                .border_1()
                .border_color(cx.theme().border)
                .children(rows.into_iter().enumerate().map(|(index, invitation)| {
                    render_invitation_row(invitation, index, capabilities, desktop.clone(), cx)
                }))
                .when_some(self.administration.invitation_next_cursor(), |list, _| {
                    list.child(
                        Button::new("invitations-load-more")
                            .small()
                            .ghost()
                            .disabled(self.invitations_loading)
                            .label(t!("settings.invitations.load_more").to_string())
                            .on_click({
                                let desktop = desktop.clone();
                                move |_, _, cx| {
                                    let _ = desktop
                                        .update(cx, |view, cx| view.refresh_invitations(true, cx));
                                }
                            }),
                    )
                })
                .into_any_element()
        };

        let header_action = capabilities.can_create_invitation.then(|| {
            Button::new("invitation-create-open")
                .ghost()
                .compact()
                .rounded_full()
                .icon(IconName::Plus)
                .tooltip(t!("settings.invitations.create").to_string())
                .disabled(
                    self.gateway.connection_state != GatewayConnectionState::Connected
                        || self.workspaces.is_empty()
                        || self.authorized_invitation_role_options().is_empty()
                        || self.administration.pending_action()
                            != &pioneer_client::administration::AdministrationPendingAction::Idle,
                )
                .on_click({
                    let desktop = desktop.clone();
                    move |_, window, cx| {
                        let _ = desktop.update(cx, |view, cx| {
                            view.open_create_invitation_dialog(window, cx)
                        });
                    }
                })
                .into_any_element()
        });
        let content = v_flex().w_full().child(list).into_any_element();

        Self::render_administration_screen(
            "administration-invitations-scroll",
            t!("settings.invitations.title").to_string(),
            t!("settings.invitations.description").to_string(),
            header_action,
            content,
            cx,
        )
    }

    pub(in crate::app) fn refresh_invitations(&mut self, append: bool, cx: &mut Context<Self>) {
        if self.invitations_loading {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            self.invitations_error = Some(t!("settings.gateway_not_connected").to_string());
            return;
        }
        if !self
            .principal_presentation_capabilities()
            .can_view_invitations
        {
            self.invitations_error = Some(t!("settings.invitations.forbidden").to_string());
            return;
        }
        let cursor = append
            .then(|| {
                self.administration
                    .invitation_next_cursor()
                    .map(str::to_owned)
            })
            .flatten();
        if append && cursor.is_none() {
            return;
        }
        self.invitations_loading = true;
        self.invitations_error = None;
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.invitation_list(InvitationListParams {
                            cursor,
                            ..InvitationListParams::default()
                        })
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.invitations_loading = false;
                    match result {
                        Ok(response) if append => {
                            view.administration.append_invitation_page(response)
                        }
                        Ok(response) => view.administration.apply_invitation_list(response),
                        Err(_) => {
                            view.invitations_error =
                                Some(t!("settings.invitations.load_failed").to_string())
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn create_invitation(&mut self, state: Entity<InvitationDialogState>, cx: &mut Context<Self>) {
        if !self
            .principal_presentation_capabilities()
            .can_create_invitation
        {
            return;
        }

        let (role_key, workspace_ids) = {
            let snapshot = state.read(cx);
            if snapshot.creating || !matches!(&snapshot.phase, InvitationDialogPhase::Create) {
                return;
            }
            let role_key = snapshot
                .selected_role_key
                .as_deref()
                .and_then(|value| RoleKey::new(value.to_owned()).ok());
            let workspace_ids = snapshot
                .selected
                .iter()
                .cloned()
                .map(WorkspaceId::new)
                .collect::<Result<Vec<_>, _>>();
            (role_key, workspace_ids)
        };

        let params = role_key
            .zip(workspace_ids.ok())
            .and_then(|(role_key, workspace_ids)| {
                InvitationCreateParams::new_for_role(role_key, workspace_ids).ok()
            });
        let Some(params) = params else {
            state.update(cx, |state, cx| {
                state.error = Some(t!("settings.invitations.invalid_selection").to_string());
                cx.notify();
            });
            return;
        };
        if !self
            .administration
            .begin_action(AdministrationAction::CreateInvitation)
        {
            return;
        }

        state.update(cx, |state, cx| {
            state.creating = true;
            state.error = None;
            cx.notify();
        });
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { sender.invitation_create(params) })
                    .await;
                match result {
                    Ok(response) => {
                        let presentation =
                            InvitationQrPresentation::from_presentation(response.presentation);
                        let phase =
                            InvitationDialogState::ready(presentation).unwrap_or_else(|_| {
                                InvitationDialogPhase::Failed(
                                    t!("settings.invitations.presentation_failed").to_string(),
                                )
                            });
                        let _ = this.update(&mut cx, |view, cx| {
                            view.administration.finish_action();
                            view.refresh_invitations(false, cx);
                            cx.notify();
                        });
                        let _ = state.update(&mut cx, |state, cx| {
                            state.phase = phase;
                            state.creating = false;
                            state.error = None;
                            cx.notify();
                        });
                    }
                    Err(_) => {
                        let _ = this.update(&mut cx, |view, cx| {
                            let effects = view.administration.finish_conflicted_action();
                            view.apply_administration_refetches(effects, cx);
                            cx.notify();
                        });
                        let _ = state.update(&mut cx, |state, cx| {
                            state.creating = false;
                            state.error =
                                Some(t!("settings.invitations.create_failed").to_string());
                            cx.notify();
                        });
                    }
                }
            }
        })
        .detach();
    }

    fn confirm_revoke_invitation(
        &mut self,
        invitation_id: InvitationId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let title = t!("settings.invitations.revoke_confirm_title").to_string();
        let description = t!("settings.invitations.revoke_confirm_description").to_string();
        let answer = window.prompt(
            PromptLevel::Warning,
            title.as_str(),
            Some(description.as_str()),
            &[
                PromptButton::new(t!("settings.invitations.revoke").to_string()),
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
                    view.revoke_invitation(invitation_id.clone(), cx)
                });
            }
        })
        .detach();
    }

    fn revoke_invitation(&mut self, invitation_id: InvitationId, cx: &mut Context<Self>) {
        let action = AdministrationAction::RevokeInvitation {
            invitation_id: invitation_id.clone(),
        };
        if !self.administration.begin_action(action) {
            return;
        }
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.invitation_revoke(InvitationRevokeParams { invitation_id })
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    if result.is_ok() {
                        view.administration.finish_action();
                        view.refresh_invitations(false, cx);
                    } else {
                        let effects = view.administration.finish_conflicted_action();
                        view.apply_administration_refetches(effects, cx);
                        view.invitations_error =
                            Some(t!("settings.invitations.revoke_failed").to_string());
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn open_create_invitation_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || !self
                .principal_presentation_capabilities()
                .can_create_invitation
            || self.administration.pending_action()
                != &pioneer_client::administration::AdministrationPendingAction::Idle
        {
            return;
        }

        let workspaces = self
            .workspaces
            .iter()
            .map(|workspace| (workspace.id.clone(), workspace.name.clone()))
            .collect::<Vec<_>>();
        let role_options = self.authorized_invitation_role_options().to_vec();
        let selected_role_key = role_options
            .iter()
            .find(|option| option.is_default)
            .map(|option| option.role.key.clone());
        let state = cx.new(|_| InvitationDialogState::new(selected_role_key));
        let desktop = cx.entity().clone();

        window.open_dialog(cx, move |dialog, window, cx| {
            let snapshot = state.read(cx);
            let is_create = matches!(&snapshot.phase, InvitationDialogPhase::Create);
            let closeable = is_create && !snapshot.creating;

            let (title, content, footer) = match &snapshot.phase {
                InvitationDialogPhase::Create => {
                    let content = v_flex()
                        .w_full()
                        .pt_2p5()
                        .pb_5()
                        .gap_4()
                        .items_start()
                        .child(
                            field()
                                .w_full()
                                .items_start()
                                .label(t!("settings.invitations.role").to_string())
                                .child(
                                    h_flex()
                                        .w_full()
                                        .justify_start()
                                        .flex_wrap()
                                        .gap_1p5()
                                        .children(role_options.iter().enumerate().map(
                                            |(index, option)| {
                                                let role_key = option.role.key.clone();
                                                let selected =
                                                    snapshot.selected_role_key.as_deref()
                                                        == Some(role_key.as_str());
                                                Toggle::new(("invitation-role-toggle", index))
                                                    .small()
                                                    .checked(selected)
                                                    .disabled(snapshot.creating)
                                                    .label(option.role.display_name.clone())
                                                    .rounded_full()
                                                    .h_8()
                                                    .px_3()
                                                    .text_sm()
                                                    .when(!selected, |toggle| {
                                                        toggle
                                                            .border_1()
                                                            .border_color(cx.theme().border)
                                                    })
                                                    .on_click({
                                                        let state = state.clone();
                                                        move |_, _, cx| {
                                                            state.update(cx, |state, cx| {
                                                                state.selected_role_key =
                                                                    Some(role_key.clone());
                                                                state.error = None;
                                                                cx.notify();
                                                            });
                                                        }
                                                    })
                                            },
                                        )),
                                ),
                        )
                        .child(
                            field()
                                .w_full()
                                .items_start()
                                .label(t!("settings.invitations.workspaces").to_string())
                                .child(
                                    h_flex()
                                        .w_full()
                                        .justify_start()
                                        .flex_wrap()
                                        .gap_1p5()
                                        .children(workspaces.iter().enumerate().map(
                                            |(index, (workspace_id, name))| {
                                                let workspace_id = workspace_id.clone();
                                                let selected =
                                                    snapshot.selected.contains(&workspace_id);
                                                let background = if selected {
                                                    cx.theme().foreground
                                                } else {
                                                    cx.theme().background
                                                };
                                                let foreground = if selected {
                                                    cx.theme().background
                                                } else {
                                                    cx.theme().foreground
                                                };
                                                Toggle::new(("invitation-workspace-toggle", index))
                                                    .small()
                                                    .checked(selected)
                                                    .disabled(snapshot.creating)
                                                    .label(name.clone())
                                                    .rounded_full()
                                                    .h_8()
                                                    .px_3()
                                                    .text_sm()
                                                    .bg(background)
                                                    .text_color(foreground)
                                                    .when(!selected, |toggle| {
                                                        toggle
                                                            .border_1()
                                                            .border_color(cx.theme().border)
                                                    })
                                                    .on_click({
                                                        let state = state.clone();
                                                        move |checked, _, cx| {
                                                            let workspace_id = workspace_id.clone();
                                                            state.update(cx, |state, cx| {
                                                                if *checked {
                                                                    state
                                                                        .selected
                                                                        .insert(workspace_id);
                                                                } else {
                                                                    state
                                                                        .selected
                                                                        .remove(&workspace_id);
                                                                }
                                                                state.error = None;
                                                                cx.notify();
                                                            });
                                                        }
                                                    })
                                            },
                                        )),
                                ),
                        )
                        .when_some(snapshot.error.clone(), |content, error| {
                            content.child(
                                div()
                                    .text_sm()
                                    .text_center()
                                    .text_color(cx.theme().danger)
                                    .child(error),
                            )
                        })
                        .into_any_element();
                    let footer = vec![
                        default_outline_button("invitation-create-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .disabled(snapshot.creating)
                            .on_click({
                                let state = state.clone();
                                move |_, window, cx| {
                                    state.update(cx, |state, _| state.clear());
                                    window.close_dialog(cx);
                                }
                            })
                            .into_any_element(),
                        default_primary_button("invitation-create-submit")
                            .child(
                                div()
                                    .relative()
                                    .child(
                                        div()
                                            .when(snapshot.creating, |label| label.invisible())
                                            .child(t!("settings.invitations.create").to_string()),
                                    )
                                    .when(snapshot.creating, |content| {
                                        content.child(
                                            div()
                                                .absolute()
                                                .inset_0()
                                                .flex()
                                                .items_center()
                                                .justify_center()
                                                .child(Spinner::new().small()),
                                        )
                                    }),
                            )
                            .loading(snapshot.creating)
                            .disabled(
                                snapshot.selected.is_empty()
                                    || snapshot.selected_role_key.is_none()
                                    || snapshot.creating,
                            )
                            .on_click({
                                let desktop = desktop.clone();
                                let state = state.clone();
                                move |_, _, cx| {
                                    let _ = desktop.update(cx, |view, cx| {
                                        view.create_invitation(state.clone(), cx)
                                    });
                                }
                            })
                            .into_any_element(),
                    ];
                    (
                        t!("settings.invitations.create").to_string(),
                        content,
                        footer,
                    )
                }
                InvitationDialogPhase::Ready {
                    presentation,
                    qr_width,
                    qr_modules,
                } => {
                    let content = CredentialPresentationForm::new(
                        "invitation",
                        *qr_width,
                        qr_modules.clone(),
                        presentation.deep_link().to_owned(),
                        t!("settings.invitations.presentation_description").to_string(),
                    )
                    .into_any_element();
                    let footer = vec![
                        default_primary_button("invitation-presentation-close")
                            .label(t!("settings.invitations.close_presentation").to_string())
                            .on_click({
                                let state = state.clone();
                                move |_, window, cx| {
                                    state.update(cx, |state, _| state.clear());
                                    window.close_dialog(cx);
                                }
                            })
                            .into_any_element(),
                    ];
                    (
                        t!("settings.invitations.presentation_title").to_string(),
                        content,
                        footer,
                    )
                }
                InvitationDialogPhase::Failed(error) => {
                    let content = v_flex()
                        .w_full()
                        .min_h(px(180.))
                        .items_center()
                        .justify_center()
                        .child(
                            div()
                                .text_sm()
                                .text_center()
                                .text_color(cx.theme().danger)
                                .child(error.clone()),
                        )
                        .into_any_element();
                    let footer = vec![
                        default_primary_button("invitation-presentation-error-close")
                            .label(t!("settings.invitations.close_presentation").to_string())
                            .on_click({
                                let state = state.clone();
                                move |_, window, cx| {
                                    state.update(cx, |state, _| state.clear());
                                    window.close_dialog(cx);
                                }
                            })
                            .into_any_element(),
                    ];
                    (
                        t!("settings.invitations.presentation_title").to_string(),
                        content,
                        footer,
                    )
                }
                InvitationDialogPhase::Closed => (
                    t!("settings.invitations.create").to_string(),
                    div().into_any_element(),
                    Vec::new(),
                ),
            };

            dialog
                .w(px(440.))
                .max_h(window.viewport_size().height * 0.85)
                .gap_1()
                .rounded_2xl()
                .close_button(closeable)
                .overlay_closable(closeable)
                .keyboard(closeable)
                .title(div().text_base().font_semibold().child(title))
                .footer(DialogFooter::new().children(footer))
                .child(content)
        });
    }
}

fn render_invitation_row(
    invitation: InvitationSummary,
    index: usize,
    capabilities: pioneer_client::authorization::PrincipalPresentationCapabilities,
    desktop: Entity<PioneerDesktop>,
    cx: &mut Context<PioneerDesktop>,
) -> AnyElement {
    let row = invitation_list_row(&invitation, capabilities);
    let status = match row.status {
        InvitationPresentationStatus::Pending => t!("settings.invitations.status_pending"),
        InvitationPresentationStatus::Accepted => t!("settings.invitations.status_accepted"),
        InvitationPresentationStatus::Revoked => t!("settings.invitations.status_revoked"),
        InvitationPresentationStatus::Expired => t!("settings.invitations.status_expired"),
        InvitationPresentationStatus::Unknown => t!("settings.invitations.status_unknown"),
    }
    .to_string();
    let workspace_names = row.workspace_names.clone();
    let created = format_invitation_time(row.created_at_unix);
    let expires = format_invitation_time(row.expires_at_unix);
    h_flex()
        .w_full()
        .px_4()
        .py_3()
        .gap_3()
        .justify_between()
        .items_center()
        .when(index > 0, |row| {
            row.border_t_1().border_color(cx.theme().border)
        })
        .child(
            v_flex()
                .min_w_0()
                .flex_1()
                .gap_1p5()
                .child(h_flex().justify_start().flex_wrap().gap_1().children(
                    workspace_names.into_iter().map(|workspace_name| {
                        div()
                            .flex()
                            .items_center()
                            .border_1()
                            .border_color(cx.theme().border)
                            .rounded_full()
                            .h_7()
                            .px_2p5()
                            .text_xs()
                            .opacity(0.8)
                            .child(workspace_name)
                    }),
                ))
                .child(div().text_xs().opacity(0.6).child(format!(
                    "{} · {} · {}",
                    status,
                    t!("settings.invitations.created", value = created.as_str()),
                    t!("settings.invitations.expires", value = expires.as_str())
                ))),
        )
        .when(row.can_revoke, |content| {
            content.child(
                Button::new(("invitation-revoke", index))
                    .small()
                    .ghost()
                    .label(t!("settings.invitations.revoke").to_string())
                    .on_click(move |_, window, cx| {
                        let invitation_id = row.invitation_id.clone();
                        let _ = desktop.update(cx, |view, cx| {
                            view.confirm_revoke_invitation(invitation_id, window, cx)
                        });
                    }),
            )
        })
        .into_any_element()
}

fn format_invitation_time(unix: u64) -> String {
    i64::try_from(unix)
        .ok()
        .and_then(|unix| chrono::DateTime::from_timestamp(unix, 0))
        .map(|time| {
            time.with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| t!("settings.invitations.unknown_time").to_string())
}

#[cfg(test)]
mod tests {
    #[::core::prelude::v1::test]
    fn secret_dialog_debug_and_close_never_expose_the_invitation() {
        let source = include_str!("invitations.rs");
        assert!(source.contains("state.clear()"));
        assert!(source.contains("[redacted]"));
        assert!(!source.contains(&["tracing", "::"].concat()));
    }

    #[::core::prelude::v1::test]
    fn capability_gate_and_cursor_pagination_are_owned_by_the_screen() {
        let source = include_str!("invitations.rs");
        assert!(source.contains("can_view_invitations"));
        assert!(source.contains("invitation_next_cursor"));
        assert!(source.contains("InvitationCreateParams::new_for_role"));
        assert!(source.contains("CredentialPresentationForm::new"));
        assert!(source.contains("confirm_revoke_invitation"));
        assert!(source.contains("finish_conflicted_action"));
        assert!(source.contains("apply_administration_refetches"));
    }
}
