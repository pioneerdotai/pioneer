use crate::{
    administration::AdministrationView,
    buttons::{default_outline_button, default_primary_button},
    credential::*,
};
use gpui_kit::component::{button::*, dialog::DialogFooter, form::field, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::administration::types::{
    InvitationCreateParams, InvitationId, InvitationRevokeParams, InvitationSummary, RoleKey,
    WorkspaceId,
};
use pioneer_client::state::client_state::GatewayConnectionState;
use pioneer_client::{
    administration::{InvitationPresentationStatus, invitation_list_row},
    gateway::invitation::InvitationQrPresentation,
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
    generation: Option<u64>,
    client: std::sync::Weak<pioneer_client::core::ClientCore>,
    phase: InvitationDialogPhase,
    selected: HashSet<String>,
    selected_role_key: Option<String>,
    creating: bool,
    error: Option<String>,
}

impl InvitationDialogState {
    fn new(
        selected_role_key: Option<String>,
        client: std::sync::Weak<pioneer_client::core::ClientCore>,
    ) -> Self {
        Self {
            client,
            generation: None,
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
        if let Some(generation) = self.generation.take() {
            if let Some(client) = self.client.upgrade() {
                client.administration_presentation_intent(pioneer_client::administration::operations::AdministrationPresentationIntent::DismissActivation { generation });
            }
        }
        self.phase = InvitationDialogPhase::Closed;
        self.selected.clear();
        self.selected_role_key = None;
        self.creating = false;
        self.error = None;
    }
}

impl Drop for InvitationDialogState {
    fn drop(&mut self) {
        self.clear();
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

impl AdministrationView {
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
        let list = if self.invitations_loading() && rows.is_empty() {
            v_flex()
                .p_6()
                .items_center()
                .gap_2()
                .child(self.invitation_loading.clone())
                .child(t!("settings.invitations.loading").to_string())
                .into_any_element()
        } else if let Some(error) = self.invitations_error().as_ref() {
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
                    Button::new(self.ui_id("invitations", "toolbar", "invitations-retry"))
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
                    render_invitation_row(
                        invitation,
                        index,
                        capabilities,
                        desktop.clone(),
                        self.ui_id("invitations", "list", "rows"),
                        cx,
                    )
                }))
                .when_some(self.administration.invitation_next_cursor(), |list, _| {
                    list.child(
                        Button::new(self.ui_id("invitations", "toolbar", "invitations-load-more"))
                            .small()
                            .ghost()
                            .disabled(self.invitations_loading())
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
            Button::new(self.ui_id("invitations", "toolbar", "invitation-create-open"))
                .ghost()
                .compact()
                .rounded_full()
                .icon(IconName::Plus)
                .tooltip(t!("settings.invitations.create").to_string())
                .disabled(
                    self.gateway.connection_state != GatewayConnectionState::Connected
                        || self.workspaces().is_empty()
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
            .workspaces()
            .iter()
            .map(|workspace| (workspace.id.clone(), workspace.name.clone()))
            .collect::<Vec<_>>();
        let role_options = self.authorized_invitation_role_options().to_vec();
        let selected_role_key = role_options
            .iter()
            .find(|option| option.is_default)
            .map(|option| option.role.key.clone());
        let namespace = self.ui_id("invitation-create", "form", "dialog");
        let state = cx.new(|_| {
            InvitationDialogState::new(selected_role_key, std::sync::Arc::downgrade(&self.client))
        });
        let desktop = cx.weak_entity();

        let lifetime = self.own_dialog(
            {
                let state = state.downgrade();
                move |_, cx| {
                    let _ = state.update(cx, |form, cx| {
                        form.clear();
                        cx.notify();
                    });
                }
            },
            window,
            cx,
        );
        lifetime.update(cx, |owner, cx| owner.track_form(&state, cx));
        let attach = lifetime.clone();

        let link_copy = {
            let state = state.downgrade();
            ActivationCopyButton::new(
                self.ui_id("invitation-create", "activation", "copy-link"),
                move |cx| {
                    let state = state.upgrade()?;
                    match &state.read(cx).phase {
                        InvitationDialogPhase::Ready { presentation, .. } => state
                            .read(cx)
                            .generation
                            .map(|generation| (generation, presentation.deep_link().to_owned())),
                        _ => None,
                    }
                },
                desktop.clone(),
                cx,
            )
        };
        let submit = cx.new(|_| InvitationSubmit {
            id: self.ui_id("invitation-create", "form", "submit"),
            form: state.downgrade(),
            owner: desktop.clone(),
        });
        window.open_dialog(cx, move |dialog, window, cx| {
            let dialog = dialog.on_close({
                let lifetime = lifetime.clone();
                move |_, window, cx| lifetime.update(cx, |owner, cx| owner.dismissed(window, cx))
            });
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
                                        .children(role_options.iter().map(|option| {
                                            let role_key = option.role.key.clone();
                                            let selected = snapshot.selected_role_key.as_deref()
                                                == Some(role_key.as_str());
                                            Toggle::new(SharedString::from(format!(
                                                "{namespace}:role:{role_key}:toggle"
                                            )))
                                            .small()
                                            .checked(selected)
                                            .disabled(snapshot.creating)
                                            .label(option.role.display_name.clone())
                                            .rounded_full()
                                            .h_8()
                                            .px_3()
                                            .text_sm()
                                            .when(!selected, |toggle| {
                                                toggle.border_1().border_color(cx.theme().border)
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
                                        })),
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
                                        .children(workspaces.iter().map(|(workspace_id, name)| {
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
                                            Toggle::new(SharedString::from(format!(
                                                "{namespace}:workspace:{workspace_id}:toggle"
                                            )))
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
                                                toggle.border_1().border_color(cx.theme().border)
                                            })
                                            .on_click({
                                                let state = state.clone();
                                                move |checked, _, cx| {
                                                    let workspace_id = workspace_id.clone();
                                                    state.update(cx, |state, cx| {
                                                        if *checked {
                                                            state.selected.insert(workspace_id);
                                                        } else {
                                                            state.selected.remove(&workspace_id);
                                                        }
                                                        state.error = None;
                                                        cx.notify();
                                                    });
                                                }
                                            })
                                        })),
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
                        default_outline_button(SharedString::from(format!(
                            "{namespace}:invitation-create-cancel"
                        )))
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
                        submit.clone().into_any_element(),
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
                        link_copy.clone(),
                    )
                    .into_any_element();
                    let footer = vec![
                        default_primary_button(SharedString::from(format!(
                            "{namespace}:invitation-presentation-close"
                        )))
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
                        default_primary_button(SharedString::from(format!(
                            "{namespace}:invitation-presentation-error-close"
                        )))
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
        attach.update(cx, |owner, cx| owner.attach(window, cx));
    }
}

fn render_invitation_row(
    invitation: InvitationSummary,
    index: usize,
    capabilities: pioneer_client::authorization::PrincipalPresentationCapabilities,
    desktop: Entity<AdministrationView>,
    namespace: SharedString,
    cx: &mut Context<AdministrationView>,
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
                Button::new(SharedString::from(format!(
                    "{namespace}:{}:revoke",
                    row.invitation_id
                )))
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

impl AdministrationView {
    fn create_invitation(&mut self, state: Entity<InvitationDialogState>, cx: &mut Context<Self>) {
        use pioneer_client::administration::operations::*;
        let form = state.read(cx);
        if form.creating || !matches!(form.phase, InvitationDialogPhase::Create) {
            return;
        }
        let params = form
            .selected_role_key
            .as_ref()
            .and_then(|key| RoleKey::new(key.clone()).ok())
            .and_then(|role| {
                form.selected
                    .iter()
                    .cloned()
                    .map(WorkspaceId::new)
                    .collect::<Result<Vec<_>, _>>()
                    .ok()
                    .and_then(|ids| InvitationCreateParams::new_for_role(role, ids).ok())
            });
        let Some(params) = params else {
            state.update(cx, |form, cx| {
                form.error = Some(t!("settings.invitations.invalid_selection").to_string());
                cx.notify();
            });
            return;
        };
        let transition = self
            .client
            .administration_command_intent(AdministrationCommand::CreateInvitation(params));
        if transition.outcome() == pioneer_client::core::ClientTransitionOutcome::Rejected {
            return;
        }
        let Some(generation) = self
            .client
            .administration_operation_snapshot()
            .map(|p| p.generation)
        else {
            return;
        };
        let Ok(operation) = self.client.take_administration_activation_operation(
            generation,
            AdministrationActivationKind::Invitation,
        ) else {
            return;
        };
        state.update(cx, |form, cx| {
            form.generation = Some(generation);
            form.creating = true;
            form.error = None;
            cx.notify();
        });
        let client = std::sync::Arc::downgrade(&self.client);
        let completion_client = client.clone();
        let state = state.downgrade();
        let task = cx.spawn(async move |_view: WeakEntity<Self>, cx| {
            let result = cx
                .background_spawn(async move { operation.execute(client) })
                .await;
            let _ = state.update(cx, |form, cx| {
                let current = completion_client.upgrade().is_some_and(|client| {
                    client.administration_operation_snapshot().is_some_and(|publication| {
                        publication.generation == generation
                            && matches!(publication.request,
                                pioneer_client::administration::pages::AdministrationLoadState::Ready
                                | pioneer_client::administration::pages::AdministrationLoadState::Failed)
                    })
                });
                if matches!(form.phase, InvitationDialogPhase::Closed)
                    || form.generation != Some(generation)
                    || !current
                {
                    return;
                }
                form.creating = false;
                match result {
                    Ok(AdministrationCompletion::InvitationCreated(response)) => {
                        form.phase = InvitationDialogState::ready(
                            InvitationQrPresentation::from_presentation(response.presentation),
                        )
                        .unwrap_or_else(|_| {
                            InvitationDialogPhase::Failed(
                                t!("settings.invitations.presentation_failed").to_string(),
                            )
                        });
                    }
                    _ => form.error = Some(t!("settings.invitations.create_failed").to_string()),
                }
                cx.notify();
            });
        });
        self.operations.retain(|task| !task.is_ready());
        self.operations.push(task);
    }
    fn confirm_revoke_invitation(
        &mut self,
        invitation_id: InvitationId,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let title = t!("settings.invitations.revoke_confirm_title").to_string();
        let description = format!(
            "{}\n{}",
            t!("settings.invitations.revoke_confirm_description"),
            invitation_id
        );
        let owner = cx.weak_entity();
        let lifetime = self.own_dialog(|_, _| {}, window, cx);
        let attach = lifetime.clone();
        window.open_alert_dialog(cx, move |alert, _, _| {
            alert.confirm().title(title.clone()).description(description.clone())
                .button_props(gpui_kit::component::dialog::DialogButtonProps::default().ok_text(t!("settings.invitations.revoke").to_string()).cancel_text(t!("buttons.cancel").to_string()))
                .on_close({ let lifetime = lifetime.clone(); move |_, window, cx| lifetime.update(cx, |owner, cx| owner.dismissed(window, cx)) })
                .on_ok({ let owner = owner.clone(); let id = invitation_id.clone(); let lifetime = lifetime.clone(); move |_, _, cx| {
                    if !lifetime.read(cx).valid() { return false; }
                    owner.update(cx, |view, _| view.client.administration_command_intent(pioneer_client::administration::operations::AdministrationCommand::RevokeInvitation(InvitationRevokeParams { invitation_id: id.clone() })).outcome() == pioneer_client::core::ClientTransitionOutcome::Changed).unwrap_or(false)
                } })
        });
        attach.update(cx, |owner, cx| owner.attach(window, cx));
    }
}

struct InvitationSubmit {
    id: SharedString,
    form: WeakEntity<InvitationDialogState>,
    owner: WeakEntity<AdministrationView>,
}
impl Render for InvitationSubmit {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(state) = self.form.upgrade() else {
            return div().into_any_element();
        };
        let snapshot = state.read(cx);
        let desktop = self.owner.clone();
        default_primary_button(self.id.clone())
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
                                .child(gpui_kit::component::spinner::Spinner::new().small()),
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
                    let _ =
                        desktop.update(cx, |view, cx| view.create_invitation(state.clone(), cx));
                }
            })
            .into_any_element()
    }
}
