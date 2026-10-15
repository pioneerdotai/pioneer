use crate::assets::PioneerIconName;
use crate::{
    administration::AdministrationView,
    buttons::{default_outline_button, default_primary_button},
    credential::*,
};
use gpui_kit::component::{
    avatar::Avatar,
    button::*,
    dialog::DialogFooter,
    menu::{ContextMenuExt, PopupMenuItem},
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::administration::types::{
    MemberDeviceCreateParams, MemberRemoveParams, MemberRestoreParams, MemberSummary,
    MemberSuspendParams, PrincipalId, PrincipalKind, PrincipalStatus, WorkspaceId,
    WorkspaceMemberAddParams, WorkspaceMemberRemoveParams,
};
use pioneer_client::{
    administration::{AdministrationAction, AdministrationPendingAction, member_list_row},
    gateway::device_activation::DeviceActivationQrPresentation,
};
use std::collections::HashSet;

#[derive(Clone)]
struct MemberWorkspacesDialogState {
    initial: HashSet<WorkspaceId>,
    selected: HashSet<WorkspaceId>,
}

struct RecoveryDialogState {
    value: Option<DeviceActivationQrPresentation>,
    generation: u64,
    client: std::sync::Weak<pioneer_client::core::ClientCore>,
}
impl RecoveryDialogState {
    fn clear(&mut self) {
        if self.value.take().is_some() {
            if let Some(client) = self.client.upgrade() {
                client.administration_presentation_intent(pioneer_client::administration::operations::AdministrationPresentationIntent::DismissActivation { generation: self.generation });
            }
        }
    }
}
impl std::ops::Deref for RecoveryDialogState {
    type Target = Option<DeviceActivationQrPresentation>;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl Drop for RecoveryDialogState {
    fn drop(&mut self) {
        self.clear();
    }
}

impl AdministrationView {
    pub(super) fn render_administration_members(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let capabilities = self.principal_presentation_capabilities();
        if !capabilities.can_view_member_directory {
            return Self::render_administration_screen(
                "administration-members-scroll",
                t!("settings.members.title").to_string(),
                t!("settings.members.description").to_string(),
                None,
                member_feedback(
                    t!("settings.members.forbidden").to_string(),
                    false,
                    self.member_loading.clone(),
                    cx,
                ),
                cx,
            );
        }
        let desktop = cx.entity().clone();
        let members = self.administration.members().cloned().collect::<Vec<_>>();
        let current_principal_id = self
            .gateway
            .current_auth
            .as_ref()
            .map(|auth| &auth.principal.id);

        let directory = if self.members_loading() && members.is_empty() {
            member_feedback(
                t!("settings.members.loading").to_string(),
                true,
                self.member_loading.clone(),
                cx,
            )
        } else if let Some(error) = self.members_error().as_ref() {
            v_flex()
                .gap_2()
                .p_4()
                .child(
                    div()
                        .text_sm()
                        .text_color(cx.theme().danger)
                        .child(error.clone()),
                )
                .child(
                    Button::new(self.ui_id("members", "toolbar", "members-retry"))
                        .small()
                        .outline()
                        .label(t!("settings.members.retry").to_string())
                        .on_click({
                            let desktop = desktop.clone();
                            move |_, _, cx| {
                                let _ = desktop.update(cx, |view, cx| {
                                    view.refresh_members(false, cx);
                                    view.refresh_all_workspace_members(cx);
                                });
                            }
                        }),
                )
                .into_any_element()
        } else if members.is_empty() {
            member_feedback(
                t!("settings.members.empty").to_string(),
                false,
                self.member_loading.clone(),
                cx,
            )
        } else {
            v_flex()
                .w_full()
                .rounded_lg()
                .border_1()
                .border_color(cx.theme().border)
                .children(members.iter().enumerate().map(|(index, member)| {
                    self.render_member_directory_row(
                        member,
                        index,
                        current_principal_id,
                        capabilities,
                        desktop.clone(),
                        cx,
                    )
                }))
                .when_some(self.administration.member_next_cursor(), |list, _| {
                    list.child(
                        Button::new(self.ui_id("members", "toolbar", "members-load-more"))
                            .small()
                            .ghost()
                            .disabled(self.members_loading())
                            .label(t!("settings.members.load_more").to_string())
                            .on_click({
                                let desktop = desktop.clone();
                                move |_, _, cx| {
                                    let _ = desktop
                                        .update(cx, |view, cx| view.refresh_members(true, cx));
                                }
                            }),
                    )
                })
                .into_any_element()
        };

        let content = v_flex().w_full().child(directory).into_any_element();

        Self::render_administration_screen(
            "administration-members-scroll",
            t!("settings.members.title").to_string(),
            t!("settings.members.description").to_string(),
            None,
            content,
            cx,
        )
    }

    fn render_member_directory_row(
        &self,
        member: &MemberSummary,
        index: usize,
        current_principal_id: Option<&PrincipalId>,
        capabilities: pioneer_client::authorization::PrincipalPresentationCapabilities,
        desktop: Entity<Self>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let status = member_status_label(member.status);
        let avatar_path = self
            .member_avatar_state
            .presentation(&member.principal_id)
            .and_then(|avatar| avatar.cached_image_path.clone());
        let memberships = self
            .workspaces()
            .iter()
            .filter_map(|workspace| {
                let workspace_id = WorkspaceId::new(workspace.id.clone()).ok()?;
                let is_member =
                    self.administration
                        .workspace_members(&workspace_id)
                        .map(|members| {
                            members
                                .iter()
                                .any(|candidate| candidate.principal_id == member.principal_id)
                        });
                Some((workspace_id, workspace.name.clone(), is_member))
            })
            .collect::<Vec<_>>();
        let workspace_data_ready = memberships.iter().all(|(workspace_id, _, membership)| {
            membership.is_some() && !self.workspace_members_loading(workspace_id)
        });
        let workspace_tags = memberships
            .iter()
            .filter_map(|(_, name, membership)| membership.unwrap_or(false).then_some(name.clone()))
            .collect::<Vec<_>>();
        let can_edit_workspaces = workspace_data_ready
            && memberships.iter().any(|(_, _, membership)| {
                let is_member = membership.unwrap_or(false);
                let actions =
                    member_list_row(member, current_principal_id, capabilities, is_member).actions;
                if is_member {
                    actions.can_remove_from_workspace
                } else {
                    actions.can_add_to_workspace
                }
            });
        let show_edit_workspaces = can_edit_workspaces
            || (!workspace_data_ready
                && member.kind == PrincipalKind::User
                && member.status == PrincipalStatus::Active
                && current_principal_id != Some(&member.principal_id)
                && (capabilities.can_add_workspace_member
                    || capabilities.can_remove_workspace_member));
        let lifecycle_actions =
            member_list_row(member, current_principal_id, capabilities, false).actions;
        let has_lifecycle_actions = lifecycle_actions.can_suspend
            || lifecycle_actions.can_restore
            || lifecycle_actions.can_create_recovery_device
            || lifecycle_actions.can_remove;
        let pending = self.member_workspaces_saving()
            || self.administration.pending_action() != &AdministrationPendingAction::Idle;
        let principal_id = member.principal_id.clone();
        let menu_member = member.clone();

        let row = v_flex()
            .id(self.ui_id("members", member.principal_id.as_str(), "row"))
            .w_full()
            .min_w_0()
            .gap_3()
            .px_4()
            .py_3()
            .when(index > 0, |row| {
                row.border_t_1().border_color(cx.theme().border)
            })
            .hover(|row| row.bg(cx.theme().muted))
            .child(
                h_flex()
                    .gap_2()
                    .child(
                        pioneer_desktop_foundation::AvatarSurface::new(Avatar::new().size_10())
                            .fallback_name(member.display_name.clone())
                            .when_some(avatar_path, |avatar, path| {
                                avatar.source(std::path::PathBuf::from(path))
                            }),
                    )
                    .child(
                        v_flex()
                            .min_w_0()
                            .flex_1()
                            .gap_0p5()
                            .child(
                                h_flex()
                                    .items_center()
                                    .gap_1p5()
                                    .child(
                                        div()
                                            .truncate()
                                            .text_sm()
                                            .font_semibold()
                                            .child(member.display_name.clone()),
                                    )
                                    .child(
                                        div()
                                            .truncate()
                                            .text_xs()
                                            .opacity(0.6)
                                            .child(format!("@{}", member.nickname,)),
                                    ),
                            )
                            .child(
                                div()
                                    .truncate()
                                    .text_xs()
                                    .opacity(0.6)
                                    .child(format!("{} · {}", member.role.display_name, status)),
                            ),
                    ),
            )
            .when(workspace_tags.len() > 0, |row| {
                row.child(h_flex().justify_start().flex_wrap().gap_1().children(
                    workspace_tags.into_iter().map(|workspace_name| {
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
            });

        if !show_edit_workspaces && !has_lifecycle_actions {
            return row.into_any_element();
        }

        row.context_menu(move |menu, _, _| {
            let mut menu = menu.min_w(px(220.));
            if show_edit_workspaces {
                let desktop = desktop.clone();
                let member = menu_member.clone();
                menu = menu.item(
                    PopupMenuItem::new(t!("settings.members.edit_workspaces").to_string())
                        .icon(PioneerIconName::Pen)
                        .disabled(!can_edit_workspaces || pending)
                        .on_click(move |_, window, cx| {
                            let member = member.clone();
                            let _ = desktop.update(cx, |view, cx| {
                                view.open_edit_member_workspaces_dialog(member, window, cx);
                            });
                        }),
                );
            }
            if show_edit_workspaces && has_lifecycle_actions {
                menu = menu.separator();
            }
            if lifecycle_actions.can_suspend {
                menu = menu.item(member_action_menu_item(
                    t!("settings.members.suspend").to_string(),
                    PioneerIconName::ShieldX,
                    AdministrationAction::SuspendMember {
                        principal_id: principal_id.clone(),
                    },
                    pending,
                    desktop.clone(),
                ));
            }
            if lifecycle_actions.can_restore {
                menu = menu.item(member_action_menu_item(
                    t!("settings.members.restore").to_string(),
                    PioneerIconName::RotateCcw,
                    AdministrationAction::RestoreMember {
                        principal_id: principal_id.clone(),
                    },
                    pending,
                    desktop.clone(),
                ));
            }
            if lifecycle_actions.can_create_recovery_device {
                menu = menu.item(member_action_menu_item(
                    t!("settings.members.recovery").to_string(),
                    PioneerIconName::ShieldCheck,
                    AdministrationAction::CreateRecoveryDevice {
                        principal_id: principal_id.clone(),
                    },
                    pending,
                    desktop.clone(),
                ));
            }
            if lifecycle_actions.can_remove {
                menu = menu.item(member_action_menu_item(
                    t!("settings.members.remove").to_string(),
                    PioneerIconName::Trash,
                    AdministrationAction::RemoveMember {
                        principal_id: principal_id.clone(),
                    },
                    pending,
                    desktop.clone(),
                ));
            }
            menu
        })
        .into_any_element()
    }

    fn open_edit_member_workspaces_dialog(
        &mut self,
        member: MemberSummary,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let current_principal_id = self
            .gateway
            .current_auth
            .as_ref()
            .map(|auth| &auth.principal.id);
        let capabilities = self.principal_presentation_capabilities();
        let workspaces = self
            .workspaces()
            .iter()
            .filter_map(|workspace| {
                let workspace_id = WorkspaceId::new(workspace.id.clone()).ok()?;
                let members = self.administration.workspace_members(&workspace_id)?;
                let is_member = members
                    .iter()
                    .any(|candidate| candidate.principal_id == member.principal_id);
                let actions =
                    member_list_row(&member, current_principal_id, capabilities, is_member).actions;
                let editable = if is_member {
                    actions.can_remove_from_workspace
                } else {
                    actions.can_add_to_workspace
                };
                Some((workspace_id, workspace.name.clone(), is_member, editable))
            })
            .collect::<Vec<_>>();
        let initial = workspaces
            .iter()
            .filter_map(|(workspace_id, _, selected, _)| selected.then_some(workspace_id.clone()))
            .collect::<HashSet<_>>();
        let namespace = self.ui_id("member-workspaces", member.principal_id.as_str(), "dialog");
        let state = cx.new(|_| MemberWorkspacesDialogState {
            initial: initial.clone(),
            selected: initial,
        });
        let desktop = cx.weak_entity();
        let principal_id = member.principal_id.clone();

        let lifetime = self.own_dialog(|_, _| {}, window, cx);
        lifetime.update(cx, |owner, cx| owner.track_form(&state, cx));
        let attach = lifetime.clone();
        window.open_dialog(cx, move |dialog, window, cx| {
            let snapshot = state.read(cx).clone();
            let changed = snapshot.selected != snapshot.initial;

            dialog
                .on_close({
                    let lifetime = lifetime.clone();
                    move |_, window, cx| {
                        lifetime.update(cx, |owner, cx| owner.dismissed(window, cx))
                    }
                })
                .w(px(520.))
                .max_h(window.viewport_size().height * 0.8)
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("settings.members.workspaces_title").to_string()),
                )
                .footer(DialogFooter::new().children(vec![
                        default_outline_button(SharedString::from(format!(
                            "{namespace}:member-workspaces-cancel"
                        )))
                        .label(t!("buttons.cancel").to_string())
                        .outline()
                        .on_click(|_, window, cx| window.close_dialog(cx))
                        .into_any_element(),
                        default_primary_button(SharedString::from(format!(
                            "{namespace}:member-workspaces-save"
                        )))
                        .label(t!("buttons.save").to_string())
                        .disabled(!changed)
                        .on_click({
                            let desktop = desktop.clone();
                            let principal_id = principal_id.clone();
                            let state = state.clone();
                            move |_, window, cx| {
                                let snapshot = state.read(cx).clone();
                                let started = desktop.update(cx, |view, cx| {
                                    view.save_member_workspaces(
                                        principal_id.clone(),
                                        snapshot.initial,
                                        snapshot.selected,
                                        cx,
                                    )
                                });
                                if started.unwrap_or(false) {
                                    window.close_dialog(cx);
                                }
                            }
                        })
                        .into_any_element(),
                    ]))
                .child(
                    h_flex()
                        .w_full()
                        .pt_4()
                        .pb_6()
                        .flex_wrap()
                        .gap_1p5()
                        .children(workspaces.iter().map(|(workspace_id, name, _, editable)| {
                            let workspace_id = workspace_id.clone();
                            let state = state.clone();
                            let selected = snapshot.selected.contains(&workspace_id);
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
                                "{namespace}:{workspace_id}:toggle"
                            )))
                            .small()
                            .checked(selected)
                            .disabled(!editable)
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
                            .on_click(move |checked, _, cx| {
                                let workspace_id = workspace_id.clone();
                                state.update(cx, |state, cx| {
                                    if *checked {
                                        state.selected.insert(workspace_id);
                                    } else {
                                        state.selected.remove(&workspace_id);
                                    }
                                    cx.notify();
                                });
                            })
                        })),
                )
        });
        attach.update(cx, |owner, cx| owner.attach(window, cx));
    }

    fn confirm_member_action(
        &mut self,
        action: AdministrationAction,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if matches!(action, AdministrationAction::CreateRecoveryDevice { .. }) {
            self.execute_member_action(action, Some(window), cx);
            return;
        }
        let (title, description) = match action {
            AdministrationAction::SuspendMember { .. } => (
                t!("settings.members.suspend_confirm"),
                t!("settings.members.suspend_description"),
            ),
            AdministrationAction::RestoreMember { .. } => (
                t!("settings.members.restore_confirm"),
                t!("settings.members.restore_description"),
            ),
            AdministrationAction::RemoveMember { .. } => (
                t!("settings.members.remove_confirm"),
                t!("settings.members.remove_description"),
            ),
            _ => return,
        };
        let target = match &action {
            AdministrationAction::SuspendMember { principal_id }
            | AdministrationAction::RestoreMember { principal_id }
            | AdministrationAction::RemoveMember { principal_id } => principal_id,
            _ => return,
        };
        let label = self
            .administration
            .members()
            .find(|member| &member.principal_id == target)
            .map(|member| member.display_name.as_str())
            .unwrap_or(target.as_str());
        let title = title.to_string();
        let description = format!("{description}\n{label} ({target})");
        let owner = cx.weak_entity();
        let lifetime = self.own_dialog(|_, _| {}, window, cx);
        let attach = lifetime.clone();
        window.open_alert_dialog(cx, move |alert, _, _| {
            alert
                .confirm()
                .title(title.clone())
                .description(description.clone())
                .button_props(
                    gpui_kit::component::dialog::DialogButtonProps::default()
                        .ok_text(t!("buttons.ok").to_string())
                        .cancel_text(t!("buttons.cancel").to_string()),
                )
                .on_close({
                    let lifetime = lifetime.clone();
                    move |_, window, cx| {
                        lifetime.update(cx, |owner, cx| owner.dismissed(window, cx))
                    }
                })
                .on_ok({
                    let owner = owner.clone();
                    let action = action.clone();
                    let lifetime = lifetime.clone();
                    move |_, _, cx| {
                        if !lifetime.read(cx).valid() {
                            return false;
                        }
                        owner
                            .update(cx, |view, cx| {
                                view.execute_member_action(action.clone(), None, cx)
                            })
                            .is_ok()
                    }
                })
        });
        attach.update(cx, |owner, cx| owner.attach(window, cx));
    }

    fn open_recovery_device_dialog(
        &mut self,
        presentation: DeviceActivationQrPresentation,
        generation: u64,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let namespace = self.ui_id("recovery", &generation.to_string(), "dialog");
        let state = cx.new(|_| RecoveryDialogState {
            value: Some(presentation),
            generation,
            client: std::sync::Arc::downgrade(&self.client),
        });
        let link_copy = {
            let state = state.downgrade();
            ActivationCopyButton::new(
                self.ui_id("recovery", "activation", "copy-link"),
                move |cx| {
                    let state = state.upgrade()?;
                    state
                        .read(cx)
                        .as_ref()
                        .map(|p| (generation, p.deep_link().to_owned()))
                },
                cx.weak_entity(),
                cx,
            )
        };
        let code_copy = {
            let state = state.downgrade();
            ActivationCopyButton::new(
                self.ui_id("recovery", "activation", "copy-code"),
                move |cx| {
                    let state = state.upgrade()?;
                    state
                        .read(cx)
                        .as_ref()
                        .map(|p| (generation, p.manual_code().to_owned()))
                },
                cx.weak_entity(),
                cx,
            )
        };
        let lifetime = self.own_dialog(
            {
                let state = state.downgrade();
                move |_, cx| {
                    let _ = state.update(cx, |value, cx| {
                        value.clear();
                        cx.notify();
                    });
                }
            },
            window,
            cx,
        );
        lifetime.update(cx, |owner, cx| owner.track_form(&state, cx));
        let attach = lifetime.clone();

        window.open_dialog(cx, move |dialog, _window, cx| {
            let snapshot = state.read(cx);
            let content = snapshot.as_ref().map(|presentation| {
                DeviceActivationForm::new(
                    DeviceActivationFormPhase::Ready(presentation.clone()),
                    t!("settings.members.recovery_description").to_string(),
                    link_copy.clone(),
                    code_copy.clone(),
                )
            });
            dialog
                .on_close({
                    let lifetime = lifetime.clone();
                    move |_, window, cx| {
                        lifetime.update(cx, |owner, cx| owner.dismissed(window, cx))
                    }
                })
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
                        .child(t!("settings.members.recovery_title").to_string()),
                )
                .footer(DialogFooter::new().children({
                    let state = state.clone();
                    vec![
                        default_primary_button(SharedString::from(format!(
                            "{namespace}:recovery-close"
                        )))
                        .label(t!("settings.members.recovery_done").to_string())
                        .on_click({
                            let state = state.clone();
                            move |_, window, cx| {
                                state.update(cx, |presentation, _| presentation.clear());
                                window.close_dialog(cx);
                            }
                        })
                        .into_any_element(),
                    ]
                }))
                .when_some(content, |dialog, content| dialog.child(content))
        });
        attach.update(cx, |owner, cx| owner.attach(window, cx));
    }
}

fn member_action_menu_item(
    label: String,
    icon: PioneerIconName,
    action: AdministrationAction,
    pending: bool,
    desktop: Entity<AdministrationView>,
) -> PopupMenuItem {
    PopupMenuItem::new(label)
        .icon(icon)
        .disabled(pending)
        .on_click(move |_, window, cx| {
            let action = action.clone();
            let _ = desktop.update(cx, |view, cx| {
                view.confirm_member_action(action, window, cx)
            });
        })
}

fn member_status_label(status: PrincipalStatus) -> String {
    match status {
        PrincipalStatus::Active => t!("settings.members.status_active"),
        PrincipalStatus::Suspended => t!("settings.members.status_suspended"),
        PrincipalStatus::Removed => t!("settings.members.status_removed"),
    }
    .to_string()
}

fn member_feedback(
    label: String,
    loading: bool,
    loading_indicator: Entity<crate::activity::LoadingIndicator>,
    cx: &mut Context<AdministrationView>,
) -> AnyElement {
    v_flex()
        .min_h(px(160.))
        .items_center()
        .justify_center()
        .gap_2()
        .when(loading, |content| content.child(loading_indicator.clone()))
        .child(div().text_sm().opacity(0.6).child(label))
        .bg(cx.theme().background)
        .into_any_element()
}

impl AdministrationView {
    fn save_member_workspaces(
        &mut self,
        principal_id: PrincipalId,
        initial: HashSet<WorkspaceId>,
        selected: HashSet<WorkspaceId>,
        _: &mut Context<Self>,
    ) -> bool {
        if initial == selected {
            return false;
        }
        self.client.administration_command_intent(pioneer_client::administration::operations::AdministrationCommand::SetMemberWorkspaces { principal_id, selected: selected.into_iter().collect() }).outcome() != pioneer_client::core::ClientTransitionOutcome::Rejected
    }
    fn execute_member_action(
        &mut self,
        action: AdministrationAction,
        window: Option<&mut Window>,
        cx: &mut Context<Self>,
    ) {
        use pioneer_client::administration::operations::*;
        let status = |id: &PrincipalId| {
            self.administration
                .members()
                .find(|member| &member.principal_id == id)
                .map(|member| member.status)
        };
        let command = match action {
            AdministrationAction::SuspendMember { principal_id } => {
                AdministrationCommand::SuspendMember(MemberSuspendParams {
                    expected_status: status(&principal_id),
                    principal_id,
                })
            }
            AdministrationAction::RestoreMember { principal_id } => {
                AdministrationCommand::RestoreMember(MemberRestoreParams {
                    expected_status: status(&principal_id),
                    principal_id,
                })
            }
            AdministrationAction::RemoveMember { principal_id } => {
                AdministrationCommand::RemoveMember(MemberRemoveParams {
                    expected_status: status(&principal_id),
                    principal_id,
                })
            }
            AdministrationAction::CreateRecoveryDevice { principal_id } => {
                AdministrationCommand::CreateRecoveryDevice(MemberDeviceCreateParams {
                    principal_id,
                })
            }
            AdministrationAction::AddWorkspaceMember {
                workspace_id,
                principal_id,
            } => AdministrationCommand::AddWorkspaceMember(WorkspaceMemberAddParams {
                workspace_id,
                principal_id,
            }),
            AdministrationAction::RemoveWorkspaceMember {
                workspace_id,
                principal_id,
            } => AdministrationCommand::RemoveWorkspaceMember(WorkspaceMemberRemoveParams {
                workspace_id,
                principal_id,
            }),
            _ => return,
        };
        let recovery = matches!(command, AdministrationCommand::CreateRecoveryDevice(_));
        if self.client.administration_command_intent(command).outcome()
            == pioneer_client::core::ClientTransitionOutcome::Rejected
            || !recovery
        {
            return;
        }
        let Some(window) = window else {
            return;
        };
        let window_handle = window.window_handle();
        let Some(generation) = self
            .client
            .administration_operation_snapshot()
            .map(|p| p.generation)
        else {
            return;
        };
        let Ok(operation) = self.client.take_administration_activation_operation(
            generation,
            AdministrationActivationKind::RecoveryDevice,
        ) else {
            return;
        };
        let client = std::sync::Arc::downgrade(&self.client);
        self.operations.retain(|task| !task.is_ready());
        self.operations
            .push(cx.spawn(async move |view: WeakEntity<Self>, cx| {
                let result = cx
                    .background_spawn(async move {
                        match operation.execute(client.clone())? {
                            AdministrationCompletion::RecoveryDeviceCreated(response) => {
                                client.upgrade().ok_or_else(|| anyhow::anyhow!("administration_action_cancelled"))?.administration_recovery_presentation(generation, response)
                            }
                            _ => Err(anyhow::anyhow!("administration_activation_mismatch")),
                        }
                    })
                    .await;
                if let Ok(presentation) = result {
                    let _ = window_handle.update(cx, |_, window, cx| {
                        let _ = view.update(cx, |view, cx| {
                            if view.visible() && view.client.administration_operation_snapshot().is_some_and(|p| p.generation == generation && p.request == pioneer_client::administration::pages::AdministrationLoadState::Ready) { view.open_recovery_device_dialog(presentation, generation, window, cx); }
                        });
                    });
                }
            }));
    }
}
