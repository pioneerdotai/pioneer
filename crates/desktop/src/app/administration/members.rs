use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    assets::PioneerIconName,
    components::{
        buttonts::{default_outline_button, default_primary_button},
        device_activation_form::{DeviceActivationForm, DeviceActivationFormPhase},
    },
};
use gpui::{prelude::*, *};
use gpui_component::{
    avatar::Avatar,
    button::*,
    dialog::DialogFooter,
    menu::{ContextMenuExt, PopupMenuItem},
    spinner::Spinner,
    theme::ActiveTheme,
    *,
};
use pioneer_client::{
    administration::{AdministrationAction, AdministrationPendingAction, member_list_row},
    gateway::device_activation::DeviceActivationQrPresentation,
};
use pioneer_protocol::{
    AuthSessionRevokeParams, AuthSessionStatus, MemberDeviceCreateParams, MemberListParams,
    MemberRemoveParams, MemberRestoreParams, MemberSummary, MemberSuspendParams, PrincipalId,
    PrincipalKind, PrincipalStatus, WorkspaceId, WorkspaceMemberAddParams,
    WorkspaceMemberListParams, WorkspaceMemberRemoveParams,
};
use std::collections::HashSet;

#[derive(Clone)]
struct MemberWorkspacesDialogState {
    initial: HashSet<WorkspaceId>,
    selected: HashSet<WorkspaceId>,
}

impl PioneerDesktop {
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
                member_feedback(t!("settings.members.forbidden").to_string(), false, cx),
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

        let directory = if self.members_loading && members.is_empty() {
            member_feedback(t!("settings.members.loading").to_string(), true, cx)
        } else if let Some(error) = self.members_error.as_ref() {
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
                    Button::new("members-retry")
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
            member_feedback(t!("settings.members.empty").to_string(), false, cx)
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
                        Button::new("members-load-more")
                            .small()
                            .ghost()
                            .disabled(self.members_loading)
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
            .workspaces
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
            membership.is_some() && !self.workspace_members_loading.contains(workspace_id)
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
        let pending = self.member_workspaces_saving
            || self.administration.pending_action() != &AdministrationPendingAction::Idle;
        let principal_id = member.principal_id.clone();
        let menu_member = member.clone();

        let row = v_flex()
            .id(("member-row", index))
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
                        Avatar::new()
                            .name(member.display_name.clone())
                            .size_10()
                            .when_some(avatar_path, |avatar, path| {
                                avatar.src(std::path::PathBuf::from(path))
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
                            .child(div().truncate().text_xs().opacity(0.6).child(format!(
                                "{} · {}",
                                member_kind_label(member.kind),
                                status
                            ))),
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
            .workspaces
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
        let state = cx.new(|_| MemberWorkspacesDialogState {
            initial: initial.clone(),
            selected: initial,
        });
        let desktop = cx.entity().clone();
        let principal_id = member.principal_id.clone();

        window.open_dialog(cx, move |dialog, window, cx| {
            let snapshot = state.read(cx).clone();
            let changed = snapshot.selected != snapshot.initial;

            dialog
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
                    default_outline_button("member-workspaces-cancel")
                        .label(t!("buttons.cancel").to_string())
                        .outline()
                        .on_click(|_, window, cx| window.close_dialog(cx))
                        .into_any_element(),
                    default_primary_button("member-workspaces-save")
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
                                if started {
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
                        .children(workspaces.iter().enumerate().map(
                            |(index, (workspace_id, name, _, editable))| {
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
                                Toggle::new(("member-workspace-toggle", index))
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
                            },
                        )),
                )
        });
    }

    fn save_member_workspaces(
        &mut self,
        principal_id: PrincipalId,
        initial: HashSet<WorkspaceId>,
        selected: HashSet<WorkspaceId>,
        cx: &mut Context<Self>,
    ) -> bool {
        if self.member_workspaces_saving
            || self.administration.pending_action() != &AdministrationPendingAction::Idle
        {
            return false;
        }
        let additions = selected.difference(&initial).cloned().collect::<Vec<_>>();
        let removals = initial.difference(&selected).cloned().collect::<Vec<_>>();
        if additions.is_empty() && removals.is_empty() {
            return false;
        }

        self.member_workspaces_saving = true;
        self.members_error = None;
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        for workspace_id in additions {
                            sender.workspace_member_add(WorkspaceMemberAddParams {
                                workspace_id,
                                principal_id: principal_id.clone(),
                            })?;
                        }
                        for workspace_id in removals {
                            sender.workspace_member_remove(WorkspaceMemberRemoveParams {
                                workspace_id,
                                principal_id: principal_id.clone(),
                            })?;
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.member_workspaces_saving = false;
                    if result.is_err() {
                        view.members_error = Some(t!("settings.members.action_failed").to_string());
                    }
                    view.refresh_members(false, cx);
                    view.refresh_all_workspace_members(cx);
                    cx.notify();
                });
            }
        })
        .detach();
        true
    }

    pub(in crate::app) fn refresh_members(&mut self, append: bool, cx: &mut Context<Self>) {
        if self.members_loading
            || !self
                .principal_presentation_capabilities()
                .can_view_member_directory
        {
            return;
        }
        let cursor = append
            .then(|| self.administration.member_next_cursor().map(str::to_owned))
            .flatten();
        if append && cursor.is_none() {
            return;
        }
        self.members_loading = true;
        self.members_error = None;
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.member_list(MemberListParams {
                            cursor,
                            limit: None,
                        })
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.members_loading = false;
                    match result {
                        Ok(response) => {
                            if append {
                                view.administration.append_member_page(response);
                            } else {
                                view.administration.apply_member_list(response);
                            }
                            view.resolve_visible_member_avatars(cx);
                        }
                        Err(_) => {
                            view.members_error =
                                Some(t!("settings.members.load_failed").to_string())
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    /// Loads the complete ACL-scoped directory used to supplement an
    /// explicit workspace member list with implicit Superusers for mentions.
    pub(in crate::app) fn ensure_active_thread_mention_directory_loaded(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || self.current_active_thread_id().is_none()
            || self.members_loading
            || self.members_error.is_some()
            || self.administration.member_directory_complete()
            || !self
                .principal_presentation_capabilities()
                .can_view_member_directory
        {
            return;
        }

        self.members_loading = true;
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        let mut pages = Vec::new();
                        let mut cursor = None;
                        loop {
                            let page = sender.member_list(MemberListParams {
                                cursor,
                                limit: Some(100),
                            })?;
                            cursor = page.next_cursor.clone();
                            pages.push(page);
                            if cursor.is_none() {
                                break;
                            }
                        }
                        Ok::<_, anyhow::Error>(pages)
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.members_loading = false;
                    match result {
                        Ok(pages) => {
                            for (index, page) in pages.into_iter().enumerate() {
                                if index == 0 {
                                    view.administration.apply_member_list(page);
                                } else {
                                    view.administration.append_member_page(page);
                                }
                            }
                            view.members_error = None;
                            view.resolve_visible_member_avatars(cx);
                        }
                        Err(_) => {
                            view.members_error =
                                Some(t!("settings.members.load_failed").to_string());
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn resolve_visible_member_avatars(&mut self, cx: &mut Context<Self>) {
        let mut members = self.administration.members().cloned().collect::<Vec<_>>();
        if let Some(workspace_id) = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id))
            .and_then(|workspace_id| WorkspaceId::new(workspace_id.to_owned()).ok())
            && let Some(workspace_members) = self.administration.workspace_members(&workspace_id)
        {
            let known_ids = members
                .iter()
                .map(|member| member.principal_id.clone())
                .collect::<std::collections::HashSet<_>>();
            members.extend(
                workspace_members
                    .iter()
                    .filter(|member| !known_ids.contains(&member.principal_id))
                    .cloned(),
            );
        }
        let requests = self.member_avatar_state.reconcile_visible_members(&members);
        self.resolve_current_principal_avatar(cx);
        self.resolve_member_avatar_requests(requests, cx);
    }

    pub(in crate::app) fn refresh_all_workspace_members(&mut self, cx: &mut Context<Self>) {
        let workspaces = self
            .workspaces
            .iter()
            .filter_map(|workspace| WorkspaceId::new(workspace.id.clone()).ok())
            .filter(|workspace_id| !self.workspace_members_loading.contains(workspace_id))
            .collect::<Vec<_>>();
        if workspaces.is_empty() {
            return;
        }
        self.workspace_members_loading
            .extend(workspaces.iter().cloned());
        let loading_workspace_ids = workspaces.clone();
        let sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let pages = cx
                    .background_spawn(async move {
                        let mut pages = Vec::new();
                        for workspace_id in workspaces {
                            let mut cursor = None;
                            loop {
                                let page =
                                    sender.workspace_member_list(WorkspaceMemberListParams {
                                        workspace_id: workspace_id.clone(),
                                        cursor,
                                        limit: Some(100),
                                    })?;
                                cursor = page.next_cursor.clone();
                                pages.push(page);
                                if cursor.is_none() {
                                    break;
                                }
                            }
                        }
                        Ok::<_, anyhow::Error>(pages)
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    for workspace_id in loading_workspace_ids {
                        view.workspace_members_loading.remove(&workspace_id);
                    }
                    if let Ok(pages) = pages {
                        let mut first = std::collections::HashSet::new();
                        for page in pages {
                            if first.insert(page.workspace_id.clone()) {
                                view.administration.apply_workspace_member_list(page);
                            } else {
                                view.administration.append_workspace_member_page(page);
                            }
                        }
                        view.resolve_visible_member_avatars(cx);
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn refresh_workspace_members(
        &mut self,
        workspace_id: WorkspaceId,
        cx: &mut Context<Self>,
    ) {
        if !self.workspace_members_loading.insert(workspace_id.clone()) {
            return;
        }
        let sender = self.gateway.ws_command_sender.clone();
        let loading_workspace_id = workspace_id.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        let mut pages = Vec::new();
                        let mut cursor = None;
                        loop {
                            let page = sender.workspace_member_list(WorkspaceMemberListParams {
                                workspace_id: workspace_id.clone(),
                                cursor,
                                limit: Some(100),
                            })?;
                            cursor = page.next_cursor.clone();
                            pages.push(page);
                            if cursor.is_none() {
                                break;
                            }
                        }
                        Ok::<_, anyhow::Error>(pages)
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    view.workspace_members_loading.remove(&loading_workspace_id);
                    if let Ok(pages) = result {
                        for (index, page) in pages.into_iter().enumerate() {
                            if index == 0 {
                                view.administration.apply_workspace_member_list(page);
                            } else {
                                view.administration.append_workspace_member_page(page);
                            }
                        }
                        view.resolve_visible_member_avatars(cx);
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app) fn ensure_active_thread_workspace_members_loaded(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(workspace_id) = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id))
            .and_then(|workspace_id| WorkspaceId::new(workspace_id.to_owned()).ok())
        else {
            return;
        };
        if self
            .administration
            .workspace_members(&workspace_id)
            .is_none()
            && !self.workspace_members_loading.contains(&workspace_id)
        {
            self.refresh_workspace_members(workspace_id, cx);
        }
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
        let title = title.to_string();
        let description = description.to_string();
        let answer = window.prompt(
            PromptLevel::Warning,
            title.as_str(),
            Some(description.as_str()),
            &[
                PromptButton::new(t!("buttons.ok").to_string()),
                PromptButton::cancel(t!("buttons.cancel").to_string()),
            ],
            cx,
        );
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                if answer.await == Ok(0) {
                    let _ = this.update(&mut cx, |view, cx| {
                        view.execute_member_action(action.clone(), None, cx)
                    });
                }
            }
        })
        .detach();
    }

    fn execute_member_action(
        &mut self,
        action: AdministrationAction,
        window: Option<&mut Window>,
        cx: &mut Context<Self>,
    ) {
        if !self.administration.begin_action(action.clone()) {
            return;
        }
        let expected_member_status = match &action {
            AdministrationAction::SuspendMember { principal_id }
            | AdministrationAction::RestoreMember { principal_id }
            | AdministrationAction::RemoveMember { principal_id } => self
                .administration
                .members()
                .find(|member| &member.principal_id == principal_id)
                .map(|member| member.status),
            _ => None,
        };
        let sender = self.gateway.ws_command_sender.clone();
        let endpoint = self
            .gateway
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.active_gateway().cloned());
        let window_handle = window.map(|window| window.window_handle());
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let action_for_request = action.clone();
                let result = cx
                    .background_spawn(async move {
                        match action_for_request {
                            AdministrationAction::SuspendMember { principal_id } => sender
                                .member_suspend(MemberSuspendParams {
                                    principal_id,
                                    expected_status: expected_member_status,
                                })
                                .map(|_| None),
                            AdministrationAction::RestoreMember { principal_id } => sender
                                .member_restore(MemberRestoreParams {
                                    principal_id,
                                    expected_status: expected_member_status,
                                })
                                .map(|_| None),
                            AdministrationAction::RemoveMember { principal_id } => sender
                                .member_remove(MemberRemoveParams {
                                    principal_id,
                                    expected_status: expected_member_status,
                                })
                                .map(|_| None),
                            AdministrationAction::CreateRecoveryDevice { principal_id } => sender
                                .member_device_create(MemberDeviceCreateParams { principal_id })
                                .map(Some),
                            AdministrationAction::AddWorkspaceMember {
                                workspace_id,
                                principal_id,
                            } => sender
                                .workspace_member_add(WorkspaceMemberAddParams {
                                    workspace_id,
                                    principal_id,
                                })
                                .map(|_| None),
                            AdministrationAction::RemoveWorkspaceMember {
                                workspace_id,
                                principal_id,
                            } => sender
                                .workspace_member_remove(WorkspaceMemberRemoveParams {
                                    workspace_id,
                                    principal_id,
                                })
                                .map(|_| None),
                            _ => Err(anyhow::anyhow!("unsupported member action")),
                        }
                    })
                    .await;
                let mut recovery_presentation = None;
                let _ = this.update(&mut cx, |view, cx| {
                    let refetches = if result.is_err() {
                        view.administration.finish_conflicted_action()
                    } else {
                        view.administration.finish_action();
                        Vec::new()
                    };
                    match result {
                        Ok(Some(response)) => {
                            if let (Some(endpoint), Some(window_handle)) = (endpoint, window_handle)
                            {
                                match DeviceActivationQrPresentation::from_created_device(
                                    &endpoint.gateway_base_url,
                                    response.activation,
                                ) {
                                    Ok(presentation) => {
                                        recovery_presentation = Some((window_handle, presentation));
                                    }
                                    Err(_) => {
                                        view.members_error =
                                            Some(t!("settings.members.recovery_failed").to_string())
                                    }
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(_) => {
                            view.members_error =
                                Some(t!("settings.members.action_failed").to_string())
                        }
                    }
                    if refetches.is_empty() {
                        view.refresh_members(false, cx);
                        view.refresh_all_workspace_members(cx);
                    } else {
                        view.apply_administration_refetches(refetches, cx);
                    }
                    cx.notify();
                });
                if let Some((window_handle, presentation)) = recovery_presentation {
                    let this = this.clone();
                    let _ = window_handle.update(&mut cx, |_window, window, cx| {
                        let _ = this.update(cx, |view, cx| {
                            view.open_recovery_device_dialog(presentation, window, cx)
                        });
                    });
                }
            }
        })
        .detach();
    }

    fn open_recovery_device_dialog(
        &mut self,
        presentation: DeviceActivationQrPresentation,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let state = cx.new(|_| Some(presentation));
        let sender = self.gateway.ws_command_sender.clone();
        window.open_dialog(cx, move |dialog, _window, cx| {
            let snapshot = state.read(cx);
            let content = snapshot.as_ref().map(|presentation| {
                DeviceActivationForm::new(
                    DeviceActivationFormPhase::Ready(presentation.clone()),
                    t!("settings.members.recovery_description").to_string(),
                )
            });
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
                        .child(t!("settings.members.recovery_title").to_string()),
                )
                .footer(DialogFooter::new().children({
                    let state = state.clone();
                    let sender = sender.clone();
                    vec![
                        default_primary_button("recovery-close")
                            .label(t!("settings.members.recovery_done").to_string())
                            .on_click({
                                let state = state.clone();
                                let sender = sender.clone();
                                move |_, window, cx| {
                                    let session_id = state
                                        .read(cx)
                                        .as_ref()
                                        .map(|presentation| presentation.session_id.clone());
                                    state.update(cx, |presentation, _| *presentation = None);
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
                    ]
                }))
                .when_some(content, |dialog, content| dialog.child(content))
        });
    }
}

fn member_action_menu_item(
    label: String,
    icon: PioneerIconName,
    action: AdministrationAction,
    pending: bool,
    desktop: Entity<PioneerDesktop>,
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

fn member_kind_label(kind: PrincipalKind) -> String {
    match kind {
        PrincipalKind::Superuser => t!("settings.members.kind_superuser"),
        PrincipalKind::User => t!("settings.members.kind_user"),
    }
    .to_string()
}

fn member_feedback(label: String, loading: bool, cx: &mut Context<PioneerDesktop>) -> AnyElement {
    v_flex()
        .min_h(px(160.))
        .items_center()
        .justify_center()
        .gap_2()
        .when(loading, |content| content.child(Spinner::new()))
        .child(div().text_sm().opacity(0.6).child(label))
        .bg(cx.theme().background)
        .into_any_element()
}

#[cfg(test)]
mod tests {
    #[::core::prelude::v1::test]
    fn desktop_members_source_uses_shared_actions_http_avatar_and_ephemeral_recovery() {
        let source = include_str!("members.rs");
        assert!(source.contains("member_list_row"));
        assert!(source.contains("resolve_member_avatar"));
        assert!(source.contains("PromptLevel::Warning"));
        assert!(source.contains("expected_status: expected_member_status"));
        assert!(source.contains("state.update(cx, |presentation, _| *presentation = None)"));
        assert!(source.contains("AuthSessionStatus::Pending"));
        assert!(source.contains("finish_conflicted_action"));
        assert!(source.contains("apply_administration_refetches"));
        assert!(source.contains("DeviceActivationForm::new"));
        assert!(source.contains("Avatar::new()"));
        assert!(source.contains("Tag::secondary()"));
        assert!(source.contains("Toggle::new"));
        assert!(source.contains(".context_menu"));
        assert!(source.contains("save_member_workspaces"));
        assert!(!source.contains(&["selected", "_member_id"].concat()));
        assert!(!source.contains(&["render_member", "_detail"].concat()));
        assert!(!source.contains(&["member", "_avatar_get"].concat()));
        assert!(!source.contains(&["tracing", "::"].concat()));
    }
}
