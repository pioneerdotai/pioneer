use std::{
    collections::{HashSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use crate::{
    app::root::{GatewayConnectionState, PioneerDesktop},
    assets::PioneerIconName,
    components::member_picker::{MemberPicker, member_picker_items},
};
use gpui::{prelude::*, *};
use gpui_component::{
    IconName,
    avatar::Avatar,
    button::*,
    menu::{ContextMenuExt, PopupMenuItem},
    spinner::Spinner,
    theme::ActiveTheme,
    *,
};
use pioneer_client::{
    composer::state_machine::{ComposerMentionCandidate, composer_mention_candidates},
    threads::scope::ThreadScopePendingAction,
};
use pioneer_protocol::{MemberSummary, PrincipalId, ThreadVisibility, WorkspaceId};

impl PioneerDesktop {
    pub(crate) fn render_thread_members_panel(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let workspace_id = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id))
            .and_then(|workspace_id| WorkspaceId::new(workspace_id.to_owned()).ok());
        let is_private = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_coordinator(thread_id))
            .and_then(|coordinator| coordinator.thread())
            .is_some_and(|thread| thread.visibility == Some(ThreadVisibility::Private));
        let directory_loading = workspace_id
            .as_ref()
            .is_some_and(|workspace_id| self.workspace_members_loading.contains(workspace_id));
        let workspace_members = workspace_id
            .as_ref()
            .and_then(|workspace_id| self.administration.workspace_members(workspace_id))
            .map(|members| members.to_vec())
            .unwrap_or_default();
        let participant_ids = self
            .thread_members
            .iter()
            .map(|participant| participant.principal_id.clone())
            .collect::<HashSet<_>>();
        let visible_members = if is_private {
            workspace_members
                .iter()
                .filter(|member| participant_ids.contains(&member.principal_id))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            workspace_members.clone()
        };
        let candidates = if is_private && !self.thread_members_loading {
            composer_mention_candidates(
                workspace_members
                    .iter()
                    .filter(|member| !participant_ids.contains(&member.principal_id))
                    .cloned(),
            )
        } else {
            Vec::new()
        };
        let add_picker = self.render_thread_member_add_picker(
            candidates,
            is_private && !directory_loading,
            window,
            cx,
        );
        let current_principal_id = self
            .gateway
            .current_auth
            .as_ref()
            .map(|auth| auth.principal.id.clone());
        let loading = self.thread_members_loading || directory_loading;
        let error = self.thread_scope_error.clone();

        let mut list = v_flex().w_full().gap_1();
        for member in &visible_members {
            list = list.child(self.render_thread_member_row(
                member,
                is_private,
                current_principal_id.as_ref(),
                cx,
            ));
        }

        v_flex()
            .id("thread-members-panel")
            .h_full()
            .w_full()
            .min_w_0()
            .border_l_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().background)
            .child(
                h_flex()
                    .px_4()
                    .pt_2p5()
                    .pb_1()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .opacity(0.4)
                            .child(t!("settings.sidebar.members").to_string()),
                    )
                    .child(add_picker),
            )
            .child(
                v_flex()
                    .id("thread-members-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scroll()
                    .px_3()
                    .pt_0p5()
                    .pb_3()
                    .gap_3()
                    .when(loading, |this| {
                        this.child(
                            h_flex()
                                .gap_2()
                                .items_center()
                                .text_xs()
                                .child(Spinner::new().small())
                                .child(t!("settings.members.loading").to_string()),
                        )
                    })
                    .when_some(error, |this, error| {
                        this.child(
                            div()
                                .rounded_md()
                                .border_1()
                                .border_color(cx.theme().danger.opacity(0.45))
                                .bg(cx.theme().danger.opacity(0.08))
                                .p_3()
                                .text_sm()
                                .text_color(cx.theme().danger)
                                .child(error),
                        )
                    })
                    .child(if visible_members.is_empty() && !loading {
                        v_flex()
                            .w_full()
                            .items_center()
                            .gap_2()
                            .py_8()
                            .text_center()
                            .opacity(0.6)
                            .child(Icon::new(PioneerIconName::UserCheck).size_4())
                            .child(
                                div()
                                    .text_xs()
                                    .child(t!("settings.members.empty").to_string()),
                            )
                            .into_any_element()
                    } else {
                        list.into_any_element()
                    }),
            )
            .into_any_element()
    }

    fn render_thread_member_add_picker(
        &mut self,
        candidates: Vec<ComposerMentionCandidate>,
        private_thread_ready: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if self.thread_member_items != candidates {
            self.thread_member_items = candidates.clone();
            let select_items = member_picker_items(candidates.iter().cloned(), |principal_id| {
                self.member_avatar_state
                    .presentation(principal_id)
                    .and_then(|avatar| avatar.cached_image_path.clone())
            });
            self.thread_member_select.update(cx, |state, cx| {
                state.set_items(select_items, window, cx);
            });
        }

        let pending = !matches!(self.thread_scope_pending, ThreadScopePendingAction::Idle);
        let disabled = !private_thread_ready
            || candidates.is_empty()
            || pending
            || self.gateway.connection_state != GatewayConnectionState::Connected;
        if disabled {
            return Button::new("thread-members-add-disabled")
                .small()
                .ghost()
                .compact()
                .disabled(true)
                .loading(pending)
                .mr(px(-8.))
                .tooltip(t!("thread.scope.add").to_string())
                .child(Icon::new(IconName::Plus).size_4().opacity(0.6))
                .into_any_element();
        }

        div()
            .mr(px(-8.))
            .child(MemberPicker::new(
                "thread-members-add-picker",
                "thread-members-add-trigger",
                &self.thread_member_select,
                Icon::new(IconName::Plus).size_4().opacity(0.6),
            ))
            .into_any_element()
    }

    fn render_thread_member_row(
        &self,
        member: &MemberSummary,
        can_manage: bool,
        current_principal_id: Option<&PrincipalId>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let principal_id = member.principal_id.clone();
        let element_key = principal_element_key(&member.principal_id);
        let is_current = current_principal_id == Some(&member.principal_id);
        let removal_disabled = !matches!(self.thread_scope_pending, ThreadScopePendingAction::Idle);
        let avatar_path = self
            .member_avatar_state
            .presentation(&member.principal_id)
            .and_then(|avatar| avatar.cached_image_path.clone());
        let desktop = cx.entity().clone();

        let row = h_flex()
            .id(("thread-member-row", element_key))
            .w_full()
            .items_center()
            .gap_3()
            .rounded_md()
            .px_1()
            .py_1p5()
            .hover(|this| this.bg(cx.theme().muted))
            .child(
                Avatar::new()
                    .name(member.display_name.clone())
                    .size_10()
                    .when_some(avatar_path, |this, path| this.src(path)),
            )
            .child(
                v_flex()
                    .min_w_0()
                    .flex_1()
                    .gap_0p5()
                    .child(
                        div()
                            .truncate()
                            .text_sm()
                            .line_height(rems(0.875))
                            .child(member.display_name.clone()),
                    )
                    .when(!member.nickname.is_empty(), |this| {
                        this.child(
                            div()
                                .truncate()
                                .text_xs()
                                .opacity(0.6)
                                .child(format!("@{}", member.nickname)),
                        )
                    }),
            );

        if can_manage && !is_current {
            row.context_menu(move |menu, _, _| {
                let principal_id = principal_id.clone();
                let desktop = desktop.clone();
                menu.min_w(px(180.)).item(
                    PopupMenuItem::new(t!("gateway.action.delete").to_string())
                        .icon(PioneerIconName::Trash)
                        .disabled(removal_disabled)
                        .on_click(move |_, _, cx| {
                            let principal_id = principal_id.clone();
                            let _ = desktop.update(cx, |view, cx| {
                                view.remove_thread_member(principal_id, cx);
                            });
                        }),
                )
            })
            .into_any_element()
        } else {
            row.into_any_element()
        }
    }
}

fn principal_element_key(principal_id: &PrincipalId) -> u64 {
    let mut hasher = DefaultHasher::new();
    principal_id.hash(&mut hasher);
    hasher.finish()
}
