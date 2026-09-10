use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crate::{
    assets::PioneerIconName, member_picker::MemberPicker, members::ThreadMembersView,
    screen::GatewayConnectionState,
};
use gpui_kit::component::{
    IconName,
    avatar::Avatar,
    button::*,
    menu::{ContextMenuExt, PopupMenuItem},
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::avatars::{MemberSummary, PrincipalId};
use pioneer_client::{
    composer::state_machine::ComposerMentionCandidate, threads::scope::ThreadScopePendingAction,
};

impl ThreadMembersView {
    pub(crate) fn render_thread_members_panel(&mut self, cx: &mut Context<Self>) -> AnyElement {
        pioneer_client::timeline::diagnostics::record_qualification_diagnostic!(record_render(
            pioneer_client::timeline::diagnostics::RenderRegion::SidePanel
        ));
        let is_private = self.is_private();
        let capabilities = self.thread_scope_capabilities();
        let directory_loading = self.thread_member_directory_loading();
        let visible_members = self
            .thread_member_input
            .as_ref()
            .map(|input| input.visible_members(is_private))
            .unwrap_or_default();
        let candidates = &self.thread_member_items;
        let add_picker = self.render_thread_member_add_picker(
            candidates.clone(),
            capabilities.can_manage_private_participants && !directory_loading,
        );
        let current_principal_id = self
            .identity_input
            .as_ref()
            .and_then(|identity| identity.current_auth.as_ref())
            .map(|auth| auth.principal.id.clone());
        let loading = self.thread_members_loading() || directory_loading;
        let error = self.thread_scope_error();

        let mut list = v_flex().w_full().gap_1();
        for member in &visible_members {
            list = list.child(self.render_thread_member_row(
                member,
                capabilities.can_manage_private_participants,
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
                                .child(
                                    crate::qualification_diagnostics::spinner!(
                                        pioneer_client::timeline::diagnostics::AnimationSourceId::ThreadMemberList,
                                    )
                                    .small(),
                                )
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
    ) -> AnyElement {
        let pending = !matches!(self.thread_scope_pending(), ThreadScopePendingAction::Idle);
        let disabled = !private_thread_ready
            || candidates.is_empty()
            || pending
            || self.connection_state != GatewayConnectionState::Connected;
        if disabled {
            return Button::new("thread-members-add-disabled")
                .small()
                .ghost()
                .compact()
                .disabled(true)
                .loading(crate::qualification_diagnostics::observed_loading!(
                    pioneer_client::timeline::diagnostics::AnimationSourceId::ThreadMemberAddButton,
                    pending,
                ))
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
        let removal_disabled =
            !matches!(self.thread_scope_pending(), ThreadScopePendingAction::Idle);
        let avatar_path = self.avatar_path(&member.principal_id, cx);
        let desktop = cx.entity().downgrade();

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
                    .when_some(avatar_path, |this, path| {
                        this.src(std::path::PathBuf::from(path))
                    }),
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
