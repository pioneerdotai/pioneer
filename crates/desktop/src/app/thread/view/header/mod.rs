use crate::{
    app::{
        file_openers::{file_opener_menu_row, file_opener_trigger},
        root::{GatewayConnectionState, PioneerDesktop},
        thread::thread_display_title,
    },
    assets::PioneerIconName,
    file_opener::{FileOpenerId, available_file_openers},
};
use gpui::{prelude::*, *};
use gpui_component::{
    button::*,
    menu::{DropdownMenu, PopupMenuItem},
    theme::ActiveTheme,
    *,
};
use pioneer_client::threads::scope::ThreadScopePendingAction;
use pioneer_protocol::{ThreadStatus, ThreadVisibility};

impl PioneerDesktop {
    pub(crate) fn render_thread_header(&self, cx: &mut Context<Self>) -> AnyElement {
        let task_navigation = self.active_task_thread_navigation().cloned();
        let thread_title = self.active_thread_header_title();
        let active_thread_id = if task_navigation.is_some() {
            None
        } else {
            self.current_active_thread_id()
                .filter(|thread_id| self.draft_thread_id() != Some(*thread_id))
                .and_then(|thread_id| {
                    self.thread_coordinator(thread_id)
                        .and_then(|coordinator| coordinator.thread())
                        .map(|_| thread_id.to_owned())
                })
        };
        let file_opener_thread_id = self
            .current_active_thread_id()
            .filter(|thread_id| self.draft_thread_id() != Some(*thread_id))
            .map(str::to_owned);
        h_flex()
            .justify_between()
            .items_center()
            .pl_6()
            .pr_4()
            .py_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                h_flex()
                    .min_w_0()
                    .flex_1()
                    .items_center()
                    .gap_1p5()
                    .when(task_navigation.is_some(), |this| {
                        this.child(
                            Button::new("task-child-thread-back")
                                .small()
                                .ghost()
                                .compact()
                                .p_0()
                                .child(Icon::new(IconName::ChevronLeft).size_4())
                                .on_click(cx.listener(|view, _, window, cx| {
                                    view.close_task_child_thread(window, cx);
                                })),
                        )
                    })
                    .child(
                        div()
                            .min_w_0()
                            .overflow_hidden()
                            .text_ellipsis()
                            .text_sm()
                            .font_semibold()
                            .child(thread_title),
                    ),
            )
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .child(self.render_thread_file_opener_picker(file_opener_thread_id, cx))
                    .child(self.render_thread_title_menu(active_thread_id, cx)),
            )
            .into_any_element()
    }

    fn render_thread_file_opener_picker(
        &self,
        thread_id: Option<String>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(thread_id) = thread_id else {
            return div().into_any_element();
        };
        let selected_opener = self.effective_file_opener_for_thread(thread_id.as_str(), cx);
        let workspace_opener = self.workspace_file_opener_for_thread(thread_id.as_str(), cx);
        let thread_override = self.thread_file_opener_override(thread_id.as_str(), cx);
        let desktop_entity = cx.entity().clone();

        file_opener_trigger("thread-file-opener-trigger", selected_opener)
            .dropdown_menu_with_anchor(Anchor::TopRight, move |menu, _, _| {
                let workspace_thread_id = thread_id.clone();
                let workspace_desktop_entity = desktop_entity.clone();
                let workspace_label = t!("thread.file_opener.use_workspace").to_string();
                let mut menu = menu
                    .min_w(px(220.))
                    .scrollable(true)
                    .item(
                        PopupMenuItem::element(move |_, cx| {
                            file_opener_menu_row(
                                workspace_opener,
                                workspace_label.clone().into(),
                                thread_override.is_none(),
                                cx,
                            )
                        })
                        .on_click(move |_, _, cx| {
                            let _ = workspace_desktop_entity.update(cx, |view, cx| {
                                view.apply_thread_file_opener_override(
                                    workspace_thread_id.as_str(),
                                    None,
                                    cx,
                                );
                                cx.notify();
                            });
                        }),
                    )
                    .separator();

                for available in available_file_openers() {
                    let opener: FileOpenerId = available.id;
                    let option_thread_id = thread_id.clone();
                    let option_desktop_entity = desktop_entity.clone();
                    let option_selected = thread_override == Some(opener);
                    menu = menu.item(
                        PopupMenuItem::element(move |_, cx| {
                            file_opener_menu_row(opener, opener.label().into(), option_selected, cx)
                        })
                        .on_click(move |_, _, cx| {
                            let _ = option_desktop_entity.update(cx, |view, cx| {
                                view.apply_thread_file_opener_override(
                                    option_thread_id.as_str(),
                                    Some(opener),
                                    cx,
                                );
                                cx.notify();
                            });
                        }),
                    );
                }
                menu
            })
            .into_any_element()
    }

    fn render_thread_title_menu(
        &self,
        thread_id: Option<String>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let Some(thread_id) = thread_id else {
            return div().into_any_element();
        };
        let Some(thread) = self
            .thread_coordinator(thread_id.as_str())
            .and_then(|coordinator| coordinator.thread())
        else {
            return div().into_any_element();
        };
        let can_manage_thread = self.can_manage_thread_presentation(thread_id.as_str());
        let visibility_action = can_manage_thread
            .then(|| {
                thread
                    .visibility
                    .map(|current_visibility| match current_visibility {
                        ThreadVisibility::Private => (
                            t!("thread.scope.make_public").to_string(),
                            ThreadVisibility::Workspace,
                            PioneerIconName::Eye,
                        ),
                        ThreadVisibility::Workspace => (
                            t!("thread.scope.make_private").to_string(),
                            ThreadVisibility::Private,
                            PioneerIconName::EyeOff,
                        ),
                    })
            })
            .flatten();
        let visibility_action_disabled = self.gateway.connection_state
            != GatewayConnectionState::Connected
            || thread.status == ThreadStatus::Closed
            || !matches!(self.thread_scope_pending, ThreadScopePendingAction::Idle);
        let desktop_entity = cx.entity().clone();
        let show_members = thread.visibility.is_some();

        Button::new("thread-title-menu")
            .small()
            .ghost()
            .compact()
            .child(Icon::new(IconName::Ellipsis).size_4().opacity(0.65))
            .dropdown_menu_with_anchor(Anchor::TopRight, move |menu, _, _| {
                let edit_thread_id = thread_id.clone();
                let visibility_thread_id = thread_id.clone();
                let desktop_entity = desktop_entity.clone();
                let edit_desktop_entity = desktop_entity.clone();
                let members_desktop_entity = desktop_entity.clone();
                let menu = menu.min_w(px(180.));
                let menu = if can_manage_thread {
                    menu.item(
                        PopupMenuItem::new(t!("sidebar.contextmenu.thread.edit").to_string())
                            .icon(PioneerIconName::Pen)
                            .on_click(move |_, window, cx| {
                                let _ = edit_desktop_entity.update(cx, |view, cx| {
                                    view.open_rename_thread_dialog(
                                        edit_thread_id.clone(),
                                        window,
                                        cx,
                                    );
                                    cx.notify();
                                });
                            }),
                    )
                } else {
                    menu
                };
                let menu = if show_members {
                    menu.item(
                        PopupMenuItem::new(t!("settings.sidebar.members").to_string())
                            .icon(PioneerIconName::UserCheck)
                            .on_click(move |_, _, cx| {
                                let _ = members_desktop_entity.update(cx, |view, cx| {
                                    view.open_thread_members_sidebar(cx);
                                });
                            }),
                    )
                } else {
                    menu
                };
                if let Some((visibility_label, target_visibility, visibility_icon)) =
                    visibility_action.clone()
                {
                    menu.item(
                        PopupMenuItem::new(visibility_label)
                            .icon(visibility_icon)
                            .disabled(visibility_action_disabled)
                            .on_click(move |_, _, cx| {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.update_thread_visibility(
                                        visibility_thread_id.clone(),
                                        target_visibility,
                                        cx,
                                    );
                                });
                            }),
                    )
                } else {
                    menu
                }
            })
            .into_any_element()
    }

    fn active_thread_header_title(&self) -> String {
        if let Some(navigation) = self.active_task_thread_navigation() {
            return navigation.title.clone();
        }

        let Some(active_thread_id) = self.current_active_thread_id() else {
            return t!("sidebar.thread.untitled").to_string();
        };

        if self.draft_thread_id() == Some(active_thread_id) {
            return t!("sidebar.thread.untitled").to_string();
        }

        let Some(coordinator) = self.thread_coordinator(active_thread_id) else {
            return t!("sidebar.thread.untitled").to_string();
        };

        let Some(thread) = coordinator.thread() else {
            return t!("sidebar.thread.untitled").to_string();
        };

        thread_display_title(thread).unwrap_or_else(|| t!("sidebar.thread.untitled").to_string())
    }
}
