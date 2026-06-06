use crate::app::{root::PioneerDesktop, thread::thread_display_title};
use gpui::{prelude::*, *};
use gpui_component::{
    button::*,
    menu::{DropdownMenu, PopupMenuItem},
    theme::ActiveTheme,
    *,
};

impl PioneerDesktop {
    pub(crate) fn render_thread_header(&self, cx: &mut Context<Self>) -> AnyElement {
        let thread_title = self.active_thread_header_title();
        let active_thread_id = self
            .current_active_thread_id()
            .filter(|thread_id| self.draft_thread_id() != Some(*thread_id))
            .and_then(|thread_id| {
                self.thread_coordinator(thread_id)
                    .and_then(|coordinator| coordinator.thread())
                    .map(|_| thread_id.to_owned())
            });

        h_flex()
            .justify_between()
            .items_center()
            .pl_6()
            .pr_4()
            .py_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                div()
                    .min_w_0()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .text_sm()
                    .font_semibold()
                    .child(thread_title),
            )
            .child(self.render_thread_title_menu(active_thread_id, cx))
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
        let desktop_entity = cx.entity().clone();

        Button::new("thread-title-menu")
            .small()
            .ghost()
            .compact()
            .child(Icon::new(IconName::Ellipsis).size_4().opacity(0.65))
            .dropdown_menu_with_anchor(Corner::TopRight, move |menu, _, _| {
                let thread_id = thread_id.clone();
                let desktop_entity = desktop_entity.clone();
                menu.min_w(px(160.)).item(
                    PopupMenuItem::new(t!("sidebar.contextmenu.thread.edit").to_string()).on_click(
                        move |_, window, cx| {
                            let thread_id = thread_id.clone();
                            let _ = desktop_entity.update(cx, |view, cx| {
                                view.open_rename_thread_dialog(thread_id, window, cx);
                                cx.notify();
                            });
                        },
                    ),
                )
            })
            .into_any_element()
    }

    fn active_thread_header_title(&self) -> String {
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
