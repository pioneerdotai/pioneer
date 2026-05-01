use crate::app::{root::PioneerDesktop, thread::fallback_title_from_first_user_text};
use gpui::{prelude::*, *};
use gpui_component::{theme::ActiveTheme, *};

impl PioneerDesktop {
    pub(crate) fn render_thread_header(&self, cx: &mut Context<Self>) -> AnyElement {
        let thread_title = self.active_thread_header_title();

        h_flex()
            .justify_between()
            .items_center()
            .px_6()
            .py_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(div().text_sm().font_semibold().child(thread_title))
            .child(div())
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

        if let Some(name) = thread
            .name
            .as_ref()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
        {
            return name.to_owned();
        }

        fallback_title_from_first_user_text(thread.preview.as_str())
            .unwrap_or_else(|| t!("sidebar.thread.untitled").to_string())
    }
}
