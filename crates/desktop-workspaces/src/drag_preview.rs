use gpui_kit::component::{theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};

#[derive(Clone)]
pub(crate) enum SidebarTreeDragItem {
    Thread { thread_id: String },
    Folder { folder_id: String },
}

#[derive(Clone)]
pub(crate) struct SidebarTreeDragPayload {
    pub(crate) label: String,
    pub(crate) item: SidebarTreeDragItem,
}

impl Render for SidebarTreeDragPayload {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .px_3()
            .py_1()
            .gap_2()
            .items_center()
            .bg(cx.theme().popover)
            .border_1()
            .border_color(cx.theme().border)
            .rounded_md()
            .shadow_md()
            .child(match self.item {
                SidebarTreeDragItem::Thread { .. } => Icon::new(IconName::File)
                    .size_4()
                    .text_color(cx.theme().popover_foreground)
                    .into_any_element(),
                SidebarTreeDragItem::Folder { .. } => Icon::new(IconName::Folder)
                    .size_4()
                    .text_color(cx.theme().popover_foreground)
                    .into_any_element(),
            })
            .child(
                div()
                    .text_sm()
                    .font_light()
                    .text_color(cx.theme().popover_foreground)
                    .child(self.label.clone()),
            )
    }
}
