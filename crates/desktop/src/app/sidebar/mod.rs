mod actions;
mod dialogs;
mod view;

use gpui::{prelude::*, *};
use gpui_component::{theme::ActiveTheme, *};
pub(in crate::app) use view::agents_doc_tree_node_key;

#[derive(Clone)]
pub(super) enum SidebarTreeDragItem {
    Thread { thread_id: String },
    Folder { folder_id: String },
}

#[derive(Clone)]
pub(super) struct SidebarTreeDragPayload {
    pub(super) label: String,
    pub(super) item: SidebarTreeDragItem,
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
