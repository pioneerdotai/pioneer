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

// GPUI's drag session API requires a transient Render host. The preview itself
// is a value; the framework creates/drops this host with the drag operation.
pub(crate) struct SidebarDragSession(pub(crate) SidebarTreeDragPayload);
impl Render for SidebarDragSession {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        SidebarTreeDragPreview(self.0.clone())
    }
}
#[derive(IntoElement)]
struct SidebarTreeDragPreview(SidebarTreeDragPayload);
impl RenderOnce for SidebarTreeDragPreview {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
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
            .child(match self.0.item {
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
                    .child(self.0.label),
            )
    }
}
