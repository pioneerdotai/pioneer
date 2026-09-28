use crate::panel_layout::{ThreadPanelControlsView, ThreadPanelLayoutStore};
use gpui_kit::{
    AppContext, Context, Entity, IntoElement, ParentElement, Render, Styled, Window,
    component::{ActiveTheme, h_flex},
};

pub(crate) struct ThreadFooterView {
    controls: Entity<ThreadPanelControlsView>,
}
impl ThreadFooterView {
    pub(crate) fn new(layout: Entity<ThreadPanelLayoutStore>, cx: &mut Context<Self>) -> Self {
        let controls = cx.new(|cx| ThreadPanelControlsView::new(layout, cx));
        Self { controls }
    }
}
impl Render for ThreadFooterView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        h_flex()
            .w_full()
            .h_8()
            .flex_none()
            .px_2()
            .border_t_1()
            .border_color(cx.theme().border)
            .items_center()
            .justify_end()
            .gap_1()
            .child(self.controls.clone())
    }
}
