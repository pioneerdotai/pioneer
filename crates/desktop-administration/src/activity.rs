use gpui_kit::component::spinner::Spinner;
use gpui_kit::{prelude::*, *};

/// Stock animation invalidations terminate at this retained loading control.
pub(crate) struct LoadingIndicator {
    active: bool,
}
impl LoadingIndicator {
    pub(crate) fn new(cx: &mut App) -> Entity<Self> {
        cx.new(|_| Self { active: false })
    }
    pub(crate) fn set_active(&mut self, active: bool, cx: &mut Context<Self>) {
        if self.active != active {
            self.active = active;
            cx.notify();
        }
    }
}
impl Render for LoadingIndicator {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div().when(self.active, |view| view.child(Spinner::new()))
    }
}
