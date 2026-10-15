//! Window-local layout and persistence. No Client or feature state lives here.
use gpui_kit::{App, Context, Pixels, Subscription, Window, px};

pub(crate) struct ShellStateStore {
    sidebar_visible: bool,
    sidebar_width: Pixels,
    _bounds: Subscription,
}
impl ShellStateStore {
    pub(crate) fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let bounds = cx.observe_window_bounds(window, |_, window, cx| {
            crate::window::persist_window_settings(window, cx);
        });
        Self {
            sidebar_visible: true,
            sidebar_width: px(320.),
            _bounds: bounds,
        }
    }
    pub(crate) fn sidebar_visible(&self) -> bool {
        self.sidebar_visible
    }
    pub(crate) fn sidebar_width(&self) -> Pixels {
        self.sidebar_width
    }
    pub(crate) fn toggle_sidebar(&mut self, cx: &mut Context<Self>) {
        self.sidebar_visible = !self.sidebar_visible;
        cx.notify();
    }
    pub(crate) fn set_sidebar_width(&mut self, width: Pixels, cx: &mut Context<Self>) {
        if !f32::from(width).is_finite() {
            return;
        }
        let width = width.clamp(px(260.), px(520.));
        if self.sidebar_width != width {
            self.sidebar_width = width;
            cx.notify();
        }
    }
    pub(crate) fn persist(&self, window: &Window, cx: &mut App) {
        crate::window::persist_window_settings(window, cx);
    }
}
