use crate::app::root::PioneerDesktop;
use gpui_kit::{prelude::*, *};
impl PioneerDesktop {
    pub(in crate::app) fn render_task_user_notifications_button(
        &self,
        _: &mut Context<Self>,
    ) -> AnyElement {
        self.task_notification_surface
            .clone()
            .map(IntoElement::into_any_element)
            .unwrap_or_else(|| div().into_any_element())
    }
}
