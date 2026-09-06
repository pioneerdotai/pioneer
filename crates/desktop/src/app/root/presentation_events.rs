use super::*;

pub(crate) struct FrameChanged;
pub(crate) struct SidebarChanged;
impl gpui_kit::EventEmitter<FrameChanged> for LegacyScreenAdapter {}
impl gpui_kit::EventEmitter<SidebarChanged> for LegacyScreenAdapter {}

#[derive(PartialEq)]
pub(super) struct FramePresentation {
    window_route: crate::desktop_navigation::WindowRoute,
    switcher: bool,
    keepawake: Option<bool>,
    can_manage: bool,
    can_notify: bool,
    notification_refresh: u64,
    unread_notifications: usize,
    notifications_loading: bool,
    notifications_error: Option<String>,
    status: Option<String>,
    artifacts: bool,
    members: bool,
}
impl LegacyScreenAdapter {
    pub(super) fn publish_frame_changes(&mut self, cx: &mut Context<Self>) {
        let capabilities = self.principal_presentation_capabilities();
        let frame = FramePresentation {
            window_route: self.window_route(),
            switcher: self
                .gateway
                .runtime
                .as_ref()
                .is_some_and(|runtime| !runtime.endpoints().is_empty()),
            keepawake: self
                .gateway
                .settings
                .as_ref()
                .map(|settings| settings.general.keepawake),
            can_manage: capabilities.can_manage_capabilities,
            can_notify: capabilities.can_read_own_notifications,
            notification_refresh: self.task_user_notifications_refresh_generation,
            unread_notifications: self
                .task_user_notifications
                .iter()
                .filter(|item| item.acknowledged_at.is_none())
                .count(),
            notifications_loading: self.task_user_notifications_loading,
            notifications_error: self.task_user_notifications_error.clone(),
            status: self
                .should_show_active_thread_status()
                .then(|| self.active_thread_status_text()),
            artifacts: self.show_thread_artifacts_sidebar,
            members: self.show_thread_members_sidebar,
        };
        if self.frame_presentation.as_ref() != Some(&frame) {
            self.frame_presentation = Some(frame);
            cx.emit(FrameChanged);
        }
    }
}
