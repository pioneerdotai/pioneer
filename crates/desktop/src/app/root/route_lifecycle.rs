use super::*;

impl LegacyScreenAdapter {
    pub(crate) fn activate_navigation(&mut self, route: MainContentView, cx: &mut Context<Self>) {
        let current = self.main_content_view();
        let toggle = current == route
            || (route == MainContentView::Threads && current == MainContentView::AgentsDoc);
        if toggle {
            self.shell_state
                .update(cx, |layout, cx| layout.toggle_sidebar(cx));
        } else {
            match route {
                MainContentView::Threads => self.set_main_content_view(route, cx),
                MainContentView::Providers => self.open_providers_screen_from_bottom_bar(cx),
                MainContentView::Mcp => self.open_mcp_screen_from_bottom_bar(cx),
                MainContentView::Skills => self.open_skills_screen_from_bottom_bar(cx),
                MainContentView::Administration => {
                    self.open_administration_screen_from_bottom_bar(cx)
                }
                MainContentView::Settings => {
                    self.open_settings_content_from_sidebar(SettingsContentView::Account, cx)
                }
                _ => return,
            }
        }
        cx.notify();
    }

    pub(crate) fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        self.window_active = active;
        self.mcp_view
            .update(cx, |view, cx| view.set_window_active(active, cx));
        self.skills_view
            .update(cx, |view, cx| view.set_window_active(active, cx));
        self.providers_view.update(cx, |view, cx| view.set_window_active(active, cx));
        self.administration_view.update(cx, |view, cx| view.set_window_active(active, cx));
        self.reconcile_route_activity(cx);
    }
    pub(in crate::app) fn reconcile_route_activity(&mut self, cx: &mut Context<Self>) {
        let activity = self
            .navigation
            .activity(self.main_content_view(), self.window_active);
        let active = activity == crate::desktop_navigation::RouteActivity::Active;
        if active
            && self.main_content_view() == MainContentView::Settings
            && self.settings_content_view() == SettingsContentView::SelfImprovement
        {
            self.start_self_improvement_status_poll(cx);
        } else {
            self.self_improvement_status_poll.take();
        }
    }

    pub(crate) fn close_route_bindings(&mut self, cx: &mut Context<Self>) {
        self.member_avatar_state.close();
        self.mcp_view
            .update(cx, |view, cx| view.set_window_active(false, cx));
        self.skills_view
            .update(cx, |view, cx| view.set_window_active(false, cx));
        self.self_improvement_status_poll.take();
        self.gateway.compatibility_task.take();
        self.gateway.settings_task.take();
        self.gateway.identity_task.take();
        self.gateway.session_task.take();
        self.gateway.transport_verification_task.take();
        self.invitation_join_input_subscriptions.clear();
        self.profile_editor_input_subscriptions.clear();
    }
}
