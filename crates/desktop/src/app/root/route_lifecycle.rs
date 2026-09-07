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
        self.reconcile_route_activity(cx);
    }
    pub(in crate::app) fn reconcile_route_activity(&mut self, cx: &mut Context<Self>) {
        let activity = self
            .navigation
            .activity(self.main_content_view(), self.window_active);
        let active = activity == crate::desktop_navigation::RouteActivity::Active;
        match self.main_content_view() {
            MainContentView::Mcp | MainContentView::McpDetails if active => {
                self.ensure_mcp_poller(cx)
            }
            _ => {
                self.mcp_poller.take();
            }
        }
        match self.main_content_view() {
            MainContentView::Skills | MainContentView::SkillDetails if active => {
                self.ensure_skills_poller(cx)
            }
            _ => {
                self.skills_poller.take();
            }
        }
        if active
            && self.main_content_view() == MainContentView::Settings
            && self.settings_content_view() == SettingsContentView::SelfImprovement
        {
            self.start_self_improvement_status_poll(cx);
        } else {
            self.self_improvement_status_poll.take();
        }
        let thread_active = active && self.main_content_view() == MainContentView::Threads;
        if !thread_active && self.desktop_voice_composer.is_active() {
            self.cancel_desktop_voice_hold("route_inactive", cx);
        }
        // Focus controls activity, not the content of a still-visible thread.
        self.thread_bindings
            .select(if self.navigation.is_visible(MainContentView::Threads) {
                self.current_active_thread_id()
            } else {
                None
            });
        self.running_indicator_views
            .borrow_mut()
            .set_active(thread_active, cx);
        if activity == crate::desktop_navigation::RouteActivity::Dormant {
            self.thread_bindings.clear();
            *self.running_indicator_views.borrow_mut() = Default::default();
            self.thread_timeline_terminal_item.borrow_mut().clear();
            self.thread_timeline_item_expanded.borrow_mut().clear();
            *self.thread_timeline_view_state.borrow_mut() = Default::default();
            *self.code_highlight_cache.borrow_mut() = Default::default();
        }
    }

    pub(crate) fn close_route_bindings(&mut self, cx: &mut Context<Self>) {
        self.member_avatar_state.close();
        self.cancel_desktop_voice_hold("window_closed", cx);
        self.desktop_voice_status_poll_generation =
            self.desktop_voice_status_poll_generation.wrapping_add(1);
        self.mcp_poller.take();
        self.skills_poller.take();
        self.self_improvement_status_poll.take();
        self.thread_binding_task.take();
        self.thread_bindings.clear();
        self.gateway.compatibility_task.take();
        self.gateway.settings_task.take();
        self.gateway.identity_task.take();
        self.gateway.session_task.take();
        self.gateway.transport_verification_task.take();
        for cancellation in self.artifact_download_cancellations.values() {
            cancellation.cancel();
        }
        self.artifact_download_cancellations.clear();
        if let Some(cancel) = self.skills_upload_cancel_token.take() {
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        self.invitation_join_input_subscriptions.clear();
        self.profile_editor_input_subscriptions.clear();
        self.composer_input_subscription.take();
        self.composer_mention_select_subscription.take();
        self.thread_member_select_subscription.take();
        *self.code_highlight_cache.borrow_mut() = Default::default();
        *self.running_indicator_views.borrow_mut() = Default::default();
        self.thread_timeline_terminal_item.borrow_mut().clear();
        self.thread_timeline_item_expanded.borrow_mut().clear();
        *self.thread_timeline_view_state.borrow_mut() = Default::default();
    }
}
