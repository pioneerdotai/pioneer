use super::{MainContentView, PioneerDesktop};
use crate::{assets::PioneerIconName, settings::WindowThemePreference, window};
use gpui_kit::component::{
    Disableable, Icon, Sizable,
    button::{Button, ButtonVariants},
    h_flex,
    separator::Separator,
    theme::{ActiveTheme, Theme, ThemeMode},
};
use gpui_kit::{prelude::*, *};

impl PioneerDesktop {
    pub(crate) fn window_route(&self) -> crate::desktop_navigation::WindowRoute {
        self.onboarding_route
    }
    pub(crate) fn title_bar_surface(
        owner: &Entity<Self>,
        window: &mut Window,
        cx: &mut App,
    ) -> AnyElement {
        owner.update(cx, |view, cx| view.render_title_bar(window, cx))
    }
    pub(crate) fn bottom_bar_surface(
        owner: &Entity<Self>,
        action_region: &FocusHandle,
        cx: &mut App,
    ) -> AnyElement {
        owner.update(cx, |view, cx| view.render_bottom_bar(action_region, cx))
    }
    pub(crate) fn sidebar_surface(
        owner: &Entity<Self>,
        route: MainContentView,
        cx: &mut App,
    ) -> AnyElement {
        owner.update(cx, |view, cx| view.render_selected_sidebar(route, cx))
    }
    fn render_title_bar(&self, _window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let invitation_active =
            self.onboarding_route == crate::desktop_navigation::WindowRoute::InvitationJoin;

        let theme_icon = if cx.theme().mode.is_dark() {
            gpui_kit::component::IconName::Sun
        } else {
            gpui_kit::component::IconName::Moon
        };

        let is_gateway_setup_required = self.is_gateway_setup_required();
        let show_gateway_switcher = !invitation_active;
        let show_task_notifications = !is_gateway_setup_required
            && self
                .principal_presentation_capabilities()
                .can_read_own_notifications;

        gpui_kit::component::TitleBar::new()
            .child(
                h_flex()
                    .w_full()
                    .pr_4()
                    .justify_between()
                    .items_center()
                    .bg(cx.theme().title_bar)
                    .child(
                        h_flex()
                            .h_full()
                            .items_center()
                            .child(if show_gateway_switcher {
                                self.onboarding_view
                                    .read(cx)
                                    .gateway_switcher_surface()
                                    .into_any_element()
                            } else {
                                div().into_any_element()
                            }),
                    )
                    .child(
                        h_flex()
                            .h_full()
                            .gap_1()
                            .items_center()
                            .child(if show_task_notifications {
                                self.render_task_user_notifications_button(cx)
                            } else {
                                div().into_any_element()
                            })
                            .child(if show_task_notifications {
                                Separator::vertical().h_4().mx_0p5().into_any_element()
                            } else {
                                div().into_any_element()
                            })
                            .child(
                                h_flex()
                                    .gap_1()
                                    .child(if !is_gateway_setup_required {
                                        self.settings_view
                                            .read(cx)
                                            .general_actions_surface()
                                            .into_any_element()
                                    } else {
                                        div().into_any_element()
                                    })
                                    .child(
                                        Button::new("toggle-theme")
                                            .ghost()
                                            .small()
                                            .compact()
                                            .child(Icon::new(theme_icon).size_3p5().opacity(0.6))
                                            .on_click(cx.listener(|_, _, window, cx| {
                                                let (mode, theme_preference) = if cx
                                                    .theme()
                                                    .mode
                                                    .is_dark()
                                                {
                                                    (ThemeMode::Light, WindowThemePreference::Light)
                                                } else {
                                                    (ThemeMode::Dark, WindowThemePreference::Dark)
                                                };
                                                Theme::change(mode, Some(window), cx);
                                                window::persist_theme_preference(
                                                    window,
                                                    theme_preference,
                                                    cx,
                                                );
                                            })),
                                    ),
                            ),
                    ),
            )
            .into_any_element()
    }
    fn render_selected_sidebar(
        &self,
        route: MainContentView,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match route {
            MainContentView::Settings => self
                .settings_view
                .read(cx)
                .sidebar_surface()
                .into_any_element(),
            MainContentView::Providers => self.providers_view.read(cx).sidebar().into_any_element(),
            MainContentView::Administration => self
                .administration_view
                .read(cx)
                .sidebar()
                .into_any_element(),
            MainContentView::McpDetails => self.mcp_view.read(cx).sidebar().into_any_element(),
            MainContentView::Mcp => self.mcp_view.read(cx).sidebar().into_any_element(),
            MainContentView::SkillDetails => self.skills_view.read(cx).sidebar().into_any_element(),
            MainContentView::Skills => self.skills_view.read(cx).sidebar().into_any_element(),
            MainContentView::Threads | MainContentView::AgentsDoc => div().into_any_element(),
        }
    }
}
impl Render for PioneerDesktop {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.onboarding_route != crate::desktop_navigation::WindowRoute::Main {
            return self.onboarding_view.clone().into_any_element();
        }
        match self.main_content_view() {
            MainContentView::Threads => div().into_any_element(),
            MainContentView::AgentsDoc => self.render_agents_doc_editor(cx),
            MainContentView::Providers => self.providers_view.clone().into_any_element(),
            MainContentView::Administration => self.administration_view.clone().into_any_element(),
            MainContentView::Mcp => self.mcp_view.clone().into_any_element(),
            MainContentView::McpDetails => self.mcp_view.clone().into_any_element(),
            MainContentView::Skills => self.skills_view.clone().into_any_element(),
            MainContentView::SkillDetails => self.skills_view.clone().into_any_element(),
            MainContentView::Settings => self.settings_view.clone().into_any_element(),
        }
    }
}
