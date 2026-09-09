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
        if self.invitation_join.is_some() {
            crate::desktop_navigation::WindowRoute::InvitationJoin
        } else if self.is_gateway_setup_required() {
            crate::desktop_navigation::WindowRoute::GatewaySetup
        } else {
            crate::desktop_navigation::WindowRoute::Main
        }
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
        let invitation_join = self.invitation_join.clone();
        let invitation_active = invitation_join.is_some();

        let theme_icon = if cx.theme().mode.is_dark() {
            gpui_kit::component::IconName::Sun
        } else {
            gpui_kit::component::IconName::Moon
        };

        let is_gateway_setup_required = self.is_gateway_setup_required();
        let show_gateway_switcher = !invitation_active
            && (!is_gateway_setup_required
                || self
                    .gateway
                    .runtime
                    .as_ref()
                    .is_some_and(|runtime| !runtime.endpoints().is_empty()));
        let keepawake_enabled = self
            .gateway
            .settings
            .as_ref()
            .is_some_and(|settings| settings.general.keepawake);
        let keepawake_available = !is_gateway_setup_required && self.gateway.settings.is_some();
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
                                self.gateway.switcher_view.clone().into_any_element()
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
                                        Button::new("toggle-keepawake")
                                            .ghost()
                                            .small()
                                            .compact()
                                            .disabled(!keepawake_available)
                                            .tooltip(
                                                t!("settings.option.keepawake.tooltip").to_string(),
                                            )
                                            .child(
                                                Icon::new(PioneerIconName::PowerOff)
                                                    .size_3p5()
                                                    .opacity(0.6)
                                                    .when(keepawake_enabled, |this| {
                                                        this.opacity(1.0)
                                                            .text_color(cx.theme().blue)
                                                    }),
                                            )
                                            .on_click(cx.listener(|view, _, _, cx| {
                                                let Some(settings) = view.gateway.settings.as_ref()
                                                else {
                                                    view.refresh_gateway_settings(cx);
                                                    cx.notify();
                                                    return;
                                                };

                                                view.apply_keepawake_setting(
                                                    !settings.general.keepawake,
                                                    cx,
                                                );
                                                cx.notify();
                                            }))
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
            MainContentView::Settings => self.render_settings_sidebar(cx),
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
        if let Some(invitation) = self.invitation_join.clone() {
            return self.render_desktop_invitation_join(invitation, window, cx);
        }
        if self.is_gateway_setup_required() {
            return self.gateway.setup_view.clone().into_any_element();
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
            MainContentView::Settings => self.render_settings(window, cx),
        }
    }
}
