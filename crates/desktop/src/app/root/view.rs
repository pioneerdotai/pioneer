use super::{MainContentView, PioneerDesktop};
use crate::{assets::PioneerIconName, settings::WindowThemePreference, window};
use gpui::{prelude::*, *};
use gpui_component::{
    Disableable, Icon, Sizable,
    button::{Button, ButtonVariants},
    h_flex,
    resizable::{h_resizable, resizable_panel},
    theme::{ActiveTheme, Theme, ThemeMode},
    v_flex,
};

impl Render for PioneerDesktop {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let sheet_layer = gpui_component::Root::render_sheet_layer(window, cx);
        let dialog_layer = gpui_component::Root::render_dialog_layer(window, cx);
        let notification_layer = gpui_component::Root::render_notification_layer(window, cx);

        let theme_icon = if cx.theme().mode.is_dark() {
            gpui_component::IconName::Sun
        } else {
            gpui_component::IconName::Moon
        };

        let is_gateway_setup_required = self.is_gateway_setup_required();
        let show_gateway_switcher = !is_gateway_setup_required
            || self
                .gateway
                .runtime
                .as_ref()
                .is_some_and(|runtime| !runtime.endpoints().is_empty());
        let is_settings_view_active = self.main_content_view == MainContentView::Settings;
        let is_providers_view_active = self.main_content_view == MainContentView::Providers;
        let is_administration_view_active =
            self.main_content_view == MainContentView::Administration;
        let is_mcp_view_active = self.main_content_view == MainContentView::Mcp;
        let is_mcp_details_view_active = self.main_content_view == MainContentView::McpDetails;
        let is_skills_view_active = self.main_content_view == MainContentView::Skills;
        let is_skill_details_view_active = self.main_content_view == MainContentView::SkillDetails;
        let keepawake_enabled = self
            .gateway
            .settings
            .as_ref()
            .is_some_and(|settings| settings.general.keepawake);
        let keepawake_available = !is_gateway_setup_required && self.gateway.settings.is_some();

        let body = if is_gateway_setup_required {
            self.render_initial_setup(window, cx)
        } else {
            let desktop_entity = cx.entity().clone();
            let content = match self.main_content_view {
                MainContentView::Threads => self.render_thread(window, cx),
                MainContentView::AgentsDoc => self.render_agents_doc_editor(cx),
                MainContentView::Providers => self.render_providers(window, cx),
                MainContentView::Administration => self.render_administration(window, cx),
                MainContentView::Mcp => self.render_mcp(window, cx),
                MainContentView::McpDetails => self.render_mcp_details(window, cx),
                MainContentView::Skills => self.render_skills(window, cx),
                MainContentView::SkillDetails => self.render_skill_details(window, cx),
                MainContentView::Settings => self.render_settings(window, cx),
            };

            let sidebar = if is_settings_view_active {
                self.render_settings_sidebar(cx)
            } else if is_providers_view_active {
                self.render_providers_sidebar(cx)
            } else if is_administration_view_active {
                self.render_administration_sidebar(cx)
            } else if is_mcp_details_view_active {
                self.render_mcp_details_sidebar(cx)
            } else if is_mcp_view_active {
                self.render_mcp_sidebar(cx)
            } else if is_skill_details_view_active {
                self.render_skill_details_sidebar(cx)
            } else if is_skills_view_active {
                self.render_skills_sidebar(cx)
            } else {
                self.render_sidebar(cx)
            };
            let sidebar = v_flex()
                .size_full()
                .bg(cx.theme().sidebar)
                .child(
                    div()
                        .flex_1()
                        .min_h_0()
                        .w_full()
                        .overflow_hidden()
                        .child(sidebar),
                )
                .into_any_element();

            v_flex()
                .size_full()
                .child(
                    div().flex_1().min_h_0().w_full().overflow_hidden().child(
                        h_resizable("desktop-layout")
                            .on_resize({
                                let desktop_entity = desktop_entity.clone();
                                move |state, _, cx| {
                                    let sidebar_width = state.read(cx).sizes().first().copied();
                                    if let Some(sidebar_width) = sidebar_width {
                                        let _ = desktop_entity.update(cx, |view, cx| {
                                            view.sidebar_panel_width = sidebar_width;
                                            cx.notify();
                                        });
                                    }
                                }
                            })
                            .child(
                                resizable_panel()
                                    .visible(self.show_sidebar)
                                    .size(self.sidebar_panel_width)
                                    .size_range(px(260.)..px(520.))
                                    .child(sidebar),
                            )
                            .child(content),
                    ),
                )
                .child(self.render_bottom_bar(cx))
                .into_any_element()
        };

        div()
            .size_full()
            .child(
                v_flex()
                    .size_full()
                    .child(
                        gpui_component::TitleBar::new().child(
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
                                            self.render_gateways_popover(cx)
                                        } else {
                                            div().into_any_element()
                                        })
                                )
                                .child(
                                    h_flex()
                                        .h_full()
                                        .gap_2()
                                        .items_center()
                                        // .child(if !is_gateway_setup_required {
                                        //     Separator::vertical().mr_1().into_any_element()
                                        // } else {
                                        //     div().into_any_element()
                                        // })
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
                                                            t!("settings.option.keepawake.tooltip")
                                                                .to_string(),
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
                                                            let Some(settings) =
                                                                view.gateway.settings.as_ref()
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
                                                        .child(
                                                            Icon::new(theme_icon).size_3p5().opacity(0.6),
                                                        )
                                                        .on_click(cx.listener(|_, _, window, cx| {
                                                            let (mode, theme_preference) =
                                                                if cx.theme().mode.is_dark() {
                                                                    (
                                                                        ThemeMode::Light,
                                                                        WindowThemePreference::Light,
                                                                    )
                                                                } else {
                                                                    (
                                                                        ThemeMode::Dark,
                                                                        WindowThemePreference::Dark,
                                                                    )
                                                                };
                                                            Theme::change(mode, Some(window), cx);
                                                            window::persist_theme_preference(
                                                                window,
                                                                theme_preference,
                                                                cx,
                                                            );
                                                        })),
                                                ),
                                        )
                                ),
                        ),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_h_0()
                            .w_full()
                            .overflow_hidden()
                            .child(body),
                    ),
            )
            .children(sheet_layer)
            .children(dialog_layer)
            .children(notification_layer)
    }
}
