use crate::{
    app::root::{MainContentView, PioneerDesktop, SettingsContentView},
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon, button::*, popover::Popover, separator::Separator, theme::ActiveTheme, *,
};

impl PioneerDesktop {
    pub(crate) fn render_bottom_bar(&self, cx: &mut Context<Self>) -> AnyElement {
        let is_threads_view_active = matches!(
            self.main_content_view,
            MainContentView::Threads | MainContentView::AgentsDoc
        );
        let show_thread_artifacts_button = self.main_content_view == MainContentView::Threads;
        let is_providers_view_active = self.main_content_view == MainContentView::Providers;
        let is_administration_view_active =
            self.main_content_view == MainContentView::Administration;
        let is_settings_view_active = self.main_content_view == MainContentView::Settings;
        let is_mcp_view_active = matches!(
            self.main_content_view,
            MainContentView::Mcp | MainContentView::McpDetails
        );
        let is_skills_view_active = matches!(
            self.main_content_view,
            MainContentView::Skills | MainContentView::SkillDetails
        );
        let show_status_button = self.should_show_active_thread_status();

        h_flex()
            .justify_between()
            .items_center()
            .px_2()
            .h_8()
            .border_t_1()
            .border_color(cx.theme().border)
            .child(
                h_flex()
                    .items_center()
                    .child(
                        Button::new("bottom-bar-open-threads")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::FolderTree)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_threads_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click(cx.listener(|view, _, _, cx| {
                                if matches!(
                                    view.main_content_view,
                                    MainContentView::Threads | MainContentView::AgentsDoc
                                ) {
                                    view.show_sidebar = !view.show_sidebar;
                                } else {
                                    view.set_main_content_view(MainContentView::Threads, cx);
                                }
                                cx.notify();
                            })),
                    )
                    .child(Separator::vertical().h_4().mx_1())
                    .child(
                        Button::new("bottom-bar-open-providers")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(IconName::Bot)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_providers_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click(cx.listener(|view, _, _, cx| {
                                if view.main_content_view == MainContentView::Providers {
                                    view.show_sidebar = !view.show_sidebar;
                                } else {
                                    view.open_providers_screen_from_bottom_bar(cx);
                                }
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("bottom-bar-open-mcp")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::Mcp)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_mcp_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click(cx.listener(|view, _, _, cx| {
                                if view.main_content_view == MainContentView::Mcp {
                                    view.show_sidebar = !view.show_sidebar;
                                } else {
                                    view.open_mcp_screen_from_bottom_bar(cx);
                                }
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("bottom-bar-open-skills")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::Zap)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_skills_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click(cx.listener(|view, _, _, cx| {
                                if view.main_content_view == MainContentView::Skills {
                                    view.show_sidebar = !view.show_sidebar;
                                } else {
                                    view.open_skills_screen_from_bottom_bar(cx);
                                }
                                cx.notify();
                            })),
                    )
                    .child(Separator::vertical().h_4().mx_1())
                    .child(
                        Button::new("bottom-bar-open-administration")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::Users)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_administration_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click(cx.listener(|view, _, _, cx| {
                                if view.main_content_view == MainContentView::Administration {
                                    view.show_sidebar = !view.show_sidebar;
                                } else {
                                    view.open_administration_screen_from_bottom_bar(cx);
                                }
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("bottom-bar-open-settings")
                            .ghost()
                            .small()
                            .compact()
                            .child(
                                Icon::new(PioneerIconName::Bolt)
                                    .size_3p5()
                                    .opacity(0.6)
                                    .when(is_settings_view_active, |this| {
                                        this.opacity(1.0).text_color(cx.theme().blue)
                                    }),
                            )
                            .on_click(cx.listener(|view, _, _, cx| {
                                if view.main_content_view == MainContentView::Settings {
                                    view.show_sidebar = !view.show_sidebar;
                                } else {
                                    view.open_settings_content_from_sidebar(
                                        SettingsContentView::General,
                                        cx,
                                    );
                                }
                                cx.notify();
                            })),
                    ),
            )
            .child(
                h_flex()
                    .items_center()
                    .child(if show_status_button {
                        self.render_active_thread_status_button()
                    } else {
                        div().into_any_element()
                    })
                    .child(if show_thread_artifacts_button {
                        self.render_thread_members_sidebar_toggle_button(cx)
                    } else {
                        div().into_any_element()
                    })
                    .child(if show_thread_artifacts_button {
                        self.render_thread_artifacts_sidebar_toggle_button(cx)
                    } else {
                        div().into_any_element()
                    }),
            )
            .into_any_element()
    }

    fn render_active_thread_status_button(&self) -> AnyElement {
        let status_text = self.active_thread_status_text();

        Popover::new("active-thread-status-popover")
            .anchor(Anchor::BottomRight)
            .trigger(
                Button::new("active-thread-status-trigger")
                    .ghost()
                    .small()
                    .compact()
                    .child(
                        Icon::new(PioneerIconName::MessageCircle)
                            .size_3p5()
                            .opacity(0.6),
                    ),
            )
            .content(move |_, _, _| {
                v_flex().w(px(320.)).gap_2().p_1().child(
                    div()
                        .text_xs()
                        .line_height(relative(1.15))
                        .whitespace_normal()
                        .child(status_text.clone()),
                )
            })
            .into_any_element()
    }

    fn render_thread_artifacts_sidebar_toggle_button(&self, cx: &mut Context<Self>) -> AnyElement {
        let artifacts_sidebar_icon = if self.show_thread_artifacts_sidebar {
            IconName::PanelRightClose
        } else {
            IconName::PanelRightOpen
        };

        Button::new("bottom-bar-toggle-thread-artifacts-sidebar")
            .ghost()
            .small()
            .compact()
            .tooltip(t!("artifacts.title").to_string())
            .child(
                Icon::new(artifacts_sidebar_icon)
                    .size_3p5()
                    .opacity(0.6)
                    .when(self.show_thread_artifacts_sidebar, |this| {
                        this.opacity(1.0).text_color(cx.theme().blue)
                    }),
            )
            .on_click(cx.listener(|view, _, _, cx| {
                view.show_thread_artifacts_sidebar = !view.show_thread_artifacts_sidebar;
                if view.show_thread_artifacts_sidebar {
                    view.show_thread_members_sidebar = false;
                }
                cx.notify();
            }))
            .into_any_element()
    }

    fn render_thread_members_sidebar_toggle_button(&self, cx: &mut Context<Self>) -> AnyElement {
        Button::new("bottom-bar-toggle-thread-members-sidebar")
            .ghost()
            .small()
            .compact()
            .tooltip(t!("settings.sidebar.members").to_string())
            .child(
                Icon::new(PioneerIconName::UserCheck)
                    .size_3p5()
                    .opacity(0.6)
                    .when(self.show_thread_members_sidebar, |this| {
                        this.opacity(1.0).text_color(cx.theme().blue)
                    }),
            )
            .on_click(cx.listener(|view, _, _, cx| {
                view.toggle_thread_members_sidebar(cx);
            }))
            .into_any_element()
    }
}
