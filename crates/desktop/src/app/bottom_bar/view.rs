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
        let can_manage_capabilities = self
            .principal_presentation_capabilities()
            .can_manage_capabilities;
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
        let show_task_notifications = self
            .principal_presentation_capabilities()
            .can_read_own_notifications;

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
                    .when(can_manage_capabilities, |this| {
                        this.child(
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
                    })
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
                                        SettingsContentView::Account,
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
                    .child(if show_task_notifications {
                        self.render_task_user_notifications_button(cx)
                    } else {
                        div().into_any_element()
                    })
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

    fn render_task_user_notifications_button(&self, cx: &mut Context<Self>) -> AnyElement {
        let unread = self
            .task_user_notifications
            .iter()
            .filter(|item| item.acknowledged_at.is_none())
            .count();
        let notifications = self
            .task_user_notifications
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>();
        let loading = self.task_user_notifications_loading;
        let error = self.task_user_notifications_error.clone();
        let desktop = cx.entity().clone();

        Popover::new("task-user-notifications-popover")
            .anchor(Anchor::BottomRight)
            .trigger(
                Button::new("task-user-notifications-trigger")
                    .ghost()
                    .small()
                    .compact()
                    .tooltip(t!("tasks.notifications.title").to_string())
                    .child(
                        h_flex()
                            .gap_1()
                            .child(Icon::new(IconName::Bell).size_3p5().opacity(if unread > 0 {
                                1.0
                            } else {
                                0.6
                            }))
                            .when(unread > 0, |this| {
                                this.child(
                                    div()
                                        .text_xs()
                                        .text_color(cx.theme().blue)
                                        .child(unread.to_string()),
                                )
                            }),
                    ),
            )
            .content(move |_, _, cx| {
                let list = notifications.clone();
                let error = error.clone();
                let desktop = desktop.clone();
                v_flex()
                    .w(px(380.))
                    .max_h(px(460.))
                    .gap_2()
                    .p_2()
                    .child(
                        div()
                            .text_sm()
                            .font_semibold()
                            .child(t!("tasks.notifications.title").to_string()),
                    )
                    .when(loading, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .opacity(0.65)
                                .child(t!("tasks.notifications.loading").to_string()),
                        )
                    })
                    .when_some(error, |this, error| {
                        this.child(div().text_xs().text_color(cx.theme().danger).child(error))
                    })
                    .when(list.is_empty() && !loading, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .opacity(0.65)
                                .child(t!("tasks.notifications.empty").to_string()),
                        )
                    })
                    .children(list.into_iter().map(|notification| {
                        let notification_id = notification.notification_id.clone();
                        let desktop = desktop.clone();
                        let acknowledged = notification.acknowledged_at.is_some();
                        let summary = task_user_notification_summary(&notification);
                        v_flex()
                            .w_full()
                            .gap_1()
                            .p_2()
                            .rounded_md()
                            .border_1()
                            .border_color(cx.theme().border)
                            .child(div().text_xs().font_semibold().child(format!(
                                "{} · {}",
                                t!("tasks.notifications.task").to_string(),
                                notification.task_id
                            )))
                            .child(
                                div()
                                    .text_xs()
                                    .line_height(relative(1.3))
                                    .whitespace_normal()
                                    .child(summary),
                            )
                            .when(!acknowledged, |this| {
                                this.child(
                                    Button::new(format!("ack-task-notification-{notification_id}"))
                                        .ghost()
                                        .xsmall()
                                        .label(t!("tasks.notifications.mark_read").to_string())
                                        .on_click(move |_, _, cx| {
                                            let notification_id = notification_id.clone();
                                            let _ = desktop.update(cx, |view, cx| {
                                                view.acknowledge_task_user_notification(
                                                    notification_id,
                                                    cx,
                                                );
                                            });
                                        }),
                                )
                            })
                            .into_any_element()
                    }))
            })
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

fn task_user_notification_summary(notification: &pioneer_protocol::TaskUserNotification) -> String {
    if let Some(result) = notification.result.as_ref() {
        return result
            .summary
            .clone()
            .unwrap_or_else(|| t!("tasks.notifications.completed").to_string());
    }
    notification
        .error
        .as_ref()
        .map(|failure| failure.error.message.clone())
        .unwrap_or_else(|| t!("tasks.notifications.completed").to_string())
}
