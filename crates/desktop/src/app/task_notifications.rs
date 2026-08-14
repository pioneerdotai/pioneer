use crate::app::root::PioneerDesktop;
use gpui::{prelude::*, *};
use gpui_component::{
    Icon, IconName, Sizable, StyledExt, button::*, h_flex, popover::Popover, theme::ActiveTheme,
    v_flex,
};

impl PioneerDesktop {
    pub(in crate::app) fn render_task_user_notifications_button(
        &self,
        cx: &mut Context<Self>,
    ) -> AnyElement {
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
            .anchor(Anchor::TopRight)
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
                    .when(loading, |this| {
                        this.child(
                            div()
                                .text_xs()
                                .opacity(0.6)
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
                                .opacity(0.6)
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
