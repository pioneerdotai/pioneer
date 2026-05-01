use super::*;

impl PioneerDesktop {
    pub(crate) fn push_install_warnings_notification(
        &mut self,
        warnings: &[GatewayInstallWarning],
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let messages = warning_notification_messages(warnings);
        if messages.is_empty() {
            return;
        }

        let operation_epoch = self.gateway.connection_epoch;

        for (index, message) in messages.into_iter().enumerate() {
            let notification_id = operation_epoch.saturating_mul(1_000) + index as u64;

            window.push_notification(
                Notification::new()
                    .with_type(NotificationType::Warning)
                    .id1::<GatewayInstallWarningNotification>((
                        "gateway-install-warning",
                        notification_id,
                    ))
                    .content(move |_, _, _| {
                        v_flex()
                            .child(
                                div()
                                    .text_sm()
                                    .opacity(0.8)
                                    .line_height(relative(1.4))
                                    .child(message.clone()),
                            )
                            .into_any_element()
                    }),
                cx,
            );
        }
    }
}
