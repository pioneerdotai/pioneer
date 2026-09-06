use super::*;

const TASK_USER_NOTIFICATION_PAGE_LIMIT: u32 = 100;

impl PioneerDesktop {
    pub(in crate::app::flow) fn request_task_user_notification_refresh(&mut self) {
        self.task_user_notifications_refresh_requested = true;
    }

    pub(in crate::app) fn refresh_task_user_notifications(&mut self, cx: &mut Context<Self>) {
        if self.task_user_notifications_workspace_id.as_deref() != self.active_workspace_id() {
            self.clear_task_user_notification_inbox();
        }
        self.request_task_user_notification_refresh();
        self.drive_task_user_notification_refresh(cx);
    }

    pub(in crate::app::flow) fn drive_task_user_notification_refresh(
        &mut self,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.task_user_notifications_refresh_requested
            || self.task_user_notifications_loading
            || self.gateway.connection_state != GatewayConnectionState::Connected
        {
            return false;
        }

        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            self.clear_task_user_notification_inbox();
            return false;
        };
        if !self
            .principal_presentation_capabilities()
            .can_read_own_notifications
        {
            self.clear_task_user_notification_inbox();
            return false;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return false;
        };

        self.task_user_notifications_refresh_requested = false;
        self.task_user_notifications_loading = true;
        self.task_user_notifications_error = None;
        self.task_user_notifications_refresh_generation = self
            .task_user_notifications_refresh_generation
            .wrapping_add(1);
        let generation = self.task_user_notifications_refresh_generation;
        let sender = self.gateway.client_runtime.ws_command_sender().clone();
        let request_workspace_id = workspace_id.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.task_user_notification_list(
                            pioneer_protocol::TaskUserNotificationListParams {
                                workspace_id: request_workspace_id,
                                cursor: None,
                                limit: Some(TASK_USER_NOTIFICATION_PAGE_LIMIT),
                            },
                        )
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if generation != view.task_user_notifications_refresh_generation
                        || view.gateway.ws_connection_id != Some(connection_id)
                        || view.active_workspace_id() != Some(workspace_id.as_str())
                    {
                        return;
                    }
                    view.task_user_notifications_loading = false;
                    match result {
                        Ok(response) => {
                            view.task_user_notifications_workspace_id = Some(workspace_id);
                            view.task_user_notifications = response.notifications;
                            view.task_user_notifications_next_cursor = response.next_cursor;
                            view.task_user_notifications_error = None;
                        }
                        Err(error) => {
                            // Keep the last verified inbox during a transient refresh failure.
                            view.task_user_notifications_error = Some(format!("{error:#}"));
                        }
                    }
                    if view.task_user_notifications_refresh_requested {
                        view.drive_task_user_notification_refresh(cx);
                    }
                    cx.notify();
                });
            }
        })
        .detach();
        true
    }

    pub(in crate::app) fn acknowledge_task_user_notification(
        &mut self,
        notification_id: String,
        cx: &mut Context<Self>,
    ) {
        if notification_id.trim().is_empty()
            || !self
                .principal_presentation_capabilities()
                .can_acknowledge_own_notifications
        {
            return;
        }
        let Some(workspace_id) = self.active_workspace_id().map(str::to_owned) else {
            return;
        };
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let sender = self.gateway.client_runtime.ws_command_sender().clone();
        let request_notification_id = notification_id.clone();
        let request_workspace_id = workspace_id.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        sender.task_user_notification_acknowledge(
                            pioneer_protocol::TaskUserNotificationAcknowledgeParams {
                                workspace_id: request_workspace_id,
                                notification_id: request_notification_id,
                            },
                        )
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id)
                        || view.active_workspace_id() != Some(workspace_id.as_str())
                    {
                        return;
                    }
                    match result {
                        Ok(response) => {
                            if let Some(existing) = view
                                .task_user_notifications
                                .iter_mut()
                                .find(|item| item.notification_id == notification_id)
                            {
                                *existing = response.notification;
                            }
                        }
                        Err(error) => {
                            view.task_user_notifications_error = Some(format!("{error:#}"));
                        }
                    }
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(in crate::app::flow) fn clear_task_user_notification_inbox(&mut self) {
        self.task_user_notifications_refresh_generation = self
            .task_user_notifications_refresh_generation
            .wrapping_add(1);
        self.task_user_notifications_workspace_id = None;
        self.task_user_notifications.clear();
        self.task_user_notifications_next_cursor = None;
        self.task_user_notifications_loading = false;
        self.task_user_notifications_refresh_requested = false;
        self.task_user_notifications_error = None;
    }
}
