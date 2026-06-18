use super::*;

impl MessageProcessor {
    pub(super) async fn send_notification_to_all_connections<T: Serialize>(
        &self,
        method: &str,
        payload: &T,
    ) {
        let connection_ids = self.session_manager.connection_ids().await;
        self.send_notification_to_connections(method, payload, connection_ids)
            .await;
    }

    pub(super) async fn send_notification_to_workspace_connections<T: Serialize>(
        &self,
        workspace_id: &str,
        method: &str,
        payload: &T,
    ) {
        let connection_ids = self
            .session_manager
            .connection_ids_for_workspace(workspace_id)
            .await;
        self.send_notification_to_connections(method, payload, connection_ids)
            .await;
    }

    pub(crate) async fn send_notification_to_thread_subscribers<T: Serialize>(
        &self,
        thread_id: &str,
        method: &str,
        payload: &T,
    ) {
        let connection_ids = self
            .thread_manager
            .subscribed_connection_ids(thread_id)
            .await;
        self.send_notification_to_connections(method, payload, connection_ids)
            .await;
    }

    pub(super) async fn send_notification_to_connections<T: Serialize>(
        &self,
        method: &str,
        payload: &T,
        connection_ids: Vec<ConnectionId>,
    ) {
        if connection_ids.is_empty() {
            return;
        }

        let notification = match JsonRpcNotification::from_params(method, payload) {
            Ok(notification) => notification,
            Err(error) => {
                error!(method, error = %error, "failed to encode notification");
                return;
            }
        };

        let serialized = match serde_json::to_string(&notification) {
            Ok(payload) => payload,
            Err(error) => {
                error!(method, error = %error, "failed to serialize notification");
                return;
            }
        };

        for target_connection_id in connection_ids {
            if let Err(error) = self
                .session_manager
                .send_text(target_connection_id, serialized.clone())
                .await
            {
                warn!(
                    connection_id = target_connection_id,
                    method,
                    error = %format!("{error:#}"),
                    "failed to send notification"
                );
            }
        }
    }

    pub(super) async fn send_error(
        &self,
        connection_id: ConnectionId,
        response: JsonRpcErrorResponse,
    ) {
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send JSON-RPC error response"
            );
        }
    }

    pub(super) async fn send_json<T: Serialize>(
        &self,
        connection_id: ConnectionId,
        value: &T,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(value)?;
        self.session_manager.send_text(connection_id, payload).await
    }
}
