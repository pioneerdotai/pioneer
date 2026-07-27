use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::Message;

use crate::auth::AuthenticatedPrincipal;
use crate::request_context::ConnectionContext;

#[cfg(test)]
pub(crate) mod test_support;

pub type ConnectionId = u64;

#[derive(Clone)]
struct SessionConnection {
    sender: mpsc::Sender<Message>,
    workspace_id: Option<String>,
    principal: Arc<AuthenticatedPrincipal>,
}

#[derive(Default)]
pub struct SessionManager {
    next_connection_id: AtomicU64,
    connections: RwLock<HashMap<ConnectionId, SessionConnection>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_connection(
        &self,
        sender: mpsc::Sender<Message>,
        principal: Arc<AuthenticatedPrincipal>,
    ) -> ConnectionId {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.connections.write().await.insert(
            connection_id,
            SessionConnection {
                sender,
                workspace_id: None,
                principal,
            },
        );
        connection_id
    }

    pub async fn unregister_connection(&self, connection_id: ConnectionId) {
        self.connections.write().await.remove(&connection_id);
    }

    pub async fn connection_ids(&self) -> Vec<ConnectionId> {
        self.connections.read().await.keys().copied().collect()
    }

    pub async fn connection_ids_for_workspace(&self, workspace_id: &str) -> Vec<ConnectionId> {
        let trimmed = workspace_id.trim();
        if trimmed.is_empty() {
            return self.connection_ids().await;
        }

        self.connections
            .read()
            .await
            .iter()
            .filter_map(|(connection_id, connection)| {
                if connection.workspace_id.as_deref() == Some(trimmed) {
                    Some(*connection_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    pub async fn connection_workspace_id(&self, connection_id: ConnectionId) -> Option<String> {
        self.connections
            .read()
            .await
            .get(&connection_id)
            .and_then(|connection| connection.workspace_id.clone())
    }

    pub(crate) async fn connection_principal(
        &self,
        connection_id: ConnectionId,
    ) -> Result<Arc<AuthenticatedPrincipal>> {
        self.connections
            .read()
            .await
            .get(&connection_id)
            .map(|connection| connection.principal.clone())
            .ok_or_else(|| anyhow!("connection `{connection_id}` is not registered"))
    }

    pub(crate) async fn connection_context(
        &self,
        connection_id: ConnectionId,
    ) -> Result<ConnectionContext> {
        self.connection_principal(connection_id)
            .await
            .map(|principal| ConnectionContext::new(connection_id, principal))
    }

    pub async fn set_connection_workspace(
        &self,
        connection_id: ConnectionId,
        workspace_id: Option<String>,
    ) {
        let mut guard = self.connections.write().await;
        let Some(connection) = guard.get_mut(&connection_id) else {
            return;
        };

        connection.workspace_id = workspace_id.and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_owned())
        });
    }

    pub async fn send_text(&self, connection_id: ConnectionId, payload: String) -> Result<()> {
        let sender = {
            self.connections
                .read()
                .await
                .get(&connection_id)
                .map(|connection| connection.sender.clone())
                .ok_or_else(|| anyhow!("connection `{connection_id}` is not registered"))?
        };

        if sender.send(Message::Text(payload.into())).await.is_err() {
            self.unregister_connection(connection_id).await;
            return Err(anyhow!("connection `{connection_id}` channel is closed"));
        }

        Ok(())
    }

    pub async fn send_binary(&self, connection_id: ConnectionId, payload: Vec<u8>) -> Result<()> {
        let sender = {
            self.connections
                .read()
                .await
                .get(&connection_id)
                .map(|connection| connection.sender.clone())
                .ok_or_else(|| anyhow!("connection `{connection_id}` is not registered"))?
        };

        if sender.send(Message::Binary(payload.into())).await.is_err() {
            self.unregister_connection(connection_id).await;
            return Err(anyhow!("connection `{connection_id}` channel is closed"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SessionManager;
    use super::test_support::{
        TEST_SUPERUSER_PRINCIPAL_ID, authenticated_test_superuser,
        register_authenticated_test_connection,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    #[tokio::test]
    async fn send_text_reaches_registered_connection() {
        let manager = SessionManager::new();
        let (tx, mut rx) = mpsc::channel(4);

        let connection_id = register_authenticated_test_connection(&manager, tx).await;

        manager
            .send_text(connection_id, "ping".to_owned())
            .await
            .expect("send_text should succeed");

        assert_eq!(rx.recv().await, Some(Message::Text("ping".into())));
    }

    #[tokio::test]
    async fn send_binary_reaches_registered_connection() {
        let manager = SessionManager::new();
        let (tx, mut rx) = mpsc::channel(4);

        let connection_id = register_authenticated_test_connection(&manager, tx).await;

        manager
            .send_binary(connection_id, b"payload".to_vec())
            .await
            .expect("send_binary should succeed");

        assert_eq!(
            rx.recv().await,
            Some(Message::Binary(b"payload".to_vec().into()))
        );
    }

    #[tokio::test]
    async fn connection_ids_for_workspace_prefers_scoped_connections() {
        let manager = SessionManager::new();
        let (tx_a, _rx_a) = mpsc::channel(2);
        let (tx_b, _rx_b) = mpsc::channel(2);

        let connection_a = register_authenticated_test_connection(&manager, tx_a).await;
        let connection_b = register_authenticated_test_connection(&manager, tx_b).await;

        manager
            .set_connection_workspace(connection_a, Some("ws_a".to_owned()))
            .await;
        manager
            .set_connection_workspace(connection_b, Some("ws_b".to_owned()))
            .await;

        let ids = manager.connection_ids_for_workspace("ws_a").await;
        assert_eq!(ids, vec![connection_a]);
    }

    #[tokio::test]
    async fn connection_ids_for_workspace_returns_only_scoped_connections() {
        let manager = SessionManager::new();
        let (tx_a, _rx_a) = mpsc::channel(2);
        let (tx_b, _rx_b) = mpsc::channel(2);

        let connection_a = register_authenticated_test_connection(&manager, tx_a).await;
        let _connection_b = register_authenticated_test_connection(&manager, tx_b).await;

        manager
            .set_connection_workspace(connection_a, Some("ws_a".to_owned()))
            .await;

        let ids = manager.connection_ids_for_workspace("ws_unknown").await;
        assert!(
            ids.is_empty(),
            "unknown workspace should not fan out to unrelated connections"
        );
    }

    #[tokio::test]
    async fn connection_workspace_id_returns_current_workspace() {
        let manager = SessionManager::new();
        let (tx, _rx) = mpsc::channel(2);
        let connection_id = register_authenticated_test_connection(&manager, tx).await;

        assert_eq!(manager.connection_workspace_id(connection_id).await, None);

        manager
            .set_connection_workspace(connection_id, Some("ws_a".to_owned()))
            .await;

        assert_eq!(
            manager
                .connection_workspace_id(connection_id)
                .await
                .as_deref(),
            Some("ws_a")
        );
    }

    #[tokio::test]
    async fn registration_stores_the_exact_authenticated_principal() {
        let manager = SessionManager::new();
        let (tx, _rx) = mpsc::channel(2);
        let expected = authenticated_test_superuser();
        let connection_id = register_authenticated_test_connection(&manager, tx).await;
        let stored = manager.connection_principal(connection_id).await.unwrap();

        assert_eq!(stored.as_ref(), expected.as_ref());
        assert_eq!(stored.principal_id.as_str(), TEST_SUPERUSER_PRINCIPAL_ID);
    }

    #[tokio::test]
    async fn connection_context_binds_registered_id_and_exact_principal() {
        let manager = SessionManager::new();
        let (tx, _rx) = mpsc::channel(2);
        let connection_id = register_authenticated_test_connection(&manager, tx).await;
        let stored = manager.connection_principal(connection_id).await.unwrap();
        let context = manager.connection_context(connection_id).await.unwrap();

        assert_eq!(context.connection_id(), connection_id);
        assert!(Arc::ptr_eq(context.principal_arc(), &stored));
        assert_eq!(
            context.principal().principal_id.as_str(),
            TEST_SUPERUSER_PRINCIPAL_ID
        );
    }

    #[tokio::test]
    async fn workspace_mutation_preserves_the_immutable_principal() {
        let manager = SessionManager::new();
        let (tx, _rx) = mpsc::channel(2);
        let connection_id = register_authenticated_test_connection(&manager, tx).await;
        let before = manager.connection_principal(connection_id).await.unwrap();

        manager
            .set_connection_workspace(connection_id, Some("ws_a".to_owned()))
            .await;
        let after = manager.connection_principal(connection_id).await.unwrap();

        assert!(Arc::ptr_eq(&before, &after));
        assert_eq!(after.principal_id.as_str(), TEST_SUPERUSER_PRINCIPAL_ID);
    }

    #[tokio::test]
    async fn unregister_removes_principal_lookup() {
        let manager = SessionManager::new();
        let (tx, _rx) = mpsc::channel(2);
        let connection_id = register_authenticated_test_connection(&manager, tx).await;

        manager.unregister_connection(connection_id).await;

        assert!(manager.connection_principal(connection_id).await.is_err());
    }

    #[tokio::test]
    async fn multiple_connections_share_one_stable_principal_identity() {
        let manager = SessionManager::new();
        let (tx_a, _rx_a) = mpsc::channel(2);
        let (tx_b, _rx_b) = mpsc::channel(2);
        let connection_a = register_authenticated_test_connection(&manager, tx_a).await;
        let connection_b = register_authenticated_test_connection(&manager, tx_b).await;

        assert_ne!(connection_a, connection_b);
        let principal_a = manager.connection_principal(connection_a).await.unwrap();
        let principal_b = manager.connection_principal(connection_b).await.unwrap();
        assert_eq!(principal_a, principal_b);
        assert_eq!(
            principal_a.principal_id.as_str(),
            TEST_SUPERUSER_PRINCIPAL_ID
        );
    }

    #[tokio::test]
    async fn shared_principal_connections_keep_distinct_message_routing() {
        let manager = SessionManager::new();
        let (tx_a, mut rx_a) = mpsc::channel(2);
        let (tx_b, mut rx_b) = mpsc::channel(2);
        let connection_a = register_authenticated_test_connection(&manager, tx_a).await;
        let connection_b = register_authenticated_test_connection(&manager, tx_b).await;

        assert_eq!(
            manager.connection_principal(connection_a).await.unwrap(),
            manager.connection_principal(connection_b).await.unwrap()
        );
        manager
            .send_text(connection_a, "only-a".to_owned())
            .await
            .unwrap();

        assert_eq!(
            rx_a.recv().await,
            Some(Message::Text("only-a".to_owned().into()))
        );
        assert!(rx_b.try_recv().is_err());
    }
}
