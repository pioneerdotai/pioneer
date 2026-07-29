use anyhow::{Result, anyhow};
use pioneer_protocol::{
    AuthSessionId, AuthSessionRevokedNotification, AuthSessionTerminationReason,
    JsonRpcNotification, constants::events,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

use crate::auth::AuthenticatedSessionPrincipal;
use crate::request_context::ConnectionContext;

#[cfg(test)]
pub(crate) mod test_support;

pub type ConnectionId = u64;

// Keep a terminal marker for the full maximum access-token lifetime. A
// handshake can authenticate immediately before revocation and be descheduled
// before registration; pruning sooner than the token validity bound could let
// that stale handshake register after the disconnect sweep. Session lease
// validation remains the durable authority after this in-memory race window.
const TERMINATED_SESSION_TOMBSTONE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
struct SessionConnection {
    sender: mpsc::Sender<Message>,
    termination_tx: watch::Sender<Option<AuthSessionTerminationReason>>,
    workspace_id: Option<String>,
    principal: Arc<AuthenticatedSessionPrincipal>,
}

#[derive(Clone, Copy)]
struct TerminatedSessionTombstone {
    reason: AuthSessionTerminationReason,
    recorded_at: Instant,
}

#[derive(Default)]
pub struct SessionManager {
    next_connection_id: AtomicU64,
    connections: RwLock<HashMap<ConnectionId, SessionConnection>>,
    session_connections: RwLock<HashMap<String, HashSet<ConnectionId>>>,
    device_sessions: RwLock<HashMap<String, HashSet<String>>>,
    terminated_sessions: RwLock<HashMap<String, TerminatedSessionTombstone>>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_connection(
        &self,
        sender: mpsc::Sender<Message>,
        principal: Arc<AuthenticatedSessionPrincipal>,
    ) -> Result<ConnectionId> {
        // Serialize registration against post-commit revocation. Without this
        // gate a handshake that authenticated immediately before a revoke
        // could register after the disconnect sweep and leave a stale socket
        // open until its next request.
        let mut terminated_sessions = self.terminated_sessions.write().await;
        prune_terminated_session_tombstones(&mut terminated_sessions);
        if let Some(tombstone) = terminated_sessions.get(principal.session_id.as_str()) {
            return Err(anyhow!(
                "auth session `{}` is terminal ({})",
                principal.session_id,
                tombstone.reason.as_str()
            ));
        }
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed) + 1;
        let session_id = principal.session_id.to_string();
        let device_id = principal.device_id.to_string();
        let (termination_tx, _) = watch::channel(None);
        self.connections.write().await.insert(
            connection_id,
            SessionConnection {
                sender,
                termination_tx,
                workspace_id: None,
                principal,
            },
        );
        self.session_connections
            .write()
            .await
            .entry(session_id.clone())
            .or_default()
            .insert(connection_id);
        self.device_sessions
            .write()
            .await
            .entry(device_id)
            .or_default()
            .insert(session_id);
        drop(terminated_sessions);
        Ok(connection_id)
    }

    pub async fn unregister_connection(&self, connection_id: ConnectionId) {
        let removed = self.connections.write().await.remove(&connection_id);
        let Some(removed) = removed else {
            return;
        };
        let session_id = removed.principal.session_id.to_string();
        let device_id = removed.principal.device_id.to_string();
        let mut session_connections = self.session_connections.write().await;
        if let Some(ids) = session_connections.get_mut(&session_id) {
            ids.remove(&connection_id);
            if ids.is_empty() {
                session_connections.remove(&session_id);
                let mut device_sessions = self.device_sessions.write().await;
                if let Some(sessions) = device_sessions.get_mut(&device_id) {
                    sessions.remove(&session_id);
                    if sessions.is_empty() {
                        device_sessions.remove(&device_id);
                    }
                }
            }
        }
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
    ) -> Result<Arc<AuthenticatedSessionPrincipal>> {
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

    pub(crate) async fn connection_termination_receiver(
        &self,
        connection_id: ConnectionId,
    ) -> Result<watch::Receiver<Option<AuthSessionTerminationReason>>> {
        self.connections
            .read()
            .await
            .get(&connection_id)
            .map(|connection| connection.termination_tx.subscribe())
            .ok_or_else(|| anyhow!("connection `{connection_id}` is not registered"))
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

    pub async fn connection_ids_for_session(
        &self,
        session_id: &AuthSessionId,
    ) -> Vec<ConnectionId> {
        let mut ids = self
            .session_connections
            .read()
            .await
            .get(session_id.as_str())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    #[cfg(test)]
    async fn session_ids_for_device(&self, device_id: &pioneer_protocol::DeviceId) -> Vec<String> {
        let mut ids = self
            .device_sessions
            .read()
            .await
            .get(device_id.as_str())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub async fn disconnect_session(
        &self,
        session_id: &AuthSessionId,
        reason: AuthSessionTerminationReason,
    ) {
        let mut terminated_sessions = self.terminated_sessions.write().await;
        prune_terminated_session_tombstones(&mut terminated_sessions);
        terminated_sessions.insert(
            session_id.to_string(),
            TerminatedSessionTombstone {
                reason,
                recorded_at: Instant::now(),
            },
        );
        let ids = self.connection_ids_for_session(session_id).await;
        drop(terminated_sessions);
        for connection_id in ids {
            let notification = JsonRpcNotification::from_params(
                events::AUTH_SESSION_REVOKED,
                &AuthSessionRevokedNotification {
                    session_id: session_id.clone(),
                    reason,
                },
            )
            .and_then(|notification| serde_json::to_string(&notification));
            let connection = self
                .connections
                .read()
                .await
                .get(&connection_id)
                .map(|connection| (connection.sender.clone(), connection.termination_tx.clone()));
            if let Some((sender, termination_tx)) = connection {
                if let Ok(notification) = notification {
                    let _ = sender.try_send(Message::Text(notification.into()));
                }
                let _ = sender.try_send(Message::Close(Some(
                        tokio_tungstenite::tungstenite::protocol::CloseFrame {
                            code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(4403),
                            reason: reason.as_str().into(),
                        },
                    )));
                let _ = termination_tx.send(Some(reason));
            }
            self.unregister_connection(connection_id).await;
        }
    }
}

fn prune_terminated_session_tombstones(
    tombstones: &mut HashMap<String, TerminatedSessionTombstone>,
) {
    tombstones
        .retain(|_, tombstone| tombstone.recorded_at.elapsed() < TERMINATED_SESSION_TOMBSTONE_TTL);
}

#[async_trait::async_trait]
impl crate::auth::AuthSessionDisconnectHook for SessionManager {
    async fn disconnect_session(
        &self,
        session_id: &AuthSessionId,
        reason: AuthSessionTerminationReason,
    ) {
        SessionManager::disconnect_session(self, session_id, reason).await;
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{
        TEST_SUPERUSER_PRINCIPAL_ID, authenticated_test_superuser,
        register_authenticated_test_connection,
    };
    use super::{SessionManager, TERMINATED_SESSION_TOMBSTONE_TTL};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    fn peer_session_principal() -> Arc<crate::auth::AuthenticatedSessionPrincipal> {
        let mut principal = (*authenticated_test_superuser()).clone();
        principal.device_id = pioneer_protocol::DeviceId::new("D00000000000000000002").unwrap();
        principal.session_id =
            pioneer_protocol::AuthSessionId::new("S00000000000000000002").unwrap();
        principal.access_jti = "J00000000000000000002".to_owned();
        Arc::new(principal)
    }

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

    #[tokio::test]
    async fn disconnect_session_closes_every_matching_connection_only() {
        let manager = SessionManager::new();
        let (tx_a1, mut rx_a1) = mpsc::channel(4);
        let (tx_a2, mut rx_a2) = mpsc::channel(4);
        let (tx_b, mut rx_b) = mpsc::channel(4);
        let session_a = authenticated_test_superuser();
        let session_b = peer_session_principal();
        let a1 = manager
            .register_connection(tx_a1, session_a.clone())
            .await
            .unwrap();
        let a2 = manager
            .register_connection(tx_a2, session_a.clone())
            .await
            .unwrap();
        let b = manager
            .register_connection(tx_b, session_b.clone())
            .await
            .unwrap();

        manager
            .disconnect_session(
                &session_a.session_id,
                pioneer_protocol::AuthSessionTerminationReason::SessionRevoked,
            )
            .await;

        for receiver in [&mut rx_a1, &mut rx_a2] {
            let event = receiver.recv().await.expect("revocation event");
            assert!(event.into_text().unwrap().contains("auth/session_revoked"));
            assert!(matches!(receiver.recv().await, Some(Message::Close(_))));
        }
        assert!(manager.connection_principal(a1).await.is_err());
        assert!(manager.connection_principal(a2).await.is_err());
        assert!(manager.connection_principal(b).await.is_ok());
        assert!(rx_b.try_recv().is_err());
        assert_eq!(
            manager
                .connection_ids_for_session(&session_b.session_id)
                .await,
            vec![b]
        );
    }

    #[tokio::test]
    async fn committed_revoke_response_precedes_notification_and_close() {
        let manager = SessionManager::new();
        let (tx, mut rx) = mpsc::channel(4);
        let principal = authenticated_test_superuser();
        let connection_id = manager
            .register_connection(tx, principal.clone())
            .await
            .unwrap();

        manager
            .send_text(connection_id, "revoke-response".to_owned())
            .await
            .unwrap();
        manager
            .disconnect_session(
                &principal.session_id,
                pioneer_protocol::AuthSessionTerminationReason::SessionRevoked,
            )
            .await;

        assert_eq!(
            rx.recv().await,
            Some(Message::Text("revoke-response".to_owned().into()))
        );
        let notification = rx.recv().await.expect("revocation notification");
        assert!(
            notification
                .into_text()
                .unwrap()
                .contains("auth/session_revoked")
        );
        assert!(matches!(rx.recv().await, Some(Message::Close(_))));
    }

    #[tokio::test]
    async fn runtime_indexes_are_derived_from_live_registration_after_restart() {
        let before_restart = SessionManager::new();
        let (tx, _rx) = mpsc::channel(2);
        let principal = peer_session_principal();
        before_restart
            .register_connection(tx, principal.clone())
            .await
            .unwrap();
        assert_eq!(
            before_restart
                .connection_ids_for_session(&principal.session_id)
                .await
                .len(),
            1
        );

        let after_restart = SessionManager::new();
        assert!(
            after_restart
                .connection_ids_for_session(&principal.session_id)
                .await
                .is_empty()
        );
        assert!(
            after_restart
                .session_ids_for_device(&principal.device_id)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn post_commit_disconnect_rejects_late_registration() {
        let manager = SessionManager::new();
        let principal = authenticated_test_superuser();
        manager
            .disconnect_session(
                &principal.session_id,
                pioneer_protocol::AuthSessionTerminationReason::SessionRevoked,
            )
            .await;

        let (sender, _receiver) = mpsc::channel(1);
        let error = manager
            .register_connection(sender, principal.clone())
            .await
            .expect_err("terminal session must reject a late connection");
        assert!(error.to_string().contains("session_revoked"));
        assert!(
            manager
                .connection_ids_for_session(&principal.session_id)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn terminal_registration_tombstones_are_short_lived_and_bounded() {
        let manager = SessionManager::new();
        let principal = authenticated_test_superuser();
        manager
            .disconnect_session(
                &principal.session_id,
                pioneer_protocol::AuthSessionTerminationReason::SessionRevoked,
            )
            .await;
        {
            let mut tombstones = manager.terminated_sessions.write().await;
            let tombstone = tombstones
                .get_mut(principal.session_id.as_str())
                .expect("terminal tombstone");
            tombstone.recorded_at = std::time::Instant::now() - TERMINATED_SESSION_TOMBSTONE_TTL;
        }

        let (sender, _receiver) = mpsc::channel(1);
        manager
            .register_connection(sender, principal)
            .await
            .expect("persisted admission owns rejection after the race window");
        assert!(manager.terminated_sessions.read().await.is_empty());
    }

    #[tokio::test]
    async fn disconnect_signal_is_delivered_even_when_outbound_queue_is_full() {
        let manager = SessionManager::new();
        let principal = authenticated_test_superuser();
        let (sender, _receiver) = mpsc::channel(1);
        let connection_id = manager
            .register_connection(sender, principal.clone())
            .await
            .unwrap();
        let mut termination = manager
            .connection_termination_receiver(connection_id)
            .await
            .unwrap();
        manager
            .send_text(connection_id, "occupy-queue".to_owned())
            .await
            .unwrap();

        manager
            .disconnect_session(
                &principal.session_id,
                pioneer_protocol::AuthSessionTerminationReason::SessionRevoked,
            )
            .await;

        termination.changed().await.unwrap();
        assert_eq!(
            *termination.borrow(),
            Some(pioneer_protocol::AuthSessionTerminationReason::SessionRevoked)
        );
        assert!(manager.connection_principal(connection_id).await.is_err());
    }
}
