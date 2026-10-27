use anyhow::{Result, anyhow};
use axum::extract::ws::{CloseFrame, Message};
use pioneer_protocol::{
    AuthSessionId, AuthSessionRevokedNotification, AuthSessionTerminationReason,
    JsonRpcNotification, PrincipalId, constants::events,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc, watch};

use crate::auth::AuthenticatedSessionPrincipal;
use crate::request_context::ConnectionContext;

#[cfg(test)]
pub(crate) mod test_support;

use pioneer_observability::{
    queues::{QueueBytes, QueueKind, QueueObserver, QueueOutcome},
    work::{Boundary, ExecutorRole, WorkItem, WorkKind},
};

#[derive(Clone)]
enum OutboundChannel {
    Plain(mpsc::Sender<Message>),
    Observed(mpsc::Sender<WorkItem<Message>>),
}
/// Observer at the existing session-owned outbound lane, not a new transport.
#[derive(Clone)]
pub(crate) struct SessionOutbound {
    channel: OutboundChannel,
    observer: QueueObserver,
}
impl SessionOutbound {
    pub(crate) fn observed(sender: mpsc::Sender<WorkItem<Message>>) -> Self {
        let observer = QueueObserver::new(
            QueueKind::GatewayOutbound,
            Some(sender.max_capacity()),
            "WebSocket text/binary payload bytes; framing/envelope excluded",
        );
        Self {
            channel: OutboundChannel::Observed(sender),
            observer,
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        match &self.channel {
            OutboundChannel::Plain(tx) => tx.capacity(),
            OutboundChannel::Observed(tx) => tx.capacity(),
        }
    }
    pub(crate) async fn send(
        &self,
        message: Message,
    ) -> Result<(), mpsc::error::SendError<Message>> {
        let mut wait = self.observer.send_wait();
        let _live_wait = pioneer_observability::live::WaitGuard::new(
            pioneer_observability::live::WaitKind::ChannelCapacity,
        );
        let bytes = message_bytes(&message);
        match &self.channel {
            OutboundChannel::Observed(tx) => {
                let permit = match tx.reserve().await {
                    Ok(p) => p,
                    Err(_) => {
                        self.observer.outcome(QueueOutcome::Rejected, 1);
                        return Err(mpsc::error::SendError(message));
                    }
                };
                wait.complete();
                permit.send(
                    WorkItem::new(
                        message,
                        Boundary::EventLane,
                        WorkKind::Event,
                        ExecutorRole::Gateway,
                    )
                    .observed(self.observer.clone(), bytes),
                );
            }
            OutboundChannel::Plain(tx) => {
                let permit = match tx.reserve().await {
                    Ok(p) => p,
                    Err(_) => return Err(mpsc::error::SendError(message)),
                };
                wait.complete();
                self.observer.enqueued(bytes);
                permit.send(message);
            }
        }
        Ok(())
    }
    pub(crate) fn try_send(
        &self,
        message: Message,
    ) -> Result<(), mpsc::error::TrySendError<Message>> {
        let bytes = message_bytes(&message);
        match &self.channel {
            OutboundChannel::Observed(tx) => match tx.try_reserve() {
                Ok(p) => {
                    p.send(
                        WorkItem::new(
                            message,
                            Boundary::EventLane,
                            WorkKind::Event,
                            ExecutorRole::Gateway,
                        )
                        .observed(self.observer.clone(), bytes),
                    );
                    Ok(())
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.observer.outcome(QueueOutcome::Rejected, 1);
                    Err(mpsc::error::TrySendError::Full(message))
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.observer.outcome(QueueOutcome::Rejected, 1);
                    Err(mpsc::error::TrySendError::Closed(message))
                }
            },
            OutboundChannel::Plain(tx) => match tx.try_reserve() {
                Ok(p) => {
                    self.observer.enqueued(bytes);
                    p.send(message);
                    Ok(())
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    Err(mpsc::error::TrySendError::Full(message))
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    Err(mpsc::error::TrySendError::Closed(message))
                }
            },
        }
    }
}
impl From<mpsc::Sender<Message>> for SessionOutbound {
    fn from(sender: mpsc::Sender<Message>) -> Self {
        let observer = QueueObserver::new(
            QueueKind::GatewayOutbound,
            Some(sender.max_capacity()),
            "compatibility sender; payload bytes; receiver coverage unavailable",
        );
        Self {
            channel: OutboundChannel::Plain(sender),
            observer,
        }
    }
}
fn message_bytes(message: &Message) -> QueueBytes {
    QueueBytes::Known(match message {
        Message::Text(text) => text.len(),
        Message::Binary(bytes) | Message::Ping(bytes) | Message::Pong(bytes) => bytes.len(),
        Message::Close(frame) => frame.as_ref().map_or(0, |f| f.reason.len() + 2),
    } as u64)
}

pub type ConnectionId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionRenewalError {
    Terminated(AuthSessionTerminationReason),
    AccessExpired,
    Retired,
}

impl ConnectionRenewalError {
    pub(crate) fn auth_code(self) -> crate::auth::AuthErrorCode {
        use crate::auth::AuthErrorCode;
        match self {
            Self::Terminated(reason) => match reason {
                AuthSessionTerminationReason::SessionRevoked => AuthErrorCode::SessionRevoked,
                AuthSessionTerminationReason::SessionExpired => AuthErrorCode::SessionExpired,
                AuthSessionTerminationReason::SessionCompromised => {
                    AuthErrorCode::SessionCompromised
                }
                AuthSessionTerminationReason::PrincipalSuspended => {
                    AuthErrorCode::PrincipalSuspended
                }
                AuthSessionTerminationReason::PrincipalRemoved => AuthErrorCode::PrincipalRemoved,
            },
            Self::AccessExpired => AuthErrorCode::CredentialExpired,
            Self::Retired => AuthErrorCode::TemporarilyUnavailable,
        }
    }
}

// Keep a terminal marker for the full maximum access-token lifetime. A
// handshake can authenticate immediately before revocation and be descheduled
// before registration; pruning sooner than the token validity bound could let
// that stale handshake register after the disconnect sweep. Session lease
// validation remains the durable authority after this in-memory race window.
const TERMINATED_SESSION_TOMBSTONE_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
struct SessionConnection {
    sender: SessionOutbound,
    termination_tx: watch::Sender<Option<AuthSessionTerminationReason>>,
    scope_change_tx: watch::Sender<()>,
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

    #[cfg(test)]
    pub async fn register_connection(
        &self,
        sender: mpsc::Sender<Message>,
        principal: Arc<AuthenticatedSessionPrincipal>,
    ) -> Result<ConnectionId> {
        self.register_outbound_connection(sender.into(), principal)
            .await
    }
    pub(crate) async fn register_outbound_connection(
        &self,
        sender: SessionOutbound,
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
        let (scope_change_tx, _) = watch::channel(());
        self.connections.write().await.insert(
            connection_id,
            SessionConnection {
                sender,
                termination_tx,
                scope_change_tx,
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
            return Vec::new();
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

    /// Serialized with revocation and fenced against a late renewal completion.
    pub(crate) async fn renew_connection_principal(
        &self,
        connection_id: ConnectionId,
        expected: &AuthenticatedSessionPrincipal,
        next: Arc<AuthenticatedSessionPrincipal>,
    ) -> std::result::Result<ConnectionContext, ConnectionRenewalError> {
        let terminated = self.terminated_sessions.read().await;
        if let Some(tombstone) = terminated.get(expected.session_id.as_str()) {
            return Err(ConnectionRenewalError::Terminated(tombstone.reason));
        }
        let mut connections = self.connections.write().await;
        let connection = connections
            .get_mut(&connection_id)
            .ok_or(ConnectionRenewalError::Retired)?;
        if let Some(reason) = *connection.termination_tx.borrow() {
            return Err(ConnectionRenewalError::Terminated(reason));
        }
        if connection.principal.as_ref() != expected
            || next.gateway_id != expected.gateway_id
            || next.principal_id != expected.principal_id
            || next.device_id != expected.device_id
            || next.session_id != expected.session_id
        {
            return Err(ConnectionRenewalError::Retired);
        }
        let now =
            crate::helpers::unix_timestamp_secs().map_err(|_| ConnectionRenewalError::Retired)?;
        if now >= expected.access_expires_at_unix {
            return Err(ConnectionRenewalError::AccessExpired);
        }
        connection.scope_change_tx.send_replace(());
        connection.principal = next.clone();
        Ok(ConnectionContext::new(connection_id, next))
    }

    /// Returns every live auth-session identity currently associated with the
    /// exact durable principal. Lifecycle callers union this runtime view with
    /// the session IDs revoked by their committed transaction, closing stale
    /// sockets without introducing a second connection registry.
    pub(crate) async fn session_ids_for_principal(
        &self,
        principal_id: &PrincipalId,
    ) -> Vec<AuthSessionId> {
        let connections = self.connections.read().await;
        let mut session_ids = connections
            .values()
            .filter(|connection| &connection.principal.principal_id == principal_id)
            .map(|connection| connection.principal.session_id.clone())
            .collect::<Vec<_>>();
        session_ids.sort();
        session_ids.dedup();
        session_ids
    }

    /// Returns every live connection for one durable principal. Callers must
    /// still reauthorize the concrete resource immediately before delivery.
    pub(crate) async fn connection_ids_for_principal(
        &self,
        principal_id: &PrincipalId,
    ) -> Vec<ConnectionId> {
        let mut connection_ids = self
            .connections
            .read()
            .await
            .iter()
            .filter_map(|(connection_id, connection)| {
                (&connection.principal.principal_id == principal_id).then_some(*connection_id)
            })
            .collect::<Vec<_>>();
        connection_ids.sort_unstable();
        connection_ids
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

    pub(crate) async fn connection_scope_receiver(
        &self,
        id: ConnectionId,
    ) -> Result<watch::Receiver<()>> {
        self.connections
            .read()
            .await
            .get(&id)
            .map(|connection| connection.scope_change_tx.subscribe())
            .ok_or_else(|| anyhow!("connection is not registered"))
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

        let workspace_id = workspace_id.and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_owned())
        });
        if workspace_id != connection.workspace_id {
            connection.scope_change_tx.send_replace(());
            connection.workspace_id = workspace_id;
        }
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

        if sender.capacity() == 0 {
            record_outbound_queue_saturation("session_send_waited");
        }
        if sender.send(Message::Text(payload.into())).await.is_err() {
            self.unregister_connection(connection_id).await;
            return Err(anyhow!("connection `{connection_id}` channel is closed"));
        }

        Ok(())
    }

    /// Run a synchronous final scope/generation gate at the existing outbound
    /// lane. The callback cannot await or hold database capacity.
    pub(crate) async fn with_notification_outbound<T>(
        &self,
        connection_id: ConnectionId,
        expected: &AuthenticatedSessionPrincipal,
        workspace: &str,
        enqueue: impl FnOnce(&SessionOutbound) -> Result<T>,
    ) -> Result<Option<T>> {
        let connections = self.connections.read().await;
        let connection = connections
            .get(&connection_id)
            .ok_or_else(|| anyhow!("connection is not registered"))?;
        let now = crate::helpers::unix_timestamp_secs()?;
        if connection.principal.as_ref() != expected
            || connection.workspace_id.as_deref() != Some(workspace)
            || connection.termination_tx.borrow().is_some()
            || now >= expected.access_expires_at_unix
        {
            return Ok(None);
        }
        enqueue(&connection.sender).map(Some)
    }

    /// Best-effort delivery; a saturated socket is retired instead of queued.
    pub async fn try_send_notification_text(
        &self,
        connection_id: ConnectionId,
        payload: String,
    ) -> Result<()> {
        let sender = {
            self.connections
                .read()
                .await
                .get(&connection_id)
                .map(|connection| connection.sender.clone())
                .ok_or_else(|| anyhow!("connection `{connection_id}` is not registered"))?
        };

        match sender.try_send(Message::Text(payload.into())) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                record_outbound_queue_saturation("notification_connection_evicted");
                self.unregister_connection(connection_id).await;
                Err(anyhow!(
                    "connection `{connection_id}` outbound notification queue is full"
                ))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.unregister_connection(connection_id).await;
                Err(anyhow!("connection `{connection_id}` channel is closed"))
            }
        }
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
                if sender.capacity() == 0 {
                    record_outbound_queue_saturation("session_disconnect");
                }
                if let Ok(notification) = notification {
                    let _ = sender.try_send(Message::Text(notification.into()));
                }
                let _ = sender.try_send(Message::Close(Some(CloseFrame {
                    code: 4403,
                    reason: reason.as_str().into(),
                })));
                let _ = termination_tx.send(Some(reason));
            }
            self.unregister_connection(connection_id).await;
        }
    }
}

fn record_outbound_queue_saturation(reason_code: &'static str) {
    tracing::debug!(
        event = "websocket_outbound_queue",
        outcome = "saturated",
        reason_code,
    );
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
    use super::{SessionManager, SessionOutbound, TERMINATED_SESSION_TOMBSTONE_TTL};
    use anyhow::anyhow;
    use axum::extract::ws::Message;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn peer_session_principal() -> Arc<crate::auth::AuthenticatedSessionPrincipal> {
        let mut principal = (*authenticated_test_superuser()).clone();
        principal.device_id = pioneer_protocol::DeviceId::new("D00000000000000000002").unwrap();
        principal.session_id =
            pioneer_protocol::AuthSessionId::new("S00000000000000000002").unwrap();
        principal.access_jti = "J00000000000000000002".to_owned();
        Arc::new(principal)
    }

    #[tokio::test]
    async fn renewal_failure_preserves_expiry_retirement_cas_and_termination_codes() {
        use super::ConnectionRenewalError;
        use crate::auth::AuthErrorCode;
        use pioneer_protocol::AuthSessionTerminationReason;
        for (case, expected_error) in [
            ("expired", ConnectionRenewalError::AccessExpired),
            ("retired", ConnectionRenewalError::Retired),
            ("cas", ConnectionRenewalError::Retired),
            (
                "revoked",
                ConnectionRenewalError::Terminated(AuthSessionTerminationReason::SessionRevoked),
            ),
        ] {
            let manager = SessionManager::new();
            let (tx, _rx) = mpsc::channel(4);
            let mut old = (*authenticated_test_superuser()).clone();
            if case == "expired" {
                old.access_expires_at_unix = 0;
            }
            let id = manager
                .register_connection(tx, Arc::new(old.clone()))
                .await
                .unwrap();
            let mut next = (*authenticated_test_superuser()).clone();
            next.access_jti = "renewed".into();
            match case {
                "retired" => manager.unregister_connection(id).await,
                "cas" => old.access_jti = "stale".into(),
                "revoked" => {
                    manager
                        .disconnect_session(
                            &old.session_id,
                            AuthSessionTerminationReason::SessionRevoked,
                        )
                        .await
                }
                _ => {}
            }
            let error = manager
                .renew_connection_principal(id, &old, Arc::new(next))
                .await
                .err()
                .unwrap();
            assert_eq!(error, expected_error, "{case}");
            assert_eq!(
                error.auth_code(),
                match case {
                    "expired" => AuthErrorCode::CredentialExpired,
                    "revoked" => AuthErrorCode::SessionRevoked,
                    _ => AuthErrorCode::TemporarilyUnavailable,
                }
            );
            if let Ok(principal) = manager.connection_principal(id).await {
                assert_ne!(
                    principal.access_jti, "renewed",
                    "failed CAS must retain its principal"
                );
            }
        }
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
    async fn notification_send_never_waits_for_a_full_connection_queue() {
        let manager = SessionManager::new();
        let (sender, _receiver) = mpsc::channel(1);
        let connection_id = register_authenticated_test_connection(&manager, sender).await;

        manager
            .send_text(connection_id, "occupy-notification-queue".to_owned())
            .await
            .expect("first message should occupy the queue");
        let error = manager
            .try_send_notification_text(connection_id, "must-not-wait".to_owned())
            .await
            .expect_err("full notification queue should evict the lagging connection");

        assert!(error.to_string().contains("queue is full"));
        assert!(manager.connection_principal(connection_id).await.is_err());
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
    async fn empty_workspace_recipient_selector_never_means_all_connections() {
        let manager = SessionManager::new();
        let (tx_a, _rx_a) = mpsc::channel(2);
        let (tx_b, _rx_b) = mpsc::channel(2);
        let connection_a = register_authenticated_test_connection(&manager, tx_a).await;
        let connection_b = register_authenticated_test_connection(&manager, tx_b).await;
        manager
            .set_connection_workspace(connection_a, Some("ws_a".to_owned()))
            .await;
        assert!(
            manager.connection_ids_for_workspace("").await.is_empty(),
            "an empty selector is no workspace, never an all-connections wildcard"
        );
        assert!(
            manager.connection_ids_for_workspace("   ").await.is_empty(),
            "a whitespace selector is no workspace, never an all-connections wildcard"
        );
        assert_ne!(connection_a, connection_b);
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
        assert_eq!(
            manager
                .session_ids_for_principal(&principal_a.principal_id)
                .await,
            vec![principal_a.session_id.clone()]
        );
        assert_eq!(
            manager
                .connection_ids_for_principal(&principal_a.principal_id)
                .await,
            vec![connection_a, connection_b]
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
    #[tokio::test]
    async fn queued_derived_payload_checks_current_workspace_identity_and_expiry_at_enqueue() {
        let manager = SessionManager::new();
        let principal = authenticated_test_superuser();
        let (tx, mut rx) = mpsc::channel(4);
        let connection = manager
            .register_connection(tx, principal.clone())
            .await
            .unwrap();
        manager
            .set_connection_workspace(connection, Some("first".into()))
            .await;
        let send = |outbound: &SessionOutbound| {
            outbound
                .try_send(Message::Text("derived value".into()))
                .map_err(|error| anyhow!("{error}"))?;
            Ok(())
        };
        assert!(
            manager
                .with_notification_outbound(connection, &principal, "first", send)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            rx.recv().await.unwrap().into_text().unwrap(),
            "derived value"
        );
        manager
            .set_connection_workspace(connection, Some("second".into()))
            .await;
        assert!(
            manager
                .with_notification_outbound(connection, &principal, "first", send)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        let mut renewed = (*principal).clone();
        renewed.access_jti = "new-access-token".into();
        manager
            .renew_connection_principal(connection, &principal, Arc::new(renewed.clone()))
            .await
            .unwrap();
        assert!(
            manager
                .with_notification_outbound(connection, &principal, "second", send)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            manager
                .with_notification_outbound(connection, &renewed, "second", send)
                .await
                .unwrap()
                .is_some()
        );
        let _ = rx.recv().await.unwrap();
        let mut expired = renewed;
        expired.access_expires_at_unix = 0;
        let (expired_tx, mut expired_rx) = mpsc::channel(1);
        let expired_connection = manager
            .register_connection(expired_tx, Arc::new(expired.clone()))
            .await
            .unwrap();
        manager
            .set_connection_workspace(expired_connection, Some("second".into()))
            .await;
        assert!(
            manager
                .with_notification_outbound(expired_connection, &expired, "second", send)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            expired_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
