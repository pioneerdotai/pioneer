use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pioneer_config::AppConfig;
use tokio::sync::{Mutex, watch};

use crate::auth::{AuthAdmissionService, GatewayAuthService};
use crate::message::MessageProcessor;
use crate::session::SessionManager;
use crate::transport::ws::admission::AuthAbuseLimiter;

use super::streams::HttpStreamRegistry;
use super::view_grants::{ViewGrantError, ViewGrantService};

#[derive(Clone, Default)]
pub(crate) struct ActiveConnectionRegistry {
    next_id: Arc<AtomicU64>,
    connections: Arc<Mutex<HashMap<u64, watch::Sender<bool>>>>,
}

impl ActiveConnectionRegistry {
    pub(crate) async fn register(&self) -> ActiveConnectionGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (cancellation_tx, cancellation_rx) = watch::channel(false);
        self.connections.lock().await.insert(id, cancellation_tx);
        ActiveConnectionGuard {
            id,
            cancellation_rx,
            registry: self.clone(),
        }
    }

    pub(crate) async fn cancel_all(&self) {
        let senders = self
            .connections
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for sender in senders {
            let _ = sender.send(true);
        }
    }

    async fn unregister(&self, id: u64) {
        self.connections.lock().await.remove(&id);
    }

    #[cfg(test)]
    pub(crate) async fn len(&self) -> usize {
        self.connections.lock().await.len()
    }
}

pub(crate) struct ActiveConnectionGuard {
    id: u64,
    cancellation_rx: watch::Receiver<bool>,
    registry: ActiveConnectionRegistry,
}

impl ActiveConnectionGuard {
    pub(crate) fn cancellation_receiver(&self) -> watch::Receiver<bool> {
        self.cancellation_rx.clone()
    }

    pub(crate) async fn unregister(self) {
        self.registry.unregister(self.id).await;
    }
}

#[derive(Clone, Default)]
pub(crate) struct ReadinessState {
    ready: Arc<AtomicBool>,
}

impl ReadinessState {
    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }
}

#[derive(Clone)]
pub(crate) struct GatewayHttpState {
    pub(crate) config: AppConfig,
    pub(crate) auth: AuthAdmissionService,
    pub(crate) auth_service: Arc<GatewayAuthService>,
    pub(crate) message_processor: Arc<MessageProcessor>,
    pub(crate) session_manager: Arc<SessionManager>,
    pub(crate) active_connections: ActiveConnectionRegistry,
    pub(crate) auth_abuse_limiter: Arc<AuthAbuseLimiter>,
    pub(crate) http_streams: Arc<HttpStreamRegistry>,
    pub(crate) view_grants: Arc<ViewGrantService>,
    readiness: ReadinessState,
}

impl GatewayHttpState {
    pub(crate) fn new(
        config: AppConfig,
        auth: AuthAdmissionService,
        auth_service: Arc<GatewayAuthService>,
        message_processor: Arc<MessageProcessor>,
        session_manager: Arc<SessionManager>,
    ) -> Result<Self, ViewGrantError> {
        let http_streams = HttpStreamRegistry::new(&config.gateway.artifacts);
        let view_grants = ViewGrantService::new(&config.gateway.artifacts)?;
        auth_service.add_disconnect_hook(http_streams.clone());
        auth_service.add_disconnect_hook(view_grants.clone());
        message_processor.set_http_stream_registry(http_streams.clone());
        message_processor.set_view_grant_service(view_grants.clone());
        Ok(Self {
            config,
            auth,
            auth_service,
            message_processor,
            session_manager,
            active_connections: ActiveConnectionRegistry::default(),
            auth_abuse_limiter: Arc::new(AuthAbuseLimiter::default()),
            http_streams,
            view_grants,
            readiness: ReadinessState::default(),
        })
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.readiness.set_ready(ready);
    }

    pub(crate) fn readiness(&self) -> ReadinessState {
        self.readiness.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn active_connection_shutdown_is_targeted_bounded_and_idempotent() {
        let registry = ActiveConnectionRegistry::default();
        let first = registry.register().await;
        let second = registry.register().await;
        let mut first_cancel = first.cancellation_receiver();
        let mut second_cancel = second.cancellation_receiver();
        assert_eq!(registry.len().await, 2);

        registry.cancel_all().await;
        first_cancel.changed().await.unwrap();
        second_cancel.changed().await.unwrap();
        assert!(*first_cancel.borrow());
        assert!(*second_cancel.borrow());

        registry.cancel_all().await;
        first.unregister().await;
        first_cancel.mark_unchanged();
        assert_eq!(registry.len().await, 1);
        second.unregister().await;
        assert_eq!(registry.len().await, 0);
    }
}
