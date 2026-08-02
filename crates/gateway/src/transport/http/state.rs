use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use pioneer_config::AppConfig;
use tokio::sync::{Mutex, watch};

use crate::auth::{AuthAdmissionService, GatewayAuthService};
use crate::message::MessageProcessor;
use crate::session::SessionManager;
use crate::transport::ws::admission::AuthAbuseLimiter;

use super::streams::{HttpStreamConfigError, HttpStreamRegistry};
use crate::view_grants::{ViewGrantError, ViewGrantService};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GatewayHttpStateError {
    InvalidHttpStreamConfig,
    InvalidViewGrantConfig(ViewGrantError),
}

impl From<HttpStreamConfigError> for GatewayHttpStateError {
    fn from(_: HttpStreamConfigError) -> Self {
        Self::InvalidHttpStreamConfig
    }
}

impl From<ViewGrantError> for GatewayHttpStateError {
    fn from(error: ViewGrantError) -> Self {
        Self::InvalidViewGrantConfig(error)
    }
}

#[derive(Default)]
struct ActiveConnectionState {
    accepting: bool,
    connections: HashMap<u64, watch::Sender<bool>>,
}

impl ActiveConnectionState {
    fn accepting() -> Self {
        Self {
            accepting: true,
            connections: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ActiveConnectionRegistry {
    next_id: Arc<AtomicU64>,
    state: Arc<Mutex<ActiveConnectionState>>,
}

impl Default for ActiveConnectionRegistry {
    fn default() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(0)),
            state: Arc::new(Mutex::new(ActiveConnectionState::accepting())),
        }
    }
}

impl ActiveConnectionRegistry {
    pub(crate) async fn register(&self) -> ActiveConnectionGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (cancellation_tx, cancellation_rx) = watch::channel(false);
        let mut state = self.state.lock().await;
        let registered = state.accepting;
        if registered {
            state.connections.insert(id, cancellation_tx);
        } else {
            let _ = cancellation_tx.send(true);
        }
        ActiveConnectionGuard {
            id,
            registered,
            cancellation_rx,
            registry: self.clone(),
        }
    }

    pub(crate) async fn cancel_all(&self) {
        let mut state = self.state.lock().await;
        state.accepting = false;
        for sender in state.connections.values() {
            let _ = sender.send(true);
        }
    }

    async fn unregister(&self, id: u64) {
        self.state.lock().await.connections.remove(&id);
    }

    #[cfg(test)]
    pub(crate) async fn len(&self) -> usize {
        self.state.lock().await.connections.len()
    }
}

pub(crate) struct ActiveConnectionGuard {
    id: u64,
    registered: bool,
    cancellation_rx: watch::Receiver<bool>,
    registry: ActiveConnectionRegistry,
}

impl ActiveConnectionGuard {
    pub(crate) fn cancellation_receiver(&self) -> watch::Receiver<bool> {
        self.cancellation_rx.clone()
    }

    pub(crate) async fn unregister(self) {
        if self.registered {
            self.registry.unregister(self.id).await;
        }
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
    ) -> Result<Self, GatewayHttpStateError> {
        let http_streams = HttpStreamRegistry::new(&config.gateway.artifacts)?;
        let view_grants = ViewGrantService::new(&config.gateway.artifacts)?;
        auth_service.add_disconnect_hook(http_streams.clone());
        auth_service.add_disconnect_hook(view_grants.clone());
        message_processor.set_artifact_stream_invalidation(http_streams.clone());
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

    pub(crate) fn is_ready(&self) -> bool {
        self.readiness.is_ready()
    }

    pub(crate) fn readiness(&self) -> ReadinessState {
        self.readiness.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_is_fail_closed_until_explicitly_enabled() {
        let readiness = ReadinessState::default();
        assert!(!readiness.is_ready());

        readiness.set_ready(true);
        assert!(readiness.is_ready());

        readiness.set_ready(false);
        assert!(!readiness.is_ready());
    }

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

        let late = registry.register().await;
        assert!(*late.cancellation_receiver().borrow());
        assert_eq!(registry.len().await, 0);
        late.unregister().await;
    }
}
