use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use pioneer_config::AppConfig;
use pioneer_protocol::GatewayReadinessStatus;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum ReadinessDegradation {
    Resilience = 1 << 0,
    SelfImprovement = 1 << 1,
    Mcp = 1 << 2,
    RemoteAccess = 1 << 3,
    DatabaseMaintenance = 1 << 4,
}

const GENERIC_DEGRADATION: u8 = 1 << 7;

#[derive(Clone, Debug, Default)]
pub(crate) struct ReadinessState {
    phase: Arc<AtomicU8>,
    degradations: Arc<AtomicU8>,
}

impl ReadinessState {
    pub(crate) fn status(&self) -> GatewayReadinessStatus {
        let phase = self.phase.load(Ordering::Acquire);
        if phase == 0 {
            return GatewayReadinessStatus::Starting;
        }
        if self.degradations.load(Ordering::Acquire) != 0 {
            return GatewayReadinessStatus::Degraded;
        }
        match phase {
            1 => GatewayReadinessStatus::AcceptingSessions,
            2 => GatewayReadinessStatus::Operational,
            _ => GatewayReadinessStatus::Starting,
        }
    }

    pub(crate) fn set_status(&self, status: GatewayReadinessStatus) {
        match status {
            GatewayReadinessStatus::Starting => {
                self.phase.store(0, Ordering::Release);
                self.degradations.store(0, Ordering::Release);
            }
            GatewayReadinessStatus::AcceptingSessions => self.phase.store(1, Ordering::Release),
            GatewayReadinessStatus::Operational => self.phase.store(2, Ordering::Release),
            GatewayReadinessStatus::Degraded => {
                self.degradations
                    .fetch_or(GENERIC_DEGRADATION, Ordering::AcqRel);
            }
        }
    }

    pub(crate) fn set_degraded(&self, component: ReadinessDegradation, degraded: bool) {
        let component = component as u8;
        if degraded {
            self.degradations.fetch_or(component, Ordering::AcqRel);
        } else {
            self.degradations.fetch_and(!component, Ordering::AcqRel);
        }
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

    pub(crate) fn set_readiness(&self, status: GatewayReadinessStatus) {
        self.readiness.set_status(status);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.readiness.status().accepts_sessions()
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
        assert_eq!(readiness.status(), GatewayReadinessStatus::Starting);

        readiness.set_status(GatewayReadinessStatus::AcceptingSessions);
        assert!(readiness.status().accepts_sessions());

        readiness.set_status(GatewayReadinessStatus::Degraded);
        assert!(readiness.status().accepts_sessions());
    }

    #[test]
    fn component_degradation_recovers_without_hiding_other_failures() {
        let readiness = ReadinessState::default();
        readiness.set_status(GatewayReadinessStatus::Operational);
        readiness.set_degraded(ReadinessDegradation::Mcp, true);
        readiness.set_degraded(ReadinessDegradation::RemoteAccess, true);
        assert_eq!(readiness.status(), GatewayReadinessStatus::Degraded);

        readiness.set_degraded(ReadinessDegradation::Mcp, false);
        assert_eq!(readiness.status(), GatewayReadinessStatus::Degraded);

        readiness.set_degraded(ReadinessDegradation::RemoteAccess, false);
        assert_eq!(readiness.status(), GatewayReadinessStatus::Operational);
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
