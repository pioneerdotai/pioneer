//! Process-local Gateway session ownership. Shells execute native effects but
//! never retain another lifecycle reducer.

use super::session_lifecycle::{
    SessionLifecycle, SessionLifecycleEffect, SessionLifecycleEvent, SessionLifecycleState,
};
use serde::Serialize;
use std::collections::BTreeMap;

use crate::state::reducers::{GatewayStatusProjection, GatewayStatusTextUpdate};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupStage {
    GatewayRuntime,
    GatewaySession,
    Authorization,
    Workspace,
    Provider,
    ThreadTree,
    ActiveThreadBootstrap,
    ActiveThreadSubscription,
    ThreadCapabilities,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupStageState {
    Pending,
    Succeeded,
    Failed,
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionDiagnosticStage {
    CredentialsLoad,
    RefreshIntentPersist,
    RefreshRequest,
    CredentialsPersist,
    ConnectAttempt,
    IdentityVerify,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SessionDiagnosticTiming {
    pub endpoint_id: String,
    pub started_at_unix_ms: u64,
    pub duration_ms: Option<u64>,
    pub state: StartupStageState,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StartupCoordinator {
    pub stages: BTreeMap<StartupStage, StartupStageState>,
    pub session_diagnostics: BTreeMap<SessionDiagnosticStage, SessionDiagnosticTiming>,
    pub transport_revision: u64,
    pub connection_id: Option<u64>,
    pub endpoint_id: Option<String>,
    pub retry_attempt: u32,
    pub retry_delay_ms: u64,
    pub transport_ready: bool,
    pub identity_pending: bool,
}

impl StartupCoordinator {
    pub fn has_failed(&self) -> bool {
        self.stages
            .values()
            .any(|state| *state == StartupStageState::Failed)
    }
    pub fn stage_succeeded(&self, stage: StartupStage) -> bool {
        self.stages.get(&stage) == Some(&StartupStageState::Succeeded)
    }
}

pub(crate) struct GatewaySessionController {
    unverified_transport: Option<crate::transport::ws::GatewayWsEvent>,
    connection_delivery: Option<crate::state::reducers::GatewayConnectionReduction>,
    deferred_events: std::collections::VecDeque<crate::transport::ws::GatewayWsEvent>,
    operation_epoch: u64,
    refresh_generation: u64,
    refresh_in_flight: bool,
    pub(crate) connections:
        BTreeMap<String, super::session_connection::GatewaySessionConnectionState>,
    next_generation: u64,
    status: Option<GatewayStatusProjection>,
    gateway_error: Option<String>,
    startup: StartupCoordinator,
    lifecycles: BTreeMap<String, SessionLifecycle>,
    access_expiries: BTreeMap<String, u64>,
}

impl Default for GatewaySessionController {
    fn default() -> Self {
        Self {
            unverified_transport: None,
            connection_delivery: None,
            deferred_events: std::collections::VecDeque::new(),
            operation_epoch: 0,
            refresh_generation: 0,
            refresh_in_flight: false,
            connections: BTreeMap::new(),
            next_generation: 1,
            status: None,
            gateway_error: None,
            startup: StartupCoordinator::default(),
            lifecycles: BTreeMap::new(),
            access_expiries: BTreeMap::new(),
        }
    }
}

impl crate::core::ClientCore {
    pub(crate) fn observe_session_stage<T, E>(
        &self,
        endpoint: &str,
        stage: SessionDiagnosticStage,
        operation: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let started = std::time::Instant::now();
        let observed = {
            let mut owner = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            if self.is_stopped() || owner.startup.session_diagnostics.contains_key(&stage) {
                false
            } else {
                let started_at_unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
                    .unwrap_or(0);
                owner.startup.session_diagnostics.insert(
                    stage,
                    SessionDiagnosticTiming {
                        endpoint_id: endpoint.into(),
                        started_at_unix_ms,
                        duration_ms: None,
                        state: StartupStageState::Pending,
                    },
                );
                self.publish_gateway_session(&owner);
                true
            }
        };
        let result = operation();
        if observed {
            let mut owner = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            if !self.is_stopped() {
                if let Some(timing) = owner.startup.session_diagnostics.get_mut(&stage) {
                    timing.duration_ms =
                        Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
                    timing.state = if result.is_ok() {
                        StartupStageState::Succeeded
                    } else {
                        StartupStageState::Failed
                    };
                }
                self.publish_gateway_session(&owner);
            }
        }
        result
    }

    pub fn partition_gateway_compatibility_events(
        &self,
        active_connection_id: Option<u64>,
        replacing: bool,
        events: impl IntoIterator<Item = crate::transport::ws::GatewayWsEvent>,
    ) -> Vec<crate::transport::ws::GatewayWsEvent> {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() {
            return vec![];
        }
        let mut applicable = Vec::new();
        for event in events {
            if crate::transport::ws::should_apply_ws_event(active_connection_id, &event) {
                applicable.push(event);
            } else if replacing {
                if owner.deferred_events.len() == 32 {
                    owner.deferred_events.pop_front();
                }
                owner.deferred_events.push_back(event);
            }
        }
        applicable
    }

    pub fn replay_gateway_compatibility_events(
        &self,
        active_connection_id: Option<u64>,
        notifications_only: bool,
    ) -> Vec<crate::transport::ws::GatewayWsEvent> {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        owner
            .deferred_events
            .drain(..)
            .filter(|event| {
                !self.is_stopped()
                    && crate::transport::ws::should_apply_ws_event(active_connection_id, event)
                    && (!notifications_only
                        || matches!(
                            event,
                            crate::transport::ws::GatewayWsEvent::Notification { .. }
                        ))
            })
            .collect()
    }

    pub fn discard_gateway_compatibility_events(&self) {
        self.gateway_session
            .lock()
            .expect("Gateway session owner poisoned")
            .deferred_events
            .clear();
    }

    /// Immutable input for the remaining Desktop workspace/thread bootstrap owner.
    /// Session state was already reduced at ingress; only unported feature context
    /// is selected here until those feature owners consume their own scopes.
    pub fn gateway_feature_connection_projection(
        &self,
        context: crate::runtime::ClientRuntimeWsEventContext,
    ) -> Option<crate::state::reducers::GatewayConnectionReduction> {
        use crate::state::client_state::GatewayConnectionState;
        let owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() || owner.startup.identity_pending {
            return None;
        }
        let mut reduction = owner.connection_delivery.clone()?;
        if reduction.connection_state == GatewayConnectionState::Connected
            && context.queue_skills_refresh
        {
            reduction
                .effects
                .push(crate::notifications::effects::ClientEffect::QueueSkillsRefresh);
        }
        if reduction.connection_state != GatewayConnectionState::Connecting
            && reduction.connection_state != GatewayConnectionState::Connected
        {
            reduction.clear_active_thread = !context.should_resume_in_flight_turn;
        }
        Some(reduction)
    }

    pub fn gateway_operation_epoch(&self) -> u64 {
        self.gateway_session
            .lock()
            .expect("Gateway session owner poisoned")
            .operation_epoch
    }

    pub fn begin_gateway_operation(&self) -> u64 {
        self.begin_authorization_epoch(None);
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() {
            return owner.operation_epoch;
        }
        owner.operation_epoch = owner
            .operation_epoch
            .checked_add(1)
            .expect("Gateway operation epoch exhausted");
        owner.refresh_generation = owner
            .refresh_generation
            .checked_add(1)
            .expect("Gateway refresh generation exhausted");
        owner.refresh_in_flight = false;
        owner.deferred_events.clear();
        for connection in owner.connections.values_mut() {
            connection.cancel_pending_request();
        }
        self.publish_gateway_session(&owner);
        owner.operation_epoch
    }

    pub fn gateway_refresh_generation(&self) -> u64 {
        self.gateway_session
            .lock()
            .expect("Gateway session owner poisoned")
            .refresh_generation
    }

    pub fn gateway_refresh_in_flight(&self) -> bool {
        self.gateway_session
            .lock()
            .expect("Gateway session owner poisoned")
            .refresh_in_flight
    }

    pub fn schedule_gateway_refresh(&self) -> Option<u64> {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() || owner.refresh_in_flight {
            return None;
        }
        owner.refresh_generation = owner
            .refresh_generation
            .checked_add(1)
            .expect("Gateway refresh generation exhausted");
        Some(owner.refresh_generation)
    }

    pub fn start_gateway_refresh(&self, generation: u64) -> bool {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() || owner.refresh_generation != generation || owner.refresh_in_flight {
            return false;
        }
        owner.refresh_in_flight = true;
        true
    }

    pub fn finish_gateway_refresh(&self, generation: u64) -> bool {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() || owner.refresh_generation != generation || !owner.refresh_in_flight {
            return false;
        }
        owner.refresh_in_flight = false;
        true
    }

    pub fn cancel_gateway_refresh(&self) {
        let mut owner = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        if self.is_stopped() {
            return;
        }
        owner.refresh_generation = owner
            .refresh_generation
            .checked_add(1)
            .expect("Gateway refresh generation exhausted");
        owner.refresh_in_flight = false;
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct GatewaySessionPublication {
    pub connections:
        BTreeMap<String, super::session_connection::GatewaySessionConnectionProjection>,
    pub status: Option<GatewayStatusProjection>,
    pub gateway_error: Option<String>,
    pub startup: StartupCoordinator,
    sessions: BTreeMap<String, SessionLifecycleState>,
    access_expiries: BTreeMap<String, u64>,
}

impl GatewaySessionPublication {
    pub fn terminal_reason(
        &self,
        endpoint_id: &str,
    ) -> Option<super::session_lifecycle::SessionTerminalReason> {
        match self.session(endpoint_id)? {
            SessionLifecycleState::Terminal { reason, .. } => Some(*reason),
            _ => None,
        }
    }
    pub fn refresh_delay(
        &self,
        endpoint_id: &str,
        now_unix: u64,
        leeway_seconds: u64,
    ) -> Option<std::time::Duration> {
        self.access_expiries.get(endpoint_id).map(|expires| {
            std::time::Duration::from_secs(
                expires
                    .saturating_sub(leeway_seconds)
                    .saturating_sub(now_unix),
            )
        })
    }

    pub fn session(&self, endpoint_id: &str) -> Option<&SessionLifecycleState> {
        self.sessions.get(endpoint_id)
    }
}

pub struct GatewaySessionTransition {
    state: SessionLifecycleState,
    effect: SessionLifecycleEffect,
}

impl GatewaySessionTransition {
    pub(crate) fn stopped() -> Self {
        Self {
            state: SessionLifecycleState::NoSession,
            effect: SessionLifecycleEffect::None,
        }
    }
    pub fn state(&self) -> &SessionLifecycleState {
        &self.state
    }
    pub fn effect(&self) -> &SessionLifecycleEffect {
        &self.effect
    }
}

impl GatewaySessionController {
    pub(crate) fn stop(&mut self) {
        for connection in self.connections.values_mut() {
            connection.invalidate();
        }
        self.lifecycles.clear();
        self.access_expiries.clear();
        self.unverified_transport = None;
        self.connection_delivery = None;
        self.deferred_events.clear();
        self.refresh_in_flight = false;
        self.startup.identity_pending = false;
        self.startup.transport_ready = false;
        for state in self.startup.stages.values_mut() {
            if *state == StartupStageState::Pending {
                *state = StartupStageState::Cancelled;
            }
        }
    }

    pub(crate) fn update_startup(
        &mut self,
        stage: StartupStage,
        state: StartupStageState,
    ) -> crate::core::ClientTransitionOutcome {
        use crate::core::ClientTransitionOutcome;
        match self.startup.stages.get(&stage) {
            Some(current) if *current == state => ClientTransitionOutcome::Noop,
            Some(StartupStageState::Pending) if state != StartupStageState::Pending => {
                self.startup.stages.insert(stage, state);
                ClientTransitionOutcome::Changed
            }
            None if state == StartupStageState::Pending => {
                self.startup.stages.insert(stage, state);
                ClientTransitionOutcome::Changed
            }
            _ => ClientTransitionOutcome::Rejected,
        }
    }

    pub(crate) fn project_status(
        &mut self,
        input: crate::state::reducers::GatewayStatusInput,
    ) -> GatewayStatusProjection {
        let mut projection = crate::state::reducers::project_gateway_status(input);
        if matches!(projection.status, GatewayStatusTextUpdate::KeepExisting) {
            if let Some(current) = &self.status {
                projection.status = current.status.clone();
            }
        }
        self.status = Some(projection.clone());
        projection
    }

    pub(crate) fn observe_transport(&mut self, event: &crate::transport::ws::GatewayWsEvent) {
        use crate::transport::ws::GatewayWsEvent;
        let endpoint_id = match event {
            GatewayWsEvent::Connecting { endpoint_id, .. }
            | GatewayWsEvent::Connected { endpoint_id, .. }
            | GatewayWsEvent::Reconnecting { endpoint_id, .. }
            | GatewayWsEvent::Disconnected { endpoint_id, .. }
            | GatewayWsEvent::ConnectFailed { endpoint_id, .. } => endpoint_id,
            GatewayWsEvent::Notification { .. } => return,
        };
        if matches!(event, GatewayWsEvent::Connected { .. })
            && self.connections.get(endpoint_id).is_some_and(|state| {
                !state.transport_verified(crate::transport::ws::event_connection_id(event))
            })
        {
            self.unverified_transport = Some(event.clone());
            self.startup.endpoint_id = Some(endpoint_id.clone());
            self.startup.connection_id = Some(crate::transport::ws::event_connection_id(event));
            self.startup.identity_pending = true;
            return;
        }
        let crate::runtime::ClientRuntimeWsEvent::Connection(reduction) =
            crate::runtime::reduce_gateway_ws_event(event.clone(), Default::default())
        else {
            return;
        };
        if self.connection_delivery.as_ref() != Some(&reduction)
            || self.startup.connection_id != Some(crate::transport::ws::event_connection_id(event))
        {
            self.startup.transport_revision = self
                .startup
                .transport_revision
                .checked_add(1)
                .expect("transport revision exhausted");
            self.connection_delivery = Some(reduction.clone());
        }
        let connection_id = crate::transport::ws::event_connection_id(event);
        self.status = Some(GatewayStatusProjection {
            status: GatewayStatusTextUpdate::Set(reduction.status),
            status_level: reduction.status_level,
            connection_state: reduction.connection_state,
            clear_gateway_error: reduction.gateway_error.is_none(),
        });
        self.gateway_error = reduction.gateway_error;
        self.startup.endpoint_id = Some(endpoint_id.clone());
        self.startup.connection_id = Some(connection_id);
        self.startup.transport_ready = matches!(event, GatewayWsEvent::Connected { .. });
        self.startup.identity_pending = false;
        if let GatewayWsEvent::Reconnecting {
            attempt, delay_ms, ..
        } = event
        {
            self.startup.retry_attempt = *attempt;
            self.startup.retry_delay_ms = *delay_ms;
        } else {
            self.startup.retry_attempt = 0;
            self.startup.retry_delay_ms = 0;
        }
    }

    pub(crate) fn finish_transport_verification(&mut self, endpoint: &str, accepted: bool) {
        let matches = matches!(&self.unverified_transport,
            Some(crate::transport::ws::GatewayWsEvent::Connected { endpoint_id, .. }) if endpoint_id == endpoint);
        if matches {
            let event = self
                .unverified_transport
                .take()
                .expect("matched transport candidate");
            self.startup.identity_pending = false;
            if accepted {
                self.observe_transport(&event);
            }
        }
    }

    pub(crate) fn reduce(
        &mut self,
        endpoint_id: &str,
        event: SessionLifecycleEvent,
    ) -> GatewaySessionTransition {
        let release = matches!(event, SessionLifecycleEvent::NoStoredSession);
        let next_generation = self.next_generation;
        let lifecycle = self
            .lifecycles
            .entry(endpoint_id.to_owned())
            .or_insert_with(|| SessionLifecycle::with_generation_floor(next_generation));
        let previous = lifecycle.state().clone();
        let effect = lifecycle.reduce(event);
        let state = lifecycle.state().clone();
        self.next_generation = self.next_generation.max(lifecycle.next_generation_floor());
        if previous != state
            && matches!(
                &state,
                SessionLifecycleState::Terminal { .. }
                    | SessionLifecycleState::Suspended { .. }
                    | SessionLifecycleState::NoSession
                    | SessionLifecycleState::NeedsDeviceActivation
            )
        {
            if let Some(connection) = self.connections.get_mut(endpoint_id) {
                connection.invalidate();
            }
            self.finish_transport_verification(endpoint_id, false);
        }
        match &state {
            SessionLifecycleState::Connecting {
                access_expires_at_unix,
                ..
            }
            | SessionLifecycleState::Active {
                access_expires_at_unix,
                ..
            } => {
                self.access_expiries
                    .insert(endpoint_id.to_owned(), *access_expires_at_unix);
            }
            SessionLifecycleState::Terminal { .. }
            | SessionLifecycleState::Suspended { .. }
            | SessionLifecycleState::NoSession
            | SessionLifecycleState::NeedsDeviceActivation => {
                self.access_expiries.remove(endpoint_id);
            }
            SessionLifecycleState::Refreshing { .. }
            | SessionLifecycleState::AwaitingSecureStorage { .. } => {}
        }

        if release {
            self.lifecycles.remove(endpoint_id);
        }
        GatewaySessionTransition { state, effect }
    }

    pub(crate) fn publication(&self) -> GatewaySessionPublication {
        GatewaySessionPublication {
            connections: super::session_connection::project_connections(&self.connections),
            status: self.status.clone(),
            gateway_error: self.gateway_error.clone(),
            startup: self.startup.clone(),
            access_expiries: self.access_expiries.clone(),
            sessions: self
                .lifecycles
                .iter()
                .map(|(id, lifecycle)| (id.clone(), lifecycle.state().clone()))
                .collect(),
        }
    }
}

/// Excludes refresh while a native adapter removes or replaces a durable session.
pub struct GatewaySessionMutationGuard {
    slot: std::sync::Arc<std::sync::Mutex<bool>>,
}

impl Drop for GatewaySessionMutationGuard {
    fn drop(&mut self) {
        if let Ok(mut mutation) = self.slot.lock() {
            *mutation = false;
        }
    }
}

impl crate::core::ClientCore {
    fn gateway_session_refresh_slot(
        &self,
        endpoint_id: &str,
    ) -> anyhow::Result<std::sync::Arc<std::sync::Mutex<bool>>> {
        if self.is_stopped() {
            anyhow::bail!("Client session runtime is stopped");
        }
        let mut slots = self
            .session_refresh_slots
            .lock()
            .map_err(|_| anyhow::anyhow!("session refresh registry poisoned"))?;
        slots.retain(|_, slot| slot.strong_count() > 0);
        if let Some(slot) = slots.get(endpoint_id).and_then(std::sync::Weak::upgrade) {
            return Ok(slot);
        }
        let slot = std::sync::Arc::new(std::sync::Mutex::new(false));
        slots.insert(endpoint_id.to_owned(), std::sync::Arc::downgrade(&slot));
        Ok(slot)
    }

    pub fn with_gateway_session_refresh<T>(
        &self,
        endpoint_id: &str,
        operation: impl FnOnce() -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let slot = self.gateway_session_refresh_slot(endpoint_id)?;
        let mutation = slot
            .lock()
            .map_err(|_| anyhow::anyhow!("session refresh lock poisoned"))?;
        if *mutation {
            anyhow::bail!("Gateway session mutation is in progress");
        }
        if self.is_stopped() {
            anyhow::bail!("Client session runtime is stopped");
        }
        operation()
    }

    pub fn begin_gateway_session_mutation(
        &self,
        endpoint_id: &str,
    ) -> anyhow::Result<GatewaySessionMutationGuard> {
        let slot = self.gateway_session_refresh_slot(endpoint_id)?;
        {
            let mut mutation = slot
                .lock()
                .map_err(|_| anyhow::anyhow!("session refresh lock poisoned"))?;
            if *mutation {
                anyhow::bail!("Gateway session mutation is already in progress");
            }
            *mutation = true;
        }
        Ok(GatewaySessionMutationGuard { slot })
    }
}

#[cfg(test)]
mod tests {
    use super::super::session_lifecycle::{GatewaySessionMetadata, SessionTerminalReason};
    use super::*;
    use crate::core::{ClientCore, ClientScope};

    #[test]
    fn deferred_compatibility_delivery_is_bounded_epoch_scoped_and_dropped_on_shutdown() {
        use crate::transport::ws::GatewayWsEvent;
        let event = |connection_id| GatewayWsEvent::Connecting {
            connection_id,
            endpoint_id: "synthetic".into(),
            endpoint_name: "Synthetic".into(),
            endpoint_kind: super::super::types::GatewayEndpointKind::Remote,
        };
        let core = ClientCore::new();
        assert!(
            core.partition_gateway_compatibility_events(Some(7), true, [event(8)])
                .is_empty()
        );
        assert_eq!(
            core.replay_gateway_compatibility_events(Some(8), false)
                .len(),
            1
        );
        assert!(
            core.partition_gateway_compatibility_events(None, true, (1..=33).map(event))
                .is_empty()
        );
        {
            let owner = core.gateway_session.lock().unwrap();
            assert_eq!(owner.deferred_events.len(), 32);
            assert_eq!(
                owner
                    .deferred_events
                    .front()
                    .map(crate::transport::ws::event_connection_id),
                Some(2)
            );
        }
        assert!(
            core.replay_gateway_compatibility_events(Some(1), false)
                .is_empty()
        );
        core.partition_gateway_compatibility_events(None, true, [event(8)]);
        assert!(
            core.replay_gateway_compatibility_events(Some(8), true)
                .is_empty()
        );
        let notification = GatewayWsEvent::Notification {
            connection_id: 8,
            notification: pioneer_protocol::GatewayNotification::Unknown(
                pioneer_protocol::UnknownGatewayNotification {
                    method: "synthetic.notification".into(),
                    workspace_id: None,
                    thread_id: None,
                    turn_id: None,
                    item_id: None,
                    params: serde_json::json!({}),
                },
            ),
        };
        core.partition_gateway_compatibility_events(None, true, [notification]);
        assert_eq!(
            core.replay_gateway_compatibility_events(Some(8), true)
                .len(),
            1
        );
        core.partition_gateway_compatibility_events(None, true, [event(8)]);
        core.begin_gateway_operation();
        assert!(
            core.replay_gateway_compatibility_events(Some(8), false)
                .is_empty()
        );
        core.partition_gateway_compatibility_events(None, true, [event(8)]);
        core.shutdown();
        assert!(
            core.replay_gateway_compatibility_events(Some(8), false)
                .is_empty()
        );
        assert!(
            core.partition_gateway_compatibility_events(Some(8), false, [event(8)])
                .is_empty()
        );
    }

    #[test]
    fn refresh_clock_and_operation_generations_reject_duplicate_and_superseded_callbacks() {
        let core = ClientCore::new();
        let other = ClientCore::new();
        let generation = core.schedule_gateway_refresh().unwrap();
        assert!(core.start_gateway_refresh(generation));
        assert!(core.schedule_gateway_refresh().is_none());
        assert!(!core.start_gateway_refresh(generation));
        assert_eq!(core.gateway_refresh_generation(), generation);
        let operation = core.begin_gateway_operation();
        assert_eq!(core.gateway_operation_epoch(), operation);
        assert!(!core.finish_gateway_refresh(generation));
        assert!(!core.gateway_refresh_in_flight());
        let next = core.schedule_gateway_refresh().unwrap();
        assert!(next > generation);
        assert!(core.start_gateway_refresh(next));
        assert!(core.finish_gateway_refresh(next));
        assert!(!core.finish_gateway_refresh(next));
        assert_eq!(other.gateway_operation_epoch(), 0);
        assert_eq!(other.gateway_refresh_generation(), 0);
        core.shutdown();
        assert!(core.schedule_gateway_refresh().is_none());
        assert!(!core.start_gateway_refresh(next));
        assert_eq!(core.begin_gateway_operation(), operation);
    }

    #[test]
    fn startup_milestones_are_process_local_and_terminal_completion_cannot_be_rewritten() {
        use crate::core::ClientTransitionOutcome;
        let core = ClientCore::new();
        let other = ClientCore::new();
        assert_eq!(
            core.update_startup_stage(StartupStage::Authorization, StartupStageState::Succeeded),
            ClientTransitionOutcome::Rejected
        );
        assert_eq!(
            core.update_startup_stage(StartupStage::Authorization, StartupStageState::Pending),
            ClientTransitionOutcome::Changed
        );
        assert_eq!(
            core.update_startup_stage(StartupStage::Authorization, StartupStageState::Succeeded),
            ClientTransitionOutcome::Changed
        );
        let complete = core.gateway_session();
        assert!(
            complete
                .startup
                .stage_succeeded(StartupStage::Authorization)
        );
        assert_eq!(
            core.update_startup_stage(StartupStage::Authorization, StartupStageState::Succeeded),
            ClientTransitionOutcome::Noop
        );
        assert_eq!(
            core.update_startup_stage(StartupStage::Authorization, StartupStageState::Failed),
            ClientTransitionOutcome::Rejected
        );
        assert!(std::sync::Arc::ptr_eq(&complete, &core.gateway_session()));
        assert!(other.gateway_session().startup.stages.is_empty());
        core.update_startup_stage(StartupStage::Provider, StartupStageState::Pending);
        core.update_startup_stage(StartupStage::Provider, StartupStageState::Cancelled);
        assert!(!core.gateway_session().startup.has_failed());
        core.shutdown();
        assert_eq!(
            core.update_startup_stage(StartupStage::Workspace, StartupStageState::Pending),
            ClientTransitionOutcome::Rejected
        );
    }

    #[test]
    fn refresh_exclusion_is_process_local_and_covers_the_entire_native_handoff() {
        let core = ClientCore::new();
        let independent = ClientCore::new();
        let slot = core.gateway_session_refresh_slot("endpoint").unwrap();
        core.with_gateway_session_refresh("endpoint", || {
            assert!(slot.try_lock().is_err());
            independent.with_gateway_session_refresh("endpoint", || Ok(()))?;
            Ok(())
        })
        .unwrap();
        assert!(slot.try_lock().is_ok());
        let mutation = core.begin_gateway_session_mutation("endpoint").unwrap();
        assert!(
            core.with_gateway_session_refresh("endpoint", || Ok(()))
                .is_err()
        );
        drop(mutation);
        assert!(
            core.with_gateway_session_refresh("endpoint", || Ok(()))
                .is_ok()
        );
    }

    fn metadata(generation: u64) -> GatewaySessionMetadata {
        GatewaySessionMetadata {
            gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
            device_id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
            session_id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
            refresh_generation: generation,
            refresh_expires_at_unix: 10_000,
        }
    }

    #[test]
    fn durable_storage_gates_access_publication_and_revoke_clears_the_clock() {
        let core = ClientCore::new();
        let start = core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::StoredSessionLoaded(metadata(0)),
        );
        let SessionLifecycleEffect::BeginRefresh { intent_id, .. } = start.effect() else {
            panic!("refresh effect");
        };
        let intent_id = *intent_id;
        core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::RefreshGrantReceived {
                intent_id,
                metadata: metadata(1),
                access_expires_at_unix: 1_000,
            },
        );
        assert_eq!(
            core.gateway_session().refresh_delay("endpoint", 900, 60),
            None
        );
        core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::SecureStorageCommitted { intent_id },
        );
        assert_eq!(
            core.gateway_session().refresh_delay("endpoint", 900, 60),
            Some(std::time::Duration::from_secs(40))
        );
        let retained = core.gateway_session();
        core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::AuthFailed {
                reason: SessionTerminalReason::SessionRevoked,
            },
        );
        let revoked = core.gateway_session();
        assert_eq!(
            revoked.terminal_reason("endpoint"),
            Some(SessionTerminalReason::SessionRevoked)
        );
        assert_eq!(revoked.refresh_delay("endpoint", 900, 60), None);
        assert!(retained.refresh_delay("endpoint", 900, 60).is_some());
    }

    #[test]
    fn independent_cores_noop_and_stale_completion_preserve_snapshot_identity() {
        let left = ClientCore::new();
        let right = ClientCore::new();
        left.reduce_gateway_session_lifecycle("endpoint", SessionLifecycleEvent::NoStoredSession);
        assert!(left.snapshot(&ClientScope::Session).is_none());
        left.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::StoredSessionLoaded(metadata(0)),
        );
        let before = left.gateway_session();
        left.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::SecureStorageCommitted {
                intent_id: u64::MAX,
            },
        );
        assert!(std::sync::Arc::ptr_eq(&before, &left.gateway_session()));
        assert!(right.gateway_session().session("endpoint").is_none());
        left.reduce_gateway_session_lifecycle("endpoint", SessionLifecycleEvent::NoStoredSession);
        assert!(left.gateway_session().session("endpoint").is_none());
    }

    #[test]
    fn released_endpoint_cannot_accept_an_old_refresh_completion() {
        let core = ClientCore::new();
        let old = core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::StoredSessionLoaded(metadata(0)),
        );
        let SessionLifecycleEffect::BeginRefresh { intent_id, .. } = old.effect() else {
            panic!("refresh effect");
        };
        core.reduce_gateway_session_lifecycle("endpoint", SessionLifecycleEvent::NoStoredSession);
        core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::StoredSessionLoaded(metadata(0)),
        );
        let current = core.gateway_session();
        core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::RefreshGrantReceived {
                intent_id: *intent_id,
                metadata: metadata(1),
                access_expires_at_unix: 1_000,
            },
        );
        assert!(std::sync::Arc::ptr_eq(&current, &core.gateway_session()));
    }

    #[test]
    fn process_task_shutdown_releases_the_core_and_rejects_late_completion() {
        let core = ClientCore::shared();
        let weak = std::sync::Arc::downgrade(&core);
        core.shutdown();
        core.reduce_gateway_session_lifecycle(
            "endpoint",
            SessionLifecycleEvent::StoredSessionLoaded(metadata(0)),
        );
        assert!(core.gateway_session().session("endpoint").is_none());
        drop(core);
        assert!(weak.upgrade().is_none());
    }
}

#[derive(Default)]
pub(crate) struct GatewayTransportLeases {
    next_id: u64,
    pending: std::collections::VecDeque<(u64, bool)>,
    active: BTreeMap<u64, bool>,
}

impl GatewayTransportLeases {
    fn grant(&mut self) {
        while let Some(&(id, exclusive)) = self.pending.front() {
            if self.active.values().any(|exclusive| *exclusive)
                || (exclusive && !self.active.is_empty())
            {
                break;
            }
            self.pending.pop_front();
            self.active.insert(id, exclusive);
            if exclusive {
                break;
            }
        }
    }
}

impl crate::core::ClientCore {
    /// Reserve synchronously so a transport transition fences later interactive work
    /// before its asynchronous shell continuation starts. This coordinates legacy
    /// workspace/composer operations without moving their feature policy here.
    pub fn reserve_gateway_transport(&self, exclusive: bool) -> anyhow::Result<u64> {
        let mut leases = self
            .gateway_transport_leases
            .lock()
            .expect("transport leases poisoned");
        anyhow::ensure!(!self.is_stopped(), "Client runtime is stopped");
        anyhow::ensure!(
            leases.pending.len() + leases.active.len() < 256,
            "Gateway transport lease capacity exceeded"
        );
        leases.next_id = leases
            .next_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Gateway transport lease identity exhausted"))?;
        let id = leases.next_id;
        leases.pending.push_back((id, exclusive));
        leases.grant();
        self.gateway_transport_ready.notify_all();
        Ok(id)
    }

    pub fn wait_gateway_transport(&self, id: u64) -> bool {
        let mut leases = self
            .gateway_transport_leases
            .lock()
            .expect("transport leases poisoned");
        loop {
            if self.is_stopped() {
                return false;
            }
            if leases.active.contains_key(&id) {
                return true;
            }
            if !leases.pending.iter().any(|(pending, _)| *pending == id) {
                return false;
            }
            leases = self
                .gateway_transport_ready
                .wait(leases)
                .expect("transport leases poisoned");
        }
    }

    pub fn release_gateway_transport(&self, id: u64) -> bool {
        let mut leases = self
            .gateway_transport_leases
            .lock()
            .expect("transport leases poisoned");
        let active = leases.active.remove(&id).is_some();
        let pending = leases
            .pending
            .iter()
            .position(|(pending, _)| *pending == id);
        if let Some(index) = pending {
            leases.pending.remove(index);
        }
        leases.grant();
        self.gateway_transport_ready.notify_all();
        active || pending.is_some()
    }

    pub(crate) fn close_gateway_transport_leases(&self) {
        let mut leases = self
            .gateway_transport_leases
            .lock()
            .expect("transport leases poisoned");
        leases.pending.clear();
        leases.active.clear();
        self.gateway_transport_ready.notify_all();
    }
}

#[cfg(test)]
mod transport_lease_tests {
    use crate::core::ClientCore;
    use std::sync::Arc;

    #[test]
    fn transition_waits_for_existing_readers_and_fences_later_readers_in_reservation_order() {
        let core = ClientCore::new();
        let first = core.reserve_gateway_transport(false).unwrap();
        let second = core.reserve_gateway_transport(false).unwrap();
        assert!(core.wait_gateway_transport(first));
        assert!(core.wait_gateway_transport(second));
        let writer = core.reserve_gateway_transport(true).unwrap();
        let later = core.reserve_gateway_transport(false).unwrap();
        assert_eq!(
            core.gateway_transport_leases
                .lock()
                .unwrap()
                .pending
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![writer, later]
        );
        assert!(core.release_gateway_transport(first));
        assert!(!core.release_gateway_transport(first));
        assert!(core.release_gateway_transport(second));
        assert!(core.wait_gateway_transport(writer));
        assert!(
            !core
                .gateway_transport_leases
                .lock()
                .unwrap()
                .active
                .contains_key(&later)
        );
        assert!(core.release_gateway_transport(writer));
        assert!(core.wait_gateway_transport(later));
        assert!(core.release_gateway_transport(later));
    }

    #[test]
    fn cancelled_reservation_unblocks_successors_and_shutdown_releases_waiters() {
        let core = Arc::new(ClientCore::new());
        let reader = core.reserve_gateway_transport(false).unwrap();
        let writer = core.reserve_gateway_transport(true).unwrap();
        let later = core.reserve_gateway_transport(false).unwrap();
        assert!(core.release_gateway_transport(writer));
        assert!(!core.wait_gateway_transport(writer));
        assert!(core.wait_gateway_transport(later));
        let blocked = core.reserve_gateway_transport(true).unwrap();
        let waiter = {
            let core = core.clone();
            std::thread::spawn(move || core.wait_gateway_transport(blocked))
        };
        core.shutdown();
        assert!(!waiter.join().unwrap());
        assert!(!core.release_gateway_transport(reader));
        assert!(core.reserve_gateway_transport(false).is_err());
    }
}
