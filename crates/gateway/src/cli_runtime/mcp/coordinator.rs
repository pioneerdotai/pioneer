use super::grants::{
    CliMcpBoundGrant, CliMcpConnectionId, CliMcpGrantError, CliMcpGrantId, CliMcpGrantRef,
    CliMcpGrantRegistryState, CliMcpGrantScope, CliMcpIssuedGrant,
};
use crate::cli_runtime::session_instance::CliSessionInstanceId;
use pioneer_cli_mcp_bridge::AttachRequest;
use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CliMcpProjectionFingerprint(String);

impl CliMcpProjectionFingerprint {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, CliMcpCoordinatorError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CliMcpCoordinatorError::InvalidIdentity);
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CliMcpProjectionGeneration(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CliMcpActivationGeneration(u64);

impl CliMcpActivationGeneration {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// Readiness belongs to a provider projection generation, not an active turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMcpProjectionReadiness {
    Preparing,
    Ready,
    Active,
}

/// This enum intentionally has exactly the three states from Proposal 53.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMcpActiveTurnState {
    Preparing,
    Active,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMcpCoordinatorError {
    Grant(CliMcpGrantError),
    InvalidIdentity,
    MissingProjection,
    MissingTurn,
    StaleProjectionGeneration,
    StaleActivationGeneration,
    ProjectionFingerprintMismatch,
    InvalidTransition,
    CallsNotActive,
    GenerationExhausted,
}

impl From<CliMcpGrantError> for CliMcpCoordinatorError {
    fn from(value: CliMcpGrantError) -> Self {
        Self::Grant(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliMcpProjectionReservation {
    pub(crate) generation: CliMcpProjectionGeneration,
    pub(crate) fingerprint: CliMcpProjectionFingerprint,
}

pub(crate) struct CliMcpTurnReservation {
    pub(crate) activation_generation: CliMcpActivationGeneration,
    #[cfg(test)]
    pub(crate) cancellation: CancellationToken,
}

impl fmt::Debug for CliMcpTurnReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliMcpTurnReservation")
            .field("activation_generation", &self.activation_generation)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliMcpListAuthorization {
    pub(crate) grant_id: CliMcpGrantId,
    pub(crate) projection_generation: CliMcpProjectionGeneration,
    pub(crate) fingerprint: CliMcpProjectionFingerprint,
    pub(crate) readiness: CliMcpProjectionReadiness,
}

#[derive(Clone)]
pub(crate) struct CliMcpCallAuthorization {
    pub(crate) grant_id: CliMcpGrantId,
    pub(crate) projection_generation: CliMcpProjectionGeneration,
    pub(crate) activation_generation: CliMcpActivationGeneration,
    pub(crate) fingerprint: CliMcpProjectionFingerprint,
    pub(crate) turn_id: String,
    pub(crate) native_thread_id: String,
    pub(crate) native_turn_id: String,
    pub(crate) cancellation: CancellationToken,
}

impl fmt::Debug for CliMcpCallAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliMcpCallAuthorization")
            .field("grant_id", &self.grant_id)
            .field("projection_generation", &self.projection_generation)
            .field("activation_generation", &self.activation_generation)
            .field("fingerprint", &self.fingerprint)
            .field("turn_id", &self.turn_id)
            .field("native_thread_id", &self.native_thread_id)
            .field("native_turn_id", &self.native_turn_id)
            .field("cancellation", &"[RUNTIME STATE]")
            .finish()
    }
}

impl PartialEq for CliMcpCallAuthorization {
    fn eq(&self, other: &Self) -> bool {
        self.grant_id == other.grant_id
            && self.projection_generation == other.projection_generation
            && self.activation_generation == other.activation_generation
            && self.fingerprint == other.fingerprint
            && self.turn_id == other.turn_id
            && self.native_thread_id == other.native_thread_id
            && self.native_turn_id == other.native_turn_id
    }
}

impl Eq for CliMcpCallAuthorization {}

struct ProjectionGenerationState {
    grant_id: CliMcpGrantId,
    connection_id: Option<CliMcpConnectionId>,
    process_instance: CliSessionInstanceId,
    generation: CliMcpProjectionGeneration,
    fingerprint: CliMcpProjectionFingerprint,
    readiness: CliMcpProjectionReadiness,
    readiness_signal: std::sync::Arc<Notify>,
}

struct CliMcpActiveTurn {
    grant_id: CliMcpGrantId,
    process_instance: CliSessionInstanceId,
    projection_generation: CliMcpProjectionGeneration,
    activation_generation: CliMcpActivationGeneration,
    turn_id: String,
    native_thread_id: Option<String>,
    native_turn_id: Option<String>,
    state: CliMcpActiveTurnState,
    cancellation: CancellationToken,
}

struct CoordinatorState {
    grants: CliMcpGrantRegistryState,
    projections: HashMap<CliMcpGrantId, ProjectionGenerationState>,
    active_turns: HashMap<CliMcpGrantId, CliMcpActiveTurn>,
    next_projection_generation: u64,
    next_activation_generation: u64,
}

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            grants: CliMcpGrantRegistryState::default(),
            projections: HashMap::new(),
            active_turns: HashMap::new(),
            next_projection_generation: 1,
            next_activation_generation: 1,
        }
    }
}

#[derive(Default)]
pub(crate) struct CliMcpCoordinator {
    state: Mutex<CoordinatorState>,
}

impl CliMcpCoordinator {
    pub(crate) async fn issue_grant(
        &self,
        scope: CliMcpGrantScope,
        expires_at_unix_ms: u64,
    ) -> Result<CliMcpIssuedGrant, CliMcpCoordinatorError> {
        let mut state = self.state.lock().await;
        Ok(state
            .grants
            .issue(scope, expires_at_unix_ms, now_unix_ms())?)
    }

    pub(crate) async fn attach(
        &self,
        request: &AttachRequest,
        scope: &CliMcpGrantScope,
        connection_id: CliMcpConnectionId,
    ) -> Result<CliMcpBoundGrant, CliMcpCoordinatorError> {
        let mut state = self.state.lock().await;
        let bound = state.grants.attach(
            &request.session_id,
            request.generation,
            scope,
            &request.nonce,
            connection_id,
            now_unix_ms(),
        )?;
        if let Some(projection) = state.projections.get_mut(&bound.grant_id()) {
            projection.connection_id = Some(connection_id);
        }
        Ok(bound)
    }

    pub(crate) async fn stage_projection(
        &self,
        grant_ref: &CliMcpGrantRef,
        fingerprint: CliMcpProjectionFingerprint,
    ) -> Result<CliMcpProjectionReservation, CliMcpCoordinatorError> {
        let mut state = self.state.lock().await;
        state.grants.validate_ref(grant_ref, now_unix_ms())?;
        if let Some(existing) = state.projections.get(&grant_ref.grant_id()) {
            if existing.fingerprint == fingerprint {
                return Ok(CliMcpProjectionReservation {
                    generation: existing.generation,
                    fingerprint,
                });
            }
            // A grant is fixed to one manifest/projection generation. A
            // changed fingerprint requires process replacement and a new
            // one-use grant; it cannot be restaged on a consumed connection.
            return Err(CliMcpCoordinatorError::InvalidTransition);
        }
        let generation = next_projection_generation(&mut state)?;
        state.projections.insert(
            grant_ref.grant_id(),
            ProjectionGenerationState {
                grant_id: grant_ref.grant_id(),
                connection_id: None,
                process_instance: grant_ref.scope().process_instance.clone(),
                generation,
                fingerprint: fingerprint.clone(),
                readiness: CliMcpProjectionReadiness::Preparing,
                readiness_signal: std::sync::Arc::new(Notify::new()),
            },
        );
        Ok(CliMcpProjectionReservation {
            generation,
            fingerprint,
        })
    }

    pub(crate) async fn reserve_turn(
        &self,
        grant_ref: &CliMcpGrantRef,
        projection_generation: CliMcpProjectionGeneration,
        turn_id: impl Into<String>,
    ) -> Result<CliMcpTurnReservation, CliMcpCoordinatorError> {
        let turn_id = normalize_identity(turn_id.into())?;
        let mut state = self.state.lock().await;
        state.grants.validate_ref(grant_ref, now_unix_ms())?;
        let projection = state
            .projections
            .get(&grant_ref.grant_id())
            .ok_or(CliMcpCoordinatorError::MissingProjection)?;
        if projection.generation != projection_generation {
            return Err(CliMcpCoordinatorError::StaleProjectionGeneration);
        }
        if state
            .active_turns
            .get(&grant_ref.grant_id())
            .is_some_and(|turn| turn.state != CliMcpActiveTurnState::Terminal)
        {
            return Err(CliMcpCoordinatorError::InvalidTransition);
        }
        let activation_generation = next_activation_generation(&mut state)?;
        let cancellation = CancellationToken::new();
        #[cfg(test)]
        let reservation_cancellation = cancellation.clone();
        state.active_turns.insert(
            grant_ref.grant_id(),
            CliMcpActiveTurn {
                grant_id: grant_ref.grant_id(),
                process_instance: grant_ref.scope().process_instance.clone(),
                projection_generation,
                activation_generation,
                turn_id,
                native_thread_id: None,
                native_turn_id: None,
                state: CliMcpActiveTurnState::Preparing,
                cancellation,
            },
        );
        Ok(CliMcpTurnReservation {
            activation_generation,
            #[cfg(test)]
            cancellation: reservation_cancellation,
        })
    }

    pub(crate) async fn mark_projection_ready(
        &self,
        bound: &CliMcpBoundGrant,
        projection_generation: CliMcpProjectionGeneration,
        observed_fingerprint: &CliMcpProjectionFingerprint,
    ) -> Result<(), CliMcpCoordinatorError> {
        let mut state = self.state.lock().await;
        state.grants.validate_bound(bound, now_unix_ms())?;
        let projection = state
            .projections
            .get_mut(&bound.grant_id())
            .ok_or(CliMcpCoordinatorError::MissingProjection)?;
        validate_projection_binding(projection, bound, projection_generation)?;
        if projection.fingerprint != *observed_fingerprint {
            return Err(CliMcpCoordinatorError::ProjectionFingerprintMismatch);
        }
        let signal = projection.readiness_signal.clone();
        match projection.readiness {
            CliMcpProjectionReadiness::Preparing | CliMcpProjectionReadiness::Ready => {
                projection.readiness = CliMcpProjectionReadiness::Ready;
                signal.notify_waiters();
                Ok(())
            }
            CliMcpProjectionReadiness::Active => Err(CliMcpCoordinatorError::InvalidTransition),
        }
    }

    pub(crate) async fn wait_projection_ready(
        &self,
        bound: &CliMcpBoundGrant,
        projection_generation: CliMcpProjectionGeneration,
        expected_fingerprint: &CliMcpProjectionFingerprint,
    ) -> Result<(), CliMcpCoordinatorError> {
        loop {
            let notified = {
                let state = self.state.lock().await;
                state.grants.validate_bound(bound, now_unix_ms())?;
                let projection = state
                    .projections
                    .get(&bound.grant_id())
                    .ok_or(CliMcpCoordinatorError::MissingProjection)?;
                validate_projection_binding(projection, bound, projection_generation)?;
                if projection.fingerprint != *expected_fingerprint {
                    return Err(CliMcpCoordinatorError::ProjectionFingerprintMismatch);
                }
                if matches!(
                    projection.readiness,
                    CliMcpProjectionReadiness::Ready | CliMcpProjectionReadiness::Active
                ) {
                    return Ok(());
                }
                projection.readiness_signal.clone().notified_owned()
            };
            notified.await;
        }
    }

    pub(crate) async fn authorize_list(
        &self,
        bound: &CliMcpBoundGrant,
        projection_generation: CliMcpProjectionGeneration,
    ) -> Result<CliMcpListAuthorization, CliMcpCoordinatorError> {
        let state = self.state.lock().await;
        state.grants.validate_bound(bound, now_unix_ms())?;
        let projection = state
            .projections
            .get(&bound.grant_id())
            .ok_or(CliMcpCoordinatorError::MissingProjection)?;
        validate_projection_binding(projection, bound, projection_generation)?;
        Ok(CliMcpListAuthorization {
            grant_id: projection.grant_id,
            projection_generation: projection.generation,
            fingerprint: projection.fingerprint.clone(),
            readiness: projection.readiness,
        })
    }

    pub(crate) async fn activate_turn(
        &self,
        bound: &CliMcpBoundGrant,
        activation_generation: CliMcpActivationGeneration,
        native_thread_id: impl Into<String>,
        native_turn_id: impl Into<String>,
    ) -> Result<(), CliMcpCoordinatorError> {
        let native_thread_id = normalize_identity(native_thread_id.into())?;
        let native_turn_id = normalize_identity(native_turn_id.into())?;
        let mut state = self.state.lock().await;
        state.grants.validate_bound(bound, now_unix_ms())?;
        let (projection_generation, process_instance) = {
            let turn = state
                .active_turns
                .get(&bound.grant_id())
                .ok_or(CliMcpCoordinatorError::MissingTurn)?;
            validate_turn_binding(turn, bound, activation_generation)?;
            if turn.state != CliMcpActiveTurnState::Preparing {
                return Err(CliMcpCoordinatorError::InvalidTransition);
            }
            (turn.projection_generation, turn.process_instance.clone())
        };
        let projection = state
            .projections
            .get_mut(&bound.grant_id())
            .ok_or(CliMcpCoordinatorError::MissingProjection)?;
        if projection.generation != projection_generation
            || projection.process_instance != process_instance
            || projection.readiness != CliMcpProjectionReadiness::Ready
        {
            return Err(CliMcpCoordinatorError::InvalidTransition);
        }
        projection.readiness = CliMcpProjectionReadiness::Active;
        let turn = state
            .active_turns
            .get_mut(&bound.grant_id())
            .ok_or(CliMcpCoordinatorError::MissingTurn)?;
        turn.native_thread_id = Some(native_thread_id);
        turn.native_turn_id = Some(native_turn_id);
        turn.state = CliMcpActiveTurnState::Active;
        Ok(())
    }

    pub(crate) async fn authorize_call(
        &self,
        bound: &CliMcpBoundGrant,
        activation_generation: CliMcpActivationGeneration,
    ) -> Result<CliMcpCallAuthorization, CliMcpCoordinatorError> {
        let state = self.state.lock().await;
        state.grants.validate_bound(bound, now_unix_ms())?;
        let turn = state
            .active_turns
            .get(&bound.grant_id())
            .ok_or(CliMcpCoordinatorError::MissingTurn)?;
        validate_turn_binding(turn, bound, activation_generation)?;
        if turn.state != CliMcpActiveTurnState::Active {
            return Err(CliMcpCoordinatorError::CallsNotActive);
        }
        let projection = state
            .projections
            .get(&bound.grant_id())
            .ok_or(CliMcpCoordinatorError::MissingProjection)?;
        validate_projection_binding(projection, bound, turn.projection_generation)?;
        if projection.readiness != CliMcpProjectionReadiness::Active {
            return Err(CliMcpCoordinatorError::CallsNotActive);
        }
        Ok(CliMcpCallAuthorization {
            grant_id: turn.grant_id,
            projection_generation: projection.generation,
            activation_generation: turn.activation_generation,
            fingerprint: projection.fingerprint.clone(),
            turn_id: turn.turn_id.clone(),
            native_thread_id: turn
                .native_thread_id
                .clone()
                .ok_or(CliMcpCoordinatorError::CallsNotActive)?,
            native_turn_id: turn
                .native_turn_id
                .clone()
                .ok_or(CliMcpCoordinatorError::CallsNotActive)?,
            cancellation: turn.cancellation.clone(),
        })
    }

    pub(crate) async fn terminal_turn(
        &self,
        bound: &CliMcpBoundGrant,
        activation_generation: CliMcpActivationGeneration,
    ) -> Result<(), CliMcpCoordinatorError> {
        let mut state = self.state.lock().await;
        state.grants.validate_bound(bound, now_unix_ms())?;
        let projection_generation = {
            let turn = state
                .active_turns
                .get_mut(&bound.grant_id())
                .ok_or(CliMcpCoordinatorError::MissingTurn)?;
            validate_turn_binding(turn, bound, activation_generation)?;
            if turn.state == CliMcpActiveTurnState::Terminal {
                return Ok(());
            }
            turn.cancellation.cancel();
            turn.state = CliMcpActiveTurnState::Terminal;
            turn.projection_generation
        };
        if let Some(projection) = state.projections.get_mut(&bound.grant_id())
            && projection.generation == projection_generation
            && projection.readiness == CliMcpProjectionReadiness::Active
        {
            projection.readiness = CliMcpProjectionReadiness::Ready;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn revoke_connection(
        &self,
        bound: &CliMcpBoundGrant,
    ) -> Result<(), CliMcpCoordinatorError> {
        let mut state = self.state.lock().await;
        state.grants.revoke_bound(bound)?;
        if let Some(turn) = state.active_turns.remove(&bound.grant_id()) {
            turn.cancellation.cancel();
        }
        if let Some(projection) = state.projections.remove(&bound.grant_id()) {
            projection.readiness_signal.notify_waiters();
        }
        Ok(())
    }

    pub(crate) async fn revoke_grant(
        &self,
        grant_ref: &CliMcpGrantRef,
    ) -> Result<(), CliMcpCoordinatorError> {
        let mut state = self.state.lock().await;
        state.grants.revoke_ref(grant_ref)?;
        if let Some(turn) = state.active_turns.remove(&grant_ref.grant_id()) {
            turn.cancellation.cancel();
        }
        if let Some(projection) = state.projections.remove(&grant_ref.grant_id()) {
            projection.readiness_signal.notify_waiters();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn revoke_process(&self, process_instance: &CliSessionInstanceId) -> usize {
        let mut state = self.state.lock().await;
        let grant_ids = state
            .projections
            .iter()
            .filter_map(|(grant_id, projection)| {
                (&projection.process_instance == process_instance).then_some(*grant_id)
            })
            .collect::<Vec<_>>();
        for grant_id in &grant_ids {
            if let Some(turn) = state.active_turns.remove(grant_id) {
                turn.cancellation.cancel();
            }
            if let Some(projection) = state.projections.remove(grant_id) {
                projection.readiness_signal.notify_waiters();
            }
        }
        state.grants.revoke_process(process_instance)
    }

    pub(crate) async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        state.grants.revoke_all();
        for (_, turn) in state.active_turns.drain() {
            turn.cancellation.cancel();
        }
        for (_, projection) in state.projections.drain() {
            projection.readiness_signal.notify_waiters();
        }
    }
}

fn validate_projection_binding(
    projection: &ProjectionGenerationState,
    bound: &CliMcpBoundGrant,
    expected_generation: CliMcpProjectionGeneration,
) -> Result<(), CliMcpCoordinatorError> {
    if projection.generation != expected_generation {
        return Err(CliMcpCoordinatorError::StaleProjectionGeneration);
    }
    if projection.grant_id != bound.grant_id()
        || projection.connection_id != Some(bound.connection_id())
        || projection.process_instance != bound.scope().process_instance
    {
        return Err(CliMcpCoordinatorError::Grant(
            CliMcpGrantError::WrongConnection,
        ));
    }
    Ok(())
}

fn validate_turn_binding(
    turn: &CliMcpActiveTurn,
    bound: &CliMcpBoundGrant,
    expected_generation: CliMcpActivationGeneration,
) -> Result<(), CliMcpCoordinatorError> {
    if turn.activation_generation != expected_generation {
        return Err(CliMcpCoordinatorError::StaleActivationGeneration);
    }
    if turn.grant_id != bound.grant_id() || turn.process_instance != bound.scope().process_instance
    {
        return Err(CliMcpCoordinatorError::Grant(
            CliMcpGrantError::WrongConnection,
        ));
    }
    Ok(())
}

fn next_projection_generation(
    state: &mut CoordinatorState,
) -> Result<CliMcpProjectionGeneration, CliMcpCoordinatorError> {
    let current = state.next_projection_generation;
    state.next_projection_generation = current
        .checked_add(1)
        .ok_or(CliMcpCoordinatorError::GenerationExhausted)?;
    Ok(CliMcpProjectionGeneration(current))
}

fn next_activation_generation(
    state: &mut CoordinatorState,
) -> Result<CliMcpActivationGeneration, CliMcpCoordinatorError> {
    let current = state.next_activation_generation;
    state.next_activation_generation = current
        .checked_add(1)
        .ok_or(CliMcpCoordinatorError::GenerationExhausted)?;
    Ok(CliMcpActivationGeneration(current))
}

fn normalize_identity(value: String) -> Result<String, CliMcpCoordinatorError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > 512 || trimmed.chars().any(char::is_control) {
        return Err(CliMcpCoordinatorError::InvalidIdentity);
    }
    Ok(trimmed.to_owned())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
    use crate::cli_runtime::mcp::grants::{CliMcpGrantError, CliMcpManifestHash};
    use pioneer_cli_mcp_bridge::BridgeGeneration;

    fn instance(generation: u64, thread_id: &str) -> CliSessionInstanceId {
        CliSessionInstanceId::unmanaged_for_test(
            CLIAgentRuntimeSessionKey::new("workspace", "codex", thread_id).expect("key"),
            generation,
        )
        .expect("instance")
    }

    fn scope(generation: u64, thread_id: &str, manifest: u8) -> CliMcpGrantScope {
        CliMcpGrantScope::new(
            instance(generation, thread_id),
            CliMcpManifestHash::new(char::from(manifest).to_string().repeat(64)).expect("manifest"),
        )
    }

    fn attach_request(issued: &CliMcpIssuedGrant) -> AttachRequest {
        AttachRequest {
            session_id: issued.bridge_session_id.clone(),
            generation: BridgeGeneration::new(issued.scope.process_instance.generation())
                .expect("generation"),
            nonce: issued.nonce.clone(),
        }
    }

    async fn issue(coordinator: &CliMcpCoordinator, scope: CliMcpGrantScope) -> CliMcpIssuedGrant {
        coordinator
            .issue_grant(scope, now_unix_ms().saturating_add(60_000))
            .await
            .expect("grant")
    }

    #[tokio::test]
    async fn cli_mcp_grants_reject_cross_scope_replay_and_stale_generation() {
        let coordinator = CliMcpCoordinator::default();
        let exact_scope = scope(1, "thread-a", b'a');
        let issued = issue(&coordinator, exact_scope.clone()).await;
        let request = attach_request(&issued);

        let cross_scope = scope(2, "thread-b", b'b');
        assert_eq!(
            coordinator
                .attach(&request, &cross_scope, CliMcpConnectionId::for_test(1),)
                .await,
            Err(CliMcpCoordinatorError::Grant(CliMcpGrantError::CrossScope))
        );

        let mut stale = attach_request(&issued);
        stale.generation = BridgeGeneration::new(2).expect("generation");
        assert_eq!(
            coordinator
                .attach(&stale, &exact_scope, CliMcpConnectionId::for_test(1),)
                .await,
            Err(CliMcpCoordinatorError::Grant(
                CliMcpGrantError::StaleGeneration
            ))
        );

        coordinator
            .attach(&request, &exact_scope, CliMcpConnectionId::for_test(1))
            .await
            .expect("first attach");
        assert_eq!(
            coordinator
                .attach(&request, &exact_scope, CliMcpConnectionId::for_test(2),)
                .await,
            Err(CliMcpCoordinatorError::Grant(CliMcpGrantError::Replay))
        );
    }

    #[tokio::test]
    async fn cli_mcp_activation_requires_ready_projection_and_exact_native_binding() {
        let coordinator = CliMcpCoordinator::default();
        let exact_scope = scope(1, "thread-a", b'a');
        let issued = issue(&coordinator, exact_scope.clone()).await;
        let grant_ref = issued.grant_ref();
        let fingerprint = CliMcpProjectionFingerprint::new("c".repeat(64)).expect("fingerprint");
        let projection = coordinator
            .stage_projection(&grant_ref, fingerprint.clone())
            .await
            .expect("projection");
        let turn = coordinator
            .reserve_turn(&grant_ref, projection.generation, "turn-1")
            .await
            .expect("turn");
        let bound = coordinator
            .attach(
                &attach_request(&issued),
                &exact_scope,
                CliMcpConnectionId::for_test(1),
            )
            .await
            .expect("attach");

        let list = coordinator
            .authorize_list(&bound, projection.generation)
            .await
            .expect("preparing list access");
        assert_eq!(list.readiness, CliMcpProjectionReadiness::Preparing);
        assert_eq!(
            coordinator
                .authorize_call(&bound, turn.activation_generation)
                .await,
            Err(CliMcpCoordinatorError::CallsNotActive)
        );
        coordinator
            .mark_projection_ready(&bound, projection.generation, &fingerprint)
            .await
            .expect("ready");

        let (first, second) = tokio::join!(
            coordinator.activate_turn(
                &bound,
                turn.activation_generation,
                "native-thread",
                "native-turn"
            ),
            coordinator.activate_turn(
                &bound,
                turn.activation_generation,
                "native-thread",
                "native-turn"
            )
        );
        assert!(first.is_ok() ^ second.is_ok(), "CAS permits one activation");
        let authorization = coordinator
            .authorize_call(&bound, turn.activation_generation)
            .await
            .expect("active call");
        assert_eq!(authorization.turn_id, "turn-1");
        assert_eq!(authorization.native_turn_id, "native-turn");

        coordinator
            .terminal_turn(&bound, turn.activation_generation)
            .await
            .expect("terminal");
        assert!(turn.cancellation.is_cancelled());
        assert_eq!(
            coordinator
                .authorize_call(&bound, turn.activation_generation)
                .await,
            Err(CliMcpCoordinatorError::CallsNotActive)
        );
        assert_eq!(
            coordinator
                .authorize_list(&bound, projection.generation)
                .await
                .expect("list remains ready")
                .readiness,
            CliMcpProjectionReadiness::Ready
        );
    }

    #[tokio::test]
    async fn stale_process_event_cannot_revoke_current_generation_or_survive_restart() {
        let coordinator = CliMcpCoordinator::default();
        let old_scope = scope(1, "thread-a", b'a');
        let new_scope = scope(2, "thread-a", b'a');
        let old = issue(&coordinator, old_scope.clone()).await;
        let new = issue(&coordinator, new_scope.clone()).await;
        let old_projection = coordinator
            .stage_projection(
                &old.grant_ref(),
                CliMcpProjectionFingerprint::new("d".repeat(64)).expect("fingerprint"),
            )
            .await
            .expect("old projection");
        let new_projection = coordinator
            .stage_projection(
                &new.grant_ref(),
                CliMcpProjectionFingerprint::new("e".repeat(64)).expect("fingerprint"),
            )
            .await
            .expect("new projection");
        let old_bound = coordinator
            .attach(
                &attach_request(&old),
                &old_scope,
                CliMcpConnectionId::for_test(1),
            )
            .await
            .expect("old attach");
        let new_bound = coordinator
            .attach(
                &attach_request(&new),
                &new_scope,
                CliMcpConnectionId::for_test(2),
            )
            .await
            .expect("new attach");

        assert_eq!(
            coordinator
                .revoke_process(&old_scope.process_instance)
                .await,
            1
        );
        assert!(
            coordinator
                .authorize_list(&old_bound, old_projection.generation)
                .await
                .is_err()
        );
        coordinator
            .authorize_list(&new_bound, new_projection.generation)
            .await
            .expect("new generation remains valid");

        let restarted = CliMcpCoordinator::default();
        assert_eq!(
            restarted
                .authorize_list(&new_bound, new_projection.generation)
                .await,
            Err(CliMcpCoordinatorError::Grant(
                CliMcpGrantError::UnknownGrant
            ))
        );
    }

    #[tokio::test]
    async fn cli_mcp_grants_revoke_connection_immediately_cancels_turn() {
        let coordinator = CliMcpCoordinator::default();
        let exact_scope = scope(1, "thread-a", b'a');
        let issued = issue(&coordinator, exact_scope.clone()).await;
        let grant_ref = issued.grant_ref();
        let projection = coordinator
            .stage_projection(
                &grant_ref,
                CliMcpProjectionFingerprint::new("f".repeat(64)).expect("fingerprint"),
            )
            .await
            .expect("projection");
        let turn = coordinator
            .reserve_turn(&grant_ref, projection.generation, "turn-1")
            .await
            .expect("turn");
        let bound = coordinator
            .attach(
                &attach_request(&issued),
                &exact_scope,
                CliMcpConnectionId::for_test(1),
            )
            .await
            .expect("attach");
        coordinator.revoke_connection(&bound).await.expect("revoke");
        assert!(turn.cancellation.is_cancelled());
        assert!(
            coordinator
                .authorize_list(&bound, projection.generation)
                .await
                .is_err()
        );
    }
}
