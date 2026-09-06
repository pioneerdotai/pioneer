//! Serialized process transport ownership and coalesced session requests.
use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex, Weak},
    time::Duration,
};

use anyhow::Result;
use pioneer_protocol::{AuthMeResponse, AuthRefreshGrant, AuthSessionId};

use super::session_controller::SessionDiagnosticStage;
use super::{
    endpoint::GatewayBaseUrl,
    session_lifecycle::{GatewaySessionMetadata, SessionLifecycleEvent, SessionTerminalReason},
    session_refresh::{
        GatewaySessionPreparation, GatewaySessionReady, GatewaySessionRefreshRequest,
        GatewaySessionStorage,
    },
};
use crate::{
    core::ClientCore,
    transport::ws::{
        GatewayWsSessionSpec,
        auth_exchange::{AuthExchangeClient, AuthExchangeError, AuthExchangeErrorKind},
    },
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GatewaySessionConnectionResult {
    pub connection_id: u64,
    pub connection_generation: u64,
    pub metadata: GatewaySessionMetadata,
    pub access_expires_at_unix: u64,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GatewaySessionConnectionFailure {
    Terminal { reason: SessionTerminalReason },
    Suspended,
    Unavailable { code: String },
}
impl std::fmt::Display for GatewaySessionConnectionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Terminal { reason } => write!(formatter, "Gateway session stopped: {reason:?}"),
            Self::Suspended => formatter.write_str("Gateway session was suspended"),
            Self::Unavailable { code } => formatter.write_str(code),
        }
    }
}
impl std::error::Error for GatewaySessionConnectionFailure {}
type ConnectionOutcome =
    std::result::Result<GatewaySessionConnectionResult, GatewaySessionConnectionFailure>;

pub(crate) struct GatewaySessionConnectionFlight {
    epoch: u64,
    result: Mutex<Option<ConnectionOutcome>>,
    ready: Condvar,
}

#[derive(Default)]
pub(crate) struct GatewaySessionConnectionState {
    epoch: u64,
    pending: bool,
    refresh_requested: bool,
    retry_attempt: u32,
    retry_delay_ms: Option<u64>,
    flight: Weak<GatewaySessionConnectionFlight>,
    ready: Option<GatewaySessionReady>,
    candidate_connection_id: Option<u64>,
    connected: Option<GatewaySessionConnectionResult>,
    failure: Option<GatewaySessionConnectionFailure>,
}

impl GatewaySessionConnectionState {
    pub(crate) fn transport_verified(&self, connection_id: u64) -> bool {
        self.connected
            .as_ref()
            .is_some_and(|connected| connected.connection_id == connection_id)
    }

    pub(crate) fn invalidate(&mut self) {
        self.pending = true;
        self.cancel_pending_request();
        self.connected = None;
    }

    pub(crate) fn cancel_pending_request(&mut self) {
        if self.flight.strong_count() == 0
            && !self.pending
            && self.candidate_connection_id.is_none()
        {
            return;
        }
        self.epoch = self
            .epoch
            .checked_add(1)
            .expect("session lifecycle epoch exhausted");
        self.pending = false;
        self.ready = None;
        self.candidate_connection_id = None;
        self.refresh_requested = false;
        self.retry_delay_ms = None;
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct GatewaySessionConnectionProjection {
    pub epoch: u64,
    pub pending: bool,
    pub refresh_requested: bool,
    pub retry_delay_ms: Option<u64>,
    pub connected: Option<GatewaySessionConnectionResult>,
    pub failure: Option<GatewaySessionConnectionFailure>,
}

pub(crate) fn project_connections(
    connections: &BTreeMap<String, GatewaySessionConnectionState>,
) -> BTreeMap<String, GatewaySessionConnectionProjection> {
    connections
        .iter()
        .map(|(id, state)| {
            (
                id.clone(),
                GatewaySessionConnectionProjection {
                    epoch: state.epoch,
                    pending: state.pending,
                    refresh_requested: state.refresh_requested,
                    retry_delay_ms: state.retry_delay_ms,
                    connected: state.connected.clone(),
                    failure: state.failure.clone(),
                },
            )
        })
        .collect()
}

impl ClientCore {
    /// Starts a prepared native transport under the process transport lease.
    /// Retrying startup returns its transport identity before readiness; identity
    /// verification is a separate Client operation after the transport connects.
    pub fn start_gateway_session_transport(
        &self,
        spec: crate::transport::ws::GatewayWsConnectSpec,
        retry_initial_failure: bool,
    ) -> Result<u64> {
        let sender = self.compatibility_runtime().ws_command_sender();
        self.start_gateway_session_transport_with_port(
            spec,
            retry_initial_failure,
            |spec, retry| {
                if retry {
                    sender.connect_with_retry(spec)
                } else {
                    sender.connect_and_wait(spec)
                }
            },
        )
    }

    fn start_gateway_session_transport_with_port(
        &self,
        spec: crate::transport::ws::GatewayWsConnectSpec,
        retry_initial_failure: bool,
        connect: impl FnOnce(crate::transport::ws::GatewayWsConnectSpec, bool) -> Result<u64>,
    ) -> Result<u64> {
        let endpoint = spec.endpoint_id.clone();
        let session = spec
            .session
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Gateway session identity is missing"))?;
        let access_token = spec
            .auth_token
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Gateway session access is missing"))?;
        let publication = self.gateway_session();
        let (metadata, connection_generation) = match publication.session(&endpoint) {
            Some(super::session_lifecycle::SessionLifecycleState::Connecting {
                metadata,
                connection_generation,
                ..
            })
            | Some(super::session_lifecycle::SessionLifecycleState::Active {
                metadata,
                connection_generation,
                ..
            }) => (metadata.clone(), *connection_generation),
            _ => anyhow::bail!("Gateway session preparation is unavailable"),
        };
        anyhow::ensure!(
            metadata.session_id == session.session_id
                && metadata.gateway_id == session.server_gateway_id,
            "Gateway session preparation does not match transport"
        );
        let mut lease = self
            .session_transport
            .lock()
            .expect("Gateway transport lease poisoned");
        let epoch = {
            let mut sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            anyhow::ensure!(!self.is_stopped(), "Client session runtime is stopped");
            let state = sessions.connections.entry(endpoint.clone()).or_default();
            state.epoch = state
                .epoch
                .checked_add(1)
                .expect("session lifecycle epoch exhausted");
            state.candidate_connection_id = None;
            state.connected = None;
            state.pending = true;
            state.failure = None;
            state.ready = Some(GatewaySessionReady {
                spec: GatewayWsSessionSpec {
                    endpoint_id: spec.endpoint_id.clone(),
                    endpoint_name: spec.endpoint_name.clone(),
                    endpoint_kind: spec.endpoint_kind,
                    gateway_base_url: spec.gateway_base_url.clone(),
                    identity: session,
                    access_token,
                    timings: spec.timings,
                },
                metadata,
                connection_generation,
            });
            let epoch = state.epoch;
            self.publish_gateway_session(&sessions);
            epoch
        };
        let sender = self.compatibility_runtime().ws_command_sender();
        let result = if retry_initial_failure {
            connect(spec, true)
        } else {
            self.observe_session_stage(&endpoint, SessionDiagnosticStage::ConnectAttempt, || {
                connect(spec, false)
            })
        };
        let connection_id = match result {
            Ok(id) => id,
            Err(error) => {
                self.retire_session_connection(&endpoint, false);
                *lease = None;
                return Err(error);
            }
        };
        if !self.session_request_is_current(&endpoint, epoch) {
            let _ = sender.disconnect();
            *lease = None;
            anyhow::bail!("Gateway session request was cancelled");
        }
        if let Some(previous) = lease.replace(endpoint.clone()) {
            if previous != endpoint {
                self.retire_session_connection(&previous, false);
            }
        }
        {
            let mut sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            sessions
                .connections
                .get_mut(&endpoint)
                .expect("prepared session")
                .candidate_connection_id = Some(connection_id);
        }
        Ok(connection_id)
    }

    pub fn verify_gateway_session_identity(
        &self,
        endpoint: &super::types::GatewayEndpoint,
        installation_id: &str,
        client_kind: pioneer_protocol::ClientKind,
    ) -> Result<Option<SessionTerminalReason>> {
        self.with_gateway_session_refresh(&endpoint.id, || {
            self.verify_gateway_session_identity_locked(endpoint, installation_id, client_kind)
        })
    }

    pub fn reject_gateway_session_identity(
        &self,
        endpoint: &str,
        connection_id: u64,
        failure: GatewaySessionConnectionFailure,
    ) {
        let mut sessions = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        let Some(state) = sessions.connections.get_mut(endpoint) else {
            return;
        };
        if self.is_stopped() || state.candidate_connection_id != Some(connection_id) {
            return;
        }
        let Some(ready) = state.ready.clone() else {
            return;
        };
        state.pending = false;
        state.candidate_connection_id = None;
        state.connected = None;
        state.failure = Some(failure.clone());
        state.refresh_requested = matches!(&failure, GatewaySessionConnectionFailure::Unavailable { code } if super::session_lifecycle::auth_code_requires_refresh(code));
        sessions.finish_transport_verification(endpoint, false);
        sessions.observe_transport(&crate::transport::ws::GatewayWsEvent::ConnectFailed {
            connection_id,
            endpoint_id: endpoint.to_owned(),
            endpoint_name: ready.spec.endpoint_name,
            endpoint_kind: ready.spec.endpoint_kind,
            gateway_base_url: ready.spec.gateway_base_url,
            error: failure.to_string(),
        });
        self.publish_gateway_session(&sessions);
        drop(sessions);
        self.invalidate_session_authorization(endpoint);
        if let GatewaySessionConnectionFailure::Terminal { reason } = failure {
            self.reduce_gateway_session_lifecycle(
                endpoint,
                SessionLifecycleEvent::AuthFailed { reason },
            );
        }
        let _ = self
            .compatibility_runtime()
            .ws_command_sender()
            .disconnect_connection(connection_id);
    }

    fn verify_gateway_session_identity_locked(
        &self,
        endpoint: &super::types::GatewayEndpoint,
        installation_id: &str,
        client_kind: pioneer_protocol::ClientKind,
    ) -> Result<Option<SessionTerminalReason>> {
        let _lease = self
            .session_transport
            .lock()
            .expect("Gateway transport lease poisoned");
        let sender = self.compatibility_runtime().ws_command_sender();
        let access = match sender.current_gateway_http_access() {
            Ok(access) => access,
            Err(crate::transport::http::GatewayHttpAuthorityError::Terminal(reason)) => {
                return Ok(Some(reason));
            }
            Err(crate::transport::http::GatewayHttpAuthorityError::TemporarilyUnavailable) => {
                anyhow::bail!("Gateway transport is unavailable")
            }
        };
        let connection_id = access.generation;
        let storage = super::session_refresh::GatewaySessionPlatformStorage(self);
        let result = self.observe_session_stage(
            &endpoint.id,
            SessionDiagnosticStage::IdentityVerify,
            || match self.verify_gateway_session_identity_with_ports(
                endpoint,
                installation_id,
                client_kind,
                access,
                &storage,
                || sender.auth_me(),
            )? {
                None => Ok(()),
                Some(reason) => Err(anyhow::Error::new(
                    GatewaySessionConnectionFailure::Terminal { reason },
                )),
            },
        );
        match result {
            Ok(()) => Ok(None),
            Err(error) => {
                let failure = match error.downcast_ref::<GatewaySessionConnectionFailure>() {
                    Some(failure) => failure.clone(),
                    None => connection_failure(error),
                };
                self.reject_gateway_session_identity(&endpoint.id, connection_id, failure.clone());
                match failure {
                    GatewaySessionConnectionFailure::Terminal { reason } => {
                        let _ = sender.disconnect_connection(connection_id);
                        Ok(Some(reason))
                    }
                    failure => Err(anyhow::Error::new(failure)),
                }
            }
        }
    }

    fn verify_gateway_session_identity_with_ports(
        &self,
        endpoint: &super::types::GatewayEndpoint,
        installation_id: &str,
        client_kind: pioneer_protocol::ClientKind,
        access: crate::transport::http::GatewayHttpAccess,
        storage: &dyn GatewaySessionStorage,
        identity: impl FnOnce() -> Result<AuthMeResponse>,
    ) -> Result<Option<SessionTerminalReason>> {
        let (ready, epoch, verified) = {
            let sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            let state = sessions
                .connections
                .get(&endpoint.id)
                .ok_or_else(|| anyhow::anyhow!("Gateway session request is unavailable"))?;
            (
                state
                    .ready
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("Gateway session preparation is unavailable"))?,
                state.epoch,
                state.transport_verified(access.generation),
            )
        };
        if let Some(reason) = self.gateway_session().terminal_reason(&endpoint.id) {
            return Ok(Some(reason));
        }
        if verified && self.current_auth().is_some() {
            return Ok(None);
        }
        anyhow::ensure!(
            self.session_request_is_current(&endpoint.id, epoch)
                && access.session_id == ready.metadata.session_id
                && access.gateway_id == ready.metadata.gateway_id,
            "Gateway session identity request is stale"
        );
        self.begin_authorization_epoch(Some((endpoint.id.clone(), access.generation)));
        let (identity_generation, http_generation) = self.current_auth_ticket();
        let stored = storage.load(endpoint);
        anyhow::ensure!(
            self.session_request_is_current(&endpoint.id, epoch)
                && self.gateway_http_generation() == http_generation,
            "Gateway session storage response is stale"
        );
        let stored = match stored {
            Ok(Some(stored)) => stored,
            _ => {
                let reason = SessionTerminalReason::SecureStorageFailed;
                self.begin_authorization_epoch(None);
                self.retire_session_connection(&endpoint.id, false);
                self.reduce_gateway_session_lifecycle(
                    &endpoint.id,
                    SessionLifecycleEvent::AuthFailed { reason },
                );
                return Ok(Some(reason));
            }
        };
        let auth = identity()?;
        anyhow::ensure!(
            self.session_request_is_current(&endpoint.id, epoch)
                && self.gateway_http_generation() == http_generation,
            "Gateway session identity response is stale"
        );
        if let Some(reason) = stored.identity_failure(
            endpoint.server_gateway_id.as_ref(),
            installation_id,
            client_kind,
            &auth,
        ) {
            self.begin_authorization_epoch(None);
            self.retire_session_connection(&endpoint.id, false);
            self.reduce_gateway_session_lifecycle(
                &endpoint.id,
                SessionLifecycleEvent::AuthFailed { reason },
            );
            return Ok(Some(reason));
        }
        self.finish_current_auth(identity_generation, http_generation, auth)?;
        let transition = self.reduce_gateway_session_lifecycle(
            &endpoint.id,
            SessionLifecycleEvent::ConnectionEstablished {
                generation: ready.connection_generation,
            },
        );
        anyhow::ensure!(
            matches!(transition.state(), super::session_lifecycle::SessionLifecycleState::Active { connection_generation, .. } if *connection_generation == ready.connection_generation),
            "Gateway session connection result is stale"
        );
        let mut sessions = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        let state = sessions
            .connections
            .get_mut(&endpoint.id)
            .expect("prepared session");
        anyhow::ensure!(
            state.epoch == epoch && !self.is_stopped(),
            "Gateway session connection result is stale"
        );
        state.connected = Some(GatewaySessionConnectionResult {
            connection_id: access.generation,
            connection_generation: ready.connection_generation,
            metadata: ready.metadata,
            access_expires_at_unix: access.access_expires_at_unix,
        });
        state.pending = false;
        state.candidate_connection_id = None;
        state.failure = None;
        sessions.finish_transport_verification(&endpoint.id, true);
        self.publish_gateway_session(&sessions);
        Ok(None)
    }

    pub fn ensure_gateway_session(
        &self,
        request: GatewaySessionRefreshRequest<'_>,
        storage: &dyn GatewaySessionStorage,
    ) -> ConnectionOutcome {
        let sender = self.compatibility_runtime().ws_command_sender();
        let cleanup_timeout = request.timeout;
        self.ensure_gateway_session_with_ports(
            request,
            storage,
            exchange_refresh,
            |base, access, session| cleanup_session(base, access, session, cleanup_timeout),
            |spec| sender.replace_access_and_wait(spec.clone().into_connect_spec()),
            || sender.auth_me(),
            || sender.disconnect(),
        )
    }

    fn ensure_gateway_session_with_ports<F, C, R, I, D>(
        &self,
        request: GatewaySessionRefreshRequest<'_>,
        storage: &dyn GatewaySessionStorage,
        mut refresh: F,
        mut cleanup: C,
        mut replace: R,
        mut identity: I,
        mut disconnect: D,
    ) -> ConnectionOutcome
    where
        F: FnMut(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> std::result::Result<AuthRefreshGrant, AuthExchangeError>,
        C: FnMut(&GatewayBaseUrl, &str, &AuthSessionId) -> Result<()>,
        R: FnMut(&GatewayWsSessionSpec) -> Result<u64>,
        I: FnMut() -> Result<AuthMeResponse>,
        D: FnMut() -> Result<()>,
    {
        let endpoint = &request.endpoint.id;
        let (flight, owner, requested_epoch) = {
            let mut sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            if self.is_stopped() {
                return Err(GatewaySessionConnectionFailure::Suspended);
            }
            let state = sessions.connections.entry(endpoint.clone()).or_default();
            if let Some(flight) = state.flight.upgrade().filter(|flight| {
                flight.epoch == state.epoch
                    || flight
                        .result
                        .lock()
                        .expect("session request result poisoned")
                        .is_none()
            }) {
                (flight, false, state.epoch)
            } else {
                let flight = Arc::new(GatewaySessionConnectionFlight {
                    epoch: state.epoch,
                    result: Mutex::new(None),
                    ready: Condvar::new(),
                });
                state.flight = Arc::downgrade(&flight);
                state.failure = None;
                state.pending = state.connected.is_none()
                    || state.ready.as_ref().is_none_or(|ready| {
                        ready.spec.identity.access_expires_at_unix
                            <= request.now_unix.saturating_add(60)
                    });
                let requested_epoch = state.epoch;
                self.publish_gateway_session(&sessions);
                (flight, true, requested_epoch)
            }
        };
        if !owner {
            let mut result = flight
                .result
                .lock()
                .expect("session request result poisoned");
            while result.is_none() && !self.is_stopped() {
                result = flight
                    .ready
                    .wait_timeout(result, Duration::from_millis(250))
                    .expect("session request wait poisoned")
                    .0;
            }
            let outcome = result
                .clone()
                .unwrap_or(Err(GatewaySessionConnectionFailure::Suspended));
            drop(result);
            let current_epoch = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned")
                .connections
                .get(endpoint)
                .map(|state| state.epoch);
            if requested_epoch != flight.epoch && !self.is_stopped() {
                return self.ensure_gateway_session_with_ports(
                    request, storage, refresh, cleanup, replace, identity, disconnect,
                );
            }
            if current_epoch != Some(flight.epoch)
                && !matches!(&outcome, Err(GatewaySessionConnectionFailure::Terminal { reason })
                    if self.gateway_session().terminal_reason(endpoint) == Some(*reason))
            {
                return Err(GatewaySessionConnectionFailure::Suspended);
            }
            return outcome;
        }
        let mut outcome = self
            .with_gateway_session_refresh(endpoint, || {
                let started = std::time::Instant::now();
                let mut attempt_request = request;
                for attempt in 0..=1 {
                    attempt_request.now_unix =
                        request.now_unix.saturating_add(started.elapsed().as_secs());
                    let outcome = self.connect_gateway_session_request(
                        &attempt_request,
                        flight.epoch,
                        storage,
                        &mut refresh,
                        &mut cleanup,
                        &mut replace,
                        &mut identity,
                        &mut disconnect,
                    );
                    if let Err(GatewaySessionConnectionFailure::Unavailable { code }) = &outcome {
                        if super::session_lifecycle::auth_code_requires_refresh(code) {
                            let mut sessions = self
                                .gateway_session
                                .lock()
                                .expect("Gateway session owner poisoned");
                            if let Some(state) = sessions.connections.get_mut(endpoint) {
                                if state.epoch == flight.epoch {
                                    state.ready = None;
                                    state.refresh_requested = true;
                                }
                            }
                            if attempt == 0 {
                                continue;
                            }
                        }
                    }
                    return Ok(outcome);
                }
                unreachable!("bounded connection retry always returns")
            })
            .unwrap_or_else(|_| Err(GatewaySessionConnectionFailure::Suspended));
        {
            let mut sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            if !self.is_stopped()
                && sessions
                    .connections
                    .get(endpoint)
                    .is_some_and(|state| state.epoch == flight.epoch)
            {
                if let Some(state) = sessions.connections.get_mut(endpoint) {
                    state.pending = false;
                    state.candidate_connection_id = None;
                    state.failure = outcome.as_ref().err().cloned();
                    match &outcome {
                        Ok(connected) => {
                            state.connected = Some(connected.clone());
                            state.retry_attempt = 0;
                            state.retry_delay_ms = None;
                        }
                        Err(GatewaySessionConnectionFailure::Unavailable { .. }) => {
                            let multiplier =
                                1_u32.checked_shl(state.retry_attempt).unwrap_or(u32::MAX);
                            let delay = request
                                .ws_timings
                                .reconnect_initial
                                .saturating_mul(multiplier)
                                .min(request.ws_timings.reconnect_max);
                            state.retry_delay_ms =
                                Some(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
                            state.retry_attempt = state.retry_attempt.saturating_add(1);
                        }
                        Err(_) => {
                            state.retry_delay_ms = None;
                        }
                    }
                    sessions.finish_transport_verification(endpoint, outcome.is_ok());
                    self.publish_gateway_session(&sessions);
                }
            } else if self.is_stopped()
                || !matches!(&outcome, Err(GatewaySessionConnectionFailure::Terminal { reason })
                    if sessions.publication().terminal_reason(endpoint) == Some(*reason))
            {
                outcome = Err(GatewaySessionConnectionFailure::Suspended);
            }
        }
        *flight
            .result
            .lock()
            .expect("session request result poisoned") = Some(outcome.clone());
        flight.ready.notify_all();
        outcome
    }

    fn connect_gateway_session_request<F, C, R, I, D>(
        &self,
        request: &GatewaySessionRefreshRequest<'_>,
        epoch: u64,
        storage: &dyn GatewaySessionStorage,
        refresh: &mut F,
        cleanup: &mut C,
        replace: &mut R,
        identity: &mut I,
        disconnect: &mut D,
    ) -> ConnectionOutcome
    where
        F: FnMut(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> std::result::Result<AuthRefreshGrant, AuthExchangeError>,
        C: FnMut(&GatewayBaseUrl, &str, &AuthSessionId) -> Result<()>,
        R: FnMut(&GatewayWsSessionSpec) -> Result<u64>,
        I: FnMut() -> Result<AuthMeResponse>,
        D: FnMut() -> Result<()>,
    {
        let endpoint = &request.endpoint.id;
        if !self.session_request_is_current(endpoint, epoch) {
            return Err(GatewaySessionConnectionFailure::Suspended);
        }
        if let Some(reason) = self.gateway_session().terminal_reason(endpoint) {
            return Err(GatewaySessionConnectionFailure::Terminal { reason });
        }
        let now = request.now_unix;
        let (cached, connected) = {
            let sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            let state = sessions
                .connections
                .get(endpoint)
                .expect("registered session request");
            (
                if state.refresh_requested {
                    None
                } else {
                    state.ready.clone()
                },
                state.connected.clone(),
            )
        };
        let ready = match cached
            .filter(|ready| ready.spec.identity.access_expires_at_unix > now.saturating_add(60))
        {
            Some(ready) => {
                if let Some(connected) = connected {
                    if connected.connection_generation == ready.connection_generation
                        && self
                            .session_transport
                            .lock()
                            .expect("Gateway transport lease poisoned")
                            .as_deref()
                            == Some(endpoint)
                    {
                        return Ok(connected);
                    }
                }
                ready
            }
            None => match self.prepare_gateway_session_guarded(
                *request,
                storage,
                || self.session_request_is_current(endpoint, epoch),
                refresh,
                cleanup,
            ) {
                Ok(GatewaySessionPreparation::Ready(ready)) => ready,
                Ok(GatewaySessionPreparation::Terminal(terminal)) => {
                    return Err(GatewaySessionConnectionFailure::Terminal {
                        reason: terminal.reason,
                    });
                }
                Err(error) => {
                    return Err(if self.session_request_is_current(endpoint, epoch) {
                        connection_failure(error)
                    } else {
                        GatewaySessionConnectionFailure::Suspended
                    });
                }
            },
        };
        {
            let mut sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            let state = sessions
                .connections
                .get_mut(endpoint)
                .expect("registered session request");
            if self.is_stopped() || state.epoch != epoch {
                return Err(GatewaySessionConnectionFailure::Suspended);
            }
            state.ready = Some(ready.clone());
            state.refresh_requested = false;
        }
        let mut lease = self
            .session_transport
            .lock()
            .expect("Gateway transport lease poisoned");
        if !self.session_request_is_current(endpoint, epoch) {
            return Err(GatewaySessionConnectionFailure::Suspended);
        }
        let connect_started = std::time::Instant::now();
        let connection_id = match self.observe_session_stage(
            endpoint,
            SessionDiagnosticStage::ConnectAttempt,
            || replace(&ready.spec),
        ) {
            Ok(id) => id,
            Err(error) => {
                if !self.session_request_is_current(endpoint, epoch) {
                    return Err(GatewaySessionConnectionFailure::Suspended);
                }
                let failure = connection_failure(error);
                if let GatewaySessionConnectionFailure::Terminal { reason } = failure {
                    self.reduce_gateway_session_lifecycle(
                        endpoint,
                        SessionLifecycleEvent::AuthFailed { reason },
                    );
                    return Err(GatewaySessionConnectionFailure::Terminal { reason });
                }
                let now_unix = request
                    .now_unix
                    .saturating_add(connect_started.elapsed().as_secs());
                let transition = self.reduce_gateway_session_lifecycle(
                    endpoint,
                    SessionLifecycleEvent::ConnectionTransportFailed {
                        generation: ready.connection_generation,
                        now_unix,
                    },
                );
                if matches!(
                    transition.effect(),
                    super::session_lifecycle::SessionLifecycleEffect::BeginRefresh { .. }
                ) {
                    return Err(GatewaySessionConnectionFailure::Unavailable {
                        code: "access_expired".into(),
                    });
                }
                return Err(failure);
            }
        };
        if let Some(previous) = lease.replace(endpoint.clone()) {
            if previous != *endpoint {
                self.retire_session_connection(&previous, false);
            }
        }
        {
            let mut sessions = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned");
            let state = sessions
                .connections
                .get_mut(endpoint)
                .expect("registered session request");
            if self.is_stopped() || state.epoch != epoch {
                drop(sessions);
                let _ = disconnect();
                *lease = None;
                return Err(GatewaySessionConnectionFailure::Suspended);
            }
            state.candidate_connection_id = Some(connection_id);
        }
        self.begin_authorization_epoch(Some((endpoint.clone(), connection_id)));
        let (identity_generation, http_generation) = self.current_auth_ticket();
        let verification =
            self.observe_session_stage(endpoint, SessionDiagnosticStage::IdentityVerify, || {
                if !self.session_request_is_current(endpoint, epoch) {
                    return Err(GatewaySessionConnectionFailure::Suspended);
                }
                let stored = storage
                    .load(request.endpoint)
                    .map_err(connection_failure)?
                    .ok_or(GatewaySessionConnectionFailure::Terminal {
                        reason: SessionTerminalReason::SecureStorageFailed,
                    })?;
                let auth = identity().map_err(connection_failure)?;
                if !self.session_request_is_current(endpoint, epoch) {
                    return Err(GatewaySessionConnectionFailure::Suspended);
                }
                if let Some(reason) = stored.identity_failure(
                    request.endpoint.server_gateway_id.as_ref(),
                    request.installation_id,
                    request.client_kind,
                    &auth,
                ) {
                    return Err(GatewaySessionConnectionFailure::Terminal { reason });
                }
                self.finish_current_auth(identity_generation, http_generation, auth)
                    .map_err(connection_failure)?;
                Ok(())
            });
        if let Err(error) = verification {
            let _ = disconnect();
            *lease = None;
            if !self.session_request_is_current(endpoint, epoch) {
                return Err(GatewaySessionConnectionFailure::Suspended);
            }
            if let GatewaySessionConnectionFailure::Terminal { reason } = error {
                self.reduce_gateway_session_lifecycle(
                    endpoint,
                    SessionLifecycleEvent::AuthFailed { reason },
                );
                return Err(GatewaySessionConnectionFailure::Terminal { reason });
            }
            return Err(error);
        }
        let transition = self.reduce_gateway_session_lifecycle(
            endpoint,
            SessionLifecycleEvent::ConnectionEstablished {
                generation: ready.connection_generation,
            },
        );
        if !matches!(transition.effect(), super::session_lifecycle::SessionLifecycleEffect::SwitchConnection { active_connection_generation, .. } if *active_connection_generation == ready.connection_generation)
        {
            let _ = disconnect();
            *lease = None;
            return Err(GatewaySessionConnectionFailure::Suspended);
        }
        Ok(GatewaySessionConnectionResult {
            connection_id,
            connection_generation: ready.connection_generation,
            metadata: ready.metadata,
            access_expires_at_unix: ready.spec.identity.access_expires_at_unix,
        })
    }

    fn session_request_is_current(&self, endpoint: &str, epoch: u64) -> bool {
        !self.is_stopped()
            && self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned")
                .connections
                .get(endpoint)
                .is_some_and(|state| state.epoch == epoch)
    }

    fn retire_session_connection(&self, endpoint: &str, suspended: bool) {
        let mut sessions = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        let state = sessions.connections.entry(endpoint.to_owned()).or_default();
        state.epoch = state
            .epoch
            .checked_add(1)
            .expect("session lifecycle epoch exhausted");
        state.connected = None;
        state.pending = false;
        state.refresh_requested = false;
        state.retry_attempt = 0;
        state.retry_delay_ms = None;
        state.ready = None;
        state.candidate_connection_id = None;
        state.failure = suspended.then_some(GatewaySessionConnectionFailure::Suspended);
        sessions.finish_transport_verification(endpoint, false);
        self.publish_gateway_session(&sessions);
    }

    pub fn refresh_gateway_session_after_unauthorized(
        &self,
        request: GatewaySessionRefreshRequest<'_>,
        storage: &dyn GatewaySessionStorage,
        rejected_connection_id: u64,
    ) -> ConnectionOutcome {
        {
            let mut lease = self
                .session_transport
                .lock()
                .expect("Gateway transport lease poisoned");
            let matches = self
                .gateway_session
                .lock()
                .expect("Gateway session owner poisoned")
                .connections
                .get(&request.endpoint.id)
                .and_then(|state| state.connected.as_ref())
                .is_some_and(|connected| connected.connection_id == rejected_connection_id);
            if matches && lease.as_deref() == Some(&request.endpoint.id) {
                self.retire_session_connection(&request.endpoint.id, false);
                *lease = None;
                self.compatibility_runtime()
                    .ws_command_sender()
                    .disconnect()
                    .map_err(connection_failure)?;
            }
        }
        self.ensure_gateway_session(request, storage)
    }

    pub fn clear_gateway_session(&self, endpoint: &str) -> Result<()> {
        self.suspend_gateway_session(endpoint)?;
        self.reduce_gateway_session_lifecycle(endpoint, SessionLifecycleEvent::NoStoredSession);
        Ok(())
    }

    pub fn stop_gateway_session(
        &self,
        endpoint: &str,
        reason: SessionTerminalReason,
    ) -> Result<()> {
        self.suspend_gateway_session(endpoint)?;
        self.reduce_gateway_session_lifecycle(
            endpoint,
            SessionLifecycleEvent::AuthFailed { reason },
        );
        Ok(())
    }

    pub fn mark_gateway_session_disconnected(&self, endpoint: &str, connection_id: Option<u64>) {
        let mut sessions = self
            .gateway_session
            .lock()
            .expect("Gateway session owner poisoned");
        let Some(state) = sessions.connections.get_mut(endpoint) else {
            return;
        };
        if state
            .connected
            .as_ref()
            .is_none_or(|connected| connection_id.is_some_and(|id| id != connected.connection_id))
        {
            return;
        }
        state.connected = None;
        self.publish_gateway_session(&sessions);
    }

    pub(crate) fn observe_session_connection(&self, event: &crate::transport::ws::GatewayWsEvent) {
        use crate::transport::ws::GatewayWsEvent;
        use pioneer_protocol::GatewayNotification;
        match event {
            GatewayWsEvent::Connected {
                endpoint_id,
                connection_id,
                ..
            } => {
                let mut sessions = self
                    .gateway_session
                    .lock()
                    .expect("Gateway session owner poisoned");
                if let Some(state) = sessions.connections.get_mut(endpoint_id) {
                    if state.ready.is_some() && !state.transport_verified(*connection_id) {
                        state.candidate_connection_id = Some(*connection_id);
                        state.pending = true;
                    }
                }
            }
            GatewayWsEvent::Disconnected {
                endpoint_id,
                connection_id,
                reason,
                ..
            } => {
                let current = self
                    .gateway_session
                    .lock()
                    .expect("Gateway session owner poisoned")
                    .connections
                    .get(endpoint_id)
                    .and_then(|state| state.connected.as_ref())
                    .is_some_and(|connected| connected.connection_id == *connection_id);
                if current
                    && let Some(reason) =
                        super::session_lifecycle::terminal_reason_from_auth_code(reason)
                {
                    self.begin_authorization_epoch(None);
                    self.retire_session_connection(endpoint_id, false);
                    self.reduce_gateway_session_lifecycle(
                        endpoint_id,
                        SessionLifecycleEvent::AuthFailed { reason },
                    );
                } else {
                    self.mark_gateway_session_disconnected(endpoint_id, Some(*connection_id));
                    if super::session_lifecycle::auth_code_requires_refresh(reason) {
                        let mut sessions = self
                            .gateway_session
                            .lock()
                            .expect("Gateway session owner poisoned");
                        if let Some(state) = sessions.connections.get_mut(endpoint_id) {
                            state.refresh_requested = true;
                            self.publish_gateway_session(&sessions);
                        }
                    }
                }
            }
            GatewayWsEvent::ConnectFailed {
                endpoint_id, error, ..
            } if super::session_lifecycle::auth_code_requires_refresh(error) => {
                let mut sessions = self
                    .gateway_session
                    .lock()
                    .expect("Gateway session owner poisoned");
                if let Some(state) = sessions.connections.get_mut(endpoint_id) {
                    state.refresh_requested = true;
                    self.publish_gateway_session(&sessions);
                }
            }
            GatewayWsEvent::Notification {
                connection_id,
                notification: GatewayNotification::AuthAccessExpiring(change),
                ..
            } => {
                let mut sessions = self
                    .gateway_session
                    .lock()
                    .expect("Gateway session owner poisoned");
                for state in sessions.connections.values_mut() {
                    if state.connected.as_ref().is_some_and(|connected| {
                        connected.connection_id == *connection_id
                            && connected.metadata.session_id == change.session_id
                    }) {
                        state.refresh_requested = true;
                    }
                }
                self.publish_gateway_session(&sessions);
            }
            GatewayWsEvent::Notification {
                connection_id,
                notification: GatewayNotification::AuthSessionRevoked(change),
            } => {
                let endpoint = self
                    .gateway_session
                    .lock()
                    .expect("Gateway session owner poisoned")
                    .connections
                    .iter()
                    .find(|(_, state)| {
                        state.connected.as_ref().is_some_and(|connected| {
                            connected.connection_id == *connection_id
                                && connected.metadata.session_id == change.session_id
                        }) || (state.candidate_connection_id == Some(*connection_id)
                            && state.ready.as_ref().is_some_and(|ready| {
                                ready.metadata.session_id == change.session_id
                            }))
                    })
                    .map(|(endpoint, _)| endpoint.clone());
                if let Some(endpoint) = endpoint {
                    let reason = super::session_lifecycle::terminal_reason_from_auth_code(
                        change.reason.as_str(),
                    )
                    .expect("typed session termination reason");
                    self.begin_authorization_epoch(None);
                    self.retire_session_connection(&endpoint, false);
                    self.reduce_gateway_session_lifecycle(
                        &endpoint,
                        SessionLifecycleEvent::AuthFailed { reason },
                    );
                    // The receiver must not wait for the transport lease: its owner can be
                    // waiting for an RPC response delivered by this same receive task.
                    let _ = self
                        .compatibility_runtime()
                        .ws_command_sender()
                        .disconnect_connection(*connection_id);
                }
            }
            _ => {}
        }
    }

    pub fn suspend_gateway_session(&self, endpoint: &str) -> Result<()> {
        self.retire_session_connection(endpoint, true);
        self.reduce_gateway_session_lifecycle(endpoint, SessionLifecycleEvent::Suspend);
        let mut lease = self
            .session_transport
            .lock()
            .expect("Gateway transport lease poisoned");
        if lease.as_deref() == Some(endpoint) {
            *lease = None;
            self.compatibility_runtime()
                .ws_command_sender()
                .disconnect()?;
        }
        Ok(())
    }
}

fn connection_failure(error: anyhow::Error) -> GatewaySessionConnectionFailure {
    let code = error
        .downcast_ref::<AuthExchangeError>()
        .and_then(|error| error.code.clone())
        .or_else(|| {
            error
                .downcast_ref::<super::session_refresh::GatewaySessionStorageError>()
                .map(|error| error.code.clone())
        })
        .or_else(|| {
            error.chain().map(ToString::to_string).find(|code| {
                super::session_lifecycle::terminal_reason_from_auth_code(code).is_some()
                    || super::session_lifecycle::auth_code_requires_refresh(code)
            })
        })
        .unwrap_or_else(|| "gateway_connection_failed".into());
    match super::session_lifecycle::terminal_reason_from_auth_code(&code) {
        Some(reason) => GatewaySessionConnectionFailure::Terminal { reason },
        None => GatewaySessionConnectionFailure::Unavailable { code },
    }
}

pub fn exchange_refresh(
    base: &GatewayBaseUrl,
    credential: &str,
    request_id: &str,
    timeout: Duration,
) -> std::result::Result<AuthRefreshGrant, AuthExchangeError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| AuthExchangeError {
            kind: AuthExchangeErrorKind::Transport,
            code: None,
            message: "failed to initialize session refresh runtime".into(),
        })?;
    runtime.block_on(AuthExchangeClient::new(timeout).refresh(
        base,
        credential,
        pioneer_protocol::AuthRefreshParams {
            refresh_request_id: request_id.into(),
            client_version: Some(env!("CARGO_PKG_VERSION").into()),
        },
    ))
}
fn cleanup_session(
    base: &GatewayBaseUrl,
    access: &str,
    session: &AuthSessionId,
    timeout: Duration,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime
        .block_on(AuthExchangeClient::new(timeout).cleanup_session_once(
            base,
            access,
            session.clone(),
        ))
        .map(|_| ())
        .map_err(anyhow::Error::new)
}

#[cfg(test)]
mod tests {
    use super::super::{
        session_envelope::GatewaySessionEnvelope,
        timings::GatewayWsTimings,
        types::{GatewayEndpoint, GatewayEndpointKind},
    };
    use super::*;
    use pioneer_protocol::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Storage(Mutex<GatewaySessionEnvelope>);
    impl GatewaySessionStorage for Storage {
        fn load(&self, _: &GatewayEndpoint) -> Result<Option<GatewaySessionEnvelope>> {
            Ok(Some(self.0.lock().unwrap().clone()))
        }
        fn persist(&self, _: &GatewayEndpoint, envelope: &GatewaySessionEnvelope) -> Result<()> {
            *self.0.lock().unwrap() = envelope.clone();
            Ok(())
        }
    }
    fn grant(generation: u64) -> AuthRefreshGrant {
        AuthRefreshGrant {
            gateway: AuthGatewaySnapshot {
                id: GatewayId::new("G00000000000000000001").unwrap(),
            },
            principal: AuthPrincipalSnapshot {
                id: PrincipalId::new("P00000000000000000001").unwrap(),
                kind: PrincipalKind::Superuser,
                display_name: "Synthetic".into(),
                nickname: "synthetic".into(),
                avatar_revision: None,
            },
            device: AuthDeviceSnapshot {
                id: DeviceId::new("D00000000000000000001").unwrap(),
                installation_id: "synthetic".into(),
                display_name: "Synthetic".into(),
                client_kind: ClientKind::Mobile,
                status: DeviceStatus::Active,
            },
            session: AuthSessionSnapshot {
                id: AuthSessionId::new("S00000000000000000001").unwrap(),
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: generation,
                refresh_expires_at_unix: 4000,
            },
            access_token: AuthSecretString::new("synthetic-access"),
            access_expires_at_unix: 2000,
            refresh_token: AuthSecretString::new(format!(
                "{}{}",
                REFRESH_CREDENTIAL_PREFIX,
                "a".repeat(REFRESH_CREDENTIAL_BODY_LEN)
            )),
            refresh_generation: generation,
            refresh_expires_at_unix: 4000,
            auth_protocol_version: DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
            credential_storage_order: CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
        }
    }
    fn auth_me() -> AuthMeResponse {
        let grant = grant(1);
        AuthMeResponse {
            gateway: grant.gateway,
            principal: grant.principal,
            device: grant.device,
            session: grant.session,
            role_key: None,
        }
    }
    fn storage() -> Storage {
        let grant = grant(0);
        Storage(Mutex::new(GatewaySessionEnvelope {
            schema_version: 2,
            gateway_id: grant.gateway.id,
            principal_id: grant.principal.id,
            device_id: grant.device.id,
            session_id: grant.session.id,
            token_family_id: grant.session.token_family_id,
            installation_id: "synthetic".into(),
            refresh_generation: 0,
            refresh_expires_at_unix: 4000,
            refresh_token: grant.refresh_token,
            pending_refresh_request_id: None,
        }))
    }
    fn endpoint(id: &str) -> GatewayEndpoint {
        GatewayEndpoint {
            id: id.into(),
            name: "Synthetic".into(),
            gateway_base_url: GatewayBaseUrl::parse_presentation("https://gateway.invalid")
                .unwrap(),
            kind: GatewayEndpointKind::Remote,
            session_ref: Some(id.into()),
            server_gateway_id: Some(grant(0).gateway.id),
            workspace_id: None,
            service_name: None,
        }
    }
    fn request(endpoint: &GatewayEndpoint) -> GatewaySessionRefreshRequest<'_> {
        GatewaySessionRefreshRequest {
            endpoint,
            installation_id: "synthetic",
            client_kind: ClientKind::Mobile,
            now_unix: 100,
            timeout: Duration::from_secs(1),
            ws_timings: GatewayWsTimings::from_millis(1000, 1000, 1000, 100, 1000, 0).unwrap(),
            retry_delays: &[],
        }
    }
    fn connect(
        core: &ClientCore,
        endpoint: &GatewayEndpoint,
        storage: &Storage,
        id: u64,
    ) -> ConnectionOutcome {
        core.ensure_gateway_session_with_ports(
            request(endpoint),
            storage,
            |_, _, _, _| Ok(grant(1)),
            |_, _, _| Ok(()),
            |_| Ok(id),
            || Ok(auth_me()),
            || Ok(()),
        )
    }

    #[test]
    fn expired_access_retries_one_rotation_and_keeps_the_last_successor_durable() {
        for fail_second_attempt in [false, true] {
            let core = ClientCore::new();
            let storage = storage();
            let mut rotations = 0;
            let mut connections = 0;
            let outcome = core.ensure_gateway_session_with_ports(
                request(&endpoint("endpoint")),
                &storage,
                |_, _, _, _| {
                    rotations += 1;
                    Ok(grant(rotations))
                },
                |_, _, _| Ok(()),
                |_| {
                    connections += 1;
                    if connections == 1 || fail_second_attempt {
                        anyhow::bail!("access_expired");
                    }
                    Ok(2)
                },
                || {
                    let mut auth = auth_me();
                    auth.session.refresh_generation = 2;
                    Ok(auth)
                },
                || Ok(()),
            );
            assert_eq!(rotations, 2);
            assert_eq!(connections, 2);
            assert_eq!(storage.0.lock().unwrap().refresh_generation, 2);
            assert!(
                storage
                    .0
                    .lock()
                    .unwrap()
                    .pending_refresh_request_id
                    .is_none()
            );
            assert_eq!(outcome.is_ok(), !fail_second_attempt);
            if fail_second_attempt {
                assert!(core.gateway_session().terminal_reason("endpoint").is_none());
                assert!(
                    core.gateway_session().connections["endpoint"]
                        .connected
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn session_control_notifications_are_reduced_only_for_the_current_connection_and_session() {
        use crate::transport::ws::GatewayWsEvent;
        let core = ClientCore::new();
        let endpoint = endpoint("endpoint");
        let storage = storage();
        let connected = connect(&core, &endpoint, &storage, 1).unwrap();
        let expiring = |connection_id| GatewayWsEvent::Notification {
            connection_id,
            notification: GatewayNotification::AuthAccessExpiring(AuthAccessExpiringNotification {
                session_id: connected.metadata.session_id.clone(),
                access_expires_at_unix: 2000,
            }),
        };
        let before = core.gateway_session();
        core.observe_session_connection(&expiring(99));
        assert!(Arc::ptr_eq(&before, &core.gateway_session()));
        core.observe_session_connection(&expiring(1));
        assert!(core.gateway_session().connections["endpoint"].refresh_requested);
        let changed = core.gateway_session();
        core.observe_session_connection(&expiring(1));
        assert!(Arc::ptr_eq(&changed, &core.gateway_session()));
        let revoked = |connection_id| GatewayWsEvent::Notification {
            connection_id,
            notification: GatewayNotification::AuthSessionRevoked(AuthSessionRevokedNotification {
                session_id: connected.metadata.session_id.clone(),
                reason: AuthSessionTerminationReason::SessionRevoked,
            }),
        };
        core.observe_session_connection(&revoked(99));
        assert!(core.current_auth().is_some());
        core.observe_session_connection(&revoked(1));
        assert!(core.current_auth().is_none());
        assert!(
            core.gateway_session().connections["endpoint"]
                .connected
                .is_none()
        );
        assert_eq!(
            core.gateway_session().terminal_reason("endpoint"),
            Some(SessionTerminalReason::SessionRevoked)
        );
        assert!(connect(&core, &endpoint, &storage, 2).is_err());
    }

    #[test]
    fn cancelled_storage_failure_cannot_publish_a_terminal_identity_result() {
        struct CancelledStorage<'a>(&'a ClientCore);
        impl GatewaySessionStorage for CancelledStorage<'_> {
            fn load(&self, _: &GatewayEndpoint) -> Result<Option<GatewaySessionEnvelope>> {
                self.0.begin_gateway_operation();
                anyhow::bail!("synthetic storage failure")
            }
            fn persist(&self, _: &GatewayEndpoint, _: &GatewaySessionEnvelope) -> Result<()> {
                panic!("verification must not persist")
            }
        }
        let core = ClientCore::new();
        let endpoint = endpoint("endpoint");
        let GatewaySessionPreparation::Ready(ready) = core
            .prepare_gateway_session(
                request(&endpoint),
                &storage(),
                |_, _, _, _| Ok(grant(1)),
                |_, _, _| Ok(()),
            )
            .unwrap()
        else {
            panic!("synthetic preparation");
        };
        let access = crate::transport::http::GatewayHttpAccess {
            gateway_base_url: endpoint.gateway_base_url.clone(),
            gateway_id: ready.metadata.gateway_id.clone(),
            session_id: ready.metadata.session_id.clone(),
            generation: 11,
            access_expires_at_unix: ready.spec.identity.access_expires_at_unix,
            access_token: ready.spec.access_token.clone(),
        };
        core.start_gateway_session_transport_with_port(
            ready.spec.into_connect_spec(),
            true,
            |_, _| Ok(11),
        )
        .unwrap();
        assert!(
            core.verify_gateway_session_identity_with_ports(
                &endpoint,
                "synthetic",
                ClientKind::Mobile,
                access,
                &CancelledStorage(&core),
                || panic!("cancelled verification must not request identity")
            )
            .is_err()
        );
        assert!(
            core.gateway_session()
                .terminal_reason(&endpoint.id)
                .is_none()
        );
    }

    #[test]
    fn cancelled_terminal_transport_and_identity_failures_remain_suspended() {
        for cancel_in_identity in [false, true] {
            let core = ClientCore::new();
            let endpoint = endpoint("endpoint");
            let outcome = core.ensure_gateway_session_with_ports(
                request(&endpoint),
                &storage(),
                |_, _, _, _| Ok(grant(1)),
                |_, _, _| Ok(()),
                |_| {
                    if cancel_in_identity {
                        return Ok(7);
                    }
                    core.begin_gateway_operation();
                    anyhow::bail!("session_revoked")
                },
                || {
                    core.begin_gateway_operation();
                    anyhow::bail!("session_revoked")
                },
                || Ok(()),
            );
            assert_eq!(outcome, Err(GatewaySessionConnectionFailure::Suspended));
            assert!(
                core.gateway_session()
                    .terminal_reason(&endpoint.id)
                    .is_none()
            );
            assert!(core.current_auth().is_none());
        }
    }

    #[test]
    fn prepared_startup_keeps_retry_mode_and_only_verifies_matching_current_identity() {
        for retry in [false, true] {
            for invalid_identity in [false, true] {
                let core = ClientCore::new();
                let endpoint = endpoint("endpoint");
                let storage = storage();
                let prepared = core
                    .prepare_gateway_session(
                        request(&endpoint),
                        &storage,
                        |_, _, _, _| Ok(grant(1)),
                        |_, _, _| Ok(()),
                    )
                    .unwrap();
                let GatewaySessionPreparation::Ready(ready) = prepared else {
                    panic!("synthetic session should prepare");
                };
                let access = crate::transport::http::GatewayHttpAccess {
                    gateway_base_url: endpoint.gateway_base_url.clone(),
                    gateway_id: ready.metadata.gateway_id.clone(),
                    session_id: ready.metadata.session_id.clone(),
                    generation: 11,
                    access_expires_at_unix: ready.spec.identity.access_expires_at_unix,
                    access_token: ready.spec.access_token.clone(),
                };
                let id = core
                    .start_gateway_session_transport_with_port(
                        ready.spec.into_connect_spec(),
                        retry,
                        |_, mode| {
                            assert_eq!(mode, retry);
                            assert!(core.current_auth().is_none());
                            Ok(11)
                        },
                    )
                    .unwrap();
                assert_eq!(id, 11);
                assert!(core.gateway_session().connections["endpoint"].pending);
                assert!(
                    core.gateway_session().connections["endpoint"]
                        .connected
                        .is_none()
                );
                let result = core
                    .verify_gateway_session_identity_with_ports(
                        &endpoint,
                        "synthetic",
                        ClientKind::Mobile,
                        access,
                        &storage,
                        || {
                            let mut auth = auth_me();
                            if invalid_identity {
                                auth.device.installation_id = "wrong-installation".into();
                            }
                            Ok(auth)
                        },
                    )
                    .unwrap();
                assert_eq!(result.is_some(), invalid_identity);
                assert_eq!(core.current_auth().is_some(), !invalid_identity);
                assert_eq!(
                    core.gateway_session().connections["endpoint"]
                        .connected
                        .is_some(),
                    !invalid_identity
                );
            }
        }
    }

    #[test]
    fn terminal_session_input_during_prepared_identity_request_cannot_restore_auth() {
        let core = ClientCore::new();
        let endpoint = endpoint("endpoint");
        let storage = storage();
        let GatewaySessionPreparation::Ready(ready) = core
            .prepare_gateway_session(
                request(&endpoint),
                &storage,
                |_, _, _, _| Ok(grant(1)),
                |_, _, _| Ok(()),
            )
            .unwrap()
        else {
            panic!("synthetic preparation");
        };
        let access = crate::transport::http::GatewayHttpAccess {
            gateway_base_url: endpoint.gateway_base_url.clone(),
            gateway_id: ready.metadata.gateway_id.clone(),
            session_id: ready.metadata.session_id.clone(),
            generation: 11,
            access_expires_at_unix: ready.spec.identity.access_expires_at_unix,
            access_token: ready.spec.access_token.clone(),
        };
        core.start_gateway_session_transport_with_port(
            ready.spec.into_connect_spec(),
            true,
            |_, _| Ok(11),
        )
        .unwrap();
        let result = core.verify_gateway_session_identity_with_ports(
            &endpoint,
            "synthetic",
            ClientKind::Mobile,
            access,
            &storage,
            || {
                core.reduce_gateway_session_lifecycle(
                    "endpoint",
                    SessionLifecycleEvent::AuthFailed {
                        reason: SessionTerminalReason::SessionRevoked,
                    },
                );
                Ok(auth_me())
            },
        );
        assert!(result.is_err());
        assert!(core.current_auth().is_none());
        assert!(
            core.gateway_session().connections["endpoint"]
                .connected
                .is_none()
        );
        assert_eq!(
            core.gateway_session().terminal_reason("endpoint"),
            Some(SessionTerminalReason::SessionRevoked)
        );
    }

    #[test]
    fn transport_ready_is_not_published_until_the_identity_port_succeeds() {
        let core = ClientCore::new();
        let endpoint = endpoint("endpoint");
        let event = crate::transport::ws::GatewayWsEvent::Connected {
            connection_id: 7,
            endpoint_id: endpoint.id.clone(),
            endpoint_name: endpoint.name.clone(),
            gateway_base_url: endpoint.gateway_base_url.clone(),
        };
        let outcome = core.ensure_gateway_session_with_ports(
            request(&endpoint),
            &storage(),
            |_, _, _, _| Ok(grant(1)),
            |_, _, _| Ok(()),
            |_| {
                core.observe_session_connection(&event);
                let mut owner = core.gateway_session.lock().unwrap();
                owner.observe_transport(&event);
                core.publish_gateway_session(&owner);
                Ok(7)
            },
            || {
                assert!(!core.gateway_session().startup.transport_ready);
                assert!(
                    core.gateway_feature_connection_projection(Default::default())
                        .is_none()
                );
                assert!(
                    core.gateway_session().connections["endpoint"]
                        .connected
                        .is_none()
                );
                Ok(auth_me())
            },
            || Ok(()),
        );
        assert!(outcome.is_ok());
        assert!(core.gateway_session().startup.transport_ready);
        assert_eq!(core.gateway_session().startup.connection_id, Some(7));
    }

    #[test]
    fn reconnect_identity_failure_is_scoped_and_a_stale_failure_cannot_retire_the_candidate() {
        let core = ClientCore::new();
        let endpoint = endpoint("endpoint");
        connect(&core, &endpoint, &storage(), 7).unwrap();
        core.mark_gateway_session_disconnected(&endpoint.id, Some(7));
        let event = crate::transport::ws::GatewayWsEvent::Connected {
            connection_id: 7,
            endpoint_id: endpoint.id.clone(),
            endpoint_name: endpoint.name.clone(),
            gateway_base_url: endpoint.gateway_base_url.clone(),
        };
        core.observe_session_connection(&event);
        {
            let mut owner = core.gateway_session.lock().unwrap();
            owner.observe_transport(&event);
            core.publish_gateway_session(&owner);
        }
        assert!(core.gateway_session().startup.identity_pending);
        let failure = GatewaySessionConnectionFailure::Unavailable {
            code: "access_expired".into(),
        };
        core.reject_gateway_session_identity(&endpoint.id, 6, failure.clone());
        assert!(core.gateway_session().startup.identity_pending);
        core.reject_gateway_session_identity(&endpoint.id, 7, failure);
        let publication = core.gateway_session();
        assert!(!publication.startup.identity_pending);
        assert!(publication.connections[&endpoint.id].refresh_requested);
        assert!(!publication.connections[&endpoint.id].pending);
        assert!(core.current_auth().is_none());
        let legacy = core
            .gateway_feature_connection_projection(Default::default())
            .unwrap();
        assert_eq!(
            legacy.connection_state,
            crate::state::client_state::GatewayConnectionState::Disconnected
        );
        assert_eq!(legacy.gateway_error.as_deref(), Some("access_expired"));
        let revision = publication.startup.transport_revision;
        core.reject_gateway_session_identity(
            &endpoint.id,
            7,
            GatewaySessionConnectionFailure::Terminal {
                reason: SessionTerminalReason::SessionRevoked,
            },
        );
        assert_eq!(core.gateway_session().startup.transport_revision, revision);
        assert!(
            core.gateway_session()
                .terminal_reason(&endpoint.id)
                .is_none()
        );
    }

    #[test]
    fn revoke_during_identity_verification_fences_the_candidate_and_late_success() {
        let core = ClientCore::new();
        let disconnects = AtomicUsize::new(0);
        let result = core.ensure_gateway_session_with_ports(
            request(&endpoint("endpoint")),
            &storage(),
            |_, _, _, _| Ok(grant(1)),
            |_, _, _| Ok(()),
            |_| Ok(7),
            || {
                core.observe_session_connection(
                    &crate::transport::ws::GatewayWsEvent::Notification {
                        connection_id: 7,
                        notification: GatewayNotification::AuthSessionRevoked(
                            AuthSessionRevokedNotification {
                                session_id: grant(1).session.id,
                                reason: AuthSessionTerminationReason::SessionRevoked,
                            },
                        ),
                    },
                );
                Ok(auth_me())
            },
            || {
                disconnects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        assert!(result.is_err());
        assert_eq!(disconnects.load(Ordering::SeqCst), 1);
        assert!(core.current_auth().is_none());
        assert!(
            core.gateway_session().connections["endpoint"]
                .connected
                .is_none()
        );
        assert_eq!(
            core.gateway_session().terminal_reason("endpoint"),
            Some(SessionTerminalReason::SessionRevoked)
        );
    }

    #[test]
    fn rejected_identity_disconnects_the_candidate_and_never_publishes_connected() {
        let core = ClientCore::new();
        let disconnects = AtomicUsize::new(0);
        let result = core.ensure_gateway_session_with_ports(
            request(&endpoint("endpoint")),
            &storage(),
            |_, _, _, _| Ok(grant(1)),
            |_, _, _| Ok(()),
            |_| Ok(1),
            || {
                let mut auth = auth_me();
                auth.device.installation_id = "wrong-installation".into();
                Ok(auth)
            },
            || {
                disconnects.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        );
        assert!(matches!(
            result,
            Err(GatewaySessionConnectionFailure::Terminal { .. })
        ));
        assert_eq!(disconnects.load(Ordering::SeqCst), 1);
        assert!(core.session_transport.lock().unwrap().is_none());
        assert!(core.current_auth().is_none());
        assert!(
            core.gateway_session().connections["endpoint"]
                .connected
                .is_none()
        );
    }

    #[test]
    fn connected_session_is_reused_and_failed_replacement_preserves_the_old_lease() {
        let core = ClientCore::new();
        let first = endpoint("first");
        let first_storage = storage();
        let connected = connect(&core, &first, &first_storage, 1).unwrap();
        let second = endpoint("second");
        let failure = core.ensure_gateway_session_with_ports(
            request(&second),
            &storage(),
            |_, _, _, _| Ok(grant(1)),
            |_, _, _| Ok(()),
            |_| anyhow::bail!("synthetic replacement failure"),
            || panic!("failed replacement must not verify identity"),
            || panic!("failed replacement must not disconnect the existing lease"),
        );
        assert!(failure.is_err());
        assert_eq!(
            core.session_transport.lock().unwrap().as_deref(),
            Some("first")
        );
        let reused = core.ensure_gateway_session_with_ports(
            request(&first),
            &first_storage,
            |_, _, _, _| panic!("valid access must not rotate again"),
            |_, _, _| Ok(()),
            |_| panic!("connected session must not reconnect"),
            || panic!("connected session is already verified"),
            || Ok(()),
        );
        assert_eq!(reused.unwrap(), connected);
    }

    #[test]
    fn stale_disconnect_cannot_clear_new_connection_and_retry_reuses_ephemeral_access() {
        let core = ClientCore::new();
        let endpoint = endpoint("endpoint");
        let storage = storage();
        connect(&core, &endpoint, &storage, 1).unwrap();
        core.mark_gateway_session_disconnected("endpoint", Some(99));
        assert_eq!(
            core.gateway_session().connections["endpoint"]
                .connected
                .as_ref()
                .unwrap()
                .connection_id,
            1
        );
        core.mark_gateway_session_disconnected("endpoint", Some(1));
        let retried = core
            .ensure_gateway_session_with_ports(
                request(&endpoint),
                &storage,
                |_, _, _, _| panic!("retry must retain the valid access generation"),
                |_, _, _| Ok(()),
                |_| Ok(2),
                || Ok(auth_me()),
                || Ok(()),
            )
            .unwrap();
        assert_eq!(retried.connection_id, 2);
        assert_eq!(storage.0.lock().unwrap().refresh_generation, 1);
    }

    #[test]
    fn superseded_preparation_persists_rotation_without_publishing_ephemeral_activation() {
        let core = ClientCore::new();
        let endpoint = endpoint("endpoint");
        let storage = storage();
        let result = core.prepare_gateway_session(
            request(&endpoint),
            &storage,
            |_, _, _, _| {
                core.begin_gateway_operation();
                Ok(grant(1))
            },
            |_, _, _| Ok(()),
        );
        assert!(result.is_err());
        assert_eq!(storage.0.lock().unwrap().refresh_generation, 1);
        assert!(
            storage
                .0
                .lock()
                .unwrap()
                .pending_refresh_request_id
                .is_none()
        );
        assert!(!matches!(
            core.gateway_session().session(&endpoint.id),
            Some(
                super::super::session_lifecycle::SessionLifecycleState::Active { .. }
                    | super::super::session_lifecycle::SessionLifecycleState::Connecting { .. }
            )
        ));
        assert!(core.current_auth().is_none());
    }

    #[test]
    fn suspension_after_server_rotation_persists_successor_without_connecting() {
        let core = Arc::new(ClientCore::new());
        let storage = Arc::new(storage());
        let entered = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        let worker = {
            let (core, storage, entered, resume) = (
                core.clone(),
                storage.clone(),
                entered.clone(),
                resume.clone(),
            );
            std::thread::spawn(move || {
                core.ensure_gateway_session_with_ports(
                    request(&endpoint("endpoint")),
                    &*storage,
                    |_, _, _, _| {
                        entered.wait();
                        resume.wait();
                        Ok(grant(1))
                    },
                    |_, _, _| Ok(()),
                    |_| panic!("suspended refresh must not activate access"),
                    || panic!("suspended refresh must not verify"),
                    || Ok(()),
                )
            })
        };
        entered.wait();
        core.suspend_gateway_session("endpoint").unwrap();
        resume.wait();
        assert_eq!(
            worker.join().unwrap(),
            Err(GatewaySessionConnectionFailure::Suspended)
        );
        assert_eq!(storage.0.lock().unwrap().refresh_generation, 1);
        assert!(
            storage
                .0
                .lock()
                .unwrap()
                .pending_refresh_request_id
                .is_none()
        );
    }

    #[test]
    fn concurrent_failed_requests_share_one_rotation_attempt_and_the_same_failure() {
        let core = Arc::new(ClientCore::new());
        let storage = Arc::new(storage());
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(std::sync::Barrier::new(2));
        let resume = Arc::new(std::sync::Barrier::new(2));
        let first = {
            let (core, storage, calls, entered, resume) = (
                core.clone(),
                storage.clone(),
                calls.clone(),
                entered.clone(),
                resume.clone(),
            );
            std::thread::spawn(move || {
                core.ensure_gateway_session_with_ports(
                    request(&endpoint("endpoint")),
                    &*storage,
                    |_, _, _, _| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        entered.wait();
                        resume.wait();
                        Err(AuthExchangeError {
                            kind: AuthExchangeErrorKind::Transport,
                            code: Some("synthetic_unavailable".into()),
                            message: "synthetic".into(),
                        })
                    },
                    |_, _, _| Ok(()),
                    |_| panic!("failed refresh must not connect"),
                    || panic!("failed refresh must not verify"),
                    || Ok(()),
                )
            })
        };
        entered.wait();
        let second = {
            let (core, storage) = (core.clone(), storage.clone());
            std::thread::spawn(move || {
                core.ensure_gateway_session_with_ports(
                    request(&endpoint("endpoint")),
                    &*storage,
                    |_, _, _, _| panic!("coalesced request must not refresh"),
                    |_, _, _| Ok(()),
                    |_| panic!("coalesced request must not connect"),
                    || panic!("coalesced request must not verify"),
                    || Ok(()),
                )
            })
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while core.gateway_session.lock().unwrap().connections["endpoint"]
            .flight
            .strong_count()
            < 2
        {
            assert!(
                std::time::Instant::now() < deadline,
                "second request failed to join"
            );
            std::thread::yield_now();
        }
        resume.wait();
        assert_eq!(first.join().unwrap(), second.join().unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
