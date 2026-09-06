//! Durable refresh sequencing shared by native session adapters.
use super::session_controller::SessionDiagnosticStage;
use super::{
    endpoint::GatewayBaseUrl,
    session_envelope::{GATEWAY_SESSION_SCHEMA_VERSION, GatewaySessionEnvelope},
    session_lifecycle::{
        GatewaySessionMetadata, SessionLifecycleEffect, SessionLifecycleEvent,
        SessionTerminalReason, terminal_reason_from_auth_code,
    },
    timings::GatewayWsTimings,
    types::GatewayEndpoint,
};
use crate::{
    core::ClientCore,
    transport::ws::{
        GatewayWsSessionIdentity, GatewayWsSessionSpec,
        auth_exchange::{AuthExchangeError, AuthExchangeErrorKind},
    },
};
use anyhow::{Result, bail};
use pioneer_protocol::{AuthRefreshGrant, AuthSecretString, AuthSessionId, ClientKind};
use std::time::Duration;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewaySessionStorageEffect {
    ReadGatewaySession {
        endpoint: GatewayEndpoint,
    },
    PersistGatewaySession {
        endpoint: GatewayEndpoint,
        envelope: GatewaySessionEnvelope,
    },
}

#[derive(Debug)]
pub struct GatewaySessionStorageError {
    pub code: String,
}
impl std::fmt::Display for GatewaySessionStorageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.code)
    }
}
impl std::error::Error for GatewaySessionStorageError {}
impl GatewaySessionStorageError {
    fn is_temporarily_unavailable(error: &anyhow::Error) -> bool {
        error
            .downcast_ref::<Self>()
            .is_some_and(|error| matches!(error.code.as_str(), "read_failed" | "write_failed"))
    }
}

pub trait GatewaySessionStorage {
    fn load(&self, endpoint: &GatewayEndpoint) -> Result<Option<GatewaySessionEnvelope>>;
    fn persist(&self, endpoint: &GatewayEndpoint, envelope: &GatewaySessionEnvelope) -> Result<()>;
}

#[derive(Clone, Copy)]
pub struct GatewaySessionRefreshRequest<'a> {
    pub endpoint: &'a GatewayEndpoint,
    pub installation_id: &'a str,
    pub client_kind: ClientKind,
    pub now_unix: u64,
    pub timeout: Duration,
    pub ws_timings: GatewayWsTimings,
    pub retry_delays: &'a [Duration],
}

#[derive(Debug)]
pub struct GatewaySessionAccessGrant {
    pub access_token: AuthSecretString,
    pub access_expires_at_unix: u64,
}

#[derive(Clone, Debug)]
pub struct GatewaySessionReady {
    pub spec: GatewayWsSessionSpec,
    pub metadata: GatewaySessionMetadata,
    pub connection_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewaySessionTerminal {
    pub metadata: Option<GatewaySessionMetadata>,
    pub reason: SessionTerminalReason,
}

#[derive(Debug)]
pub enum GatewaySessionPreparation {
    Ready(GatewaySessionReady),
    Terminal(GatewaySessionTerminal),
}

impl ClientCore {
    pub fn prepare_gateway_session<S, F, C>(
        &self,
        request: GatewaySessionRefreshRequest<'_>,
        storage: &S,
        refresh: F,
        cleanup: C,
    ) -> Result<GatewaySessionPreparation>
    where
        S: GatewaySessionStorage + ?Sized,
        F: FnMut(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> Result<AuthRefreshGrant, AuthExchangeError>,
        C: FnMut(&GatewayBaseUrl, &str, &AuthSessionId) -> Result<()>,
    {
        let operation_epoch = self.gateway_operation_epoch();
        self.prepare_gateway_session_guarded(
            request,
            storage,
            || !self.is_stopped() && self.gateway_operation_epoch() == operation_epoch,
            refresh,
            cleanup,
        )
    }

    pub(crate) fn prepare_gateway_session_guarded<S, F, C>(
        &self,
        request: GatewaySessionRefreshRequest<'_>,
        storage: &S,
        current: impl Fn() -> bool,
        mut refresh: F,
        mut cleanup: C,
    ) -> Result<GatewaySessionPreparation>
    where
        S: GatewaySessionStorage + ?Sized,
        F: FnMut(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> Result<AuthRefreshGrant, AuthExchangeError>,
        C: FnMut(&GatewayBaseUrl, &str, &AuthSessionId) -> Result<()>,
    {
        let endpoint = request.endpoint;
        let stop = |metadata, reason| {
            if current() {
                self.reduce_gateway_session_lifecycle(
                    &endpoint.id,
                    SessionLifecycleEvent::AuthFailed { reason },
                );
            }
            GatewaySessionPreparation::Terminal(GatewaySessionTerminal { metadata, reason })
        };
        anyhow::ensure!(current(), "Client session request was cancelled");
        let loaded = self.observe_session_stage(
            &endpoint.id,
            SessionDiagnosticStage::CredentialsLoad,
            || storage.load(endpoint),
        );
        anyhow::ensure!(current(), "Client session request was cancelled");
        let mut stored = match loaded {
            Ok(Some(stored)) => stored,
            Ok(None) => return Ok(stop(None, SessionTerminalReason::AuthenticationRequired)),
            Err(error) if GatewaySessionStorageError::is_temporarily_unavailable(&error) => {
                return Err(error);
            }
            Err(_) => return Ok(stop(None, SessionTerminalReason::SecureStorageFailed)),
        };
        if let Some(reason) = self.gateway_session().terminal_reason(&endpoint.id) {
            return Ok(GatewaySessionPreparation::Terminal(
                GatewaySessionTerminal {
                    metadata: Some(stored.metadata()),
                    reason,
                },
            ));
        }
        if stored.validate().is_err() {
            return Ok(stop(None, SessionTerminalReason::SecureStorageFailed));
        }
        if endpoint.server_gateway_id.as_ref() != Some(&stored.gateway_id) {
            return Ok(stop(
                Some(stored.metadata()),
                SessionTerminalReason::GatewayIdentityMismatch,
            ));
        }
        if stored.installation_id != request.installation_id {
            return Ok(stop(
                Some(stored.metadata()),
                SessionTerminalReason::SessionCompromised,
            ));
        }
        if request.now_unix >= stored.refresh_expires_at_unix {
            return Ok(stop(
                Some(stored.metadata()),
                SessionTerminalReason::SessionExpired,
            ));
        }
        let SessionLifecycleEffect::BeginRefresh { intent_id, .. } = self
            .reduce_gateway_session_lifecycle(
                &endpoint.id,
                SessionLifecycleEvent::StoredSessionLoaded(stored.metadata()),
            )
            .effect()
            .clone()
        else {
            bail!("shared session lifecycle did not request refresh");
        };
        let request_id = stored
            .pending_refresh_request_id
            .clone()
            .unwrap_or_else(|| pioneer_protocol::generate_id(pioneer_protocol::REQUEST_ID_LEN));
        if stored.pending_refresh_request_id.is_none() {
            stored.pending_refresh_request_id = Some(request_id.clone());
            let persisted = self.observe_session_stage(
                &endpoint.id,
                SessionDiagnosticStage::RefreshIntentPersist,
                || storage.persist(endpoint, &stored),
            );
            anyhow::ensure!(current(), "Client session request was cancelled");
            if let Err(error) = persisted {
                if GatewaySessionStorageError::is_temporarily_unavailable(&error) {
                    return Err(error);
                }
                self.reduce_gateway_session_lifecycle(
                    &endpoint.id,
                    SessionLifecycleEvent::SecureStorageFailed { intent_id },
                );
                return Ok(stop(
                    Some(stored.metadata()),
                    SessionTerminalReason::SecureStorageFailed,
                ));
            }
        }
        let mut attempt = 0;
        let grant = loop {
            anyhow::ensure!(current(), "Client session request was cancelled");
            match self.observe_session_stage(
                &endpoint.id,
                SessionDiagnosticStage::RefreshRequest,
                || {
                    refresh(
                        &endpoint.gateway_base_url,
                        stored.refresh_token.expose_secret(),
                        &request_id,
                        request.timeout,
                    )
                },
            ) {
                Ok(grant) => break grant,
                Err(error)
                    if transient_refresh_error(&error) && attempt < request.retry_delays.len() =>
                {
                    std::thread::sleep(request.retry_delays[attempt]);
                    attempt += 1;
                }
                Err(error) => {
                    anyhow::ensure!(current(), "Client session request was cancelled");
                    if let Some(reason) = terminal_refresh_error(&error) {
                        return Ok(stop(Some(stored.metadata()), reason));
                    }
                    self.reduce_gateway_session_lifecycle(
                        &endpoint.id,
                        SessionLifecycleEvent::RefreshTransportLost { intent_id },
                    );
                    return Err(anyhow::Error::new(error));
                }
            }
        };
        let cleanup_access = grant.access_token.clone();
        let cleanup_session = grant.session.id.clone();
        let (rotated, access) =
            match stored.rotate(request.installation_id, request.client_kind, grant) {
                Ok(rotated) => rotated,
                Err(_) => {
                    let _ = cleanup(
                        &endpoint.gateway_base_url,
                        cleanup_access.expose_secret(),
                        &cleanup_session,
                    );
                    return Ok(stop(
                        Some(stored.metadata()),
                        SessionTerminalReason::SessionCompromised,
                    ));
                }
            };
        let metadata = rotated.metadata();
        let effect = if current() {
            self.reduce_gateway_session_lifecycle(
                &endpoint.id,
                SessionLifecycleEvent::RefreshGrantReceived {
                    intent_id,
                    metadata: metadata.clone(),
                    access_expires_at_unix: access.access_expires_at_unix,
                },
            )
            .effect()
            .clone()
        } else {
            SessionLifecycleEffect::None
        };
        // The successor remains recoverable even if cancellation arrived after
        // the server committed rotation. It is never activated before storage.
        if let Err(error) = self.observe_session_stage(
            &endpoint.id,
            SessionDiagnosticStage::CredentialsPersist,
            || storage.persist(endpoint, &rotated),
        ) {
            let _ = cleanup(
                &endpoint.gateway_base_url,
                access.access_token.expose_secret(),
                &rotated.session_id,
            );
            if !current() {
                return Err(error);
            }
            self.reduce_gateway_session_lifecycle(
                &endpoint.id,
                SessionLifecycleEvent::SecureStorageFailed { intent_id },
            );
            if self.is_stopped() {
                return Err(error);
            }
            return Ok(stop(
                Some(metadata),
                SessionTerminalReason::SecureStorageFailed,
            ));
        }
        anyhow::ensure!(current(), "Client session request was cancelled");
        anyhow::ensure!(
            matches!(
                effect,
                SessionLifecycleEffect::PersistRefreshBeforeAccess { .. }
            ),
            "shared session lifecycle rejected refreshed session metadata"
        );
        let SessionLifecycleEffect::ConnectWithEphemeralAccess {
            connection_generation,
        } = self
            .reduce_gateway_session_lifecycle(
                &endpoint.id,
                SessionLifecycleEvent::SecureStorageCommitted { intent_id },
            )
            .effect()
            .clone()
        else {
            bail!("shared session lifecycle did not activate persisted refresh");
        };
        Ok(GatewaySessionPreparation::Ready(GatewaySessionReady {
            spec: GatewayWsSessionSpec {
                endpoint_id: endpoint.id.clone(),
                endpoint_name: endpoint.name.clone(),
                endpoint_kind: endpoint.kind,
                gateway_base_url: endpoint.gateway_base_url.clone(),
                identity: GatewayWsSessionIdentity {
                    server_gateway_id: rotated.gateway_id,
                    session_id: rotated.session_id,
                    device_id: rotated.device_id,
                    access_expires_at_unix: access.access_expires_at_unix,
                    refresh_leeway_seconds: 60,
                },
                access_token: access.access_token,
                timings: request.ws_timings,
            },
            metadata,
            connection_generation,
        }))
    }
}

impl GatewaySessionEnvelope {
    pub fn metadata(&self) -> GatewaySessionMetadata {
        GatewaySessionMetadata {
            gateway_id: self.gateway_id.clone(),
            device_id: self.device_id.clone(),
            session_id: self.session_id.clone(),
            refresh_generation: self.refresh_generation,
            refresh_expires_at_unix: self.refresh_expires_at_unix,
        }
    }

    pub fn rotate(
        &self,
        installation_id: &str,
        kind: ClientKind,
        grant: AuthRefreshGrant,
    ) -> Result<(Self, GatewaySessionAccessGrant)> {
        anyhow::ensure!(
            self.accepts_refresh(installation_id, kind, &grant),
            "inconsistent Gateway refresh grant"
        );
        Ok((
            Self {
                schema_version: GATEWAY_SESSION_SCHEMA_VERSION,
                gateway_id: self.gateway_id.clone(),
                principal_id: self.principal_id.clone(),
                device_id: self.device_id.clone(),
                session_id: self.session_id.clone(),
                token_family_id: self.token_family_id.clone(),
                installation_id: self.installation_id.clone(),
                refresh_generation: grant.refresh_generation,
                refresh_expires_at_unix: grant.refresh_expires_at_unix,
                refresh_token: grant.refresh_token,
                pending_refresh_request_id: None,
            },
            GatewaySessionAccessGrant {
                access_token: grant.access_token,
                access_expires_at_unix: grant.access_expires_at_unix,
            },
        ))
    }
}

fn transient_refresh_error(error: &AuthExchangeError) -> bool {
    error.kind == AuthExchangeErrorKind::Server
        && matches!(
            error.code.as_deref(),
            Some("temporarily_unavailable" | "auth_not_ready")
        )
}
fn terminal_refresh_error(error: &AuthExchangeError) -> Option<SessionTerminalReason> {
    if transient_refresh_error(error) {
        return None;
    }
    if let Some(reason) = error
        .code
        .as_deref()
        .and_then(terminal_reason_from_auth_code)
    {
        return Some(reason);
    }
    match error.kind {
        AuthExchangeErrorKind::TransportBeforeRequest
        | AuthExchangeErrorKind::Timeout
        | AuthExchangeErrorKind::Transport
        | AuthExchangeErrorKind::Protocol => None,
        AuthExchangeErrorKind::InvalidEndpoint => {
            Some(SessionTerminalReason::GatewayIdentityMismatch)
        }
        AuthExchangeErrorKind::CredentialMethodMismatch | AuthExchangeErrorKind::Server => {
            Some(SessionTerminalReason::RefreshCredentialInvalid)
        }
    }
}

/// Bridge-neutral storage port. Native adapters own execution and complete the
/// typed operation; the waiting session worker owns the workflow.
pub struct GatewaySessionPlatformStorage<'a>(pub &'a ClientCore);
impl GatewaySessionStorage for GatewaySessionPlatformStorage<'_> {
    fn load(&self, endpoint: &GatewayEndpoint) -> Result<Option<GatewaySessionEnvelope>> {
        match self
            .0
            .request_platform_effect(GatewaySessionStorageEffect::ReadGatewaySession {
                endpoint: endpoint.clone(),
            })? {
            crate::core::ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope } => {
                Ok(envelope)
            }
            crate::core::ClientEffectResult::Failed { code } => {
                Err(GatewaySessionStorageError { code }.into())
            }
            _ => bail!("Unexpected session storage completion"),
        }
    }
    fn persist(&self, endpoint: &GatewayEndpoint, envelope: &GatewaySessionEnvelope) -> Result<()> {
        match self.0.request_platform_effect(
            GatewaySessionStorageEffect::PersistGatewaySession {
                endpoint: endpoint.clone(),
                envelope: envelope.clone(),
            },
        )? {
            crate::core::ClientEffectResult::Completed => Ok(()),
            crate::core::ClientEffectResult::Failed { code } => {
                Err(GatewaySessionStorageError { code }.into())
            }
            _ => bail!("Unexpected session storage completion"),
        }
    }
}
