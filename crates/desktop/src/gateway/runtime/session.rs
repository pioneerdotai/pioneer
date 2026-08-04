use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use pioneer_client::{
    gateway::endpoint::GatewayBaseUrl,
    gateway::runtime as client_gateway_runtime,
    gateway::session_lifecycle::{
        GatewaySessionMetadata, SessionLifecycle, SessionLifecycleEffect, SessionLifecycleEvent,
        SessionTerminalReason, terminal_reason_from_auth_code,
    },
    transport::ws::{
        GatewayWsCommandSender, GatewayWsSessionIdentity, GatewayWsSessionSpec,
        auth_exchange::{AuthExchangeClient, AuthExchangeError, AuthExchangeErrorKind},
    },
};
use pioneer_protocol::{
    AuthMeResponse, AuthRefreshGrant, AuthRefreshParams, AuthSessionStatus, ClientKind,
    CredentialStorageOrder, DeviceStatus, PrincipalKind, generate_id,
};

use crate::gateway::{
    activation::{DesktopSessionAccessGrant, revoke_session_best_effort},
    secrets::{
        DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION, DesktopGatewaySessionSecret,
        is_valid_refresh_credential,
    },
};

use super::GatewayRuntime;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RefreshSlotKey {
    registry_path: PathBuf,
    endpoint_id: String,
}

static REFRESH_SLOTS: OnceLock<Mutex<HashMap<RefreshSlotKey, Weak<Mutex<RefreshSlot>>>>> =
    OnceLock::new();

#[derive(Default)]
struct RefreshSlot {
    mutation_in_progress: bool,
}

pub(crate) struct DesktopSessionMutationGuard {
    slot: Arc<Mutex<RefreshSlot>>,
}

impl Drop for DesktopSessionMutationGuard {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.slot.lock() {
            slot.mutation_in_progress = false;
        }
    }
}

#[derive(Debug)]
pub(crate) struct DesktopSessionReady {
    pub spec: GatewayWsSessionSpec,
    pub metadata: GatewaySessionMetadata,
    #[cfg(test)]
    pub connection_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DesktopSessionTerminal {
    pub metadata: Option<GatewaySessionMetadata>,
    pub reason: SessionTerminalReason,
}

#[derive(Debug)]
pub(crate) enum DesktopSessionPreparation {
    Ready(DesktopSessionReady),
    Terminal(DesktopSessionTerminal),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DesktopSessionConnectionOutcome {
    Connected {
        connection_id: u64,
        metadata: GatewaySessionMetadata,
        access_expires_at_unix: u64,
    },
    Terminal(DesktopSessionTerminal),
}

impl GatewayRuntime {
    pub(crate) fn verify_gateway_session_identity(
        &self,
        endpoint_id: &str,
        sender: &GatewayWsCommandSender,
    ) -> Result<Option<SessionTerminalReason>> {
        let endpoint = self
            .endpoint_by_id(endpoint_id)
            .with_context(|| format!("unknown desktop Gateway endpoint `{endpoint_id}`"))?;
        let session_ref = endpoint
            .session_ref
            .as_deref()
            .context("desktop Gateway endpoint has no session reference")?;
        let stored = match self.secrets.get_gateway_session(session_ref) {
            Ok(Some(stored)) => stored,
            Ok(None) | Err(_) => {
                return Ok(Some(SessionTerminalReason::SecureStorageFailed));
            }
        };

        let me = sender.auth_me()?;
        let expected_installation_id = self
            .registry
            .installation_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("desktop Gateway registry has no installation id")?;
        Ok(validate_gateway_session_identity(
            endpoint.server_gateway_id.as_ref(),
            expected_installation_id,
            &stored,
            &me,
        ))
    }

    pub(crate) fn prepare_gateway_session(
        &mut self,
        endpoint_id: &str,
    ) -> Result<DesktopSessionPreparation> {
        self.with_gateway_session_refresh_slot(endpoint_id, |runtime, refresh_slot| {
            runtime.prepare_gateway_session_locked(endpoint_id, refresh_slot)
        })
    }

    fn with_gateway_session_refresh_slot<T>(
        &mut self,
        endpoint_id: &str,
        operation: impl FnOnce(&mut Self, &mut RefreshSlot) -> Result<T>,
    ) -> Result<T> {
        let refresh_slot = refresh_slot(self.registry_path.as_path(), endpoint_id)?;
        let mut refresh_slot = refresh_slot
            .lock()
            .map_err(|_| anyhow::anyhow!("desktop Gateway refresh lock is poisoned"))?;
        operation(self, &mut refresh_slot)
    }

    fn prepare_gateway_session_locked(
        &mut self,
        endpoint_id: &str,
        refresh_slot: &mut RefreshSlot,
    ) -> Result<DesktopSessionPreparation> {
        if let Some(reason) = self.terminal_sessions.get(endpoint_id).copied() {
            return Ok(DesktopSessionPreparation::Terminal(
                DesktopSessionTerminal {
                    metadata: self.session_metadata(endpoint_id).unwrap_or(None),
                    reason,
                },
            ));
        }
        if refresh_slot.mutation_in_progress {
            bail!("desktop Gateway session mutation is in progress");
        }
        let cleanup_timeout = self.timings.startup_timeout;
        self.prepare_gateway_session_serialized(
            endpoint_id,
            refresh_slot,
            exchange_refresh,
            |gateway_base_url, access_token, session_id| {
                revoke_session_best_effort(
                    gateway_base_url,
                    access_token,
                    session_id,
                    cleanup_timeout,
                )
            },
        )
    }

    #[cfg(test)]
    fn prepare_gateway_session_with_refresh<F>(
        &mut self,
        endpoint_id: &str,
        refresh: F,
    ) -> Result<DesktopSessionPreparation>
    where
        F: FnOnce(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> std::result::Result<AuthRefreshGrant, AuthExchangeError>,
    {
        if let Some(reason) = self.terminal_sessions.get(endpoint_id).copied() {
            return Ok(DesktopSessionPreparation::Terminal(
                DesktopSessionTerminal {
                    metadata: self.session_metadata(endpoint_id).unwrap_or(None),
                    reason,
                },
            ));
        }
        let slot = refresh_slot(self.registry_path.as_path(), endpoint_id)?;
        let mut slot = slot
            .lock()
            .map_err(|_| anyhow::anyhow!("desktop Gateway refresh lock is poisoned"))?;
        if slot.mutation_in_progress {
            bail!("desktop Gateway session mutation is in progress");
        }
        self.prepare_gateway_session_serialized(endpoint_id, &mut slot, refresh, |_, _, _| Ok(()))
    }

    pub(crate) fn replace_gateway_session_access(
        &mut self,
        endpoint_id: &str,
        sender: &GatewayWsCommandSender,
    ) -> Result<DesktopSessionConnectionOutcome> {
        self.with_gateway_session_refresh_slot(endpoint_id, |runtime, refresh_slot| {
            match runtime.prepare_gateway_session_locked(endpoint_id, refresh_slot)? {
                DesktopSessionPreparation::Terminal(terminal) => {
                    Ok(DesktopSessionConnectionOutcome::Terminal(terminal))
                }
                DesktopSessionPreparation::Ready(ready) => {
                    let expires_at = ready.spec.identity.access_expires_at_unix;
                    let connection_id =
                        sender.replace_access_and_wait(ready.spec.into_connect_spec())?;
                    Ok(DesktopSessionConnectionOutcome::Connected {
                        connection_id,
                        metadata: ready.metadata,
                        access_expires_at_unix: expires_at,
                    })
                }
            }
        })
    }

    /// Refreshes and replaces an access credential only while the rejected
    /// generation is still current. The existing per-session refresh slot is
    /// held through the WS replacement so concurrent WS and HTTP recovery
    /// paths cannot rotate the refresh credential twice.
    pub(crate) fn replace_gateway_session_access_after_rejection(
        &mut self,
        endpoint_id: &str,
        sender: &GatewayWsCommandSender,
        rejected_generation: u64,
    ) -> Result<Option<DesktopSessionConnectionOutcome>> {
        self.with_gateway_session_refresh_slot(endpoint_id, |runtime, refresh_slot| {
            if let Ok(current) = sender.current_gateway_http_access()
                && current.generation != rejected_generation
            {
                return Ok(None);
            }

            let outcome = match runtime.prepare_gateway_session_locked(endpoint_id, refresh_slot)? {
                DesktopSessionPreparation::Terminal(terminal) => {
                    DesktopSessionConnectionOutcome::Terminal(terminal)
                }
                DesktopSessionPreparation::Ready(ready) => {
                    let expires_at = ready.spec.identity.access_expires_at_unix;
                    let connection_id =
                        sender.replace_access_and_wait(ready.spec.into_connect_spec())?;
                    DesktopSessionConnectionOutcome::Connected {
                        connection_id,
                        metadata: ready.metadata,
                        access_expires_at_unix: expires_at,
                    }
                }
            };
            Ok(Some(outcome))
        })
    }

    pub(crate) fn recover_gateway_session_access(
        &mut self,
        endpoint_id: &str,
        sender: &GatewayWsCommandSender,
    ) -> Result<DesktopSessionConnectionOutcome> {
        self.with_gateway_session_refresh_slot(endpoint_id, |runtime, refresh_slot| {
            match runtime.prepare_gateway_session_locked(endpoint_id, refresh_slot)? {
                DesktopSessionPreparation::Terminal(terminal) => {
                    Ok(DesktopSessionConnectionOutcome::Terminal(terminal))
                }
                DesktopSessionPreparation::Ready(ready) => {
                    let expires_at = ready.spec.identity.access_expires_at_unix;
                    let connection_id =
                        sender.connect_with_retry(ready.spec.into_connect_spec())?;
                    Ok(DesktopSessionConnectionOutcome::Connected {
                        connection_id,
                        metadata: ready.metadata,
                        access_expires_at_unix: expires_at,
                    })
                }
            }
        })
    }

    pub(crate) fn session_terminal_reason(
        &self,
        endpoint_id: &str,
    ) -> Option<SessionTerminalReason> {
        self.terminal_sessions.get(endpoint_id).copied()
    }

    pub(crate) fn clear_session_terminal_for_explicit_retry(&mut self, endpoint_id: &str) {
        self.terminal_sessions.remove(endpoint_id);
        self.access_expiries.remove(endpoint_id);
    }

    pub(crate) fn active_session_matches(
        &self,
        session_id: &pioneer_protocol::AuthSessionId,
    ) -> bool {
        self.active_gateway_id()
            .and_then(|endpoint_id| self.session_metadata(endpoint_id).ok().flatten())
            .is_some_and(|metadata| metadata.session_id == *session_id)
    }

    pub(crate) fn mark_active_session_terminal(
        &mut self,
        reason: SessionTerminalReason,
    ) -> Result<Option<DesktopSessionTerminal>> {
        let Some(endpoint_id) = self.active_gateway_id().map(str::to_owned) else {
            return Ok(None);
        };
        let metadata = self.session_metadata(endpoint_id.as_str()).unwrap_or(None);
        self.terminal_sessions.insert(endpoint_id.clone(), reason);
        self.access_expiries.remove(endpoint_id.as_str());
        Ok(Some(DesktopSessionTerminal { metadata, reason }))
    }

    pub(crate) fn forget_gateway_session_after_logout(&mut self, endpoint_id: &str) -> Result<()> {
        let session_ref = self
            .endpoint_by_id(endpoint_id)
            .with_context(|| format!("unknown desktop Gateway endpoint `{endpoint_id}`"))?
            .session_ref
            .clone();
        self.access_expiries.remove(endpoint_id);
        self.terminal_sessions.insert(
            endpoint_id.to_owned(),
            SessionTerminalReason::SessionRevoked,
        );
        if let Some(session_ref) = session_ref.as_deref() {
            self.secrets.delete_gateway_session(session_ref)?;
        }
        Ok(())
    }

    pub(crate) fn discard_gateway_session_runtime_state(&mut self, endpoint_id: &str) {
        self.access_expiries.remove(endpoint_id);
        self.terminal_sessions.remove(endpoint_id);
    }

    pub(crate) fn active_session_refresh_delay(
        &self,
        now_unix: u64,
        refresh_leeway_seconds: u64,
    ) -> Option<Duration> {
        let endpoint_id = self.active_gateway_id()?;
        let expires_at = *self.access_expiries.get(endpoint_id)?;
        Some(Duration::from_secs(
            expires_at
                .saturating_sub(refresh_leeway_seconds)
                .saturating_sub(now_unix),
        ))
    }

    fn prepare_gateway_session_serialized<F, C>(
        &mut self,
        endpoint_id: &str,
        _refresh_slot: &mut RefreshSlot,
        refresh: F,
        cleanup_session: C,
    ) -> Result<DesktopSessionPreparation>
    where
        F: FnOnce(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> std::result::Result<AuthRefreshGrant, AuthExchangeError>,
        C: FnMut(&GatewayBaseUrl, &str, &pioneer_protocol::AuthSessionId) -> Result<()>,
    {
        let endpoint = self
            .endpoint_by_id(endpoint_id)
            .cloned()
            .with_context(|| format!("unknown desktop Gateway endpoint `{endpoint_id}`"))?;
        let Some(session_ref) = endpoint.session_ref.clone() else {
            return Ok(self.enter_terminal(
                endpoint_id,
                None,
                SessionTerminalReason::AuthenticationRequired,
            ));
        };
        let stored = match self.secrets.get_gateway_session(session_ref.as_str()) {
            Ok(Some(stored)) => stored,
            Ok(None) => {
                return Ok(self.enter_terminal(
                    endpoint_id,
                    None,
                    SessionTerminalReason::AuthenticationRequired,
                ));
            }
            Err(_) => {
                return Ok(self.enter_terminal(
                    endpoint_id,
                    None,
                    SessionTerminalReason::SecureStorageFailed,
                ));
            }
        };
        if endpoint.server_gateway_id.as_ref() != Some(&stored.gateway_id) {
            return Ok(self.enter_terminal(
                endpoint_id,
                Some(metadata(&stored)),
                SessionTerminalReason::GatewayIdentityMismatch,
            ));
        }
        let expected_installation_id = self
            .registry
            .installation_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("desktop Gateway registry has no installation id")?;
        if stored.installation_id != expected_installation_id {
            return Ok(self.enter_terminal(
                endpoint_id,
                Some(metadata(&stored)),
                SessionTerminalReason::SessionCompromised,
            ));
        }

        if unix_timestamp_secs()? >= stored.refresh_expires_at_unix {
            return Ok(self.enter_terminal(
                endpoint_id,
                Some(metadata(&stored)),
                SessionTerminalReason::SessionExpired,
            ));
        }
        self.refresh_stored_session(
            endpoint_id,
            endpoint,
            session_ref,
            stored,
            refresh,
            cleanup_session,
        )
    }

    fn refresh_stored_session<F, C>(
        &mut self,
        endpoint_id: &str,
        endpoint: pioneer_client::gateway::types::GatewayEndpoint,
        session_ref: String,
        mut stored: DesktopGatewaySessionSecret,
        refresh: F,
        mut cleanup_session: C,
    ) -> Result<DesktopSessionPreparation>
    where
        F: FnOnce(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> std::result::Result<AuthRefreshGrant, AuthExchangeError>,
        C: FnMut(&GatewayBaseUrl, &str, &pioneer_protocol::AuthSessionId) -> Result<()>,
    {
        let mut lifecycle = SessionLifecycle::default();
        let SessionLifecycleEffect::BeginRefresh { intent_id, .. } = lifecycle.reduce(
            SessionLifecycleEvent::StoredSessionLoaded(metadata(&stored)),
        ) else {
            bail!("shared session lifecycle did not request cold-start refresh");
        };
        let refresh_request_id = stored
            .pending_refresh_request_id
            .clone()
            .unwrap_or_else(|| generate_id(pioneer_protocol::REQUEST_ID_LEN));
        if stored.pending_refresh_request_id.is_none() {
            stored.pending_refresh_request_id = Some(refresh_request_id.clone());
            if let Err(error) = self.secrets.put_gateway_session(
                session_ref.as_str(),
                &stored,
                Some(format!("{} session", endpoint.name)),
            ) {
                let effect =
                    lifecycle.reduce(SessionLifecycleEvent::SecureStorageFailed { intent_id });
                let SessionLifecycleEffect::Stop { reason } = effect else {
                    return Err(error);
                };
                return Ok(self.enter_terminal(endpoint_id, Some(metadata(&stored)), reason));
            }
        }
        let refresh = match refresh(
            &endpoint.gateway_base_url,
            stored.refresh_token.expose_secret(),
            refresh_request_id.as_str(),
            self.timings.startup_timeout,
        ) {
            Ok(refresh) => refresh,
            Err(error) => {
                let Some(reason) = terminal_reason_for_refresh_error(&error) else {
                    // The predecessor and request id are both durable. A
                    // retry can recover either a pre-dispatch failure or an
                    // exchange whose response was lost after server commit.
                    let effect =
                        lifecycle.reduce(SessionLifecycleEvent::RefreshTransportLost { intent_id });
                    if !matches!(
                        effect,
                        SessionLifecycleEffect::BeginRefresh {
                            intent_id: retry_intent,
                            ..
                        } if retry_intent == intent_id
                    ) {
                        bail!("shared session lifecycle rejected retryable refresh failure");
                    }
                    return Err(anyhow::Error::new(error));
                };
                let event = match reason {
                    SessionTerminalReason::RefreshOutcomeUnknown => {
                        SessionLifecycleEvent::RefreshTransportLost { intent_id }
                    }
                    reason => SessionLifecycleEvent::AuthFailed { reason },
                };
                let effect = lifecycle.reduce(event);
                let SessionLifecycleEffect::Stop { reason } = effect else {
                    bail!("shared session lifecycle did not stop after refresh failure");
                };
                return Ok(self.enter_terminal(endpoint_id, Some(metadata(&stored)), reason));
            }
        };
        let cleanup_access = refresh.access_token.clone();
        let cleanup_session_id = refresh.session.id.clone();
        let expected_installation_id = self
            .registry
            .installation_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("desktop Gateway registry has no installation id")?;
        let (rotated, access) = match rotated_session(&stored, expected_installation_id, refresh) {
            Ok(result) => result,
            Err(_) => {
                let _ = cleanup_session(
                    &endpoint.gateway_base_url,
                    cleanup_access.expose_secret(),
                    &cleanup_session_id,
                );
                let effect = lifecycle.reduce(SessionLifecycleEvent::AuthFailed {
                    reason: SessionTerminalReason::SessionCompromised,
                });
                let SessionLifecycleEffect::Stop { reason } = effect else {
                    bail!("shared session lifecycle did not stop after invalid refresh grant");
                };
                return Ok(self.enter_terminal(endpoint_id, Some(metadata(&stored)), reason));
            }
        };
        self.persist_and_plan_access(
            endpoint_id,
            endpoint,
            session_ref,
            rotated,
            access,
            lifecycle,
            intent_id,
            cleanup_session,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_and_plan_access<C>(
        &mut self,
        endpoint_id: &str,
        endpoint: pioneer_client::gateway::types::GatewayEndpoint,
        session_ref: String,
        session: DesktopGatewaySessionSecret,
        access: DesktopSessionAccessGrant,
        mut lifecycle: SessionLifecycle,
        intent_id: u64,
        mut cleanup_session: C,
    ) -> Result<DesktopSessionPreparation>
    where
        C: FnMut(&GatewayBaseUrl, &str, &pioneer_protocol::AuthSessionId) -> Result<()>,
    {
        let next_metadata = metadata(&session);
        let SessionLifecycleEffect::PersistRefreshBeforeAccess { .. } =
            lifecycle.reduce(SessionLifecycleEvent::RefreshGrantReceived {
                intent_id,
                metadata: next_metadata.clone(),
                access_expires_at_unix: access.access_expires_at_unix,
            })
        else {
            bail!("shared session lifecycle rejected refreshed session metadata");
        };
        if let Err(error) = self.secrets.put_gateway_session(
            session_ref.as_str(),
            &session,
            Some(format!("{} session", endpoint.name)),
        ) {
            let _ = cleanup_session(
                &endpoint.gateway_base_url,
                access.access_token.expose_secret(),
                &session.session_id,
            );
            let effect = lifecycle.reduce(SessionLifecycleEvent::SecureStorageFailed { intent_id });
            let SessionLifecycleEffect::Stop { reason } = effect else {
                return Err(error);
            };
            return Ok(self.enter_terminal(endpoint_id, Some(next_metadata), reason));
        }
        self.finish_durable_access(
            endpoint_id,
            endpoint,
            session,
            access,
            lifecycle,
            intent_id,
            next_metadata,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_durable_access(
        &mut self,
        endpoint_id: &str,
        endpoint: pioneer_client::gateway::types::GatewayEndpoint,
        session: DesktopGatewaySessionSecret,
        access: DesktopSessionAccessGrant,
        mut lifecycle: SessionLifecycle,
        intent_id: u64,
        next_metadata: GatewaySessionMetadata,
    ) -> Result<DesktopSessionPreparation> {
        let SessionLifecycleEffect::ConnectWithEphemeralAccess {
            connection_generation: _connection_generation,
        } = lifecycle.reduce(SessionLifecycleEvent::SecureStorageCommitted { intent_id })
        else {
            bail!("shared session lifecycle did not activate persisted refresh");
        };
        self.terminal_sessions.remove(endpoint_id);
        self.access_expiries
            .insert(endpoint_id.to_owned(), access.access_expires_at_unix);
        Ok(DesktopSessionPreparation::Ready(DesktopSessionReady {
            spec: GatewayWsSessionSpec {
                endpoint_id: endpoint.id,
                endpoint_name: endpoint.name,
                endpoint_kind: endpoint.kind,
                gateway_base_url: endpoint.gateway_base_url,
                identity: GatewayWsSessionIdentity {
                    server_gateway_id: session.gateway_id,
                    session_id: session.session_id,
                    device_id: session.device_id,
                    access_expires_at_unix: access.access_expires_at_unix,
                    refresh_leeway_seconds: 60,
                },
                access_token: access.access_token,
                timings: client_gateway_runtime::ws_timings_for_endpoint(
                    self.ws_timings,
                    endpoint.kind,
                    Duration::from_secs(5),
                ),
            },
            metadata: next_metadata,
            #[cfg(test)]
            connection_generation: _connection_generation,
        }))
    }

    fn enter_terminal(
        &mut self,
        endpoint_id: &str,
        metadata: Option<GatewaySessionMetadata>,
        reason: SessionTerminalReason,
    ) -> DesktopSessionPreparation {
        self.terminal_sessions
            .insert(endpoint_id.to_owned(), reason);
        self.access_expiries.remove(endpoint_id);
        DesktopSessionPreparation::Terminal(DesktopSessionTerminal { metadata, reason })
    }

    fn session_metadata(&self, endpoint_id: &str) -> Result<Option<GatewaySessionMetadata>> {
        let Some(endpoint) = self.endpoint_by_id(endpoint_id) else {
            return Ok(None);
        };
        let Some(session_ref) = endpoint.session_ref.as_deref() else {
            return Ok(None);
        };
        Ok(self
            .secrets
            .get_gateway_session(session_ref)?
            .as_ref()
            .map(metadata))
    }
}

fn exchange_refresh(
    gateway_base_url: &GatewayBaseUrl,
    credential: &str,
    refresh_request_id: &str,
    timeout: Duration,
) -> std::result::Result<AuthRefreshGrant, AuthExchangeError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| AuthExchangeError {
            kind: AuthExchangeErrorKind::Transport,
            code: None,
            message: "failed to initialize desktop refresh runtime".to_owned(),
        })?;
    runtime.block_on(AuthExchangeClient::new(timeout).refresh(
        gateway_base_url,
        credential,
        AuthRefreshParams {
            refresh_request_id: refresh_request_id.to_owned(),
            client_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
        },
    ))
}

fn rotated_session(
    previous: &DesktopGatewaySessionSecret,
    expected_installation_id: &str,
    grant: AuthRefreshGrant,
) -> Result<(DesktopGatewaySessionSecret, DesktopSessionAccessGrant)> {
    if grant.auth_protocol_version != pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION
        || grant.credential_storage_order
            != CredentialStorageOrder::PersistRefreshBeforeActivatingAccess
        || grant.gateway.id != previous.gateway_id
        || grant.principal.id != previous.principal_id
        || grant.principal.kind != pioneer_protocol::PrincipalKind::Superuser
        || grant.session.id != previous.session_id
        || grant.session.device_id != previous.device_id
        || grant.session.token_family_id != previous.token_family_id
        || grant.device.id != previous.device_id
        || grant.device.installation_id != expected_installation_id
        || grant.device.installation_id != previous.installation_id
        || grant.device.client_kind != pioneer_protocol::ClientKind::Desktop
        || grant.device.status != pioneer_protocol::DeviceStatus::Active
        || grant.session.status != pioneer_protocol::AuthSessionStatus::Active
        || previous.refresh_generation.checked_add(1) != Some(grant.refresh_generation)
        || grant.session.refresh_generation != grant.refresh_generation
        || grant.session.refresh_expires_at_unix != grant.refresh_expires_at_unix
        || grant.access_token.expose_secret().is_empty()
        || grant.access_expires_at_unix == 0
        || !is_valid_refresh_credential(grant.refresh_token.expose_secret())
    {
        bail!("inconsistent Gateway refresh grant");
    }
    Ok((
        DesktopGatewaySessionSecret {
            schema_version: DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION,
            gateway_id: previous.gateway_id.clone(),
            principal_id: previous.principal_id.clone(),
            device_id: previous.device_id.clone(),
            session_id: previous.session_id.clone(),
            token_family_id: previous.token_family_id.clone(),
            installation_id: previous.installation_id.clone(),
            refresh_generation: grant.refresh_generation,
            refresh_expires_at_unix: grant.refresh_expires_at_unix,
            refresh_token: grant.refresh_token,
            pending_refresh_request_id: None,
        },
        DesktopSessionAccessGrant {
            access_token: grant.access_token,
            access_expires_at_unix: grant.access_expires_at_unix,
        },
    ))
}

fn metadata(session: &DesktopGatewaySessionSecret) -> GatewaySessionMetadata {
    GatewaySessionMetadata {
        gateway_id: session.gateway_id.clone(),
        device_id: session.device_id.clone(),
        session_id: session.session_id.clone(),
        refresh_generation: session.refresh_generation,
        refresh_expires_at_unix: session.refresh_expires_at_unix,
    }
}

fn validate_gateway_session_identity(
    pinned_gateway_id: Option<&pioneer_protocol::GatewayId>,
    expected_installation_id: &str,
    stored: &DesktopGatewaySessionSecret,
    me: &AuthMeResponse,
) -> Option<SessionTerminalReason> {
    if pinned_gateway_id != Some(&stored.gateway_id) || me.gateway.id != stored.gateway_id {
        return Some(SessionTerminalReason::GatewayIdentityMismatch);
    }
    if me.principal.id != stored.principal_id
        || me.principal.kind != PrincipalKind::Superuser
        || me.device.id != stored.device_id
        || me.device.installation_id != expected_installation_id
        || me.device.installation_id != stored.installation_id
        || me.device.client_kind != ClientKind::Desktop
        || me.device.status != DeviceStatus::Active
        || me.session.id != stored.session_id
        || me.session.device_id != stored.device_id
        || me.session.token_family_id != stored.token_family_id
        || me.session.status != AuthSessionStatus::Active
        || me.session.refresh_generation != stored.refresh_generation
        || me.session.refresh_expires_at_unix != stored.refresh_expires_at_unix
    {
        return Some(SessionTerminalReason::SessionCompromised);
    }
    None
}

fn terminal_reason_for_refresh_error(error: &AuthExchangeError) -> Option<SessionTerminalReason> {
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

fn refresh_slot(registry_path: &Path, endpoint_id: &str) -> Result<Arc<Mutex<RefreshSlot>>> {
    let mut slots = REFRESH_SLOTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| anyhow::anyhow!("desktop refresh lock registry is poisoned"))?;
    slots.retain(|_, slot| slot.strong_count() > 0);
    let key = RefreshSlotKey {
        registry_path: registry_path.to_path_buf(),
        endpoint_id: endpoint_id.to_owned(),
    };
    if let Some(slot) = slots.get(&key).and_then(Weak::upgrade) {
        return Ok(slot);
    }
    let slot = Arc::new(Mutex::new(RefreshSlot::default()));
    slots.insert(key, Arc::downgrade(&slot));
    Ok(slot)
}

fn begin_desktop_session_mutation(
    registry_path: &Path,
    endpoint_id: &str,
) -> Result<DesktopSessionMutationGuard> {
    let slot = refresh_slot(registry_path, endpoint_id)?;
    {
        let mut state = slot
            .lock()
            .map_err(|_| anyhow::anyhow!("desktop Gateway refresh lock is poisoned"))?;
        if state.mutation_in_progress {
            bail!("desktop Gateway session mutation is already in progress");
        }
        state.mutation_in_progress = true;
    }
    Ok(DesktopSessionMutationGuard { slot })
}

impl GatewayRuntime {
    pub(crate) fn begin_session_mutation(
        &self,
        endpoint_id: &str,
    ) -> Result<DesktopSessionMutationGuard> {
        begin_desktop_session_mutation(self.registry_path.as_path(), endpoint_id)
    }
}

fn unix_timestamp_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_secs())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use pioneer_client::gateway::types::GatewayEndpointKind;
    use pioneer_keystore::MemorySecretStore;
    use pioneer_protocol::{
        AuthDeviceSnapshot, AuthSessionSnapshot, AuthSessionStatus, ClientKind, DeviceStatus,
        TokenFamilyId,
    };

    use crate::gateway::{
        registry::default_registry,
        secrets::{DesktopGatewaySessionSecret, DesktopSecrets},
        tests::test_config,
        timings::{gateway_timings_from_config, gateway_ws_timings_from_config},
    };

    use super::*;

    static NEXT_ENDPOINT: AtomicU64 = AtomicU64::new(1);

    fn fixture() -> (GatewayRuntime, Arc<MemorySecretStore>, String) {
        let config = test_config();
        let timings = gateway_timings_from_config(&config.desktop.gateway).unwrap();
        let ws_timings = gateway_ws_timings_from_config(&config.desktop.gateway).unwrap();
        let mut registry = default_registry(&config).unwrap();
        registry.installation_id = Some("desktop-installation".to_owned());
        let endpoint_id = format!("local-{}", NEXT_ENDPOINT.fetch_add(1, Ordering::Relaxed));
        let local = registry.local.as_mut().unwrap();
        local.id = endpoint_id.clone();
        local.kind = GatewayEndpointKind::Local;
        local.gateway_base_url =
            pioneer_client::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                "http://localhost:17878",
            )
            .unwrap();
        local.session_ref = Some(endpoint_id.clone());
        local.server_gateway_id = Some(gateway_id());
        registry.active_gateway_id = Some(endpoint_id.clone());
        let store = Arc::new(MemorySecretStore::new());
        let secrets = DesktopSecrets::new(store.clone());
        secrets
            .put_gateway_session(endpoint_id.as_str(), &stored_session(0), None)
            .unwrap();
        (
            GatewayRuntime {
                config,
                timings,
                ws_timings,
                registry_path: std::env::temp_dir().join("unused-session-runtime-registry"),
                registry,
                secrets,
                terminal_sessions: HashMap::new(),
                access_expiries: HashMap::new(),
            },
            store,
            endpoint_id,
        )
    }

    fn gateway_id() -> pioneer_protocol::GatewayId {
        pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap()
    }

    fn refresh_token(generation: u64) -> String {
        format!(
            "{}{generation:020}{}",
            pioneer_protocol::REFRESH_CREDENTIAL_PREFIX,
            "0".repeat(pioneer_protocol::REFRESH_CREDENTIAL_BODY_LEN - 20)
        )
    }

    fn stored_session(generation: u64) -> DesktopGatewaySessionSecret {
        DesktopGatewaySessionSecret {
            schema_version: DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION,
            gateway_id: gateway_id(),
            principal_id: pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
            device_id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
            session_id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
            token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
            installation_id: "desktop-installation".to_owned(),
            refresh_generation: generation,
            refresh_expires_at_unix: 4_102_444_800,
            refresh_token: pioneer_protocol::AuthSecretString::new(refresh_token(generation)),
            pending_refresh_request_id: None,
        }
    }

    fn refresh_grant(generation: u64) -> AuthRefreshGrant {
        AuthRefreshGrant {
            gateway: pioneer_protocol::AuthGatewaySnapshot { id: gateway_id() },
            principal: pioneer_protocol::AuthPrincipalSnapshot {
                id: pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
                kind: pioneer_protocol::PrincipalKind::Superuser,
                display_name: "Superuser".to_owned(),
                nickname: "superuser".to_owned(),
            },
            access_token: pioneer_protocol::AuthSecretString::new(format!(
                "access_generation_{generation}"
            )),
            access_expires_at_unix: 4_000_000_000,
            refresh_token: pioneer_protocol::AuthSecretString::new(refresh_token(generation)),
            refresh_expires_at_unix: 4_102_444_800,
            refresh_generation: generation,
            session: AuthSessionSnapshot {
                id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
                device_id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
                token_family_id: TokenFamilyId::new("F00000000000000000001").unwrap(),
                status: AuthSessionStatus::Active,
                refresh_generation: generation,
                refresh_expires_at_unix: 4_102_444_800,
            },
            device: AuthDeviceSnapshot {
                id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
                installation_id: "desktop-installation".to_owned(),
                display_name: "Pioneer Desktop".to_owned(),
                client_kind: ClientKind::Desktop,
                status: DeviceStatus::Active,
            },
            auth_protocol_version: pioneer_protocol::DEVICE_SESSION_AUTH_PROTOCOL_VERSION,
            credential_storage_order: CredentialStorageOrder::PersistRefreshBeforeActivatingAccess,
        }
    }

    fn auth_me(generation: u64) -> AuthMeResponse {
        let grant = refresh_grant(generation);
        AuthMeResponse {
            gateway: grant.gateway,
            principal: grant.principal,
            device: grant.device,
            session: grant.session,
            role_key: None,
        }
    }

    #[test]
    fn verified_session_identity_accepts_only_the_pinned_durable_session() {
        let stored = stored_session(2);
        let mut me = auth_me(2);

        assert_eq!(
            validate_gateway_session_identity(
                Some(&stored.gateway_id),
                "desktop-installation",
                &stored,
                &me,
            ),
            None
        );

        me.gateway.id = pioneer_protocol::GatewayId::new("G00000000000000000099").unwrap();
        assert_eq!(
            validate_gateway_session_identity(
                Some(&stored.gateway_id),
                "desktop-installation",
                &stored,
                &me,
            ),
            Some(SessionTerminalReason::GatewayIdentityMismatch)
        );
    }

    #[test]
    fn verified_session_identity_rejects_changed_session_metadata() {
        let stored = stored_session(2);
        let mut me = auth_me(2);
        me.device.installation_id = "another-installation".to_owned();

        assert_eq!(
            validate_gateway_session_identity(
                Some(&stored.gateway_id),
                "desktop-installation",
                &stored,
                &me,
            ),
            Some(SessionTerminalReason::SessionCompromised)
        );
    }

    #[test]
    fn cold_start_refresh_persists_rotation_before_returning_ephemeral_access() {
        let (mut runtime, _, endpoint_id) = fixture();
        let prepared = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, _, _| {
                assert_eq!(raw, refresh_token(0));
                Ok(refresh_grant(1))
            })
            .unwrap();
        let DesktopSessionPreparation::Ready(ready) = prepared else {
            panic!("ready session expected")
        };
        assert_eq!(ready.metadata.refresh_generation, 1);
        assert_eq!(ready.connection_generation, 1);
        assert_eq!(
            runtime
                .secrets
                .get_gateway_session(endpoint_id.as_str())
                .unwrap()
                .unwrap()
                .refresh_generation,
            1
        );
        assert!(!format!("{ready:?}").contains("access_generation_1"));
    }

    #[test]
    fn unbound_endpoint_requires_explicit_authentication() {
        let (mut runtime, _, endpoint_id) = fixture();
        let endpoint = runtime.registry.local.as_mut().expect("local endpoint");
        endpoint.session_ref = None;
        endpoint.server_gateway_id = None;

        let prepared = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                panic!("unbound endpoint must not attempt refresh")
            })
            .expect("terminal preparation");

        assert!(matches!(
            prepared,
            DesktopSessionPreparation::Terminal(DesktopSessionTerminal {
                reason: SessionTerminalReason::AuthenticationRequired,
                ..
            })
        ));
    }

    #[test]
    fn missing_session_envelope_requires_explicit_authentication() {
        let (mut runtime, _, endpoint_id) = fixture();
        runtime
            .secrets
            .delete_gateway_session(endpoint_id.as_str())
            .expect("delete session fixture");

        let prepared = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                panic!("missing session must not attempt refresh")
            })
            .expect("terminal preparation");

        assert!(matches!(
            prepared,
            DesktopSessionPreparation::Terminal(DesktopSessionTerminal {
                reason: SessionTerminalReason::AuthenticationRequired,
                ..
            })
        ));
    }

    #[test]
    fn sequential_runtime_refreshes_use_the_latest_durable_generation() {
        let (mut first, store, endpoint_id) = fixture();
        let mut second = GatewayRuntime {
            config: first.config.clone(),
            timings: first.timings,
            ws_timings: first.ws_timings,
            registry_path: first.registry_path.clone(),
            registry: first.registry.clone(),
            secrets: DesktopSecrets::new(store),
            terminal_sessions: HashMap::new(),
            access_expiries: HashMap::new(),
        };
        let calls = Arc::new(AtomicU64::new(0));
        let first_calls = calls.clone();
        first
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), move |_, _, _, _| {
                first_calls.fetch_add(1, Ordering::SeqCst);
                Ok(refresh_grant(1))
            })
            .unwrap();
        second
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, _, _| {
                assert_eq!(raw, refresh_token(1));
                Ok(refresh_grant(2))
            })
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            second
                .secrets
                .get_gateway_session(endpoint_id.as_str())
                .unwrap()
                .unwrap()
                .refresh_generation,
            2
        );
    }

    #[test]
    fn destructive_session_mutation_excludes_refresh_until_guard_drops() {
        let (mut runtime, _, endpoint_id) = fixture();
        let mutation = runtime
            .begin_session_mutation(endpoint_id.as_str())
            .expect("begin mutation");

        let error = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                panic!("refresh must not start during a destructive session mutation")
            })
            .expect_err("mutation must exclude refresh");
        assert!(
            format!("{error:#}").contains("session mutation is in progress"),
            "unexpected error: {error:#}"
        );

        drop(mutation);
        runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                Ok(refresh_grant(1))
            })
            .expect("refresh after mutation");
    }

    #[test]
    fn serialized_refresh_operation_holds_the_shared_slot_until_handoff_finishes() {
        let (mut runtime, _, endpoint_id) = fixture();
        let slot = refresh_slot(runtime.registry_path.as_path(), endpoint_id.as_str())
            .expect("shared refresh slot");

        runtime
            .with_gateway_session_refresh_slot(endpoint_id.as_str(), |_, _| {
                assert!(
                    slot.try_lock().is_err(),
                    "the slot must remain held through credential handoff"
                );
                Ok(())
            })
            .expect("serialized refresh operation");

        assert!(
            slot.try_lock().is_ok(),
            "the slot must be released afterwards"
        );
    }

    #[test]
    fn pre_dispatch_refresh_failure_keeps_the_durable_credential_retryable() {
        let (mut runtime, _, endpoint_id) = fixture();
        let first = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, _, _| {
                assert_eq!(raw, refresh_token(0));
                Err(AuthExchangeError {
                    kind: AuthExchangeErrorKind::TransportBeforeRequest,
                    code: None,
                    message: "Gateway connection failed before request dispatch".to_owned(),
                })
            })
            .expect_err("pre-dispatch failure must remain transient");
        assert!(format!("{first:#}").contains("before request dispatch"));

        let second = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, _, _| {
                assert_eq!(raw, refresh_token(0));
                Ok(refresh_grant(1))
            })
            .expect("the unchanged durable credential must be retryable");
        assert!(matches!(second, DesktopSessionPreparation::Ready(_)));
    }

    #[test]
    fn refresh_response_loss_retries_with_the_durable_request_id() {
        let (mut runtime, _, endpoint_id) = fixture();
        let first = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                Err(AuthExchangeError {
                    kind: AuthExchangeErrorKind::Timeout,
                    code: None,
                    message: "response outcome unknown".to_owned(),
                })
            })
            .expect_err("lost response remains retryable");
        assert!(format!("{first:#}").contains("outcome unknown"));
        let pending_request_id = runtime
            .secrets
            .get_gateway_session(endpoint_id.as_str())
            .unwrap()
            .unwrap()
            .pending_refresh_request_id
            .expect("refresh intent must be durable before dispatch");
        let second = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, request_id, _| {
                assert_eq!(raw, refresh_token(0));
                assert_eq!(request_id, pending_request_id);
                Ok(refresh_grant(1))
            })
            .expect("same exchange request recovers the committed successor");
        assert!(matches!(second, DesktopSessionPreparation::Ready(_)));
        assert!(
            runtime
                .secrets
                .get_gateway_session(endpoint_id.as_str())
                .unwrap()
                .unwrap()
                .pending_refresh_request_id
                .is_none()
        );
    }

    #[test]
    fn refresh_response_loss_recovers_after_desktop_restart() {
        let (mut first, store, endpoint_id) = fixture();
        first
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                Err(AuthExchangeError {
                    kind: AuthExchangeErrorKind::Timeout,
                    code: None,
                    message: "response outcome unknown".to_owned(),
                })
            })
            .expect_err("lost response remains retryable");
        let pending_request_id = first
            .secrets
            .get_gateway_session(endpoint_id.as_str())
            .unwrap()
            .unwrap()
            .pending_refresh_request_id
            .expect("refresh intent persisted before process restart");
        let mut restarted = GatewayRuntime {
            config: first.config.clone(),
            timings: first.timings,
            ws_timings: first.ws_timings,
            registry_path: first.registry_path.clone(),
            registry: first.registry.clone(),
            secrets: DesktopSecrets::new(store),
            terminal_sessions: HashMap::new(),
            access_expiries: HashMap::new(),
        };

        let prepared = restarted
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, request_id, _| {
                assert_eq!(raw, refresh_token(0));
                assert_eq!(request_id, pending_request_id);
                Ok(refresh_grant(1))
            })
            .expect("restart must retry the durable refresh exchange");

        assert!(matches!(prepared, DesktopSessionPreparation::Ready(_)));
        assert!(
            restarted
                .secrets
                .get_gateway_session(endpoint_id.as_str())
                .unwrap()
                .unwrap()
                .pending_refresh_request_id
                .is_none()
        );
    }

    #[test]
    fn malformed_refresh_response_preserves_the_recoverable_exchange() {
        let (mut runtime, _, endpoint_id) = fixture();
        let first = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                Err(AuthExchangeError {
                    kind: AuthExchangeErrorKind::Protocol,
                    code: None,
                    message: "malformed response after refresh request".to_owned(),
                })
            })
            .expect_err("malformed response remains retryable with the same exchange id");
        assert!(format!("{first:#}").contains("malformed response"));
        let pending_request_id = runtime
            .secrets
            .get_gateway_session(endpoint_id.as_str())
            .unwrap()
            .unwrap()
            .pending_refresh_request_id
            .unwrap();
        let second = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, request_id, _| {
                assert_eq!(request_id, pending_request_id);
                Ok(refresh_grant(1))
            })
            .expect("protocol retry recovers through server idempotency");
        assert!(matches!(second, DesktopSessionPreparation::Ready(_)));
    }

    #[test]
    fn malformed_refresh_grant_with_zero_access_expiry_is_rejected() {
        let previous = stored_session(0);
        let mut grant = refresh_grant(1);
        grant.access_expires_at_unix = 0;
        assert!(rotated_session(&previous, "desktop-installation", grant).is_err());
    }

    #[test]
    fn refresh_grant_for_a_different_desktop_installation_is_rejected() {
        let previous = stored_session(0);
        let mut grant = refresh_grant(1);
        grant.device.installation_id = "different-installation".to_owned();
        assert!(rotated_session(&previous, "desktop-installation", grant).is_err());
    }

    #[test]
    fn refresh_grant_for_a_different_token_family_is_rejected() {
        let previous = stored_session(0);
        let mut grant = refresh_grant(1);
        grant.session.token_family_id = TokenFamilyId::new("F00000000000000000002").unwrap();
        assert!(rotated_session(&previous, "desktop-installation", grant).is_err());
    }

    #[test]
    fn changed_registry_installation_is_terminal_before_refresh() {
        let (mut runtime, _, endpoint_id) = fixture();
        runtime.registry.installation_id = Some("different-installation".to_owned());
        let prepared = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                panic!("installation mismatch must fail before presenting refresh")
            })
            .unwrap();

        assert!(matches!(
            prepared,
            DesktopSessionPreparation::Terminal(DesktopSessionTerminal {
                reason: SessionTerminalReason::SessionCompromised,
                ..
            })
        ));
    }

    #[test]
    fn restart_reloads_rotated_envelope_and_refreshes_next_generation() {
        let (mut first, store, endpoint_id) = fixture();
        first
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                Ok(refresh_grant(1))
            })
            .unwrap();
        let mut restarted = GatewayRuntime {
            config: first.config.clone(),
            timings: first.timings,
            ws_timings: first.ws_timings,
            registry_path: first.registry_path.clone(),
            registry: first.registry.clone(),
            secrets: DesktopSecrets::new(store),
            terminal_sessions: HashMap::new(),
            access_expiries: HashMap::new(),
        };
        restarted
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, _, _| {
                assert_eq!(raw, refresh_token(1));
                Ok(refresh_grant(2))
            })
            .unwrap();
        assert_eq!(
            restarted
                .secrets
                .get_gateway_session(endpoint_id.as_str())
                .unwrap()
                .unwrap()
                .refresh_generation,
            2
        );
    }

    #[test]
    fn server_terminal_code_stops_reconnect_path() {
        let (mut runtime, _, endpoint_id) = fixture();
        let prepared = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, _, _, _| {
                Err(AuthExchangeError {
                    kind: AuthExchangeErrorKind::Server,
                    code: Some("session_revoked".to_owned()),
                    message: "session is revoked".to_owned(),
                })
            })
            .unwrap();
        assert!(matches!(
            prepared,
            DesktopSessionPreparation::Terminal(DesktopSessionTerminal {
                reason: SessionTerminalReason::SessionRevoked,
                ..
            })
        ));
        assert_eq!(
            runtime.session_terminal_reason(endpoint_id.as_str()),
            Some(SessionTerminalReason::SessionRevoked)
        );
    }
}
