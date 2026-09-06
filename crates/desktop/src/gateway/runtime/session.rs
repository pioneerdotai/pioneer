#[cfg(test)]
use crate::gateway::activation::DesktopSessionAccessGrant;
#[cfg(test)]
use pioneer_protocol::CredentialStorageOrder;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use crate::gateway::secrets::DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION;
use anyhow::{Context, Result};
use pioneer_client::{
    gateway::endpoint::GatewayBaseUrl,
    gateway::runtime as client_gateway_runtime,
    gateway::session_lifecycle::{
        GatewaySessionMetadata, SessionLifecycleEvent, SessionTerminalReason,
    },
    transport::ws::{
        GatewayWsCommandSender,
        auth_exchange::{AuthExchangeClient, AuthExchangeError, AuthExchangeErrorKind},
    },
};
#[cfg(test)]
use pioneer_protocol::AuthMeResponse;
use pioneer_protocol::{AuthRefreshGrant, AuthRefreshParams, ClientKind};

use crate::gateway::{
    activation::revoke_session_best_effort, secrets::DesktopGatewaySessionSecret,
};

use super::GatewayRuntime;

const TRANSIENT_REFRESH_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
];

pub(crate) use pioneer_client::gateway::session_controller::GatewaySessionMutationGuard as DesktopSessionMutationGuard;

pub(crate) use pioneer_client::gateway::session_refresh::{
    GatewaySessionPreparation as DesktopSessionPreparation,
    GatewaySessionTerminal as DesktopSessionTerminal,
};

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
        _sender: &GatewayWsCommandSender,
    ) -> Result<Option<SessionTerminalReason>> {
        let endpoint = self
            .endpoint_by_id(endpoint_id)
            .context("unknown Gateway endpoint")?;
        let installation_id = self
            .registry
            .installation_id
            .as_deref()
            .context("Gateway installation identity is missing")?;
        self.client_core.verify_gateway_session_identity(
            endpoint,
            installation_id,
            ClientKind::Desktop,
        )
    }

    pub(crate) fn start_gateway_session_transport(
        &self,
        spec: pioneer_client::transport::ws::GatewayWsConnectSpec,
        retry_initial_failure: bool,
    ) -> Result<u64> {
        self.client_core
            .start_gateway_session_transport(spec, retry_initial_failure)
    }

    pub(crate) fn prepare_gateway_session(
        &mut self,
        endpoint_id: &str,
    ) -> Result<DesktopSessionPreparation> {
        self.with_gateway_session_refresh_slot(endpoint_id, |runtime| {
            runtime.prepare_gateway_session_locked(endpoint_id)
        })
    }

    fn with_gateway_session_refresh_slot<T>(
        &mut self,
        endpoint_id: &str,
        operation: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let core = self.client_core.clone();
        core.with_gateway_session_refresh(endpoint_id, || operation(self))
    }

    fn prepare_gateway_session_locked(
        &mut self,
        endpoint_id: &str,
    ) -> Result<DesktopSessionPreparation> {
        if let Some(reason) = self
            .client_core
            .gateway_session()
            .terminal_reason(endpoint_id)
        {
            return Ok(DesktopSessionPreparation::Terminal(
                DesktopSessionTerminal {
                    metadata: self.session_metadata(endpoint_id).unwrap_or(None),
                    reason,
                },
            ));
        }
        let cleanup_timeout = self.timings.startup_timeout;
        let core = self.client_core.clone();
        let storage =
            pioneer_client::gateway::session_refresh::GatewaySessionPlatformStorage(&core);
        self.prepare_gateway_session_serialized(
            endpoint_id,
            &storage,
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
        F: FnMut(
            &GatewayBaseUrl,
            &str,
            &str,
            Duration,
        ) -> std::result::Result<AuthRefreshGrant, AuthExchangeError>,
    {
        if let Some(reason) = self
            .client_core
            .gateway_session()
            .terminal_reason(endpoint_id)
        {
            return Ok(DesktopSessionPreparation::Terminal(
                DesktopSessionTerminal {
                    metadata: self.session_metadata(endpoint_id).unwrap_or(None),
                    reason,
                },
            ));
        }
        let core = self.client_core.clone();
        core.with_gateway_session_refresh(endpoint_id, || {
            let storage = self.secrets.clone();
            self.prepare_gateway_session_serialized(
                endpoint_id,
                &storage,
                refresh,
                |_, _, _| Ok(()),
            )
        })
    }

    pub(crate) fn replace_gateway_session_access(
        &mut self,
        endpoint_id: &str,
        _sender: &GatewayWsCommandSender,
    ) -> Result<DesktopSessionConnectionOutcome> {
        self.ensure_gateway_session_connection(endpoint_id, None)
    }

    fn ensure_gateway_session_connection(
        &self,
        endpoint_id: &str,
        rejected_generation: Option<u64>,
    ) -> Result<DesktopSessionConnectionOutcome> {
        let endpoint = self
            .endpoint_by_id(endpoint_id)
            .cloned()
            .context("unknown Gateway endpoint")?;
        let installation_id = self
            .registry
            .installation_id
            .as_deref()
            .context("Gateway installation identity is missing")?;
        let storage = pioneer_client::gateway::session_refresh::GatewaySessionPlatformStorage(
            &self.client_core,
        );
        let request = pioneer_client::gateway::session_refresh::GatewaySessionRefreshRequest {
            endpoint: &endpoint,
            installation_id,
            client_kind: ClientKind::Desktop,
            now_unix: unix_timestamp_secs()?,
            timeout: self.timings.startup_timeout,
            ws_timings: client_gateway_runtime::ws_timings_for_endpoint(
                self.ws_timings,
                endpoint.kind,
                Duration::from_secs(5),
            ),
            retry_delays: &TRANSIENT_REFRESH_RETRY_DELAYS,
        };
        let result = match rejected_generation {
            Some(rejected) => self
                .client_core
                .refresh_gateway_session_after_unauthorized(request, &storage, rejected),
            None => self.client_core.ensure_gateway_session(request, &storage),
        };
        match result {
            Ok(connected) => Ok(DesktopSessionConnectionOutcome::Connected {
                connection_id: connected.connection_id, metadata: connected.metadata,
                access_expires_at_unix: connected.access_expires_at_unix,
            }),
            Err(pioneer_client::gateway::session_connection::GatewaySessionConnectionFailure::Terminal { reason }) => {
                let metadata = match self.client_core.gateway_session().session(endpoint_id) {
                    Some(pioneer_client::gateway::session_lifecycle::SessionLifecycleState::Terminal { metadata, .. }) => metadata.clone(),
                    _ => None,
                };
                Ok(DesktopSessionConnectionOutcome::Terminal(DesktopSessionTerminal { metadata, reason }))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn replace_gateway_session_access_after_rejection(
        &mut self,
        endpoint_id: &str,
        sender: &GatewayWsCommandSender,
        rejected_generation: u64,
    ) -> Result<Option<DesktopSessionConnectionOutcome>> {
        if let Ok(current) = sender.current_gateway_http_access()
            && current.generation != rejected_generation
        {
            return Ok(None);
        }
        self.ensure_gateway_session_connection(endpoint_id, Some(rejected_generation))
            .map(Some)
    }

    pub(crate) fn recover_gateway_session_access(
        &mut self,
        endpoint_id: &str,
        _sender: &GatewayWsCommandSender,
    ) -> Result<DesktopSessionConnectionOutcome> {
        self.with_gateway_session_refresh_slot(endpoint_id, |runtime| {
            match runtime.prepare_gateway_session_locked(endpoint_id)? {
                DesktopSessionPreparation::Terminal(terminal) => {
                    Ok(DesktopSessionConnectionOutcome::Terminal(terminal))
                }
                DesktopSessionPreparation::Ready(ready) => {
                    let expires_at = ready.spec.identity.access_expires_at_unix;
                    let connection_id = runtime
                        .start_gateway_session_transport(ready.spec.into_connect_spec(), true)?;
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
        self.client_core
            .gateway_session()
            .terminal_reason(endpoint_id)
    }

    pub(crate) fn clear_session_terminal_for_explicit_retry(&mut self, endpoint_id: &str) {
        self.client_core
            .reduce_gateway_session_lifecycle(endpoint_id, SessionLifecycleEvent::NoStoredSession);
    }

    pub(crate) fn forget_gateway_session_after_logout(&mut self, endpoint_id: &str) -> Result<()> {
        let session_ref = self
            .endpoint_by_id(endpoint_id)
            .with_context(|| format!("unknown desktop Gateway endpoint `{endpoint_id}`"))?
            .session_ref
            .clone();
        self.client_core.reduce_gateway_session_lifecycle(
            endpoint_id,
            SessionLifecycleEvent::AuthFailed {
                reason: SessionTerminalReason::SessionRevoked,
            },
        );
        if let Some(session_ref) = session_ref.as_deref() {
            self.secrets.delete_gateway_session(session_ref)?;
        }
        Ok(())
    }

    pub(crate) fn discard_gateway_session_runtime_state(&mut self, endpoint_id: &str) {
        self.client_core
            .reduce_gateway_session_lifecycle(endpoint_id, SessionLifecycleEvent::NoStoredSession);
    }

    pub(crate) fn active_session_refresh_delay(
        &self,
        now_unix: u64,
        refresh_leeway_seconds: u64,
    ) -> Option<Duration> {
        let endpoint_id = self.active_gateway_id()?;
        self.client_core.gateway_session().refresh_delay(
            endpoint_id,
            now_unix,
            refresh_leeway_seconds,
        )
    }

    fn prepare_gateway_session_serialized<F, C>(
        &mut self,
        endpoint_id: &str,
        storage: &dyn pioneer_client::gateway::session_refresh::GatewaySessionStorage,
        refresh: F,
        cleanup_session: C,
    ) -> Result<DesktopSessionPreparation>
    where
        F: FnMut(
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
        let installation_id = self
            .registry
            .installation_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .context("desktop Gateway registry has no installation id")?;
        self.client_core.prepare_gateway_session(
            pioneer_client::gateway::session_refresh::GatewaySessionRefreshRequest {
                endpoint: &endpoint,
                installation_id,
                client_kind: ClientKind::Desktop,
                now_unix: unix_timestamp_secs()?,
                timeout: self.timings.startup_timeout,
                ws_timings: client_gateway_runtime::ws_timings_for_endpoint(
                    self.ws_timings,
                    endpoint.kind,
                    Duration::from_secs(5),
                ),
                retry_delays: &TRANSIENT_REFRESH_RETRY_DELAYS,
            },
            storage,
            refresh,
            cleanup_session,
        )
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

#[cfg(test)]
fn rotated_session(
    previous: &DesktopGatewaySessionSecret,
    installation_id: &str,
    grant: AuthRefreshGrant,
) -> Result<(DesktopGatewaySessionSecret, DesktopSessionAccessGrant)> {
    previous.rotate(installation_id, ClientKind::Desktop, grant)
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

#[cfg(test)]
fn validate_gateway_session_identity(
    pinned_gateway_id: Option<&pioneer_protocol::GatewayId>,
    expected_installation_id: &str,
    stored: &DesktopGatewaySessionSecret,
    me: &AuthMeResponse,
) -> Option<SessionTerminalReason> {
    stored.identity_failure(
        pinned_gateway_id,
        expected_installation_id,
        ClientKind::Desktop,
        me,
    )
}

impl GatewayRuntime {
    pub(crate) fn begin_session_mutation(
        &self,
        endpoint_id: &str,
    ) -> Result<DesktopSessionMutationGuard> {
        self.client_core.begin_gateway_session_mutation(endpoint_id)
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
        PrincipalKind, TokenFamilyId,
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
                client_core: std::sync::Arc::new(pioneer_client::core::ClientCore::new()),
                config,
                timings,
                ws_timings,
                registry_path: std::env::temp_dir().join("unused-session-runtime-registry"),
                registry,
                secrets,
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
                avatar_revision: None,
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
    fn invited_member_identity_and_refresh_are_accepted() {
        let stored = stored_session(0);
        let mut grant = refresh_grant(1);
        grant.principal.kind = PrincipalKind::User;
        grant.principal.display_name = "Invited Member".to_owned();
        grant.principal.nickname = "invited_member".to_owned();

        let mut me = auth_me(0);
        me.principal = grant.principal.clone();
        assert_eq!(
            validate_gateway_session_identity(
                Some(&stored.gateway_id),
                "desktop-installation",
                &stored,
                &me,
            ),
            None
        );
        assert!(rotated_session(&stored, "desktop-installation", grant).is_ok());
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
            client_core: std::sync::Arc::new(pioneer_client::core::ClientCore::new()),
            config: first.config.clone(),
            timings: first.timings,
            ws_timings: first.ws_timings,
            registry_path: first.registry_path.clone(),
            registry: first.registry.clone(),
            secrets: DesktopSecrets::new(store),
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
    fn transient_refresh_retries_reuse_the_durable_request_id() {
        let (mut runtime, _, endpoint_id) = fixture();
        let mut attempts = 0_usize;
        let mut durable_request_id = None;

        let prepared = runtime
            .prepare_gateway_session_with_refresh(endpoint_id.as_str(), |_, raw, request_id, _| {
                assert_eq!(raw, refresh_token(0));
                if let Some(expected) = durable_request_id.as_deref() {
                    assert_eq!(request_id, expected);
                } else {
                    durable_request_id = Some(request_id.to_owned());
                }
                attempts = attempts.saturating_add(1);
                if attempts <= TRANSIENT_REFRESH_RETRY_DELAYS.len() {
                    Err(AuthExchangeError {
                        kind: AuthExchangeErrorKind::Server,
                        code: Some("temporarily_unavailable".to_owned()),
                        message: "Gateway database is busy".to_owned(),
                    })
                } else {
                    Ok(refresh_grant(1))
                }
            })
            .expect("transient Gateway backpressure should recover in place");

        assert_eq!(attempts, TRANSIENT_REFRESH_RETRY_DELAYS.len() + 1);
        assert!(matches!(prepared, DesktopSessionPreparation::Ready(_)));
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
            client_core: std::sync::Arc::new(pioneer_client::core::ClientCore::new()),
            config: first.config.clone(),
            timings: first.timings,
            ws_timings: first.ws_timings,
            registry_path: first.registry_path.clone(),
            registry: first.registry.clone(),
            secrets: DesktopSecrets::new(store),
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
            client_core: std::sync::Arc::new(pioneer_client::core::ClientCore::new()),
            config: first.config.clone(),
            timings: first.timings,
            ws_timings: first.ws_timings,
            registry_path: first.registry_path.clone(),
            registry: first.registry.clone(),
            secrets: DesktopSecrets::new(store),
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
