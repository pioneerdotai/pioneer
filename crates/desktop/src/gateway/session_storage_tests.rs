//! Synthetic native-storage fixtures replay the Client session contracts.
use crate::gateway::secrets::DESKTOP_GATEWAY_SESSION_SCHEMA_VERSION;
use anyhow::{Context, Result};
use pioneer_client::gateway::{
    endpoint::GatewayBaseUrl,
    session_lifecycle::{GatewaySessionMetadata, SessionTerminalReason},
    session_refresh::{
        GatewaySessionAccessGrant as DesktopSessionAccessGrant,
        GatewaySessionPreparation as DesktopSessionPreparation,
        GatewaySessionTerminal as DesktopSessionTerminal,
    },
};
use pioneer_client::transport::ws::auth_exchange::{AuthExchangeError, AuthExchangeErrorKind};
use pioneer_protocol::{AuthMeResponse, AuthRefreshGrant, CredentialStorageOrder};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
const TRANSIENT_REFRESH_RETRY_DELAYS: [Duration; 3] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
];
struct SessionFixture {
    client_core: std::sync::Arc<pioneer_client::core::ClientCore>,
    config: pioneer_config::AppConfig,
    timings: pioneer_client::gateway::timings::GatewayTimings,
    ws_timings: pioneer_client::gateway::timings::GatewayWsTimings,
    registry_path: std::path::PathBuf,
    registry: pioneer_client::gateway::types::GatewayRegistry,
    secrets: DesktopSecrets,
}
impl SessionFixture {
    fn session_terminal_reason(&self, id: &str) -> Option<SessionTerminalReason> {
        self.client_core.gateway_session().terminal_reason(id)
    }
    fn begin_session_mutation(
        &self,
        id: &str,
    ) -> Result<pioneer_client::gateway::session_controller::GatewaySessionMutationGuard> {
        self.client_core.begin_gateway_session_mutation(id)
    }
    fn prepare_gateway_session_with_refresh<F>(
        &mut self,
        id: &str,
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
        let endpoint = pioneer_client::gateway::runtime::endpoint_by_id(&self.registry, id)
            .context("synthetic endpoint")?;
        if let Some(reason) = self.session_terminal_reason(id) {
            return Ok(DesktopSessionPreparation::Terminal(
                DesktopSessionTerminal {
                    reason,
                    metadata: None,
                },
            ));
        }
        self.client_core.with_gateway_session_refresh(id, || {
            self.client_core.prepare_gateway_session(
                pioneer_client::gateway::session_refresh::GatewaySessionRefreshRequest {
                    endpoint,
                    installation_id: self.registry.installation_id.as_deref().unwrap(),
                    client_kind: ClientKind::Desktop,
                    now_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                    timeout: self.timings.startup_timeout,
                    ws_timings: pioneer_client::gateway::runtime::ws_timings_for_endpoint(
                        self.ws_timings,
                        endpoint.kind,
                        Duration::from_secs(5),
                    ),
                    retry_delays: &TRANSIENT_REFRESH_RETRY_DELAYS,
                },
                &self.secrets,
                refresh,
                |_, _, _| Ok(()),
            )
        })
    }
}
fn rotated_session(
    previous: &DesktopGatewaySessionSecret,
    installation_id: &str,
    grant: AuthRefreshGrant,
) -> Result<(DesktopGatewaySessionSecret, DesktopSessionAccessGrant)> {
    previous.rotate(installation_id, ClientKind::Desktop, grant)
}
fn validate_gateway_session_identity(
    pin: Option<&pioneer_protocol::GatewayId>,
    installation: &str,
    stored: &DesktopGatewaySessionSecret,
    me: &AuthMeResponse,
) -> Option<SessionTerminalReason> {
    stored.identity_failure(pin, installation, ClientKind::Desktop, me)
}

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

static NEXT_ENDPOINT: AtomicU64 = AtomicU64::new(1);

fn fixture() -> (SessionFixture, Arc<MemorySecretStore>, String) {
    let config = test_config();
    let timings = gateway_timings_from_config(&config.desktop.gateway).unwrap();
    let ws_timings = gateway_ws_timings_from_config(&config.desktop.gateway).unwrap();
    let mut registry = default_registry(&config).unwrap();
    registry.installation_id = Some("desktop-installation".to_owned());
    let endpoint_id = format!("local-{}", NEXT_ENDPOINT.fetch_add(1, Ordering::Relaxed));
    let local = registry.local.as_mut().unwrap();
    local.id = endpoint_id.clone();
    local.kind = GatewayEndpointKind::Local;
    local.gateway_base_url = pioneer_client::gateway::endpoint::GatewayBaseUrl::parse_presentation(
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
        SessionFixture {
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
    let mut second = SessionFixture {
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
    let mut restarted = SessionFixture {
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
    let mut restarted = SessionFixture {
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
